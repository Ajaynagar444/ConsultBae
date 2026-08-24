"""Repair decisions and the staging load.

Split in two: repair logic is pure and tested without a database; the staging
load runs against consultbae_test.
"""

from __future__ import annotations

import psycopg
import pytest

from src.pipeline.config import SOURCES_BY_KEY
from src.pipeline.ingest import read_source, run_ingestion
from src.pipeline.repair import (
    Verdict,
    assess,
    detect_column_shift,
    is_blank_row,
    is_embedded_header,
)
from src.pipeline.stage import latest_run, stage_run, transform

NAUKRI = SOURCES_BY_KEY["naukri"]
GIG = SOURCES_BY_KEY["gig_workers"]
CBNEXUS = SOURCES_BY_KEY["cbnexus"]


def payload_at(spec, line_no: int) -> dict:
    """The real payload for one source line, straight from the CSV."""
    _, rows = read_source(spec)
    return next(r.payload for r in rows if r.line_no == line_no)


# ===========================================================================
# repair - pure
# ===========================================================================

def test_blank_row_detected():
    assert is_blank_row(payload_at(GIG, 12), GIG) is True


def test_normal_row_is_not_blank():
    assert is_blank_row(payload_at(GIG, 7), GIG) is False


def test_embedded_header_detected():
    assert is_embedded_header(payload_at(CBNEXUS, 16), CBNEXUS) is True


def test_normal_row_is_not_a_header():
    assert is_embedded_header(payload_at(CBNEXUS, 2), CBNEXUS) is False


def test_embedded_header_detection_is_case_insensitive():
    faked = {c: c.upper() for c in CBNEXUS.columns}
    assert is_embedded_header(faked, CBNEXUS) is True


def test_column_shift_detected_and_repaired():
    """source2 line 20 against the correct line 7."""
    broken = payload_at(GIG, 20)
    correct = payload_at(GIG, 7)

    result = detect_column_shift(broken, GIG)
    assert result is not None
    repaired, note = result

    assert repaired == correct
    assert "rotated" in note


def test_repair_explains_its_evidence():
    _, note = detect_column_shift(payload_at(GIG, 20), GIG)
    assert "email_id" in note and "'@'" in note


def test_a_correct_row_is_not_repaired():
    assert detect_column_shift(payload_at(GIG, 7), GIG) is None


def test_repair_is_not_keyed_on_a_line_number():
    """Rotating any good row must be detected the same way."""
    good = payload_at(GIG, 2)
    values = [good[c] for c in GIG.columns]
    rotated = dict(zip(GIG.columns, values[-1:] + values[:-1]))

    result = detect_column_shift(rotated, GIG)
    assert result is not None
    assert result[0] == good


def test_row_without_an_email_is_not_scrambled():
    """No '@' anywhere means we cannot prove a rotation, so leave it alone."""
    noisy = {c: "x" for c in GIG.columns}
    assert detect_column_shift(noisy, GIG) is None


def test_assess_returns_the_right_verdicts():
    assert assess(payload_at(GIG, 12), GIG).verdict is Verdict.QUARANTINE
    assert assess(payload_at(GIG, 12), GIG).reason == "blank_row"
    assert assess(payload_at(CBNEXUS, 16), CBNEXUS).reason == "embedded_header"
    assert assess(payload_at(GIG, 20), GIG).verdict is Verdict.REPAIRED
    assert assess(payload_at(GIG, 2), GIG).verdict is Verdict.OK


def test_only_three_rows_in_the_whole_corpus_are_not_ok():
    odd = []
    for spec in (NAUKRI, GIG, CBNEXUS):
        for row in read_source(spec)[1]:
            v = assess(row.payload, spec)
            if v.verdict is not Verdict.OK:
                odd.append((spec.key, row.line_no, v.verdict.value))
    assert sorted(odd) == [
        ("cbnexus", 16, "quarantine"),
        ("gig_workers", 12, "quarantine"),
        ("gig_workers", 20, "repaired"),
    ]


# ===========================================================================
# transform - pure
# ===========================================================================

def test_quarantined_row_carries_its_reason():
    row = transform(1, GIG, payload_at(GIG, 12))
    assert row.columns["is_quarantined"] is True
    assert row.columns["quarantined_as"] == "blank_row"


def test_repaired_row_is_not_quarantined_and_explains_itself():
    row = transform(1, GIG, payload_at(GIG, 20))
    assert row.columns["is_quarantined"] is False
    assert row.columns["was_repaired"] is True
    assert row.columns["repair_note"]


def test_repaired_row_normalises_to_the_same_person_as_the_original():
    """Line 20 repaired must equal line 7 staged - the duplicate is now visible.

    Note what this test does NOT do: it does not merge them. Recognising that
    two staged rows describe one person is the matching layer's job.
    """
    repaired = transform(1, GIG, payload_at(GIG, 20)).columns
    original = transform(2, GIG, payload_at(GIG, 7)).columns

    for f in ("email_norm", "full_name", "name_key", "city_norm",
              "rate_amount", "rate_source_unit", "status", "skills"):
        assert repaired[f] == original[f], f


def test_naukri_row_normalises_end_to_end():
    row = transform(1, NAUKRI, payload_at(NAUKRI, 2))
    c = row.columns
    assert c["full_name"] == "Tanvi Gupta"
    assert c["email_norm"] == "tanvi.gupta31@example.com"
    assert c["phone_e164"] == "+919000000254"
    assert c["city_norm"] == "Bengaluru"
    assert c["ctc_annual_inr"] == 417964
    assert c["ctc_source_unit"] == "rupee"
    assert str(c["applied_on"]) == "2026-07-24"
    assert c["skills"] == ["n8n", "langchain", "rest apis", "mongodb", "sql"]


def test_r_verma_and_rohit_verma_stage_with_different_name_keys():
    """Same person, but staging must not be the thing that says so."""
    a = transform(1, NAUKRI, payload_at(NAUKRI, 25)).columns
    b = transform(2, NAUKRI, payload_at(NAUKRI, 31)).columns
    assert a["name_key"] != b["name_key"]
    assert a["email_norm"] == b["email_norm"]     # the real evidence
    assert a["phone_e164"] == b["phone_e164"]


def test_two_deepak_nairs_stage_as_two_rows_with_different_emails():
    rows = read_source(GIG)[1]
    a = transform(1, GIG, next(r.payload for r in rows if r.line_no == 15)).columns
    b = transform(2, GIG, next(r.payload for r in rows if r.line_no == 32)).columns
    assert a["name_key"] == b["name_key"] == "deepak nair"
    assert a["email_norm"] != b["email_norm"]


# ===========================================================================
# staging load - integration
# ===========================================================================

@pytest.fixture
def ingested(conn):
    return run_ingestion(conn).run_id


def test_one_staged_row_per_raw_row(conn, ingested):
    r = stage_run(conn, ingested)
    assert r.raw_rows == 105
    assert r.staged == 105

    counts = conn.execute(
        """SELECT (SELECT count(*) FROM raw_record WHERE run_id = %s),
                  (SELECT count(*) FROM staged_person s
                    JOIN raw_record rr ON rr.id = s.raw_record_id
                   WHERE rr.run_id = %s)""",
        (ingested, ingested),
    ).fetchone()
    assert counts[0] == counts[1] == 105


def test_quarantine_and_repair_counts(conn, ingested):
    r = stage_run(conn, ingested)
    assert r.quarantined == 2
    assert r.repaired == 1


def test_quarantined_rows_are_the_expected_two(conn, ingested):
    stage_run(conn, ingested)
    rows = conn.execute(
        """SELECT rr.source_system::text, rr.source_line_no, s.quarantined_as::text
             FROM staged_person s JOIN raw_record rr ON rr.id = s.raw_record_id
            WHERE rr.run_id = %s AND s.is_quarantined ORDER BY 1, 2""",
        (ingested,),
    ).fetchall()
    assert rows == [
        ("cbnexus_contacts", 16, "embedded_header"),
        ("gig_workers", 12, "blank_row"),
    ]


def test_every_quarantined_row_states_a_reason(conn, ingested):
    stage_run(conn, ingested)
    bad = conn.execute(
        "SELECT count(*) FROM staged_person WHERE is_quarantined AND quarantined_as IS NULL"
    ).fetchone()[0]
    assert bad == 0


def test_repaired_row_is_stored_repaired(conn, ingested):
    stage_run(conn, ingested)
    row = conn.execute(
        """SELECT s.email_norm, s.status::text, s.rate_amount, s.was_repaired, s.repair_note
             FROM staged_person s JOIN raw_record rr ON rr.id = s.raw_record_id
            WHERE rr.run_id = %s AND rr.source_system = 'gig_workers'
              AND rr.source_line_no = 20""",
        (ingested,),
    ).fetchone()
    email, status, rate, repaired, note = row
    assert email == "isha.chopra95@mailtest.example.org"
    assert status == "active"
    assert float(rate) == 1406.0
    assert repaired is True
    assert "rotated" in note


def test_raw_layer_is_untouched_by_staging(conn, ingested):
    """The whole point of an append-only raw layer."""
    before = conn.execute(
        "SELECT id, payload, row_sha256 FROM raw_record WHERE run_id = %s ORDER BY id",
        (ingested,),
    ).fetchall()
    stage_run(conn, ingested)
    after = conn.execute(
        "SELECT id, payload, row_sha256 FROM raw_record WHERE run_id = %s ORDER BY id",
        (ingested,),
    ).fetchall()
    assert before == after


def test_the_corrupt_row_is_still_corrupt_in_raw(conn, ingested):
    stage_run(conn, ingested)
    raw = conn.execute(
        """SELECT payload FROM raw_record
            WHERE run_id = %s AND source_system = 'gig_workers' AND source_line_no = 20""",
        (ingested,),
    ).fetchone()[0]
    assert raw["email_id"] == "react, javascript, mysql"   # untouched


def test_provenance_every_staged_row_points_at_a_raw_row(conn, ingested):
    stage_run(conn, ingested)
    orphans = conn.execute(
        """SELECT count(*) FROM staged_person s
            LEFT JOIN raw_record rr ON rr.id = s.raw_record_id
           WHERE rr.id IS NULL"""
    ).fetchone()[0]
    assert orphans == 0


def test_no_person_records_were_created(conn, ingested):
    """Staging must not merge anyone. That is a later step."""
    stage_run(conn, ingested)
    assert conn.execute("SELECT count(*) FROM person").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM person_source_link").fetchone()[0] == 0


# ---- idempotency -----------------------------------------------------------

def test_staging_twice_gives_the_same_rows(conn, ingested):
    first = stage_run(conn, ingested)
    snapshot = conn.execute(
        """SELECT raw_record_id, full_name, email_norm, phone_e164, city_norm,
                  ctc_annual_inr, rate_amount, applied_on, skills, is_quarantined
             FROM staged_person ORDER BY raw_record_id"""
    ).fetchall()

    second = stage_run(conn, ingested)
    again = conn.execute(
        """SELECT raw_record_id, full_name, email_norm, phone_e164, city_norm,
                  ctc_annual_inr, rate_amount, applied_on, skills, is_quarantined
             FROM staged_person ORDER BY raw_record_id"""
    ).fetchall()

    assert first.staged == second.staged == 105
    assert snapshot == again


def test_data_issues_do_not_accumulate_on_rerun(conn, ingested):
    stage_run(conn, ingested)
    first = conn.execute("SELECT count(*) FROM data_issue WHERE run_id = %s", (ingested,)).fetchone()[0]
    stage_run(conn, ingested)
    second = conn.execute("SELECT count(*) FROM data_issue WHERE run_id = %s", (ingested,)).fetchone()[0]
    assert first == second > 0


def test_staged_rows_do_not_accumulate_on_rerun(conn, ingested):
    stage_run(conn, ingested)
    stage_run(conn, ingested)
    stage_run(conn, ingested)
    total = conn.execute("SELECT count(*) FROM staged_person").fetchone()[0]
    assert total == 105


# ---- data issues -----------------------------------------------------------

def test_the_structural_issues_are_recorded(conn, ingested):
    stage_run(conn, ingested)
    codes = dict(conn.execute(
        "SELECT issue_code, count(*) FROM data_issue WHERE run_id = %s GROUP BY 1",
        (ingested,),
    ).fetchall())
    assert codes["row_blank"] == 1
    assert codes["row_embedded_header"] == 1
    assert codes["row_column_shift_repaired"] == 1


def test_documented_issue_classes_all_appear(conn, ingested):
    stage_run(conn, ingested)
    codes = {r[0] for r in conn.execute(
        "SELECT DISTINCT issue_code FROM data_issue WHERE run_id = %s", (ingested,)
    ).fetchall()}
    for expected in (
        "row_blank", "row_embedded_header", "row_column_shift_repaired",
        "phone_format_normalised", "email_case_normalised",
        "name_abbreviated", "ctc_unit_lakh", "rate_unit_monthly",
        "city_alias_applied", "city_region_preserved",
        "status_case_normalised", "verified_spelling_normalised",
        "skills_case_normalised",
        "date_format_dd_mm_yyyy", "date_format_mm_dd_yyyy",
        "date_format_yyyy_mm_dd", "date_format_d_mon_yyyy",
    ):
        assert expected in codes, expected


def test_every_issue_records_what_was_done(conn, ingested):
    stage_run(conn, ingested)
    blank = conn.execute(
        """SELECT count(*) FROM data_issue
            WHERE run_id = %s AND (action_taken IS NULL OR trim(action_taken) = '')""",
        (ingested,),
    ).fetchone()[0]
    assert blank == 0


def test_mixed_ctc_units_split_as_profiled(conn, ingested):
    stage_run(conn, ingested)
    counts = dict(conn.execute(
        """SELECT issue_code, count(*) FROM data_issue
            WHERE run_id = %s AND issue_code LIKE 'ctc_unit_%%' GROUP BY 1""",
        (ingested,),
    ).fetchall())
    assert counts == {"ctc_unit_lakh": 21, "ctc_unit_rupee": 21}


def test_report_view_works(conn, ingested):
    stage_run(conn, ingested)
    rows = conn.execute("SELECT * FROM v_data_issue_report").fetchall()
    assert rows


# ---- normalised values in the database -------------------------------------

def test_no_uppercase_email_survives_staging(conn, ingested):
    stage_run(conn, ingested)
    bad = conn.execute(
        "SELECT count(*) FROM staged_person WHERE email_norm <> lower(email_norm)"
    ).fetchone()[0]
    assert bad == 0


def test_every_stored_phone_is_e164(conn, ingested):
    stage_run(conn, ingested)
    bad = conn.execute(
        r"SELECT count(*) FROM staged_person WHERE phone_e164 !~ '^\+91[0-9]{10}$'"
    ).fetchone()[0]
    assert bad == 0


def test_city_aliases_are_gone(conn, ingested):
    stage_run(conn, ingested)
    leftover = conn.execute(
        "SELECT count(*) FROM staged_person WHERE city_norm IN ('Bangalore', 'bangalore', 'Gurgaon', 'GURGAON')"
    ).fetchone()[0]
    assert leftover == 0


def test_delhi_ncr_flagged_as_region_in_the_database(conn, ingested):
    stage_run(conn, ingested)
    rows = conn.execute(
        "SELECT count(*) FROM staged_person WHERE city_norm = 'Delhi NCR' AND is_region"
    ).fetchone()[0]
    assert rows == 3


def test_latest_run_helper_finds_the_run(conn, ingested):
    assert latest_run(conn) == ingested
