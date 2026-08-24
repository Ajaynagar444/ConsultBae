"""The schema must exist, and its constraints must actually fire.

A CHECK that is present but never rejects anything is worse than no CHECK,
because it reads as protection. Each test below writes deliberately bad data and
asserts the database refuses it.
"""

from __future__ import annotations

import psycopg
import pytest

EXPECTED_TABLES = {
    "audio_submission", "data_issue", "ingestion_run", "match_review",
    "person", "person_email", "person_phone", "person_skill",
    "person_source_link", "raw_record", "schema_migration", "skill",
    "staged_person",
}
EXPECTED_VIEWS = {"v_audio_submission", "v_data_issue_report", "v_person_full"}
EXPECTED_ENUMS = {
    "ctc_unit", "gig_status", "issue_severity", "match_method", "probe_status",
    "quality_label", "quarantine_reason", "rate_unit", "review_status",
    "source_system",
}


# --------------------------------------------------------------------------
# objects
# --------------------------------------------------------------------------

def test_all_tables_exist(conn):
    found = {r[0] for r in conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    )}
    assert EXPECTED_TABLES <= found, f"missing: {sorted(EXPECTED_TABLES - found)}"


def test_all_views_exist(conn):
    found = {r[0] for r in conn.execute(
        "SELECT viewname FROM pg_views WHERE schemaname = 'public'"
    )}
    assert EXPECTED_VIEWS <= found, f"missing: {sorted(EXPECTED_VIEWS - found)}"


def test_all_enum_types_exist(conn):
    found = {r[0] for r in conn.execute(
        """SELECT t.typname FROM pg_type t
           JOIN pg_namespace n ON n.oid = t.typnamespace
           WHERE n.nspname = 'public' AND t.typtype = 'e'"""
    )}
    assert EXPECTED_ENUMS <= found, f"missing: {sorted(EXPECTED_ENUMS - found)}"


def test_views_are_queryable(conn):
    for view in sorted(EXPECTED_VIEWS):
        conn.execute(f"SELECT * FROM {view} LIMIT 0")


def test_pg_trgm_is_not_installed(conn):
    """Matching is deterministic; an unused extension is one more thing to defend."""
    n = conn.execute("SELECT count(*) FROM pg_extension WHERE extname = 'pg_trgm'").fetchone()[0]
    assert n == 0


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

@pytest.fixture
def run_id(conn) -> int:
    return conn.execute(
        "INSERT INTO ingestion_run (note) VALUES ('test') RETURNING id"
    ).fetchone()[0]


@pytest.fixture
def raw_id(conn, run_id) -> int:
    return conn.execute(
        """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
           VALUES (%s, 'gig_workers', 2, '{"a": 1}'::jsonb, repeat('a', 64)) RETURNING id""",
        (run_id,),
    ).fetchone()[0]


@pytest.fixture
def person_id(conn) -> int:
    return conn.execute(
        """INSERT INTO person (full_name, name_key, primary_email)
           VALUES ('Test Person', 'test person', 'test@example.com') RETURNING id"""
    ).fetchone()[0]


def rejects(conn, sql, params=()):
    """Assert the statement is refused, without poisoning the outer transaction."""
    with pytest.raises(psycopg.Error):
        with conn.transaction():
            conn.execute(sql, params)


# --------------------------------------------------------------------------
# raw layer
# --------------------------------------------------------------------------

def test_raw_payload_must_be_an_object(conn, run_id):
    rejects(conn, """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
                     VALUES (%s, 'gig_workers', 5, '[]'::jsonb, repeat('a', 64))""", (run_id,))


def test_raw_line_number_starts_at_two(conn, run_id):
    """Line 1 is the header in all three files."""
    rejects(conn, """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
                     VALUES (%s, 'gig_workers', 1, '{}'::jsonb, repeat('a', 64))""", (run_id,))


def test_raw_natural_key_is_unique(conn, run_id, raw_id):
    rejects(conn, """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
                     VALUES (%s, 'gig_workers', 2, '{}'::jsonb, repeat('b', 64))""", (run_id,))


def test_raw_checksum_must_be_hex(conn, run_id):
    rejects(conn, """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
                     VALUES (%s, 'gig_workers', 9, '{}'::jsonb, repeat('Z', 64))""", (run_id,))


# --------------------------------------------------------------------------
# staging layer
# --------------------------------------------------------------------------

def stage(conn, raw_id, **cols):
    base = {"raw_record_id": raw_id, "source_system": "gig_workers"}
    base.update(cols)
    names = ", ".join(base)
    holes = ", ".join(["%s"] * len(base))
    return f"INSERT INTO staged_person ({names}) VALUES ({holes})", tuple(base.values())


def test_staging_accepts_a_clean_row(conn, raw_id):
    sql, params = stage(
        conn, raw_id,
        full_name="Tanvi Gupta", name_key="tanvi gupta",
        email_norm="tanvi.gupta31@example.com", phone_e164="+919000000254",
        skills=["n8n", "web scraping"],
    )
    conn.execute(sql, params)


def test_staging_rejects_uppercase_email(conn, raw_id):
    sql, params = stage(conn, raw_id, email_norm="TANVI@EXAMPLE.COM")
    rejects(conn, sql, params)


def test_staging_rejects_unnormalised_phone(conn, raw_id):
    """09000000254 and +91-900... must be normalised before they get here."""
    sql, params = stage(conn, raw_id, phone_e164="09000000254")
    rejects(conn, sql, params)


def test_staging_requires_an_identifier_unless_quarantined(conn, raw_id):
    sql, params = stage(conn, raw_id, full_name="Nameless")
    rejects(conn, sql, params)


def test_staging_allows_a_quarantined_row_with_no_identifier(conn, raw_id):
    """source2 line 12 is blank; it is kept, flagged, not dropped."""
    sql, params = stage(conn, raw_id, is_quarantined=True, quarantined_as="blank_row")
    conn.execute(sql, params)


def test_staging_quarantine_flag_and_reason_travel_together(conn, raw_id):
    sql, params = stage(conn, raw_id, email_norm="a@b.com", is_quarantined=True)
    rejects(conn, sql, params)


def test_staging_repaired_row_must_explain_itself(conn, raw_id):
    """source2 line 20 is column-shifted; the repair must be recorded."""
    sql, params = stage(conn, raw_id, email_norm="a@b.com", was_repaired=True)
    rejects(conn, sql, params)


def test_staging_rejects_uppercase_skills(conn, raw_id):
    sql, params = stage(conn, raw_id, email_norm="a@b.com", skills=["n8n", "Web Scraping"])
    rejects(conn, sql, params)


def test_staging_rejects_untrimmed_skills(conn, raw_id):
    sql, params = stage(conn, raw_id, email_norm="a@b.com", skills=["n8n", " web scraping"])
    rejects(conn, sql, params)


def test_staging_rejects_ctc_amount_without_its_unit(conn, raw_id):
    """source1 mixes rupees and lakhs; an amount with no unit is meaningless."""
    sql, params = stage(conn, raw_id, email_norm="a@b.com", ctc_annual_inr=417964)
    rejects(conn, sql, params)


def test_staging_rejects_rate_amount_without_its_unit(conn, raw_id):
    """source2 mixes /hr and k/month, and the two do not reconcile."""
    sql, params = stage(conn, raw_id, email_norm="a@b.com", rate_amount=1415)
    rejects(conn, sql, params)


def test_staging_allows_zero_projects_completed(conn, raw_id):
    """source3 line 9 has a legitimate 0. Zero is not null."""
    sql, params = stage(conn, raw_id, phone_e164="+919000000143", projects_completed=0)
    conn.execute(sql, params)


def test_staging_rejects_negative_projects_completed(conn, raw_id):
    sql, params = stage(conn, raw_id, phone_e164="+919000000143", projects_completed=-1)
    rejects(conn, sql, params)


# --------------------------------------------------------------------------
# golden layer
# --------------------------------------------------------------------------

def test_person_requires_a_contact_route(conn):
    rejects(conn, "INSERT INTO person (full_name, name_key) VALUES ('X', 'x')")


def test_an_email_identifies_exactly_one_person(conn, person_id):
    """The invariant that stops two golden records claiming the same address."""
    other = conn.execute(
        """INSERT INTO person (full_name, name_key, primary_email)
           VALUES ('Other', 'other', 'other@example.com') RETURNING id"""
    ).fetchone()[0]
    conn.execute("INSERT INTO person_email (person_id, email) VALUES (%s, %s)",
                 (person_id, "shared@example.com"))
    rejects(conn, "INSERT INTO person_email (person_id, email) VALUES (%s, %s)",
            (other, "shared@example.com"))


def test_a_phone_identifies_exactly_one_person(conn, person_id):
    other = conn.execute(
        """INSERT INTO person (full_name, name_key, primary_phone_e164)
           VALUES ('Other', 'other', '+919000000001') RETURNING id"""
    ).fetchone()[0]
    conn.execute("INSERT INTO person_phone (person_id, phone_e164) VALUES (%s, %s)",
                 (person_id, "+919000000254"))
    rejects(conn, "INSERT INTO person_phone (person_id, phone_e164) VALUES (%s, %s)",
            (other, "+919000000254"))


def test_person_may_hold_several_emails(conn, person_id):
    """source1 lines 27 and 37: one Nikhil Chopra, two addresses."""
    conn.execute("""INSERT INTO person_email (person_id, email, is_primary)
                    VALUES (%s, 'nikhil.chopra70@example.com', true)""", (person_id,))
    conn.execute("""INSERT INTO person_email (person_id, email, is_primary)
                    VALUES (%s, 'alt.nikhil.chopra70@example.com', false)""", (person_id,))
    n = conn.execute("SELECT count(*) FROM person_email WHERE person_id = %s",
                     (person_id,)).fetchone()[0]
    assert n == 2


def test_only_one_primary_email_per_person(conn, person_id):
    conn.execute("""INSERT INTO person_email (person_id, email, is_primary)
                    VALUES (%s, 'a@example.com', true)""", (person_id,))
    rejects(conn, """INSERT INTO person_email (person_id, email, is_primary)
                     VALUES (%s, 'b@example.com', true)""", (person_id,))


def test_a_source_row_maps_to_exactly_one_person(conn, person_id, raw_id):
    """The core merge invariant: double-counting a source row is impossible."""
    other = conn.execute(
        """INSERT INTO person (full_name, name_key, primary_email)
           VALUES ('Other', 'other', 'other@example.com') RETURNING id"""
    ).fetchone()[0]
    conn.execute("""INSERT INTO person_source_link (person_id, raw_record_id, method)
                    VALUES (%s, %s, 'email_exact')""", (person_id, raw_id))
    rejects(conn, """INSERT INTO person_source_link (person_id, raw_record_id, method)
                     VALUES (%s, %s, 'phone_exact')""", (other, raw_id))


def test_match_confidence_is_bounded(conn, person_id, raw_id):
    rejects(conn, """INSERT INTO person_source_link (person_id, raw_record_id, method, confidence)
                     VALUES (%s, %s, 'name_guarded', 1.5)""", (person_id, raw_id))


def test_data_issue_requires_an_action(conn, run_id):
    """An issue with no recorded action is not a report."""
    rejects(conn, """INSERT INTO data_issue (run_id, issue_code, detail, action_taken)
                     VALUES (%s, 'mixed_units', 'CTC mixes rupees and lakhs', '   ')""", (run_id,))


def test_match_review_stores_each_pair_once(conn, run_id, raw_id):
    """(a,b) and (b,a) must not both queue as separate review items."""
    second = conn.execute(
        """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
           VALUES (%s, 'cbnexus_contacts', 28, '{}'::jsonb, repeat('c', 64)) RETURNING id""",
        (run_id,),
    ).fetchone()[0]
    lo, hi = sorted((raw_id, second))
    conn.execute("""INSERT INTO match_review (run_id, raw_record_id_a, raw_record_id_b, reason, confidence)
                    VALUES (%s, %s, %s, 'Arjun Mehta: ambiguous', 0.5)""", (run_id, lo, hi))
    rejects(conn, """INSERT INTO match_review (run_id, raw_record_id_a, raw_record_id_b, reason, confidence)
                     VALUES (%s, %s, %s, 'duplicate of the same pair', 0.5)""", (run_id, hi, lo))


# --------------------------------------------------------------------------
# audio layer
# --------------------------------------------------------------------------

AUDIO_COLS = ("submitted_name, submitted_phone_raw, storage_key, mime_type, size_bytes")


def test_audio_accepts_a_complete_probe(conn):
    conn.execute(f"""INSERT INTO audio_submission ({AUDIO_COLS},
                       probe, duration_seconds, sample_rate_hz, bitrate_bps, loudness_dbfs)
                     VALUES ('A', '9000000254', 'k/ok.wav', 'audio/wav', 1000,
                       'ok', 12.5, 44100, 128000, -18.4)""")


def test_audio_ok_requires_all_four_metrics(conn):
    """The assignment's four mandatory fields, enforced by the database."""
    rejects(conn, f"""INSERT INTO audio_submission ({AUDIO_COLS},
                        probe, duration_seconds, sample_rate_hz)
                      VALUES ('A', '9000000254', 'k/part.wav', 'audio/wav', 1000,
                        'ok', 12.5, 44100)""")


def test_audio_rejects_positive_loudness(conn):
    """dBFS is measured against full scale, so it is always <= 0."""
    rejects(conn, f"""INSERT INTO audio_submission ({AUDIO_COLS}, loudness_dbfs)
                      VALUES ('A', '9000000254', 'k/loud.wav', 'audio/wav', 1000, 3.0)""")


def test_audio_rejects_implausible_sample_rate(conn):
    rejects(conn, f"""INSERT INTO audio_submission ({AUDIO_COLS}, sample_rate_hz)
                      VALUES ('A', '9000000254', 'k/sr.wav', 'audio/wav', 1000, 300)""")


def test_audio_failed_probe_must_say_why(conn):
    rejects(conn, f"""INSERT INTO audio_submission ({AUDIO_COLS}, probe)
                      VALUES ('A', '9000000254', 'k/f.wav', 'audio/wav', 1000, 'failed')""")


def test_audio_storage_key_is_unique(conn):
    conn.execute(f"""INSERT INTO audio_submission ({AUDIO_COLS})
                     VALUES ('A', '9000000254', 'k/dup.wav', 'audio/wav', 1000)""")
    rejects(conn, f"""INSERT INTO audio_submission ({AUDIO_COLS})
                      VALUES ('B', '9000000255', 'k/dup.wav', 'audio/wav', 2000)""")


def test_audio_survives_person_deletion(conn, person_id):
    """Losing an upload because a person record changed would be unacceptable."""
    conn.execute(f"""INSERT INTO audio_submission ({AUDIO_COLS}, person_id)
                     VALUES ('A', '9000000254', 'k/orphan.wav', 'audio/wav', 1000, %s)""",
                 (person_id,))
    conn.execute("DELETE FROM person WHERE id = %s", (person_id,))
    row = conn.execute(
        "SELECT person_id FROM audio_submission WHERE storage_key = 'k/orphan.wav'"
    ).fetchone()
    assert row is not None and row[0] is None


def test_audio_view_reports_khz_and_kbps(conn):
    """The assignment asks for sample rate in kHz specifically."""
    conn.execute(f"""INSERT INTO audio_submission ({AUDIO_COLS},
                       probe, duration_seconds, sample_rate_hz, bitrate_bps, loudness_dbfs)
                     VALUES ('A', '9000000254', 'k/v.wav', 'audio/wav', 1000,
                       'ok', 3.0, 44100, 128000, -20.0)""")
    khz, kbps = conn.execute(
        "SELECT sample_rate_khz, bitrate_kbps FROM v_audio_submission WHERE storage_key = 'k/v.wav'"
    ).fetchone()
    assert float(khz) == 44.1
    assert int(kbps) == 128


# --------------------------------------------------------------------------
# cascade behaviour
# --------------------------------------------------------------------------

def test_deleting_a_run_cascades_to_its_raw_rows(conn, run_id, raw_id):
    conn.execute("DELETE FROM ingestion_run WHERE id = %s", (run_id,))
    n = conn.execute("SELECT count(*) FROM raw_record WHERE id = %s", (raw_id,)).fetchone()[0]
    assert n == 0
