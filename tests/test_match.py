"""Cross-source matching and deduplication.

The five real-dataset cases each get their own test, plus the guard conditions,
survivorship, provenance, idempotency and determinism. Counts (56 people, 60
pre-guard clusters) come from docs/data-profile.md section 2 - they are the
independently derived expectation the algorithm must reproduce, not values the
algorithm was tuned to.
"""

from __future__ import annotations

import psycopg
import pytest

from src.pipeline.ingest import run_ingestion
from src.pipeline.match import (
    CONFIDENCE,
    GuardVerdict,
    Row,
    guarded_name_verdict,
    run_matching,
    survivorship,
)
from src.pipeline.stage import stage_run


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def matched(conn):
    """Full pipeline once: ingest -> stage -> match. Returns (conn, run_id, result)."""
    run_id = run_ingestion(conn).run_id
    stage_run(conn, run_id)
    result = run_matching(conn, run_id)
    return conn, run_id, result


def person_of_email(conn, email: str):
    row = conn.execute(
        """SELECT p.id, p.full_name FROM person p
           JOIN person_email e ON e.person_id = p.id WHERE e.email = %s""",
        (email,),
    ).fetchone()
    return row


def mkrow(raw_id=1, source="gig_workers", line=2, name="A B", key="a b",
          email=None, phone=None, city=None, **kw) -> Row:
    defaults = dict(
        raw_record_id=raw_id, source_system=source, source_line_no=line,
        full_name=name, name_key=key, email=email, phone=phone, city=city,
        is_region=False, experience=None, ctc=None, ctc_unit=None, rate=None,
        rate_unit=None, status=None, verified=None, projects=None,
        applied_on=None, skills=(), was_repaired=False,
    )
    defaults.update(kw)
    return Row(**defaults)


# ===========================================================================
# guard conditions - pure
# ===========================================================================

def _verdict(a, b, n_clusters=2, counts=None) -> GuardVerdict:
    counts = counts or {"gig_workers": 1, "cbnexus_contacts": 1}
    return guarded_name_verdict(a, b, "a b", n_clusters, counts)


def test_guard_allows_the_clean_case():
    a = [mkrow(1, "gig_workers", email="x@y.com")]
    b = [mkrow(2, "cbnexus_contacts", phone="+919000000001")]
    assert _verdict(a, b).allowed


def test_guard_condition_2_name_must_be_unique_per_source():
    a = [mkrow(1, "gig_workers", email="x@y.com")]
    b = [mkrow(2, "cbnexus_contacts", phone="+919000000001")]
    v = _verdict(a, b, counts={"gig_workers": 1, "cbnexus_contacts": 2})
    assert not v.allowed and "condition 2" in v.reason


def test_guard_condition_3_same_source_on_both_sides():
    a = [mkrow(1, "gig_workers", email="x@y.com")]
    b = [mkrow(2, "gig_workers", email="z@y.com")]
    v = guarded_name_verdict(a, b, "a b", 2, {"gig_workers": 2})
    assert not v.allowed and "condition" in v.reason


def test_guard_condition_3_conflicting_emails():
    a = [mkrow(1, "gig_workers", email="x@y.com")]
    b = [mkrow(2, "cbnexus_contacts", email="z@y.com")]
    v = _verdict(a, b)
    assert not v.allowed and "emails" in v.reason


def test_guard_condition_3_conflicting_phones():
    a = [mkrow(1, "naukri_applicants", phone="+919000000001", email="x@y.com")]
    b = [mkrow(2, "cbnexus_contacts", phone="+919000000002")]
    v = _verdict(a, b, counts={"naukri_applicants": 1, "cbnexus_contacts": 1})
    assert not v.allowed and "phones" in v.reason


def test_guard_condition_4_three_clusters_is_ambiguous():
    a = [mkrow(1, "gig_workers", email="x@y.com")]
    b = [mkrow(2, "cbnexus_contacts", phone="+919000000001")]
    v = _verdict(a, b, n_clusters=3)
    assert not v.allowed and "condition 4" in v.reason


def test_guard_condition_5_name_in_bridge_source_blocks():
    a = [mkrow(1, "gig_workers", email="x@y.com")]
    b = [mkrow(2, "cbnexus_contacts", phone="+919000000001")]
    v = _verdict(a, b, counts={"gig_workers": 1, "cbnexus_contacts": 1,
                               "naukri_applicants": 1})
    assert not v.allowed and "condition 5" in v.reason


# ===========================================================================
# the five real-dataset cases
# ===========================================================================

def test_case_1_r_verma_and_rohit_verma_are_one_person(matched):
    """Merged on identifiers; the abbreviated name never enters into it."""
    conn, run_id, _ = matched
    rows = conn.execute(
        """SELECT p.id, p.full_name, l.method::text, l.confidence
             FROM person_source_link l
             JOIN person p ON p.id = l.person_id
             JOIN raw_record r ON r.id = l.raw_record_id
            WHERE r.run_id = %s AND r.source_system = 'naukri_applicants'
              AND r.source_line_no IN (25, 31)""",
        (run_id,),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == rows[1][0]                       # one person
    assert {r[2] for r in rows} == {"exact_email"}        # identifier evidence
    assert all(float(r[3]) == 1.0 for r in rows)
    assert rows[0][1] == "Rohit Verma"                    # longest name won


def test_case_1_merge_evidence_is_recorded_as_email_and_phone(matched):
    conn, run_id, _ = matched
    detail = conn.execute(
        """SELECT detail FROM data_issue
            WHERE run_id = %s AND issue_code = 'intra_source_duplicate'
              AND detail LIKE '%%Rohit Verma%%'""",
        (run_id,),
    ).fetchone()[0]
    assert "exact normalized email and phone" in detail


def test_case_2_isha_chopra_repaired_row_is_the_same_person(matched):
    """source2 lines 7 and 20 both link to one person; both raw rows survive."""
    conn, run_id, _ = matched
    rows = conn.execute(
        """SELECT l.person_id, r.source_line_no, s.was_repaired
             FROM person_source_link l
             JOIN raw_record r ON r.id = l.raw_record_id
             JOIN staged_person s ON s.raw_record_id = r.id
            WHERE r.run_id = %s AND r.source_system = 'gig_workers'
              AND r.source_line_no IN (7, 20)
            ORDER BY r.source_line_no""",
        (run_id,),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == rows[1][0]
    assert rows[0][2] is False and rows[1][2] is True     # repair flag visible


def test_case_2_isha_cluster_spans_all_three_sources(matched):
    conn, run_id, _ = matched
    person_id = person_of_email(conn, "isha.chopra95@mailtest.example.org")[0]
    n_rows, n_sources = conn.execute(
        """SELECT count(*), count(DISTINCT r.source_system)
             FROM person_source_link l JOIN raw_record r ON r.id = l.raw_record_id
            WHERE l.person_id = %s""",
        (person_id,),
    ).fetchone()
    assert n_rows == 4          # s1 line 9, s2 lines 7 + 20, s3 line 18
    assert n_sources == 3


def test_case_3_nikhil_chopra_one_person_two_emails(matched):
    conn, run_id, _ = matched
    person_id, name = person_of_email(conn, "nikhil.chopra70@example.com")
    emails = conn.execute(
        """SELECT email, is_primary FROM person_email
            WHERE person_id = %s ORDER BY is_primary DESC, email""",
        (person_id,),
    ).fetchall()
    assert len(emails) == 2
    assert emails[0] == ("nikhil.chopra70@example.com", True)          # non-alt primary
    assert emails[1] == ("alt.nikhil.chopra70@example.com", False)     # alt preserved


def test_case_3_nikhil_rows_merged_by_phone_not_email(matched):
    """The two rows share a phone; their emails differ."""
    conn, run_id, _ = matched
    methods = [r[0] for r in conn.execute(
        """SELECT l.method::text FROM person_source_link l
             JOIN raw_record r ON r.id = l.raw_record_id
            WHERE r.run_id = %s AND r.source_system = 'naukri_applicants'
              AND r.source_line_no IN (27, 37)""",
        (run_id,),
    ).fetchall()]
    assert methods == ["exact_phone", "exact_phone"]


def test_case_4_deepak_nair_stays_two_people(matched):
    """The anti-false-positive case: same name, different people."""
    conn, run_id, _ = matched
    a = person_of_email(conn, "deepak.nair44@example.com")
    b = person_of_email(conn, "deepak.nair57@example.in")
    assert a is not None and b is not None
    assert a[0] != b[0]


def test_case_4_deepak_nair_is_not_in_the_review_queue(matched):
    """Two gig_workers rows -> positively distinct, not merely unproven."""
    conn, run_id, _ = matched
    n = conn.execute(
        "SELECT count(*) FROM match_review WHERE reason LIKE '%%deepak nair%%'"
    ).fetchone()[0]
    assert n == 0


def test_case_4_deepak_nair_cities_stay_apart(matched):
    conn, run_id, _ = matched
    cities = dict(conn.execute(
        """SELECT pe.email, p.city FROM person p
             JOIN person_email pe ON pe.person_id = p.id
            WHERE pe.email IN ('deepak.nair44@example.com', 'deepak.nair57@example.in')"""
    ).fetchall())
    assert cities["deepak.nair44@example.com"] == "Bengaluru"
    assert cities["deepak.nair57@example.in"] == "New Delhi"


def test_case_5_arjun_mehta_is_three_people(matched):
    conn, run_id, _ = matched
    people = conn.execute(
        "SELECT id, primary_email, primary_phone_e164 FROM person WHERE name_key = 'arjun mehta' ORDER BY id"
    ).fetchall()
    assert len(people) == 3
    identifiers = {(p[1], p[2]) for p in people}
    assert ("arjun.mehta9@example.in", "+919000000131") in identifiers      # A
    assert (None, "+919000000272") in identifiers                            # B
    assert ("arjun.mehta77@mailtest.example.org", None) in identifiers       # C


def test_case_5_a_is_linked_by_phone_across_s1_and_s3(matched):
    conn, run_id, _ = matched
    rows = conn.execute(
        """SELECT r.source_system::text, l.method::text
             FROM person_source_link l
             JOIN person p ON p.id = l.person_id
             JOIN raw_record r ON r.id = l.raw_record_id
            WHERE p.primary_email = 'arjun.mehta9@example.in'
            ORDER BY r.source_system""",
    ).fetchall()
    # Compared as a set: enum columns sort by declaration order, not name.
    assert set(rows) == {("cbnexus_contacts", "exact_phone"),
                         ("naukri_applicants", "exact_phone")}


def test_case_5_c_relationships_are_queued_not_merged(matched):
    """C (source2 line 18) must appear in match_review against both A and B."""
    conn, run_id, _ = matched
    pairs = conn.execute(
        """SELECT ra.source_system::text, ra.source_line_no,
                  rb.source_system::text, rb.source_line_no, m.confidence, m.status::text
             FROM match_review m
             JOIN raw_record ra ON ra.id = m.raw_record_id_a
             JOIN raw_record rb ON rb.id = m.raw_record_id_b
            WHERE m.run_id = %s ORDER BY 1, 2, 3, 4""",
        (run_id,),
    ).fetchall()
    assert len(pairs) == 2
    involved = {(p[0], p[1]) for p in pairs} | {(p[2], p[3]) for p in pairs}
    assert ("gig_workers", 18) in involved            # C in both pairs
    assert ("naukri_applicants", 20) in involved      # vs A
    assert ("cbnexus_contacts", 28) in involved       # vs B
    assert all(float(p[4]) == 0.5 for p in pairs)
    assert all(p[5] == "open" for p in pairs)


# ===========================================================================
# guarded merges and counts
# ===========================================================================

def test_the_four_safe_name_merges_happen(matched):
    conn, run_id, result = matched
    assert len(result.guarded_merges) == 4
    for name in ("divya chopra", "karan chopra", "manish bhatia", "vikram mehta"):
        n = conn.execute(
            "SELECT count(*) FROM person WHERE name_key = %s", (name,)
        ).fetchone()[0]
        assert n == 1, name
        sources = conn.execute(
            """SELECT count(DISTINCT r.source_system) FROM person p
                 JOIN person_source_link l ON l.person_id = p.id
                 JOIN raw_record r ON r.id = l.raw_record_id
                WHERE p.name_key = %s""",
            (name,),
        ).fetchone()[0]
        assert sources == 2, name


def test_guarded_merges_carry_reduced_confidence(matched):
    conn, _, _ = matched
    confidences = {float(r[0]) for r in conn.execute(
        "SELECT DISTINCT confidence FROM person_source_link WHERE method = 'guarded_name'"
    ).fetchall()}
    assert confidences == {CONFIDENCE["guarded_name"]}


def test_counts_emerge_from_the_rules(matched):
    _, _, result = matched
    assert result.staged_total == 105
    assert result.quarantined_excluded == 2
    assert result.rows_matched == 103
    assert result.clusters_before_guarded == 60
    assert result.people == 56


def test_cluster_shapes(matched):
    _, _, result = matched
    assert dict(result.cluster_shapes) == {
        "s1+s2+s3": 15, "s1+s3": 10, "s1": 15, "s2": 11, "s2+s3": 4, "s3": 1,
    }


def test_every_nonquarantined_row_is_linked_exactly_once(matched):
    conn, run_id, _ = matched
    unlinked, doubly = conn.execute(
        """SELECT
             (SELECT count(*) FROM staged_person s
               JOIN raw_record r ON r.id = s.raw_record_id
              WHERE r.run_id = %s AND NOT s.is_quarantined
                AND NOT EXISTS (SELECT 1 FROM person_source_link l
                                 WHERE l.raw_record_id = s.raw_record_id)),
             (SELECT count(*) FROM (
                SELECT raw_record_id FROM person_source_link
                GROUP BY raw_record_id HAVING count(*) > 1) t)""",
        (run_id,),
    ).fetchone()
    assert unlinked == 0
    assert doubly == 0


def test_quarantined_rows_are_not_linked_to_anyone(matched):
    conn, run_id, _ = matched
    n = conn.execute(
        """SELECT count(*) FROM person_source_link l
             JOIN staged_person s ON s.raw_record_id = l.raw_record_id
            WHERE s.is_quarantined"""
    ).fetchone()[0]
    assert n == 0


# ===========================================================================
# survivorship
# ===========================================================================

def test_survivorship_longest_name_wins():
    g = survivorship([
        mkrow(1, "naukri_applicants", name="R. Verma", key="r verma", email="rv@x.com"),
        mkrow(2, "naukri_applicants", name="Rohit Verma", key="rohit verma", email="rv@x.com"),
    ])
    assert g.full_name == "Rohit Verma"


def test_survivorship_non_null_beats_null():
    g = survivorship([
        mkrow(1, "naukri_applicants", email="a@x.com", experience=None),
        mkrow(2, "gig_workers", email="a@x.com", rate=500.0, rate_unit="per_hour"),
    ])
    assert g.rate == 500.0 and g.rate_unit == "per_hour"


def test_survivorship_alt_email_is_never_primary():
    g = survivorship([
        mkrow(1, "naukri_applicants", email="alt.nik@x.com", phone="+919000000103"),
        mkrow(2, "naukri_applicants", email="nik@x.com", phone="+919000000103"),
    ])
    assert g.primary_email == "nik@x.com"
    assert set(g.emails) == {"nik@x.com", "alt.nik@x.com"}


def test_survivorship_city_prefers_specific_over_region():
    g = survivorship([
        mkrow(1, "naukri_applicants", email="m@x.com", city="Delhi NCR", is_region=True),
        mkrow(2, "gig_workers", email="m@x.com", city="New Delhi"),
    ])
    assert g.city == "New Delhi"
    assert g.is_region is False
    assert "Delhi NCR" in g.city_conflict


def test_survivorship_new_delhi_beats_bare_delhi():
    g = survivorship([
        mkrow(1, "naukri_applicants", email="m@x.com", city="Delhi"),
        mkrow(2, "cbnexus_contacts", phone="+919000000001", city="New Delhi"),
    ])
    assert g.city == "New Delhi"


def test_city_conflicts_recorded_in_database(matched):
    conn, run_id, result = matched
    n = conn.execute(
        """SELECT count(*) FROM data_issue
            WHERE run_id = %s AND issue_code = 'city_conflict_across_sources'""",
        (run_id,),
    ).fetchone()[0]
    assert n == result.city_conflicts == 5    # the five profiled disagreements


def test_meera_bhatia_city_resolves_most_specific(matched):
    """Delhi NCR (s1) / New Delhi (s2) / Delhi (s3) -> New Delhi."""
    conn, _, _ = matched
    city, is_region = conn.execute(
        "SELECT city, is_region FROM person WHERE name_key = 'meera bhatia'"
    ).fetchone()
    assert city == "New Delhi"
    assert is_region is False


# ===========================================================================
# provenance, idempotency, determinism, raw safety
# ===========================================================================

def test_every_link_has_method_and_confidence(matched):
    conn, _, _ = matched
    bad = conn.execute(
        """SELECT count(*) FROM person_source_link
            WHERE method IS NULL OR confidence IS NULL
               OR confidence <= 0 OR confidence > 1"""
    ).fetchone()[0]
    assert bad == 0


def test_link_methods_use_the_agreed_vocabulary(matched):
    conn, _, _ = matched
    methods = {r[0] for r in conn.execute(
        "SELECT DISTINCT method::text FROM person_source_link"
    ).fetchall()}
    assert methods <= {"exact_email", "exact_phone", "guarded_name", "unmatched"}


def test_rerun_is_idempotent(matched):
    conn, run_id, first = matched
    snapshot = conn.execute(
        """SELECT p.full_name, p.primary_email, p.primary_phone_e164, p.city,
                  (SELECT count(*) FROM person_source_link l WHERE l.person_id = p.id)
             FROM person p ORDER BY p.full_name, p.primary_email"""
    ).fetchall()

    second = run_matching(conn, run_id)
    again = conn.execute(
        """SELECT p.full_name, p.primary_email, p.primary_phone_e164, p.city,
                  (SELECT count(*) FROM person_source_link l WHERE l.person_id = p.id)
             FROM person p ORDER BY p.full_name, p.primary_email"""
    ).fetchall()

    assert first.people == second.people == 56
    assert snapshot == again

    reviews = conn.execute(
        "SELECT count(*) FROM match_review WHERE run_id = %s", (run_id,)
    ).fetchone()[0]
    assert reviews == 2                       # not 4 after two runs

    dupes = conn.execute(
        """SELECT count(*) FROM data_issue
            WHERE run_id = %s AND issue_code = 'intra_source_duplicate'""",
        (run_id,),
    ).fetchone()[0]
    assert dupes == 3                          # R. Verma, Nikhil, Isha - once each


def test_matching_never_touches_raw(matched):
    conn, run_id, _ = matched
    before = conn.execute(
        "SELECT id, payload, row_sha256 FROM raw_record WHERE run_id = %s ORDER BY id",
        (run_id,),
    ).fetchall()
    run_matching(conn, run_id)
    after = conn.execute(
        "SELECT id, payload, row_sha256 FROM raw_record WHERE run_id = %s ORDER BY id",
        (run_id,),
    ).fetchall()
    assert before == after


def test_deterministic_output_across_runs(matched):
    """Two full matches produce identical golden data, ids aside."""
    conn, run_id, _ = matched

    def snapshot():
        return {
            "people": conn.execute(
                """SELECT full_name, primary_email, primary_phone_e164, city,
                          experience_years, ctc_annual_inr, rate_amount, status::text
                     FROM person ORDER BY full_name, primary_email NULLS LAST"""
            ).fetchall(),
            "emails": conn.execute(
                "SELECT email, is_primary FROM person_email ORDER BY email"
            ).fetchall(),
            "links": conn.execute(
                """SELECT r.source_system::text, r.source_line_no, l.method::text, l.confidence
                     FROM person_source_link l JOIN raw_record r ON r.id = l.raw_record_id
                    ORDER BY 1, 2"""
            ).fetchall(),
        }

    a = snapshot()
    run_matching(conn, run_id)
    b = snapshot()
    assert a == b


def test_v_person_full_agrees_with_the_result(matched):
    conn, _, result = matched
    n = conn.execute("SELECT count(*) FROM v_person_full").fetchone()[0]
    assert n == result.people == 56
    linked = conn.execute(
        "SELECT sum(source_row_count) FROM v_person_full"
    ).fetchone()[0]
    assert linked == 103
