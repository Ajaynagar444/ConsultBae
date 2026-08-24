"""Raw ingestion layer.

Two groups: pure parsing tests that touch the real CSVs but no database, and
integration tests that load into consultbae_test.

The load-bearing claim is that the raw layer is lossless. Several tests below
assert that the three deliberately broken rows - the blank row, the embedded
header, the column-shifted row - survive ingestion untouched, because the
temptation to "helpfully" drop them is exactly what would destroy the audit
trail.
"""

from __future__ import annotations

import csv
import dataclasses

import psycopg
import pytest

from src.pipeline import config
from src.pipeline.config import SOURCES, SOURCES_BY_KEY, SourceSpec
from src.pipeline.ingest import (
    KEY_EMPTY,
    KEY_EXTRA,
    KEY_MISSING,
    RawRow,
    create_run,
    ingest_source,
    read_source,
    row_sha256,
    run_ingestion,
)

NAUKRI = SOURCES_BY_KEY["naukri"]
GIG = SOURCES_BY_KEY["gig_workers"]
CBNEXUS = SOURCES_BY_KEY["cbnexus"]


# ===========================================================================
# parsing - no database
# ===========================================================================

def test_all_three_sources_are_configured():
    assert [s.key for s in SOURCES] == ["naukri", "gig_workers", "cbnexus"]
    for spec in SOURCES:
        assert spec.path.exists(), f"missing {spec.path}"


@pytest.mark.parametrize(
    "spec,expected_rows,expected_cols",
    [(NAUKRI, 42, 8), (GIG, 32, 6), (CBNEXUS, 31, 5)],
)
def test_row_and_column_counts_match_the_profile(spec, expected_rows, expected_cols):
    header, rows = read_source(spec)
    assert len(header) == expected_cols
    assert len(rows) == expected_rows


def test_total_is_105_physical_data_rows():
    total = sum(len(read_source(s)[1]) for s in SOURCES)
    assert total == 105 == config.EXPECTED_TOTAL_ROWS


def test_line_numbers_start_at_two_and_are_contiguous():
    """Line 1 is the header in every file, so data starts at 2."""
    for spec in SOURCES:
        _, rows = read_source(spec)
        numbers = [r.line_no for r in rows]
        assert numbers[0] == 2
        assert numbers == list(range(2, 2 + len(rows))), spec.key


def test_header_is_not_ingested_as_a_data_row():
    header, rows = read_source(NAUKRI)
    assert header[0] == "Full Name"
    assert all(r.payload.get("Full Name") != "Full Name" for r in rows)


def test_source_identification_maps_key_to_db_enum():
    assert NAUKRI.db_enum == "naukri_applicants"
    assert GIG.db_enum == "gig_workers"
    assert CBNEXUS.db_enum == "cbnexus_contacts"
    assert len({s.db_enum for s in SOURCES}) == 3


# ---- the three broken rows must survive ----------------------------------

def test_embedded_header_row_is_preserved_as_data():
    """source3 line 16 repeats the header. It is a data row and stays one."""
    _, rows = read_source(CBNEXUS)
    row = next(r for r in rows if r.line_no == 16)
    assert row.payload["Name"] == "Name"
    assert row.payload["Phone Number"] == "Phone Number"
    assert row.payload["Projects Completed"] == "Projects Completed"


def test_blank_row_is_preserved():
    """source2 line 12 is all-empty. Preserved, not skipped."""
    _, rows = read_source(GIG)
    row = next(r for r in rows if r.line_no == 12)
    assert set(row.payload.values()) == {""}
    assert len(row.payload) == 6


def test_column_shifted_row_is_preserved_verbatim():
    """source2 line 20 has its fields rotated one position right.

    The raw layer stores the corruption exactly as it is. Detecting and
    repairing it is the staging layer's job.
    """
    _, rows = read_source(GIG)
    row = next(r for r in rows if r.line_no == 20)
    assert row.payload["email_id"] == "react, javascript, mysql"
    assert row.payload["worker_name"] == "ISHA.CHOPRA95@MAILTEST.EXAMPLE.ORG"
    assert row.payload["rate"] == "Isha Chopra"
    assert row.payload["status"] == "Pune"


def test_no_row_is_dropped_for_being_suspicious():
    """Every physical line below the header is present, broken ones included."""
    for spec, expected in [(NAUKRI, 42), (GIG, 32), (CBNEXUS, 31)]:
        text = spec.path.read_text(encoding=config.SOURCE_ENCODING)
        physical = len(text.splitlines()) - 1  # minus the header
        _, rows = read_source(spec)
        assert len(rows) == physical == expected, spec.key


# ---- payload fidelity ------------------------------------------------------

@pytest.mark.parametrize("spec", SOURCES, ids=lambda s: s.key)
def test_payload_matches_a_plain_dictreader(spec):
    """The payload is exactly what a stock CSV reader sees - no cleaning."""
    text = spec.path.read_text(encoding=config.SOURCE_ENCODING)
    expected = list(csv.DictReader(text.splitlines()))
    _, rows = read_source(spec)

    assert len(rows) == len(expected)
    for row, want in zip(rows, expected):
        assert row.payload == dict(want), f"{spec.key} line {row.line_no}"


def test_payload_keeps_original_column_names_including_spaces():
    _, rows = read_source(NAUKRI)
    assert "Experience (Years)" in rows[0].payload
    assert "Full Name" in rows[0].payload


def test_payload_preserves_whitespace_and_casing():
    """City values carry trailing spaces and mixed case. Untouched here."""
    _, rows = read_source(NAUKRI)
    cities = [r.payload["City"] for r in rows]
    assert "gurugram " in cities        # trailing space intact
    assert "GURGAON" in cities          # casing intact
    assert "pune" in cities


# ---- hashing ---------------------------------------------------------------

def test_hash_is_deterministic_across_reads():
    first = {r.line_no: r.sha256 for r in read_source(NAUKRI)[1]}
    second = {r.line_no: r.sha256 for r in read_source(NAUKRI)[1]}
    assert first == second


def test_hash_is_64_hex_characters():
    for spec in SOURCES:
        for row in read_source(spec)[1]:
            assert len(row.sha256) == 64
            assert all(c in "0123456789abcdef" for c in row.sha256)


def test_hash_differs_for_different_content():
    assert row_sha256("a,b,c") != row_sha256("a,b,d")


def test_hash_ignores_the_trailing_line_terminator():
    """A file gaining a final newline must not change every hash."""
    assert row_sha256("a,b,c") == row_sha256("a,b,c\n") == row_sha256("a,b,c\r\n")


def test_identical_rows_hash_identically():
    """source1 lines 27 and 37 differ only by an 'alt.' email prefix."""
    _, rows = read_source(NAUKRI)
    by_line = {r.line_no: r for r in rows}
    assert by_line[27].sha256 != by_line[37].sha256
    assert row_sha256(by_line[27].raw_text) == by_line[27].sha256


def test_hash_is_reproducible_from_the_stored_raw_text():
    for spec in SOURCES:
        for row in read_source(spec)[1]:
            assert row_sha256(row.raw_text) == row.sha256


# ---- synthetic edge cases --------------------------------------------------

@pytest.fixture
def synthetic(tmp_path, monkeypatch):
    """Build a throwaway CSV and a SourceSpec pointing at it."""
    def _make(name: str, content: str) -> SourceSpec:
        monkeypatch.setattr(config, "RAW_DATA_DIR", tmp_path)
        (tmp_path / name).write_text(content, encoding="utf-8", newline="")
        return SourceSpec(key="synthetic", filename=name,
                          db_enum="gig_workers", description="test")
    return _make


def test_quoted_newline_keeps_line_numbers_accurate(synthetic):
    """A record spanning two physical lines must not desync the numbering."""
    spec = synthetic("multi.csv", 'a,b\r\n1,"line one\r\nline two"\r\n3,4\r\n')
    _, rows = read_source(spec)
    assert len(rows) == 2
    assert rows[0].line_no == 2
    assert rows[0].payload["b"] == "line one\r\nline two"
    assert rows[1].line_no == 4          # not 3 - the quoted field used two lines
    assert rows[1].payload == {"a": "3", "b": "4"}


def test_extra_fields_are_preserved_not_truncated(synthetic):
    spec = synthetic("extra.csv", "a,b\r\n1,2,3,4\r\n")
    _, rows = read_source(spec)
    assert rows[0].payload["a"] == "1"
    assert rows[0].payload[KEY_EXTRA] == ["3", "4"]


def test_missing_fields_are_recorded(synthetic):
    spec = synthetic("short.csv", "a,b,c\r\n1\r\n")
    _, rows = read_source(spec)
    assert rows[0].payload["a"] == "1"
    assert rows[0].payload[KEY_MISSING] == ["b", "c"]


def test_truly_empty_line_is_recorded_not_skipped(synthetic):
    """Physical lines: 1 header, 2 '1,2', 3 empty, 4 '3,4'."""
    spec = synthetic("empty.csv", "a,b\r\n1,2\r\n\r\n3,4\r\n")
    _, rows = read_source(spec)
    assert [r.line_no for r in rows] == [2, 3, 4]
    assert rows[1].payload == {KEY_EMPTY: True}


def test_duplicate_header_columns_do_not_collide(synthetic):
    spec = synthetic("dupe.csv", "a,a\r\n1,2\r\n")
    header, rows = read_source(spec)
    assert header == ["a", "a__1"]
    assert rows[0].payload == {"a": "1", "a__1": "2"}


def test_missing_file_raises_clearly(synthetic):
    spec = synthetic("present.csv", "a\r\n1\r\n")
    gone = dataclasses.replace(spec, filename="not_here.csv")
    with pytest.raises(FileNotFoundError, match="source file missing"):
        read_source(gone)


def test_header_only_file_yields_no_rows(synthetic):
    spec = synthetic("headeronly.csv", "a,b\r\n")
    header, rows = read_source(spec)
    assert header == ["a", "b"]
    assert rows == []


# ===========================================================================
# integration - against consultbae_test
# ===========================================================================

def test_ingest_all_three_sources(conn):
    result = run_ingestion(conn)

    assert result.ok, result.errors
    assert result.read == 105
    assert result.inserted == 105
    assert result.skipped == 0

    stored = conn.execute(
        "SELECT count(*) FROM raw_record WHERE run_id = %s", (result.run_id,)
    ).fetchone()[0]
    assert stored == 105


def test_rows_land_under_the_right_source_system(conn):
    result = run_ingestion(conn)
    counts = dict(conn.execute(
        """SELECT source_system::text, count(*) FROM raw_record
            WHERE run_id = %s GROUP BY 1""", (result.run_id,)
    ).fetchall())
    assert counts == {
        "naukri_applicants": 42,
        "gig_workers": 32,
        "cbnexus_contacts": 31,
    }


def test_stored_line_numbers_match_the_files(conn):
    result = run_ingestion(conn)
    for spec in SOURCES:
        stored = [r[0] for r in conn.execute(
            """SELECT source_line_no FROM raw_record
                WHERE run_id = %s AND source_system = %s::source_system
                ORDER BY source_line_no""", (result.run_id, spec.db_enum)
        ).fetchall()]
        _, rows = read_source(spec)
        assert stored == [r.line_no for r in rows], spec.key


def test_payload_survives_the_jsonb_round_trip(conn):
    """What comes back out of the database is what went in."""
    result = run_ingestion(conn)
    for spec in SOURCES:
        _, rows = read_source(spec)
        stored = dict(conn.execute(
            """SELECT source_line_no, payload FROM raw_record
                WHERE run_id = %s AND source_system = %s::source_system""",
            (result.run_id, spec.db_enum),
        ).fetchall())
        for row in rows:
            assert stored[row.line_no] == row.payload, f"{spec.key} line {row.line_no}"


def test_broken_rows_are_in_the_database(conn):
    """The blank row, the embedded header and the shifted row all made it."""
    result = run_ingestion(conn)

    blank = conn.execute(
        """SELECT payload FROM raw_record
            WHERE run_id = %s AND source_system = 'gig_workers' AND source_line_no = 12""",
        (result.run_id,),
    ).fetchone()[0]
    assert set(blank.values()) == {""}

    embedded = conn.execute(
        """SELECT payload FROM raw_record
            WHERE run_id = %s AND source_system = 'cbnexus_contacts' AND source_line_no = 16""",
        (result.run_id,),
    ).fetchone()[0]
    assert embedded["Name"] == "Name"

    shifted = conn.execute(
        """SELECT payload FROM raw_record
            WHERE run_id = %s AND source_system = 'gig_workers' AND source_line_no = 20""",
        (result.run_id,),
    ).fetchone()[0]
    assert shifted["rate"] == "Isha Chopra"


def test_every_stored_row_has_a_hash(conn):
    result = run_ingestion(conn)
    bad = conn.execute(
        """SELECT count(*) FROM raw_record
            WHERE run_id = %s AND (row_sha256 IS NULL OR row_sha256 !~ '^[0-9a-f]{64}$')""",
        (result.run_id,),
    ).fetchone()[0]
    assert bad == 0


def test_stored_hashes_match_recomputed_ones(conn):
    result = run_ingestion(conn)
    for spec in SOURCES:
        stored = dict(conn.execute(
            """SELECT source_line_no, row_sha256 FROM raw_record
                WHERE run_id = %s AND source_system = %s::source_system""",
            (result.run_id, spec.db_enum),
        ).fetchall())
        for row in read_source(spec)[1]:
            assert stored[row.line_no] == row_sha256(row.raw_text)


# ---- idempotency -----------------------------------------------------------

def test_reingesting_the_same_run_inserts_nothing(conn):
    first = run_ingestion(conn)
    second = run_ingestion(conn, run_id=first.run_id)

    assert second.read == 105
    assert second.inserted == 0
    assert second.skipped == 105
    assert second.ok

    total = conn.execute(
        "SELECT count(*) FROM raw_record WHERE run_id = %s", (first.run_id,)
    ).fetchone()[0]
    assert total == 105


def test_a_new_run_is_distinguishable(conn):
    first = run_ingestion(conn)
    second = run_ingestion(conn)

    assert first.run_id != second.run_id
    assert second.inserted == 105

    per_run = dict(conn.execute(
        "SELECT run_id, count(*) FROM raw_record GROUP BY 1 ORDER BY 1"
    ).fetchall())
    assert per_run[first.run_id] == 105
    assert per_run[second.run_id] == 105


def test_reingesting_one_source_leaves_the_others_alone(conn):
    first = run_ingestion(conn)
    again = run_ingestion(conn, specs=[GIG], run_id=first.run_id)
    assert again.inserted == 0
    assert again.skipped == 32


# ---- run lifecycle ---------------------------------------------------------

def test_run_is_marked_succeeded_and_timestamped(conn):
    result = run_ingestion(conn)
    status, started, finished, note = conn.execute(
        "SELECT status, started_at, finished_at, note FROM ingestion_run WHERE id = %s",
        (result.run_id,),
    ).fetchone()
    assert status == "succeeded"
    assert finished is not None and finished >= started
    assert "inserted=105" in note


def test_unknown_run_id_is_rejected(conn):
    with pytest.raises(ValueError, match="does not exist"):
        run_ingestion(conn, run_id=999_999)


def test_dry_run_writes_nothing(conn):
    before = conn.execute("SELECT count(*) FROM raw_record").fetchone()[0]
    result = run_ingestion(conn, dry_run=True)
    after = conn.execute("SELECT count(*) FROM raw_record").fetchone()[0]

    assert result.read == 105
    assert result.inserted == 0
    assert result.run_id is None
    assert before == after


# ---- failure handling ------------------------------------------------------

def test_a_failing_row_is_reported_not_swallowed(conn):
    """Every row fails here; the run must say so rather than claim success."""
    run_id = create_run(conn, note="failure path")
    broken = dataclasses.replace(GIG, db_enum="not_a_real_source")

    result = ingest_source(conn, run_id, broken)

    assert result.read == 32
    assert result.inserted == 0
    assert len(result.errors) == 32
    assert not result.ok
    assert "gig_workers line 2" in result.errors[0]


def test_one_bad_row_does_not_block_the_rest(conn):
    """A savepoint per row: a mid-file failure must not lose the good rows."""
    run_id = create_run(conn, note="partial")

    # Occupy line 5 with a row this run already has, using a different payload,
    # so the real insert conflicts and is skipped rather than erroring.
    conn.execute(
        """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
           VALUES (%s, 'cbnexus_contacts', 5, '{"pre": "existing"}'::jsonb, repeat('a', 64))""",
        (run_id,),
    )

    result = ingest_source(conn, run_id, CBNEXUS)

    assert result.read == 31
    assert result.inserted == 30
    assert result.skipped == 1
    assert result.ok

    kept = conn.execute(
        """SELECT payload FROM raw_record
            WHERE run_id = %s AND source_system = 'cbnexus_contacts' AND source_line_no = 5""",
        (run_id,),
    ).fetchone()[0]
    assert kept == {"pre": "existing"}   # first write wins, nothing overwritten


def test_run_is_marked_failed_when_rows_error(conn):
    broken = dataclasses.replace(GIG, db_enum="not_a_real_source")
    result = run_ingestion(conn, specs=[broken])

    assert not result.ok
    status = conn.execute(
        "SELECT status FROM ingestion_run WHERE id = %s", (result.run_id,)
    ).fetchone()[0]
    assert status == "failed"


def test_missing_source_file_aborts_before_writing(conn):
    gone = dataclasses.replace(CBNEXUS, filename="does_not_exist.csv")
    before = conn.execute("SELECT count(*) FROM raw_record").fetchone()[0]
    with pytest.raises(FileNotFoundError):
        run_ingestion(conn, specs=[gone])
    after = conn.execute("SELECT count(*) FROM raw_record").fetchone()[0]
    assert before == after
