"""Verify the database foundation is in place and correct.

Run after scripts/migrate.py. Checks connectivity, server version, that every
expected object exists, and that the constraints which carry the design's
guarantees actually reject bad data - a constraint that exists but does not
fire is worse than no constraint, because it looks like protection.

    python scripts/db_check.py

Exit code 0 if everything passes, 1 otherwise, so it works in CI.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

EXPECTED_TABLES = [
    "audio_submission",
    "data_issue",
    "ingestion_run",
    "match_review",
    "person",
    "person_email",
    "person_phone",
    "person_skill",
    "person_source_link",
    "raw_record",
    "schema_migration",
    "skill",
    "staged_person",
]

EXPECTED_VIEWS = ["v_audio_submission", "v_data_issue_report", "v_person_full"]

EXPECTED_TYPES = [
    "ctc_unit",
    "gig_status",
    "issue_severity",
    "match_method",
    "probe_status",
    "quality_label",
    "quarantine_reason",
    "rate_unit",
    "review_status",
    "source_system",
]

# (label, SQL that MUST raise). Each asserts a guarantee the design depends on.
NEGATIVE_TESTS = [
    (
        "raw_record rejects a non-object payload",
        """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
           VALUES (%(run)s, 'gig_workers', 2, '[]'::jsonb, repeat('a', 64))""",
    ),
    (
        "raw_record rejects line_no < 2 (line 1 is the header)",
        """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
           VALUES (%(run)s, 'gig_workers', 1, '{}'::jsonb, repeat('a', 64))""",
    ),
    (
        "person_email rejects a non-lowercase address",
        """INSERT INTO person_email (person_id, email) VALUES (%(person)s, 'MIXED@Example.COM')""",
    ),
    (
        "person_phone rejects a malformed number",
        """INSERT INTO person_phone (person_id, phone_e164) VALUES (%(person)s, '9000000254')""",
    ),
    (
        "person requires at least one contact route",
        """INSERT INTO person (full_name, name_key) VALUES ('No Contact', 'no contact')""",
    ),
    (
        "audio_submission rejects positive dBFS loudness",
        """INSERT INTO audio_submission
             (submitted_name, submitted_phone_raw, storage_key, mime_type, size_bytes, loudness_dbfs)
           VALUES ('T', '9000000254', 'k1', 'audio/wav', 1, 3.0)""",
    ),
    (
        "audio_submission probe='ok' requires all four metrics",
        """INSERT INTO audio_submission
             (submitted_name, submitted_phone_raw, storage_key, mime_type, size_bytes,
              probe, duration_seconds, sample_rate_hz)
           VALUES ('T', '9000000254', 'k2', 'audio/wav', 1, 'ok', 1.0, 44100)""",
    ),
    (
        "match_review rejects an unordered pair",
        """INSERT INTO match_review (run_id, raw_record_id_a, raw_record_id_b, reason, confidence)
           VALUES (%(run)s, %(raw)s, %(raw)s, 'self', 0.5)""",
    ),
]

PASS, FAIL, INFO = "  OK  ", "  X   ", "  --  "
failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"{PASS if ok else FAIL}{label}{(' - ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set. Copy .env.example to .env and fill it in.")

    print(f"connecting to {url.split('@')[-1]} ...\n")

    try:
        conn = psycopg.connect(url, autocommit=False)
    except psycopg.OperationalError as exc:
        print(f"{FAIL}cannot connect\n\n{exc}")
        return 1

    with conn:
        # ---- connectivity and version -------------------------------------
        version = conn.execute("SELECT version()").fetchone()[0]
        major = int(conn.execute("SHOW server_version_num").fetchone()[0]) // 10000
        print(f"{INFO}{version.split(' on ')[0]}")
        check("server is PostgreSQL 16 or newer", major >= 16, f"major={major}")

        db, user = conn.execute("SELECT current_database(), current_user").fetchone()
        check("connected to the project database", db == "consultbae", f"database={db} user={user}")

        # ---- migrations ----------------------------------------------------
        applied = conn.execute(
            "SELECT count(*) FROM schema_migration"
        ).fetchone()[0]
        on_disk = len(sorted((PROJECT_ROOT / "db" / "migrations").glob("[0-9][0-9][0-9]_*.sql")))
        check("all migrations applied", applied == on_disk, f"{applied}/{on_disk}")

        # ---- objects -------------------------------------------------------
        tables = [r[0] for r in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1"
        ).fetchall()]
        missing = sorted(set(EXPECTED_TABLES) - set(tables))
        check(f"all {len(EXPECTED_TABLES)} tables present", not missing, f"missing {missing}" if missing else "")

        views = [r[0] for r in conn.execute(
            "SELECT viewname FROM pg_views WHERE schemaname='public' ORDER BY 1"
        ).fetchall()]
        missing_v = sorted(set(EXPECTED_VIEWS) - set(views))
        check(f"all {len(EXPECTED_VIEWS)} views present", not missing_v, f"missing {missing_v}" if missing_v else "")

        types = [r[0] for r in conn.execute(
            """SELECT t.typname FROM pg_type t
               JOIN pg_namespace n ON n.oid = t.typnamespace
               WHERE n.nspname='public' AND t.typtype='e' ORDER BY 1"""
        ).fetchall()]
        missing_t = sorted(set(EXPECTED_TYPES) - set(types))
        check(f"all {len(EXPECTED_TYPES)} enum types present", not missing_t, f"missing {missing_t}" if missing_t else "")

        n_idx = conn.execute(
            "SELECT count(*) FROM pg_indexes WHERE schemaname='public'"
        ).fetchone()[0]
        check("indexes created", n_idx >= 25, f"{n_idx} indexes")

        n_ck = conn.execute(
            """SELECT count(*) FROM pg_constraint c
               JOIN pg_namespace n ON n.oid = c.connamespace
               WHERE n.nspname='public' AND c.contype='c'"""
        ).fetchone()[0]
        check("check constraints created", n_ck >= 30, f"{n_ck} CHECK constraints")

        # ---- raw layer integrity -------------------------------------------
        # Not "is it empty" - it is legitimately full once ingestion has run.
        # What matters is that whatever is in there is internally consistent.
        rows, runs = conn.execute(
            "SELECT count(*), count(DISTINCT run_id) FROM raw_record"
        ).fetchone()

        if rows == 0:
            print(f"{INFO}raw_record is empty - run: python -m src.pipeline.ingest")
        else:
            print(f"{INFO}raw_record holds {rows} rows across {runs} ingestion run(s)")

            bad_hash = conn.execute(
                """SELECT count(*) FROM raw_record
                    WHERE row_sha256 IS NULL OR row_sha256 !~ '^[0-9a-f]{64}$'"""
            ).fetchone()[0]
            check("every raw row has a well-formed hash", bad_hash == 0,
                  f"{bad_hash} bad" if bad_hash else "")

            bad_payload = conn.execute(
                "SELECT count(*) FROM raw_record WHERE jsonb_typeof(payload) <> 'object'"
            ).fetchone()[0]
            check("every payload is a JSON object", bad_payload == 0,
                  f"{bad_payload} bad" if bad_payload else "")

            # Each completed run should hold the full 105 rows: 42 + 32 + 31.
            # A short run means ingestion stopped early and went unnoticed.
            short = conn.execute(
                """SELECT count(*) FROM (
                       SELECT r.run_id FROM raw_record r
                       JOIN ingestion_run ir ON ir.id = r.run_id
                       WHERE ir.status = 'succeeded'
                       GROUP BY r.run_id HAVING count(*) <> 105
                   ) t"""
            ).fetchone()[0]
            check("every succeeded run holds all 105 source rows", short == 0,
                  f"{short} run(s) incomplete" if short else "")

            # Line numbers must be gapless per source: 2..43, 2..33, 2..32.
            gaps = conn.execute(
                """SELECT count(*) FROM (
                       SELECT run_id, source_system FROM raw_record
                       GROUP BY run_id, source_system
                       HAVING max(source_line_no) - min(source_line_no) + 1 <> count(*)
                   ) t"""
            ).fetchone()[0]
            check("line numbers are gapless in every source", gaps == 0,
                  f"{gaps} with gaps" if gaps else "")

            orphans = conn.execute(
                """SELECT count(*) FROM ingestion_run
                    WHERE status = 'running' AND started_at < now() - interval '1 hour'"""
            ).fetchone()[0]
            check("no ingestion run left hanging", orphans == 0,
                  f"{orphans} stuck in 'running'" if orphans else "")

        # ---- pg_trgm should NOT be installed -------------------------------
        trgm = conn.execute(
            "SELECT count(*) FROM pg_extension WHERE extname='pg_trgm'"
        ).fetchone()[0]
        check("pg_trgm not installed (matching is deterministic)", trgm == 0)

        conn.rollback()

        # ---- negative tests: constraints must actually fire ----------------
        print("\nconstraint enforcement (each statement must be rejected):")

        # Scratch parents, rolled back afterwards so nothing is left behind.
        with conn.transaction() as outer:
            run_id = conn.execute(
                "INSERT INTO ingestion_run (note) VALUES ('db_check scratch') RETURNING id"
            ).fetchone()[0]
            raw_id = conn.execute(
                """INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
                   VALUES (%s, 'gig_workers', 2, '{}'::jsonb, repeat('a', 64)) RETURNING id""",
                (run_id,),
            ).fetchone()[0]
            person_id = conn.execute(
                """INSERT INTO person (full_name, name_key, primary_email)
                   VALUES ('Scratch Person', 'scratch person', 'scratch@example.com') RETURNING id"""
            ).fetchone()[0]

            params = {"run": run_id, "raw": raw_id, "person": person_id}

            for label, sql in NEGATIVE_TESTS:
                try:
                    with conn.transaction():
                        conn.execute(sql, params)
                    check(label, False, "statement was ACCEPTED but should have been rejected")
                except psycopg.errors.CheckViolation:
                    check(label, True)
                except psycopg.Error as exc:
                    # Any rejection is a pass; note the type if it was not a CHECK.
                    check(label, True, type(exc).__name__)

            outer.force_rollback = True

        left = conn.execute("SELECT count(*) FROM person").fetchone()[0]
        check("scratch data rolled back cleanly", left == 0, f"person has {left} rows")

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s)")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("database foundation verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
