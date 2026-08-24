"""The migration runner itself.

Re-running migrations must be a no-op, and editing an already-applied file must
be caught rather than silently ignored - otherwise the database and the repo
drift apart and nobody notices until something breaks in a demo.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS = PROJECT_ROOT / "db" / "migrations"


def run_migrate(dsn: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "migrate.py"), *args],
        cwd=PROJECT_ROOT,
        env={**os.environ, "DATABASE_URL": dsn},
        capture_output=True,
        text=True,
    )


def test_migration_files_are_numbered_contiguously():
    files = sorted(MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql"))
    assert files, "no migrations found"
    numbers = [int(re.match(r"(\d{3})_", f.name).group(1)) for f in files]
    assert numbers == list(range(1, len(numbers) + 1)), f"gap or duplicate: {numbers}"


def test_expected_migrations_are_present():
    names = {f.name for f in MIGRATIONS.glob("*.sql")}
    assert names == {
        "001_extensions.sql",
        "002_raw.sql",
        "003_staging.sql",
        "004_golden.sql",
        "005_audio.sql",
    }


def test_migrations_avoid_postgres_17_plus_syntax():
    """These must run on PostgreSQL 16, not only on the 18 used in development."""
    banned = {
        "uuidv7": "PG18",
        "NOT ENFORCED": "PG18",
        "JSON_TABLE": "PG17",
        "MERGE ... RETURNING": "PG17",
    }
    for path in MIGRATIONS.glob("*.sql"):
        # Strip comments so the pg_trgm escalation note does not trip anything.
        body = "\n".join(
            line.split("--")[0] for line in path.read_text(encoding="utf-8").splitlines()
        ).lower()
        for token, version in banned.items():
            assert token.lower() not in body, f"{path.name} uses {token} ({version} only)"


def test_rerunning_migrations_is_a_noop(migrated_db):
    result = run_migrate(migrated_db)
    assert result.returncode == 0, result.stderr
    assert "already applied" in result.stdout
    assert "nothing to do" in result.stdout


def test_status_reports_every_migration_as_applied(migrated_db):
    result = run_migrate(migrated_db, "--status")
    assert result.returncode == 0, result.stderr
    assert "PENDING" not in result.stdout
    assert "MODIFIED" not in result.stdout
    assert result.stdout.count("applied") >= 5


def test_edited_migration_is_detected(migrated_db, tmp_path):
    """Tamper with a checksum in the database and confirm the runner refuses."""
    import psycopg

    with psycopg.connect(migrated_db) as c:
        original = c.execute(
            "SELECT checksum FROM schema_migration WHERE filename = '002_raw.sql'"
        ).fetchone()[0]
        c.execute(
            "UPDATE schema_migration SET checksum = %s WHERE filename = '002_raw.sql'",
            ("0" * 64,),
        )
        c.commit()
        try:
            result = run_migrate(migrated_db)
            assert result.returncode != 0
            assert "has changed since it was applied" in result.stdout + result.stderr
        finally:
            c.execute(
                "UPDATE schema_migration SET checksum = %s WHERE filename = '002_raw.sql'",
                (original,),
            )
            c.commit()


def test_reset_rebuilds_from_scratch(migrated_db):
    import psycopg

    result = run_migrate(migrated_db, "--reset")
    assert result.returncode == 0, result.stderr
    assert result.stdout.count("+ ") == 5

    with psycopg.connect(migrated_db) as c:
        n = c.execute("SELECT count(*) FROM schema_migration").fetchone()[0]
    assert n == 5
