"""Apply db/migrations/*.sql in filename order.

Idempotent: every applied file is recorded in schema_migration with a checksum,
so re-running is a no-op and an edited-after-apply migration is caught instead
of silently diverging from what is actually in the database.

    python scripts/migrate.py            # apply anything outstanding
    python scripts/migrate.py --status   # show what is applied, change nothing
    python scripts/migrate.py --reset    # drop the schema and re-apply from scratch

DDL in PostgreSQL is transactional, so each file applies all-or-nothing: a
migration that fails half way leaves the database exactly as it was.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = PROJECT_ROOT / "db" / "migrations"

BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migration (
    filename   text        PRIMARY KEY,
    checksum   char(64)    NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
"""


def dsn() -> str:
    load_dotenv(PROJECT_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set. Copy .env.example to .env and fill it in.")
    return url


def migrations() -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))
    if not files:
        sys.exit(f"No migrations found in {MIGRATIONS_DIR}")
    return files


def checksum(path: Path) -> str:
    # Normalise line endings so a CRLF/LF change does not read as a content change.
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def show_status(conn: psycopg.Connection) -> None:
    conn.execute(BOOTSTRAP)
    applied = dict(conn.execute("SELECT filename, checksum FROM schema_migration").fetchall())
    print(f"{'migration':<28} {'status':<12} checksum")
    print("-" * 62)
    for path in migrations():
        here = checksum(path)
        if path.name not in applied:
            state = "PENDING"
        elif applied[path.name] != here:
            state = "MODIFIED!"
        else:
            state = "applied"
        print(f"{path.name:<28} {state:<12} {here[:12]}")


def reset(conn: psycopg.Connection) -> None:
    """Drop everything this project owns and start clean.

    DROP SCHEMA public CASCADE removes tables, views, types, functions and
    triggers in one step, which is exactly what a re-runnable demo needs. It is
    safe here because this database holds nothing but this project.
    """
    print("  dropping schema public ...")
    conn.execute("DROP SCHEMA IF EXISTS public CASCADE")
    conn.execute("CREATE SCHEMA public")
    # Re-grant what the postgres default template would have given us.
    conn.execute("GRANT ALL ON SCHEMA public TO CURRENT_USER")
    conn.execute("GRANT ALL ON SCHEMA public TO public")
    conn.commit()


def apply_all(conn: psycopg.Connection) -> int:
    conn.execute(BOOTSTRAP)
    conn.commit()

    applied = dict(conn.execute("SELECT filename, checksum FROM schema_migration").fetchall())
    count = 0

    for path in migrations():
        here = checksum(path)
        previous = applied.get(path.name)

        if previous == here:
            print(f"  = {path.name}  (already applied)")
            continue

        if previous is not None:
            sys.exit(
                f"\n{path.name} has changed since it was applied.\n"
                f"  in database: {previous[:12]}\n"
                f"  on disk:     {here[:12]}\n"
                "Migrations are immutable once applied. Add a new numbered file, "
                "or re-run with --reset to rebuild from scratch."
            )

        sql = path.read_text(encoding="utf-8")
        try:
            conn.execute(sql)
            conn.execute(
                "INSERT INTO schema_migration (filename, checksum) VALUES (%s, %s)",
                (path.name, here),
            )
            conn.commit()
        except psycopg.Error as exc:
            conn.rollback()
            sys.exit(f"\n  X {path.name} failed, rolled back:\n\n{exc}")

        print(f"  + {path.name}")
        count += 1

    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reset", action="store_true", help="drop the schema and re-apply everything")
    parser.add_argument("--status", action="store_true", help="show applied/pending, change nothing")
    args = parser.parse_args()

    url = dsn()
    # Never print the password.
    where = url.split("@")[-1] if "@" in url else url
    print(f"database: {where}")

    with psycopg.connect(url, autocommit=False) as conn:
        if args.status:
            show_status(conn)
            return 0

        if args.reset:
            reset(conn)

        applied = apply_all(conn)

    print(f"\n{applied} migration(s) applied." if applied else "\nnothing to do, schema up to date.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
