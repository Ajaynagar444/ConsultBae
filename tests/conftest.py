"""Shared pytest fixtures.

Every test runs against TEST_DATABASE_URL (consultbae_test), never the working
database. The schema is rebuilt once per session by invoking the real
scripts/migrate.py as a subprocess, so the tests exercise the actual migration
runner rather than a parallel copy of it that could drift.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def test_dsn() -> str:
    load_dotenv(PROJECT_ROOT / ".env")
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not set in .env")
    try:
        with psycopg.connect(dsn, connect_timeout=5):
            pass
    except psycopg.OperationalError as exc:
        pytest.skip(f"test database unreachable: {exc}")
    return dsn


@pytest.fixture(scope="session")
def migrated_db(test_dsn: str) -> str:
    """Rebuild the test schema from scratch, once per session."""
    env = {**os.environ, "DATABASE_URL": test_dsn}
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "migrate.py"), "--reset"],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"migrate.py --reset failed:\n{result.stdout}\n{result.stderr}")
    return test_dsn


@pytest.fixture
def conn(migrated_db: str):
    """A connection whose work is always rolled back.

    Tests can insert freely; nothing survives the test. That keeps them order
    independent without paying to rebuild the schema each time.
    """
    with psycopg.connect(migrated_db) as c:
        tx = c.transaction(force_rollback=True)
        with tx:
            yield c
