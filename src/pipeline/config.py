"""Configuration and the source-system mapping.

One place that answers: which files do we read, what do we call each source, and
which database enum value does it map to. Everything downstream imports from
here rather than hard-coding a filename.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Files are read as UTF-8 with BOM tolerance. Profiling confirmed all three are
# pure ASCII with no BOM, but utf-8-sig costs nothing and stops a future
# Excel-exported file from silently prefixing the first header with ﻿.
SOURCE_ENCODING = "utf-8-sig"


@dataclass(frozen=True)
class SourceSpec:
    """One source file and its identity across the system.

    `key` is what a human types on the command line. `db_enum` is the
    source_system value stored in the database - deliberately more descriptive
    than the short key, because in a SQL result set `naukri_applicants` explains
    itself and `naukri` does not.
    """

    key: str
    filename: str
    db_enum: str
    description: str

    @property
    def path(self) -> Path:
        return RAW_DATA_DIR / self.filename


# The three sources, in the order the assignment lists them.
SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec(
        key="naukri",
        filename="source1_naukri_applicants.csv",
        db_enum="naukri_applicants",
        description="Recruitment ATS. Has email AND phone - the only bridge between the other two.",
    ),
    SourceSpec(
        key="gig_workers",
        filename="source2_gig_workers.csv",
        db_enum="gig_workers",
        description="Gig marketplace. Email only, no phone column.",
    ),
    SourceSpec(
        key="cbnexus",
        filename="source3_cbnexus_contacts.csv",
        db_enum="cbnexus_contacts",
        description="CBNexus CRM. Phone only, no email column.",
    ),
)

SOURCES_BY_KEY: dict[str, SourceSpec] = {s.key: s for s in SOURCES}

# Physical data rows per file, measured during profiling (docs/data-profile.md
# section 2). Asserted after ingestion: if a file changes underneath us, the run
# says so rather than quietly loading a different number of rows.
EXPECTED_ROW_COUNTS: dict[str, int] = {
    "naukri": 42,
    "gig_workers": 32,
    "cbnexus": 31,
}
EXPECTED_TOTAL_ROWS = sum(EXPECTED_ROW_COUNTS.values())  # 105


def database_url() -> str:
    """DSN from .env.

    Note: a password containing '@' must be percent-encoded here (@ -> %40),
    or the driver reads everything after it as the hostname.
    """
    load_dotenv(PROJECT_ROOT / ".env")
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit(
            "DATABASE_URL is not set. Copy .env.example to .env and fill it in."
        )
    return url


def safe_dsn(url: str) -> str:
    """The DSN with credentials stripped, for logging."""
    return url.split("@")[-1] if "@" in url else url
