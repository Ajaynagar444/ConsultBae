"""Configuration and the source-system mapping.

One place that answers: which files do we read, what do we call each source, and
which database enum value does it map to. Everything downstream imports from
here rather than hard-coding a filename.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
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

    `columns` is the file's header exactly as written, which the staging layer
    uses to recognise an embedded header row and to work out a column shift.
    `fields` maps a semantic role onto whichever column carries it in this
    file, so the staging code is written once instead of three times.
    """

    key: str
    filename: str
    db_enum: str
    description: str
    columns: tuple[str, ...] = ()
    fields: "dict[str, str]" = field(default_factory=dict)

    @property
    def path(self) -> Path:
        return RAW_DATA_DIR / self.filename

    def column_for(self, role: str) -> str | None:
        return self.fields.get(role)


# The three sources, in the order the assignment lists them.
SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec(
        key="naukri",
        filename="source1_naukri_applicants.csv",
        db_enum="naukri_applicants",
        description="Recruitment ATS. Has email AND phone - the only bridge between the other two.",
        columns=("Full Name", "Email", "Phone", "City", "Experience (Years)",
                 "Current CTC", "Applied Date", "Skills"),
        fields={
            "name": "Full Name",
            "email": "Email",
            "phone": "Phone",
            "city": "City",
            "experience": "Experience (Years)",
            "ctc": "Current CTC",
            "applied_date": "Applied Date",
            "skills": "Skills",
        },
    ),
    SourceSpec(
        key="gig_workers",
        filename="source2_gig_workers.csv",
        db_enum="gig_workers",
        description="Gig marketplace. Email only, no phone column.",
        columns=("email_id", "worker_name", "rate", "location", "status", "skill_tags"),
        fields={
            "email": "email_id",
            "name": "worker_name",
            "rate": "rate",
            "city": "location",
            "status": "status",
            "skills": "skill_tags",
        },
    ),
    SourceSpec(
        key="cbnexus",
        filename="source3_cbnexus_contacts.csv",
        db_enum="cbnexus_contacts",
        description="CBNexus CRM. Phone only, no email column.",
        columns=("Name", "Phone Number", "City", "Verified", "Projects Completed"),
        fields={
            "name": "Name",
            "phone": "Phone Number",
            "city": "City",
            "verified": "Verified",
            "projects": "Projects Completed",
        },
    ),
)

SOURCES_BY_KEY: dict[str, SourceSpec] = {s.key: s for s in SOURCES}
SOURCES_BY_DB_ENUM: dict[str, SourceSpec] = {s.db_enum: s for s in SOURCES}

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
