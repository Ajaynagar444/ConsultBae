"""Staging: raw_record -> staged_person + data_issue.

Exactly one staged_person per raw_record, including the rows that are broken.
Nothing is merged, nothing is deduplicated, and no person record is created -
staging is still 1:1 with the source. Matching is a separate step with separate
evidence.

Idempotent: staging is a pure function of the raw layer, so re-running replaces
the derived rows for that ingestion run rather than appending to them. That is
also why data_issue rows do not accumulate on a rerun.

    python -m src.pipeline.stage                 # latest succeeded run
    python -m src.pipeline.stage --run-id 2
    python -m src.pipeline.stage --dry-run
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Sequence

import psycopg

from . import normalize as nz
from .config import SOURCES_BY_DB_ENUM, SourceSpec, database_url, safe_dsn
from .logging_setup import configure, get_logger
from .repair import Verdict, assess

log = get_logger("stage")

# Issue codes that describe a cosmetic clean-up rather than a defect. Recorded
# at info severity so the Task 4 report can separate "we tidied this" from
# "this was wrong".
COSMETIC = {
    "email_case_normalised", "email_whitespace_trimmed",
    "phone_format_normalised",
    "name_whitespace_normalised", "name_case_unusual",
    "city_case_normalised", "city_whitespace_trimmed", "city_alias_applied",
    "skills_case_normalised", "skills_duplicates_removed",
    "status_case_normalised", "verified_spelling_normalised",
    "ctc_unit_rupee", "rate_unit_hourly",
}
STRUCTURAL_ERROR = {"row_blank", "row_embedded_header"}

# A missing field is only worth reporting when the source is supposed to have
# it. source2 has no phone column, so "phone_missing" there is not a finding.
IGNORE_MISSING = {
    "email_missing", "phone_missing", "city_missing", "name_missing",
    "skills_missing", "ctc_missing", "rate_missing", "date_missing",
    "status_missing", "verified_missing", "experience_missing",
    "projects_missing",
}


def severity_for(code: str) -> str:
    if code in STRUCTURAL_ERROR:
        return "error"
    if code in COSMETIC:
        return "info"
    return "warning"


ACTION_FOR: dict[str, str] = {
    "row_blank": "quarantined in staging; raw row preserved",
    "row_embedded_header": "quarantined in staging; raw row preserved",
    "row_column_shift_repaired": "columns rotated back into header order for staging; raw row preserved",
    "email_case_normalised": "lowercased",
    "email_whitespace_trimmed": "trimmed",
    "email_invalid": "left null; row kept",
    "phone_format_normalised": "reduced to 10 national digits and +91 E.164",
    "phone_invalid": "left null; row kept",
    "name_whitespace_normalised": "trimmed and internal whitespace collapsed",
    "name_abbreviated": "kept as written; name_key not expanded, identity not inferred",
    "name_case_unusual": "casing preserved; display form is a survivorship decision",
    "city_alias_applied": "mapped to the canonical city name",
    "city_case_normalised": "canonical casing applied",
    "city_whitespace_trimmed": "trimmed",
    "city_region_preserved": "kept as a region; not collapsed to a city",
    "city_unknown": "kept verbatim; not guessed at",
    "ctc_unit_lakh": "value below 100 read as lakhs and converted to annual INR; source unit recorded",
    "ctc_unit_rupee": "already absolute INR; source unit recorded",
    "ctc_unparseable": "left null; row kept",
    "rate_unit_hourly": "stored with unit per_hour; not converted",
    "rate_unit_monthly": "k/month expanded to INR and stored with unit per_month; not converted to hourly",
    "rate_unparseable": "left null; row kept",
    "status_case_normalised": "folded to the canonical status",
    "status_unrecognised": "left null; corrupt value not admitted as a status",
    "verified_spelling_normalised": "folded to a boolean",
    "verified_unrecognised": "left null",
    "skills_case_normalised": "lowercased and trimmed",
    "skills_duplicates_removed": "duplicate tokens dropped",
    "skills_unknown_token": "token kept verbatim and flagged; no skill invented",
    "date_unrecognised_format": "left null; not guessed at",
    "date_invalid": "left null; not guessed at",
}

for _label in ("yyyy_mm_dd", "dd_mm_yyyy", "mm_dd_yyyy", "d_mon_yyyy"):
    ACTION_FOR[f"date_format_{_label}"] = "parsed with explicit format detection and stored as a date"
    COSMETIC.add(f"date_format_{_label}")


# ---------------------------------------------------------------------------


@dataclass
class StageResult:
    run_id: int
    raw_rows: int = 0
    staged: int = 0
    quarantined: int = 0
    repaired: int = 0
    issues: int = 0
    issue_counts: Counter = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class StagedRow:
    raw_record_id: int
    source_system: str
    columns: dict[str, Any]
    issues: list[tuple[str, str | None, str]]  # (code, column, detail)


# ---------------------------------------------------------------------------
# transform one raw row
# ---------------------------------------------------------------------------


def transform(raw_id: int, spec: SourceSpec, payload: dict) -> StagedRow:
    """Repair if needed, then normalise every field the source carries."""
    verdict = assess(payload, spec)
    issues: list[tuple[str, str | None, str]] = [
        (code, None, verdict.note or "") for code in verdict.issues
    ]

    cols: dict[str, Any] = {
        "raw_record_id": raw_id,
        "source_system": spec.db_enum,
        "is_quarantined": verdict.verdict is Verdict.QUARANTINE,
        "quarantined_as": verdict.reason,
        "was_repaired": verdict.verdict is Verdict.REPAIRED,
        "repair_note": verdict.note if verdict.verdict is Verdict.REPAIRED else None,
        "skills": [],
    }

    if verdict.verdict is Verdict.QUARANTINE:
        # Nothing to normalise: the row is not a person. It is stored so the
        # count still reconciles against raw, and so the report can point at it.
        return StagedRow(raw_id, spec.db_enum, cols, issues)

    data = verdict.payload

    def run(role: str, fn) -> nz.Norm | None:
        column = spec.column_for(role)
        if column is None:
            return None
        result = fn(data.get(column))
        for code in result.issues:
            if code in IGNORE_MISSING:
                continue
            issues.append((code, column, result.detail))
        return result

    if (n := run("name", nz.normalize_name)) and n.ok:
        cols["full_name"] = n.value.display
        cols["name_key"] = n.value.key

    if (n := run("email", nz.normalize_email)) and n.ok:
        cols["email_norm"] = n.value

    if (n := run("phone", nz.normalize_phone)) and n.ok:
        cols["phone_e164"] = n.value.e164

    if (n := run("city", nz.normalize_city)) and n.ok:
        cols["city_raw"] = n.original
        cols["city_norm"] = n.value.name
        cols["is_region"] = n.value.is_region

    if (n := run("experience", nz.normalize_experience)) and n.ok:
        cols["experience_years"] = n.value

    if (n := run("ctc", nz.normalize_ctc)) and n.ok:
        cols["ctc_annual_inr"], cols["ctc_source_unit"] = n.value

    if (n := run("rate", nz.normalize_rate)) and n.ok:
        cols["rate_amount"], cols["rate_source_unit"] = n.value

    if (n := run("applied_date", nz.normalize_date)) and n.ok:
        cols["applied_on"] = n.value

    if (n := run("status", nz.normalize_status)) and n.ok:
        cols["status"] = n.value

    if (n := run("verified", nz.normalize_verified)) and n.ok:
        cols["is_verified"] = n.value

    if (n := run("projects", nz.normalize_projects)) and n.ok:
        cols["projects_completed"] = n.value

    if (n := run("skills", nz.normalize_skills)) and n.ok:
        cols["skills"] = list(n.value)

    return StagedRow(raw_id, spec.db_enum, cols, issues)


# ---------------------------------------------------------------------------
# database
# ---------------------------------------------------------------------------


def latest_run(conn: psycopg.Connection) -> int | None:
    row = conn.execute(
        """SELECT id FROM ingestion_run
            WHERE status = 'succeeded'
              AND EXISTS (SELECT 1 FROM raw_record r WHERE r.run_id = ingestion_run.id)
            ORDER BY id DESC LIMIT 1"""
    ).fetchone()
    return row[0] if row else None


def stage_run(conn: psycopg.Connection, run_id: int, dry_run: bool = False) -> StageResult:
    """Rebuild the staging layer for one ingestion run.

    The caller owns the transaction.
    """
    result = StageResult(run_id=run_id)

    raw_rows = conn.execute(
        """SELECT id, source_system::text, payload, source_line_no
             FROM raw_record WHERE run_id = %s
            ORDER BY source_system, source_line_no""",
        (run_id,),
    ).fetchall()

    result.raw_rows = len(raw_rows)
    if not raw_rows:
        raise ValueError(f"ingestion_run {run_id} has no raw records")

    staged: list[StagedRow] = []
    for raw_id, db_enum, payload, line_no in raw_rows:
        spec = SOURCES_BY_DB_ENUM.get(db_enum)
        if spec is None:
            result.errors.append(f"unknown source_system {db_enum!r} on raw_record {raw_id}")
            continue
        row = transform(raw_id, spec, payload)
        staged.append(row)

        if row.columns["is_quarantined"]:
            result.quarantined += 1
            log.warning("row quarantined", extra={"ctx": {
                "source": spec.key, "line": line_no,
                "reason": row.columns["quarantined_as"]}})
        if row.columns["was_repaired"]:
            result.repaired += 1
            log.warning("row repaired", extra={"ctx": {
                "source": spec.key, "line": line_no,
                "note": row.columns["repair_note"]}})

        for code, _col, _detail in row.issues:
            result.issue_counts[code] += 1

    result.issues = sum(result.issue_counts.values())

    if dry_run:
        log.info("dry run - nothing written", extra={"ctx": {"run_id": run_id}})
        result.staged = len(staged)
        return result

    # Replace rather than append: staging is derived, so a rerun must converge
    # on the same rows instead of piling up duplicates. Scoped to this run's
    # raw records, so other runs are untouched.
    conn.execute(
        """DELETE FROM staged_person
            WHERE raw_record_id IN (SELECT id FROM raw_record WHERE run_id = %s)""",
        (run_id,),
    )
    conn.execute("DELETE FROM data_issue WHERE run_id = %s", (run_id,))

    for row in staged:
        cols = {k: v for k, v in row.columns.items() if v is not None or k == "skills"}
        names = ", ".join(cols)
        holes = ", ".join(["%s"] * len(cols))
        try:
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO staged_person ({names}) VALUES ({holes})",
                    tuple(cols.values()),
                )
            result.staged += 1
        except psycopg.Error as exc:
            msg = f"raw_record {row.raw_record_id}: {type(exc).__name__}: {exc}"
            result.errors.append(msg)
            log.error("staging insert failed", extra={"ctx": {
                "raw_record_id": row.raw_record_id, "error": type(exc).__name__}})
            continue

        for code, column, detail in row.issues:
            conn.execute(
                """INSERT INTO data_issue
                     (run_id, raw_record_id, source_system, issue_code, severity,
                      column_name, detail, action_taken)
                   VALUES (%s, %s, %s::source_system, %s, %s::issue_severity, %s, %s, %s)""",
                (run_id, row.raw_record_id, row.source_system, code,
                 severity_for(code), column,
                 detail or code.replace("_", " "),
                 ACTION_FOR.get(code, "recorded")),
            )

    log.info("staging complete", extra={"ctx": {
        "run_id": run_id, "raw": result.raw_rows, "staged": result.staged,
        "quarantined": result.quarantined, "repaired": result.repaired,
        "issues": result.issues}})
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _report(r: StageResult, dry_run: bool) -> None:
    print()
    print(f"  ingestion run     : {r.run_id}")
    print(f"  raw records       : {r.raw_rows}")
    print(f"  staged records    : {r.staged}")
    print(f"  quarantined       : {r.quarantined}")
    print(f"  repaired          : {r.repaired}")
    print(f"  data issues       : {r.issues}")
    if dry_run:
        print("\n  dry run - nothing written")

    if r.issue_counts:
        print(f"\n  {'issue code':<34}{'sev':<9}count")
        print("  " + "-" * 50)
        for code, n in sorted(r.issue_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"  {code:<34}{severity_for(code):<9}{n}")

    if r.errors:
        print(f"\n  {len(r.errors)} error(s):")
        for e in r.errors[:10]:
            print(f"    - {e}")


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m src.pipeline.stage",
        description="Normalise and repair raw rows into staged_person. 1:1 with raw; no merging.",
    )
    p.add_argument("--run-id", type=int, help="ingestion run to stage (default: latest succeeded)")
    p.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--log-format", default="text", choices=["text", "json"])
    args = p.parse_args(argv)

    configure(level=args.log_level, fmt=args.log_format)
    url = database_url()
    log.info("connecting", extra={"ctx": {"dsn": safe_dsn(url)}})

    try:
        with psycopg.connect(url) as conn:
            run_id = args.run_id or latest_run(conn)
            if run_id is None:
                log.error("no ingested run found; run python -m src.pipeline.ingest first")
                return 2
            result = stage_run(conn, run_id, dry_run=args.dry_run)
            if args.dry_run:
                conn.rollback()
    except psycopg.OperationalError as exc:
        log.error("cannot connect", extra={"ctx": {"error": str(exc)}})
        return 2
    except ValueError as exc:
        log.error("staging aborted", extra={"ctx": {"error": str(exc)}})
        return 2

    _report(result, args.dry_run)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
