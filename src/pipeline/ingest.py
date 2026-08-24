"""Raw ingestion: CSV files -> raw_record.

The raw layer is append-only and lossless. Nothing is cleaned, judged or
dropped here. The blank row, the embedded header row and the column-shifted row
are all ingested exactly like any other row - they are data-quality problems,
and data-quality handling belongs to the staging layer. If this module ever
starts deciding a row is "bad", the audit trail stops being an audit trail.

    python -m src.pipeline.ingest
    python -m src.pipeline.ingest --run-id 3        # re-ingest into a run
    python -m src.pipeline.ingest --source naukri   # one file only
    python -m src.pipeline.ingest --dry-run         # parse, report, write nothing
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from dataclasses import dataclass, field
from typing import Iterator, Sequence

import psycopg
from psycopg.types.json import Jsonb

from .config import (
    EXPECTED_ROW_COUNTS,
    EXPECTED_TOTAL_ROWS,
    SOURCES,
    SOURCES_BY_KEY,
    SOURCE_ENCODING,
    SourceSpec,
    database_url,
    safe_dsn,
)
from .logging_setup import configure, get_logger

log = get_logger("ingest")

# Reserved payload keys. Prefixed and suffixed with __ so they cannot collide
# with a real CSV column name.
KEY_EXTRA = "__extra_fields__"
KEY_MISSING = "__missing_fields__"
KEY_EMPTY = "__empty_line__"


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RawRow:
    """One parsed CSV record, ready to store. Nothing here is normalised."""

    line_no: int
    payload: dict
    sha256: str
    raw_text: str


class _LineTracker:
    """Feeds lines to csv.reader while recording which ones each record used.

    A CSV record can span several physical lines when a field contains a quoted
    newline. Counting records would therefore give the wrong line number. This
    wrapper reports the exact physical lines consumed per record, so
    source_line_no stays true to the file even if a future export quotes a
    newline. None of the three current files do, but the cost of being right is
    about ten lines.
    """

    def __init__(self, lines: Sequence[str]) -> None:
        self._iter = iter(lines)
        self._consumed: list[str] = []

    def __iter__(self) -> "_LineTracker":
        return self

    def __next__(self) -> str:
        line = next(self._iter)
        self._consumed.append(line)
        return line

    def take(self) -> list[str]:
        consumed, self._consumed = self._consumed, []
        return consumed


def row_sha256(text: str) -> str:
    """Deterministic hash of one record's source text.

    The trailing line terminator is stripped first, so a file that merely gains
    or loses a final newline does not change every hash, and two byte-identical
    rows hash identically regardless of where they sit in the file.
    """
    return hashlib.sha256(text.rstrip("\r\n").encode("utf-8")).hexdigest()


def _build_payload(header: list[str], fields: list[str], line_no: int, source: str) -> dict:
    """Map one record onto the file's own column names.

    Field counts are uniform in all three current files, so the mismatch
    branches below never fire on this dataset. They exist because silently
    truncating a row is exactly the kind of data loss the raw layer is supposed
    to make impossible.
    """
    if not fields:
        # A completely empty physical line. csv yields [] rather than a list of
        # empty strings. Recorded as such rather than skipped.
        log.warning("empty line preserved", extra={"ctx": {"source": source, "line": line_no}})
        return {KEY_EMPTY: True}

    payload: dict = dict(zip(header, fields))

    if len(fields) > len(header):
        extra = fields[len(header):]
        payload[KEY_EXTRA] = extra
        log.warning(
            "row has more fields than the header; extras preserved",
            extra={"ctx": {"source": source, "line": line_no,
                           "header_cols": len(header), "row_cols": len(fields)}},
        )
    elif len(fields) < len(header):
        missing = header[len(fields):]
        payload[KEY_MISSING] = missing
        log.warning(
            "row has fewer fields than the header",
            extra={"ctx": {"source": source, "line": line_no,
                           "header_cols": len(header), "row_cols": len(fields)}},
        )

    return payload


def _dedupe_header(header: list[str], source: str) -> list[str]:
    """Make column names unique so dict(zip(...)) cannot silently drop one."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in header:
        if name in seen:
            seen[name] += 1
            unique = f"{name}__{seen[name]}"
            log.warning(
                "duplicate header column renamed",
                extra={"ctx": {"source": source, "original": name, "renamed": unique}},
            )
        else:
            seen[name] = 0
            unique = name
        out.append(unique)
    return out


def _physical_lines(text: str) -> list[str]:
    """Split on \\n only, keeping terminators.

    Not str.splitlines(): that also breaks on \\x0b, \\x0c, \\x1c and \\u2028,
    none of which end a line as far as a CSV file is concerned. If one appeared
    inside a quoted field, splitlines would desync every line number after it.
    """
    parts = text.split("\n")
    lines = [p + "\n" for p in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    return lines


def read_source(spec: SourceSpec) -> tuple[list[str], list[RawRow]]:
    """Parse one CSV into its header and every data row below it.

    Line 1 is the header, so data starts at line 2. The embedded header row at
    line 16 of source3 is a *data* row and is returned like any other - only the
    first record is treated as the header.
    """
    if not spec.path.exists():
        raise FileNotFoundError(f"source file missing: {spec.path}")

    # newline="" is required by the csv module, and it also stops Python's
    # universal-newline translation from rewriting CRLF to LF. Without it the
    # stored raw_text would not match the file on disk, and a \r\n inside a
    # quoted field would be silently altered.
    with spec.path.open("r", encoding=SOURCE_ENCODING, newline="") as fh:
        text = fh.read()
    physical_lines = _physical_lines(text)

    tracker = _LineTracker(physical_lines)
    reader = csv.reader(tracker)

    header: list[str] | None = None
    rows: list[RawRow] = []
    cursor = 0  # physical lines consumed so far

    for fields in reader:
        used = tracker.take()
        start_line = cursor + 1
        cursor += len(used)

        if header is None:
            header = _dedupe_header(fields, spec.key)
            continue

        raw_text = "".join(used)
        rows.append(
            RawRow(
                line_no=start_line,
                payload=_build_payload(header, fields, start_line, spec.key),
                sha256=row_sha256(raw_text),
                raw_text=raw_text,
            )
        )

    if header is None:
        raise ValueError(f"{spec.filename} is empty - no header row")

    return header, rows


# ---------------------------------------------------------------------------
# writing
# ---------------------------------------------------------------------------


@dataclass
class SourceResult:
    key: str
    read: int = 0
    inserted: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class IngestResult:
    run_id: int | None
    sources: list[SourceResult] = field(default_factory=list)
    dry_run: bool = False

    @property
    def read(self) -> int:
        return sum(s.read for s in self.sources)

    @property
    def inserted(self) -> int:
        return sum(s.inserted for s in self.sources)

    @property
    def skipped(self) -> int:
        return sum(s.skipped for s in self.sources)

    @property
    def errors(self) -> list[str]:
        return [e for s in self.sources for e in s.errors]

    @property
    def ok(self) -> bool:
        return not self.errors


INSERT_SQL = """
INSERT INTO raw_record (run_id, source_system, source_line_no, payload, row_sha256)
VALUES (%s, %s::source_system, %s, %s, %s)
ON CONFLICT (run_id, source_system, source_line_no) DO NOTHING
"""


def create_run(conn: psycopg.Connection, note: str | None = None) -> int:
    run_id = conn.execute(
        "INSERT INTO ingestion_run (note) VALUES (%s) RETURNING id", (note,)
    ).fetchone()[0]
    log.info("ingestion run created", extra={"ctx": {"run_id": run_id}})
    return run_id


def finish_run(conn: psycopg.Connection, run_id: int, succeeded: bool, note: str) -> None:
    conn.execute(
        """UPDATE ingestion_run
              SET status = %s, finished_at = now(), note = %s
            WHERE id = %s""",
        ("succeeded" if succeeded else "failed", note, run_id),
    )
    log.info(
        "ingestion run finished",
        extra={"ctx": {"run_id": run_id, "status": "succeeded" if succeeded else "failed"}},
    )


def ingest_source(
    conn: psycopg.Connection, run_id: int, spec: SourceSpec, dry_run: bool = False
) -> SourceResult:
    """Load one file into raw_record.

    Idempotent within a run: the natural key (run_id, source_system,
    source_line_no) means re-running against the same run inserts nothing and
    counts every row as skipped. A *new* run re-ingests everything under a new
    id, so the two loads stay distinguishable.
    """
    result = SourceResult(key=spec.key)

    header, rows = read_source(spec)
    result.read = len(rows)
    log.info(
        "source read",
        extra={"ctx": {"source": spec.key, "file": spec.filename,
                       "columns": len(header), "rows": len(rows)}},
    )

    expected = EXPECTED_ROW_COUNTS.get(spec.key)
    if expected is not None and len(rows) != expected:
        # Not fatal: the file is allowed to change. But it must be noticed.
        log.warning(
            "row count differs from the profiled baseline",
            extra={"ctx": {"source": spec.key, "expected": expected, "found": len(rows)}},
        )

    if dry_run:
        log.info("dry run - nothing written", extra={"ctx": {"source": spec.key}})
        return result

    for row in rows:
        try:
            # A savepoint per row: one bad row cannot abort the rest of the
            # file, and we still find out about it.
            with conn.transaction():
                cur = conn.execute(
                    INSERT_SQL,
                    (run_id, spec.db_enum, row.line_no, Jsonb(row.payload), row.sha256),
                )
            if cur.rowcount == 1:
                result.inserted += 1
            else:
                result.skipped += 1
                log.debug(
                    "row already present for this run",
                    extra={"ctx": {"source": spec.key, "line": row.line_no}},
                )
        except psycopg.Error as exc:
            message = f"{spec.key} line {row.line_no}: {type(exc).__name__}: {exc}"
            result.errors.append(message)
            log.error(
                "row failed to insert",
                extra={"ctx": {"source": spec.key, "line": row.line_no,
                               "error": type(exc).__name__}},
            )

    log.info(
        "source ingested",
        extra={"ctx": {"source": spec.key, "read": result.read,
                       "inserted": result.inserted, "skipped": result.skipped,
                       "errors": len(result.errors)}},
    )
    return result


def run_ingestion(
    conn: psycopg.Connection,
    specs: Sequence[SourceSpec] = SOURCES,
    run_id: int | None = None,
    dry_run: bool = False,
    note: str | None = None,
) -> IngestResult:
    """Create (or reuse) a run, ingest every source, then close the run out.

    The caller owns the transaction. Nothing here commits, so this composes
    inside a larger unit of work and the test suite can roll everything back.
    The CLI's `with psycopg.connect(...)` block commits on clean exit.
    """
    if dry_run:
        result = IngestResult(run_id=None, dry_run=True)
        for spec in specs:
            result.sources.append(ingest_source(conn, -1, spec, dry_run=True))
        return result

    # Check every file up front. Creating a run and then dying on a missing
    # file three sources in leaves a confusing half-run behind.
    missing = [s.filename for s in specs if not s.path.exists()]
    if missing:
        raise FileNotFoundError(f"source file missing: {', '.join(missing)}")

    if run_id is None:
        run_id = create_run(conn, note=note or "raw CSV ingestion")
    else:
        exists = conn.execute(
            "SELECT 1 FROM ingestion_run WHERE id = %s", (run_id,)
        ).fetchone()
        if not exists:
            raise ValueError(f"ingestion_run {run_id} does not exist")

    result = IngestResult(run_id=run_id)
    for spec in specs:
        result.sources.append(ingest_source(conn, run_id, spec))

    summary = (
        f"read={result.read} inserted={result.inserted} "
        f"skipped={result.skipped} errors={len(result.errors)}"
    )
    finish_run(conn, run_id, succeeded=result.ok, note=summary)

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _report(result: IngestResult) -> None:
    """Human-facing summary on stdout, separate from the log stream on stderr."""
    print()
    print(f"{'source':<14}{'read':>8}{'inserted':>10}{'skipped':>9}{'errors':>8}")
    print("-" * 49)
    for s in result.sources:
        print(f"{s.key:<14}{s.read:>8}{s.inserted:>10}{s.skipped:>9}{len(s.errors):>8}")
    print("-" * 49)
    print(f"{'TOTAL':<14}{result.read:>8}{result.inserted:>10}"
          f"{result.skipped:>9}{len(result.errors):>8}")
    print()

    if result.dry_run:
        print("dry run - nothing was written")
    else:
        print(f"ingestion_run id : {result.run_id}")
        print(f"status           : {'succeeded' if result.ok else 'FAILED'}")

    if result.read != EXPECTED_TOTAL_ROWS:
        print(f"note: read {result.read} rows, profiled baseline is {EXPECTED_TOTAL_ROWS}")

    if result.errors:
        print(f"\n{len(result.errors)} error(s):")
        for e in result.errors:
            print(f"  - {e}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.pipeline.ingest",
        description="Ingest the three source CSVs into raw_record. Append-only, lossless.",
    )
    parser.add_argument(
        "--source", action="append", choices=sorted(SOURCES_BY_KEY),
        help="ingest only this source; repeatable. Default: all three.",
    )
    parser.add_argument(
        "--run-id", type=int,
        help="reuse an existing ingestion_run instead of creating one "
             "(re-running against the same run inserts nothing)",
    )
    parser.add_argument("--dry-run", action="store_true", help="parse and report, write nothing")
    parser.add_argument("--note", help="note stored on the ingestion_run")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--log-format", default="text", choices=["text", "json"])
    args = parser.parse_args(argv)

    configure(level=args.log_level, fmt=args.log_format)

    specs = [SOURCES_BY_KEY[k] for k in args.source] if args.source else list(SOURCES)

    if args.dry_run:
        result = run_ingestion(None, specs, dry_run=True)  # type: ignore[arg-type]
        _report(result)
        return 0

    url = database_url()
    log.info("connecting", extra={"ctx": {"dsn": safe_dsn(url)}})

    try:
        with psycopg.connect(url) as conn:
            result = run_ingestion(conn, specs, run_id=args.run_id, note=args.note)
    except FileNotFoundError as exc:
        log.error("source file missing", extra={"ctx": {"error": str(exc)}})
        return 2
    except psycopg.OperationalError as exc:
        log.error("cannot connect to the database", extra={"ctx": {"error": str(exc)}})
        return 2

    _report(result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
