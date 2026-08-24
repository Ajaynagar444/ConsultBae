"""Structural repair and quarantine decisions for raw rows.

Three rows in this dataset are not usable as they stand. This module decides
which, and why, using evidence from the row's own structure. Nothing here is
keyed on a line number: hardcoding "source2 line 20 is broken" would pass the
tests and fail the moment the file is re-exported with one extra row.

The raw layer is never touched. A repair produces a NEW payload for staging;
raw_record keeps the corruption verbatim, forever.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from .config import SourceSpec
from .normalize import CANONICAL_SKILLS, STATUS_MAP

# Reserved payload keys written by the ingest layer for rows whose field count
# did not match the header. They are bookkeeping, not source columns.
INGEST_KEYS = {"__extra_fields__", "__missing_fields__", "__empty_line__"}


class Verdict(str, Enum):
    OK = "ok"
    QUARANTINE = "quarantine"
    REPAIRED = "repaired"


@dataclass(frozen=True)
class RepairResult:
    verdict: Verdict
    payload: dict          # what staging should normalise
    reason: str | None = None   # quarantine_reason enum value, when quarantined
    note: str | None = None     # human explanation, required for a repair
    issues: tuple[str, ...] = ()


def _values(payload: dict, spec: SourceSpec) -> list[str]:
    """The row's values in header order, ignoring ingest bookkeeping keys."""
    return [str(payload.get(col, "")) for col in spec.columns]


# ---------------------------------------------------------------------------
# A. fully blank row
# ---------------------------------------------------------------------------


def is_blank_row(payload: dict, spec: SourceSpec) -> bool:
    """Every column empty. source2 line 12 is `,,,,,` - six empty fields.

    Also catches the ingest layer's marker for a physically empty line.
    """
    if payload.get("__empty_line__"):
        return True
    values = _values(payload, spec)
    return bool(values) and all(v.strip() == "" for v in values)


# ---------------------------------------------------------------------------
# B. embedded header row
# ---------------------------------------------------------------------------


def is_embedded_header(payload: dict, spec: SourceSpec) -> bool:
    """The row repeats the file's own header. source3 line 16.

    Compared case-insensitively and whitespace-insensitively against the
    declared header, so a re-export that changes casing is still caught.
    """
    values = _values(payload, spec)
    if len(values) != len(spec.columns):
        return False
    return all(
        v.strip().casefold() == col.strip().casefold()
        for v, col in zip(values, spec.columns)
    )


# ---------------------------------------------------------------------------
# C. column-shifted row
# ---------------------------------------------------------------------------

_RATE_RE = re.compile(r"^\d+(?:\.\d+)?\s*(?:/\s*hr|k\s*/\s*month)$", re.I)


def _looks_like_email(value: str) -> bool:
    return "@" in value and "." in value.split("@")[-1] and " " not in value.strip()


def _coherence_score(values: list[str], spec: SourceSpec) -> int:
    """How well a candidate ordering fits what each column is supposed to hold.

    Used to confirm a proposed rotation rather than trust it. Only checks
    columns whose content is recognisable by shape - email, rate, status,
    skills - which is enough to distinguish the right rotation from the five
    wrong ones on a six-column row.
    """
    by_col = dict(zip(spec.columns, values))
    score = 0

    if col := spec.column_for("email"):
        if _looks_like_email(by_col.get(col, "")):
            score += 1
    if col := spec.column_for("rate"):
        if _RATE_RE.match(by_col.get(col, "").strip()):
            score += 1
    if col := spec.column_for("status"):
        if by_col.get(col, "").strip().lower() in STATUS_MAP:
            score += 1
    if col := spec.column_for("skills"):
        tokens = [t.strip().lower() for t in by_col.get(col, "").split(",") if t.strip()]
        if tokens and all(t in CANONICAL_SKILLS for t in tokens):
            score += 1
    return score


def detect_column_shift(payload: dict, spec: SourceSpec) -> tuple[dict, str] | None:
    """Detect a rotated row and return the corrected payload plus an explanation.

    The evidence, for source2 line 20:

        header : email_id | worker_name | rate | location | status | skill_tags
        row    : "react,  | ISHA.CHOPRA | Isha | 1406/hr  | Pune   | active
                  js,     | 95@...ORG   | Chopra
                  mysql"

    email_id holds no '@' and worker_name does, so the row is rotated one place
    right. The index of the '@' field IS the rotation offset - no constant is
    needed. Rotating left by that offset is then confirmed with a coherence
    check before it is accepted, so a row that merely lacks an email is
    quarantined rather than scrambled further.

    Returns None when the row is not a rotation.
    """
    email_col = spec.column_for("email")
    if not email_col:
        return None

    values = _values(payload, spec)
    if len(values) != len(spec.columns):
        return None

    # Already correct: nothing to do.
    if _looks_like_email(str(payload.get(email_col, ""))):
        return None

    offsets = [i for i, v in enumerate(values) if _looks_like_email(v)]
    if len(offsets) != 1:
        # No email anywhere, or several - not a rotation we can prove.
        return None

    offset = offsets[0]
    rotated = values[offset:] + values[:offset]

    before = _coherence_score(values, spec)
    after = _coherence_score(rotated, spec)
    if after <= before:
        return None

    repaired = dict(payload)
    for col, value in zip(spec.columns, rotated):
        repaired[col] = value

    note = (
        f"columns rotated right by {offset}; "
        f"rotated left by {offset} to restore header order "
        f"(coherence {before} -> {after} of 4). "
        f"Detected because {email_col!r} held no '@' and "
        f"{spec.columns[offset]!r} did."
    )
    return repaired, note


# ---------------------------------------------------------------------------
# the decision
# ---------------------------------------------------------------------------


def assess(payload: dict, spec: SourceSpec) -> RepairResult:
    """Decide what staging should do with one raw row.

    Order matters: a blank row would also fail the shift check, and an embedded
    header must not be mistaken for a shifted one.
    """
    if is_blank_row(payload, spec):
        return RepairResult(
            verdict=Verdict.QUARANTINE,
            payload=payload,
            reason="blank_row",
            note="every column empty",
            issues=("row_blank",),
        )

    if is_embedded_header(payload, spec):
        return RepairResult(
            verdict=Verdict.QUARANTINE,
            payload=payload,
            reason="embedded_header",
            note="row repeats the file header verbatim",
            issues=("row_embedded_header",),
        )

    if shifted := detect_column_shift(payload, spec):
        repaired, note = shifted
        return RepairResult(
            verdict=Verdict.REPAIRED,
            payload=repaired,
            note=note,
            issues=("row_column_shift_repaired",),
        )

    return RepairResult(verdict=Verdict.OK, payload=payload)
