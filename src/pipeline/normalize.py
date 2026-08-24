"""Pure normalisation functions. No database, no I/O, no logging.

Every rule here comes from a measured observation in docs/data-profile.md, not
from a guess about what Indian phone numbers or Indian city names look like in
general. Where the data does not say, these functions return None rather than
inventing a value - a null that is visibly null beats a plausible number nobody
can trace.

Each function returns a Norm: the cleaned value, the original input, and the
issue codes that describe what had to change. Returning the issues from the
pure layer means data-quality detection is unit-testable without a database,
and stage.py just records what it is handed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

# ---------------------------------------------------------------------------
# result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Norm:
    """A normalised value plus the story of how it got that way."""

    value: Any
    original: str
    issues: tuple[str, ...] = ()
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.value is not None

    def with_issue(self, code: str, detail: str = "") -> "Norm":
        return Norm(self.value, self.original, self.issues + (code,), detail or self.detail)


def _blank(raw: Any) -> bool:
    return raw is None or str(raw).strip() == ""


# ---------------------------------------------------------------------------
# email
# ---------------------------------------------------------------------------

# Deliberately permissive: this is a normaliser, not an RFC 5322 validator.
# It only has to reject values that clearly are not addresses, such as the
# skill list that lands in email_id on the column-shifted row.
_EMAIL_RE = re.compile(r"^[^@\s,]+@[^@\s,]+\.[^@\s,]+$")


def normalize_email(raw: Any) -> Norm:
    """Trim and lowercase. 9 source2 rows arrive ALL-CAPS."""
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("email_missing",))

    trimmed = original.strip()
    lowered = trimmed.lower()

    if not _EMAIL_RE.match(lowered):
        return Norm(None, original, ("email_invalid",), f"not an email address: {trimmed!r}")

    issues: list[str] = []
    if trimmed != original:
        issues.append("email_whitespace_trimmed")
    if lowered != trimmed:
        issues.append("email_case_normalised")

    return Norm(lowered, original, tuple(issues))


# ---------------------------------------------------------------------------
# phone
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Phone:
    national: str  # 10 digits, no country code
    e164: str      # +91XXXXXXXXXX


def normalize_phone(raw: Any) -> Norm:
    """Fold the observed Indian formats onto one canonical pair.

    Formats measured across source1 and source3:
        +919000000254   9000000237   09000000287
        919000000231    +91-9000000131

    All 72 real numbers reduce to exactly 10 national digits. Anything that
    does not is returned as None with a reason - guessing at a malformed number
    would put an unreachable person into the golden layer.
    """
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("phone_missing",))

    digits = re.sub(r"\D", "", original)
    stripped = digits

    # Peel the country code / trunk prefix, longest first so 091... is handled
    # before 91... would mangle it.
    if len(stripped) == 13 and stripped.startswith("091"):
        stripped = stripped[3:]
    elif len(stripped) == 12 and stripped.startswith("91"):
        stripped = stripped[2:]
    elif len(stripped) == 11 and stripped.startswith("0"):
        stripped = stripped[1:]

    # Indian mobile numbers are 10 digits and begin 6-9.
    if not re.fullmatch(r"[6-9]\d{9}", stripped):
        return Norm(None, original, ("phone_invalid",),
                    f"cannot reduce to a 10-digit Indian mobile: {original!r}")

    issues: list[str] = []
    if original.strip() != stripped:
        issues.append("phone_format_normalised")

    return Norm(Phone(national=stripped, e164=f"+91{stripped}"), original, tuple(issues))


# ---------------------------------------------------------------------------
# name
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Name:
    display: str  # trimmed, whitespace-collapsed, original casing kept
    key: str      # lowercase, punctuation stripped - a BLOCKING key only


def normalize_name(raw: Any) -> Norm:
    """Clean a name for display and derive a blocking key.

    Two deliberate non-decisions:

    Casing is preserved. `RITU SHARMA` stays as written, because choosing a
    display form is a survivorship decision for the golden layer, and staging
    is 1:1 with the source.

    `R. Verma` produces the key `r verma`, NOT `rohit verma`. Expanding an
    initial would be inventing an identity claim. That row is the same person
    as `Rohit Verma`, but the evidence for that is a shared email and phone -
    never the name. name_key exists to generate candidates, never to confirm
    them.
    """
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("name_missing",))

    display = re.sub(r"\s+", " ", original.strip())
    key = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", display.lower())).strip()

    if not key:
        return Norm(None, original, ("name_invalid",), f"no usable characters: {original!r}")

    issues: list[str] = []
    if display != original:
        issues.append("name_whitespace_normalised")
    # An abbreviated forename, e.g. "R. Verma". Recorded so the survivorship
    # step knows to prefer the longer form when the records merge.
    if re.search(r"(^|\s)[A-Za-z]\.(\s|$)", display):
        issues.append("name_abbreviated")
    if display != display.title() and display.isupper():
        issues.append("name_case_unusual")

    return Norm(Name(display=display, key=key), original, tuple(issues))


# ---------------------------------------------------------------------------
# city
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class City:
    name: str
    is_region: bool


# Only the aliases actually present in the three files. Every key here was
# observed; none is speculative.
CITY_ALIASES: dict[str, str] = {
    "bangalore": "Bengaluru",
    "bengaluru": "Bengaluru",
    "gurgaon": "Gurugram",
    "gurugram": "Gurugram",
    "new delhi": "New Delhi",
    "delhi": "Delhi",
    "delhi ncr": "Delhi NCR",
    "noida": "Noida",
    "pune": "Pune",
}

# Not a city. Collapsing it to Delhi, Noida or Gurugram would be a guess that
# silently relocates people.
REGIONS = {"Delhi NCR"}


def normalize_city(raw: Any) -> Norm:
    """Fold casing, trailing whitespace and the two alias pairs.

    `Delhi` and `New Delhi` are kept distinct: they appear as separate values
    in the sources, and merging them is a judgement call that belongs to
    survivorship, not to staging.
    """
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("city_missing",))

    trimmed = original.strip()
    lookup = re.sub(r"\s+", " ", trimmed.lower())
    canonical = CITY_ALIASES.get(lookup)

    issues: list[str] = []
    if trimmed != original:
        issues.append("city_whitespace_trimmed")

    if canonical is None:
        # Unknown value: keep it rather than dropping it, but say so.
        return Norm(City(name=trimmed, is_region=False), original,
                    tuple(issues) + ("city_unknown",), f"not a known city: {trimmed!r}")

    if canonical != trimmed:
        issues.append("city_alias_applied" if lookup != canonical.lower()
                      else "city_case_normalised")

    is_region = canonical in REGIONS
    if is_region:
        issues.append("city_region_preserved")

    return Norm(City(name=canonical, is_region=is_region), original, tuple(issues))


# ---------------------------------------------------------------------------
# CTC
# ---------------------------------------------------------------------------

LAKH = 100_000
# Measured: 21 rows are absolute rupees (327,287-1,195,422) and 21 are lakhs
# (2.4-11.9). The two ranges do not overlap anywhere near this threshold.
LAKH_THRESHOLD = 100


def normalize_ctc(raw: Any) -> Norm:
    """Resolve the two units source1 mixes in one column.

    Rule from the profile: a value below 100 is lakhs, otherwise it is already
    rupees. On this dataset the split is exact - there is no value between 11.9
    and 327,287, so nothing is ambiguous.
    """
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("ctc_missing",))

    try:
        amount = float(original.strip().replace(",", ""))
    except ValueError:
        return Norm(None, original, ("ctc_unparseable",), f"not a number: {original!r}")

    if amount <= 0:
        return Norm(None, original, ("ctc_invalid",), f"non-positive: {original!r}")

    if amount < LAKH_THRESHOLD:
        return Norm((int(round(amount * LAKH)), "lakh"), original,
                    ("ctc_unit_lakh",), f"{amount} lakh -> {int(round(amount * LAKH))} INR")

    return Norm((int(round(amount)), "rupee"), original, ("ctc_unit_rupee",))


# ---------------------------------------------------------------------------
# rate
# ---------------------------------------------------------------------------

_RATE_HOUR = re.compile(r"^(\d+(?:\.\d+)?)\s*/\s*hr$", re.I)
_RATE_MONTH = re.compile(r"^(\d+(?:\.\d+)?)\s*k\s*/\s*month$", re.I)


def normalize_rate(raw: Any) -> Norm:
    """Keep the unit. Never convert monthly to hourly.

    The two scales do not reconcile: 15k/month is about Rs.94/hour at 160
    hours, against an observed hourly floor of Rs.330. Any conversion factor
    would be fabricated, so the unit travels with the number and a comparison
    is left to whoever can state their assumption out loud.

    `k/month` is expanded to whole rupees, so 15k/month is stored as 15000 with
    unit per_month. That is a unit expansion, not a unit conversion.
    """
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("rate_missing",))

    text = original.strip()

    if m := _RATE_HOUR.match(text):
        return Norm((float(m.group(1)), "per_hour"), original, ("rate_unit_hourly",))

    if m := _RATE_MONTH.match(text):
        return Norm((float(m.group(1)) * 1000, "per_month"), original,
                    ("rate_unit_monthly",), f"{m.group(1)}k/month -> {float(m.group(1)) * 1000} INR/month")

    return Norm(None, original, ("rate_unparseable",), f"unrecognised rate: {text!r}")


# ---------------------------------------------------------------------------
# applied date
# ---------------------------------------------------------------------------

# The four formats measured in source1, with the evidence that disambiguates
# the two numeric-with-separator ones:
#   slash values include 07/13, 08/16, 08/21  -> day > 12, so / is MM/DD/YYYY
#   dash  values include 21-08, 24-07, 28-07  -> first > 12, so - is DD-MM-YYYY
DATE_FORMATS: tuple[tuple[str, str, str], ...] = (
    (r"^\d{4}-\d{2}-\d{2}$",            "%Y-%m-%d", "YYYY-MM-DD"),
    (r"^\d{2}-\d{2}-\d{4}$",            "%d-%m-%Y", "DD-MM-YYYY"),
    (r"^\d{2}/\d{2}/\d{4}$",            "%m/%d/%Y", "MM/DD/YYYY"),
    (r"^\d{1,2} [A-Za-z]{3} \d{4}$",    "%d %b %Y", "D Mon YYYY"),
)

# Static sanity window. Not now(): a moving boundary would make a value that
# parsed yesterday fail today.
DATE_MIN = date(2000, 1, 1)
DATE_MAX = date(2100, 1, 1)


def normalize_date(raw: Any) -> Norm:
    """Parse by explicit format detection, and refuse anything unrecognised."""
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("date_missing",))

    text = original.strip()

    for pattern, fmt, label in DATE_FORMATS:
        if not re.match(pattern, text):
            continue
        try:
            parsed = datetime.strptime(text, fmt).date()
        except ValueError:
            # Matched the shape but is not a real date, e.g. 31-02-2026.
            return Norm(None, original, ("date_invalid",),
                        f"{label} shape but not a real date: {text!r}")
        if not (DATE_MIN <= parsed <= DATE_MAX):
            return Norm(None, original, ("date_out_of_range",), f"{parsed.isoformat()}")
        issues = ("date_format_" + label.lower().replace("/", "_").replace("-", "_").replace(" ", "_"),)
        return Norm(parsed, original, issues, label)

    return Norm(None, original, ("date_unrecognised_format",),
                f"no known format matches: {text!r}")


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

STATUS_MAP: dict[str, str] = {
    "active": "active",
    "inactive": "inactive",
    "paused": "paused",
}


def normalize_status(raw: Any) -> Norm:
    """3 real states, 5 spellings in source2.

    `Pune` sits in the status column of the un-repaired shifted row. It must
    NOT become a valid status - that is precisely the corruption we are trying
    to keep out of the clean layer.
    """
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("status_missing",))

    text = original.strip()
    canonical = STATUS_MAP.get(text.lower())

    if canonical is None:
        return Norm(None, original, ("status_unrecognised",), f"not a status: {text!r}")

    issues = () if text == canonical else ("status_case_normalised",)
    return Norm(canonical, original, issues)


# ---------------------------------------------------------------------------
# verified
# ---------------------------------------------------------------------------

VERIFIED_TRUE = {"y", "yes", "true", "1"}
VERIFIED_FALSE = {"n", "no", "false", "0"}


def normalize_verified(raw: Any) -> Norm:
    """Y / Yes / yes -> true, N / No -> false. `Verified` (the header row) -> None."""
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("verified_missing",))

    text = original.strip()
    low = text.lower()

    if low in VERIFIED_TRUE:
        value = True
    elif low in VERIFIED_FALSE:
        value = False
    else:
        return Norm(None, original, ("verified_unrecognised",), f"not a boolean: {text!r}")

    issues = () if text in ("Y", "N") else ("verified_spelling_normalised",)
    return Norm(value, original, issues)


# ---------------------------------------------------------------------------
# skills
# ---------------------------------------------------------------------------

# The 15 tokens that actually occur, case-folded. source1 writes them in
# TitleCase and source2 in lowercase; lowercasing makes the two agree exactly,
# which was verified across all 15 overlapping people.
CANONICAL_SKILLS: frozenset[str] = frozenset({
    "docker", "fastapi", "javascript", "langchain", "mongodb", "mysql", "n8n",
    "pandas", "python", "react", "rest apis", "selenium", "sql", "web scraping",
    "zapier",
})


def normalize_skills(raw: Any) -> Norm:
    """Split, lowercase, trim, dedupe, and check against the real vocabulary.

    Order is preserved on first appearance rather than sorted, so the stored
    list still reflects how the source wrote it.

    An unrecognised token is KEPT, not dropped, and flagged. Dropping would be
    silent data loss; inventing a canonical form for it would be worse. The only
    unknown token this dataset can produce is `active`, from the shifted row
    before repair.
    """
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm((), original, ("skills_missing",))

    seen: list[str] = []
    unknown: list[str] = []
    duplicates = 0
    changed = False

    for piece in original.split(","):
        token = re.sub(r"\s+", " ", piece.strip().lower())
        if not token:
            continue
        if token != piece.strip():
            changed = True
        if token in seen:
            duplicates += 1
            continue
        if token not in CANONICAL_SKILLS:
            unknown.append(token)
        seen.append(token)

    issues: list[str] = []
    if changed:
        issues.append("skills_case_normalised")
    if duplicates:
        issues.append("skills_duplicates_removed")
    if unknown:
        issues.append("skills_unknown_token")

    return Norm(tuple(seen), original, tuple(issues),
                f"unknown: {unknown}" if unknown else "")


# ---------------------------------------------------------------------------
# plain numerics
# ---------------------------------------------------------------------------


def normalize_experience(raw: Any) -> Norm:
    """Years, observed range 0.8 - 5.6."""
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("experience_missing",))
    try:
        years = float(original.strip())
    except ValueError:
        return Norm(None, original, ("experience_unparseable",), f"not a number: {original!r}")
    if not (0 <= years <= 60):
        return Norm(None, original, ("experience_out_of_range",), f"{years}")
    return Norm(years, original)


def normalize_projects(raw: Any) -> Norm:
    """Completed project count. Zero is a real value, not a missing one."""
    original = "" if raw is None else str(raw)
    if _blank(raw):
        return Norm(None, original, ("projects_missing",))
    try:
        count = int(original.strip())
    except ValueError:
        return Norm(None, original, ("projects_unparseable",), f"not an integer: {original!r}")
    if count < 0:
        return Norm(None, original, ("projects_invalid",), f"negative: {count}")
    return Norm(count, original)
