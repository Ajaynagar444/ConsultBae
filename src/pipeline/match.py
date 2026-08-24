"""Cross-source person matching: staged_person -> person + provenance.

Deterministic-first, conservative, auditable. Four passes, in order:

  1. exact normalised email      (confidence 1.00)
  2. exact normalised phone      (confidence 1.00)
  3. guarded name-only merge     (confidence 0.85) - five explicit conditions
  4. anything unresolved         -> match_review, never a forced merge

Name equality alone NEVER merges two records. Neither do city, skills, status,
rate or CTC - those fields support review; they do not prove identity.

The one distinctness axiom, stated once and used everywhere: after passes 1-2,
two clusters that each still contain a row from the SAME source system are
different people. Within one system, rows that share no identifier are separate
accounts - every real same-person duplicate in this data (R. Verma, Nikhil
Chopra, the repaired Isha Chopra row) shares an email or phone with its twin
and has therefore already been merged by the time this axiom is consulted.
That axiom is what keeps the two Deepak Nairs apart (both have gig_workers
rows) and the two source3 Arjun Mehtas apart, while still allowing the
source2-only Arjun Mehta row to be queued for human review against both.

Confidences are deterministic constants tied to the kind of evidence, not
probabilities: an exact identifier is 1.00; a guarded name merge is 0.85
because the guard is strong but the evidence is still only a name; a review
pair is written at 0.50 - genuinely undecidable from this data.

    python -m src.pipeline.match
    python -m src.pipeline.match --run-id 3
    python -m src.pipeline.match --dry-run
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Sequence

import psycopg

from .config import database_url, safe_dsn
from .logging_setup import configure, get_logger
from .stage import latest_run

log = get_logger("match")

CONFIDENCE = {
    "exact_email": 1.00,
    "exact_phone": 1.00,
    "guarded_name": 0.85,
    "unmatched": 1.00,   # the row is trivially its own person
}
REVIEW_CONFIDENCE = 0.50

# Survivorship rule 1: prefer the source holding the deciding identity keys.
# source1 carries both email and phone, so it outranks the others.
SOURCE_PRIORITY = {"naukri_applicants": 0, "gig_workers": 1, "cbnexus_contacts": 2}


# ---------------------------------------------------------------------------
# input row
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Row:
    raw_record_id: int
    source_system: str
    source_line_no: int
    full_name: str | None
    name_key: str | None
    email: str | None
    phone: str | None
    city: str | None
    is_region: bool
    experience: float | None
    ctc: int | None
    ctc_unit: str | None
    rate: float | None
    rate_unit: str | None
    status: str | None
    verified: bool | None
    projects: int | None
    applied_on: object
    skills: tuple[str, ...]
    was_repaired: bool

    @property
    def priority(self) -> tuple[int, int]:
        return (SOURCE_PRIORITY[self.source_system], self.source_line_no)


# ---------------------------------------------------------------------------
# union-find
# ---------------------------------------------------------------------------


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # Deterministic: the smaller root wins, so cluster identity does not
            # depend on iteration order.
            lo, hi = sorted((ra, rb))
            self.parent[hi] = lo


# ---------------------------------------------------------------------------
# result
# ---------------------------------------------------------------------------


@dataclass
class MatchResult:
    run_id: int
    staged_total: int = 0
    quarantined_excluded: int = 0
    rows_matched: int = 0
    people: int = 0
    clusters_before_guarded: int = 0
    by_method: Counter = field(default_factory=Counter)
    guarded_merges: list[str] = field(default_factory=list)
    review_pairs: list[str] = field(default_factory=list)
    cluster_shapes: Counter = field(default_factory=Counter)
    cluster_sizes: Counter = field(default_factory=Counter)
    city_conflicts: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


# ---------------------------------------------------------------------------
# passes 1-2: exact identifiers
# ---------------------------------------------------------------------------


def cluster_by_identifiers(rows: list[Row]) -> UnionFind:
    uf = UnionFind()
    by_email: dict[str, list[Row]] = defaultdict(list)
    by_phone: dict[str, list[Row]] = defaultdict(list)

    for row in rows:
        uf.find(row.raw_record_id)
        if row.email:
            by_email[row.email].append(row)
        if row.phone:
            by_phone[row.phone].append(row)

    # Pass 1: exact normalised email.
    for group in by_email.values():
        for other in group[1:]:
            uf.union(group[0].raw_record_id, other.raw_record_id)

    # Pass 2: exact normalised phone. Transitive with pass 1 through the
    # union-find, which is what produces the three-way s1+s2+s3 clusters.
    for group in by_phone.values():
        for other in group[1:]:
            uf.union(group[0].raw_record_id, other.raw_record_id)

    return uf


def clusters_of(uf: UnionFind, rows: list[Row]) -> dict[int, list[Row]]:
    out: dict[int, list[Row]] = defaultdict(list)
    for row in rows:
        out[uf.find(row.raw_record_id)].append(row)
    for members in out.values():
        members.sort(key=lambda r: r.priority)
    return dict(out)


# ---------------------------------------------------------------------------
# pass 3: the guarded name-only merge
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GuardVerdict:
    allowed: bool
    reason: str


def guarded_name_verdict(
    a: list[Row],
    b: list[Row],
    name_key: str,
    clusters_bearing_name: int,
    name_count_per_source: dict[str, int],
    bridge_source: str = "naukri_applicants",
) -> GuardVerdict:
    """The five conditions, each named and each individually testable.

    A name-only merge of clusters a and b is allowed ONLY if all five hold.
    """
    # 1. identical name_key - guaranteed by construction, asserted anyway.
    if not (any(r.name_key == name_key for r in a) and any(r.name_key == name_key for r in b)):
        return GuardVerdict(False, "condition 1 failed: name_key not identical")

    # 2. the name occurs exactly once in each source it appears in at all.
    dupes = {s: n for s, n in name_count_per_source.items() if n > 1}
    if dupes:
        return GuardVerdict(False, f"condition 2 failed: name not unique per source: {dupes}")

    # 3. no conflicting identifier evidence. Two parts:
    #    (a) the distinctness axiom - a shared source system means two accounts;
    #    (b) both sides carrying the same identifier type with different values
    #        (had they matched, passes 1-2 would already have merged them).
    if {r.source_system for r in a} & {r.source_system for r in b}:
        return GuardVerdict(False, "condition 3 failed: both clusters contain rows from the same source")
    if {r.email for r in a if r.email} and {r.email for r in b if r.email}:
        return GuardVerdict(False, "condition 3 failed: both clusters carry emails that did not match")
    if {r.phone for r in a if r.phone} and {r.phone for r in b if r.phone}:
        return GuardVerdict(False, "condition 3 failed: both clusters carry phones that did not match")

    # 4. the name is not part of an ambiguous constellation.
    if clusters_bearing_name != 2:
        return GuardVerdict(False,
                            f"condition 4 failed: {clusters_bearing_name} clusters bear this name, not 2")

    # 5. the name is absent from the bridge source, where email/phone evidence
    #    could otherwise have existed and is conspicuously missing.
    if name_count_per_source.get(bridge_source, 0) > 0:
        return GuardVerdict(False, "condition 5 failed: name present in the bridge source")

    return GuardVerdict(True, "all five guard conditions hold")


def apply_guarded_name_pass(
    uf: UnionFind, rows: list[Row], result: MatchResult
) -> tuple[set[int], list[tuple[Row, Row, str]]]:
    """Returns (raw ids merged by name, review pairs as (anchor_a, anchor_b, reason))."""
    clusters = clusters_of(uf, rows)
    result.clusters_before_guarded = len(clusters)

    name_to_clusters: dict[str, list[int]] = defaultdict(list)
    name_count_per_source: dict[str, Counter] = defaultdict(Counter)
    for root, members in clusters.items():
        for r in members:
            if r.name_key:
                if root not in name_to_clusters[r.name_key]:
                    name_to_clusters[r.name_key].append(r.name_key and root)
                name_count_per_source[r.name_key][r.source_system] += 1

    merged_by_name: set[int] = set()
    review: list[tuple[Row, Row, str]] = []

    for name_key in sorted(name_to_clusters):          # sorted -> deterministic
        roots = sorted(set(name_to_clusters[name_key]))
        if len(roots) < 2:
            continue

        counts = dict(name_count_per_source[name_key])

        # Evaluate every cross-cluster pair bearing this name.
        for i in range(len(roots)):
            for j in range(i + 1, len(roots)):
                a, b = clusters[roots[i]], clusters[roots[j]]
                verdict = guarded_name_verdict(a, b, name_key, len(roots), counts)

                if verdict.allowed:
                    uf.union(roots[i], roots[j])
                    for r in a + b:
                        merged_by_name.add(r.raw_record_id)
                    result.guarded_merges.append(
                        f"{name_key!r}: {a[0].source_system} line {a[0].source_line_no}"
                        f" + {b[0].source_system} line {b[0].source_line_no}"
                    )
                    log.info("guarded name merge", extra={"ctx": {
                        "name": name_key,
                        "a": f"{a[0].source_system}:{a[0].source_line_no}",
                        "b": f"{b[0].source_system}:{b[0].source_line_no}"}})
                    continue

                # Not mergeable. Distinct, or reviewable?
                shared_source = bool({r.source_system for r in a} & {r.source_system for r in b})
                if shared_source:
                    # The distinctness axiom: positively two people. No review.
                    log.info("name collision resolved as distinct", extra={"ctx": {
                        "name": name_key, "why": "same source system on both sides"}})
                    continue

                reason = (
                    f"name {name_key!r} matches but cannot be proven: {verdict.reason}. "
                    f"A={a[0].source_system} line {a[0].source_line_no}"
                    f" (email={a[0].email or '-'}, phone={a[0].phone or '-'}); "
                    f"B={b[0].source_system} line {b[0].source_line_no}"
                    f" (email={b[0].email or '-'}, phone={b[0].phone or '-'})"
                )
                review.append((a[0], b[0], reason))
                result.review_pairs.append(reason)
                log.warning("ambiguous match queued for review", extra={"ctx": {
                    "name": name_key,
                    "a": f"{a[0].source_system}:{a[0].source_line_no}",
                    "b": f"{b[0].source_system}:{b[0].source_line_no}"}})

    return merged_by_name, review


# ---------------------------------------------------------------------------
# link methods
# ---------------------------------------------------------------------------


def method_for_row(row: Row, members: list[Row], merged_by_name: set[int]) -> str:
    """The evidence that put this row into its cluster."""
    others = [m for m in members if m.raw_record_id != row.raw_record_id]
    if not others:
        return "unmatched"
    if row.email and any(m.email == row.email for m in others):
        return "exact_email"
    if row.phone and any(m.phone == row.phone for m in others):
        return "exact_phone"
    if row.raw_record_id in merged_by_name:
        return "guarded_name"
    return "unmatched"


# ---------------------------------------------------------------------------
# survivorship
# ---------------------------------------------------------------------------

# City specificity for rule 4: a region is least specific; bare "Delhi" is
# broader than "New Delhi"; everything else is a proper city.
def _city_specificity(city: str, is_region: bool) -> int:
    if is_region:
        return 0
    if city == "Delhi":
        return 1
    return 2


@dataclass
class Golden:
    full_name: str
    name_key: str
    primary_email: str | None
    primary_phone: str | None
    emails: list[str]
    phones: list[str]
    city: str | None
    is_region: bool
    city_conflict: list[str]
    experience: float | None
    ctc: int | None
    rate: float | None
    rate_unit: str | None
    status: str | None
    verified: bool | None
    projects: int | None
    applied_on: object
    skills: list[str]


def _email_rank(email: str, holders: list[Row]) -> tuple:
    """Primary-email choice, documented in data-profile.md section 7:
    an address whose local part starts with 'alt.' is secondary. Then prefer
    the deciding-key source, then earliest line, then the value itself so the
    order is total and deterministic."""
    is_alt = email.split("@")[0].startswith("alt.")
    best = min((r.priority for r in holders), default=(9, 9))
    return (is_alt, best, email)


def survivorship(members: list[Row]) -> Golden:
    """Fold one cluster into one golden record. members arrive priority-sorted."""
    def first(attr: str):
        # Rule 3: non-null beats null; rule 1 via the priority sort.
        for r in members:
            v = getattr(r, attr)
            if v is not None:
                return v
        return None

    # Rule 2: longest complete name wins ('Rohit Verma' over 'R. Verma').
    # Tie-break on source priority, then the name itself, for determinism.
    named = [r for r in members if r.full_name]
    best_name = min(named, key=lambda r: (-len(r.full_name), r.priority, r.full_name))

    emails_holders: dict[str, list[Row]] = defaultdict(list)
    phones_holders: dict[str, list[Row]] = defaultdict(list)
    for r in members:
        if r.email:
            emails_holders[r.email].append(r)
        if r.phone:
            phones_holders[r.phone].append(r)

    emails = sorted(emails_holders, key=lambda e: _email_rank(e, emails_holders[e]))
    phones = sorted(phones_holders, key=lambda p: (min(r.priority for r in phones_holders[p]), p))

    # Rule 4: city - most specific non-region value wins; conflict recorded.
    cities: dict[str, bool] = {}
    for r in members:
        if r.city and r.city not in cities:
            cities[r.city] = r.is_region
    city = None
    is_region = False
    conflict: list[str] = []
    if cities:
        ranked = sorted(
            cities.items(),
            key=lambda kv: (
                -_city_specificity(kv[0], kv[1]),
                min(r.priority for r in members if r.city == kv[0]),
                kv[0],
            ),
        )
        city, is_region = ranked[0]
        if len(cities) > 1:
            conflict = [c for c, _ in ranked]

    # Skills: union, first-seen order across priority-sorted members.
    skills: list[str] = []
    for r in members:
        for s in r.skills:
            if s not in skills:
                skills.append(s)

    return Golden(
        full_name=best_name.full_name,
        name_key=best_name.name_key,
        primary_email=emails[0] if emails else None,
        primary_phone=phones[0] if phones else None,
        emails=emails,
        phones=phones,
        city=city,
        is_region=is_region,
        city_conflict=conflict,
        experience=first("experience"),
        ctc=first("ctc"),
        rate=first("rate"),
        rate_unit=first("rate_unit"),
        status=first("status"),
        verified=first("verified"),
        projects=first("projects"),
        applied_on=first("applied_on"),
        skills=skills,
    )


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------


def load_rows(conn: psycopg.Connection, run_id: int) -> tuple[list[Row], int, int]:
    records = conn.execute(
        """SELECT s.raw_record_id, s.source_system::text, r.source_line_no,
                  s.full_name, s.name_key, s.email_norm, s.phone_e164,
                  s.city_norm, s.is_region, s.experience_years, s.ctc_annual_inr,
                  s.ctc_source_unit::text, s.rate_amount, s.rate_source_unit::text,
                  s.status::text, s.is_verified, s.projects_completed,
                  s.applied_on, s.skills, s.was_repaired, s.is_quarantined
             FROM staged_person s
             JOIN raw_record r ON r.id = s.raw_record_id
            WHERE r.run_id = %s
            ORDER BY s.source_system, r.source_line_no""",
        (run_id,),
    ).fetchall()

    rows: list[Row] = []
    quarantined = 0
    for rec in records:
        (raw_id, src, line, full_name, name_key, email, phone, city, is_region,
         exp, ctc, ctc_unit, rate, rate_unit, status, verified, projects,
         applied, skills, repaired, is_q) = rec
        if is_q:
            quarantined += 1
            continue
        rows.append(Row(
            raw_record_id=raw_id, source_system=src, source_line_no=line,
            full_name=full_name, name_key=name_key, email=email, phone=phone,
            city=city, is_region=is_region,
            experience=float(exp) if exp is not None else None,
            ctc=ctc, ctc_unit=ctc_unit,
            rate=float(rate) if rate is not None else None,
            rate_unit=rate_unit, status=status, verified=verified,
            projects=projects, applied_on=applied,
            skills=tuple(skills or ()), was_repaired=repaired,
        ))
    return rows, len(records), quarantined


SHAPE_LABEL = {"naukri_applicants": "s1", "gig_workers": "s2", "cbnexus_contacts": "s3"}

# data_issue codes owned by this module; deleted and re-emitted on rerun.
MATCH_ISSUE_CODES = ("intra_source_duplicate", "city_conflict_across_sources",
                     "name_ambiguous_review")


def run_matching(conn: psycopg.Connection, run_id: int, dry_run: bool = False) -> MatchResult:
    """Match one staged run into golden person records. Caller owns the txn."""
    result = MatchResult(run_id=run_id)
    rows, staged_total, quarantined = load_rows(conn, run_id)
    result.staged_total = staged_total
    result.quarantined_excluded = quarantined
    result.rows_matched = len(rows)
    if not rows:
        raise ValueError(f"run {run_id} has no staged rows - run staging first")

    uf = cluster_by_identifiers(rows)
    merged_by_name, review_pairs = apply_guarded_name_pass(uf, rows, result)
    clusters = clusters_of(uf, rows)
    result.people = len(clusters)

    for members in clusters.values():
        shape = "+".join(sorted({SHAPE_LABEL[r.source_system] for r in members},
                                key=lambda s: s))
        result.cluster_shapes[shape] += 1
        result.cluster_sizes[len(members)] += 1
        for r in members:
            result.by_method[method_for_row(r, members, merged_by_name)] += 1

    if dry_run:
        return result

    # Rebuild the golden layer for a deterministic, idempotent result. person
    # cascades to person_email/person_phone/person_skill/person_source_link.
    conn.execute("DELETE FROM person")
    conn.execute("DELETE FROM match_review WHERE run_id = %s", (run_id,))
    conn.execute(
        "DELETE FROM data_issue WHERE run_id = %s AND issue_code = ANY(%s)",
        (run_id, list(MATCH_ISSUE_CODES)),
    )

    skill_ids: dict[str, int] = {}

    def skill_id(name: str) -> int:
        if name not in skill_ids:
            conn.execute(
                "INSERT INTO skill (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (name,))
            skill_ids[name] = conn.execute(
                "SELECT id FROM skill WHERE name = %s", (name,)).fetchone()[0]
        return skill_ids[name]

    def issue(raw_id: int, source: str, code: str, severity: str,
              detail: str, action: str) -> None:
        conn.execute(
            """INSERT INTO data_issue (run_id, raw_record_id, source_system,
                                       issue_code, severity, detail, action_taken)
               VALUES (%s, %s, %s::source_system, %s, %s::issue_severity, %s, %s)""",
            (run_id, raw_id, source, code, severity, detail, action))

    # Deterministic person order: by anchor (lowest raw id) of each cluster.
    for root in sorted(clusters, key=lambda r: min(m.raw_record_id for m in clusters[r])):
        members = clusters[root]
        g = survivorship(members)

        person_id = conn.execute(
            """INSERT INTO person (full_name, name_key, primary_email,
                                   primary_phone_e164, city, is_region,
                                   experience_years, ctc_annual_inr,
                                   rate_amount, rate_source_unit, status,
                                   is_verified, projects_completed, applied_on)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::rate_unit,
                       %s::gig_status, %s, %s, %s)
               RETURNING id""",
            (g.full_name, g.name_key, g.primary_email, g.primary_phone,
             g.city, g.is_region, g.experience, g.ctc, g.rate, g.rate_unit,
             g.status, g.verified, g.projects, g.applied_on),
        ).fetchone()[0]

        for email in g.emails:
            conn.execute(
                "INSERT INTO person_email (person_id, email, is_primary) VALUES (%s, %s, %s)",
                (person_id, email, email == g.primary_email))
        for phone in g.phones:
            conn.execute(
                "INSERT INTO person_phone (person_id, phone_e164, is_primary) VALUES (%s, %s, %s)",
                (person_id, phone, phone == g.primary_phone))
        for s in g.skills:
            conn.execute(
                "INSERT INTO person_skill (person_id, skill_id) VALUES (%s, %s)",
                (person_id, skill_id(s)))
        for r in members:
            method = method_for_row(r, members, merged_by_name)
            conn.execute(
                """INSERT INTO person_source_link
                       (person_id, raw_record_id, method, confidence)
                   VALUES (%s, %s, %s::match_method, %s)""",
                (person_id, r.raw_record_id, method, CONFIDENCE[method]))

        # Task 4 evidence: intra-source duplicates that this merge resolved.
        per_source = Counter(r.source_system for r in members)
        for source, n in per_source.items():
            if n > 1:
                dupes = [r for r in members if r.source_system == source]
                shared_email = len({r.email for r in dupes if r.email}) == 1 and dupes[0].email
                shared_phone = len({r.phone for r in dupes if r.phone}) == 1 and dupes[0].phone
                evidence = ("exact normalized email and phone" if shared_email and shared_phone
                            else "exact normalized email" if shared_email
                            else "exact normalized phone")
                issue(dupes[0].raw_record_id, source, "intra_source_duplicate", "warning",
                      f"{source} lines {[r.source_line_no for r in dupes]} are one person "
                      f"({g.full_name}); evidence: {evidence}",
                      "merged into one person; every source row preserved and linked")

        if g.city_conflict:
            result.city_conflicts += 1
            issue(members[0].raw_record_id, members[0].source_system,
                  "city_conflict_across_sources", "warning",
                  f"{g.full_name}: sources disagree on city: {g.city_conflict}",
                  f"kept most specific non-region value {g.city!r}; conflict recorded")

    # Pass 4: the review queue.
    for a, b, reason in review_pairs:
        lo, hi = sorted((a.raw_record_id, b.raw_record_id))
        conn.execute(
            """INSERT INTO match_review (run_id, raw_record_id_a, raw_record_id_b,
                                         reason, confidence)
               VALUES (%s, %s, %s, %s, %s)""",
            (run_id, lo, hi, reason, REVIEW_CONFIDENCE))
        issue(lo, a.source_system, "name_ambiguous_review", "warning",
              reason, "queued in match_review; not auto-merged")

    log.info("matching complete", extra={"ctx": {
        "run_id": run_id, "people": result.people,
        "before_guarded": result.clusters_before_guarded,
        "review_pairs": len(review_pairs)}})
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _report(r: MatchResult, dry_run: bool) -> None:
    print()
    print(f"  ingestion run              : {r.run_id}")
    print(f"  staged records             : {r.staged_total}")
    print(f"  quarantined (excluded)     : {r.quarantined_excluded}")
    print(f"  records entering matching  : {r.rows_matched}")
    print(f"  clusters before guarded    : {r.clusters_before_guarded}")
    print(f"  guarded name merges        : {len(r.guarded_merges)}")
    print(f"  canonical people           : {r.people}")
    print(f"  ambiguous pairs for review : {len(r.review_pairs)}")
    print(f"  city conflicts recorded    : {r.city_conflicts}")

    print(f"\n  {'link method':<16}rows")
    print("  " + "-" * 22)
    for method in ("exact_email", "exact_phone", "guarded_name", "unmatched"):
        print(f"  {method:<16}{r.by_method.get(method, 0)}")

    print(f"\n  {'cluster shape':<12}people")
    print("  " + "-" * 20)
    for shape, n in sorted(r.cluster_shapes.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {shape:<12}{n}")

    print(f"\n  {'cluster size':<14}count")
    print("  " + "-" * 20)
    for size, n in sorted(r.cluster_sizes.items()):
        print(f"  {size:<14}{n}")

    if r.guarded_merges:
        print("\n  guarded name merges:")
        for m in r.guarded_merges:
            print(f"    - {m}")
    if r.review_pairs:
        print("\n  queued for human review:")
        for m in r.review_pairs:
            print(f"    - {m}")
    if dry_run:
        print("\n  dry run - nothing written")


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m src.pipeline.match",
        description="Deterministic cross-source person matching into the golden layer.",
    )
    p.add_argument("--run-id", type=int, help="ingestion run to match (default: latest)")
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
                log.error("no ingested run found")
                return 2
            result = run_matching(conn, run_id, dry_run=args.dry_run)
            if args.dry_run:
                conn.rollback()
    except psycopg.OperationalError as exc:
        log.error("cannot connect", extra={"ctx": {"error": str(exc)}})
        return 2
    except ValueError as exc:
        log.error("matching aborted", extra={"ctx": {"error": str(exc)}})
        return 2

    _report(result, args.dry_run)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
