# Data Profile — 3 source CSVs

Produced before any code was written, by profiling the files directly (`csv`
module + set arithmetic + union-find). Every number below is measured, not
estimated. This document is the evidence base for the matching strategy in
Task 1 and for the data-issues report in Task 4.

Source files, committed verbatim under `data/raw/`:

| File | SHA-256 (first 16) | Bytes |
|---|---|---|
| `source1_naukri_applicants.csv` | `ff27b1c3f4d702c5` | 5,296 |
| `source2_gig_workers.csv`       | `7d3dd7f1a3eda010` | 3,415 |
| `source3_cbnexus_contacts.csv`  | `e615a893e8676d62` | 1,269 |

All three: CRLF line endings, pure ASCII, no BOM, no ragged rows. The corruption
in this dataset is **semantic, not structural** — every file parses cleanly with
a stock CSV reader, which is exactly why the problems are easy to miss.

---

## 1. Source schemas

### `source1_naukri_applicants.csv` — recruitment ATS (42 data rows × 8 cols)

| Column | Type | Notes |
|---|---|---|
| `Full Name` | text | one initialised form (`R. Verma`) |
| `Email` | text | identifier; lowercase here, mixed case in source 2 |
| `Phone` | text | 3 formats: `+919…`, `9…`, `09…` |
| `City` | text | free text; casing + trailing spaces |
| `Experience (Years)` | decimal | 0.8 – 5.6 |
| `Current CTC` | **mixed unit** | 21 rows in rupees, 21 rows in lakhs |
| `Applied Date` | **4 formats** | see issue 10 |
| `Skills` | comma list | TitleCase vocabulary |

### `source2_gig_workers.csv` — gig marketplace (32 data rows × 6 cols)

| Column | Type | Notes |
|---|---|---|
| `email_id` | text | **only** identifier; 9 rows ALL-CAPS |
| `worker_name` | text | |
| `rate` | **mixed unit** | 16 × `N/hr`, 14 × `Nk/month` |
| `location` | text | casing + trailing spaces |
| `status` | text | 3 real states in 5 spellings |
| `skill_tags` | comma list | all-lowercase |

**No phone column.**

### `source3_cbnexus_contacts.csv` — CBNexus CRM (31 data rows × 5 cols)

| Column | Type | Notes |
|---|---|---|
| `Name` | text | mixed Title Case / ALL CAPS |
| `Phone Number` | text | **only** identifier; 3 formats: `9…`, `919…`, `+91-9…` |
| `City` | text | casing + trailing spaces |
| `Verified` | text | boolean in 5 spellings |
| `Projects Completed` | integer | 0 – 15, complete |

**No email column.**

### The structural consequence

```
  source1  =  email + phone     <-- the only bridge
  source2  =  email          (no phone)
  source3  =          phone  (no email)
```

Source 1 is the **only** file holding both identifier types. Any person present
in sources 2 and 3 but absent from source 1 has **no shared key at all** and can
only be linked by name. That is where the planted ambiguity lives.

---

## 2. Row counts

| File | Lines | Header | Data rows | Junk rows | Usable | Distinct people |
|---|---|---|---|---|---|---|
| source1 | 43 | 1 | 42 | 0 | 42 | **40** (2 intra-file duplicate pairs) |
| source2 | 33 | 1 | 32 | 1 blank + 1 shifted-duplicate | 30 | **30** |
| source3 | 32 | 1 | 31 | 1 embedded header | 30 | **30** |
| | | | **105** | 3 | **102** | **100 source-level** |

### Resolved entity count

Union-find over normalised **email + phone only** (no name matching), after
dropping the blank row and the embedded header:

| Cluster shape | Count |
|---|---|
| source1 + source2 + source3 | 15 |
| source1 + source3 | 10 |
| source1 only | 15 |
| source2 only | 15 (after repairing the shifted row) |
| source3 only | 5 |
| **Total** | **60** |

Then 4 safe name-only merges (section 8): **60 → 56 unique people.**

Two facts worth noting: there are **zero** source1+source2 clusters that are not
also in source3, and all 5 source3-only records have a source2-only name twin.

---

## 3. Missing values

Genuinely sparse data is almost absent — the traps are corruption, not nulls.

- **source1: zero blanks in any column, all 42 rows.**
- **source2:** exactly one fully blank row (line 12) — all 6 fields empty.
- **source3: zero blanks**, but line 16 is a repeated header row posing as data,
  which pollutes every column's value domain.
- **Structurally missing by design:** source2 has no phone (30 records), source3
  has no email (30 records), source1 has no source ID. 15 source2-only people
  have no phone anywhere in the corpus; 5 source3-only people have no email
  anywhere.
- source3 line 9 (`SAHIL MALHOTRA`) has `Projects Completed = 0`. That is a
  **legitimate zero, not a null** — it must not be coerced.

---

## 4. Structural corruption

Three rows are not data at all:

**1. Column-shift corruption — source2 line 20.** Fields are rotated one
position to the right:

```
expected: email_id | worker_name | rate        | location | status | skill_tags
actual:   "react,  | ISHA.CHOPRA | Isha Chopra | 1406/hr  | Pune   | active
           js,     | 95@...ORG   |             |          |        |
           mysql"  |             |             |          |        |
          ^skills  | ^email      | ^name       | ^rate    | ^city  | ^status
```

Detectable without guessing: `email_id` contains no `@`, and `worker_name` does.
Once repaired, the row is an **exact duplicate of source2 line 7** (Isha Chopra)
— so the corruption hides a duplicate.

**2. Embedded header row — source3 line 16.** A verbatim repeat of the header.
This is what injects the bogus value `Verified` into the `Verified` column and
`City` into the city domain.

**3. Fully blank row — source2 line 12.**

Handling: all three are **quarantined in the staging layer, not deleted** —
flagged with a reason and left queryable, so the pipeline's own output is the
audit trail.

---

## 5. Data-quality issues (16 classes)

### Structural

| # | Issue | Where | Action |
|---|---|---|---|
| 1 | Column-shift corruption | source2 line 20 | Detect via `@` position, repair, then dedupe against line 7 |
| 2 | Embedded header row | source3 line 16 | Quarantine |
| 3 | Fully blank row | source2 line 12 | Quarantine |

### Identity / formatting

| # | Issue | Where | Action |
|---|---|---|---|
| 4 | 3 phone formats per file (`+919…`/`9…`/`09…`; `9…`/`919…`/`+91-9…`) | source1, source3 | Normalise to 10-digit national + E.164. All 72 real numbers land on exactly 10 digits |
| 5 | ALL-CAPS emails | 9 rows in source2 | `trim().lower()` before any comparison |
| 6 | Mixed name casing (`Rohit Nair` vs `RITU SHARMA`) | source3 | Title Case for display; separate `name_key` for blocking |
| 7 | Abbreviated name (`R. Verma` = `Rohit Verma`) | source1 lines 25, 31 | Survivorship: longest complete name wins |

### Unit / semantic

| # | Issue | Detail | Action |
|---|---|---|---|
| 8 | `Current CTC` mixes two units | 21 rows absolute rupees (327,287–1,195,422), 21 rows lakhs (2.4–11.9). Splits perfectly on "has a decimal point"; under a `value < 100 ⇒ lakhs` rule there are **zero ambiguous values**, and post-conversion the range (3.27L–11.95L) is coherent | Store `ctc_annual_inr` + `ctc_source_unit` |
| 9 | `rate` mixes two units | 16 × `/hr` (330–1,483), 14 × `k/month` (15–79). These do **not** reconcile: 15k/month is ≈₹94/hr at 160h, against an hourly floor of ₹330 | **Do not convert** — that would be inventing data. Store `rate_amount` + `rate_unit` |
| 10 | `Applied Date` in 4 formats | `DD-MM-YYYY` (12), `MM/DD/YYYY` (11), `YYYY-MM-DD` (9), `D Mon YYYY` (10) | Per-format parser, see section 6 |

### Categorical

| # | Issue | Raw values | Action |
|---|---|---|---|
| 11 | `status` — 3 states, 5 spellings | `Active`, `ACTIVE`, `active`, `Inactive`, `paused` (+ `Pune` from the shifted row) | Enum `active`/`inactive`/`paused` |
| 12 | `Verified` — boolean, 5 spellings | `Y`, `Yes`, `yes`, `N`, `No` (+ `Verified` from the header row) | Boolean |
| 13 | City — 20 distinct strings for ~6 cities | casing (`NOIDA`/`Noida`/`noida`), trailing whitespace (14 cells across all 3 files), and the alias pairs `Bengaluru`/`Bangalore` + `Gurgaon`/`Gurugram` | Alias map, see section 6 |
| 14 | `Delhi NCR` is a region, not a city | cannot be safely collapsed to Delhi, Noida or Gurugram | Keep verbatim, set `is_region = true` |

### Cross-source consistency

| # | Issue | Detail | Action |
|---|---|---|---|
| 15 | City disagreements for 5 confirmed-same people | Meera Bhatia = `Delhi NCR` (s1) / `New Delhi` (s2) / `Delhi` (s3); Arjun Mishra = `Delhi`/`Delhi`/`New Delhi`; also Rahul Malhotra, Priya Saxena, Isha Kapoor | Survivorship rule 4; log a `data_issue` either way |
| 16 | source2 `skill_tags` is a case-folded **copy** of source1 `Skills` | Verified byte-identical after lowercasing for all 15 overlapping people, **0 differences** | source2 skills carry no independent information for people already in source1; they are new data only for the 15 source2-only workers. Worth stating rather than claiming false enrichment |

Checks that **passed** — recorded so they are not re-litigated: field counts are
uniform in every file; no non-ASCII bytes; no BOM; no exact duplicate rows
within any file; every phone normalises to 10 digits; all dates fall between
2 Jun and 22 Aug 2026 with **none in the future**; every email/phone match
across sources agrees on name.

---

## 6. Normalisation rules

| Field | Rule |
|---|---|
| Phone | strip non-digits → drop leading `91` (len 12) or `0` (len 11) → store 10-digit national + `+91` E.164. Verified: 72/72 land on 10 digits |
| Email | `trim().lower()` |
| Name | trim, collapse internal whitespace, Title Case for display; store `name_key` (lowercase, punctuation-stripped) for blocking only |
| City | trim → lower → alias map (`bangalore → Bengaluru`, `gurgaon → Gurugram`) → Title Case. `Delhi NCR` kept verbatim with `is_region = true` |
| CTC | `value < 100 ⇒ lakhs`, multiply by 100,000. Store `ctc_annual_inr` (bigint) + `ctc_source_unit` |
| Rate | **do not unify.** Store `rate_amount` (numeric) + `rate_unit` (`per_hour`/`per_month`). Any comparable figure is derived in a view with the assumption stated |
| Date | per-format detection, in order. Reject unmatched input rather than guessing |
| Status | → `active` / `inactive` / `paused` |
| Verified | `Y`/`Yes`/`yes` → true; `N`/`No` → false |
| Skills | lowercase, trim, dedupe → canonical vocabulary of 15 tokens |

### Date disambiguation

Individually, `07/03/2026` and `03-07-2026` are ambiguous. Each **format class**
is disambiguated by evidence across the column:

- Slash values include `07/13`, `08/16`, `08/21` — day > 12, so `/` is **MM/DD/YYYY**.
- Dash values include `21-08`, `24-07`, `28-07` — first component > 12, so `-`
  with a 2-digit lead is **DD-MM-YYYY**.
- `YYYY-MM-DD` and `D Mon YYYY` are unambiguous on their face.

The rule is derived from the data, not assumed. Sanity check: all 42 parsed
dates land in Jun–Aug 2026 and none is in the future.

### Canonical skill vocabulary (15 tokens)

`docker`, `fastapi`, `javascript`, `langchain`, `mongodb`, `mysql`, `n8n`,
`pandas`, `python`, `react`, `rest apis`, `selenium`, `sql`, `web scraping`,
`zapier`

A 16th token, `active`, appears only as an artefact of the shifted row
(issue 1) and must be dropped, not added to the vocabulary.

---

## 7. Intra-file duplicates

| Where | Rows | Evidence | Verdict |
|---|---|---|---|
| source1 | 25 + 31 | identical email, phone, CTC, date and skills; names `R. Verma` / `Rohit Verma` | **Same person.** Survivor: the longer name |
| source1 | 27 + 37 | identical phone `9000000103`, city, CTC, date and skills; emails `alt.nikhil.chopra70@example.com` / `nikhil.chopra70@example.com` | **Same person with two emails.** Keep both in `person_email`; primary is the non-`alt.` address |
| source2 | 7 + 20 | line 20 is the column-shifted copy of line 7 | **Same record.** Repair, then dedupe |

The `Nikhil Chopra` pair is the reason the schema needs a **`person_email` child
table** rather than a single email column — collapsing to one address would
silently discard a real identifier that a future source might match on.

---

## 8. Cross-source matching

### Deterministic links (zero false positives on this dataset)

| Link | Key | Matches | Corroboration |
|---|---|---|---|
| source1 ↔ source2 | normalised email | **15** | all 15 agree on name |
| source1 ↔ source3 | normalised phone | **25** | all 25 agree on normalised name; **no** phone match disagrees on name |
| source2 ↔ source3 | *none exists* | 0 | 20 shared names, all routed through source1 |

### Name-only candidates — the only place fuzzy logic is needed

Five pairs are source2-only on one side and source3-only on the other:

| Name | source2 | source3 | Verdict |
|---|---|---|---|
| Divya Chopra | line 21 | line 30 | **Safe merge** — name unique in both files, absent from source1 |
| Karan Chopra | line 22 | line 31 | **Safe merge** |
| Manish Bhatia | line 19 | line 29 | **Safe merge** |
| Vikram Mehta | line 23 | line 32 | **Safe merge** |
| Arjun Mehta | line 18 | line 28 | **AMBIGUOUS — do not auto-merge** (section 9) |

The 4 safe merges take the cluster count from 60 to **56**.

---

## 9. The Arjun Mehta ambiguity

Three candidate records, one name:

| # | Source | Key | City | Other |
|---|---|---|---|---|
| A | source1 line 20 **+** source3 line 5 | `arjun.mehta9@example.in`, phone `9000000131` | NOIDA / Noida | linked by phone, high confidence |
| B | source3 line 28 | phone `9000000272` | Noida | 14 projects, Verified = Yes |
| C | source2 line 18 | `arjun.mehta77@mailtest.example.org`, **no phone** | Noida | Inactive |

**C could belong to A or to B.** The emails differ (`mehta9` vs `mehta77`), C has
no phone at all, and all three sit in Noida. There is **no evidence in the data**
that resolves it.

**Decision: leave all three unmerged.** Write the pair to a `match_review` queue
with `confidence = 0.5` and a stated reason. Merging would be inventing data;
silently dropping one would lose a real record. Recognising an unresolvable
match — and building the queue a human resolves it in — is the correct
engineering answer, not a gap.

---

## 10. The Deepak Nair duplicate-person trap

The mirror image of section 9: one name, two **genuinely different people**.

| # | Source | Key | City | Skills |
|---|---|---|---|---|
| A | source1 line 33 + source2 line 15 + source3 line 25 | `deepak.nair44@example.com`, phone `9000000296` | Bengaluru | react, n8n, mongodb, pandas |
| B | source2 line 32 | `deepak.nair57@example.in` | New Delhi | javascript, react, docker, web scraping, mysql, sql |

Different email, different city, different skill set, and B has no source3 twin.
A naive name-based merge fuses two separate people into one record — the exact
failure the assignment is testing for.

This is not an isolated risk. In source1 alone the first name `Nikhil` appears
4 times and `Priya`, `Isha`, `Rohit`, `Rahul` and `Arjun` appear 3 times each,
across different surnames and identifiers.

---

## 11. Recommended matching strategy

**Name equality alone must never trigger a merge.** Name is a *blocking* key;
email or phone is the *deciding* key.

Three passes, deterministic before fuzzy:

1. **Pass 1 — exact normalised email.** Links source1 ↔ source2. 15 merges.
2. **Pass 2 — exact normalised phone.** Links source1 ↔ source3. 25 merges.
   Passes 1 and 2 are transitive via union-find, which is what produces the
   15 three-way clusters.
3. **Pass 3 — guarded name-only.** Merge two records on name **only if all** of
   the following hold:
   - the normalised name is unique within each of its two files, **and**
   - neither record was already linked in pass 1 or 2, **and**
   - the name does not appear in source1 (the bridge file).

   4 merges here. Anything failing the guard goes to `match_review` instead of
   being merged or dropped.

**Expected result: 56 golden person records**, with the Arjun Mehta trio flagged
for human review and Deepak Nair correctly kept as two people.

### Survivorship rules (applied when building the golden record)

1. Prefer the source holding the deciding key (source1 for email + phone).
2. Longest complete name wins (`Rohit Verma` over `R. Verma`).
3. Non-null beats null.
4. On city conflict, prefer the most specific non-region value; log a
   `data_issue` regardless so the conflict stays visible.
5. Never merge on name alone.

Every merge is recorded in `person_source_link` with its `match_method` and
`confidence`, so any golden record can be traced back to the exact source lines
and the reason they were joined.
