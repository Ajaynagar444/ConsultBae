# Task 2 — n8n Duplicate Detection Workflow

One n8n workflow that receives a CSV over a webhook, checks every row against
the Task 1 PostgreSQL database, and answers with a duplicate alert or a
new-record verdict per row. Detection only — it never merges records and never
creates a canonical person; creation belongs to the audited Task 1 pipeline.

```
Incoming CSV
   │  POST /webhook/consultbae-duplicate-check
   ▼
Receive CSV            (Webhook trigger)
   ▼
Parse CSV + Normalise  (Code: same email/phone rules as Task 1 normalize.py)
   ▼
Find Existing Person   (PostgreSQL: exact email first, then exact phone)
   ▼
Duplicate?             (IF: did the lookup return a person_id?)
 ├─ yes ─ Format Duplicate Alert ─┐
 └─ no ── Format New Record ──────┤
                                  ▼
                       Collect Both Branches (Merge)
                                  ▼
                       Summarise → Respond With Report
```

## Design decisions

- **Identity rules are byte-for-byte the Task 1 rules.** Email: trim +
  lowercase. Phone: strip non-digits, peel the `091`/`91`/`0` prefixes, accept
  only 10 digits starting 6–9, store as `+91` E.164. An invalid identifier
  becomes `null`, never a guess — the same policy the pipeline applies.
- **Match priority is exact email, then exact phone** — the same deterministic
  order as Task 1 matching. No fuzzy matching, no name matching: a name alone
  never proves identity in this dataset (two different Deepak Nairs share one).
- **The SQL is parameterised** (`$1..$4` query replacements), so a hostile CSV
  cannot inject SQL, and **read-only** — the workflow only SELECTs.
- **One row always comes back per incoming row** (`LEFT JOIN LATERAL`), so the
  IF node branches on `person_id` being present instead of juggling
  empty-result items.
- **Absent values travel as the sentinel `-`, not `''`.** n8n's
  query-replacement splits parameters on commas and silently drops empty
  segments — with `''` a row missing its phone shifted every later parameter
  and Postgres failed with `there is no parameter $4`. The SQL maps the
  sentinel back to NULL (`NULLIF($1, '-')`), and the parse step strips commas
  from names so a `Doe, Jane` can never split the parameter list either.
- **Cross-identifier conflicts are surfaced:** if the email matches person X
  while the phone matches person Y, the alert carries a
  `"needs human review"` warning instead of silently trusting the email.
- **The alert is the webhook response** — free, reliable, and visible in a
  screen recording. No paid service, no Slack credential required.

## Requirements

| What | Value |
|---|---|
| n8n | 2.35.7 (what this was built and tested on) |
| Node.js | 20 / 22 / 24 |
| PostgreSQL | the Task 1 database (`consultbae`), populated: `ingest → stage → match` |

## Running n8n (no Docker)

n8n is installed with npm into a plain folder — nothing global, nothing on C:.

```powershell
mkdir D:\Medro\n8n-runtime; cd D:\Medro\n8n-runtime
npm init -y
npm install n8n

# keep n8n's own data (SQLite, encryption key) off C: and out of the repo
$env:N8N_USER_FOLDER = 'D:\Medro\n8n-data'
$env:N8N_DIAGNOSTICS_ENABLED = 'false'
.\node_modules\.bin\n8n.cmd start        # UI on http://localhost:5678
```

First boot runs SQLite migrations for a minute or two; until
`/healthz/readiness` returns ok the webhook answers
`503 Database is not ready!`. Later boots are ready in seconds. On the first
UI visit n8n asks you to create the local owner account.

## Importing the workflow

**UI:** n8n → Workflows → *Import from File* → pick `automation/workflow.json`.

**CLI (what the repo's verification used):**

```powershell
.\node_modules\.bin\n8n.cmd import:workflow --input="<repo>\automation\workflow.json"
.\node_modules\.bin\n8n.cmd list:workflow                      # note the ID
.\node_modules\.bin\n8n.cmd update:workflow --id=<ID> --active=true
.\node_modules\.bin\n8n.cmd start
```

## PostgreSQL credential

Credentials are **not** part of the exported JSON (n8n strips them — that is
why no secret lives in this repo). Create one and attach it to the *Find
Existing Person* node:

n8n UI → Credentials → *Add credential* → **Postgres**:

| Field | Value |
|---|---|
| Name | `ConsultBae Postgres` |
| Host | `localhost` |
| Database | `consultbae` |
| User | `consultbae` |
| Password | your `POSTGRES_PASSWORD` from `.env` |
| Port | `5432` |
| SSL | `disable` (local server) |

The workflow references the credential by name; after import, open the
Postgres node once and select it if n8n has not bound it automatically.

## Triggering it

The webhook accepts the CSV as JSON (`{"csv": "..."}`) or as a form field
(`csv=...`). Expected header: `name,email,phone` (any order; extra columns are
ignored).

```bash
# bash / Git Bash — form-encoded straight from a file
curl -s -X POST "http://localhost:5678/webhook/consultbae-duplicate-check" \
     --data-urlencode "csv@automation/samples/duplicate_email.csv"
```

```powershell
# PowerShell
$csv = Get-Content automation\samples\duplicate_email.csv -Raw
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:5678/webhook/consultbae-duplicate-check" `
  -ContentType "application/json" `
  -Body (@{ csv = $csv } | ConvertTo-Json)
```

While a workflow is open in the editor you can also use *Listen for test
event*, which exposes the same flow at `/webhook-test/...` for one call.

## Expected behaviour

**Duplicate** (identifier already in the golden layer):

```json
{
  "status": "DUPLICATE",
  "incoming": { "name": "...", "email": "...", "phone": "..." },
  "matched_person": { "id": 12, "name": "Isha Chopra", "city": "Pune" },
  "match": { "method": "exact_email", "confidence": 1,
             "evidence": "incoming email equals stored email ..." },
  "action": "not inserted - flagged for review"
}
```

**New person** (no identifier matches): `status: "NEW"`, with
`"safe to continue as a new record (this workflow does not create one)"`.

**No usable identifier** (bad email *and* bad phone): `status: "UNCHECKABLE"` —
the row is refused for identity purposes rather than waved through.

The response wraps per-row results in a summary:

```json
{ "summary": { "rows_checked": 4, "duplicates": 2, "new_records": 1, "uncheckable": 1 },
  "results": [ ... ] }
```

## Sample inputs (`automation/samples/`)

| File | What it proves |
|---|---|
| `duplicate_email.csv` | Isha Chopra with her email in ALL-CAPS → normalisation still finds her (`exact_email`) |
| `duplicate_phone.csv` | Rohit Nair by phone written `+91-9000000268` → prefix peeled, found (`exact_phone`) |
| `new_person.csv` | synthetic Asha Testperson → `NEW` |
| `mixed.csv` | four rows at once: R. Verma's shared email, Tanvi Gupta's phone in `0`-prefix form, a new person, and a row with junk identifiers |

## Actual test results

Executed against the live database (56 canonical people), n8n 2.35.7,
production webhook, workflow imported from this exact JSON:

| Input | Result | Evidence |
|---|---|---|
| `duplicate_email.csv` — Isha's email in ALL-CAPS | `DUPLICATE` → person 14 *Isha Chopra*, Pune | `exact_email`, confidence 1.0 |
| `duplicate_phone.csv` — phone as `+91-9000000268` | `DUPLICATE` → person 36 *Rohit Nair*, Gurugram | `exact_phone`, confidence 1.0 |
| `new_person.csv` — synthetic Asha Testperson | `NEW` — "no existing person matched either identifier" | — |
| `mixed.csv` row 1 — name `R. Verma`, the shared email | `DUPLICATE` → person 30 **Rohit Verma** — the Task 1 merge visible through the automation | `exact_email` |
| `mixed.csv` row 2 — phone as `09000000254` | `DUPLICATE` → person 7 *Tanvi Gupta* | `exact_phone` |
| `mixed.csv` row 3 — new person | `NEW` | — |
| `mixed.csv` row 4 — junk email + junk phone | `UNCHECKABLE` — refused for identity purposes, not waved through | — |

Summary line for `mixed.csv`:
`{"rows_checked": 4, "duplicates": 2, "new_records": 1, "uncheckable": 1}`
