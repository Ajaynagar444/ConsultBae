-- 003_staging.sql
-- Layer 2: staging. Cleaned and typed, but still ONE ROW PER SOURCE ROW.
-- No merging happens here - that is the golden layer's job.
--
-- Bad rows are quarantined, never deleted. The blank row, the embedded header
-- and the column-shifted row all land here flagged with a reason, so the
-- pipeline's own output is the evidence for the Task 4 report.
--
-- Every CHECK below encodes a rule from docs/data-profile.md section 6. They
-- are here rather than only in Python because a constraint cannot be forgotten
-- by a future code path, and a violation fails loudly at load time instead of
-- producing a quietly wrong golden record.

-- ---------------------------------------------------------------------------
-- Enums for the columns that mix units or spellings in the sources.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ctc_unit') THEN
        -- source1 "Current CTC" mixes absolute rupees and lakhs, 21 rows each.
        CREATE TYPE ctc_unit AS ENUM ('rupee', 'lakh');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'rate_unit') THEN
        -- source2 "rate" mixes N/hr and Nk/month. These do NOT reconcile
        -- (15k/month is about Rs.94/hr against an hourly floor of Rs.330), so
        -- the unit is preserved rather than converted away.
        CREATE TYPE rate_unit AS ENUM ('per_hour', 'per_month');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'gig_status') THEN
        -- source2 "status": 3 real states in 5 spellings.
        CREATE TYPE gig_status AS ENUM ('active', 'inactive', 'paused');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'quarantine_reason') THEN
        CREATE TYPE quarantine_reason AS ENUM (
            'blank_row',        -- source2 line 12
            'embedded_header',  -- source3 line 16
            'unparseable'       -- reserved: a row we cannot type at all
        );
    END IF;
END
$$;


CREATE TABLE IF NOT EXISTS staged_person (
    -- PK is the raw row it came from: staging is strictly 1:1 with raw.
    raw_record_id      bigint PRIMARY KEY REFERENCES raw_record (id) ON DELETE CASCADE,
    source_system      source_system NOT NULL,

    -- ---- identity ----------------------------------------------------------
    full_name          text,
    -- Lowercase, punctuation-stripped. Used ONLY as a blocking key for
    -- candidate generation - never on its own as a merge decision.
    name_key           text,
    email_norm         text,
    -- E.164. All 72 real phone numbers across source1 and source3 normalise to
    -- exactly 10 national digits, verified during profiling.
    phone_e164         text,

    -- ---- location ----------------------------------------------------------
    city_raw           text,
    city_norm          text,
    -- 'Delhi NCR' is a region, not a city, and cannot be safely collapsed to
    -- Delhi / Noida / Gurugram. Flagged rather than guessed at.
    is_region          boolean NOT NULL DEFAULT false,

    -- ---- source1 fields ----------------------------------------------------
    experience_years   numeric(4,1),
    ctc_annual_inr     bigint,
    ctc_source_unit    ctc_unit,
    applied_on         date,

    -- ---- source2 fields ----------------------------------------------------
    rate_amount        numeric(10,2),
    rate_source_unit   rate_unit,
    status             gig_status,

    -- ---- source3 fields ----------------------------------------------------
    is_verified        boolean,
    projects_completed integer,

    -- ---- shared ------------------------------------------------------------
    skills             text[] NOT NULL DEFAULT '{}',

    -- ---- provenance / quality ---------------------------------------------
    is_quarantined     boolean NOT NULL DEFAULT false,
    quarantined_as     quarantine_reason,
    was_repaired       boolean NOT NULL DEFAULT false,
    repair_note        text,
    staged_at          timestamptz NOT NULL DEFAULT now(),

    -- ---- constraints -------------------------------------------------------

    -- Quarantine flag and reason travel together, in both directions.
    CONSTRAINT staged_person_quarantine_ck
        CHECK (is_quarantined = (quarantined_as IS NOT NULL)),

    -- A repaired row must say what was repaired. source2 line 20 is the only
    -- one on this dataset: fields rotated one position right.
    CONSTRAINT staged_person_repair_ck
        CHECK (NOT was_repaired OR repair_note IS NOT NULL),

    -- A usable row must carry at least one identifier. Without this, a parsing
    -- bug could silently produce rows that can never be matched to anyone.
    CONSTRAINT staged_person_has_identifier_ck
        CHECK (
            is_quarantined
            OR email_norm IS NOT NULL
            OR phone_e164 IS NOT NULL
        ),

    -- Email is stored already normalised. 9 source2 rows arrive ALL-CAPS.
    CONSTRAINT staged_person_email_norm_ck
        CHECK (email_norm IS NULL
               OR (email_norm = lower(email_norm) AND email_norm LIKE '%@%.%')),

    -- +91 followed by exactly 10 digits.
    CONSTRAINT staged_person_phone_e164_ck
        CHECK (phone_e164 IS NULL OR phone_e164 ~ '^\+91[0-9]{10}$'),

    CONSTRAINT staged_person_name_key_ck
        CHECK (name_key IS NULL OR name_key = lower(name_key)),

    -- Observed range is 0.8 - 5.6. Bounds are generous but catch a unit slip.
    CONSTRAINT staged_person_experience_ck
        CHECK (experience_years IS NULL
               OR (experience_years >= 0 AND experience_years <= 60)),

    -- Always stored in rupees; ctc_source_unit records what the file said.
    -- Observed post-conversion range is 3.27L - 11.95L.
    CONSTRAINT staged_person_ctc_ck
        CHECK (ctc_annual_inr IS NULL OR ctc_annual_inr > 0),
    CONSTRAINT staged_person_ctc_unit_ck
        CHECK ((ctc_annual_inr IS NULL) = (ctc_source_unit IS NULL)),

    CONSTRAINT staged_person_rate_ck
        CHECK (rate_amount IS NULL OR rate_amount > 0),
    -- An amount without its unit is meaningless given the two scales in play.
    CONSTRAINT staged_person_rate_unit_ck
        CHECK ((rate_amount IS NULL) = (rate_source_unit IS NULL)),

    -- source3 line 9 has a legitimate 0. Zero is not null.
    CONSTRAINT staged_person_projects_ck
        CHECK (projects_completed IS NULL OR projects_completed >= 0),

    -- Static bounds, not now(): a CHECK must be immutable, and a moving
    -- boundary would make old rows spontaneously invalid. All 42 source1 dates
    -- fall in Jun-Aug 2026.
    CONSTRAINT staged_person_applied_on_ck
        CHECK (applied_on IS NULL
               OR applied_on BETWEEN DATE '2000-01-01' AND DATE '2100-01-01'),

    -- Skills are lowercase, trimmed, non-empty, with no NULL elements.
    --
    -- Written without a subquery on purpose: PostgreSQL rejects subqueries in
    -- CHECK constraints ("cannot use subquery in check constraint"), so the
    -- obvious ARRAY(SELECT lower(trim(s)) FROM unnest(skills) s) formulation is
    -- not available. Joining the array and comparing against its own lowercase
    -- form tests every element at once; the LIKE tests catch whitespace sitting
    -- next to a separator, which is how a stray ", " in the source would show
    -- up. array_to_string skips NULL elements, so they are checked separately.
    CONSTRAINT staged_person_skills_ck
        CHECK (
            array_position(skills, NULL) IS NULL
            AND array_position(skills, '') IS NULL
            AND array_to_string(skills, '|') = lower(array_to_string(skills, '|'))
            AND array_to_string(skills, '|') NOT LIKE ' %'
            AND array_to_string(skills, '|') NOT LIKE '% '
            AND array_to_string(skills, '|') NOT LIKE '% |%'
            AND array_to_string(skills, '|') NOT LIKE '%| %'
        )
);

COMMENT ON TABLE staged_person IS
    'Cleaned and typed, still 1:1 with raw_record. Bad rows are quarantined here, not deleted.';
COMMENT ON COLUMN staged_person.name_key IS
    'Blocking key only. Name equality alone never decides a merge - see docs/data-profile.md section 11.';
COMMENT ON COLUMN staged_person.is_region IS
    'True for Delhi NCR, which is a region and cannot be resolved to a single city.';

-- The three matching passes read these. Partial indexes because a large share
-- of rows have no email (source3) or no phone (source2), and there is no point
-- indexing those NULLs.
CREATE INDEX IF NOT EXISTS ix_staged_person_email
    ON staged_person (email_norm) WHERE email_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_staged_person_phone
    ON staged_person (phone_e164) WHERE phone_e164 IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_staged_person_name_key
    ON staged_person (name_key) WHERE name_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_staged_person_source
    ON staged_person (source_system);

-- Feeds the Task 4 report.
CREATE INDEX IF NOT EXISTS ix_staged_person_quarantined
    ON staged_person (quarantined_as) WHERE is_quarantined;
