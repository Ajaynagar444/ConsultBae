-- 004_golden.sql
-- Layer 3: golden. One row per real person.
--
-- Expected population on this dataset: 56 person rows from 102 usable source
-- rows. The count is derived in docs/data-profile.md section 2 and is asserted
-- by the test suite, so a regression in the matcher fails a test rather than
-- quietly shipping the wrong number of people.
--
-- The important structural claims, all enforced below rather than assumed:
--   * a source row belongs to exactly one person   -> person_source_link UNIQUE
--   * an email identifies exactly one person       -> person_email UNIQUE
--   * a phone identifies exactly one person        -> person_phone UNIQUE
--   * a person has at most one primary of each     -> partial UNIQUE indexes

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'match_method') THEN
        CREATE TYPE match_method AS ENUM (
            'singleton',    -- appeared in exactly one source, nothing to merge
            'email_exact',  -- pass 1: source1 <-> source2, 15 links
            'phone_exact',  -- pass 2: source1 <-> source3, 25 links
            'name_guarded', -- pass 3: unique name in both files, absent from the bridge
            'manual'        -- resolved by a human from match_review
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'issue_severity') THEN
        CREATE TYPE issue_severity AS ENUM ('info', 'warning', 'error');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'review_status') THEN
        CREATE TYPE review_status AS ENUM ('open', 'merged', 'rejected');
    END IF;
END
$$;


-- ---------------------------------------------------------------------------
-- The golden record.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS person (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name          text NOT NULL,
    name_key           text NOT NULL,

    -- Denormalised primaries for convenience; person_email / person_phone are
    -- the source of truth. Nikhil Chopra has two emails, which is exactly why
    -- a single email column here would not be enough on its own.
    primary_email      text,
    primary_phone_e164 text,

    city               text,
    is_region          boolean NOT NULL DEFAULT false,

    experience_years   numeric(4,1),
    ctc_annual_inr     bigint,
    rate_amount        numeric(10,2),
    rate_source_unit   rate_unit,
    status             gig_status,
    is_verified        boolean,
    projects_completed integer,
    applied_on         date,

    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT person_full_name_ck   CHECK (length(trim(full_name)) > 0),
    CONSTRAINT person_name_key_ck    CHECK (name_key = lower(name_key) AND length(name_key) > 0),
    CONSTRAINT person_email_ck       CHECK (primary_email IS NULL OR primary_email = lower(primary_email)),
    CONSTRAINT person_phone_ck       CHECK (primary_phone_e164 IS NULL OR primary_phone_e164 ~ '^\+91[0-9]{10}$'),
    CONSTRAINT person_experience_ck  CHECK (experience_years IS NULL OR experience_years BETWEEN 0 AND 60),
    CONSTRAINT person_ctc_ck         CHECK (ctc_annual_inr IS NULL OR ctc_annual_inr > 0),
    CONSTRAINT person_rate_ck        CHECK (rate_amount IS NULL OR rate_amount > 0),
    CONSTRAINT person_rate_unit_ck   CHECK ((rate_amount IS NULL) = (rate_source_unit IS NULL)),
    CONSTRAINT person_projects_ck    CHECK (projects_completed IS NULL OR projects_completed >= 0),
    -- Every person must be reachable somehow.
    CONSTRAINT person_contactable_ck CHECK (primary_email IS NOT NULL OR primary_phone_e164 IS NOT NULL)
);

COMMENT ON TABLE person IS
    'Golden record: one row per real person. Expected 56 on this dataset.';

DROP TRIGGER IF EXISTS trg_person_updated_at ON person;
CREATE TRIGGER trg_person_updated_at
    BEFORE UPDATE ON person
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS ix_person_name_key ON person (name_key);
CREATE INDEX IF NOT EXISTS ix_person_city     ON person (city);


-- ---------------------------------------------------------------------------
-- All known emails / phones. Multi-valued because real people have more than
-- one: source1 lines 27 and 37 are one person with two addresses.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS person_email (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    person_id  bigint  NOT NULL REFERENCES person (id) ON DELETE CASCADE,
    email      text    NOT NULL,
    is_primary boolean NOT NULL DEFAULT false,

    -- An email address identifies exactly one person. If the matcher ever tries
    -- to give the same address to two people, the load fails here.
    CONSTRAINT person_email_uq    UNIQUE (email),
    CONSTRAINT person_email_lc_ck CHECK (email = lower(email) AND email LIKE '%@%.%')
);

-- At most one primary per person.
CREATE UNIQUE INDEX IF NOT EXISTS uq_person_email_primary
    ON person_email (person_id) WHERE is_primary;

CREATE INDEX IF NOT EXISTS ix_person_email_person ON person_email (person_id);


CREATE TABLE IF NOT EXISTS person_phone (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    person_id  bigint  NOT NULL REFERENCES person (id) ON DELETE CASCADE,
    phone_e164 text    NOT NULL,
    is_primary boolean NOT NULL DEFAULT false,

    CONSTRAINT person_phone_uq    UNIQUE (phone_e164),
    CONSTRAINT person_phone_fmt_ck CHECK (phone_e164 ~ '^\+91[0-9]{10}$')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_person_phone_primary
    ON person_phone (person_id) WHERE is_primary;

CREATE INDEX IF NOT EXISTS ix_person_phone_person ON person_phone (person_id);


-- ---------------------------------------------------------------------------
-- Skills. 15 canonical tokens across both sources; see data-profile section 6.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skill (
    id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,

    CONSTRAINT skill_name_uq UNIQUE (name),
    CONSTRAINT skill_name_ck CHECK (name = lower(trim(name)) AND length(name) > 0)
);

CREATE TABLE IF NOT EXISTS person_skill (
    person_id bigint NOT NULL REFERENCES person (id) ON DELETE CASCADE,
    skill_id  bigint NOT NULL REFERENCES skill  (id) ON DELETE CASCADE,
    PRIMARY KEY (person_id, skill_id)
);

-- Reverse lookup: "everyone who knows n8n", which Task 2's automation needs.
CREATE INDEX IF NOT EXISTS ix_person_skill_skill ON person_skill (skill_id);


-- ---------------------------------------------------------------------------
-- Provenance. The table that makes the whole merge defensible: every golden
-- record traces to the exact source lines that produced it, with the reason.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS person_source_link (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    person_id     bigint       NOT NULL REFERENCES person     (id) ON DELETE CASCADE,
    raw_record_id bigint       NOT NULL REFERENCES raw_record (id) ON DELETE CASCADE,
    method        match_method NOT NULL,
    confidence    numeric(3,2) NOT NULL DEFAULT 1.00,
    linked_at     timestamptz  NOT NULL DEFAULT now(),

    -- THE core invariant: a source row belongs to exactly one person. This is
    -- the constraint that enforces "the same person appearing in multiple files
    -- must become ONE record" - a double-count is impossible, not just
    -- unlikely.
    CONSTRAINT person_source_link_raw_uq UNIQUE (raw_record_id),
    CONSTRAINT person_source_link_conf_ck CHECK (confidence > 0 AND confidence <= 1)
);

COMMENT ON TABLE person_source_link IS
    'Provenance. UNIQUE(raw_record_id) guarantees each source row maps to exactly one person.';

CREATE INDEX IF NOT EXISTS ix_person_source_link_person ON person_source_link (person_id);
CREATE INDEX IF NOT EXISTS ix_person_source_link_method ON person_source_link (method);


-- ---------------------------------------------------------------------------
-- Task 4: the data issues report, generated by the pipeline rather than
-- hand-written. issue_code maps to the 16 classes in data-profile section 5.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_issue (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        bigint         NOT NULL REFERENCES ingestion_run (id) ON DELETE CASCADE,
    raw_record_id bigint         REFERENCES raw_record (id) ON DELETE CASCADE,
    source_system source_system,
    issue_code    text           NOT NULL,
    severity      issue_severity NOT NULL DEFAULT 'warning',
    column_name   text,
    detail        text           NOT NULL,
    action_taken  text           NOT NULL,
    detected_at   timestamptz    NOT NULL DEFAULT now(),

    CONSTRAINT data_issue_code_ck   CHECK (issue_code = lower(issue_code) AND length(issue_code) > 0),
    CONSTRAINT data_issue_detail_ck CHECK (length(trim(detail)) > 0),
    CONSTRAINT data_issue_action_ck CHECK (length(trim(action_taken)) > 0)
);

COMMENT ON COLUMN data_issue.action_taken IS
    'What the pipeline actually did. Required: an issue with no recorded action is not a report.';

CREATE INDEX IF NOT EXISTS ix_data_issue_run    ON data_issue (run_id);
CREATE INDEX IF NOT EXISTS ix_data_issue_code   ON data_issue (issue_code);
CREATE INDEX IF NOT EXISTS ix_data_issue_raw    ON data_issue (raw_record_id);


-- ---------------------------------------------------------------------------
-- Ambiguous matches a human must resolve. On this dataset the Arjun Mehta trio
-- lands here at confidence 0.50: source2 line 18 could belong to either the
-- source1+source3 person or the standalone source3 person, and nothing in the
-- data decides it. Surfacing that is the correct answer, not a gap.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_review (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          bigint        NOT NULL REFERENCES ingestion_run (id) ON DELETE CASCADE,
    raw_record_id_a bigint        NOT NULL REFERENCES raw_record (id) ON DELETE CASCADE,
    raw_record_id_b bigint        NOT NULL REFERENCES raw_record (id) ON DELETE CASCADE,
    reason          text          NOT NULL,
    confidence      numeric(3,2)  NOT NULL,
    status          review_status NOT NULL DEFAULT 'open',
    resolved_by     text,
    resolved_at     timestamptz,

    CONSTRAINT match_review_distinct_ck CHECK (raw_record_id_a <> raw_record_id_b),
    -- Store each pair once, in a stable order, so (a,b) and (b,a) cannot both
    -- appear as separate review items.
    CONSTRAINT match_review_ordered_ck  CHECK (raw_record_id_a < raw_record_id_b),
    CONSTRAINT match_review_pair_uq     UNIQUE (raw_record_id_a, raw_record_id_b),
    CONSTRAINT match_review_conf_ck     CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT match_review_resolved_ck CHECK ((status = 'open') = (resolved_at IS NULL))
);

CREATE INDEX IF NOT EXISTS ix_match_review_open ON match_review (status) WHERE status = 'open';


-- ---------------------------------------------------------------------------
-- Convenience views. Read-only, used by the Task 4 report and by Task 2's n8n
-- flow so the automation does not need to re-implement joins.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_person_full AS
SELECT
    p.id,
    p.full_name,
    p.primary_email,
    p.primary_phone_e164,
    p.city,
    p.experience_years,
    p.ctc_annual_inr,
    p.rate_amount,
    p.rate_source_unit,
    p.status,
    p.is_verified,
    p.projects_completed,
    p.applied_on,
    COALESCE(sk.skills, '{}')          AS skills,
    COALESCE(src.sources, '{}')        AS sources,
    COALESCE(src.source_row_count, 0)  AS source_row_count
FROM person p
LEFT JOIN LATERAL (
    SELECT array_agg(s.name ORDER BY s.name) AS skills
    FROM person_skill ps
    JOIN skill s ON s.id = ps.skill_id
    WHERE ps.person_id = p.id
) sk ON true
LEFT JOIN LATERAL (
    SELECT array_agg(DISTINCT r.source_system::text) AS sources,
           count(*)                                  AS source_row_count
    FROM person_source_link psl
    JOIN raw_record r ON r.id = psl.raw_record_id
    WHERE psl.person_id = p.id
) src ON true;

COMMENT ON VIEW v_person_full IS
    'One row per person with skills and originating sources flattened. Used by Task 2 and the report.';


CREATE OR REPLACE VIEW v_data_issue_report AS
SELECT
    issue_code,
    severity,
    source_system,
    count(*)                         AS occurrences,
    min(detected_at)                 AS first_seen,
    (array_agg(DISTINCT action_taken))[1] AS action_taken
FROM data_issue
GROUP BY issue_code, severity, source_system
ORDER BY severity DESC, occurrences DESC, issue_code;

COMMENT ON VIEW v_data_issue_report IS
    'Task 4 report, aggregated straight from what the pipeline recorded.';
