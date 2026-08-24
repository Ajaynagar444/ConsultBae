-- 002_raw.sql
-- Layer 1: raw. Append-only, lossless, one row per physical source line.
--
-- Nothing in this layer is cleaned, parsed or judged. A row that is blank, a
-- repeated header, or column-shifted is stored exactly as read. That is the
-- point: the raw layer is the audit trail that lets any golden record be traced
-- back to the bytes it came from, and lets the whole pipeline be re-derived
-- without re-reading the CSVs.

-- ---------------------------------------------------------------------------
-- One row per execution of the ingest pipeline.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_run (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    status        text        NOT NULL DEFAULT 'running',
    note          text,

    CONSTRAINT ingestion_run_status_ck
        CHECK (status IN ('running', 'succeeded', 'failed')),
    -- A finished run must have an end time, and a running one must not.
    CONSTRAINT ingestion_run_finished_ck
        CHECK ((status = 'running') = (finished_at IS NULL)),
    CONSTRAINT ingestion_run_duration_ck
        CHECK (finished_at IS NULL OR finished_at >= started_at)
);

COMMENT ON TABLE ingestion_run IS
    'One row per pipeline execution. Everything downstream is attributed to a run.';


-- ---------------------------------------------------------------------------
-- One row per physical CSV line (excluding the file header on line 1).
--
-- payload holds the line as a JSON object keyed by the file's own column names,
-- so the raw layer needs no per-source schema. Three files, three shapes, one
-- table.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_record (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id          bigint        NOT NULL REFERENCES ingestion_run (id) ON DELETE CASCADE,
    source_system   source_system NOT NULL,
    source_line_no  integer       NOT NULL,
    payload         jsonb         NOT NULL,
    row_sha256      char(64)      NOT NULL,
    ingested_at     timestamptz   NOT NULL DEFAULT now(),

    -- Line 1 is the header in all three files, so data starts at line 2.
    CONSTRAINT raw_record_line_no_ck
        CHECK (source_line_no >= 2),
    -- payload must be a JSON object, not a scalar or array. Without this a bug
    -- in the loader could store `"null"` or `[]` and stay invisible until the
    -- staging layer fell over.
    CONSTRAINT raw_record_payload_object_ck
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT raw_record_sha256_ck
        CHECK (row_sha256 ~ '^[0-9a-f]{64}$'),
    -- The natural key. Makes re-running a given run idempotent and makes
    -- "which line did this come from" answerable without guessing.
    CONSTRAINT raw_record_natural_key_uq
        UNIQUE (run_id, source_system, source_line_no)
);

COMMENT ON TABLE raw_record IS
    'Append-only. One row per source CSV line, stored verbatim as jsonb. Never updated.';
COMMENT ON COLUMN raw_record.source_line_no IS
    '1-based physical line number in the source file. Line 1 is the header, so data starts at 2.';
COMMENT ON COLUMN raw_record.row_sha256 IS
    'SHA-256 of the raw line. Detects duplicate physical lines and verifies the file has not drifted.';

-- Lookups by source + line: the "show me line 20 of source 2" query used
-- constantly when explaining a merge decision.
CREATE INDEX IF NOT EXISTS ix_raw_record_source_line
    ON raw_record (source_system, source_line_no);

-- Finds physically identical lines across the corpus.
CREATE INDEX IF NOT EXISTS ix_raw_record_sha256
    ON raw_record (row_sha256);

-- Ad-hoc querying into the payload without a per-source schema, e.g.
--   SELECT * FROM raw_record WHERE payload @> '{"City": "NOIDA"}';
CREATE INDEX IF NOT EXISTS ix_raw_record_payload_gin
    ON raw_record USING gin (payload jsonb_path_ops);

CREATE INDEX IF NOT EXISTS ix_raw_record_run
    ON raw_record (run_id);
