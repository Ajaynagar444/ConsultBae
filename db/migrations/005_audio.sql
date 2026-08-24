-- 005_audio.sql
-- Task 3: audio submissions.
--
-- Created now, with the rest of the schema, because the assignment requires the
-- audio record to land in "your database from Task 1" - it is part of the same
-- model, not a bolted-on side table. person_id is nullable and resolved through
-- the SAME matcher as Task 1: a submitter who already exists links to their
-- person row, a new one creates one.
--
-- The four required metrics (duration, sample rate, bitrate, loudness) are
-- enforced by a constraint rather than left to application code - see
-- audio_submission_probe_ok_ck below.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'probe_status') THEN
        CREATE TYPE probe_status AS ENUM (
            'pending',  -- stored, not yet analysed
            'ok',       -- ffprobe succeeded, all four metrics present
            'failed'    -- unreadable / not audio / ffprobe error
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'quality_label') THEN
        CREATE TYPE quality_label AS ENUM ('good', 'fair', 'poor', 'unknown');
    END IF;
END
$$;


CREATE TABLE IF NOT EXISTS audio_submission (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Resolved via the Task 1 matcher. Nullable: the row is stored even if the
    -- submitter cannot be matched or created, because losing an upload is worse
    -- than an unlinked record.
    person_id          bigint REFERENCES person (id) ON DELETE SET NULL,

    -- What the submitter actually typed, kept verbatim alongside the normalised
    -- phone. Same principle as the raw layer: never discard the input.
    submitted_name     text NOT NULL,
    submitted_phone_raw   text NOT NULL,
    submitted_phone_e164  text,

    -- ---- stored file -------------------------------------------------------
    storage_key        text   NOT NULL,
    original_filename  text,
    mime_type          text   NOT NULL,
    size_bytes         bigint NOT NULL,

    -- ---- required extracted metadata --------------------------------------
    duration_seconds   numeric(10,3),
    sample_rate_hz     integer,
    bitrate_bps        integer,
    loudness_dbfs      numeric(6,2),

    -- ---- bonus: rough noise / quality estimate ----------------------------
    noise_floor_dbfs   numeric(6,2),
    snr_db             numeric(6,2),
    quality            quality_label NOT NULL DEFAULT 'unknown',

    -- ---- probe bookkeeping -------------------------------------------------
    probe              probe_status NOT NULL DEFAULT 'pending',
    probe_error        text,
    created_at         timestamptz  NOT NULL DEFAULT now(),
    updated_at         timestamptz  NOT NULL DEFAULT now(),

    -- ---- constraints -------------------------------------------------------
    CONSTRAINT audio_submission_storage_key_uq UNIQUE (storage_key),
    CONSTRAINT audio_submission_name_ck   CHECK (length(trim(submitted_name)) > 0),
    CONSTRAINT audio_submission_phone_ck
        CHECK (submitted_phone_e164 IS NULL OR submitted_phone_e164 ~ '^\+91[0-9]{10}$'),
    CONSTRAINT audio_submission_size_ck   CHECK (size_bytes > 0),
    CONSTRAINT audio_submission_mime_ck   CHECK (mime_type LIKE 'audio/%' OR mime_type LIKE 'video/%'),

    -- Plausibility bounds. 8 kHz is telephone quality, 192 kHz is studio; a
    -- value outside that is a parsing bug, not a real recording.
    CONSTRAINT audio_submission_duration_ck    CHECK (duration_seconds IS NULL OR duration_seconds > 0),
    CONSTRAINT audio_submission_sample_rate_ck CHECK (sample_rate_hz IS NULL OR sample_rate_hz BETWEEN 8000 AND 192000),
    CONSTRAINT audio_submission_bitrate_ck     CHECK (bitrate_bps IS NULL OR bitrate_bps > 0),
    -- dBFS is measured against full scale, so it is always <= 0. Getting a
    -- positive number back means the wrong ffmpeg filter was parsed.
    CONSTRAINT audio_submission_loudness_ck    CHECK (loudness_dbfs IS NULL OR loudness_dbfs <= 0),
    CONSTRAINT audio_submission_noise_ck       CHECK (noise_floor_dbfs IS NULL OR noise_floor_dbfs <= 0),

    -- The assignment requirement, as a constraint: a submission that claims a
    -- successful probe MUST have all four metrics. It is not possible to mark
    -- something 'ok' while silently missing bitrate.
    CONSTRAINT audio_submission_probe_ok_ck CHECK (
        probe <> 'ok'
        OR (duration_seconds IS NOT NULL
            AND sample_rate_hz IS NOT NULL
            AND bitrate_bps    IS NOT NULL
            AND loudness_dbfs  IS NOT NULL)
    ),
    -- A failed probe must say why.
    CONSTRAINT audio_submission_probe_failed_ck CHECK (probe <> 'failed' OR probe_error IS NOT NULL)
);

COMMENT ON TABLE audio_submission IS
    'Task 3. One row per submitted recording, linked to person via the Task 1 matcher.';
COMMENT ON COLUMN audio_submission.storage_key IS
    'Path relative to MEDIA_ROOT. Deliberately not an absolute path, so the store can move to S3.';
COMMENT ON COLUMN audio_submission.loudness_dbfs IS
    'Mean volume in dBFS from ffmpeg volumedetect. Always <= 0.';
COMMENT ON CONSTRAINT audio_submission_probe_ok_ck ON audio_submission IS
    'Encodes the assignment requirement: duration, sample rate, bitrate and loudness are all mandatory on success.';

DROP TRIGGER IF EXISTS trg_audio_submission_updated_at ON audio_submission;
CREATE TRIGGER trg_audio_submission_updated_at
    BEFORE UPDATE ON audio_submission
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS ix_audio_submission_person  ON audio_submission (person_id);
-- The listing view is newest-first.
CREATE INDEX IF NOT EXISTS ix_audio_submission_created ON audio_submission (created_at DESC);
-- Finds work for a background re-probe.
CREATE INDEX IF NOT EXISTS ix_audio_submission_pending ON audio_submission (probe) WHERE probe <> 'ok';


-- Backing query for the "list all submissions" view.
CREATE OR REPLACE VIEW v_audio_submission AS
SELECT
    a.id,
    a.created_at,
    a.submitted_name,
    COALESCE(a.submitted_phone_e164, a.submitted_phone_raw) AS phone,
    p.id                     AS person_id,
    p.full_name              AS matched_person,
    a.storage_key,
    a.mime_type,
    a.size_bytes,
    a.duration_seconds,
    -- The assignment asks for sample rate in kHz specifically.
    round(a.sample_rate_hz / 1000.0, 1) AS sample_rate_khz,
    a.bitrate_bps,
    round(a.bitrate_bps / 1000.0)       AS bitrate_kbps,
    a.loudness_dbfs,
    a.noise_floor_dbfs,
    a.snr_db,
    a.quality,
    a.probe,
    a.probe_error
FROM audio_submission a
LEFT JOIN person p ON p.id = a.person_id
ORDER BY a.created_at DESC;

COMMENT ON VIEW v_audio_submission IS
    'Task 3 listing view. Exposes sample rate in kHz and bitrate in kbps as the assignment asks.';
