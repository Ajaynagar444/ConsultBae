-- 001_extensions.sql
-- Foundation: extensions, shared enums, shared trigger function.
--
-- Target: PostgreSQL 16-compatible SQL. Nothing here uses a feature newer than
-- PG12, so these migrations run unchanged on 16 and on the 18.3 server this
-- project develops against.

-- ---------------------------------------------------------------------------
-- Extensions: none are required.
--
-- pg_trgm is deliberately NOT enabled. The matching strategy in
-- docs/data-profile.md is fully deterministic - exact normalised email, exact
-- normalised phone, then a guarded name-only pass that requires the name to be
-- unique within both files. Trigram similarity would only be needed if we
-- started matching on approximate names, and doing that on this dataset is
-- actively wrong: `Deepak Nair` is two different people, so a similarity score
-- would fuse them. Enabling an extension we do not use is one more thing to
-- justify with nothing to show for it.
--
-- If fuzzy name matching ever becomes necessary, this is the escalation:
--     CREATE EXTENSION IF NOT EXISTS pg_trgm;
--     CREATE INDEX ix_person_name_key_trgm ON person USING gin (name_key gin_trgm_ops);
--
-- gen_random_uuid() is in core since PG13, and pgcrypto is not needed either.
-- We use bigint identity columns regardless: they read better in a live demo
-- and this dataset is ~100 rows.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- Shared enum: which source file a row came from.
-- Used by both the raw and staging layers, so it lives in the foundation.
-- An enum rather than a text+CHECK because the set is closed and known: three
-- files, named in the assignment.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'source_system') THEN
        CREATE TYPE source_system AS ENUM (
            'naukri_applicants',   -- source1: has email AND phone (the bridge)
            'gig_workers',         -- source2: email only
            'cbnexus_contacts'     -- source3: phone only
        );
    END IF;
END
$$;


-- ---------------------------------------------------------------------------
-- Shared trigger function: maintain updated_at on mutable tables.
--
-- Only the golden layer uses this. The raw layer is append-only and has no
-- updated_at by design - if a raw row could change, it would stop being an
-- audit trail.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION set_updated_at() IS
    'BEFORE UPDATE trigger: stamps updated_at. Used by golden-layer tables only.';
