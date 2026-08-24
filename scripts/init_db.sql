-- Creates the project role and databases on an existing PostgreSQL server.
-- Run once, as the postgres superuser:
--
--   & "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -f scripts/init_db.sql
--
-- psql prompts for the postgres superuser password. The role password below
-- must match POSTGRES_PASSWORD in .env (and be percent-encoded in DATABASE_URL:
-- @ becomes %40, or the DSN parses the host wrong).

-- Application role. NOSUPERUSER on purpose: the pipeline only ever needs to own
-- its own schema, and a least-privilege role is one less thing to explain.
DROP ROLE IF EXISTS consultbae;
CREATE ROLE consultbae WITH LOGIN PASSWORD :app_password NOSUPERUSER NOCREATEROLE CREATEDB;

-- Main database.
DROP DATABASE IF EXISTS consultbae;
CREATE DATABASE consultbae OWNER consultbae ENCODING 'UTF8';

-- Separate database for pytest, so a test run can never touch real data.
DROP DATABASE IF EXISTS consultbae_test;
CREATE DATABASE consultbae_test OWNER consultbae ENCODING 'UTF8';

\echo 'Created role consultbae and databases consultbae, consultbae_test.'
\echo 'Role password must match POSTGRES_PASSWORD in .env.'
