-- Creates the project role and databases on an existing PostgreSQL server.
-- Run once, as the postgres superuser.
--
-- The role password is passed in as a psql variable rather than written here,
-- so this file never contains a credential and can be committed safely. Keep it
-- identical to POSTGRES_PASSWORD in your .env.
--
-- PowerShell:
--   $env:PGPASSWORD = '<postgres superuser password>'
--   & 'C:\Program Files\PostgreSQL\18\bin\psql.exe' -U postgres `
--       -v app_password="'<app password>'" -f scripts/init_db.sql
--
-- bash:
--   PGPASSWORD='<postgres superuser password>' psql -U postgres \
--       -v app_password="'<app password>'" -f scripts/init_db.sql
--
-- Note the doubled quoting: -v app_password="'secret'". psql substitutes the
-- variable literally, so the inner single quotes are what make it a SQL string.
--
-- If the password contains '@', remember it must be percent-encoded as %40
-- inside DATABASE_URL in .env, though not in POSTGRES_PASSWORD.

-- Abort on the first error rather than ploughing on. This also gives the guard
-- below a way to exit non-zero: psql's \quit always exits 0, and \quit 1 is not
-- a thing (psql warns "extra argument ignored"), so a missing variable would
-- otherwise look like a successful run. Raising instead exits 3.
\set ON_ERROR_STOP on

\if :{?app_password}
\else
  \echo 'ERROR: app_password is not set.'
  \echo 'Re-run with:  psql -U postgres -v app_password="''yourpassword''" -f scripts/init_db.sql'
  DO $guard$ BEGIN RAISE EXCEPTION 'app_password is not set'; END $guard$;
\endif

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
\echo 'The role password must match POSTGRES_PASSWORD in .env.'
