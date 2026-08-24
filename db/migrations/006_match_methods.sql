-- 006_match_methods.sql
-- Rename the match_method enum values to the agreed vocabulary.
--
-- 004 named these email_exact / phone_exact / name_guarded / singleton /
-- manual. The matching layer's contract names them exact_email / exact_phone /
-- guarded_name / unmatched / manual_review. Renaming beats maintaining a
-- translation table between the code and the database, where every future
-- reader would have to learn both halves.
--
-- ALTER TYPE ... RENAME VALUE has existed since PostgreSQL 10, so this stays
-- within the 16-compatible target. Renaming is safe with rows already present -
-- the value's identity is its OID, not its label - and person_source_link is
-- empty at this point regardless.

ALTER TYPE match_method RENAME VALUE 'email_exact'  TO 'exact_email';
ALTER TYPE match_method RENAME VALUE 'phone_exact'  TO 'exact_phone';
ALTER TYPE match_method RENAME VALUE 'name_guarded' TO 'guarded_name';
ALTER TYPE match_method RENAME VALUE 'manual'       TO 'manual_review';

-- 'singleton' described the cluster; 'unmatched' describes the link, which is
-- what this column actually records: this source row was tied to its person by
-- nothing except being itself.
ALTER TYPE match_method RENAME VALUE 'singleton'    TO 'unmatched';

COMMENT ON TYPE match_method IS
    'How a source row was tied to its person. exact_email/exact_phone are '
    'deterministic identifier matches; guarded_name passes the five-condition '
    'name rule; unmatched means the row is its own person; manual_review means '
    'a human resolved it from match_review.';
