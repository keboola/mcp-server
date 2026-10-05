-- AI-2883 dynamic OAuth client registration: records whether a session was granted the
-- 'projectless' (whole-stack) OAuth scope, so a session can advertise the scope it actually has.
-- The final design requests 'claudai projectless' for every registered client (RFC Decision §10),
-- so new sessions are always TRUE; FALSE only exists for a session persisted by an earlier build
-- of this change, where a dynamically-approved client got 'claudai' alone. Every row created before
-- this migration was a 'claudai projectless' grant -- hence the TRUE default/backfill.
ALTER TABLE oauth_sessions ADD COLUMN oauth_projectless BOOLEAN NOT NULL DEFAULT TRUE;
