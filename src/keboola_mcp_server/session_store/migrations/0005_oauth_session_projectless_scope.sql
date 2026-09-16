-- AI-2883 dynamic OAuth client registration: a Flow B (dynamically-approved) client's session
-- must not advertise the 'projectless' (whole-stack) OAuth scope when Connection only ever
-- granted it 'claudai' -- see oauth.py's _scope_for docstring. Every row created before this
-- migration predates that distinction (dynamic registration didn't exist yet), so it was always
-- a 'claudai projectless' grant -- hence the TRUE default/backfill.
ALTER TABLE oauth_sessions ADD COLUMN oauth_projectless BOOLEAN NOT NULL DEFAULT TRUE;
