-- kai_session_scope RFC (pat_token_support/RFC.md, increment 6): persisted multi-project scope
-- for deployed header-token (Kai) sessions. Kai's raw kbc_at_/kbc_pat_ token is refreshed
-- independently of this server and is not stable across that refresh, so rows are keyed by
-- sha256(conversation_id:user_id) instead of a token hash. No credential material is stored here
-- (unlike oauth_sessions) -- just the confirmed scope -- so no encryption is needed.

CREATE TABLE kai_sessions (
    session_key   BYTEA PRIMARY KEY,
    project_ids   INTEGER[] NOT NULL,
    read_only     BOOLEAN NOT NULL DEFAULT FALSE,
    confirmed     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
