-- oauth_session_persistence RFC: one row per OAuth-authenticated MCP session.
-- Real Keboola credentials are stored only as AES-256-GCM ciphertext (session_store/crypto.py);
-- the MCP client holds only the opaque access/refresh token, whose SHA-256 hash is looked up here.

CREATE TABLE oauth_sessions (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    access_token_hash        BYTEA NOT NULL,
    refresh_token_hash       BYTEA,
    client_id                TEXT NOT NULL,
    user_email               TEXT,
    kbc_access_token_enc     BYTEA NOT NULL,
    kbc_refresh_token_enc    BYTEA NOT NULL,
    kbc_access_expires_at    TIMESTAMPTZ NOT NULL,
    scope_project_ids        INTEGER[],
    scope_read_only          BOOLEAN NOT NULL DEFAULT FALSE,
    scope_confirmed          BOOLEAN NOT NULL DEFAULT FALSE,
    scope_scoped_token_enc   BYTEA,
    scope_scoped_expires_at  TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at               TIMESTAMPTZ
);

-- Unique so a hash collision (practically impossible with SHA-256) can't silently pick the
-- wrong session; also serves as the lookup index for the two access paths.
CREATE UNIQUE INDEX oauth_sessions_access_token_hash_idx ON oauth_sessions (access_token_hash);
CREATE UNIQUE INDEX oauth_sessions_refresh_token_hash_idx ON oauth_sessions (refresh_token_hash)
    WHERE refresh_token_hash IS NOT NULL;
