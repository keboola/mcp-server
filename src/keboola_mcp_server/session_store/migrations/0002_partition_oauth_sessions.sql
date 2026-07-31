-- Partitions oauth_sessions by month (RANGE on created_at) for time-boundable retention (RFC
-- oauth_session_persistence, "Session expiry / cleanup" open question). Dropping a whole month is
-- an instant DROP TABLE, no VACUUM needed, unlike a DELETE ... WHERE sweep. See
-- session_store/retention.py for the ongoing monthly maintenance (creates upcoming partitions
-- ahead of time, drops ones older than the retention window) -- this migration only performs the
-- one-time structural conversion.
--
-- Trade-off (explicit, not accidental): PostgreSQL requires a partitioned table's UNIQUE/PRIMARY
-- KEY indexes to include the partition key. access_token_hash/refresh_token_hash/id can therefore
-- only be enforced unique WITHIN a partition (a calendar month), not table-wide. A same-hash
-- collision across two different months on a 256-bit random token is cryptographically negligible
-- -- an acceptable relaxation, not a real gap.
--
-- Recreates the table rather than converting it in place, copying any existing rows across (they
-- land in whichever partition -- or the DEFAULT catch-all -- their created_at falls into). Safe
-- because no production OAuth sessions exist on this schema yet (dev/testing stacks only).

ALTER TABLE oauth_sessions RENAME TO oauth_sessions_pre_partition;

-- Index/constraint names are global per-schema, not per-table -- renaming the table alone leaves
-- these attached to it under their old names, colliding with the new table's indexes below.
ALTER TABLE oauth_sessions_pre_partition RENAME CONSTRAINT oauth_sessions_pkey TO oauth_sessions_pre_partition_pkey;
ALTER INDEX oauth_sessions_access_token_hash_idx RENAME TO oauth_sessions_pre_partition_access_token_hash_idx;
ALTER INDEX oauth_sessions_refresh_token_hash_idx RENAME TO oauth_sessions_pre_partition_refresh_token_hash_idx;

CREATE TABLE oauth_sessions (
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
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
    revoked_at               TIMESTAMPTZ,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Plain (non-unique) index on id alone: revoke()/rotate_kbc_tokens()/rotate_opaque_tokens() all
-- look up by id, and the composite PK above doesn't help a query that only has id.
CREATE INDEX oauth_sessions_id_idx ON oauth_sessions (id);

CREATE UNIQUE INDEX oauth_sessions_access_token_hash_idx ON oauth_sessions (access_token_hash, created_at);
CREATE UNIQUE INDEX oauth_sessions_refresh_token_hash_idx ON oauth_sessions (refresh_token_hash, created_at)
    WHERE refresh_token_hash IS NOT NULL;

-- Catch-all for rows outside any explicit month partition -- notably the rows copied over from
-- oauth_sessions_pre_partition below, and a safety net if partition maintenance ever lags. Never
-- touched by session_store/retention.py's cleanup (only oauth_sessions_YYYY_MM names are).
CREATE TABLE oauth_sessions_default PARTITION OF oauth_sessions DEFAULT;

-- This month's and next month's partitions, so writes never fail for lack of one -- a
-- RANGE-partitioned INSERT with no matching partition raises immediately, it does not fall
-- through to a partition created moments later. session_store/retention.py takes over creating
-- further-ahead partitions (and dropping old ones) every month after this.
DO $$
DECLARE
    this_month DATE := date_trunc('month', now());
    next_month DATE := this_month + INTERVAL '1 month';
BEGIN
    EXECUTE format(
        'CREATE TABLE oauth_sessions_%s PARTITION OF oauth_sessions FOR VALUES FROM (%L) TO (%L)',
        to_char(this_month, 'YYYY_MM'), this_month, next_month
    );
    EXECUTE format(
        'CREATE TABLE oauth_sessions_%s PARTITION OF oauth_sessions FOR VALUES FROM (%L) TO (%L)',
        to_char(next_month, 'YYYY_MM'), next_month, next_month + INTERVAL '1 month'
    );
END $$;

INSERT INTO oauth_sessions SELECT * FROM oauth_sessions_pre_partition;

DROP TABLE oauth_sessions_pre_partition;
