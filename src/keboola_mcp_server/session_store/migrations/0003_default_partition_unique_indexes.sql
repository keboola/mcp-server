-- 0002's composite (access_token_hash, created_at) index doesn't enforce hash uniqueness --
-- created_at differs per row. A plain index on the partition table itself does.
-- retention.ensure_partitions() adds the same pair on every new month partition.
CREATE UNIQUE INDEX oauth_sessions_default_access_token_hash_uidx ON oauth_sessions_default (access_token_hash);
CREATE UNIQUE INDEX oauth_sessions_default_refresh_token_hash_uidx ON oauth_sessions_default (refresh_token_hash)
    WHERE refresh_token_hash IS NOT NULL;
