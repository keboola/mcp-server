-- AI-4005: counters for the OAuth rate limits that must hold across every replica of the server (calls to Connection's
-- client validation, new clients sent to the approval screen, /authorize requests per caller address). Each row is one
-- bucket in one fixed time window; the counters are incremented with a single atomic upsert, so no replica needs to
-- know about the others. Rows older than their window are of no use and are removed by `gc-sessions`.
-- No credential material is stored: the bucket names carry at most a derived client id or a caller address.

CREATE TABLE oauth_rate_buckets (
    bucket        TEXT        NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    hits          INTEGER     NOT NULL DEFAULT 1,
    PRIMARY KEY (bucket, window_start)
);

CREATE INDEX oauth_rate_buckets_window_start_idx ON oauth_rate_buckets (window_start);
