-- DMD-2469: the RFC 8707 resource the client asked for at /authorize or /token, so /mcp can refuse a token that was
-- minted for some other resource. NULL for every session created before this column, and for a client that sent
-- no resource: those are accepted as before.
ALTER TABLE oauth_sessions ADD COLUMN resource TEXT;
