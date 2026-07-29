"""Postgres-backed OAuth session storage (PSGO-261, oauth_session_persistence RFC).

Replaces the self-contained OAuth access/refresh JWTs with an opaque, server-side session
reference: the MCP client holds only a random lookup key, never the real Keboola credentials.
See ``feature_spec/oauth_session_persistence/RFC.md`` for the design.
"""
