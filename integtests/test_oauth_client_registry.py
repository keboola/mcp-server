"""
`ConnectionClientRegistry.check_registration()` (AI-2883, feature_spec/oauth_dynamic_client_registration)
against a real, live Connection instance -- not the mocked transport `tests/test_oauth.py` uses.

Unauthenticated and read-only (POST /oauth/clients/validate never touches a project), so this
needs no project lock -- just `storage_api_url`, which is also Connection's own host (see
`server.py`'s `oauth_server_url` default: `https://connection.{hostname_suffix}`, the same host
`storage_api_url` already points at).

Deliberately narrower than a full Allow/Deny click-through: Connection's own live-stack E2E suite
(`connection/tests/E2E/Auth/McpClientValidationTest.php`) already covers that interactive path.
What was missing -- and what this closes -- is proof that this server's own `check_registration()`
maps Connection's *real* response codes correctly, not just a mocked 200/404.
"""

import uuid

import pytest

from keboola_mcp_server.oauth import ConnectionClientRegistry, _ClientRegistration

# Pre-registered by Connection's PreRegisterClaudeAiOAuthClientMigration -- present on every stack
# (the migration runs on RUN_ON_MIGRATE | RUN_ON_INIT).
_PRE_REGISTERED_CLIENT_ID = 'claude-ai'
_PRE_REGISTERED_REDIRECT_URI = 'https://claude.ai/api/mcp/auth_callback'


@pytest.mark.asyncio
async def test_check_registration_accepts_the_pre_registered_claude_ai_pair(storage_api_url: str):
    registry = ConnectionClientRegistry(storage_api_url)

    result = await registry.check_registration(_PRE_REGISTERED_CLIENT_ID, _PRE_REGISTERED_REDIRECT_URI)

    assert result is _ClientRegistration.REGISTERED


@pytest.mark.asyncio
async def test_check_registration_rejects_an_unregistered_pair(storage_api_url: str):
    registry = ConnectionClientRegistry(storage_api_url)

    # A client_id no migration or approval flow could plausibly have created.
    result = await registry.check_registration(f'mcp-test-{uuid.uuid4().hex[:16]}', 'https://example.com/callback')

    assert result is _ClientRegistration.NOT_REGISTERED
