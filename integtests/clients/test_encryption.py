from typing import Any

import pytest

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient


def test_client_does_not_send_authorization_headers(keboola_client: KeboolaClient) -> None:
    """Check that the encryption client does not send any authorization headers."""
    assert 'Authorization' not in keboola_client.encryption_client.raw_client.headers
    assert 'X-StorageAPI-Token' not in keboola_client.encryption_client.raw_client.headers


@pytest.mark.asyncio
async def test_encrypt_string_not_equal(keboola_client: KeboolaClient) -> None:
    project_id = await keboola_client.storage_client.project_id()
    plaintext = 'my-plain-text'
    encrypted = await keboola_client.encryption_client.encrypt(
        value=plaintext,
        project_id=str(project_id),
        component_id=DATA_APP_COMPONENT_ID,
    )
    assert isinstance(encrypted, str)
    assert encrypted != plaintext


@pytest.mark.asyncio
async def test_encrypt_dict_hash_keys_only(keboola_client: KeboolaClient) -> None:
    project_id = await keboola_client.storage_client.project_id()
    payload: dict[str, Any] = {
        '#secret': 'sensitive-value',
        'public': 'visible-value',
    }
    result = await keboola_client.encryption_client.encrypt(
        value=payload,
        project_id=str(project_id),
        component_id=DATA_APP_COMPONENT_ID,
    )
    assert isinstance(result, dict)
    # Values under keys beginning with '#' should be encrypted (changed)
    assert result['#secret'] != payload['#secret']
    # Non-secret values should remain the same
    assert result['public'] == payload['public']
