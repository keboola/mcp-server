from typing import Any, Awaitable, Callable

import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.base import JsonDict, RawKeboolaClient
from keboola_mcp_server.clients.encryption import REDACTED_SECRET_VALUE, EncryptionClient
from keboola_mcp_server.clients.storage import AsyncStorageClient

WriteCall = Callable[[AsyncStorageClient, dict[str, Any]], Awaitable[JsonDict]]


def _create_config(client: AsyncStorageClient, configuration: dict[str, Any]) -> Awaitable[JsonDict]:
    return client.configuration_create(
        component_id='keboola.ex-test', name='test', description='test', configuration=configuration
    )


def _update_config(client: AsyncStorageClient, configuration: dict[str, Any]) -> Awaitable[JsonDict]:
    return client.configuration_update(
        component_id='keboola.ex-test',
        configuration_id='config-1',
        configuration=configuration,
        change_description='change',
    )


def _create_row(client: AsyncStorageClient, configuration: dict[str, Any]) -> Awaitable[JsonDict]:
    return client.configuration_row_create(
        component_id='keboola.ex-test', config_id='config-1', name='row', description='row', configuration=configuration
    )


def _update_row(client: AsyncStorageClient, configuration: dict[str, Any]) -> Awaitable[JsonDict]:
    return client.configuration_row_update(
        component_id='keboola.ex-test',
        config_id='config-1',
        configuration_row_id='row-1',
        configuration=configuration,
        change_description='change',
    )


WRITE_CALLS: list[WriteCall] = [_create_config, _update_config, _create_row, _update_row]


@pytest.fixture
def raw_client(mocker: MockerFixture) -> RawKeboolaClient:
    raw = mocker.AsyncMock(RawKeboolaClient)
    raw.post.return_value = {'id': 'config-1', 'version': 1}
    raw.put.return_value = {'id': 'config-1', 'version': 2}
    # used by project_id() -> GET tokens/verify
    raw.get.return_value = {'owner': {'id': 4214}}
    return raw


@pytest.fixture
def encryption_client(mocker: MockerFixture) -> EncryptionClient:
    return mocker.AsyncMock(EncryptionClient)


class TestConfigurationWriteEncryption:
    """The storage client must encrypt plaintext '#'-prefixed secrets before writing configurations."""

    @pytest.mark.parametrize('write_call', WRITE_CALLS)
    @pytest.mark.asyncio
    async def test_plaintext_secrets_are_encrypted_before_save(
        self, raw_client: RawKeboolaClient, encryption_client: EncryptionClient, write_call: WriteCall
    ) -> None:
        plaintext_config = {'parameters': {'user': 'admin', '#password': 'plain-secret'}}
        encrypted_config = {'parameters': {'user': 'admin', '#password': 'KBC::ProjectSecure::abcd'}}
        encryption_client.encrypt.return_value = encrypted_config

        client = AsyncStorageClient(raw_client=raw_client, encryption_client=encryption_client)
        await write_call(client, plaintext_config)

        encryption_client.encrypt.assert_called_once_with(
            plaintext_config, component_id='keboola.ex-test', project_id='4214'
        )
        # the payload sent to the Storage API must contain the encrypted configuration
        http_call = raw_client.post if raw_client.post.called else raw_client.put
        sent_payload = http_call.call_args.kwargs['data']
        assert sent_payload['configuration'] == encrypted_config

    @pytest.mark.parametrize(
        'configuration',
        [
            {'parameters': {'user': 'admin'}},  # no secrets at all
            {'parameters': {'#password': 'KBC::ProjectSecure::abcd'}},  # already encrypted
        ],
    )
    @pytest.mark.asyncio
    async def test_no_plaintext_secrets_skips_encryption(
        self,
        raw_client: RawKeboolaClient,
        encryption_client: EncryptionClient,
        configuration: dict[str, Any],
    ) -> None:
        client = AsyncStorageClient(raw_client=raw_client, encryption_client=encryption_client)
        await _create_config(client, configuration)

        encryption_client.encrypt.assert_not_called()
        sent_payload = raw_client.post.call_args.kwargs['data']
        assert sent_payload['configuration'] == configuration

    @pytest.mark.asyncio
    async def test_fails_closed_without_encryption_client(self, raw_client: RawKeboolaClient) -> None:
        client = AsyncStorageClient(raw_client=raw_client, encryption_client=None)

        with pytest.raises(ValueError, match='plaintext secret values'):
            await _create_config(client, {'parameters': {'#password': 'plain-secret'}})

        raw_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_rejects_redacted_placeholder_values(
        self, raw_client: RawKeboolaClient, encryption_client: EncryptionClient
    ) -> None:
        client = AsyncStorageClient(raw_client=raw_client, encryption_client=encryption_client)

        with pytest.raises(ValueError, match='redacted secret values'):
            await _create_config(client, {'parameters': {'#password': REDACTED_SECRET_VALUE}})

        encryption_client.encrypt.assert_not_called()
        raw_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_encryption_failure_aborts_save(
        self, raw_client: RawKeboolaClient, encryption_client: EncryptionClient
    ) -> None:
        encryption_client.encrypt.side_effect = RuntimeError('encryption service unavailable')
        client = AsyncStorageClient(raw_client=raw_client, encryption_client=encryption_client)

        with pytest.raises(RuntimeError, match='encryption service unavailable'):
            await _create_config(client, {'parameters': {'#password': 'plain-secret'}})

        raw_client.post.assert_not_called()
