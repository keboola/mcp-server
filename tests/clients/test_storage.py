from collections.abc import Awaitable, Callable
from typing import Any

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


class TestSearchEndpoints:
    """The storage client must build correct query parameters for the SAPI search endpoints."""

    @pytest.mark.parametrize(
        ('branch_id', 'branch_scope', 'expected_branch_params'),
        [
            (None, 'current', {'branchTypes[]': 'production'}),
            ('123', 'current', {'branchTypes[]': 'development', 'branchIds[]': '123'}),
            (None, 'all', {}),
            ('123', 'all', {}),
        ],
        ids=['default_branch_current', 'dev_branch_current', 'default_branch_all', 'dev_branch_all'],
    )
    @pytest.mark.asyncio
    async def test_global_search_branch_scope(
        self,
        raw_client: RawKeboolaClient,
        branch_id: str | None,
        branch_scope: str,
        expected_branch_params: dict[str, Any],
    ) -> None:
        async def get_side_effect(endpoint: str, params: dict[str, Any] | None = None, **kwargs: Any) -> JsonDict:
            if endpoint == 'tokens/verify':
                return {'owner': {'id': 4214}}
            assert endpoint == 'global-search'
            return {'all': 0, 'items': [], 'byType': {}, 'byProject': {}}

        raw_client.get.side_effect = get_side_effect
        client = AsyncStorageClient(raw_client=raw_client, branch_id=branch_id)

        await client.global_search('foo', limit=10, offset=5, branch_scope=branch_scope)

        params = raw_client.get.call_args.kwargs['params']
        assert params == {'query': 'foo', 'projectIds[]': ['4214'], 'limit': 10, 'offset': 5, **expected_branch_params}

    @pytest.mark.asyncio
    async def test_component_configurations_search_params(self, raw_client: RawKeboolaClient) -> None:
        raw_client.get.return_value = [{'id': 'config-1', 'componentId': 'keboola.ex-test'}]
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        result = await client.component_configurations_search(
            component_id='keboola.ex-test',
            metadata_keys=['KBC.configuration.folderName', 'KBC.other'],
        )

        raw_client.get.assert_called_once_with(
            endpoint='branch/123/search/component-configurations',
            params={
                'componentId': 'keboola.ex-test',
                'metadataKeys[0]': 'KBC.configuration.folderName',
                'metadataKeys[1]': 'KBC.other',
            },
        )
        assert result == [{'id': 'config-1', 'componentId': 'keboola.ex-test'}]

    @pytest.mark.asyncio
    async def test_component_configurations_search_requires_filter(self, raw_client: RawKeboolaClient) -> None:
        client = AsyncStorageClient(raw_client=raw_client)
        assert await client.component_configurations_search() == []
        raw_client.get.assert_not_called()


class TestSharedBuckets:
    @pytest.mark.asyncio
    async def test_shared_bucket_list_uses_branch_scoped_endpoint(self, raw_client: RawKeboolaClient) -> None:
        raw_client.get.return_value = [{'id': 'in.c-foo'}]
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        result = await client.shared_bucket_list()

        raw_client.get.assert_called_once_with(endpoint='branch/123/shared-buckets', params=None)
        assert result == [{'id': 'in.c-foo'}]

    @pytest.mark.asyncio
    async def test_shared_bucket_list_branch_id_override(self, raw_client: RawKeboolaClient) -> None:
        raw_client.get.return_value = []
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        await client.shared_bucket_list(branch_id='456')

        raw_client.get.assert_called_once_with(endpoint='branch/456/shared-buckets', params=None)

    @pytest.mark.asyncio
    async def test_bucket_link_posts_expected_payload(self, raw_client: RawKeboolaClient) -> None:
        raw_client.post.return_value = {'id': 'in.c-linked'}
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        result = await client.bucket_link(
            name='linked', stage='in', source_project_id='proj-1', source_bucket_id='in.c-foo'
        )

        raw_client.post.assert_called_once_with(
            endpoint='branch/123/buckets',
            data={'name': 'linked', 'stage': 'in', 'sourceProjectId': 'proj-1', 'sourceBucketId': 'in.c-foo'},
            params=None,
            timeout=None,
        )
        assert result == {'id': 'in.c-linked'}

    @pytest.mark.asyncio
    async def test_bucket_link_includes_optional_display_name(self, raw_client: RawKeboolaClient) -> None:
        raw_client.post.return_value = {'id': 'in.c-linked'}
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        await client.bucket_link(
            name='linked',
            stage='in',
            source_project_id='proj-1',
            source_bucket_id='in.c-foo',
            display_name='Linked Bucket',
        )

        assert raw_client.post.call_args.kwargs['data']['displayName'] == 'Linked Bucket'
