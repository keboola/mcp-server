from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.base import InvalidArgumentError, JsonDict, RawKeboolaClient
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


class TestConfigurationFolder:
    @pytest.mark.parametrize('folder', ['Marketing', ''], ids=['set', 'clear'])
    @pytest.mark.asyncio
    async def test_configuration_folder_update(self, raw_client: RawKeboolaClient, folder: str) -> None:
        """The folder is sent alone in the update request, so no configuration version is created."""
        raw_client.put.return_value = {'id': 'config-1', 'folder': folder or None}
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        result = await client.configuration_folder_update('keboola.ex-test', 'config-1', folder)

        raw_client.put.assert_called_once_with(
            endpoint='branch/123/components/keboola.ex-test/configs/config-1',
            data={'folder': folder},
            params=None,
        )
        assert result == {'id': 'config-1', 'folder': folder or None}


class TestEmptyIds:
    @pytest.mark.parametrize(
        ('method', 'kwargs'),
        [
            ('configuration_detail', {'component_id': '', 'configuration_id': 'c1'}),
            ('configuration_detail', {'component_id': 'keboola.ex-test', 'configuration_id': ''}),
            ('configuration_list', {'component_id': ''}),
        ],
    )
    @pytest.mark.asyncio
    async def test_empty_id_raises_invalid_argument_before_any_request(
        self, raw_client: RawKeboolaClient, method: str, kwargs: dict[str, str]
    ) -> None:
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        with pytest.raises(InvalidArgumentError, match='Invalid'):
            await getattr(client, method)(**kwargs)

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

    @pytest.mark.asyncio
    async def test_bucket_link_sends_explicit_empty_display_name(self, raw_client: RawKeboolaClient) -> None:
        """An explicit empty string must still be sent, not dropped like the None default."""
        raw_client.post.return_value = {'id': 'in.c-linked'}
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        await client.bucket_link(
            name='linked', stage='in', source_project_id='proj-1', source_bucket_id='in.c-foo', display_name=''
        )

        assert raw_client.post.call_args.kwargs['data']['displayName'] == ''

    @pytest.mark.asyncio
    async def test_bucket_link_omits_display_name_when_not_given(self, raw_client: RawKeboolaClient) -> None:
        raw_client.post.return_value = {'id': 'in.c-linked'}
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        await client.bucket_link(name='linked', stage='in', source_project_id='proj-1', source_bucket_id='in.c-foo')

        assert 'displayName' not in raw_client.post.call_args.kwargs['data']


class TestTableDefinitionUpdate:
    """The native definition update is an asynchronous Storage job that the client waits for."""

    @pytest.mark.parametrize(
        ('kwargs', 'expected_payload'),
        [
            (
                {'description': 'Table desc'},
                {'description': 'Table desc', 'isDescriptionSystemManaged': False},
            ),
            (
                {'description': '', 'column_descriptions': {'a': 'A desc'}},
                {
                    'description': '',
                    'isDescriptionSystemManaged': False,
                    'columns': [{'name': 'a', 'description': 'A desc'}],
                },
            ),
            (
                {'column_descriptions': {'a': 'A desc', 'b': 'B desc'}},
                {'columns': [{'name': 'a', 'description': 'A desc'}, {'name': 'b', 'description': 'B desc'}]},
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_payload_and_polling(
        self, mocker: MockerFixture, raw_client: RawKeboolaClient, kwargs: dict[str, Any], expected_payload: dict
    ) -> None:
        mocker.patch('keboola_mcp_server.clients.storage.asyncio.sleep', new=mocker.AsyncMock())
        raw_client.put.return_value = {'id': 7, 'status': 'waiting'}
        raw_client.get.side_effect = [{'id': 7, 'status': 'processing'}, {'id': 7, 'status': 'success'}]
        client = AsyncStorageClient(raw_client=raw_client, branch_id='123')

        job = await client.table_definition_update('in.c-b.t', **kwargs)

        assert job['status'] == 'success'
        raw_client.put.assert_called_once_with(
            endpoint='branch/123/tables/in.c-b.t/definition', data=expected_payload, params=None
        )
        assert raw_client.get.call_count == 2

    @pytest.mark.parametrize('initial_status', ['error', 'processing'])
    @pytest.mark.asyncio
    async def test_failed_job_raises(
        self, mocker: MockerFixture, raw_client: RawKeboolaClient, initial_status: str
    ) -> None:
        mocker.patch('keboola_mcp_server.clients.storage.asyncio.sleep', new=mocker.AsyncMock())
        failed = {'id': 7, 'status': 'error', 'error': {'message': 'Backend failed'}}
        raw_client.put.return_value = failed if initial_status == 'error' else {'id': 7, 'status': initial_status}
        raw_client.get.return_value = failed
        client = AsyncStorageClient(raw_client=raw_client)

        with pytest.raises(RuntimeError, match='Backend failed'):
            await client.table_definition_update('in.c-b.t', description='x')

    @pytest.mark.asyncio
    async def test_unfinished_job_times_out(self, mocker: MockerFixture, raw_client: RawKeboolaClient) -> None:
        mocker.patch('keboola_mcp_server.clients.storage.asyncio.sleep', new=mocker.AsyncMock())
        mocker.patch.object(AsyncStorageClient, 'JOB_TIMEOUT_SECONDS', -1.0)
        raw_client.put.return_value = {'id': 7, 'status': 'processing'}
        client = AsyncStorageClient(raw_client=raw_client)

        with pytest.raises(TimeoutError, match='did not finish'):
            await client.table_definition_update('in.c-b.t', description='x')

    @pytest.mark.parametrize(
        ('status_code', 'message', 'is_noop'),
        [
            (400, 'API error: No table definition changes were provided.', True),
            (400, 'API error: Invalid column.', False),
            (404, 'No table definition changes were provided.', False),
        ],
    )
    @pytest.mark.asyncio
    async def test_unchanged_values_are_a_noop(
        self, raw_client: RawKeboolaClient, status_code: int, message: str, is_noop: bool
    ) -> None:
        raw_client.put.side_effect = httpx.HTTPStatusError(
            message, request=httpx.Request('PUT', 'https://x'), response=httpx.Response(status_code)
        )
        client = AsyncStorageClient(raw_client=raw_client)

        if is_noop:
            assert await client.table_definition_update('in.c-b.t', description='x') == {}
        else:
            with pytest.raises(httpx.HTTPStatusError):
                await client.table_definition_update('in.c-b.t', description='x')

    @pytest.mark.asyncio
    async def test_nothing_to_update_raises(self, raw_client: RawKeboolaClient) -> None:
        client = AsyncStorageClient(raw_client=raw_client)

        with pytest.raises(ValueError, match='At least one'):
            await client.table_definition_update('in.c-b.t')
        raw_client.put.assert_not_called()
