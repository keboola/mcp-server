import importlib.metadata
import json
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import httpx
import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.base import RawKeboolaClient
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.clients.storage import AsyncStorageClient
from keboola_mcp_server.config import ServerRuntimeInfo
from keboola_mcp_server.mcp import SessionStateMiddleware


@pytest.fixture
def keboola_client() -> KeboolaClient:
    return KeboolaClient(storage_api_url='https://connection.nowhere', storage_api_token='test-token')


@pytest.fixture
def mock_http_request() -> httpx.Request:
    """Create a mock HTTP request."""
    request = Mock(spec=httpx.Request)
    request.url = 'https://api.example.com/test'
    request.method = 'GET'
    return request


@pytest.fixture
def mock_http_response_500(mock_http_request: httpx.Request) -> httpx.Response:
    """Create a mock HTTP response with 500 status."""
    response = Mock(spec=httpx.Response)
    response.status_code = 500
    response.reason_phrase = 'Internal Server Error'
    response.url = 'https://api.example.com/test'
    response.request = mock_http_request
    response.is_error = True
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        message=f"{response.reason_phrase} for url '{response.url}'", request=mock_http_request, response=response
    )
    return response


@pytest.fixture
def mock_http_response_404(mock_http_request: httpx.Request) -> httpx.Response:
    """Create a mock HTTP response with 404 status."""
    response = Mock(spec=httpx.Response)
    response.status_code = 404
    response.reason_phrase = 'Not Found'
    response.url = 'https://api.example.com/test'
    response.request = mock_http_request
    response.is_error = True
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        message=f"{response.reason_phrase} for url '{response.url}'", request=mock_http_request, response=response
    )
    return response


class TestRawKeboolaClient:
    """Test suite for enhanced HTTP client error handling."""

    @pytest.fixture
    def raw_client(self) -> RawKeboolaClient:
        """Create a RawKeboolaClient instance for testing."""
        return RawKeboolaClient(base_api_url='https://api.example.com', api_token='test-token')

    def test_raise_for_status_500_with_exception_id(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors are enhanced with exception ID when available."""

        # Mock response with valid JSON containing exception ID
        mock_http_response_500.json.return_value = {
            'exceptionId': 'exc-123-456',
            'message': 'Application error',
            'errorCode': 'DB_ERROR',
            'requestId': 'req-789',
        }

        match = (
            "Internal Server Error for url 'https://api.example.com/test'\n"
            'Exception ID: exc-123-456\n'
            'When contacting Keboola support please provide the exception ID.'
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_500_without_exception_id(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors without exception ID fall back gracefully."""

        # Mock response with JSON but no exception ID
        mock_http_response_500.json.return_value = {'message': 'Internal server error', 'errorCode': 'INTERNAL_ERROR'}

        with pytest.raises(httpx.HTTPStatusError, match="Internal Server Error for url 'https://api.example.com/test'"):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_500_with_malformed_json(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors with malformed JSON fall back to standard error handling."""

        # Mock response with invalid JSON
        type(mock_http_response_500).text = PropertyMock(return_value='Invalid JSON')
        mock_http_response_500.json.side_effect = ValueError('Invalid JSON')

        match = "Internal Server Error for url 'https://api.example.com/test'\nAPI error: Invalid JSON"
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_404_uses_standard_exception(
        self, raw_client: RawKeboolaClient, mock_http_response_404: httpx.Response
    ):
        """Test that HTTP 404 errors use standard HTTPStatusError."""

        mock_http_response_404.json.return_value = {
            'exceptionId': 'exc-123-456',
            'error': 'The bucket "foo.bar.baz" was not found in the project "123"',
            'code': 'storage.buckets.notFound',
        }

        match = (
            "Not Found for url 'https://api.example.com/test'\n"
            'API error: The bucket "foo.bar.baz" was not found in the project "123"\n'
            'Exception ID: exc-123-456\n'
            'When contacting Keboola support please provide the exception ID.'
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_404)

    @pytest.mark.asyncio
    async def test_get_method_integration_with_enhanced_error_handling(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that GET method integrates with enhanced error handling."""

        # Mock the HTTP client to return a 500 error
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.get.return_value = mock_http_response_500
            mock_http_response_500.json.return_value = {'exceptionId': 'test-exc-123', 'message': 'Test error message'}

            match = (
                "Internal Server Error for url 'https://api.example.com/test'\n"
                'Exception ID: test-exc-123\n'
                'When contacting Keboola support please provide the exception ID.'
            )
            with pytest.raises(httpx.HTTPStatusError, match=match):
                await raw_client.get('test-endpoint')

    @pytest.mark.asyncio
    async def test_post_preserves_non_ascii_characters(self, raw_client: RawKeboolaClient):
        """Test that POST requests preserve non-ASCII characters (e.g. Czech diacritics) in JSON payloads."""
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.post.return_value = (response := Mock(spec=httpx.Response))
            response.status_code = 200
            response.json.return_value = {}

            data = {'script': "SELECT * WHERE name = 'Česká republika'"}
            await raw_client.post('test-endpoint', data=data)

            call_kwargs = mock_client.post.call_args
            content_bytes = call_kwargs.kwargs['content']
            content_str = content_bytes.decode('utf-8')

            # Verify non-ASCII characters are preserved, not escaped to \uXXXX
            assert 'Česká republika' in content_str
            assert '\\u010c' not in content_str


class TestAsyncStorageClient:
    @pytest.fixture
    def storage_client(self, mocker: MockerFixture) -> AsyncStorageClient:
        raw = mocker.AsyncMock(RawKeboolaClient)
        return AsyncStorageClient(raw_client=raw, branch_id=None)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('limit', 'offset', 'expected_params'),
        [
            pytest.param(50, 0, {'runId': '456', 'limit': 50, 'offset': 0, 'forceUuid': 'true'}, id='basic'),
            pytest.param(10, 100, {'runId': '456', 'limit': 10, 'offset': 100, 'forceUuid': 'true'}, id='with_offset'),
        ],
    )
    async def test_list_events(
        self,
        storage_client: AsyncStorageClient,
        limit: int,
        offset: int,
        expected_params: dict[str, Any],
    ):
        """Tests list_events calls the correct endpoint with the right params."""
        storage_client.raw_client.get.return_value = []

        await storage_client.list_events(job_id='456', limit=limit, offset=offset)

        storage_client.raw_client.get.assert_called_once_with(
            endpoint='events',
            params=expected_params,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('message', 'component_id', 'configuration_id', 'event_type', 'params', 'results', 'duration', 'run_id'),
        [
            ('foo', 'bar', None, None, None, None, None, None),
            ('foo', 'bar', 'baz', 'error', {'param1': 'value1'}, {'result1': 'value1'}, 123, '987654321'),
        ],
    )
    async def test_trigger_event(
        self,
        message: str,
        component_id: str,
        configuration_id: str | None,
        event_type: str | None,
        params: Mapping[str, Any] | None,
        results: Mapping[str, Any],
        duration: int | None,
        run_id: str | None,
        keboola_client: KeboolaClient,
    ):
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.post.return_value = (response := Mock(spec=httpx.Response))
            response.status_code = 200
            response.json.return_value = {'id': '13008826', 'uuid': '01958f48-b1fc-7f05-b9b9-8a4a7b385bc3'}

            result = await keboola_client.storage_client.trigger_event(
                message=message,
                component_id=component_id,
                configuration_id=configuration_id,
                event_type=event_type,
                params=params,
                results=results,
                duration=duration,
                run_id=run_id,
            )

            assert result == {'id': '13008826', 'uuid': '01958f48-b1fc-7f05-b9b9-8a4a7b385bc3'}
            expected_payload = {
                key: value
                for key, value in [
                    ('message', message),
                    ('component', component_id),
                    ('configurationId', configuration_id),
                    ('type', event_type),
                    ('params', params),
                    ('results', results),
                    ('duration', duration),
                    ('runId', run_id),
                ]
                if value
            }
            mock_client.post.assert_called_once_with(
                'https://connection.nowhere/v2/storage/events',
                params=None,
                headers={
                    'Content-Type': 'application/json',
                    'Accept-Encoding': 'gzip',
                    'X-StorageAPI-Token': 'test-token',
                },
                content=json.dumps(expected_payload, ensure_ascii=False).encode('utf-8'),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('description', 'component_access', 'expires_in', 'expected_data'),
        [
            # Basic token creation with just description
            ('Test token', None, None, {'description': 'Test token'}),
            # Token with component access
            (
                'OAuth token',
                ['keboola.ex-google-analytics-v4'],
                None,
                {'description': 'OAuth token', 'componentAccess': ['keboola.ex-google-analytics-v4']},
            ),
            # Token with expiration
            ('Short-lived token', None, 3600, {'description': 'Short-lived token', 'expiresIn': 3600}),
            # Token with all parameters
            (
                'Full token',
                ['keboola.ex-gmail', 'keboola.ex-google-analytics-v4'],
                7200,
                {
                    'description': 'Full token',
                    'componentAccess': ['keboola.ex-gmail', 'keboola.ex-google-analytics-v4'],
                    'expiresIn': 7200,
                },
            ),
        ],
    )
    async def test_token_create(
        self,
        description: str,
        component_access: list[str] | None,
        expires_in: int | None,
        expected_data: dict[str, Any],
        keboola_client: KeboolaClient,
    ):
        """Test token creation with various parameter combinations."""
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.post.return_value = (response := Mock(spec=httpx.Response))
            response.status_code = 201
            response.json.return_value = {
                'id': '12345',
                'token': 'KBC_TOKEN_TEST_12345',
                'description': description,
                'created': '2023-01-01T00:00:00+00:00',
                'expiresIn': expires_in,
                'componentAccess': component_access or [],
            }

            result = await keboola_client.storage_client.token_create(
                description=description, component_access=component_access, expires_in=expires_in
            )

            # Verify the response
            assert result['token'] == 'KBC_TOKEN_TEST_12345'
            assert result['description'] == description

            # Verify the API call was made with correct parameters
            mock_client.post.assert_called_once_with(
                'https://connection.nowhere/v2/storage/tokens',
                params=None,
                headers={
                    'Content-Type': 'application/json',
                    'Accept-Encoding': 'gzip',
                    'X-StorageAPI-Token': 'test-token',
                },
                content=json.dumps(expected_data, ensure_ascii=False).encode('utf-8'),
            )


class TestKeboolaClient:
    @pytest.fixture
    def runtime_config(self) -> ServerRuntimeInfo:
        return ServerRuntimeInfo(transport='stdio', server_id='test')

    @pytest.fixture
    def keboola_client_with_headers(self, runtime_config: ServerRuntimeInfo) -> KeboolaClient:
        headers = SessionStateMiddleware._get_headers(runtime_config)
        return KeboolaClient(
            storage_api_url='https://connection.nowhere', storage_api_token='test-token', headers=headers
        )

    @pytest.mark.asyncio
    async def test_keboola_client_passing_headers(self, keboola_client_with_headers: KeboolaClient):
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.get.return_value = (response := Mock(spec=httpx.Response))
            response.status_code = 201
            response.json.return_value = {'test': 'test'}
            result = await keboola_client_with_headers.storage_client.verify_token()
            assert result == {'test': 'test'}
            kbc_version = importlib.metadata.version('keboola-mcp-server')
            mcp_version = importlib.metadata.version('mcp')
            fastmcp_version = importlib.metadata.version('fastmcp')
            mock_client.get.assert_called_once_with(
                'https://connection.nowhere/v2/storage/tokens/verify',
                params=None,
                headers={
                    'Content-Type': 'application/json',
                    'Accept-Encoding': 'gzip',
                    'X-StorageAPI-Token': 'test-token',
                    'User-Agent': f'Keboola MCP Server/{kbc_version} app_env=local transport=stdio',
                    'MCP-Server-Transport': 'stdio',
                    'MCP-Server-Versions': (
                        f'keboola-mcp-server/{kbc_version} mcp/{mcp_version} fastmcp/{fastmcp_version}'
                    ),
                },
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status_code', 'expected_match'),
        [
            (404, 'Branch "non-existent-branch" not found'),
            (500, 'Internal Server Error'),
        ],
        ids=['not_found', 'server_error'],
    )
    async def test_with_branch_id_http_error(
        self, keboola_client: KeboolaClient, status_code: int, expected_match: str
    ):
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())

            response = Mock(spec=httpx.Response)
            response.status_code = status_code
            response.is_error = True
            response.text = '{"error":"some error"}'
            response.json.return_value = {'error': 'some error'}
            response.request = Mock(spec=httpx.Request)
            response.raise_for_status.side_effect = httpx.HTTPStatusError(
                'Internal Server Error', request=response.request, response=response
            )
            mock_client.get.return_value = response

            with pytest.raises(httpx.HTTPStatusError, match=expected_match):
                await keboola_client.with_branch_id('non-existent-branch')
            mock_client.get.assert_called_once()

    def test_metastore_client_url_derivation(self) -> None:
        client = KeboolaClient(
            storage_api_url='https://connection.canary-orion.keboola.dev',
            storage_api_token='sapi_token_456',
        )

        assert client.metastore_client.raw_client.base_api_url == 'https://metastore.canary-orion.keboola.dev'
        assert client.metastore_client.raw_client.headers['X-StorageAPI-Token'] == 'sapi_token_456'

    # The clients below use the bearer token (Authorization header) when one is available and fall
    # back to the raw storage token (X-StorageAPI-Token) otherwise. Data-science needs it for
    # admin-context git-credential endpoints (AI-3398). Jobs-queue/AI-service/sync-actions are
    # deliberately NOT in this list — see test_queue_backed_clients_never_use_bearer_token.
    @pytest.mark.parametrize(
        'client_attr',
        [
            'scheduler_client',
            'metastore_client',
            'data_science_client',
        ],
    )
    @pytest.mark.parametrize(
        ('bearer_token', 'storage_token', 'expected_token'),
        [
            ('oauth_bearer_123', 'sapi_token_456', 'Bearer oauth_bearer_123'),
            (None, 'sapi_token_456', 'sapi_token_456'),
            ('', 'sapi_token_456', 'sapi_token_456'),
        ],
        ids=['with_bearer_token', 'without_bearer_token', 'empty_bearer_token'],
    )
    def test_client_bearer_token_selection(
        self, client_attr: str, bearer_token: str | None, storage_token: str, expected_token: str
    ):
        """Clients use the bearer token when available, falling back to the storage token."""
        client = KeboolaClient(
            storage_api_url='https://connection.keboola.com',
            storage_api_token=storage_token,
            bearer_token=bearer_token,
        )

        headers = getattr(client, client_attr).raw_client.headers

        if expected_token.startswith('Bearer '):
            assert headers.get('Authorization') == expected_token
            assert 'X-StorageAPI-Token' not in headers
        else:
            assert headers.get('X-StorageAPI-Token') == expected_token
            assert 'Authorization' not in headers

    @pytest.mark.parametrize(
        'client_attr',
        ['jobs_queue_client', 'ai_service_client', 'sync_actions_client'],
    )
    def test_queue_backed_clients_never_use_bearer_token(self, client_attr: str) -> None:
        """Regression for INC-02580 / SUPPORT-17416.

        The Job Queue re-sends the credential it receives to Storage as a legacy Storage token
        (NewJobFactory, hardcoded AuthType::STORAGE_TOKEN), so an OAuth bearer forwarded here comes
        back as `Invalid access token` and every run_job of an OAuth session fails. These three
        clients must therefore always send the Storage token, even when a bearer token is present.
        """
        client = KeboolaClient(
            storage_api_url='https://connection.keboola.com',
            storage_api_token='sapi_token_456',
            bearer_token='oauth_bearer_123',
        )

        headers = getattr(client, client_attr).raw_client.headers

        assert headers.get('X-StorageAPI-Token') == 'sapi_token_456'
        assert 'Authorization' not in headers


def test_flow_schema_cache_roundtrip():
    client = KeboolaClient(
        storage_api_url='https://connection.keboola.com',
        storage_api_token='dummy-token',
    )
    assert client.get_cached_flow_schema('keboola.flow') is None
    schema = {'type': 'object'}
    client.cache_flow_schema('keboola.flow', schema)
    assert client.get_cached_flow_schema('keboola.flow') is schema
    # other flow types are independent
    assert client.get_cached_flow_schema('keboola.orchestrator') is None


@pytest.mark.parametrize(
    ('metadata', 'key', 'provider', 'preferred_providers', 'default', 'expected'),
    [
        # Basic retrieval by key
        (
            [{'key': 'description', 'value': 'Test description'}, {'key': 'owner', 'value': 'John Doe'}],
            'description',
            None,
            None,
            None,
            'Test description',
        ),
        # Key not found returns None
        (
            [{'key': 'description', 'value': 'Test description'}],
            'nonexistent',
            None,
            None,
            None,
            None,
        ),
        # Key not found returns default value
        (
            [{'key': 'description', 'value': 'Test description'}],
            'nonexistent',
            None,
            None,
            'default_value',
            'default_value',
        ),
        # Filter by provider
        (
            [
                {'key': 'description', 'value': 'Provider A description', 'provider': 'provider-a'},
                {'key': 'description', 'value': 'Provider B description', 'provider': 'provider-b'},
            ],
            'description',
            'provider-b',
            None,
            None,
            'Provider B description',
        ),
        # Most recent by timestamp
        (
            [
                {'key': 'description', 'value': 'Old description', 'timestamp': '2024-01-01T00:00:00Z'},
                {'key': 'description', 'value': 'New description', 'timestamp': '2024-12-01T00:00:00Z'},
                {'key': 'description', 'value': 'Middle description', 'timestamp': '2024-06-01T00:00:00Z'},
            ],
            'description',
            None,
            None,
            None,
            'New description',
        ),
        # Handles missing timestamps
        (
            [
                {'key': 'description', 'value': 'No timestamp'},
                {'key': 'description', 'value': 'With timestamp', 'timestamp': '2024-01-01T00:00:00Z'},
            ],
            'description',
            None,
            None,
            None,
            'With timestamp',
        ),
        # Preferred providers prioritized
        (
            [
                {
                    'key': 'description',
                    'value': 'Provider A',
                    'provider': 'provider-a',
                    'timestamp': '2024-01-01T00:00:00Z',
                },
                {
                    'key': 'description',
                    'value': 'Provider B',
                    'provider': 'provider-b',
                    'timestamp': '2024-01-02T00:00:00Z',
                },
                {
                    'key': 'description',
                    'value': 'Provider C',
                    'provider': 'provider-c',
                    'timestamp': '2024-01-03T00:00:00Z',
                },
                {
                    'key': 'description',
                    'value': 'Provider X',
                    'provider': 'provider-X',  # not in the preferred_providers list
                    'timestamp': '2024-01-03T00:00:00Z',
                },
            ],
            'description',
            None,
            ['provider-b', 'provider-c', 'provider-a'],
            None,
            'Provider B',
        ),
        # Timestamp used when same provider preference
        (
            [
                {
                    'key': 'description',
                    'value': 'Old preferred',
                    'provider': 'provider-a',
                    'timestamp': '2024-01-01T00:00:00Z',
                },
                {
                    'key': 'description',
                    'value': 'New preferred',
                    'provider': 'provider-a',
                    'timestamp': '2024-12-01T00:00:00Z',
                },
            ],
            'description',
            None,
            ['provider-a'],
            None,
            'New preferred',
        ),
        # Empty metadata list returns None
        (
            [],
            'description',
            None,
            None,
            None,
            None,
        ),
        # Empty metadata list returns default
        (
            [],
            'description',
            None,
            None,
            'default_value',
            'default_value',
        ),
        # None value returns default
        (
            [{'key': 'description', 'value': None}],
            'description',
            None,
            None,
            'default_value',
            'default_value',
        ),
        # Combined provider and timestamp filtering
        (
            [
                {
                    'key': 'description',
                    'value': 'Provider A old',
                    'provider': 'provider-a',
                    'timestamp': '2024-01-01T00:00:00Z',
                },
                {
                    'key': 'description',
                    'value': 'Provider A new',
                    'provider': 'provider-a',
                    'timestamp': '2024-12-01T00:00:00Z',
                },
                {
                    'key': 'description',
                    'value': 'Provider B',
                    'provider': 'provider-b',
                    'timestamp': '2024-12-31T00:00:00Z',
                },
            ],
            'description',
            'provider-a',
            None,
            None,
            'Provider A new',
        ),
        # Metadata entries without the provider field
        (
            [
                {'key': 'description', 'value': 'No provider entry', 'timestamp': '2024-01-01T00:00:00Z'},
                {
                    'key': 'description',
                    'value': 'With provider',
                    'provider': 'provider-a',
                    'timestamp': '2024-01-02T00:00:00Z',
                },
            ],
            'description',
            None,
            ['provider-a'],
            None,
            'With provider',
        ),
    ],
    ids=[
        'basic_retrieval_by_key',
        'key_not_found_returns_none',
        'key_not_found_returns_default',
        'filter_by_provider',
        'most_recent_by_timestamp',
        'handles_missing_timestamps',
        'preferred_providers_prioritized',
        'timestamp_used_when_same_preference',
        'empty_metadata_list_returns_none',
        'empty_metadata_list_returns_default',
        'none_value_returns_default',
        'combined_provider_and_timestamp',
        'no_provider_in_metadata',
    ],
)
def test_get_metadata_property(
    metadata: list[Mapping[str, Any]],
    key: str,
    provider: str | None,
    preferred_providers: list[str] | None,
    default: Any,
    expected: Any,
):
    """Test get_metadata_property with various scenarios."""
    result = get_metadata_property(
        metadata=metadata,
        key=key,
        provider=provider,
        preferred_providers=preferred_providers,
        default=default,
    )
    assert result == expected


class TestStepUpStorageClient:
    """
    KeboolaClient.step_up_storage_client builds the Kubernetes step-up Storage client.

    The stack that the server itself belongs to is resolved once, when the server starts, and passed
    to the client as `own_stack_storage_api_url` (see `ServerState.own_stack_storage_api_url`); the
    step-up header is attached only when the session talks to that very stack.
    """

    OWN_STACK_URL = 'https://connection.keboola.com'

    @pytest.mark.parametrize(
        'own_stack_storage_api_url',
        [
            OWN_STACK_URL,
            # The default port is just another spelling of the same endpoint. `KeboolaClient` drops
            # the port when building its own Storage API URL, so this must still be our own stack.
            f'{OWN_STACK_URL}:443',
        ],
        ids=['plain', 'default_port'],
    )
    def test_attaches_step_up_header_and_keeps_user_token(self, tmp_path, own_stack_storage_api_url):
        token_file = tmp_path / 'token'
        token_file.write_text('sa-jwt\n')
        client = KeboolaClient(
            storage_api_url=self.OWN_STACK_URL,
            storage_api_token='user-token',
            headers={'User-Agent': 'test'},
            own_stack_storage_api_url=own_stack_storage_api_url,
        )

        stepped = client.step_up_storage_client(str(token_file))

        headers = stepped.raw_client.headers
        # The SA JWT rides as the step-up header ...
        assert headers['X-Kubernetes-Authorization'] == 'Bearer sa-jwt'
        # ... alongside the user's own token (no privileged token is minted) ...
        assert headers['X-StorageAPI-Token'] == 'user-token'
        # ... and pre-existing headers are preserved.
        assert headers['User-Agent'] == 'test'

    @pytest.mark.parametrize('readonly', [None, True, False])
    def test_propagates_readonly_guard(self, tmp_path, readonly):
        token_file = tmp_path / 'token'
        token_file.write_text('sa-jwt')
        client = KeboolaClient(
            storage_api_url=self.OWN_STACK_URL,
            storage_api_token='user-token',
            readonly=readonly,
            own_stack_storage_api_url=self.OWN_STACK_URL,
        )

        stepped = client.step_up_storage_client(str(token_file))

        assert stepped.raw_client.readonly == client.storage_client.raw_client.readonly

    @pytest.mark.asyncio
    @pytest.mark.parametrize('branch_id', [None, '123'], ids=['main_branch', 'dev_branch'])
    async def test_own_stack_survives_branch_switch(self, tmp_path, mocker, branch_id):
        """`with_branch_id()` builds a new client, which must keep knowing which stack is ours."""
        token_file = tmp_path / 'token'
        token_file.write_text('sa-jwt')
        client = KeboolaClient(
            storage_api_url=self.OWN_STACK_URL,
            storage_api_token='user-token',
            branch_id='999',
            own_stack_storage_api_url=self.OWN_STACK_URL,
        )
        client.storage_client.dev_branch_detail = mocker.AsyncMock(return_value={'isDefault': False})

        branched = await client.with_branch_id(branch_id)

        assert branched is not client
        assert (
            branched.step_up_storage_client(str(token_file)).raw_client.headers['X-Kubernetes-Authorization']
            == 'Bearer sa-jwt'
        )

    def test_fails_loudly_on_empty_token_file(self, tmp_path):
        token_file = tmp_path / 'token'
        token_file.write_text('  \n')
        client = KeboolaClient(
            storage_api_url=self.OWN_STACK_URL,
            storage_api_token='user-token',
            own_stack_storage_api_url=self.OWN_STACK_URL,
        )

        with pytest.raises(ValueError, match='token file is empty'):
            client.step_up_storage_client(str(token_file))

    def test_fails_loudly_on_missing_token_file(self, tmp_path):
        client = KeboolaClient(
            storage_api_url=self.OWN_STACK_URL,
            storage_api_token='user-token',
            own_stack_storage_api_url=self.OWN_STACK_URL,
        )

        with pytest.raises(FileNotFoundError):
            client.step_up_storage_client(str(tmp_path / 'missing'))

    @pytest.mark.parametrize(
        ('storage_api_url', 'own_stack_storage_api_url'),
        [
            # Another Keboola stack ...
            ('https://connection.north-europe.azure.keboola.com', OWN_STACK_URL),
            # ... and hosts that only look like this server's stack. All of them satisfy the
            # 'connection.' prefix that the Storage API URL itself is required to have.
            ('https://connection.keboola.com.attacker.example', OWN_STACK_URL),
            ('https://connection.attacker.example', OWN_STACK_URL),
            # A genuinely different port is a different endpoint.
            (OWN_STACK_URL, f'{OWN_STACK_URL}:8443'),
            # A server with no stack of its own (locally run) has no stack to step up on.
            (OWN_STACK_URL, None),
        ],
        ids=['other_stack', 'lookalike_suffix', 'foreign_domain', 'other_port', 'no_own_stack'],
    )
    def test_no_step_up_header_for_foreign_stack(self, tmp_path, storage_api_url, own_stack_storage_api_url):
        """The ServiceAccount JWT belongs to this server's stack and must not travel anywhere else."""
        # A readable token file, so that it is the destination check — not a missing file — that
        # keeps the header away.
        token_file = tmp_path / 'token'
        token_file.write_text('sa-jwt')
        client = KeboolaClient(
            storage_api_url=storage_api_url,
            storage_api_token='user-token',
            own_stack_storage_api_url=own_stack_storage_api_url,
        )

        stepped = client.step_up_storage_client(str(token_file))

        assert 'X-Kubernetes-Authorization' not in (stepped.raw_client.headers or {})
        assert stepped is client.storage_client
