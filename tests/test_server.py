from typing import Optional
from unittest.mock import patch

import pytest
from starlette.requests import Request

from keboola_mcp_server.server import (
    RequestParameterSource,
    TransportType,
    _get_session_params,
    _infer_session_params,
    create_server,
)
from keboola_mcp_server.tools.components import (
    GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
    RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME,
    RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
)


class TestServer:
    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server()
        tools = await server.get_tools()
        assert sorted(tool.name for tool in tools.values()) == [
            'create_sql_transformation',
            'docs_query',
            'get_bucket_detail',
            GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
            'get_job_detail',
            'get_sql_dialect',
            'get_table_detail',
            'query_table',
            'retrieve_bucket_tables',
            'retrieve_buckets',
            RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME,
            'retrieve_jobs',
            RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
            'start_job',
            'update_bucket_description',
            'update_table_description',
        ]

    @pytest.mark.asyncio
    async def test_tools_have_descriptions(self):
        server = create_server()
        tools = await server.get_tools()

        missing_descriptions: list[str] = []
        for tool in tools.values():
            if not tool.description:
                missing_descriptions.append(tool.name)

        missing_descriptions.sort()
        assert not missing_descriptions, f'These tools have no description: {missing_descriptions}'


@pytest.mark.parametrize(
    ('current_transport', 'request_param_source'),
    [
        ('streamable-http', 'headers'),
        ('streamable-http', 'query_params'),
        ('sse', 'headers'),
        ('sse', 'query_params'),
        ('stdio', None),
    ],
)
def test_infer_session_params_request(mocker, current_transport: str, request_param_source: str):

    # Create a mock request with query parameters based on the request_param_source
    mock_request = None
    expected_params = {'storage_token': 'test-storage-token', 'workspace_schema': 'test-workspace-schema'}
    os_env_parameters = {}
    if current_transport in ('streamable-http', 'sse'):
        mock_request = mocker.MagicMock(spec=Request)
        if request_param_source == 'headers':
            mock_request.headers = expected_params

        if request_param_source == 'query_params':
            mock_request.query_params = expected_params
    elif current_transport == 'stdio':
        mock_request = None
        os_env_parameters = expected_params

    # Patch the _safe_get_http_request function to return our mock request
    with (
        patch('keboola_mcp_server.server.get_http_request', return_value=mock_request),
        patch('keboola_mcp_server.server.os.environ', os_env_parameters),
    ):
        params = _infer_session_params()
        assert params == expected_params


@pytest.mark.parametrize(
    ('current_transport', 'request_param_source'),
    [
        ('streamable-http', 'headers'),
        ('streamable-http', 'query_params'),
        ('streamable-http', None),
        ('sse', 'headers'),
        ('sse', 'query_params'),
        ('sse', None),
        ('stdio', None),
    ],
)
def test_get_session_params(
    mocker, current_transport: Optional[TransportType], request_param_source: Optional[RequestParameterSource]
):

    # Create a mock request with query parameters based on the request_param_source
    mock_request = None
    expected_params = {'storage_token': 'test-storage-token', 'workspace_schema': 'test-workspace-schema'}
    os_env_parameters = {}
    if current_transport in ('streamable-http', 'sse'):
        mock_request = mocker.MagicMock(spec=Request)
        if request_param_source == 'headers':
            mock_request.headers = expected_params

        if request_param_source == 'query_params':
            mock_request.query_params = expected_params

        if not request_param_source:
            mock_request.query_params = expected_params

    elif current_transport == 'stdio':
        mock_request = None
        os_env_parameters = expected_params

    # Patch the _safe_get_http_request function to return our mock request
    with (
        patch('keboola_mcp_server.server.get_http_request', return_value=mock_request),
        patch('keboola_mcp_server.server.os.environ', os_env_parameters),
    ):
        params = _get_session_params(current_transport, request_param_source)
        assert params == expected_params
