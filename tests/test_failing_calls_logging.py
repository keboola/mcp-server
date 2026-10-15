import inspect
import logging

import httpx
import pytest
from fastmcp import Client, Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware.logging import LoggingMiddleware
from fastmcp.tools import FunctionTool

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.errors import SanitizedLoggingMiddleware, tool_errors
from keboola_mcp_server.server import create_server
from tests.conftest import CANARY, formatted_logs


class TestFailingCallsThroughFastMcp:
    """Failing tool calls through a real server with its middleware, checking every logger."""

    @pytest.fixture
    def mcp_server(self, mocker) -> FastMCP:
        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.get',
            return_value={'owner': {'id': '123'}},
        )
        mocker.patch('keboola_mcp_server.clients.base.KeboolaServiceClient.post', return_value={'id': '1'})
        config = Config.from_dict(
            {
                'storage_token': '123-test-storage-token',
                'storage_api_url': 'https://connection.keboola.com',
                'transport': 'stdio',
            }
        )
        server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)
        for tool in (self.edit_config, self.call_upstream, self.fail_plainly, self.validated, self.rate_limited):
            server.add_tool(FunctionTool.from_function(tool))
        return server

    @staticmethod
    @tool_errors()
    async def edit_config(_ctx: Context, parameter_updates: list[dict[str, str]]) -> str:
        raise ValueError(f'Search string "{CANARY}" not found in path "api.#token"')

    @staticmethod
    @tool_errors()
    async def call_upstream(_ctx: Context, name: str) -> str:
        raise RuntimeError(f'Upstream said #password: {CANARY}')

    @staticmethod
    @tool_errors()
    async def fail_plainly(_ctx: Context, name: str) -> str:
        raise RuntimeError('HTTP 503 for config 7')

    @staticmethod
    @tool_errors()
    async def validated(_ctx: Context, parameters: dict[str, int]) -> str:
        return 'ok'

    @staticmethod
    @tool_errors()
    async def rate_limited(_ctx: Context, kind: str, parameters: dict | None = None) -> str:
        if kind == 'timeout':
            raise httpx.ReadTimeout('read timed out')
        request = httpx.Request('POST', 'https://connection.keboola.com/v2/storage/x')
        raise httpx.HTTPStatusError('429', request=request, response=httpx.Response(429, request=request))

    @pytest.mark.asyncio
    async def test_argument_validation_failure_with_a_secret_is_not_logged(self, caplog, mcp_server: FastMCP):
        """Argument validation fails before `tool_errors` runs, and its text echoes the invalid input."""
        caplog.set_level(logging.DEBUG)

        async with Client(mcp_server) as client:
            with pytest.raises(ToolError):
                await client.call_tool('validated', arguments={'parameters': {'#pw': CANARY}})

        assert any('validated' in r.getMessage() for r in caplog.records)
        assert CANARY not in formatted_logs(caplog)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('kind', 'expected_text'),
        [
            ('rate_limit', 'Rate limited by upstream API, please retry later'),
            ('timeout', 'Upstream request timed out, please retry'),
        ],
    )
    @pytest.mark.parametrize(
        'parameters', [{'#password': CANARY}, {'host': 'db.example.com'}], ids=['call_with_secret', 'call_without']
    )
    async def test_retry_hints_survive_for_a_call_carrying_a_secret(
        self, kind: str, expected_text: str, parameters: dict, mcp_server: FastMCP
    ):
        async with Client(mcp_server) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool('rate_limited', arguments={'kind': kind, 'parameters': parameters})

        assert expected_text in str(excinfo.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('tool', 'arguments', 'expected_client_text'),
        [
            pytest.param(
                'edit_config',
                {
                    'parameter_updates': [
                        {'op': 'str_replace', 'path': 'api.#token', 'search_for': CANARY, 'replace_with': 'x'}
                    ]
                },
                f'Search string "{CANARY}" not found in path "api.#token"',
                id='call_carrying_a_secret',
            ),
            pytest.param(
                'call_upstream',
                {'name': 'db'},
                f'Upstream said #password: {CANARY}',
                id='message_showing_a_secret_key',
            ),
        ],
    )
    async def test_no_logger_shows_the_secret(
        self, tool: str, arguments: dict, expected_client_text: str, caplog, mcp_server: FastMCP
    ):
        caplog.set_level(logging.DEBUG)

        async with Client(mcp_server) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool(tool, arguments=arguments)

        assert expected_client_text in str(excinfo.value)
        assert {r.name for r in caplog.records} >= {'keboola_mcp_server.errors', 'fastmcp.middleware.logging'}
        assert CANARY not in formatted_logs(caplog)

    @pytest.mark.asyncio
    async def test_failure_without_a_secret_is_logged_as_it_is(self, caplog, mcp_server: FastMCP):
        caplog.set_level(logging.DEBUG)

        async with Client(mcp_server) as client:
            with pytest.raises(ToolError):
                await client.call_tool('fail_plainly', arguments={'name': 'db'})

        assert 'HTTP 503 for config 7' in formatted_logs(caplog)


def test_logging_middleware_override_still_matches_fastmcp():
    """`SanitizedLoggingMiddleware` replaces a private FastMCP method; a FastMCP upgrade must not silently bypass it."""
    parent_method = getattr(LoggingMiddleware, '_create_error_message', None)

    assert parent_method is not None, 'FastMCP no longer has LoggingMiddleware._create_error_message'
    assert SanitizedLoggingMiddleware._create_error_message is not parent_method
    assert list(inspect.signature(SanitizedLoggingMiddleware._create_error_message).parameters) == list(
        inspect.signature(parent_method).parameters
    )
