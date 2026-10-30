from typing import Any

import pytest
from fastmcp import Client, FastMCP
from fastmcp.client.elicitation import ElicitResult
from fastmcp.exceptions import ToolError

from keboola_mcp_server.elicitation import UrlActionRequiredError, UrlElicitationMiddleware, offer_url

UNLOCK_URL = 'https://connection.test.keboola.com/admin/projects/1/storage/data-streams'
UNLOCK_TEXT = f'Data Streams are not enabled. Give the user this link: {UNLOCK_URL}'
SETUP_URL = 'https://connection.test.keboola.com/admin/projects/1/data-apps/123'


def _server(tool_calls: list[str]) -> FastMCP:
    server = FastMCP('test', middleware=[UrlElicitationMiddleware()])

    @server.tool
    def needs_unlock() -> str:
        tool_calls.append('needs_unlock')
        raise UrlActionRequiredError('Data Streams are not enabled.', url=UNLOCK_URL)

    @server.tool
    def needs_setup() -> str:
        tool_calls.append('needs_setup')
        offer_url('Enter the client secret.', SETUP_URL)
        return 'ok'

    @server.tool
    def fails() -> str:
        tool_calls.append('fails')
        raise ToolError('Plain failure.')

    return server


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('tool_name', 'capable_client', 'client_action', 'expected_error', 'expected_text', 'expected_elicitations'),
    [
        ('needs_unlock', True, 'accept', True, UNLOCK_TEXT, [('url', UNLOCK_URL, 'Data Streams are not enabled.')]),
        ('needs_unlock', True, 'decline', True, UNLOCK_TEXT, [('url', UNLOCK_URL, 'Data Streams are not enabled.')]),
        ('needs_unlock', False, None, True, UNLOCK_TEXT, []),
        ('fails', True, 'accept', True, 'Plain failure.', []),
        ('needs_setup', True, 'accept', False, 'ok', [('url', SETUP_URL, 'Enter the client secret.')]),
        ('needs_setup', True, 'decline', False, 'ok', [('url', SETUP_URL, 'Enter the client secret.')]),
        ('needs_setup', False, None, False, 'ok', []),
    ],
)
async def test_url_action_opens_page_and_keeps_tool_result(
    tool_name: str,
    capable_client: bool,
    client_action: str | None,
    expected_error: bool,
    expected_text: str,
    expected_elicitations: list[tuple[str, str, str]],
) -> None:
    received: list[Any] = []
    tool_calls: list[str] = []

    async def handler(message: str, response_type: Any, params: Any, context: Any) -> ElicitResult:
        received.append(params)
        return ElicitResult(action=client_action)

    async with Client(_server(tool_calls), elicitation_handler=handler if capable_client else None) as client:
        result = await client.call_tool(tool_name, {}, raise_on_error=False)

    assert result.is_error is expected_error
    assert result.content[0].text == expected_text
    assert [(p.mode, p.url, p.message) for p in received] == expected_elicitations
    # On the 2026-07-28 protocol the client answers the URL request by calling the tool again.
    assert tool_calls == [tool_name] * (1 + len(expected_elicitations))


def test_offer_url_outside_a_tool_call_does_nothing() -> None:
    offer_url('Enter the client secret.', SETUP_URL)
