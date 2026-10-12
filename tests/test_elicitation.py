from typing import Any

import pytest
from fastmcp import Client, FastMCP
from fastmcp.client.elicitation import ElicitResult
from fastmcp.exceptions import ToolError

from keboola_mcp_server.elicitation import UrlActionRequiredError, UrlElicitationMiddleware

UNLOCK_URL = 'https://connection.test.keboola.com/admin/projects/1/storage/data-streams'
UNLOCK_TEXT = f'Data Streams are not enabled. Give the user this link: {UNLOCK_URL}'


def _server() -> FastMCP:
    server = FastMCP('test', middleware=[UrlElicitationMiddleware()])

    @server.tool
    def needs_unlock() -> str:
        raise UrlActionRequiredError('Data Streams are not enabled.', url=UNLOCK_URL)

    @server.tool
    def fails() -> str:
        raise ToolError('Plain failure.')

    return server


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('tool_name', 'capable_client', 'client_action', 'expected_text', 'expected_elicitations'),
    [
        ('needs_unlock', True, 'accept', UNLOCK_TEXT, [('url', UNLOCK_URL, 'Data Streams are not enabled.')]),
        ('needs_unlock', True, 'decline', UNLOCK_TEXT, [('url', UNLOCK_URL, 'Data Streams are not enabled.')]),
        ('needs_unlock', False, None, UNLOCK_TEXT, []),
        ('fails', True, 'accept', 'Plain failure.', []),
    ],
)
async def test_url_action_opens_page_and_keeps_tool_error(
    tool_name: str,
    capable_client: bool,
    client_action: str | None,
    expected_text: str,
    expected_elicitations: list[tuple[str, str, str]],
) -> None:
    received: list[Any] = []

    async def handler(message: str, response_type: Any, params: Any, context: Any) -> ElicitResult:
        received.append(params)
        return ElicitResult(action=client_action)

    async with Client(_server(), elicitation_handler=handler if capable_client else None) as client:
        result = await client.call_tool(tool_name, {}, raise_on_error=False)

    assert result.is_error
    assert result.content[0].text == expected_text
    assert [(p.mode, p.url, p.message) for p in received] == expected_elicitations
