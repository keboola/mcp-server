import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from mcp.shared.exceptions import MCPError, UrlElicitationRequiredError

from keboola_mcp_server.elicitation import UrlActionRequiredError, UrlElicitationMiddleware

UNLOCK_URL = 'https://connection.test.keboola.com/admin/projects/1/storage/data-streams'


def _server() -> FastMCP:
    server = FastMCP('test', middleware=[UrlElicitationMiddleware()])

    @server.tool
    def needs_unlock() -> str:
        raise UrlActionRequiredError('Data Streams are not enabled.', url=UNLOCK_URL)

    @server.tool
    def fails() -> str:
        raise ToolError('Plain failure.')

    return server


async def _accept(*_args: object) -> dict:
    return {}


@pytest.mark.asyncio
async def test_url_action_becomes_url_elicitation_for_capable_client() -> None:
    async with Client(_server(), elicitation_handler=_accept) as client:
        with pytest.raises(MCPError) as exc_info:
            await client.call_tool('needs_unlock', {})

    error = UrlElicitationRequiredError.from_error(exc_info.value.error)
    assert [(e.url, e.message, e.mode) for e in error.elicitations] == [
        (UNLOCK_URL, 'Data Streams are not enabled.', 'url')
    ]
    assert error.elicitations[0].elicitation_id
    assert UNLOCK_URL in error.error.message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('tool_name', 'elicitation_handler', 'expected_text'),
    [
        ('needs_unlock', None, f'Data Streams are not enabled. Give the user this link: {UNLOCK_URL}'),
        ('fails', _accept, 'Plain failure.'),
    ],
)
async def test_tool_error_kept_otherwise(tool_name: str, elicitation_handler, expected_text: str) -> None:
    async with Client(_server(), elicitation_handler=elicitation_handler) as client:
        result = await client.call_tool(tool_name, {}, raise_on_error=False)

    assert result.is_error
    assert result.content[0].text == expected_text
