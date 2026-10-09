"""
URL-mode elicitation (MCP 2025-11-25): lets a tool ask the client to open a page in the user's browser.

A tool raises `UrlActionRequiredError`. Clients that declare URL elicitation support get it converted into
`UrlElicitationRequiredError` (JSON-RPC -32042), so they can open the page after the user consents. Every
other client gets the original tool error, whose text already contains the URL.

The conversion lives in a middleware because fastmcp masks `MCPError`s raised inside a tool body into an
`isError` tool result, which would drop the -32042 code and the URL.
"""

import logging
import uuid

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from mcp import types as mt
from mcp.shared.exceptions import UrlElicitationRequiredError

LOG = logging.getLogger(__name__)


class UrlActionRequiredError(ToolError):
    """A tool error whose fix is an action the user takes on a web page."""

    def __init__(self, message: str, url: str) -> None:
        super().__init__(f'{message} Give the user this link: {url}')
        self.user_message = message
        self.url = url


def client_supports_url_elicitation(ctx: Context | None) -> bool:
    if ctx is None:
        return False
    try:
        capabilities = ctx.session.client_capabilities
    except RuntimeError:
        return False
    elicitation = capabilities.elicitation if capabilities else None
    return bool(elicitation and elicitation.url is not None)


class UrlElicitationMiddleware(fmw.Middleware):
    """Turns `UrlActionRequiredError` into a URL elicitation for clients that support it."""

    async def on_call_tool(
        self,
        context: fmw.MiddlewareContext[mt.CallToolRequestParams],
        call_next: fmw.CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        try:
            return await call_next(context)
        except UrlActionRequiredError as e:
            if not client_supports_url_elicitation(context.fastmcp_context):
                raise
            LOG.info(f'Asking the client to open {e.url} (tool "{context.message.name}").')
            elicitation = mt.ElicitRequestURLParams(message=e.user_message, url=e.url, elicitation_id=str(uuid.uuid4()))
            raise UrlElicitationRequiredError([elicitation], message=str(e)) from e
