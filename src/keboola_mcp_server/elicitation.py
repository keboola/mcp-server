"""
URL-mode elicitation (MCP 2025-11-25): lets a tool ask the client to open a page in the user's browser.

A tool that fails because the user must act on a web page raises `UrlActionRequiredError`. For clients that
declare URL elicitation support, the middleware asks the client to open the page during the call, so it can open
it after the user consents, and then lets the original tool error through, so the model still learns why and what
to tell the user:

- 2026-07-28 protocol: the call returns an `InputRequiredResult` with the URL request; the client retries with the
  answer and the retry gets the tool error.
- Older protocols: an `elicitation/create` request over the back-channel, then the tool error.

This is not the `URLElicitationRequiredError` (-32042) flow: that one makes clients wait for the server to
confirm the out-of-band step and then retry the call, which fits an OAuth login but not an action that takes
days, like Keboola enabling a feature. Without a way to ask (an older protocol over stateless HTTP), the client
gets only the tool error, whose text already contains the URL.

A tool that succeeds but still needs the user to finish a step on a web page calls `offer_url()`. The middleware
then asks the client in the same way and returns the tool result instead of the error. On the 2026-07-28 protocol
the retry runs the tool again, so only tools that are safe to repeat may call it.
"""

import logging
import uuid
from contextvars import ContextVar
from typing import NamedTuple

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from fastmcp.tools.base import InputRequiredToolResult, ToolResult
from mcp import types as mt
from mcp_types.version import MODERN_PROTOCOL_VERSIONS

LOG = logging.getLogger(__name__)

OPEN_URL_INPUT_KEY = 'open_url'


class UrlAction(NamedTuple):
    message: str
    url: str


# A mutable list set by the middleware, so entries added by the tool stay visible even if it runs in a copied context.
_offered_urls: ContextVar[list[UrlAction] | None] = ContextVar('offered_urls', default=None)


def offer_url(message: str, url: str) -> None:
    """Asks the client to open the URL after the tool call succeeds. Does nothing outside a tool call."""
    offered = _offered_urls.get()
    if offered is not None:
        offered.append(UrlAction(message, url))


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


def uses_input_required_rounds(ctx: Context) -> bool:
    """True on the 2026-07-28 protocol, which asks the client for input via `InputRequiredResult` rounds."""
    rc = ctx.request_context
    return rc is not None and rc.protocol_version in MODERN_PROTOCOL_VERSIONS


def open_url_input_request(action: UrlAction) -> InputRequiredToolResult:
    params = mt.ElicitRequestURLParams(message=action.message, url=action.url, elicitation_id=str(uuid.uuid4()))
    return InputRequiredToolResult(
        mt.InputRequiredResult(input_requests={OPEN_URL_INPUT_KEY: mt.ElicitRequest(params=params)})
    )


async def open_url_in_client(ctx: Context, action: UrlAction) -> None:
    """Asks the client to open the URL; never fails, the tool's own answer is returned either way."""
    try:
        result = await ctx.session.elicit_url(
            message=action.message,
            url=action.url,
            elicitation_id=str(uuid.uuid4()),
            related_request_id=ctx.request_id,
        )
        LOG.info(f'URL elicitation for {action.url} answered with "{result.action}".')
    except Exception as e:
        LOG.info(f'Could not ask the client to open {action.url}, returning the link only: {e}')


class UrlElicitationMiddleware(fmw.Middleware):
    """Offers to open the page of a `UrlActionRequiredError` or of `offer_url()` in clients that support it."""

    async def on_call_tool(
        self,
        context: fmw.MiddlewareContext[mt.CallToolRequestParams],
        call_next: fmw.CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        offered: list[UrlAction] = []
        token = _offered_urls.set(offered)
        try:
            result = await call_next(context)
        except UrlActionRequiredError as e:
            action = UrlAction(e.user_message, e.url)
            input_request = await self._ask_client(context, action)
            if input_request is not None:
                return input_request
            raise
        finally:
            _offered_urls.reset(token)

        if offered:
            input_request = await self._ask_client(context, offered[0])
            if input_request is not None:
                return input_request
        return result

    @staticmethod
    async def _ask_client(
        context: fmw.MiddlewareContext[mt.CallToolRequestParams], action: UrlAction
    ) -> InputRequiredToolResult | None:
        """Asks a URL-capable client to open the page; returns the input request to send on the 2026-07-28 protocol."""
        ctx = context.fastmcp_context
        if ctx is None or not client_supports_url_elicitation(ctx):
            return None
        if not uses_input_required_rounds(ctx):
            await open_url_in_client(ctx, action)
            return None
        if OPEN_URL_INPUT_KEY in (ctx.input_responses or {}):
            return None
        LOG.info(f'Asking the client to open {action.url} (tool "{context.message.name}").')
        return open_url_input_request(action)
