"""The router's `/mcp`: a streaming reverse proxy to the stack that issued the bearer token, named by its prefix."""

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

import httpx
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.types import Receive, Scope, Send

from keboola_mcp_server.router.responses import oauth_error, too_many_requests, upstream_failure
from keboola_mcp_server.router.stacks import Stack, stack_key_of

LOG = logging.getLogger(__name__)

# The MCP transport's own headers and the bearer token, nothing else: no header selects a stack or reconfigures the
# regional server (its per-request `X-*` settings stay a direct-connection feature), and `X-Forwarded-For` would only
# let a caller choose the address the regional server sees. Every `Mcp-*` header is the transport's: the session and
# protocol version, and since 2026-07-28 `Mcp-Method`, `Mcp-Name` and `Mcp-Param-*`, which the stack checks against
# the body (400 without them).
_FORWARDED_REQUEST_HEADERS = ('authorization', 'content-type', 'accept', 'last-event-id')
_FORWARDED_REQUEST_HEADER_PREFIX = 'mcp-'
# Never the stack's `WWW-Authenticate`: it names the stack's own metadata, and a client following it would leave the
# router's authorization server.
_FORWARDED_RESPONSE_HEADERS = ('content-type', 'cache-control', 'mcp-session-id', 'mcp-protocol-version', 'allow')
# A slot frees up when one of the token's own requests ends, so the client may retry soon.
_IN_FLIGHT_RETRY_AFTER_SECONDS = 1


def _bearer_token(request: Request) -> str | None:
    scheme, _, token = request.headers.get('authorization', '').partition(' ')
    if scheme.lower() != 'bearer':
        return None
    return token.strip() or None


def _unauthorized(resource_metadata_url: str, *, error_description: str | None) -> Response:
    """401 that sends the client to the router's protected-resource metadata. Without a token at all it carries no
    error code (RFC 6750 §3.1); with one the router cannot route, `invalid_token`."""
    challenge = f'Bearer resource_metadata="{resource_metadata_url}"'
    if error_description is not None:
        challenge = f'Bearer error="invalid_token", error_description="{error_description}", ' + challenge[7:]
    return oauth_error(
        'invalid_token', error_description or 'Authentication required.', 401, headers={'WWW-Authenticate': challenge}
    )


class _UpstreamStreamingResponse(StreamingResponse):
    """Streams the stack's answer and closes it when the response ends, however it ends: a client that goes away
    leaves Starlette's background task unrun on some paths, and an unclosed answer keeps its pooled connection. The
    token's in-flight slot is released with it."""

    def __init__(
        self,
        answer: httpx.Response,
        content: AsyncIterator[bytes],
        headers: Mapping[str, str],
        release: Callable[[], None],
    ) -> None:
        super().__init__(content, status_code=answer.status_code, headers=headers)
        self._answer = answer
        self._release = release

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            try:
                await self._answer.aclose()
            finally:
                self._release()


def create_mcp_proxy(
    *,
    stacks: Mapping[str, Stack],
    upstreams: Mapping[str, httpx.AsyncClient],
    resource_metadata_url: str,
    connect_timeout_seconds: float,
    read_timeout_seconds: float,
    over_limit: Callable[[str], bool],
    retry_after: Callable[[], int],
    acquire_slot: Callable[[str], Callable[[], None] | None],
) -> Callable[[Request], Awaitable[Response]]:
    """The `/mcp` handler.

    :param upstreams: One client per stack key: each has its own connection pool, so a stack that answers slowly
        uses up only its own connections, and the other stacks' users are not affected.

    :param over_limit: Counts one request with a bearer token and says whether it is over the token's limit (the
        token is counted by its hash, see `rate_limit.credential_key`).
    :param retry_after: Seconds until the limit's window ends, for the 429's `Retry-After`.
    :param acquire_slot: Takes one of the token's in-flight slots (counted by its hash too): its release, or None
        when the token has as many requests in flight as it may. A slot is held from the upstream call until the
        answer ends, so one token cannot hold the whole pool with slow tool calls.
    :param read_timeout_seconds: How long the stack may stay silent: before the response starts (a tool call that runs
        long) and between two chunks of a streamed one. Exceeded before the response starts, the client gets 504;
        later, the stream ends.
    """
    timeout = httpx.Timeout(
        connect=connect_timeout_seconds,
        read=read_timeout_seconds,
        write=connect_timeout_seconds,
        pool=connect_timeout_seconds,
    )

    async def relay(answer: httpx.Response, stack: Stack) -> AsyncIterator[bytes]:
        try:
            async for chunk in answer.aiter_bytes():
                yield chunk
        except httpx.HTTPError as e:
            # The status line is already out; all that is left is to end the stream.
            LOG.warning(f'[router] Upstream stream interrupted: stack_key={stack.key}: {type(e).__name__}')

    async def proxy(request: Request) -> Response:
        token = _bearer_token(request)
        if token is None:
            return _unauthorized(resource_metadata_url, error_description=None)
        if (stack := stacks.get(stack_key_of(token) or '')) is None:
            return _unauthorized(
                resource_metadata_url, error_description='The access token was not issued by this server.'
            )
        request.state.stack_key = stack.key
        if over_limit(token):
            return too_many_requests(retry_after())

        url = f'{stack.upstream}/mcp' + (f'?{request.url.query}' if request.url.query else '')
        # Every copy, in order: the stack refuses a repeated `Mcp-Method` or `Mcp-Name`, so the router must not pick one.
        headers = [
            (name, value)
            for name, value in request.headers.items()
            if name in _FORWARDED_REQUEST_HEADERS or name.startswith(_FORWARDED_REQUEST_HEADER_PREFIX)
        ]
        # Byte for byte: a compressed answer would be decompressed here, and the decompressor may hold SSE bytes back.
        headers.append(('accept-encoding', 'identity'))
        upstream = upstreams[stack.key]
        upstream_request = upstream.build_request(
            request.method, url, content=await request.body(), headers=headers, timeout=timeout
        )
        if (release := acquire_slot(token)) is None:
            return too_many_requests(_IN_FLIGHT_RETRY_AFTER_SECONDS, 'Too many requests in progress. Try again.')
        # Released on every way out but a streamed answer, which releases it when the answer ends. A client that goes
        # away before the stack answers does not cancel the handler (uvicorn), so its slot is held until the stack
        # answers or the read timeout ends; a handler that is cancelled releases it at once.
        try:
            answer = await upstream.send(upstream_request, stream=True)
        except httpx.HTTPError as e:
            release()
            return upstream_failure(e, stack_key=stack.key, path='/mcp')
        except BaseException:
            release()
            raise
        try:
            if answer.status_code == 401:
                # Revoked, or the stack no longer knows the token: log in again through the router, not the stack.
                await answer.aclose()
                release()
                return _unauthorized(resource_metadata_url, error_description='The access token is not valid.')
            return _UpstreamStreamingResponse(
                answer,
                relay(answer, stack),
                headers={name: value for name in _FORWARDED_RESPONSE_HEADERS if (value := answer.headers.get(name))},
                release=release,
            )
        except BaseException:
            # e.g. a header value the response cannot encode: the answer and the slot must not outlive the request
            try:
                await answer.aclose()
            finally:
                release()
            raise

    return proxy
