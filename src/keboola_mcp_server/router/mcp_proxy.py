"""The router's `/mcp`: a streaming reverse proxy to the stack that issued the bearer token, named by its prefix."""

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.types import Receive, Scope, Send

from keboola_mcp_server.router.rate_limit import too_many_requests
from keboola_mcp_server.router.stacks import Stack, stack_key_of

LOG = logging.getLogger(__name__)

# The MCP transport's own headers and the bearer token, nothing else: no header selects a stack or reconfigures the
# regional server (its per-request `X-*` settings stay a direct-connection feature), and `X-Forwarded-For` would only
# let a caller choose the address the regional server sees.
_FORWARDED_REQUEST_HEADERS = (
    'authorization',
    'content-type',
    'accept',
    'mcp-session-id',
    'mcp-protocol-version',
    'last-event-id',
)
# Never the stack's `WWW-Authenticate`: it names the stack's own metadata, and a client following it would leave the
# router's authorization server.
_FORWARDED_RESPONSE_HEADERS = ('content-type', 'cache-control', 'mcp-session-id', 'mcp-protocol-version', 'allow')


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
    return JSONResponse(
        {'error': 'invalid_token', 'error_description': error_description or 'Authentication required.'},
        status_code=401,
        headers={'WWW-Authenticate': challenge},
    )


class _UpstreamStreamingResponse(StreamingResponse):
    """Streams the stack's answer and closes it when the response ends, however it ends: a client that goes away
    leaves Starlette's background task unrun on some paths, and an unclosed answer keeps its pooled connection."""

    def __init__(self, answer: httpx.Response, content: AsyncIterator[bytes], headers: Mapping[str, str]) -> None:
        super().__init__(content, status_code=answer.status_code, headers=headers)
        self._answer = answer

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._answer.aclose()


def _gateway_error(status_code: int, description: str) -> Response:
    return JSONResponse({'error': 'temporarily_unavailable', 'error_description': description}, status_code=status_code)


def create_mcp_proxy(
    *,
    stacks: Mapping[str, Stack],
    upstreams: Mapping[str, httpx.AsyncClient],
    resource_metadata_url: str,
    connect_timeout_seconds: float,
    read_timeout_seconds: float,
    over_limit: Callable[[str], bool],
) -> Callable[[Request], Awaitable[Response]]:
    """The `/mcp` handler.

    :param upstreams: One client per stack key: each has its own connection pool, so a stack that answers slowly
        uses up only its own connections, and the other stacks' users are not affected.

    :param over_limit: Counts one request with a bearer token and says whether it is over the token's limit (the
        token is counted by its hash, see `rate_limit.credential_key`).
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
            return too_many_requests()

        url = f'{stack.upstream}/mcp' + (f'?{request.url.query}' if request.url.query else '')
        headers = {name: value for name in _FORWARDED_REQUEST_HEADERS if (value := request.headers.get(name))}
        # Byte for byte: a compressed answer would be decompressed here, and the decompressor may hold SSE bytes back.
        headers['accept-encoding'] = 'identity'
        upstream = upstreams[stack.key]
        upstream_request = upstream.build_request(
            request.method, url, content=await request.body(), headers=headers, timeout=timeout
        )
        try:
            answer = await upstream.send(upstream_request, stream=True)
        except httpx.PoolTimeout:
            LOG.warning(f"[router] No free connection in the router's pool for the stack: stack_key={stack.key}")
            return _gateway_error(503, 'The router is busy. Try again.')
        except httpx.TimeoutException:
            LOG.warning(f'[router] Upstream timed out: stack_key={stack.key}, path=/mcp')
            return _gateway_error(504, 'The Keboola stack did not answer in time.')
        except httpx.HTTPError as e:
            LOG.warning(f'[router] Upstream unreachable: stack_key={stack.key}, path=/mcp: {type(e).__name__}')
            return _gateway_error(502, 'The Keboola stack could not be reached.')

        if answer.status_code == 401:
            # Revoked, or the stack no longer knows the token: log in again through the router, not the stack.
            await answer.aclose()
            return _unauthorized(resource_metadata_url, error_description='The access token is not valid.')
        return _UpstreamStreamingResponse(
            answer,
            relay(answer, stack),
            headers={name: value for name in _FORWARDED_RESPONSE_HEADERS if (value := answer.headers.get(name))},
        )

    return proxy
