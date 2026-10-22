"""The router's own error answers, shared by every route so that `/token`, `/revoke` and `/mcp` cannot drift apart."""

import logging
from collections.abc import Mapping

import httpx
from starlette.responses import JSONResponse

LOG = logging.getLogger(__name__)


def oauth_error(
    error: str, description: str, status_code: int = 400, *, headers: Mapping[str, str] | None = None
) -> JSONResponse:
    """An OAuth error body (RFC 6749 §5.2), never cached: it answers a request that carried a credential."""
    return JSONResponse(
        {'error': error, 'error_description': description},
        status_code=status_code,
        headers={'Cache-Control': 'no-store', 'Pragma': 'no-cache', **(headers or {})},
    )


def too_many_requests(retry_after: int, description: str = 'Too many requests. Try again later.') -> JSONResponse:
    """:param retry_after: Seconds after which the request may succeed, for `Retry-After`."""
    return oauth_error('temporarily_unavailable', description, 429, headers={'Retry-After': str(retry_after)})


def upstream_failure(error: httpx.HTTPError, *, stack_key: str, path: str) -> JSONResponse:
    """The answer to a stack that could not be asked: 503 when the router's own pool has no free connection (not the
    stack's fault), 504 when the stack did not answer in time, 502 otherwise."""
    if isinstance(error, httpx.PoolTimeout):
        LOG.warning(f"[router] No free connection in the router's pool: stack_key={stack_key}, path={path}")
        return oauth_error('temporarily_unavailable', 'The router is busy. Try again.', 503)
    if isinstance(error, httpx.TimeoutException):
        LOG.warning(f'[router] Upstream timed out: stack_key={stack_key}, path={path}')
        return oauth_error('temporarily_unavailable', 'The Keboola stack did not answer in time.', 504)
    LOG.warning(f'[router] Upstream unreachable: stack_key={stack_key}, path={path}: {type(error).__name__}')
    return oauth_error('temporarily_unavailable', 'The Keboola stack could not be reached.', 502)
