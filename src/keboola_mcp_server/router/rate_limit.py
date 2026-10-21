"""Per-client-address rate limits of the router, kept in process memory: the router has no database. Each replica
counts on its own, so the fleet allows up to the replica count times the limit."""

import logging
import time

from starlette.responses import JSONResponse
from starlette.routing import get_route_path
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.oauth import caller_address

LOG = logging.getLogger(__name__)

_WINDOW_SECONDS = 60
# Distinct addresses counted per window. A flood of fresh addresses beyond it is let through rather than allowed to
# grow the map without bound.
_MAX_TRACKED_ADDRESSES = 100_000


class PerAddressRateLimitMiddleware:
    """Counts requests per (endpoint group, caller address) in fixed one-minute windows and answers 429 above the
    group's limit. A path outside every group, or an address that cannot be told, is not counted."""

    def __init__(self, app: ASGIApp, *, limits: dict[str, int], trusted_hops: int) -> None:
        """:param limits: route path -> requests per minute per address, for the paths that are limited."""
        self._app = app
        self._limits = limits
        self._trusted_hops = trusted_hops
        self._window = 0
        self._dropped = 0
        self._counts: dict[tuple[str, str], int] = {}

    def _hit(self, path: str, address: str) -> int:
        window = int(time.monotonic() // _WINDOW_SECONDS)
        if window != self._window:
            self._window, self._counts = window, {}
            if self._dropped:
                LOG.warning(f'[router] {self._dropped} requests were not counted: too many distinct addresses')
                self._dropped = 0
        key = (path, address)
        if key not in self._counts and len(self._counts) >= _MAX_TRACKED_ADDRESSES:
            self._dropped += 1
            return 0
        self._counts[key] = self._counts.get(key, 0) + 1
        return self._counts[key]

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = get_route_path(scope).rstrip('/') if scope['type'] == 'http' else ''
        limit = self._limits.get(path)
        if limit is None or scope.get('method') == 'OPTIONS':
            await self._app(scope, receive, send)
            return
        address = caller_address(scope, self._trusted_hops)
        if address is None or (hits := self._hit(path, address)) <= limit:
            await self._app(scope, receive, send)
            return
        if hits == limit + 1:  # once per address and window, so a flood does not flood the log too
            LOG.warning(f'[router] Per-address rate limit reached: path={path}, address={address}, limit={limit}/min')
        response = JSONResponse(
            {'error': 'temporarily_unavailable', 'error_description': 'Too many requests. Try again in a minute.'},
            status_code=429,
            headers={'Retry-After': str(_WINDOW_SECONDS)},
        )
        await response(scope, receive, send)
