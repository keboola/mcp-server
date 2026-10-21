"""Rate limits of the router, kept in process memory: the router has no database. Each replica counts on its own, so
the fleet allows up to the replica count times a limit.

What is counted follows who calls (RFC Security Considerations, "Abuse / DoS"): `/authorize` and `/register` per client
address, but `/token`, `/revoke` and `/mcp` per credential, because those calls come server to server from the AI
client's few shared egress addresses, where a per-address limit sized for one user would refuse every user at once.
A credential is counted by its SHA-256 only, so no token reaches the limiter's memory or the logs."""

import hashlib
import logging
import time
from collections.abc import Callable

from starlette.responses import JSONResponse, Response
from starlette.routing import get_route_path
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.oauth import caller_address

LOG = logging.getLogger(__name__)

WINDOW_SECONDS = 60
# Distinct keys counted per window. A flood of fresh keys beyond it is let through rather than allowed to grow the map
# without bound.
_MAX_TRACKED_KEYS = 100_000


def credential_key(credential: str) -> str:
    """What a credential is counted (and, shortened, logged) by: its SHA-256, never the value."""
    return hashlib.sha256(credential.encode()).hexdigest()


def too_many_requests() -> Response:
    return JSONResponse(
        {'error': 'temporarily_unavailable', 'error_description': 'Too many requests. Try again in a minute.'},
        status_code=429,
        headers={'Retry-After': str(WINDOW_SECONDS)},
    )


class FixedWindowCounter:
    """Requests per (group, key) in fixed one-minute windows."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._window = 0
        self._dropped = 0
        self._counts: dict[tuple[str, str], int] = {}

    def hit(self, group: str, key: str) -> int:
        """Counts one request and returns the count of this window, 0 for a key beyond the tracked maximum."""
        window = int(self._clock() // WINDOW_SECONDS)
        if window != self._window:
            self._window, self._counts = window, {}
            if self._dropped:
                LOG.warning(f'[router] {self._dropped} requests were not counted: too many distinct keys')
                self._dropped = 0
        counted = (group, key)
        if counted not in self._counts and len(self._counts) >= _MAX_TRACKED_KEYS:
            self._dropped += 1
            return 0
        self._counts[counted] = self._counts.get(counted, 0) + 1
        return self._counts[counted]

    def over_limit(self, group: str, key: str, limit: int, *, log_key: str) -> bool:
        """Counts one request; whether it is over `limit`. Logged once per key and window, so a flood does not flood
        the log too.

        :param log_key: What the log line names the key by: an address, or a shortened credential hash.
        """
        hits = self.hit(group, key)
        if hits == limit + 1:
            LOG.warning(f'[router] Rate limit reached: group={group}, key={log_key}, limit={limit}/min')
        return hits > limit


class PerAddressRateLimitMiddleware:
    """Counts requests to the paths in `limits` per caller address and answers 429 above the path's limit. A path
    outside `limits`, a CORS preflight, or an address that cannot be told is not counted."""

    def __init__(self, app: ASGIApp, *, limits: dict[str, int], trusted_hops: int, counter: FixedWindowCounter) -> None:
        """:param limits: route path -> requests per minute per address, for the paths that are limited."""
        self._app = app
        self._limits = limits
        self._trusted_hops = trusted_hops
        self._counter = counter

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = get_route_path(scope).rstrip('/') if scope['type'] == 'http' else ''
        limit = self._limits.get(path)
        preflight = scope.get('method') == 'OPTIONS' and b'access-control-request-method' in dict(scope['headers'])
        if limit is None or preflight:
            await self._app(scope, receive, send)
            return
        address = caller_address(scope, self._trusted_hops)
        if address is None or not self._counter.over_limit(f'address:{path}', address, limit, log_key=address):
            await self._app(scope, receive, send)
            return
        await too_many_requests()(scope, receive, send)
