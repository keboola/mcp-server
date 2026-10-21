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

from starlette.routing import get_route_path
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.oauth import caller_address
from keboola_mcp_server.router.responses import too_many_requests

LOG = logging.getLogger(__name__)

WINDOW_SECONDS = 60
# Distinct keys tracked per window, so that a flood of made-up keys cannot grow the map without bound.
_MAX_TRACKED_KEYS = 100_000
_OVERFLOW_KEY = '*overflow*'


def credential_key(credential: str) -> str:
    """What a credential is counted (and, shortened, logged) by: its SHA-256, never the value."""
    return hashlib.sha256(credential.encode()).hexdigest()


class FixedWindowCounter:
    """Requests per (group, key) in fixed one-minute windows."""

    def __init__(self, clock: Callable[[], float] = time.monotonic, *, overflow_limit: int | None = None) -> None:
        """:param overflow_limit: Once the window tracks the maximum of distinct keys, a new key is counted against one
        shared key of its group with this limit. None lets it through uncounted."""
        self._clock = clock
        self._overflow_limit = overflow_limit
        self._window = 0
        self._capped = False
        self._uncounted = 0
        self._counts: dict[tuple[str, str], int] = {}

    def seconds_left(self) -> int:
        """Seconds until the current window ends, at least 1: the Retry-After of a refused request."""
        return max(1, WINDOW_SECONDS - int(self._clock() % WINDOW_SECONDS))

    def over_limit(self, group: str, key: str, limit: int, *, log_key: str) -> bool:
        """Counts one request; whether it is over `limit`. Logged once per key and window, so a flood does not flood
        the log too.

        :param log_key: What the log line names the key by: an address, or a shortened credential hash.
        """
        window = int(self._clock() // WINDOW_SECONDS)
        if window != self._window:
            if self._uncounted:
                LOG.warning(f'[router] {self._uncounted} requests were not counted: too many distinct keys')
            self._window, self._counts, self._capped, self._uncounted = window, {}, False, 0
        counted, bucket_limit, bucket_log_key = (group, key), limit, log_key
        if counted not in self._counts and len(self._counts) >= _MAX_TRACKED_KEYS:
            if not self._capped:
                self._capped = True
                outcome = 'not counted' if self._overflow_limit is None else 'counted in the overflow bucket'
                LOG.warning(f'[router] {_MAX_TRACKED_KEYS} distinct keys this minute: new keys are {outcome}')
            if self._overflow_limit is None:
                self._uncounted += 1
                return False
            counted, bucket_limit, bucket_log_key = (group, _OVERFLOW_KEY), self._overflow_limit, _OVERFLOW_KEY
        hits = self._counts[counted] = self._counts.get(counted, 0) + 1
        if hits == bucket_limit + 1:
            LOG.warning(f'[router] Rate limit reached: group={group}, key={bucket_log_key}, limit={bucket_limit}/min')
        return hits > bucket_limit


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
        await too_many_requests(self._counter.seconds_left())(scope, receive, send)


class InFlightLimiter:
    """Requests in flight per key: what a rate limit cannot bound when each request may hold a pooled connection for
    minutes. A key leaves the map when its last request ends, so the map is as large as the requests in flight."""

    def __init__(self) -> None:
        self._in_flight: dict[str, int] = {}

    def acquire(self, key: str, limit: int, *, log_key: str) -> Callable[[], None] | None:
        """Takes one of `key`'s `limit` slots: its release, which may be called more than once, or None when every
        slot is taken.

        :param log_key: What the log line names the key by, as in `FixedWindowCounter.over_limit`.
        """
        held = self._in_flight.get(key, 0)
        if held >= limit:
            LOG.warning(f'[router] Concurrency limit reached: key={log_key}, limit={limit}')
            return None
        self._in_flight[key] = held + 1
        released = False

        def release() -> None:
            nonlocal released
            if released:
                return
            released = True
            if (left := self._in_flight[key] - 1) > 0:
                self._in_flight[key] = left
            else:
                del self._in_flight[key]

        return release
