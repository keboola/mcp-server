"""
Connection management for the Keboola MCP server.

This module provides connection tracking and backpressure functionality to handle
high concurrency scenarios. Python's asyncio event loop runs on a single thread,
so with many concurrent SSE connections (e.g., 1000+), every new request competes
for the same event loop. This can cause simple operations to work but complex ones
(like tools/list) to timeout.

The connection limit with backpressure prevents degradation for existing connections
by rejecting new connections when the server is at capacity, returning HTTP 503.
"""

import json
import logging
import threading
from contextlib import contextmanager
from typing import Generator

from starlette.types import ASGIApp, Receive, Scope, Send

LOG = logging.getLogger(__name__)

DEFAULT_MAX_CONNECTIONS = 1000


class ConnectionMetrics:
    """
    Thread-safe connection counter for tracking active SSE/HTTP connections.

    This class provides a simple mechanism to track the number of active connections
    and enforce a maximum connection limit. When the limit is reached, new connections
    should be rejected with HTTP 503 (Service Unavailable) to prevent degradation
    of service for existing connections.

    The implementation uses a threading.Lock for thread-safety since uvicorn workers
    run in separate processes, but within each process, multiple coroutines may
    access the counter concurrently.
    """

    def __init__(self, max_connections: int = DEFAULT_MAX_CONNECTIONS) -> None:
        """
        Initialize the connection metrics.

        :param max_connections: Maximum number of concurrent connections allowed.
        """
        self._lock = threading.Lock()
        self._count = 0
        self._max_connections = max_connections

    @property
    def count(self) -> int:
        """Return the current number of active connections."""
        with self._lock:
            return self._count

    @property
    def max_connections(self) -> int:
        """Return the maximum number of connections allowed."""
        return self._max_connections

    def is_at_capacity(self) -> bool:
        """Check if the server is at connection capacity."""
        with self._lock:
            return self._count >= self._max_connections

    def increment(self) -> bool:
        """
        Increment the connection count if not at capacity.

        :return: True if the connection was accepted, False if at capacity.
        """
        with self._lock:
            if self._count >= self._max_connections:
                LOG.warning(
                    f'Connection rejected: at capacity ({self._count}/{self._max_connections})'
                )
                return False
            self._count += 1
            LOG.debug(f'Connection accepted: {self._count}/{self._max_connections}')
            return True

    def decrement(self) -> None:
        """Decrement the connection count."""
        with self._lock:
            if self._count > 0:
                self._count -= 1
                LOG.debug(f'Connection closed: {self._count}/{self._max_connections}')

    @contextmanager
    def track_connection(self) -> Generator[bool, None, None]:
        """
        Context manager for tracking a connection's lifecycle.

        Usage:
            with connection_metrics.track_connection() as accepted:
                if not accepted:
                    return JSONResponse({"error": "Server at capacity"}, status_code=503)
                # Handle the connection...

        :yields: True if the connection was accepted, False if at capacity.
        """
        accepted = self.increment()
        try:
            yield accepted
        finally:
            if accepted:
                self.decrement()

    def get_stats(self) -> dict[str, int]:
        """Return connection statistics."""
        with self._lock:
            return {
                'active_connections': self._count,
                'max_connections': self._max_connections,
                'available_connections': max(0, self._max_connections - self._count),
            }


# Global connection metrics instance - shared across the application
# Each uvicorn worker process will have its own instance
_connection_metrics: ConnectionMetrics | None = None


def get_connection_metrics() -> ConnectionMetrics | None:
    """Get the global connection metrics instance."""
    return _connection_metrics


def init_connection_metrics(max_connections: int = DEFAULT_MAX_CONNECTIONS) -> ConnectionMetrics:
    """
    Initialize the global connection metrics instance.

    :param max_connections: Maximum number of concurrent connections allowed.
    :return: The initialized ConnectionMetrics instance.
    """
    global _connection_metrics
    _connection_metrics = ConnectionMetrics(max_connections)
    LOG.info(f'Initialized connection metrics with max_connections={max_connections}')
    return _connection_metrics


class ConnectionLimitMiddleware:
    """
    ASGI middleware that enforces connection limits with backpressure.

    This middleware tracks active connections and returns HTTP 503 (Service Unavailable)
    when the server is at capacity. This prevents degradation for existing connections
    by rejecting new ones rather than allowing the event loop to become overloaded.

    Why this is needed:
    - Python's asyncio event loop runs on a single thread
    - With many concurrent SSE connections (e.g., 1000+), every new request competes
      for the same event loop
    - This causes simple operations to work but complex ones (like tools/list) to timeout
    - By limiting connections and returning 503, we ensure existing connections remain responsive

    The middleware only tracks HTTP connections and applies limits to SSE/MCP endpoints.
    Health check and info endpoints are excluded from connection tracking.
    """

    # Paths that should be excluded from connection tracking (health checks, etc.)
    EXCLUDED_PATHS = frozenset(['/', '/health-check'])

    def __init__(self, app: ASGIApp, connection_metrics: ConnectionMetrics | None = None) -> None:
        """
        Initialize the connection limit middleware.

        :param app: The ASGI application to wrap.
        :param connection_metrics: ConnectionMetrics instance to use. If None, uses the global instance.
        """
        self._app = app
        self._connection_metrics = connection_metrics

    def _get_metrics(self) -> ConnectionMetrics | None:
        """Get the connection metrics instance (local or global)."""
        return self._connection_metrics or get_connection_metrics()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """
        Process an ASGI request with connection tracking.

        For HTTP requests to tracked endpoints:
        1. Check if at capacity - if so, return 503
        2. Increment connection count
        3. Process the request
        4. Decrement connection count when done
        """
        if scope['type'] != 'http':
            await self._app(scope, receive, send)
            return

        path = scope.get('path', '')

        # Skip connection tracking for excluded paths (health checks, etc.)
        if path in self.EXCLUDED_PATHS:
            await self._app(scope, receive, send)
            return

        metrics = self._get_metrics()

        # If no metrics configured, pass through without tracking
        if metrics is None:
            await self._app(scope, receive, send)
            return

        # Check capacity and reject if at limit
        if not metrics.increment():
            await self._send_503_response(send, metrics)
            return

        try:
            await self._app(scope, receive, send)
        finally:
            metrics.decrement()

    async def _send_503_response(self, send: Send, metrics: ConnectionMetrics) -> None:
        """Send a 503 Service Unavailable response."""
        stats = metrics.get_stats()
        body = json.dumps({
            'error': 'Server at capacity',
            'message': (
                'The server has reached its maximum connection limit. '
                'Please retry your request later.'
            ),
            'active_connections': stats['active_connections'],
            'max_connections': stats['max_connections'],
        }).encode('utf-8')

        await send({
            'type': 'http.response.start',
            'status': 503,
            'headers': [
                (b'content-type', b'application/json'),
                (b'content-length', str(len(body)).encode('utf-8')),
                (b'retry-after', b'5'),
            ],
        })
        await send({
            'type': 'http.response.body',
            'body': body,
        })
