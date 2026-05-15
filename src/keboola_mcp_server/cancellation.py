"""Process-wide registry mapping MCP JSON-RPC request IDs to their running asyncio Tasks.

In stateless streamable-HTTP mode (the deployment shape used by this server) every
incoming MCP request creates a fresh transport with its own session. The MCP SDK's
built-in cancellation routing (`mcp/shared/session.py`, `_in_flight` dict on the
session) therefore cannot route a `notifications/cancelled` to the tool call it is
meant to abort — they live on different transport instances.

This module bridges that gap with a side-channel:

  * Tools that may run long enough to need cancellation register their task here,
    keyed by their JSON-RPC request id, for the duration of the call.
  * An ASGI middleware (`CancellationInterceptorMiddleware`) peeks at incoming
    `POST /mcp` bodies and, when it sees a `notifications/cancelled`, calls
    `cancel(request_id)` to abort the registered task.
  * The cancelled task surfaces `asyncio.CancelledError` inside the tool, which
    (for `query_data` -> Snowflake) trips the existing CancelledError branch in
    `_SnowflakeWorkspace.execute_query` and fires the backend `cancel_job`.

Scope of this implementation: in-memory, single process. Cross-replica cancellation
without sticky sessions requires a shared store (planned: Postgres) — the async
function signatures here are designed to make that swap non-breaking.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from starlette.types import ASGIApp, Message, Receive, Scope, Send

LOG = logging.getLogger(__name__)


_running: dict[str, asyncio.Task] = {}


def _normalize_request_id(request_id: str | int) -> str:
    """JSON-RPC ids can be int or str; we key everything as str to avoid type mismatches."""
    return str(request_id)


async def register(request_id: str | int, task: asyncio.Task) -> None:
    """Register `task` as the worker for `request_id`. Overwrites silently if a
    task is already registered (logged at WARNING — shouldn't normally happen)."""
    key = _normalize_request_id(request_id)
    if key in _running:
        LOG.warning(f'Cancellation registry: overwriting existing task for request_id={key}')
    _running[key] = task


async def unregister(request_id: str | int) -> None:
    """Remove `request_id` from the registry. No-op if not present."""
    _running.pop(_normalize_request_id(request_id), None)


async def cancel(request_id: str | int) -> bool:
    """Cancel the task registered for `request_id`.

    Returns True if a live task was found and cancelled, False otherwise.
    """
    key = _normalize_request_id(request_id)
    task = _running.get(key)
    if task is None or task.done():
        return False
    task.cancel()
    LOG.info(f'Cancellation registry: cancelled task for request_id={key}')
    return True


@asynccontextmanager
async def track_request(request_id: str | int, task: asyncio.Task) -> AsyncIterator[None]:
    """Context manager: register `task` for `request_id` on entry, unregister on exit
    (success or failure). Use this when you want exception-safe lifecycle management."""
    await register(request_id, task)
    try:
        yield
    finally:
        await unregister(request_id)


class CancellationInterceptorMiddleware:
    """ASGI middleware that intercepts MCP `notifications/cancelled` payloads on
    `POST /mcp` and routes them to the cancellation registry.

    The body is buffered, peeked at, then replayed unchanged to the downstream app
    so the MCP SDK still processes the notification normally (its own `_in_flight`
    lookup will be empty in stateless mode — that's the gap this middleware fills).

    Any parse error / unexpected shape is silently ignored: this layer must never
    break legitimate MCP traffic.
    """

    def __init__(self, app: ASGIApp):
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not _should_inspect(scope):
            await self._app(scope, receive, send)
            return

        chunks: list[Message] = []
        while True:
            msg = await receive()
            chunks.append(msg)
            if msg.get('type') != 'http.request' or not msg.get('more_body', False):
                break

        body = b''.join(c.get('body', b'') for c in chunks if c.get('type') == 'http.request')

        await self._try_cancel_from_body(body)

        chunk_iter = iter(chunks)

        async def replayed_receive() -> Message:
            try:
                return next(chunk_iter)
            except StopIteration:
                return await receive()

        await self._app(scope, replayed_receive, send)

    @staticmethod
    async def _try_cancel_from_body(body: bytes) -> None:
        try:
            payload = json.loads(body) if body else None
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        if payload.get('method') != 'notifications/cancelled':
            return
        params = payload.get('params')
        if not isinstance(params, dict):
            return
        request_id = params.get('requestId')
        if request_id is None:
            return
        cancelled = await cancel(request_id)
        if not cancelled:
            LOG.debug(
                f'Cancellation interceptor: no live task for request_id={request_id} '
                f'(likely already completed or running on a different replica)'
            )


def _should_inspect(scope: Scope) -> bool:
    if scope.get('type') != 'http':
        return False
    if scope.get('method') != 'POST':
        return False
    path = scope.get('path', '')
    return path in ('/mcp', '/mcp/')
