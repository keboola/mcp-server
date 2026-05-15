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

# Cancel-before-register intents: when `cancel()` arrives for a request id whose
# task hasn't been registered yet (e.g. the cancel notification was processed by
# the middleware before the tools/call coroutine reached `register()`), we record
# the id here and the next `register()` for that id will honour it. Bounded to
# avoid unbounded growth from orphaned cancels (request never reached this pod).
_pending_cancels: set[str] = set()
_MAX_PENDING_CANCELS = 1024


def _normalize_request_id(request_id: str | int) -> str:
    """JSON-RPC ids can be int or str; we key everything as str to avoid type mismatches."""
    return str(request_id)


async def register(request_id: str | int, task: asyncio.Task) -> None:
    """Register `task` as the worker for `request_id`. Overwrites silently if a
    task is already registered (logged at WARNING — shouldn't normally happen).

    Honours any cancel intent recorded for this id before `register()` ran: if
    `cancel()` was called for the same id earlier, the task is cancelled immediately.
    """
    key = _normalize_request_id(request_id)
    if key in _running:
        LOG.warning(f'Cancellation registry: overwriting existing task for request_id={key}')
    _running[key] = task

    if key in _pending_cancels:
        _pending_cancels.discard(key)
        if not task.done():
            task.cancel()
            LOG.info(
                f'Cancellation registry: honoured pre-cancellation on register for request_id={key} '
                f'(cancel notification arrived before the tool task was registered)'
            )


async def unregister(request_id: str | int) -> None:
    """Remove `request_id` from the registry. No-op if not present."""
    _running.pop(_normalize_request_id(request_id), None)


async def cancel(request_id: str | int) -> bool:
    """Cancel the task registered for `request_id`.

    Returns True if a live task was found and cancelled. If no task is registered
    at all, records the cancel intent so the next matching `register()` will honour
    it — and returns False. If a task is registered but has already completed,
    returns False without recording an intent (the work is already done).
    """
    key = _normalize_request_id(request_id)
    task = _running.get(key)
    if task is not None:
        if task.done():
            return False
        task.cancel()
        LOG.info(f'Cancellation registry: cancelled task for request_id={key}')
        return True

    # No task registered yet. Remember the intent for an upcoming register() —
    # this closes the race window between `asyncio.create_task` in the tool
    # coroutine and the registry registration that follows it.
    if len(_pending_cancels) >= _MAX_PENDING_CANCELS:
        # Drop an arbitrary existing intent to keep the set bounded. Pathological
        # case only — in normal operation intents are consumed by register().
        _pending_cancels.discard(next(iter(_pending_cancels)))
    _pending_cancels.add(key)
    return False


@asynccontextmanager
async def track_request(request_id: str | int, task: asyncio.Task) -> AsyncIterator[None]:
    """Context manager: register `task` for `request_id` on entry, unregister on exit
    (success or failure). Use this when you want exception-safe lifecycle management."""
    await register(request_id, task)
    try:
        yield
    finally:
        await unregister(request_id)


_MAX_INSPECT_BYTES = 8 * 1024
_CANCEL_METHOD_TOKEN = b'notifications/cancelled'


class CancellationInterceptorMiddleware:
    """ASGI middleware that intercepts MCP `notifications/cancelled` payloads on
    `POST /mcp` and routes them to the cancellation registry.

    We peek ONLY at the first body chunk (cancel notifications are always small,
    well under a single ASGI chunk — typically ~100 bytes). If that chunk is fully
    self-contained (`more_body=False`), fits within `_MAX_INSPECT_BYTES`, and
    contains the literal `notifications/cancelled` substring, we parse it as JSON
    and route to the registry. In every other case we skip inspection entirely.

    Crucially, the body itself is NOT buffered into memory: we replay the first
    chunk and pass the original receive callable through for any subsequent chunks,
    so large `tools/call` payloads stream straight to the downstream app without
    being duplicated in memory. The cheap substring check also avoids `json.loads`
    on the (vast majority of) request bodies that aren't cancel notifications.

    Any parse error / unexpected shape is silently ignored: this layer must never
    break legitimate MCP traffic.
    """

    def __init__(self, app: ASGIApp):
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not _should_inspect(scope):
            await self._app(scope, receive, send)
            return

        first_msg = await receive()

        if (
            first_msg.get('type') == 'http.request'
            and not first_msg.get('more_body', False)
            and len(first_msg.get('body', b'')) <= _MAX_INSPECT_BYTES
        ):
            await self._try_cancel_from_body(first_msg['body'])

        sent_first = False

        async def replayed_receive() -> Message:
            nonlocal sent_first
            if not sent_first:
                sent_first = True
                return first_msg
            return await receive()

        await self._app(scope, replayed_receive, send)

    @staticmethod
    async def _try_cancel_from_body(body: bytes) -> None:
        # Cheap substring filter first: this skips the JSON parse for every body
        # that doesn't even mention the cancel method name.
        if _CANCEL_METHOD_TOKEN not in body:
            return
        try:
            payload = json.loads(body)
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
                f'(likely already completed or running on a different replica); '
                f'cancel intent recorded for an upcoming register() if any'
            )


def _should_inspect(scope: Scope) -> bool:
    if scope.get('type') != 'http':
        return False
    if scope.get('method') != 'POST':
        return False
    path = scope.get('path', '')
    return path in ('/mcp', '/mcp/')
