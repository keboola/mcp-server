"""Tests for the process-wide cancellation registry and ASGI interceptor."""

import asyncio
import contextlib
import json

import pytest

from keboola_mcp_server import cancellation
from keboola_mcp_server.cancellation import (
    CancellationInterceptorMiddleware,
    cancel,
    register,
    track_request,
    unregister,
)


@pytest.fixture(autouse=True)
def _clear_registry():
    """Ensure each test starts with an empty registry and no leftover cancel intents."""
    cancellation._running.clear()
    cancellation._pending_cancels.clear()
    yield
    cancellation._running.clear()
    cancellation._pending_cancels.clear()


@pytest.mark.asyncio
async def test_register_then_cancel_aborts_the_task() -> None:
    started = asyncio.Event()
    done_inside_task = asyncio.Event()

    async def runner():
        started.set()
        try:
            await asyncio.Event().wait()  # block forever
        except asyncio.CancelledError:
            done_inside_task.set()
            raise

    task = asyncio.create_task(runner())
    await started.wait()
    await register('req-1', task)

    assert await cancel('req-1') is True
    with pytest.raises(asyncio.CancelledError):
        await task
    assert done_inside_task.is_set()


@pytest.mark.asyncio
async def test_cancel_returns_false_when_no_task() -> None:
    assert await cancel('does-not-exist') is False
    # Intent must be recorded so a later register() honours it.
    assert 'does-not-exist' in cancellation._pending_cancels


@pytest.mark.asyncio
async def test_cancel_before_register_is_honoured_on_register() -> None:
    """The cancel notification can race ahead of registration. When that happens,
    the intent must survive and the upcoming register() must cancel the task."""
    started = asyncio.Event()
    cancelled_inside = asyncio.Event()

    async def runner():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_inside.set()
            raise

    # Cancel arrives FIRST — no task is registered yet.
    assert await cancel('race-id') is False
    assert 'race-id' in cancellation._pending_cancels

    # Now the tool coroutine reaches its register() call.
    task = asyncio.create_task(runner())
    await started.wait()
    await register('race-id', task)

    # register() consumed the intent and cancelled the task.
    assert 'race-id' not in cancellation._pending_cancels
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled_inside.is_set()


@pytest.mark.asyncio
async def test_pending_cancels_is_bounded() -> None:
    """A flood of orphaned cancels must not grow the set unbounded."""
    original_cap = cancellation._MAX_PENDING_CANCELS
    try:
        cancellation._MAX_PENDING_CANCELS = 4
        for i in range(20):
            await cancel(f'orphan-{i}')
        assert len(cancellation._pending_cancels) <= 4
    finally:
        cancellation._MAX_PENDING_CANCELS = original_cap


@pytest.mark.asyncio
async def test_cancel_returns_false_when_task_already_done() -> None:
    async def noop():
        return 'ok'

    task = asyncio.create_task(noop())
    await task
    await register('req-done', task)
    assert await cancel('req-done') is False
    # An already-completed task is "done work" — don't record a stale intent.
    assert 'req-done' not in cancellation._pending_cancels


@pytest.mark.asyncio
async def test_track_request_unregisters_on_normal_exit() -> None:
    async def noop():
        return None

    task = asyncio.create_task(noop())
    async with track_request('req-2', task):
        assert 'req-2' in cancellation._running
    assert 'req-2' not in cancellation._running
    await task


@pytest.mark.asyncio
async def test_track_request_unregisters_on_exception() -> None:
    async def noop():
        return None

    task = asyncio.create_task(noop())

    async def raise_inside_context() -> None:
        async with track_request('req-3', task):
            raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        await raise_inside_context()
    assert 'req-3' not in cancellation._running
    await task


@pytest.mark.asyncio
@pytest.mark.parametrize('request_id', [42, '42', 'a-string-id'])
async def test_register_normalises_int_and_str_ids(request_id) -> None:
    async def noop():
        return None

    task = asyncio.create_task(noop())
    try:
        await register(request_id, task)
        assert str(request_id) in cancellation._running
        # Cancellation works whether caller passes the same form or its string version
        await register(request_id, task)  # idempotent on re-register
    finally:
        await unregister(request_id)
        await task


async def _drive_middleware(
    body: bytes | None = None,
    *,
    chunks_override: list[dict] | None = None,
    path: str = '/mcp',
    method: str = 'POST',
) -> dict:
    """Run the middleware against a fake ASGI request and return what the downstream
    app received (so we can assert the body was replayed unchanged).

    Either pass a single-chunk `body` or override with a list of `http.request`
    chunks (for multi-chunk tests).
    """
    received_chunks: list[dict] = []

    async def downstream(scope, receive, send):
        while True:
            msg = await receive()
            received_chunks.append(msg)
            if msg.get('type') != 'http.request' or not msg.get('more_body', False):
                break
        await send({'type': 'http.response.start', 'status': 202, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    scope = {'type': 'http', 'method': method, 'path': path}
    if chunks_override is None:
        chunks_to_send = [{'type': 'http.request', 'body': body or b'', 'more_body': False}]
    else:
        chunks_to_send = chunks_override
    sent_iter = iter(chunks_to_send)

    async def receive():
        try:
            return next(sent_iter)
        except StopIteration:
            return {'type': 'http.disconnect'}

    async def send(_msg):
        pass

    middleware = CancellationInterceptorMiddleware(downstream)
    await middleware(scope, receive, send)
    return {'received_chunks': received_chunks}


@pytest.mark.asyncio
async def test_middleware_routes_cancel_notification_to_registry() -> None:
    started = asyncio.Event()
    cancelled_flag = asyncio.Event()

    async def runner():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_flag.set()
            raise

    task = asyncio.create_task(runner())
    await started.wait()  # ensure runner is inside the try block before cancelling
    await register('req-mw', task)
    try:
        body = json.dumps(
            {'jsonrpc': '2.0', 'method': 'notifications/cancelled', 'params': {'requestId': 'req-mw'}}
        ).encode()
        result = await _drive_middleware(body)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled_flag.is_set()
        # Body must have been replayed verbatim to the downstream app
        replayed = b''.join(c.get('body', b'') for c in result['received_chunks'])
        assert replayed == body
    finally:
        await unregister('req-mw')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'payload',
    [
        {'jsonrpc': '2.0', 'method': 'tools/call', 'id': 1, 'params': {}},  # unrelated request
        {'jsonrpc': '2.0', 'method': 'notifications/progress', 'params': {'progressToken': 'x'}},  # non-cancel
        {'jsonrpc': '2.0', 'method': 'notifications/cancelled', 'params': {}},  # missing requestId
        {'jsonrpc': '2.0'},  # no method
    ],
    ids=['tools_call', 'progress_notification', 'cancel_without_request_id', 'no_method'],
)
async def test_middleware_passes_non_cancel_payloads_through(payload: dict) -> None:
    async def runner():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise

    task = asyncio.create_task(runner())
    await register('untouched', task)
    try:
        body = json.dumps(payload).encode()
        await _drive_middleware(body)
        await asyncio.sleep(0)
        assert not task.done()  # the unrelated task is still running
    finally:
        task.cancel()
        await unregister('untouched')


@pytest.mark.asyncio
async def test_middleware_tolerates_garbage_body() -> None:
    # No registered tasks; the middleware must not crash on malformed input.
    # The garbage doesn't contain the cancel-method token, so json.loads is never called.
    await _drive_middleware(b'not json')
    await _drive_middleware(b'')


@pytest.mark.asyncio
async def test_middleware_skips_parse_when_substring_missing(mocker) -> None:
    """The cheap `b'notifications/cancelled' in body` substring check must run
    BEFORE json.loads — large `tools/call` payloads pay no JSON-parse cost."""
    spy = mocker.spy(cancellation.json, 'loads')
    # A realistic-ish tools/call body that does NOT contain the cancel-method token.
    big_body = json.dumps(
        {
            'jsonrpc': '2.0',
            'method': 'tools/call',
            'id': 1,
            'params': {'name': 'query_data', 'arguments': {'sql_query': 'SELECT ' + 'x,' * 500}},
        }
    ).encode()
    await _drive_middleware(big_body)
    assert spy.call_count == 0


@pytest.mark.asyncio
async def test_middleware_skips_inspection_for_multi_chunk_bodies() -> None:
    """Cancel notifications are tiny and always fit in a single ASGI chunk. If the
    body spans multiple chunks we don't bother inspecting — but we MUST still
    stream-replay every chunk untouched to the downstream app."""
    chunks = [
        {'type': 'http.request', 'body': b'{"jsonrpc":"2.0","method":"', 'more_body': True},
        {'type': 'http.request', 'body': b'notifications/cancelled","params":{"requestId":"x"}}', 'more_body': False},
    ]
    started = asyncio.Event()

    async def runner():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(runner())
    await started.wait()
    await register('x', task)
    try:
        result = await _drive_middleware(chunks_override=chunks)
        await asyncio.sleep(0)
        # Multi-chunk → we did not parse → task is still running.
        assert not task.done()
        # But the chunks must have reached downstream verbatim.
        downstream_bodies = [c.get('body', b'') for c in result['received_chunks']]
        assert downstream_bodies == [chunks[0]['body'], chunks[1]['body']]
    finally:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
        await unregister('x')


@pytest.mark.asyncio
async def test_middleware_skips_inspection_when_body_exceeds_cap(mocker) -> None:
    """A single oversized chunk must NOT be parsed for cancel — protects against
    a hostile or accidental large body being put through json.loads."""
    spy = mocker.spy(cancellation.json, 'loads')
    # Build a payload that exceeds the cap and happens to contain the cancel token
    # (so the substring check would have matched, had we run it).
    oversized = (
        b'{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"x"},'
        b'"_pad":"' + b'A' * (cancellation._MAX_INSPECT_BYTES + 16) + b'"}'
    )
    await _drive_middleware(oversized)
    assert spy.call_count == 0


@pytest.mark.asyncio
async def test_middleware_skips_non_mcp_paths() -> None:
    async def runner():
        await asyncio.Event().wait()

    task = asyncio.create_task(runner())
    await register('still-alive', task)
    try:
        # A cancel-shaped body, but on a different path — must not route to registry.
        body = json.dumps({'method': 'notifications/cancelled', 'params': {'requestId': 'still-alive'}}).encode()
        await _drive_middleware(body, path='/health')
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        task.cancel()
        await unregister('still-alive')
