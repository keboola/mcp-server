"""Tests for the process-wide cancellation registry and ASGI interceptor."""

import asyncio
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
    """Ensure each test starts with an empty registry."""
    cancellation._running.clear()
    yield
    cancellation._running.clear()


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


@pytest.mark.asyncio
async def test_cancel_returns_false_when_task_already_done() -> None:
    async def noop():
        return 'ok'

    task = asyncio.create_task(noop())
    await task
    await register('req-done', task)
    assert await cancel('req-done') is False


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


async def _drive_middleware(body: bytes, *, path: str = '/mcp', method: str = 'POST') -> dict:
    """Run the middleware against a single fake ASGI request and return what the
    downstream app received (so we can assert the body was replayed unchanged)."""
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
    chunks_to_send = [{'type': 'http.request', 'body': body, 'more_body': False}]
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
    await _drive_middleware(b'not json')
    await _drive_middleware(b'')


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
