import asyncio
import json
import logging

import httpx
import pytest
from mcp.shared.inbound import (
    InboundModernRoute,
    classify_inbound_request,
    find_duplicated_routing_header,
    mcp_param_headers,
    validate_mcp_param_headers,
    x_mcp_header_map,
)
from mcp_types import CLIENT_CAPABILITIES_META_KEY, PROTOCOL_VERSION_META_KEY
from mcp_types.version import MODERN_PROTOCOL_VERSIONS
from starlette.requests import ClientDisconnect
from starlette.testclient import TestClient

from keboola_mcp_server.router import app as router_app
from keboola_mcp_server.router import rate_limit
from keboola_mcp_server.router.app import create_router_app
from keboola_mcp_server.router.rate_limit import credential_key
from keboola_mcp_server.router.settings import RouterSettings
from keboola_mcp_server.router.stacks import DEFAULT_STACKS, parse_stacks
from tests.router.helpers import ROUTER_URL, Upstream, router_client

_INITIALIZE = {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}}
_PRM = 'resource_metadata="https://mcp-router.example.com/.well-known/oauth-protected-resource"'
_STACK_PRM = 'resource_metadata="https://mcp.eu-central-1.keboola.com/.well-known/oauth-protected-resource"'
_NOT_ISSUED_CHALLENGE = (
    f'Bearer error="invalid_token", error_description="The access token was not issued by this server.", {_PRM}'
)


class TestMcpProxy:
    @pytest.mark.parametrize(
        ('token', 'expected_upstream'),
        [
            ('eu-aws.opaque', 'https://mcp.eu-central-1.keboola.com/mcp'),
            ('us-gcp.opaque', 'https://mcp.us-east4.gcp.keboola.com/mcp'),
        ],
    )
    @pytest.mark.parametrize('path', ['/mcp', '/mcp/'])
    # RFC 7235 §2.1: the scheme is case-insensitive
    @pytest.mark.parametrize('scheme', ['Bearer', 'bearer'])
    def test_routes_by_the_prefix_of_the_bearer_token(
        self, token: str, expected_upstream: str, path: str, scheme: str
    ) -> None:
        # The allowlists are the contract with the regional server: without `Accept` and `Content-Type` it refuses
        # every MCP call, so each forwarded header is pinned in both directions.
        forwarded_response = {
            'content-type': 'application/json',
            'cache-control': 'no-cache',
            'mcp-session-id': 'sess-1',
            'mcp-protocol-version': '2025-06-18',
            'allow': 'GET, POST, DELETE',
        }
        answer = httpx.Response(
            200,
            content=json.dumps({'jsonrpc': '2.0', 'id': 1, 'result': {}}).encode(),
            headers={**forwarded_response, 'Set-Cookie': 'x=1', 'WWW-Authenticate': 'Bearer realm="stack"'},
        )
        forwarded_request = {
            'authorization': f'{scheme} {token}',
            'content-type': 'application/json',
            'accept': 'application/json, text/event-stream',
            'mcp-session-id': 'sess-1',
            'mcp-protocol-version': '2025-06-18',
            'last-event-id': 'event-41',
        }
        upstream = Upstream(answer)
        with router_client(upstream) as client:
            response = client.post(
                f'{path}?transport=streamable&a=1',
                content=json.dumps(_INITIALIZE).encode(),
                headers={
                    **forwarded_request,
                    'X-Forwarded-For': '203.0.113.5',
                    'Cookie': 'keboola_mcp_stack=us-aws',
                    'X-Storage-Api-Url': 'https://connection.keboola.com',
                },
            )

        assert response.status_code == 200
        assert response.json() == {'jsonrpc': '2.0', 'id': 1, 'result': {}}
        for name, value in forwarded_response.items():
            assert response.headers[name] == value
        assert 'set-cookie' not in response.headers
        assert 'www-authenticate' not in response.headers
        [sent] = upstream.requests
        assert str(sent.url) == f'{expected_upstream}?transport=streamable&a=1'
        assert json.loads(sent.content) == _INITIALIZE
        for name, value in forwarded_request.items():
            assert sent.headers[name] == value
        assert sent.headers['accept-encoding'] == 'identity'
        for header in ('x-forwarded-for', 'cookie', 'x-storage-api-url'):
            assert header not in sent.headers
        # a long tool call may keep the stack silent for the /mcp read timeout, not the 30 s of /token
        assert sent.extensions['timeout'] == {'connect': 30.0, 'read': 300.0, 'write': 30.0, 'pool': 30.0}

    def test_the_routing_headers_of_2026_07_28_reach_the_stack_as_its_sdk_checks_them(self) -> None:
        """A `2026-07-28` request carries its method, name and arguments in `Mcp-*` headers, and the stack's SDK
        refuses it with 400 when they are missing or repeated: the router forwards them all, every copy included."""
        version = MODERN_PROTOCOL_VERSIONS[0]
        schema = {'type': 'object', 'properties': {'region': {'type': 'string', 'x-mcp-header': 'Region'}}}
        arguments = {'region': 'eu-west'}
        body = {
            'jsonrpc': '2.0',
            'id': 2,
            'method': 'tools/call',
            'params': {
                'name': 'query_data',
                'arguments': arguments,
                '_meta': {PROTOCOL_VERSION_META_KEY: version, CLIENT_CAPABILITIES_META_KEY: {}},
            },
        }
        headers = {
            'Authorization': 'Bearer us-aws.opaque',
            'Mcp-Protocol-Version': version,
            'Mcp-Method': 'tools/call',
            'Mcp-Name': 'query_data',
            **mcp_param_headers(x_mcp_header_map(schema), arguments),
        }
        upstream = Upstream(httpx.Response(200, json={}))
        with router_client(upstream) as client:
            client.post('/mcp', json=body, headers=headers)
            client.post('/mcp', json=body, headers=[*headers.items(), ('Mcp-Method', 'tools/list')])

        sent, repeated = upstream.requests
        assert isinstance(classify_inbound_request(body, headers=dict(sent.headers)), InboundModernRoute)
        assert validate_mcp_param_headers(schema, arguments, dict(sent.headers)) is None
        assert sent.headers['mcp-param-region'] == 'eu-west'
        assert find_duplicated_routing_header(repeated.headers.multi_items()) == 'mcp-method'

    @pytest.mark.parametrize(
        ('authorization', 'upstream_answer', 'expected_challenge'),
        [
            (None, None, f'Bearer {_PRM}'),
            ('Basic dXNlcjpwYXNz', None, f'Bearer {_PRM}'),
            ('Bearer no-prefix', None, _NOT_ISSUED_CHALLENGE),
            ('Bearer xx-unknown.opaque', None, _NOT_ISSUED_CHALLENGE),
            # the stack refused the token: the client is sent back to the router's metadata, never the stack's
            (
                'Bearer eu-aws.revoked',
                httpx.Response(401, headers={'WWW-Authenticate': f'Bearer {_STACK_PRM}'}),
                f'Bearer error="invalid_token", error_description="The access token is not valid.", {_PRM}',
            ),
        ],
    )
    def test_401_points_to_the_router_metadata(
        self, authorization: str | None, upstream_answer: httpx.Response | None, expected_challenge: str
    ) -> None:
        upstream = Upstream(upstream_answer)
        with router_client(upstream) as client:
            response = client.post(
                '/mcp', json=_INITIALIZE, headers={'Authorization': authorization} if authorization else {}
            )

        assert response.status_code == 401
        assert response.headers['www-authenticate'] == expected_challenge
        assert response.json()['error'] == 'invalid_token'
        assert len(upstream.requests) == (1 if upstream_answer is not None else 0)

    @pytest.mark.asyncio
    async def test_a_streamed_response_reaches_the_client_chunk_by_chunk(self) -> None:
        """The second event is held back by the stack until the client has the first one: a proxy that buffered the
        response would never deliver the first, and the test would time out."""
        first_delivered = asyncio.Event()

        async def events():
            yield b'event: message\ndata: {"jsonrpc":"2.0","method":"notifications/progress"}\n\n'
            await first_delivered.wait()
            yield b'event: message\ndata: {"jsonrpc":"2.0","id":1,"result":{}}\n\n'

        upstream = Upstream(httpx.Response(200, headers={'Content-Type': 'text/event-stream'}, content=events()))
        router_settings = RouterSettings(router_url=ROUTER_URL, stacks=parse_stacks(None))
        app = create_router_app(router_settings, upstream_transport=httpx.MockTransport(upstream))
        sent: list[dict] = []

        requests = [{'type': 'http.request', 'body': json.dumps(_INITIALIZE).encode(), 'more_body': False}]

        async def receive() -> dict:
            if requests:
                return requests.pop()
            await asyncio.Event().wait()  # the client stays connected
            raise AssertionError('unreachable')

        async def send(message: dict) -> None:
            sent.append(message)
            if message['type'] == 'http.response.body' and b'progress' in message.get('body', b''):
                first_delivered.set()

        scope = {
            'type': 'http',
            'asgi': {'version': '3.0'},
            'http_version': '1.1',
            'method': 'POST',
            'scheme': 'https',
            'server': ('mcp-router.example.com', 443),
            'client': ('203.0.113.5', 50000),
            'path': '/mcp',
            'raw_path': b'/mcp',
            'root_path': '',
            'query_string': b'',
            'headers': [(b'authorization', b'Bearer eu-aws.opaque'), (b'content-type', b'application/json')],
        }
        await asyncio.wait_for(app(scope, receive, send), timeout=5)

        [start] = [m for m in sent if m['type'] == 'http.response.start']
        assert start['status'] == 200
        assert (b'content-type', b'text/event-stream') in start['headers']
        bodies = [m['body'] for m in sent if m['type'] == 'http.response.body' and m.get('body')]
        assert len(bodies) == 2
        assert b'progress' in bodies[0]
        assert b'"result"' in bodies[1]

    def test_each_stack_gets_its_own_mcp_pool(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Built without a test transport, as in production: an injected transport ignores `limits`, so every other
        test shares one pool whatever the app configures."""
        created: list[dict] = []

        class RecordingClient(httpx.AsyncClient):
            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                created.append(kwargs)

        monkeypatch.setattr(httpx, 'AsyncClient', RecordingClient)
        create_router_app(RouterSettings(router_url=ROUTER_URL, stacks=parse_stacks(None), mcp_max_connections=7))

        mcp = [kwargs for kwargs in created if 'limits' in kwargs]
        # /token and /revoke keep httpx's default pool of their own
        assert len([kwargs for kwargs in created if 'limits' not in kwargs]) == 1
        assert len(mcp) == len(DEFAULT_STACKS)
        assert all(kwargs['limits'].max_connections == 7 and kwargs['transport'] is None for kwargs in mcp)

    def test_a_stack_silent_after_the_first_chunk_ends_the_stream(self) -> None:
        """Past the status line a timeout can only end the stream: the client keeps what it got, and the error does
        not escape the app."""
        first = b'event: message\ndata: {"jsonrpc":"2.0","method":"notifications/progress"}\n\n'

        async def events():
            yield first
            raise httpx.ReadTimeout('the stack went silent')

        upstream = Upstream(httpx.Response(200, headers={'Content-Type': 'text/event-stream'}, content=events()))
        with router_client(upstream) as client:
            response = client.post('/mcp', json=_INITIALIZE, headers={'Authorization': 'Bearer eu-aws.opaque'})

        assert response.status_code == 200
        assert response.content == first

    @pytest.mark.asyncio
    @pytest.mark.parametrize('spec_version', ['2.3', '2.4'])
    async def test_a_client_that_goes_away_closes_the_upstream_stream(self, spec_version: str) -> None:
        """Starlette sees a gone client as an `http.disconnect` (ASGI HTTP spec 2.3, uvicorn) or as `send` raising
        (2.4); either way the stack's answer is closed at once, not when the garbage collector gets to it."""
        closed = asyncio.Event()

        class EndlessEvents(httpx.AsyncByteStream):
            """What the pooled connection sees: `aclose()` is what hands it back."""

            async def __aiter__(self):
                while True:
                    yield b'event: ping\ndata: {}\n\n'
                    await asyncio.sleep(0.01)

            async def aclose(self) -> None:
                closed.set()

        upstream = Upstream(httpx.Response(200, headers={'Content-Type': 'text/event-stream'}, stream=EndlessEvents()))
        router_settings = RouterSettings(router_url=ROUTER_URL, stacks=parse_stacks(None))
        app = create_router_app(router_settings, upstream_transport=httpx.MockTransport(upstream))
        messages = [{'type': 'http.request', 'body': b'', 'more_body': False}]
        first_chunk = asyncio.Event()

        async def receive() -> dict:
            if messages:
                return messages.pop()
            await first_chunk.wait()
            return {'type': 'http.disconnect'}

        async def send(message: dict) -> None:
            if message['type'] == 'http.response.body' and message.get('body'):
                if first_chunk.is_set() and spec_version == '2.4':
                    raise OSError('the client went away')
                first_chunk.set()

        scope = {
            'type': 'http',
            'asgi': {'version': '3.0', 'spec_version': spec_version},
            'http_version': '1.1',
            'method': 'GET',
            'scheme': 'https',
            'server': ('mcp-router.example.com', 443),
            'client': ('203.0.113.5', 50000),
            'path': '/mcp',
            'raw_path': b'/mcp',
            'root_path': '',
            'query_string': b'',
            'headers': [(b'authorization', b'Bearer eu-aws.opaque')],
        }
        try:
            await asyncio.wait_for(app(scope, receive, send), timeout=5)
        except ClientDisconnect:
            assert spec_version == '2.4'
        assert closed.is_set()

    @pytest.mark.parametrize(
        ('failure', 'expected_status'),
        [
            (httpx.ReadTimeout('a long tool call'), 504),
            (httpx.ConnectTimeout('slow'), 504),
            (httpx.ConnectError('down'), 502),
            # the router's own pool is full: not the stack's fault
            (httpx.PoolTimeout('pool'), 503),
        ],
    )
    @pytest.mark.parametrize('path', ['/mcp', '/token'])
    def test_an_upstream_failure_is_a_gateway_error(self, failure: Exception, expected_status: int, path: str) -> None:
        with router_client(Upstream(failure)) as client:
            if path == '/mcp':
                response = client.post(path, json=_INITIALIZE, headers={'Authorization': 'Bearer us-aws.opaque'})
            else:
                response = client.post(path, data={'grant_type': 'refresh_token', 'refresh_token': 'us-aws.rt'})

        assert response.status_code == expected_status
        assert response.json()['error'] == 'temporarily_unavailable'
        assert response.headers['cache-control'] == 'no-store'

    def test_a_token_over_the_limit_gets_429_while_another_token_from_the_same_ip_passes(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        upstream = Upstream(httpx.Response(200, json={}))

        def call(client: TestClient, path: str, token: str) -> int:
            headers = {'Authorization': f'Bearer {token}', 'X-Forwarded-For': '198.51.100.3'}
            return client.post(path, json=_INITIALIZE, headers=headers).status_code

        with caplog.at_level(logging.WARNING), router_client(upstream, mcp_rate_limit_per_token=2) as client:
            first = [call(client, path, 'us-aws.first') for path in ('/mcp', '/mcp/', '/mcp')]
            second = call(client, '/mcp', 'us-aws.second')

        assert first == [200, 200, 429]  # both spellings count as one endpoint
        assert second == 200
        with router_client(Upstream(httpx.Response(200, json={})), now=125.0, mcp_rate_limit_per_token=1) as client:
            headers = {'Authorization': 'Bearer us-aws.third'}
            refused = [client.post('/mcp', json=_INITIALIZE, headers=headers) for _ in range(2)][-1]
        assert refused.status_code == 429
        assert refused.headers['retry-after'] == '55'  # the seconds left in the window (the test clock is at 125 s)
        assert len(upstream.requests) == 3
        logged = '\n'.join(r.getMessage() for r in caplog.records)
        assert 'Rate limit reached: group=credential:/mcp, key=sha256:' in logged
        assert 'us-aws.first' not in logged

    @pytest.mark.asyncio
    async def test_a_token_may_hold_only_its_in_flight_slots(self, caplog: pytest.LogCaptureFixture) -> None:
        """A token whose slot holds a streamed answer gets 429 for the next request, another token does not, and the
        slot frees up when the answer ends: the rate limit alone would let one token's slow calls hold the pool. A
        stalled upload holds no slot (the body is read first), and a handler cancelled while the stack has not answered
        gives its slot back."""
        finish = asyncio.Event()
        upstream_reached = asyncio.Event()
        uploading = asyncio.Event()

        async def held():
            yield b'event: message\ndata: {}\n\n'
            await finish.wait()

        async def answer(request: httpx.Request) -> httpx.Response:
            if request.headers.get('mcp-session-id') == 'held':
                return httpx.Response(200, headers={'Content-Type': 'text/event-stream'}, content=held())
            if request.headers.get('mcp-session-id') == 'never-answered':
                upstream_reached.set()
                await asyncio.Event().wait()
            return httpx.Response(200, json={})

        router_settings = RouterSettings(
            router_url=ROUTER_URL, stacks=parse_stacks(None), mcp_max_in_flight_per_token=1
        )
        app = create_router_app(router_settings, upstream_transport=httpx.MockTransport(answer))
        streaming = asyncio.Event()

        async def call(token: str, session: str = 'quick', *, upload: bool = True) -> dict:
            requests = [{'type': 'http.request', 'body': json.dumps(_INITIALIZE).encode(), 'more_body': False}]
            sent: list[dict] = []

            async def receive() -> dict:
                if requests and upload:
                    return requests.pop()
                if not upload:
                    uploading.set()
                await asyncio.Event().wait()  # the client stays connected
                raise AssertionError('unreachable')

            async def send(message: dict) -> None:
                sent.append(message)
                if message['type'] == 'http.response.body' and message.get('body') and session == 'held':
                    streaming.set()

            scope = {
                'type': 'http',
                'asgi': {'version': '3.0'},
                'http_version': '1.1',
                'method': 'POST',
                'scheme': 'https',
                'server': ('mcp-router.example.com', 443),
                'client': ('203.0.113.5', 50000),
                'path': '/mcp',
                'raw_path': b'/mcp',
                'root_path': '',
                'query_string': b'',
                'headers': [(b'authorization', f'Bearer {token}'.encode()), (b'mcp-session-id', session.encode())],
            }
            await asyncio.wait_for(app(scope, receive, send), timeout=5)
            return next(m for m in sent if m['type'] == 'http.response.start')

        stream = asyncio.create_task(call('us-aws.first', 'held'))
        await asyncio.wait_for(streaming.wait(), timeout=5)
        with caplog.at_level(logging.WARNING):
            refused = await call('us-aws.first')
            refused_again = await call('us-aws.first')
        other = await call('us-aws.second')
        finish.set()
        streamed = await stream
        after = await call('us-aws.first')

        stalled = asyncio.create_task(call('us-aws.third', upload=False))
        await asyncio.wait_for(uploading.wait(), timeout=5)
        beside_the_upload = await call('us-aws.third')
        stalled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stalled
        cancelled = asyncio.create_task(call('us-aws.fourth', 'never-answered'))
        await asyncio.wait_for(upstream_reached.wait(), timeout=5)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        after_the_cancel = await call('us-aws.fourth')

        assert refused['status'] == refused_again['status'] == 429
        assert (b'retry-after', b'1') in refused['headers']
        # logged once while the token stays full, by its hash only
        [line] = [r.getMessage() for r in caplog.records if 'Concurrency limit reached' in r.getMessage()]
        assert line == f'[router] Concurrency limit reached: key=sha256:{credential_key("us-aws.first")[:12]}, limit=1'
        assert 'us-aws.first' not in '\n'.join(r.getMessage() for r in caplog.records)
        assert [other['status'], streamed['status'], after['status']] == [200, 200, 200]
        assert [beside_the_upload['status'], after_the_cancel['status']] == [200, 200]

    @pytest.mark.parametrize(
        'answer',
        [
            httpx.Response(200, json={}),
            httpx.Response(401),
            httpx.ReadTimeout('a long tool call'),
            httpx.ConnectError('down'),
            # a header the response cannot encode fails the request after the stack answered (500)
            httpx.Response(200, headers=[(b'mcp-session-id', 'sess-č'.encode())], json={}),
        ],
    )
    def test_every_way_out_frees_the_in_flight_slot(self, answer: httpx.Response | Exception) -> None:
        with router_client(Upstream(answer), raise_server_exceptions=False, mcp_max_in_flight_per_token=1) as client:
            statuses = [
                client.post('/mcp', json=_INITIALIZE, headers={'Authorization': 'Bearer us-aws.t'}).status_code
                for _ in range(3)
            ]

        assert 429 not in statuses
        assert len(set(statuses)) == 1

    @pytest.mark.parametrize(
        'case',
        [
            # the per-address ceiling is for /register only
            'address_ceiling_ignored',
            # a token of no known stack is answered 401 and not counted
            'unknown_prefix_not_counted',
            # random tokens on /mcp (a public prefix) do not switch off the throttle on refresh tokens
            'mcp_flood_keeps_token_limit',
            # past the key cap /mcp fails open: no session is refused because others filled the map
            'mcp_past_the_cap_fails_open',
        ],
    )
    def test_the_mcp_limit_is_kept_apart(self, case: str, monkeypatch: pytest.MonkeyPatch) -> None:
        upstream = Upstream(httpx.Response(200, json={}))
        headers = {'X-Forwarded-For': '198.51.100.4'}

        def mcp(client: TestClient, token: str) -> int:
            auth = {**headers, 'Authorization': f'Bearer {token}'}
            return client.post('/mcp', json=_INITIALIZE, headers=auth).status_code

        if case in ('mcp_flood_keeps_token_limit', 'mcp_past_the_cap_fails_open'):
            monkeypatch.setattr(rate_limit, '_MAX_TRACKED_KEYS', 1)
        if case == 'mcp_past_the_cap_fails_open':
            # an overflow bucket of 1 would refuse the second request; fail-open refuses none
            monkeypatch.setattr(router_app, '_CREDENTIAL_OVERFLOW_LIMIT_PER_MINUTE', 1)
        with router_client(
            upstream, api_rate_limit_per_ip=1, mcp_rate_limit_per_token=1, token_rate_limit_per_credential=1
        ) as client:
            if case == 'address_ceiling_ignored':
                assert [mcp(client, f'us-aws.t{i}') for i in range(3)] == [200, 200, 200]
            elif case == 'unknown_prefix_not_counted':
                assert [mcp(client, 'xx.t'), mcp(client, 'xx.t'), mcp(client, 'us-aws.t'), mcp(client, 'us-aws.t')] == [
                    401,
                    401,
                    200,
                    429,
                ]
            elif case == 'mcp_past_the_cap_fails_open':
                mcp(client, 'us-aws.first')
                assert [mcp(client, 'us-aws.later') for _ in range(3)] == [200, 200, 200]
            else:
                mcp(client, 'us-aws.random')
                refresh = {'grant_type': 'refresh_token', 'refresh_token': 'us-aws.rt'}
                assert [client.post('/token', data=refresh, headers=headers).status_code for _ in range(2)] == [
                    200,
                    429,
                ]

    @pytest.mark.parametrize('path', ['/mcp', '/mcp/', '/token'])
    def test_a_browser_client_may_call_and_read_the_mcp_transport(self, path: str) -> None:
        """A preflight for the MCP transport's headers is allowed, and a cross-origin answer exposes the headers the
        client must read: without them a browser refuses the request or hides the session id."""
        origin = {'Origin': 'https://app.example'}
        requested = (
            'authorization, content-type, mcp-protocol-version, mcp-session-id, last-event-id, mcp-method, mcp-name, '
            'mcp-param-region'
        )
        answer = httpx.Response(200, json={}, headers={'Mcp-Session-Id': 'sess-1'})
        with router_client(Upstream(answer)) as client:
            preflight = client.options(
                path,
                headers={
                    **origin,
                    'Access-Control-Request-Method': 'POST',
                    'Access-Control-Request-Headers': requested,
                },
            )
            if path == '/token':
                refresh = {'grant_type': 'refresh_token', 'refresh_token': 'us-aws.rt'}
                call = client.post(path, data=refresh, headers=origin)
            else:
                call = client.post(path, json=_INITIALIZE, headers={**origin, 'Authorization': 'Bearer us-aws.opaque'})

        assert preflight.status_code == 200
        assert preflight.headers['access-control-allow-origin'] == '*'
        assert {h.strip().lower() for h in preflight.headers['access-control-allow-headers'].split(',')} >= set(
            requested.split(', ')
        )
        assert call.status_code == 200
        assert {h.strip().lower() for h in call.headers['access-control-expose-headers'].split(',')} >= {
            'mcp-session-id',
            'mcp-protocol-version',
            'www-authenticate',
            'retry-after',
        }

    @pytest.mark.parametrize('method', ['GET', 'DELETE'])
    @pytest.mark.parametrize('path', ['/mcp', '/mcp/'])
    def test_get_and_delete_are_forwarded_and_logged(
        self, method: str, path: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """`/mcp` is the busiest route: its request-log line must name its method, path and stack, never
        `<other>` or `stack_key=-`."""
        upstream = Upstream(httpx.Response(405, headers={'Allow': 'POST'}))
        with caplog.at_level(logging.INFO), router_client(upstream) as client:
            response = client.request(
                method, path, headers={'Authorization': 'Bearer us-aws.opaque', 'Mcp-Session-Id': 'sess-1'}
            )

        assert response.status_code == 405
        assert response.headers['allow'] == 'POST'
        [sent] = upstream.requests
        assert sent.method == method
        assert sent.headers['mcp-session-id'] == 'sess-1'
        [line] = [r.getMessage() for r in caplog.records if ' status=' in r.getMessage()]
        assert line.startswith(f'[router] {method} {path} status=405 stack_key=us-aws duration_ms=')
