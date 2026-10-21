import json
import logging
from urllib.parse import parse_qs, parse_qsl, urlparse

import httpx
import pytest
from starlette.testclient import TestClient

from keboola_mcp_server.cli import parse_args
from keboola_mcp_server.router import rate_limit
from keboola_mcp_server.router.app import create_router_app
from keboola_mcp_server.router.picker import render_picker
from keboola_mcp_server.router.settings import RouterSettings
from keboola_mcp_server.router.stacks import DEFAULT_STACKS, parse_stacks, router_issuer, router_resource

ROUTER_URL = 'https://mcp-router.example.com'


class _Upstream:
    """Records what the router sends to the regional servers and answers with a fixed response."""

    def __init__(self, response: httpx.Response | Exception | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self._response = response or httpx.Response(200, json={'access_token': 'eu-aws.at'})

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def _client(upstream: _Upstream | None = None, **settings) -> TestClient:
    router_settings = RouterSettings(router_url=ROUTER_URL, stacks=parse_stacks(None), **settings)
    app = create_router_app(
        router_settings,
        upstream_transport=httpx.MockTransport(upstream or _Upstream()),
        # one rate-limit window for the whole test, so a minute boundary cannot reset the counts halfway
        rate_limit_clock=lambda: 120.0,
    )
    return TestClient(app, base_url=ROUTER_URL, follow_redirects=False)


class TestMetadata:
    def test_one_issuer_in_both_documents(self) -> None:
        with _client() as client:
            prm = client.get('/.well-known/oauth-protected-resource').json()
            prm_path_aware = client.get('/.well-known/oauth-protected-resource/mcp').json()
            metadata = client.get('/.well-known/oauth-authorization-server').json()

        assert prm == prm_path_aware
        assert prm['resource'] == 'https://mcp-router.example.com/mcp'
        assert prm['authorization_servers'] == ['https://mcp-router.example.com']
        assert metadata['issuer'] == prm['authorization_servers'][0]
        assert metadata['authorization_endpoint'] == 'https://mcp-router.example.com/authorize'
        assert metadata['token_endpoint'] == 'https://mcp-router.example.com/token'
        assert metadata['registration_endpoint'] == 'https://mcp-router.example.com/register'
        assert metadata['revocation_endpoint'] == 'https://mcp-router.example.com/revoke'
        assert metadata['code_challenge_methods_supported'] == ['S256']
        assert metadata['authorization_response_iss_parameter_supported'] is True

    @pytest.mark.parametrize(
        ('router_url', 'issuer'),
        [
            ('https://mcp-router.example.com', 'https://mcp-router.example.com'),
            ('https://MCP-Router.example.com/', 'https://mcp-router.example.com'),
            ('https://mcp-router.example.com/mcp', 'https://mcp-router.example.com'),
        ],
    )
    def test_issuer_is_the_origin_of_the_router_url(self, router_url: str, issuer: str) -> None:
        assert router_issuer(router_url) == issuer
        assert router_resource(router_url) == f'{issuer}/mcp'

    @pytest.mark.parametrize(('challenge', 'expected_status'), [('token-from-openai', 200), (None, 404)])
    def test_openai_apps_challenge(self, challenge: str | None, expected_status: int) -> None:
        with _client(openai_apps_challenge=challenge) as client:
            response = client.get('/.well-known/openai-apps-challenge')

        assert response.status_code == expected_status
        assert response.headers['content-type'].startswith('text/plain')
        if challenge:
            assert response.text == challenge

    def test_register_answers_locally_with_a_fresh_client_id(self) -> None:
        upstream = _Upstream()
        body = {'redirect_uris': ['https://chatgpt.com/connector_platform_oauth_redirect'], 'client_name': 'ChatGPT'}
        with _client(upstream) as client:
            first = client.post('/register', json={**body, 'token_endpoint_auth_method': 'none'})
            second = client.post('/register', json={**body, 'token_endpoint_auth_method': 'none'})

        assert first.status_code == second.status_code == 201
        assert first.json()['client_id'] != second.json()['client_id']
        assert upstream.requests == []


class TestAuthorize:
    QUERY = (
        'response_type=code&client_id=abc&redirect_uri=https%3A%2F%2Fchatgpt.com%2Fconnector_platform_oauth_redirect'
        '&code_challenge=xyz&code_challenge_method=S256&state=s%2B1&resource=https%3A%2F%2Fmcp-router.example.com%2Fmcp'
        '&scope=&scope=extra'
    )

    @pytest.mark.parametrize('stack', [s.key for s in DEFAULT_STACKS])
    def test_a_stack_from_the_table_skips_the_picker(self, stack: str) -> None:
        with _client() as client:
            response = client.get(f'/authorize?{self.QUERY}&stack={stack}&via=direct')

        assert response.status_code == 302
        location = urlparse(response.headers['location'])
        upstream = next(s.upstream for s in DEFAULT_STACKS if s.key == stack)
        assert f'{location.scheme}://{location.netloc}' == upstream
        assert location.path == '/authorize'
        # every original parameter, duplicates and blanks included, in order; `stack` dropped, one `via=router`
        assert parse_qsl(location.query, keep_blank_values=True) == [
            *parse_qsl(self.QUERY, keep_blank_values=True),
            ('via', 'router'),
        ]

    @pytest.mark.parametrize('stack', ['', 'evil', 'https://attacker.example', 'us-aws.', 'US-AWS'])
    def test_any_other_stack_shows_the_picker(self, stack: str) -> None:
        with _client() as client:
            response = client.get(f'/authorize?{self.QUERY}&stack={stack}')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('text/html')
        assert response.headers['content-security-policy'] == (
            "default-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'"
        )
        assert response.headers['referrer-policy'] == 'no-referrer'
        assert 'location' not in response.headers
        assert '<form' not in response.text

    @staticmethod
    def _links(page: str) -> dict[str, str]:
        """The picker's stack links: link text (the stack name) -> href, as a browser would read them."""
        from html.parser import HTMLParser

        class Links(HTMLParser):
            def __init__(self) -> None:
                super().__init__()
                self.links: dict[str, str] = {}
                self._href: str | None = None

            def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
                if tag == 'a':
                    self._href = dict(attrs).get('href') or ''

            def handle_data(self, data: str) -> None:
                if self._href is not None and data.strip():
                    self.links[data.strip()] = self._href
                    self._href = None

        parser = Links()
        parser.feed(page)
        return parser.links

    def test_each_stack_is_a_link_carrying_every_parameter(self) -> None:
        """Following a stack's link reaches that stack with every original parameter, in order, duplicates and blanks
        included."""
        with _client() as client:
            page = client.get(f'/authorize?{self.QUERY}&stack=no-such-stack')
            links = self._links(page.text)
            followed = client.get(links['EU Frankfurt GCP'])

        assert set(links) == {s.name for s in DEFAULT_STACKS}
        assert all(href.startswith('/authorize?') for href in links.values())
        assert followed.status_code == 302
        location = urlparse(followed.headers['location'])
        assert f'{location.scheme}://{location.netloc}' == 'https://mcp.europe-west3.gcp.keboola.com'
        assert parse_qsl(location.query, keep_blank_values=True) == [
            *parse_qsl(self.QUERY, keep_blank_values=True),
            ('via', 'router'),
        ]

    def test_choosing_a_stack_remembers_it_for_the_picker(self) -> None:
        with _client() as client:
            response = client.get(f'/authorize?{self.QUERY}&stack=eu-gcp')
            client.cookies.set('keboola_mcp_stack', 'eu-gcp', path='/authorize')
            picker = client.get(f'/authorize?{self.QUERY}')

        cookie = response.headers['set-cookie']
        assert cookie.startswith('keboola_mcp_stack=eu-gcp;')
        for attribute in ('Max-Age=7776000', 'Path=/authorize', 'Secure', 'HttpOnly', 'SameSite=lax'):
            assert attribute in cookie
        assert picker.text.count('class="stack last-used"') == 1
        assert '<a class="stack last-used" href="/authorize?' in picker.text
        assert 'stack=eu-gcp"' in picker.text.split('last-used')[1].split('>')[0]

    def test_a_post_is_not_served(self) -> None:
        with _client() as client:
            response = client.post(f'/authorize?{self.QUERY}', data={'stack': 'eu-gcp'})

        assert response.status_code == 405
        assert 'location' not in response.headers

    @pytest.mark.parametrize(
        ('name', 'value'),
        [
            ('state', '"><script>alert(1)</script>'),
            ('redirect_uri', "' onmouseover='alert(1)"),
            ('"><b>', '<b>&"d"'),
            # an encoded `&stack=` must stay inside its value, never become a parameter of its own
            ('state', 'x&stack=us-gcp'),
            ('stack', 'evil-stack'),
        ],
    )
    def test_the_picker_links_are_escaped_and_cannot_gain_a_parameter(self, name: str, value: str) -> None:
        page = render_picker(stacks=DEFAULT_STACKS, params=[(name, value)], preselected=None)
        links = self._links(page)

        assert '<script>' not in page
        assert '<b>' not in page
        assert value not in page
        for stack in DEFAULT_STACKS:
            query = parse_qsl(urlparse(links[stack.name]).query, keep_blank_values=True)
            # the original parameter unchanged (the request's own `stack` dropped), the link's stack last and alone
            assert query == ([] if name == 'stack' else [(name, value)]) + [('stack', stack.key)]
            assert stack.login_host in page

    def test_a_hostile_query_string_through_the_route(self) -> None:
        hostile = 'state=%22%3E%3Cscript%3Ealert(1)%3C%2Fscript%3E&redirect_uri=x%26stack%3Dus-gcp'
        with _client() as client:
            page = client.get(f'/authorize?{hostile}')
            followed = client.get(self._links(page.text)['EU Frankfurt AWS'])

        assert '<script>' not in page.text
        location = urlparse(followed.headers['location'])
        assert f'{location.scheme}://{location.netloc}' == 'https://mcp.eu-central-1.keboola.com'
        assert parse_qsl(location.query) == [
            ('state', '"><script>alert(1)</script>'),
            ('redirect_uri', 'x&stack=us-gcp'),
            ('via', 'router'),
        ]

    def test_the_picker_stylesheet_is_served_by_the_router_itself(self) -> None:
        with _client() as client:
            page = client.get('/authorize')
            stylesheet = client.get('/router-static/picker.css')

        assert 'href="/router-static/picker.css"' in page.text
        assert stylesheet.status_code == 200
        assert stylesheet.headers['content-type'].startswith('text/css')


class TestTokenAndRevoke:
    @pytest.mark.parametrize(
        ('form', 'upstream_host'),
        [
            ({'grant_type': 'authorization_code', 'code': 'eu-aws.code.jwt', 'code_verifier': 'v'}, 'eu-central-1'),
            ({'grant_type': 'refresh_token', 'refresh_token': 'us-gcp.opaque'}, 'us-east4.gcp'),
        ],
    )
    def test_token_is_forwarded_verbatim_to_the_stack_of_the_prefix(self, form: dict, upstream_host: str) -> None:
        upstream = _Upstream(httpx.Response(200, json={'access_token': 'x'}, headers={'Cache-Control': 'no-store'}))
        with _client(upstream) as client:
            response = client.post(
                '/token',
                data={**form, 'client_id': 'abc'},
                headers={
                    'Authorization': 'Basic YWJjOnNlY3JldA==',
                    'X-Forwarded-For': '203.0.113.9',
                    'Cookie': 'keboola_mcp_stack=us-aws',
                },
            )

        assert response.status_code == 200
        assert response.json() == {'access_token': 'x'}
        assert response.headers['cache-control'] == 'no-store'
        [sent] = upstream.requests
        assert str(sent.url) == f'https://mcp.{upstream_host}.keboola.com/token'
        assert parse_qs(sent.content.decode()) == {k: [v] for k, v in {**form, 'client_id': 'abc'}.items()}
        assert sent.headers['authorization'] == 'Basic YWJjOnNlY3JldA=='
        assert sent.headers['content-type'] == 'application/x-www-form-urlencoded'
        assert 'x-forwarded-for' not in sent.headers
        assert 'cookie' not in sent.headers

    def test_only_oauth_response_headers_come_back(self) -> None:
        answer = httpx.Response(
            400,
            json={'error': 'invalid_grant'},
            headers={'Set-Cookie': 'upstream=1', 'Location': 'https://elsewhere.example', 'WWW-Authenticate': 'Basic'},
        )
        with _client(_Upstream(answer)) as client:
            response = client.post('/token', data={'grant_type': 'refresh_token', 'refresh_token': 'us-aws.rt'})

        assert response.status_code == 400
        assert response.json() == {'error': 'invalid_grant'}
        assert response.headers['www-authenticate'] == 'Basic'
        assert 'set-cookie' not in response.headers
        assert 'location' not in response.headers

    @pytest.mark.parametrize(
        ('form', 'expected_status', 'expected_error'),
        [
            ({'grant_type': 'authorization_code', 'code': 'not-prefixed'}, 400, 'invalid_grant'),
            ({'grant_type': 'authorization_code', 'code': 'xx-unknown.code'}, 400, 'invalid_grant'),
            ({'grant_type': 'refresh_token'}, 400, 'invalid_request'),
            # RFC 6749 §3.1: routed by one value while the stack would read the other
            ({'grant_type': 'authorization_code', 'code': ['eu-aws.a', 'us-aws.b']}, 400, 'invalid_request'),
            ({'grant_type': 'client_credentials', 'code': 'eu-aws.code'}, 400, 'unsupported_grant_type'),
            ({'code': 'eu-aws.code'}, 400, 'unsupported_grant_type'),
        ],
    )
    def test_token_without_a_known_prefix_is_answered_without_an_upstream_call(
        self, form: dict, expected_status: int, expected_error: str
    ) -> None:
        upstream = _Upstream()
        with _client(upstream) as client:
            response = client.post('/token', data=form)

        assert response.status_code == expected_status
        assert response.json()['error'] == expected_error
        assert upstream.requests == []

    @pytest.mark.parametrize(
        ('token', 'expected_status', 'expected_upstream'),
        [
            ('eu-azure.opaque', 200, 'https://mcp.north-europe.azure.keboola.com/revoke'),
            # RFC 7009 §2.2: an unknown token is not an error
            ('unknown.opaque', 200, None),
            ('no-prefix', 200, None),
            # RFC 7009 §2.1: the token is required
            (None, 400, None),
        ],
    )
    def test_revoke_routes_by_prefix_and_answers_200_for_an_unknown_token(
        self, token: str | None, expected_status: int, expected_upstream: str | None
    ) -> None:
        upstream = _Upstream(httpx.Response(200))
        with _client(upstream) as client:
            response = client.post('/revoke', data={'token': token, 'client_id': 'abc'} if token else {})

        assert response.status_code == expected_status
        assert [str(r.url) for r in upstream.requests] == ([expected_upstream] if expected_upstream else [])

    @pytest.mark.parametrize(
        ('failure', 'expected_status'),
        [(httpx.ReadTimeout('slow'), 504), (httpx.ConnectError('down'), 502)],
    )
    def test_an_upstream_failure_is_a_gateway_error(self, failure: Exception, expected_status: int) -> None:
        with _client(_Upstream(failure)) as client:
            response = client.post('/token', data={'grant_type': 'refresh_token', 'refresh_token': 'us-aws.rt'})

        assert response.status_code == expected_status
        assert response.json()['error'] == 'temporarily_unavailable'

    def test_logs_carry_metadata_only(self, caplog: pytest.LogCaptureFixture) -> None:
        secrets = ['Basic c2VjcmV0LWNsaWVudDpzM2NyM3Q=', 'eu-aws.the-authorization-code', 'the-code-verifier']
        with caplog.at_level(logging.DEBUG), _client() as client:
            client.post(
                '/token',
                data={'grant_type': 'authorization_code', 'code': secrets[1], 'code_verifier': secrets[2]},
                headers={'Authorization': secrets[0]},
            )
            client.get('/authorize?state=the-client-state&code_challenge=the-challenge&stack=us-aws')

        # the test client's own request lines (to the router URL) are not the router's logs
        logged = '\n'.join(
            f'{r.getMessage()} {r.args}'
            for r in caplog.records
            if not (r.getMessage().startswith('HTTP Request:') and ROUTER_URL in r.getMessage())
        )
        for value in (*secrets, 'c2VjcmV0', 'the-client-state', 'the-challenge'):
            assert value not in logged
        assert '[router] POST /token status=200 stack_key=eu-aws' in logged
        assert '[router] GET /authorize status=302 stack_key=us-aws' in logged

    def test_a_path_the_router_does_not_serve_is_not_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO), _client() as client:
            client.get('/nothing%0a[router] GET /token status=200 stack_key=forged')

        [line] = [r.getMessage() for r in caplog.records if ' status=' in r.getMessage()]
        assert line.startswith('[router] GET <other> status=404 stack_key=- ')
        assert 'forged' not in line

    @pytest.mark.parametrize(
        'path', ['/authorize/', '/token/', '/register/', '/.well-known/oauth-authorization-server/']
    )
    def test_a_trailing_slash_never_redirects_to_the_request_host(self, path: str) -> None:
        with _client() as client:
            response = client.request(
                'POST' if path in ('/token/', '/register/') else 'GET',
                f'{path}?state=s',
                headers={'Host': 'evil.example'},
            )

        assert response.status_code == 404
        assert 'location' not in response.headers

    def test_bodies_are_capped(self) -> None:
        upstream = _Upstream()
        with _client(upstream) as client:
            register = client.post(
                '/register', json={'redirect_uris': ['https://a.example/cb'], 'client_name': 'x' * 70_000}
            )
            token = client.post(
                '/token', data={'grant_type': 'refresh_token', 'refresh_token': 'us-aws.' + 'x' * 70_000}
            )

        assert register.status_code == token.status_code == 413
        assert upstream.requests == []


class TestRateLimits:
    def test_authorize_is_limited_per_browser_address(self) -> None:
        with _client(authorize_rate_limit_per_ip=2) as client:
            statuses = [
                client.get('/authorize', headers={'X-Forwarded-For': '203.0.113.7'}).status_code for _ in range(3)
            ]
            other = client.get('/authorize', headers={'X-Forwarded-For': '203.0.113.8'}).status_code

        assert statuses == [200, 200, 429]
        assert other == 200

    def test_register_is_limited_per_address_only_when_configured(self) -> None:
        body = {'redirect_uris': ['https://chatgpt.com/connector_platform_oauth_redirect']}
        headers = {'X-Forwarded-For': '198.51.100.1'}
        with _client() as client:
            unlimited = {client.post('/register', json=body, headers=headers).status_code for _ in range(5)}
        with _client(api_rate_limit_per_ip=2) as client:
            limited = [client.post('/register', json=body, headers=headers).status_code for _ in range(3)]

        assert unlimited == {201}
        assert limited == [201, 201, 429]

    @pytest.mark.parametrize(
        ('path', 'form'),
        [
            ('/token', lambda credential: {'grant_type': 'refresh_token', 'refresh_token': credential}),
            ('/token', lambda credential: {'grant_type': 'authorization_code', 'code': credential}),
            ('/revoke', lambda credential: {'token': credential}),
        ],
    )
    def test_token_and_revoke_are_limited_per_credential_not_per_address(self, path: str, form) -> None:
        """Every ChatGPT user calls from OpenAI's few egress addresses: two credentials from one address are counted
        apart, and the limit holds for a credential whatever address it comes from."""
        upstream = _Upstream(httpx.Response(200, json={}))
        with _client(upstream, token_rate_limit_per_credential=2) as client:
            statuses = [
                client.post(path, data=form(credential), headers={'X-Forwarded-For': address}).status_code
                for credential, address in [
                    ('us-aws.first', '198.51.100.1'),
                    ('us-aws.first', '198.51.100.2'),
                    ('us-aws.first', '198.51.100.3'),
                    ('us-aws.second', '198.51.100.1'),
                ]
            ]

        assert statuses == [200, 200, 429, 200]
        assert len(upstream.requests) == 3

    def test_credential_limits_are_kept_apart(self) -> None:
        """One credential counts separately at /token and /revoke (a throttled refresh token can still be revoked);
        the optional per-address ceiling is for /register only; a value without a known stack is not counted."""
        upstream = _Upstream(httpx.Response(200, json={}))
        headers = {'X-Forwarded-For': '198.51.100.9'}
        refresh = {'grant_type': 'refresh_token', 'refresh_token': 'us-aws.rt'}
        with _client(upstream, token_rate_limit_per_credential=1, api_rate_limit_per_ip=1) as client:
            token = [client.post('/token', data=refresh, headers=headers).status_code for _ in range(2)]
            revoke = client.post('/revoke', data={'token': 'us-aws.rt'}, headers=headers).status_code
            unknown = [
                client.post('/token', data={**refresh, 'refresh_token': 'xx.rt'}, headers=headers).status_code
                for _ in range(3)
            ]

        assert token == [200, 429]
        assert revoke == 200
        assert unknown == [400, 400, 400]  # invalid_grant every time, never 429

    def test_no_credential_reaches_the_logs_or_the_limiter(self, caplog: pytest.LogCaptureFixture) -> None:
        from keboola_mcp_server.router.app import create_router_app as build

        credential = 'us-aws.the-refresh-token'
        app = build(
            RouterSettings(router_url=ROUTER_URL, stacks=parse_stacks(None), token_rate_limit_per_credential=1),
            upstream_transport=httpx.MockTransport(_Upstream()),
        )
        with caplog.at_level(logging.DEBUG), TestClient(app, base_url=ROUTER_URL) as client:
            for _ in range(3):
                client.post('/token', data={'grant_type': 'refresh_token', 'refresh_token': credential})

        logged = '\n'.join(r.getMessage() for r in caplog.records if not r.getMessage().startswith('HTTP Request:'))
        assert 'Rate limit reached: group=credential:/token, key=sha256:' in logged
        assert credential not in logged and 'the-refresh-token' not in logged

    def test_an_options_request_that_is_no_preflight_is_counted_and_never_handled(self) -> None:
        upstream = _Upstream()
        body = {'redirect_uris': ['https://a.example/cb']}
        headers = {'X-Forwarded-For': '198.51.100.2'}
        with _client(upstream, api_rate_limit_per_ip=1) as client:
            statuses = [
                client.request('OPTIONS', '/register', json=body, headers=headers).status_code for _ in range(2)
            ]
            preflight = client.options(
                '/register',
                headers={**headers, 'Origin': 'https://app.example', 'Access-Control-Request-Method': 'POST'},
            )

        assert statuses == [405, 429]
        assert preflight.status_code == 200  # a real preflight is answered by CORS and not counted
        assert preflight.headers['access-control-allow-origin'] == '*'

    def test_keys_beyond_the_cap_are_not_counted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(rate_limit, '_MAX_TRACKED_KEYS', 1)
        with _client(authorize_rate_limit_per_ip=1) as client:
            statuses = [
                client.get('/authorize', headers={'X-Forwarded-For': address}).status_code
                for address in ('203.0.113.1', '203.0.113.1', '203.0.113.2', '203.0.113.2')
            ]

        assert statuses == [200, 429, 200, 200]


class TestSettings:
    def test_from_env(self) -> None:
        stacks = [{'key': 'dev', 'name': 'Dev', 'upstream': 'https://mcp.dev.example', 'login_host': 'connection.dev'}]
        settings = RouterSettings.from_env(
            {
                'KBC_ROUTER_URL': ROUTER_URL,
                'KBC_ROUTER_STACKS': json.dumps(stacks),
                'OPENAI_APPS_CHALLENGE': 'abc',
                'KBC_ROUTER_API_RATE_LIMIT_PER_IP': '600',
                'KBC_ROUTER_TOKEN_RATE_LIMIT_PER_CREDENTIAL': '20',
                'KBC_OAUTH_TRUSTED_PROXY_HOPS': '2',
            }
        )

        assert settings.issuer == ROUTER_URL
        assert list(settings.stacks) == ['dev']
        assert settings.openai_apps_challenge == 'abc'
        assert settings.authorize_rate_limit_per_ip == 60
        assert settings.api_rate_limit_per_ip == 600
        assert settings.token_rate_limit_per_credential == 20
        assert RouterSettings.from_env({'KBC_ROUTER_URL': ROUTER_URL}).token_rate_limit_per_credential == 10
        assert settings.trusted_proxy_hops == 2
        assert list(RouterSettings.from_env({'KBC_ROUTER_URL': ROUTER_URL}).stacks) == [s.key for s in DEFAULT_STACKS]

    @pytest.mark.parametrize(
        'env',
        [
            {},
            {'KBC_ROUTER_URL': 'mcp-router.example.com'},
            {'KBC_ROUTER_URL': ROUTER_URL, 'KBC_ROUTER_API_RATE_LIMIT_PER_IP': '0'},
            {'KBC_ROUTER_URL': ROUTER_URL, 'KBC_ROUTER_STACKS': '[]'},
            {'KBC_ROUTER_URL': ROUTER_URL, 'KBC_ROUTER_STACKS': '[{"key": "eu-aws"}]'},
            {'KBC_ROUTER_URL': 'http://mcp-router.example.com'},
            {'KBC_ROUTER_URL': 'https://user@mcp-router.example.com'},
        ],
    )
    def test_from_env_refuses_an_invalid_configuration(self, env: dict[str, str]) -> None:
        with pytest.raises(ValueError):
            RouterSettings.from_env(env)

    @pytest.mark.parametrize(
        'entry',
        [
            {'key': 'eu.aws', 'name': 'x', 'upstream': 'https://mcp.example', 'login_host': 'x'},
            {'key': 'eu-aws', 'name': 'x', 'upstream': 'http://mcp.example', 'login_host': 'x'},
            {'key': 'eu-aws', 'name': 'x', 'upstream': 'https://mcp.example/mcp', 'login_host': 'x'},
            {'key': 'eu-aws', 'name': 'x', 'upstream': 'https://user@mcp.example', 'login_host': 'x'},
            {'key': 'eu-aws', 'name': 'x', 'upstream': "https://mcp.example;script-src 'none'", 'login_host': 'x'},
        ],
    )
    def test_the_stack_table_is_validated(self, entry: dict[str, str]) -> None:
        with pytest.raises(ValueError):
            parse_stacks(json.dumps([entry]))

    def test_duplicate_stack_keys_are_refused(self) -> None:
        entry = {'key': 'eu-aws', 'name': 'x', 'upstream': 'https://mcp.example', 'login_host': 'x'}
        with pytest.raises(ValueError):
            parse_stacks(json.dumps([entry, entry]))

    @pytest.mark.parametrize(('args', 'mode'), [([], 'server'), (['--mode', 'router'], 'router')])
    def test_cli_mode(self, args: list[str], mode: str) -> None:
        assert parse_args(args).mode == mode
