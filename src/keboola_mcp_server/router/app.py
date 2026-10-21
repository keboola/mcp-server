"""The router app (`--mode=router`): OAuth metadata, `/register`, the stack picker on `/authorize`, and `/token` and
`/revoke` routed by the stack key prefix. It holds no Keboola credentials, no database and no session state, and talks
only to the MCP servers in its stack table, never to Connection."""

import contextlib
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from urllib.parse import parse_qs, parse_qsl, urlencode

import httpx
from mcp.server.auth.handlers.register import RegistrationHandler
from mcp.server.auth.settings import ClientRegistrationOptions
from mcp.server.transport_security import RequestBodyLimitMiddleware
from mcp.shared.auth import OAuthClientInformationFull
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from starlette.routing import Route, request_response
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.router.picker import PICKER_CSS, PICKER_HEADERS, PICKER_STYLESHEET_PATH, render_picker
from keboola_mcp_server.router.rate_limit import PerAddressRateLimitMiddleware
from keboola_mcp_server.router.settings import RouterSettings
from keboola_mcp_server.router.stacks import Stack, router_resource, stack_key_of

LOG = logging.getLogger(__name__)

_STACK_COOKIE = 'keboola_mcp_stack'
_STACK_COOKIE_MAX_AGE_SECONDS = 90 * 24 * 60 * 60
# A registration, token or revocation request is a few hundred bytes; nothing bigger is read into memory, let alone
# forwarded.
_MAX_BODY_BYTES = 64 * 1024
# What travels upstream with the verbatim body: its type, and client authentication (client_secret_basic). Nothing
# that identifies the caller: the regional server sees the router's address, so a forwarded `X-Forwarded-For` would only
# let a caller pick the address the regional server counts.
_FORWARDED_REQUEST_HEADERS = ('content-type', 'accept', 'authorization')
_FORWARDED_RESPONSE_HEADERS = ('content-type', 'cache-control', 'pragma', 'www-authenticate')
# The parameter of each grant that carries the stack key prefix.
_GRANT_TOKEN_PARAMETER = {'authorization_code': 'code', 'refresh_token': 'refresh_token'}
_PUBLIC_AND_SECRET_CLIENT_AUTH = ['none', 'client_secret_post', 'client_secret_basic']


class _RequestLogMiddleware:
    """One log line per request with metadata only: method, route, status, stack key, latency. Never the query string
    (`/authorize` carries `state` and the PKCE challenge), a header or a body, and never a path the router does not
    serve: the path is the caller's text, so it could forge log lines."""

    def __init__(self, app: ASGIApp, *, known_paths: frozenset[str]) -> None:
        self._app = app
        self._known_paths = known_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self._app(scope, receive, send)
            return
        state = scope.setdefault('state', {})
        started = time.perf_counter()
        status = 500

        async def send_and_record(message: dict) -> None:
            nonlocal status
            if message['type'] == 'http.response.start':
                status = message['status']
            await send(message)

        try:
            await self._app(scope, receive, send_and_record)
        finally:
            path = scope['path'] if scope['path'] in self._known_paths else '<other>'
            method = scope['method'] if scope['method'] in ('GET', 'POST', 'OPTIONS', 'HEAD') else '<other>'
            LOG.info(
                f'[router] {method} {path} status={status} stack_key={state.get("stack_key") or "-"} '
                f'duration_ms={(time.perf_counter() - started) * 1000:.0f}'
            )


class _StatelessClients:
    """`/register` with the regional server's semantics: a fresh client id and nothing persisted. Trust never comes
    from a registration but from the Connection client row the regional server looks up by `redirect_uri`, and the
    regional server accepts any client id (its `get_client` builds the client from the id alone)."""

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        return None


def _endpoint(
    handler: Callable[[Request], Awaitable[Response]], *, methods: list[str], cross_origin: bool
) -> tuple[ASGIApp, list[str]]:
    """The ASGI app and the methods of a route: the body capped at `_MAX_BODY_BYTES` (413 above it), and with
    `cross_origin`, CORS for browser-based MCP clients, which read the metadata and call `/register` and `/token`
    from another origin (as the SDK does on the regional server; never on `/authorize`, which is navigated to)."""
    served = [*methods, 'HEAD'] if 'GET' in methods else methods

    async def only_its_methods(request: Request) -> Response:
        # The route also accepts OPTIONS for CORS, and CORS answers only a real preflight itself: any other OPTIONS
        # would reach the handler, which would treat it as the POST it serves.
        if request.method not in served:
            return Response(status_code=405, headers={'Allow': ', '.join(served)})
        return await handler(request)

    app: ASGIApp = RequestBodyLimitMiddleware(request_response(only_its_methods), _MAX_BODY_BYTES)
    if not cross_origin:
        return app, methods
    allowed = [*methods, 'OPTIONS']
    return CORSMiddleware(
        app, allow_origins=['*'], allow_methods=allowed, allow_headers=['Mcp-Protocol-Version']
    ), allowed


def _form_fields(body: bytes) -> dict[str, list[str]]:
    return parse_qs(body.decode('utf-8', 'replace'), keep_blank_values=True)


def _single(fields: dict[str, list[str]], name: str) -> str | None:
    """The one value of a parameter, or None when it is absent; raises ValueError when it is repeated (RFC 6749 §3.1:
    a parameter must not be sent twice, and the router must not route by one value while the stack reads another)."""
    values = fields.get(name)
    if not values:
        return None
    if len(values) > 1:
        raise ValueError(name)
    return values[0]


def _oauth_error(error: str, description: str, status_code: int = 400) -> JSONResponse:
    return JSONResponse(
        {'error': error, 'error_description': description},
        status_code=status_code,
        headers={'Cache-Control': 'no-store', 'Pragma': 'no-cache'},
    )


def create_router_app(
    settings: RouterSettings,
    *,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
    rate_limit_clock: Callable[[], float] = time.monotonic,
) -> Starlette:
    """Builds the router app.

    :param upstream_transport: The transport of the client that calls the regional servers; tests pass a mock.
    :param rate_limit_clock: The clock of the rate-limit windows; tests pass a fixed one.
    """
    issuer = settings.issuer
    stacks = settings.stacks
    upstream = httpx.AsyncClient(
        transport=upstream_transport,
        timeout=httpx.Timeout(settings.upstream_timeout_seconds),
        follow_redirects=False,
    )

    async def protected_resource_metadata(_request: Request) -> Response:
        return JSONResponse(
            {
                'resource': router_resource(settings.router_url),
                'authorization_servers': [issuer],
                'bearer_methods_supported': ['header'],
            }
        )

    async def authorization_server_metadata(_request: Request) -> Response:
        return JSONResponse(
            {
                'issuer': issuer,
                'authorization_endpoint': f'{issuer}/authorize',
                'token_endpoint': f'{issuer}/token',
                'registration_endpoint': f'{issuer}/register',
                'revocation_endpoint': f'{issuer}/revoke',
                'response_types_supported': ['code'],
                'grant_types_supported': ['authorization_code', 'refresh_token'],
                'token_endpoint_auth_methods_supported': _PUBLIC_AND_SECRET_CLIENT_AUTH,
                'revocation_endpoint_auth_methods_supported': _PUBLIC_AND_SECRET_CLIENT_AUTH,
                'code_challenge_methods_supported': ['S256'],
                # The regional server that completes the login returns this issuer as `iss` (one logical
                # authorization server, see `router_issuer`).
                'authorization_response_iss_parameter_supported': True,
            }
        )

    def upstream_authorize_url(stack: Stack, query_string: str) -> str:
        """The chosen stack's `/authorize` with every original parameter except `stack` (and a `via` of the caller's
        own), plus `via=router`. The host only ever comes from the stack table."""
        params = [(k, v) for k, v in parse_qsl(query_string, keep_blank_values=True) if k not in ('stack', 'via')]
        return f'{stack.upstream}/authorize?{urlencode([*params, ("via", "router")])}'

    def picker(request: Request, status_code: int = 200) -> Response:
        cookie = request.cookies.get(_STACK_COOKIE)
        return HTMLResponse(
            render_picker(
                stacks=stacks.values(),
                query_string=request.url.query,
                preselected=cookie if cookie in stacks else None,
            ),
            status_code=status_code,
            headers=PICKER_HEADERS,
        )

    async def authorize(request: Request) -> Response:
        if request.method in ('GET', 'HEAD'):
            # A reviewer link (or any link) can name the stack and skip the picker; any other value shows the picker.
            if (stack := stacks.get(request.query_params.get('stack') or '')) is None:
                return picker(request)
            request.state.stack_key = stack.key
            return RedirectResponse(upstream_authorize_url(stack, request.url.query), status_code=302)

        # The picker's own submit: the authorization request stays in the query string (the form action) and the body
        # carries only the choice. An authorization request sent as a POST body is not supported here.
        chosen = (_form_fields(await request.body()).get('stack') or [''])[-1]
        if (stack := stacks.get(chosen)) is None:
            return picker(request, status_code=400)
        request.state.stack_key = stack.key
        response = RedirectResponse(upstream_authorize_url(stack, request.url.query), status_code=302)
        # Only a preselection for next time: it authenticates nobody, and a refresh never needs it (the token prefix
        # routes it).
        response.set_cookie(
            _STACK_COOKIE,
            stack.key,
            max_age=_STACK_COOKIE_MAX_AGE_SECONDS,
            path='/authorize',
            secure=True,
            httponly=True,
            samesite='lax',
        )
        return response

    async def forward(request: Request, stack: Stack, path: str, body: bytes) -> Response:
        request.state.stack_key = stack.key
        headers = {name: value for name in _FORWARDED_REQUEST_HEADERS if (value := request.headers.get(name))}
        try:
            answer = await upstream.post(f'{stack.upstream}{path}', content=body, headers=headers)
        except httpx.TimeoutException:
            LOG.warning(f'[router] Upstream timed out: stack_key={stack.key}, path={path}')
            return _oauth_error('temporarily_unavailable', 'The Keboola stack did not answer in time.', 504)
        except httpx.HTTPError as e:
            LOG.warning(f'[router] Upstream unreachable: stack_key={stack.key}, path={path}: {type(e).__name__}')
            return _oauth_error('temporarily_unavailable', 'The Keboola stack could not be reached.', 502)
        return Response(
            answer.content,
            status_code=answer.status_code,
            headers={name: value for name in _FORWARDED_RESPONSE_HEADERS if (value := answer.headers.get(name))},
        )

    async def token(request: Request) -> Response:
        body = await request.body()
        fields = _form_fields(body)
        try:
            grant_type = _single(fields, 'grant_type')
            if (parameter := _GRANT_TOKEN_PARAMETER.get(grant_type or '')) is None:
                return _oauth_error(
                    'unsupported_grant_type', 'Only authorization_code and refresh_token are supported.'
                )
            value = _single(fields, parameter)
        except ValueError as e:
            return _oauth_error('invalid_request', f'The parameter {e} is repeated.')
        if value is None:
            return _oauth_error('invalid_request', f'The {parameter} parameter is missing.')
        if (stack := stacks.get(stack_key_of(value) or '')) is None:
            # Answered here, without asking any stack: no stack issued a grant without a known prefix.
            return _oauth_error('invalid_grant', f'The {parameter} was not issued by this authorization server.')
        return await forward(request, stack, '/token', body)

    async def revoke(request: Request) -> Response:
        body = await request.body()
        try:
            value = _single(_form_fields(body), 'token')
        except ValueError:
            return _oauth_error('invalid_request', 'The parameter token is repeated.')
        if value is None:
            return _oauth_error('invalid_request', 'The token parameter is missing.')  # RFC 7009 §2.1
        if (stack := stacks.get(stack_key_of(value) or '')) is None:
            # RFC 7009 §2.2: an unknown token is answered with 200, there is nothing to revoke.
            return Response(status_code=200)
        return await forward(request, stack, '/revoke', body)

    async def openai_apps_challenge(_request: Request) -> Response:
        if not settings.openai_apps_challenge:
            return PlainTextResponse('Not Found', status_code=404)
        return PlainTextResponse(settings.openai_apps_challenge)

    async def picker_stylesheet(_request: Request) -> Response:
        return Response(PICKER_CSS, media_type='text/css', headers={'Cache-Control': 'public, max-age=3600'})

    async def health_check(_request: Request) -> Response:
        return JSONResponse({'status': 'ok'})

    registration = RegistrationHandler(
        provider=_StatelessClients(),  # type: ignore[arg-type]  # only register_client() is ever called
        options=ClientRegistrationOptions(enabled=True),
    )

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await upstream.aclose()

    limits: dict[str, int] = {}
    if settings.authorize_rate_limit_per_ip:
        limits['/authorize'] = settings.authorize_rate_limit_per_ip
    if settings.api_rate_limit_per_ip:
        limits.update(dict.fromkeys(('/register', '/token', '/revoke', '/mcp'), settings.api_rate_limit_per_ip))
    else:
        LOG.warning(
            '[router] No per-address limit on /register, /token, /revoke and /mcp: set KBC_ROUTER_API_RATE_LIMIT_PER_IP, '
            'sized for the AI clients\' shared egress addresses.'
        )

    endpoints: list[tuple[str, Callable[[Request], Awaitable[Response]], list[str], bool]] = [
        ('/.well-known/oauth-protected-resource', protected_resource_metadata, ['GET'], True),
        ('/.well-known/oauth-protected-resource/mcp', protected_resource_metadata, ['GET'], True),
        ('/.well-known/oauth-authorization-server', authorization_server_metadata, ['GET'], True),
        ('/.well-known/openid-configuration', authorization_server_metadata, ['GET'], True),
        ('/.well-known/openai-apps-challenge', openai_apps_challenge, ['GET'], False),
        ('/register', registration.handle, ['POST'], True),
        ('/authorize', authorize, ['GET', 'POST'], False),
        ('/token', token, ['POST'], True),
        ('/revoke', revoke, ['POST'], True),
        (PICKER_STYLESHEET_PATH, picker_stylesheet, ['GET'], False),
        ('/health-check', health_check, ['GET'], False),
        ('/healthz', health_check, ['GET'], False),
    ]
    routes = []
    for path, handler, methods, cross_origin in endpoints:
        app, allowed = _endpoint(handler, methods=methods, cross_origin=cross_origin)
        routes.append(Route(path, app, methods=allowed))

    router_app = Starlette(
        routes=routes,
        middleware=[
            Middleware(_RequestLogMiddleware, known_paths=frozenset(path for path, *_ in endpoints)),
            Middleware(
                PerAddressRateLimitMiddleware,
                limits=limits,
                trusted_hops=settings.trusted_proxy_hops,
                clock=rate_limit_clock,
            ),
        ],
        lifespan=lifespan,
    )
    # Starlette answers `/token/` with a 307 to a Location built from the Host header, which would send a client's code
    # or refresh token to whatever host the request named (the regional server guards the same case with
    # `UntrustedAuthorizeRedirectMiddleware`). The router serves no slash variants, so it never redirects there.
    router_app.router.redirect_slashes = False
    return router_app
