"""The unauthenticated entry point: the one path a person with no Keboola account can take
(`keboola_mcp_server.anonymous_entry`, remote_agent_provisioning RFC).

Two things have to hold at once here, and they pull in opposite directions: a caller with no
credential must be able to create a project and then work in it, and a caller with no credential
must be able to reach nothing else -- not a token in the pod's environment, not a local login, not
somebody else's provisioned session.
"""

import base64
import contextlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from fastmcp import FastMCP
from fastmcp.client import Client, StreamableHttpTransport
from fastmcp.exceptions import ToolError
from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.requests import Request

from keboola_mcp_server import server as server_module
from keboola_mcp_server.anonymous_entry import (
    ANONYMOUS_ENTRY_SCOPE_KEY,
    ANONYMOUS_MOUNT_PATH,
    AnonymousCredentialLeak,
    AnonymousEntryMiddleware,
    carries_a_credential,
    credential_unchanged,
    is_anonymous_entry,
    only_provisioned_credential,
    strip_credentials,
)
from keboola_mcp_server.auth_login import ProvisionedProject, TokenSet
from keboola_mcp_server.cli import create_anonymous_app, create_http_app
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import SessionStateMiddleware, ToolsFilteringMiddleware
from keboola_mcp_server.scope import SessionScope
from keboola_mcp_server.server import create_server
from keboola_mcp_server.session_store.repository import OAuthSession

STACK = 'https://connection.test.keboola.com'
ENCRYPTION_KEY = base64.b64encode(b'0' * 32).decode()


def _request(headers: dict[str, str] | None = None, *, anonymous: bool) -> Request:
    scope: dict[str, Any] = {
        'type': 'http',
        'method': 'POST',
        'path': '/mcp/',
        'headers': [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
    }
    if anonymous:
        scope[ANONYMOUS_ENTRY_SCOPE_KEY] = True
    return Request(scope)


class TestWhatCountsAsACredential:
    """`carries_a_credential` asks `Config` which headers are credentials rather than listing them
    itself -- these are the spellings that must keep being caught when it does."""

    @pytest.mark.parametrize(
        'headers',
        [
            {'Authorization': 'Bearer kbc_at_whatever'},
            {'Authorization': 'Bearer something-else-entirely'},
            {'X-Storage-Api-Token': 'legacy-token'},
            {'X-Storage-Token': 'legacy-token'},
            {'Storage-Api-Token': 'legacy-token'},
            {'KBC-Storage-Token': 'legacy-token'},
            {'X-Bearer-Token': 'oauth-bearer'},
        ],
    )
    def test_a_credential_in_any_of_its_spellings_is_recognised(self, headers: dict[str, str]) -> None:
        assert carries_a_credential(Headers(headers))

    @pytest.mark.parametrize(
        'headers',
        [
            {},
            {'Content-Type': 'application/json'},
            # A header that merely picks a stack or a branch is not a credential.
            {'X-Storage-Api-Url': STACK, 'X-Branch-Id': '123'},
            # An empty Authorization is the client sending nothing, not sending a credential.
            {'Authorization': ''},
        ],
    )
    def test_an_ordinary_request_is_not_mistaken_for_one(self, headers: dict[str, str]) -> None:
        assert not carries_a_credential(Headers(headers))


class TestTheEndpointLendsNothing:
    """What the anonymous endpoint strips is not only what the caller sent -- that is refused at the
    door -- but everything the *server* would otherwise hand a session that asked for nothing."""

    def test_the_servers_own_token_does_not_become_the_anonymous_callers(self) -> None:
        # A KBC_STORAGE_TOKEN in the pod's environment is the operator's credential. Serving it to
        # an unauthenticated caller would hand a stranger the operator's projects.
        server_config = Config(storage_api_url=STACK, storage_token='the-operators-own-token')

        config = SessionStateMiddleware.apply_request_config(
            _request(anonymous=True), server_config, own_stack_storage_api_url=STACK
        )

        assert config.storage_token is None
        assert config.bearer_token is None
        # ...while the stack URL, which is not a credential and which `create_project` needs, stays.
        assert config.storage_api_url == STACK

    def test_the_same_request_on_the_authenticated_endpoint_keeps_it(self) -> None:
        # The stripping must be a property of the endpoint, not of having no headers: /mcp with a
        # server-configured token is an ordinary single-tenant deployment and keeps working.
        server_config = Config(storage_api_url=STACK, storage_token='the-operators-own-token')

        config = SessionStateMiddleware.apply_request_config(
            _request(anonymous=False), server_config, own_stack_storage_api_url=STACK
        )

        assert config.storage_token == 'the-operators-own-token'

    def test_a_project_pin_goes_too(self) -> None:
        # `project_id` picks which project a credential is narrowed to; with no credential it can
        # only ever be a leftover pointing somewhere this caller has no business.
        config = strip_credentials(Config(storage_api_url=STACK, project_id='1234'))
        assert config.project_id is None

    def test_is_anonymous_entry_is_false_for_an_ordinary_request(self) -> None:
        assert not is_anonymous_entry(_request(anonymous=False))
        assert not is_anonymous_entry(None)


class TestTheDoor:
    """`AnonymousEntryMiddleware` at the ASGI layer: it marks the request and turns away anyone
    holding a credential."""

    @staticmethod
    async def _call(headers: dict[str, str]) -> tuple[list[dict], list[dict]]:
        seen: list[dict] = []
        messages: list[dict] = []

        async def app(scope, _receive, send):
            seen.append(scope)
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'ok'})

        async def send(message):
            messages.append(message)

        scope = {
            'type': 'http',
            'method': 'POST',
            'path': '/',
            'headers': [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        }
        await AnonymousEntryMiddleware(app)(scope, None, send)
        return seen, messages

    @pytest.mark.asyncio
    async def test_an_ordinary_request_is_marked_and_passed_through(self) -> None:
        seen, messages = await self._call({})

        assert len(seen) == 1
        assert is_anonymous_entry(Request(seen[0]))
        assert messages[0]['status'] == 200

    @pytest.mark.asyncio
    async def test_a_credential_carrying_request_is_turned_away_rather_than_quietly_downgraded(self) -> None:
        # Ignoring the credential instead would let a client that believes it is authenticated
        # provision throwaway projects under an anonymous session and never find out.
        seen, messages = await self._call({'Authorization': 'Bearer kbc_at_mine'})

        assert seen == []
        assert messages[0]['status'] == 400
        body = messages[1]['body'].decode()
        assert 'credentials_not_accepted_here' in body
        assert '/mcp' in body
        # The refusal must not quote back what it refused.
        assert 'kbc_at_mine' not in body


class TestTheCredentialInvariant:
    """`only_provisioned_credential` / `credential_unchanged`: the two checks that bracket the one
    place an anonymous request is allowed to acquire a token."""

    def test_the_provisioned_credential_is_the_one_it_may_hold(self) -> None:
        config = Config(storage_api_url=STACK, storage_token='kbc_at_provisioned')
        assert only_provisioned_credential(config, 'sess-1') == 'kbc_at_provisioned'

    def test_no_credential_at_all_is_fine(self) -> None:
        assert only_provisioned_credential(Config(storage_api_url=STACK), None) is None

    def test_a_credential_without_a_handle_fails_the_request(self) -> None:
        config = Config(storage_api_url=STACK, storage_token='somebody-elses-token')
        with pytest.raises(AnonymousCredentialLeak, match='without holding a provisioned'):
            only_provisioned_credential(config, None)

    def test_swapping_the_credential_afterwards_fails_the_request(self) -> None:
        # What `_resolve_local_tokens` used to do on a non-deployed server: read the provisioned
        # `kbc_at_` token as an ordinary local session and refresh it from the host's own login.
        config = Config(storage_api_url=STACK, storage_token='the-operators-own-token')
        with pytest.raises(AnonymousCredentialLeak, match='replaced after'):
            credential_unchanged(config, 'kbc_at_provisioned')

    def test_keeping_it_passes(self) -> None:
        credential_unchanged(Config(storage_api_url=STACK, storage_token='kbc_at_p'), 'kbc_at_p')


class TestTheLocalLoginIsNeverBorrowed:
    """A provisioned `kbc_at_` token on a non-deployed server looks exactly like an ordinary local
    programmatic session, so `_resolve_local_tokens` would refresh it from the host's login store --
    handing the operator's identity to an unauthenticated caller on any dev machine with the flag
    on. The `anonymous` flag is what tells those two apart."""

    @pytest.mark.asyncio
    async def test_an_anonymous_session_does_not_reach_the_login_store(self, monkeypatch) -> None:
        get_access_token = AsyncMock(return_value='the-operators-own-token')
        monkeypatch.setattr('keboola_mcp_server.mcp.get_access_token', get_access_token)
        config = Config(storage_api_url=STACK, storage_token='kbc_at_provisioned')
        scope = SessionScope(project_ids=[4321], confirmed=True, provisioned_session_id='sess-1')

        resolved, _ = await SessionStateMiddleware._resolve_local_tokens(config, scope, anonymous=True)

        assert resolved.storage_token == 'kbc_at_provisioned'
        get_access_token.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_ordinary_local_session_still_does(self, monkeypatch) -> None:
        # The guard must be about *who is asking*, not about the token's shape -- a local session
        # with the same kind of token still gets its login refreshed.
        get_access_token = AsyncMock(return_value='kbc_at_refreshed')
        monkeypatch.setattr('keboola_mcp_server.mcp.get_access_token', get_access_token)
        config = Config(storage_api_url=STACK, storage_token='kbc_at_local')

        resolved, _ = await SessionStateMiddleware._resolve_local_tokens(config, None)

        assert resolved.storage_token == 'kbc_at_refreshed'
        get_access_token.assert_awaited_once()


class TestWhetherTheEndpointIsServedAtAll:
    @staticmethod
    def _server(**overrides) -> FastMCP:
        server = create_server(
            Config(storage_api_url=STACK, **overrides), runtime_info=ServerRuntimeInfo(transport='streamable-http')
        )
        assert isinstance(server, FastMCP)
        return server

    def test_off_unless_the_deployment_asks_for_it(self) -> None:
        # The default matters more than most: this is the one endpoint that answers without
        # credentials, so it has to be something an operator turned on deliberately.
        assert create_anonymous_app(self._server(), config=Config(), stateless_http=True) is None

    def test_not_served_when_there_is_no_authentication_to_be_anonymous_next_to(self, caplog) -> None:
        # Without OAuth, /mcp already takes anyone -- a second path that additionally refuses
        # credentials would only be a worse version of the endpoint they already have.
        app = create_anonymous_app(
            self._server(), config=Config(allow_anonymous_provisioning=True), stateless_http=True
        )

        assert app is None
        assert 'no authentication configured' in caplog.text

    def test_served_next_to_an_authenticated_endpoint(self) -> None:
        server = self._server(
            oauth_client_id='client-id',
            oauth_client_secret='client-secret',
            oauth_server_url=STACK,
            mcp_server_url='https://mcp.test.keboola.com',
            postgres_dsn='postgresql://u:p@host/db',
            session_encryption_key=ENCRYPTION_KEY,
        )

        app = create_anonymous_app(server, config=Config(allow_anonymous_provisioning=True), stateless_http=True)

        assert app is not None

    @pytest.mark.parametrize(
        ('env_value', 'expected'),
        [('true', True), ('1', True), ('yes', True), ('false', False), ('', False)],
    )
    def test_the_setting_is_read_from_the_environment(self, env_value: str, expected: bool) -> None:
        config = Config().replace_by({'KBC_ALLOW_ANONYMOUS_PROVISIONING': env_value})
        assert bool(config.allow_anonymous_provisioning) is expected

    def test_a_request_header_can_never_turn_it_on(self) -> None:
        # A caller choosing to be served by the path that accepts no credentials would be choosing
        # their own authorization. Every spelling `_read_options` accepts must be ignored.
        for name in (
            'allow_anonymous_provisioning',
            'KBC_allow_anonymous_provisioning',
            'X-allow-anonymous-provisioning',
        ):
            config = Config().replace_by_headers(Headers({name: 'true'}))
            assert config.allow_anonymous_provisioning is None, name


class _StoreForOneProvisionedSession:
    """Just enough `SessionStore` for the provisioning round trip, in memory."""

    def __init__(self) -> None:
        self.sessions: dict[str, OAuthSession] = {}

    async def create(
        self, *, client_id, user_email, kbc_access_token, kbc_refresh_token, kbc_access_expires_at, **_kwargs
    ):
        session = OAuthSession(
            id=f'sess-{len(self.sessions) + 1}',
            client_id=client_id,
            user_email=user_email,
            kbc_access_token=kbc_access_token,
            kbc_refresh_token=kbc_refresh_token,
            kbc_access_expires_at=kbc_access_expires_at,
            scope_project_ids=None,
            scope_read_only=False,
            scope_confirmed=False,
            scope_scoped_token=None,
            scope_scoped_expires_at=None,
            oauth_projectless=True,
        )
        self.sessions[session.id] = session
        return 'opaque-access', 'opaque-refresh', session

    async def get_by_id(self, session_id: str) -> OAuthSession | None:
        return self.sessions.get(session_id)

    async def get_by_access_token(self, access_token: str) -> OAuthSession | None:
        return None

    async def close(self) -> None:
        return None


class TestANewcomerWithNoAccount:
    """The whole point of the feature, over the real ASGI stack: both endpoints mounted as
    `run_server` mounts them, and a client that has nothing to authenticate with."""

    PROJECT_ID = 4321
    CONFIRM_URL = f'{STACK}/agent-project/confirm?token=kbc_apc_claim_secret'

    @pytest.fixture
    def store(self) -> _StoreForOneProvisionedSession:
        return _StoreForOneProvisionedSession()

    @pytest.fixture
    def app(self, store, monkeypatch) -> Starlette:
        monkeypatch.setattr(server_module, 'PostgresSessionStore', lambda *_a, **_kw: store)

        async def fake_provision(storage_api_url, *, client_id, project_name=None, backend=None, **_kw):
            return ProvisionedProject(
                project_id=self.PROJECT_ID,
                project_name=project_name or 'Agent project',
                backend='snowflake',
                confirm_url=self.CONFIRM_URL,
                tokens=TokenSet(
                    access_token='kbc_at_provisioned_secret',
                    refresh_token='kbc_rt_provisioned_secret',
                    expires_at=(datetime.now(timezone.utc) + timedelta(hours=1)).timestamp(),
                    project_ids=[self.PROJECT_ID],
                ),
                backend_init_dispatched_async=False,
            )

        monkeypatch.setattr('keboola_mcp_server.tools.project.provision_agent_project', fake_provision)

        config = Config(
            storage_api_url=STACK,
            oauth_client_id='client-id',
            oauth_client_secret='client-secret',
            oauth_server_url=STACK,
            mcp_server_url='https://mcp.test.keboola.com',
            postgres_dsn='postgresql://u:p@host/db',
            session_encryption_key=ENCRYPTION_KEY,
            jwt_secret=ENCRYPTION_KEY,
            allow_anonymous_provisioning=True,
        )
        mcp_server, app = create_http_app(config, stateless_http=True)
        assert isinstance(mcp_server, FastMCP)

        return app

    @staticmethod
    @asynccontextmanager
    async def _raw(app: Starlette):
        """An HTTP client over the mounted apps, with the lifespan running.

        `httpx.ASGITransport` does not run it, and without it the streamable-HTTP session manager
        has no task group to serve from. Entered here, inside the test's own task, because the
        lifespan owns anyio cancel scopes that cannot be exited from a different one -- which is
        what a fixture that yielded around the test would do.
        """
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://testserver') as http,
        ):
            yield http

    @classmethod
    @asynccontextmanager
    async def _connected(cls, app: Starlette, path: str = ANONYMOUS_MOUNT_PATH, **client_kwargs):
        def factory(**kwargs):
            return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://testserver', **kwargs)

        async with app.router.lifespan_context(app):
            transport = StreamableHttpTransport(
                url=f'http://testserver{path}/', httpx_client_factory=factory, **client_kwargs
            )
            async with Client(transport) as client:
                yield client

    @pytest.mark.asyncio
    async def test_the_authenticated_endpoint_still_refuses_an_unauthenticated_client(self, app: Starlette) -> None:
        # The reason this is a second URL at all: /mcp must keep answering an unauthenticated call
        # with a 401, because that refusal is how every MCP client discovers it should run OAuth.
        async with self._raw(app) as http:
            response = await http.post(
                '/mcp/',
                json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'},
                headers={'Accept': 'application/json, text/event-stream'},
            )

        assert response.status_code == 401
        assert 'WWW-Authenticate' in response.headers

    @pytest.mark.asyncio
    async def test_a_credential_is_refused_at_the_anonymous_endpoint(self, app: Starlette) -> None:
        async with self._raw(app) as http:
            response = await http.post(
                f'{ANONYMOUS_MOUNT_PATH}/',
                json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'},
                headers={'Authorization': 'Bearer kbc_at_mine'},
            )

        assert response.status_code == 400
        assert response.json()['error'] == 'credentials_not_accepted_here'

    @pytest.mark.asyncio
    async def test_a_newcomer_creates_a_project_and_the_handle_signs_them_in_to_it(
        self, app: Starlette, store: _StoreForOneProvisionedSession, monkeypatch
    ) -> None:
        captured: list[Config] = []
        original = SessionStateMiddleware.create_session_state.__func__

        async def capture(cls, config, *args, **kwargs):
            captured.append(config)
            return await original(cls, config, *args, **kwargs)

        monkeypatch.setattr(SessionStateMiddleware, 'create_session_state', classmethod(capture))

        async with self._connected(app) as client:
            # Nothing to authenticate with, and yet the endpoint answers.
            assert 'create_project' in {t.name for t in await client.list_tools()}

            result = await client.call_tool('create_project', {'name': 'My first project'})

        created = result.structured_content
        assert created['project_id'] == self.PROJECT_ID
        assert created['confirm_url'] == self.CONFIRM_URL
        # DMD-1939: the credentials stay on the server; only the opaque handle travels back.
        assert 'kbc_at_' not in str(result.content)
        assert 'kbc_rt_' not in str(result.content)
        scope_token = created['scope_token']
        assert scope_token

        # The session the project was created in had no credential of its own...
        assert captured
        assert all(c.storage_token is None for c in captured)
        # ...and the credentials it produced went to the store, not to the caller.
        [session] = store.sessions.values()
        assert session.kbc_access_token == 'kbc_at_provisioned_secret'
        assert scope_token != session.kbc_access_token

        # A later call carrying the handle is signed in to the new project. What is asserted is
        # the config the middleware built for that call, not the tool's own outcome -- this test
        # has no Keboola to talk to, so the call is cut short right after that config is built.
        monkeypatch.setattr(
            ToolsFilteringMiddleware,
            'get_token_info',
            AsyncMock(side_effect=RuntimeError('no Keboola in this test')),
        )
        # A host that is itself logged in: on a non-deployed server the provisioned `kbc_at_` token
        # reads as an ordinary local programmatic session, so this is what `_resolve_local_tokens`
        # would otherwise swap it for -- the operator's identity, handed to an anonymous caller.
        monkeypatch.setattr(
            'keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='the-operators-own-token')
        )
        captured.clear()
        async with self._connected(app) as client:
            with contextlib.suppress(Exception):
                await client.call_tool('get_project_info', {'scope_token': scope_token})

        signed_in = [c for c in captured if c.storage_token]
        assert signed_in, 'the handle did not resolve into the provisioned credential'
        assert signed_in[0].storage_token == 'kbc_at_provisioned_secret'
        assert signed_in[0].project_id == str(self.PROJECT_ID)
        assert all(c.storage_token != 'the-operators-own-token' for c in captured)

    @pytest.mark.asyncio
    async def test_a_claimed_project_says_so_instead_of_internal_server_error(
        self, app: Starlette, store: _StoreForOneProvisionedSession
    ) -> None:
        """Found on canary-orion. Claiming the project revokes the session, which is the normal end
        of its life and the one thing requirement 5 of the RFC asks to be said plainly. The session
        did end exactly as designed -- and the caller still saw "Internal server error", because
        `ProvisionedSessionEnded` raised from a middleware is not a tool result."""
        async with self._connected(app) as client:
            created = (await client.call_tool('create_project', {'name': 'claimed'})).structured_content
        scope_token = created['scope_token']

        # What the claim does, server-side: the row is gone.
        store.sessions.clear()

        async with self._connected(app) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool('get_project_info', {'scope_token': scope_token})

        message = str(excinfo.value)
        assert 'Internal server error' not in message
        assert 'has ended' in message
        assert 'claimed' in message
        assert 'sign in to this server with that account' in message
        assert str(created['project_id']) in message

    @pytest.mark.asyncio
    async def test_without_the_handle_a_later_call_gets_nothing(self, app: Starlette) -> None:
        """The handle is the only thing that signs an anonymous session in. A caller that never had
        one -- or a second caller who found the URL -- reaches a bootstrap session, which is told how
        to get a credential rather than handed one."""
        async with self._connected(app) as client:
            assert 'get_project_info' in {t.name for t in await client.list_tools()}

            with pytest.raises(ToolError) as excinfo:
                await client.call_tool('get_project_info')

        message = str(excinfo.value)
        assert 'no Keboola credentials' in message
        assert 'create_project' in message
