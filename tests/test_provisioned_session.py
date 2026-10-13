"""Credential lifecycle of a project provisioned by `create_project` on the deployed server --
`keboola_mcp_server.provisioned_session` (remote_agent_provisioning RFC).
"""

import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from keboola_mcp_server.auth_login import TokenSet
from keboola_mcp_server.config import Config
from keboola_mcp_server.provisioned_session import (
    ProvisionedSessionEnded,
    ProvisionedSessionRotationLost,
    handle_unauthorized,
    resolve_session,
)
from keboola_mcp_server.scope import SessionScope
from keboola_mcp_server.session_store.repository import LockedSession, OAuthSession, SessionStore


class TestProvisionedSessionOnADeployedServer:
    """A project created by `create_project` where there is no per-user credential file: the
    credentials live in the session store and the caller holds only the row's handle inside its
    `scope_token` (remote_agent_provisioning RFC).
    """

    STACK = 'https://connection.keboola.com'

    @staticmethod
    def _session(expires_in: float = 3600.0, **overrides):
        defaults = {
            'id': 'sess-row-1',
            'client_id': 'claude-code',
            'user_email': None,
            'kbc_access_token': 'kbc_at_provisioned',
            'kbc_refresh_token': 'kbc_rt_provisioned',
            'kbc_access_expires_at': datetime.now(timezone.utc) + timedelta(seconds=expires_in),
            'scope_project_ids': None,
            'scope_read_only': False,
            'scope_confirmed': False,
            'scope_scoped_token': None,
            'scope_scoped_expires_at': None,
            # Not meaningful for a provisioned session (it never went through OAuth); the row
            # carries the store's default.
            'oauth_projectless': True,
        }
        return OAuthSession(**{**defaults, **overrides})

    @staticmethod
    def _with_lock(store, session):
        """Gives a mock store a `lock_session` yielding a handle over `session`, as Postgres would.
        Returns the handle so a test can assert on the writes made through it."""
        locked = AsyncMock(spec=LockedSession)
        locked.session = session

        @asynccontextmanager
        async def _lock(_session_id):
            yield locked

        store.lock_session = _lock
        return locked

    @pytest.mark.asyncio
    async def test_the_handle_becomes_this_request_s_credentials(self) -> None:
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session()
        scope = SessionScope(project_ids=[4321], confirmed=True, provisioned_session_id='sess-row-1')

        config = await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        assert config.storage_token == 'kbc_at_provisioned'
        assert config.project_id == '4321'
        store.get_by_id.assert_awaited_once_with('sess-row-1')

    @pytest.mark.asyncio
    async def test_the_handle_wins_over_the_caller_s_own_credential(self) -> None:
        # A provisioned project belongs to the stack's agent maintainer, so the caller's own
        # session cannot reach it -- resolving the handle is the only way their call can work.
        # Replay by a *different* caller is stopped by scope_token's encryption and aad binding,
        # not by refusing to use the handle here.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session()
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')

        config = await resolve_session(Config(storage_api_url=self.STACK, storage_token='kbc_at_mine'), scope, store)

        assert config.storage_token == 'kbc_at_provisioned'
        assert config.project_id == '4321'

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'scope',
        [None, SessionScope(project_ids=[1]), SessionScope(project_ids=[1], scoped_token='kbc_at_x')],
        ids=['no_scope', 'ordinary_scope', 'scoped_token_scope'],
    )
    async def test_untouched_without_a_provisioned_handle(self, scope) -> None:
        store = AsyncMock(spec=SessionStore)
        original = Config(storage_api_url=self.STACK)

        config = await resolve_session(original, scope, store)

        assert config is original
        store.get_by_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_revoked_row_keeps_explaining_itself(self) -> None:
        # The human claimed the project; the row is gone. Falling back to whatever credential the
        # request carries would only produce a confusing failure against a project that credential
        # cannot reach -- on this call and on every later one.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = None
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')

        with pytest.raises(ProvisionedSessionEnded, match='has ended') as excinfo:
            await resolve_session(
                Config(storage_api_url=self.STACK, storage_token='kbc_at_mine'),
                scope,
                store,
            )

        assert '4321' in str(excinfo.value)
        assert 'sign in' in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_an_unreadable_row_is_treated_as_transient(self) -> None:
        # A Postgres blip is not a revocation: leave the request alone rather than telling the user
        # their project is gone.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.side_effect = RuntimeError('connection reset')
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')
        original = Config(storage_api_url=self.STACK, storage_token='kbc_at_mine')

        config = await resolve_session(original, scope, store)

        assert config is original

    @pytest.mark.asyncio
    async def test_a_near_expiry_token_is_refreshed_server_side_and_persisted(self) -> None:
        # The caller cannot refresh what it never holds, so the server must.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        locked = self._with_lock(store, self._session(expires_in=10))
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')
        refreshed = TokenSet(access_token='kbc_at_fresh', refresh_token='kbc_rt_fresh', expires_at=time.time() + 3600)

        with patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(return_value=refreshed)):
            config = await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        assert config.storage_token == 'kbc_at_fresh'
        locked.rotate_kbc_tokens.assert_awaited_once()
        assert locked.rotate_kbc_tokens.await_args.kwargs['kbc_access_token'] == 'kbc_at_fresh'
        assert locked.rotate_kbc_tokens.await_args.kwargs['kbc_refresh_token'] == 'kbc_rt_fresh'

    @pytest.mark.asyncio
    async def test_a_failed_refresh_keeps_the_current_token(self) -> None:
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        self._with_lock(store, self._session(expires_in=10))
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')

        with patch(
            'keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(side_effect=httpx.ConnectTimeout('down'))
        ):
            config = await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        assert config.storage_token == 'kbc_at_provisioned'
        store.rotate_kbc_tokens.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_loser_of_a_refresh_race_uses_the_winner_s_tokens(self) -> None:
        # Connection rotates refresh tokens: a second refresh with the same one would spend a token
        # the winner already invalidated, and its write-back would strand the session. Under the
        # lock the row already carries fresh credentials, so no second call is made at all.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        self._with_lock(store, self._session(expires_in=3600, kbc_access_token='kbc_at_winner'))
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')

        with patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock()) as refresh:
            config = await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        assert config.storage_token == 'kbc_at_winner'
        refresh.assert_not_awaited()
        store.rotate_kbc_tokens.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refused_refresh_token_ends_the_session(self) -> None:
        # Claiming the project revokes the whole Keboola session, so its refresh token dies with
        # it. Connection's 401 is final: revoke the row and say so, rather than handing back a
        # dead access token and letting the next call fail without explanation. Same convention
        # as SimpleOAuthProvider._refresh_kbc_tokens.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        locked = self._with_lock(store, self._session(expires_in=10))
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')
        refused = httpx.HTTPStatusError('401', request=MagicMock(), response=MagicMock(status_code=401))

        with (
            patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(side_effect=refused)),
            pytest.raises(ProvisionedSessionEnded, match='has ended'),
        ):
            await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        # Revoked on the locked connection -- the lock already makes this the only worker that
        # could have rotated the credentials, so no compare-and-swap is needed.
        locked.revoke.assert_awaited_once()
        locked.rotate_kbc_tokens.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_5xx_from_the_refresh_endpoint_keeps_the_session(self) -> None:
        # Not a refusal: Connection could not answer. Keep the token and let the 401 handling deal
        # with it if it is actually dead.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        locked = self._with_lock(store, self._session(expires_in=10))
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')
        failed = httpx.HTTPStatusError('503', request=MagicMock(), response=MagicMock(status_code=503))

        with patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(side_effect=failed)):
            config = await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        assert config.storage_token == 'kbc_at_provisioned'
        locked.revoke.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_session_revoked_while_waiting_for_the_lock_ends_the_session(self) -> None:
        # Someone claimed the project while this request queued on the lock. Refreshing a row that
        # is gone is impossible, and carrying on with the old token would only 401 -- say so.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        locked = self._with_lock(store, None)
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')

        with (
            patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock()) as refresh,
            pytest.raises(ProvisionedSessionEnded, match='has ended'),
        ):
            await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        refresh.assert_not_awaited()
        locked.rotate_kbc_tokens.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_rotation_whose_in_lock_write_fails_is_stored_outside_the_lock(self) -> None:
        # The refresh token is already spent by the time the write runs, so the new pair is the
        # only valid one left: dropping it (and returning the superseded access token) would leave
        # the stored pair dead, and the next refresh would 401 and be read as "the human claimed
        # the project" -- ending a session that is perfectly alive.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        locked = self._with_lock(store, self._session(expires_in=10))
        locked.rotate_kbc_tokens.side_effect = RuntimeError('connection reset')
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')
        refreshed = TokenSet(access_token='kbc_at_fresh', refresh_token='kbc_rt_fresh', expires_at=time.time() + 3600)

        with patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(return_value=refreshed)):
            config = await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        assert config.storage_token == 'kbc_at_fresh'
        store.rotate_kbc_tokens.assert_awaited_once()
        assert store.rotate_kbc_tokens.await_args.kwargs['kbc_refresh_token'] == 'kbc_rt_fresh'

    @pytest.mark.asyncio
    async def test_a_rotation_that_cannot_be_stored_at_all_is_raised_not_hidden(self) -> None:
        # Both writes failed, so Connection now expects credentials that exist nowhere. Returning
        # the superseded access token would look like a working request and hide that for good.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)
        locked = self._with_lock(store, self._session(expires_in=10))
        locked.rotate_kbc_tokens.side_effect = RuntimeError('connection reset')
        store.rotate_kbc_tokens.side_effect = RuntimeError('still down')
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')
        refreshed = TokenSet(access_token='kbc_at_fresh', refresh_token='kbc_rt_fresh', expires_at=time.time() + 3600)

        with (
            patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(return_value=refreshed)),
            pytest.raises(ProvisionedSessionRotationLost, match='Retry the call'),
        ):
            await resolve_session(Config(storage_api_url=self.STACK), scope, store)

    @pytest.mark.asyncio
    async def test_a_failure_before_the_refresh_spends_anything_is_still_transient(self) -> None:
        # The guard on the two tests above: only a failure *after* a successful refresh is fatal.
        # One taking the lock spends nothing, so the stored token (which may still have minutes
        # left) is handed back and the 401 handling deals with it if it is actually dead.
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = self._session(expires_in=10)

        @asynccontextmanager
        async def _lock(_session_id):
            raise RuntimeError('pool exhausted')
            yield  # pragma: no cover

        store.lock_session = _lock
        scope = SessionScope(project_ids=[4321], provisioned_session_id='sess-row-1')

        config = await resolve_session(Config(storage_api_url=self.STACK), scope, store)

        assert config.storage_token == 'kbc_at_provisioned'


class TestProvisionedSessionEndsWhenTheProjectIsClaimed:
    """What a 401 on a provisioned session means. The authority is the refresh token, not the
    access token: an access token 401s for reasons that leave the session alive (it expired after
    an idle hour, or an earlier refresh hit a blip and this request carried the stale token), and
    the row is the only handle to an unclaimed project.
    """

    STACK = 'https://connection.keboola.com'
    CONFIG = Config(storage_api_url=STACK, storage_token='kbc_at_provisioned')
    SCOPE = SessionScope(project_ids=[4321], confirmed=True, provisioned_session_id='sess-row-1')

    def _store(self, session, *, locked_session=...):
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.return_value = session
        locked = TestProvisionedSessionOnADeployedServer._with_lock(
            store, session if locked_session is ... else locked_session
        )
        return store, locked

    @pytest.mark.asyncio
    async def test_a_refused_refresh_ends_the_session(self) -> None:
        session = TestProvisionedSessionOnADeployedServer._session()
        store, locked = self._store(session)
        refused = httpx.HTTPStatusError('401', request=MagicMock(), response=MagicMock(status_code=401))

        with (
            patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(side_effect=refused)),
            pytest.raises(ProvisionedSessionEnded, match='has ended') as excinfo,
        ):
            await handle_unauthorized(self.CONFIG, self.SCOPE, store)

        locked.revoke.assert_awaited_once()
        message = str(excinfo.value)
        assert 'claimed' in message
        assert 'sign in' in message
        assert '4321' in message

    @pytest.mark.asyncio
    async def test_a_gone_row_ends_the_session(self) -> None:
        store, _ = self._store(None)

        with pytest.raises(ProvisionedSessionEnded, match='has ended'):
            await handle_unauthorized(self.CONFIG, self.SCOPE, store)

    @pytest.mark.asyncio
    async def test_a_merely_stale_access_token_keeps_the_session(self) -> None:
        # The refresh works, so the session is alive: the 401 came from an expired access token,
        # not a claim. Revoking here would destroy a live project'"'"'s only handle.
        session = TestProvisionedSessionOnADeployedServer._session()
        store, locked = self._store(session)
        refreshed = TokenSet(access_token='kbc_at_fresh', refresh_token='kbc_rt_fresh', expires_at=time.time() + 3600)

        with patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(return_value=refreshed)):
            await handle_unauthorized(self.CONFIG, self.SCOPE, store)

        locked.revoke.assert_not_awaited()
        locked.rotate_kbc_tokens.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'failure',
        [
            httpx.ConnectTimeout('down'),
            httpx.HTTPStatusError('503', request=MagicMock(), response=MagicMock(status_code=503)),
        ],
        ids=['transport_error', 'server_error'],
    )
    async def test_a_connection_blip_keeps_the_session(self, failure: Exception) -> None:
        # The regression this guards: a blip after an idle hour used to revoke the row and tell the
        # agent the project had been claimed, losing an unclaimed project for good.
        session = TestProvisionedSessionOnADeployedServer._session()
        store, locked = self._store(session)

        with patch('keboola_mcp_server.provisioned_session.refresh_tokens', AsyncMock(side_effect=failure)):
            await handle_unauthorized(self.CONFIG, self.SCOPE, store)

        locked.revoke.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unreadable_row_keeps_the_session(self) -> None:
        store = AsyncMock(spec=SessionStore)
        store.get_by_id.side_effect = RuntimeError('connection reset')

        await handle_unauthorized(self.CONFIG, self.SCOPE, store)
