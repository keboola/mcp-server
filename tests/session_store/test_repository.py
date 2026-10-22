from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from tests.session_store.conftest import requires_postgres

pytestmark = [pytest.mark.asyncio, requires_postgres]


async def test_create_and_get_by_access_token(store) -> None:
    expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
    access_token, refresh_token, session = await store.create(
        client_id='claude.ai',
        user_email='m@k.com',
        kbc_access_token='kbc_at_secret',
        kbc_refresh_token='kbc_rt_secret',
        kbc_access_expires_at=expires_at,
    )

    assert access_token
    assert refresh_token
    assert access_token != refresh_token
    assert session.client_id == 'claude.ai'
    assert session.kbc_access_token == 'kbc_at_secret'
    assert session.kbc_refresh_token == 'kbc_rt_secret'
    assert session.scope_confirmed is False
    assert session.scope_project_ids is None

    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.id == session.id
    assert fetched.kbc_access_token == 'kbc_at_secret'


async def test_get_by_access_token_unknown_returns_none(store) -> None:
    assert await store.get_by_access_token('does-not-exist') is None


async def test_create_defaults_oauth_projectless_to_true(store) -> None:
    """Pre-AI-2883 callers (and every row created before this column existed) get the historical
    whole-stack grant -- see the 0005 migration's backfill default."""
    _, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    assert session.oauth_projectless is True


async def test_create_persists_oauth_projectless_false_for_flow_b(store) -> None:
    access_token, _, session = await store.create(
        client_id='some-dynamic-tool',
        user_email=None,
        kbc_access_token='kbc_at_y',
        kbc_refresh_token='kbc_rt_y',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        oauth_projectless=False,
    )
    assert session.oauth_projectless is False

    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.oauth_projectless is False


@pytest.mark.parametrize('resource', [None, 'https://mcp.keboola.com/mcp'])
async def test_create_persists_the_resource(store, resource: str | None) -> None:
    """DMD-2469: the RFC 8707 resource the tokens were issued for; None for a client that named none."""
    access_token, refresh_token, session = await store.create(
        client_id='chatgpt',
        user_email=None,
        kbc_access_token='kbc_at_r',
        kbc_refresh_token='kbc_rt_r',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        resource=resource,
    )
    assert session.resource == resource
    assert (await store.get_by_access_token(access_token)).resource == resource
    assert (await store.get_by_refresh_token(refresh_token)).resource == resource


async def test_get_by_refresh_token(store) -> None:
    _, refresh_token, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    fetched = await store.get_by_refresh_token(refresh_token)
    assert fetched is not None
    assert fetched.id == session.id


async def test_rotate_kbc_tokens_replaces_credentials(store) -> None:
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_old',
        kbc_refresh_token='kbc_rt_old',
        kbc_access_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    new_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    await store.rotate_kbc_tokens(
        session.id, kbc_access_token='kbc_at_new', kbc_refresh_token='kbc_rt_new', kbc_access_expires_at=new_expiry
    )

    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.kbc_access_token == 'kbc_at_new'
    assert fetched.kbc_refresh_token == 'kbc_rt_new'


async def test_update_scope_confirms_project_selection(store) -> None:
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    scoped_expiry = datetime.now(timezone.utc) + timedelta(minutes=30)
    await store.update_scope(
        session.id,
        project_ids=[18, 83],
        read_only=True,
        confirmed=True,
        scoped_token='kbc_pat_scoped',
        scoped_expires_at=scoped_expiry,
    )

    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.scope_project_ids == [18, 83]
    assert fetched.scope_read_only is True
    assert fetched.scope_confirmed is True
    assert fetched.scope_scoped_token == 'kbc_pat_scoped'


async def test_update_scope_without_scoped_token(store) -> None:
    # The whole-stack fallback path (resolver exchange unavailable) confirms scope with no minted
    # token -- must not choke on a None scoped_token.
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    await store.update_scope(
        session.id, project_ids=[18], read_only=False, confirmed=True, scoped_token=None, scoped_expires_at=None
    )
    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.scope_scoped_token is None


async def test_revoke_makes_session_unreachable(store) -> None:
    access_token, refresh_token, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    await store.revoke(session.id)

    assert await store.get_by_access_token(access_token) is None
    assert await store.get_by_refresh_token(refresh_token) is None


async def test_revoke_if_kbc_refresh_token_only_revokes_the_session_holding_that_token(store) -> None:
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    await store.rotate_kbc_tokens(
        session.id,
        kbc_access_token='kbc_at_y',
        kbc_refresh_token='kbc_rt_y',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    # A caller still holding the pre-rotation token must not end the rotated session.
    assert await store.revoke_if_kbc_refresh_token(session.id, 'kbc_rt_x') is False
    assert await store.get_by_access_token(access_token) is not None

    assert await store.revoke_if_kbc_refresh_token(session.id, 'kbc_rt_y') is True
    assert await store.get_by_access_token(access_token) is None
    # Already revoked, and unknown, sessions report False.
    assert await store.revoke_if_kbc_refresh_token(session.id, 'kbc_rt_y') is False
    assert await store.revoke_if_kbc_refresh_token('00000000-0000-0000-0000-000000000000', 'kbc_rt_y') is False


async def test_credentials_are_encrypted_at_rest(store) -> None:
    # Read the raw row directly -- the plaintext secret must never appear in storage.
    _, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_should_not_appear_in_plaintext',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    raw = await store._pool.fetchrow('SELECT kbc_access_token_enc FROM oauth_sessions WHERE id = $1', session.id)
    assert b'kbc_at_should_not_appear_in_plaintext' not in raw['kbc_access_token_enc']


async def test_a_revoke_inside_the_lock_is_committed(store) -> None:
    """The regression: `_refresh_locked` used to raise out of the `async with` right after calling
    `LockedSession.revoke`, so the UPDATE unwound through `conn.transaction().__aexit__` and rolled
    back -- the session logged "revoked" and then kept answering `get_by_id` forever, spending
    another refresh call on a dead token every request. Only a real transaction shows this; the
    mocked `lock_session` in `tests/test_provisioned_session.py` has none.
    """
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    async with store.lock_session(session.id) as locked:
        assert locked.session is not None
        await locked.revoke()

    assert await store.get_by_id(session.id) is None
    assert await store.get_by_access_token(access_token) is None


async def test_a_rotation_inside_the_lock_is_committed(store) -> None:
    """The same commit guarantee for the write the happy path makes -- losing it would spend the
    refresh token for nothing."""
    _, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_old',
        kbc_refresh_token='kbc_rt_old',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    expires_at = datetime.now(timezone.utc) + timedelta(hours=2)

    async with store.lock_session(session.id) as locked:
        await locked.rotate_kbc_tokens(
            kbc_access_token='kbc_at_new', kbc_refresh_token='kbc_rt_new', kbc_access_expires_at=expires_at
        )

    stored = await store.get_by_id(session.id)
    assert stored is not None
    assert stored.kbc_access_token == 'kbc_at_new'
    assert stored.kbc_refresh_token == 'kbc_rt_new'


async def test_a_failed_write_inside_the_lock_leaves_the_row_untouched(store) -> None:
    """Why `_RotationWriteFailed` has to unwind rather than return: the failed statement poisons
    the surrounding transaction, so nothing written in it can be committed afterwards and the
    rotation has to be retried on a fresh connection."""
    _, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_old',
        kbc_refresh_token='kbc_rt_old',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    with pytest.raises(asyncpg.PostgresError):
        async with store.lock_session(session.id) as locked:
            await locked.rotate_kbc_tokens(
                kbc_access_token='kbc_at_new',
                kbc_refresh_token='kbc_rt_new',
                kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            await locked._conn.execute('SELECT * FROM a_table_that_does_not_exist')

    stored = await store.get_by_id(session.id)
    assert stored is not None
    assert stored.kbc_access_token == 'kbc_at_old'

    # ... and the retry on a fresh connection, which is what `_persist_outside_lock` does, works.
    await store.rotate_kbc_tokens(
        session.id,
        kbc_access_token='kbc_at_new',
        kbc_refresh_token='kbc_rt_new',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
    )
    stored = await store.get_by_id(session.id)
    assert stored is not None
    assert stored.kbc_access_token == 'kbc_at_new'
