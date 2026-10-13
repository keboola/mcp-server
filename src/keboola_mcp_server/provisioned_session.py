"""Credential lifecycle of a project provisioned by ``create_project`` on the deployed server
(remote_agent_provisioning RFC).

Such a session is the deployed server's equivalent of the local credential file: the Keboola
access/refresh tokens live in the session store, encrypted at rest, and the caller holds only the
row's handle inside its ``scope_token``. Everything that follows from that -- resolving the handle
into a usable token, refreshing it under a cross-replica lock, and deciding what a 401 means -- is
here rather than in ``mcp.py``: none of it touches middleware state, it is all a function of
``(config, scope, store)``.
"""

import dataclasses
import logging
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import TYPE_CHECKING, cast

import httpx

from keboola_mcp_server.auth_login import REFRESH_SKEW_SECONDS, TokenSet, refresh_tokens
from keboola_mcp_server.config import Config
from keboola_mcp_server.session_store.repository import OAuthSession, SessionStore

if TYPE_CHECKING:
    from keboola_mcp_server.scope import SessionScope

LOG = logging.getLogger(__name__)


class ProvisionedSessionEnded(Exception):
    """A session created by `create_project` is over: Connection refused its refresh token, or the
    row is gone. Its own type, not a `ValueError`, because the refresh path has to tell this apart
    from the ordinary `ValueError`s raised inside it -- `json.JSONDecodeError` from a proxy's
    non-JSON 200, `float()` in `parse_token_response`, `datetime.fromtimestamp` -- which are
    transient and must not end a live session."""


class ProvisionedSessionRotationLost(Exception):
    """A refresh succeeded but neither write of the new pair did, so the credentials Connection
    now expects exist nowhere. Raised instead of returning the superseded access token, which would
    look like a working request while the stored refresh token is already spent -- the next refresh
    would then 401 and be read as "the human claimed the project", ending a session that is in fact
    alive."""


def session_ended(project_id: int | None) -> ProvisionedSessionEnded:
    """The one explanation a caller gets once their provisioned session is gone.

    Shared by both places that discover it -- the 401 that first reveals the revocation, and every
    later request whose handle no longer resolves -- so the answer does not degrade into an
    unexplained failure after the first one.
    """
    return ProvisionedSessionEnded(
        f'The temporary session that created project {project_id} has ended. This normally means the '
        'project has been claimed and now belongs to a real Keboola account -- stop resending the '
        'scope_token from "create_project" and sign in to this server with that account to keep '
        'working in the project.'
    )


def _needs_refresh(session: OAuthSession) -> bool:
    return session.kbc_access_expires_at <= datetime.now(timezone.utc) + timedelta(seconds=REFRESH_SKEW_SECONDS)


async def resolve_session(config: Config, scope: 'SessionScope | None', store: SessionStore | None) -> Config:
    """Turns a provisioned session's handle (``SessionScope.provisioned_session_id``) into the
    access token for this request, refreshing it server-side when it nears expiry.

    This is the deployed server's equivalent of the local credential file: the Keboola tokens never
    leave the server, so the caller cannot refresh them and must not be asked to.

    The handle takes precedence over whatever credential the request otherwise carries. That is not
    a weakening: a provisioned project belongs to the stack's agent maintainer, so a caller's own
    session cannot reach it at all, and resolving the handle is the only way the call they asked for
    can work. Replay by someone else is prevented where it has to be -- `scope_token` is encrypted
    and bound to the session it was minted for (`resolve_provisioned_binding_aad`), so a different
    caller cannot even decrypt it.

    A *gone* row ends the request with the "this session has ended" explanation rather than letting
    it fall back to the caller's own credential: the scope still points at a project that credential
    cannot reach, so the fallback can only produce a confusing failure -- and after the claim has
    revoked the row, it would produce one on every single later call. A row that merely cannot be
    *read* right now (a Postgres blip) is different: that is transient, so the config is left
    untouched and the request takes its chances.
    """
    if store is None or scope is None or scope.provisioned_session_id is None:
        return config
    try:
        session = await store.get_by_id(scope.provisioned_session_id)
    except Exception as e:
        LOG.warning(f'Could not load the provisioned session: {e}', exc_info=True)
        return config
    if session is None:
        # Revoked (the human claimed the project) or expired out of the store.
        LOG.info('The provisioned session referenced by this scope_token no longer exists.')
        raise session_ended(scope.active_project_id)

    access_token = session.kbc_access_token
    if _needs_refresh(session):
        access_token = await refresh_session(config, session, store, scope.active_project_id)
    project_id = str(scope.active_project_id) if scope.active_project_id is not None else config.project_id
    return dataclasses.replace(config, storage_token=access_token, project_id=project_id)


@dataclasses.dataclass(frozen=True)
class _RefreshOutcome:
    """What happened inside the lock, reported back instead of raised, so the ``async with`` is
    left normally and the transaction it owns commits (see `_refresh_locked`)."""

    access_token: str | None = None
    ended: bool = False


class _RotationWriteFailed(Exception):
    """Carries a successful refresh back out of the lock when persisting it failed, so the caller
    can retry the write on a fresh connection. Raised (not returned) on purpose: the failed
    statement has already poisoned the surrounding transaction, so it has to unwind rather than
    try to commit."""

    def __init__(self, refreshed: TokenSet) -> None:
        super().__init__('Could not persist the refreshed credentials of a provisioned session.')
        self.refreshed = refreshed


async def _refresh_locked(
    config: Config, session: OAuthSession, store: SessionStore, *, force: bool
) -> _RefreshOutcome:
    """The part of a refresh that must hold the session lock.

    Returns rather than raises for the two ordinary endings (the row is gone, Connection refused the
    refresh token) because `revoke` writes on this very connection: raising here would unwind
    through `conn.transaction().__aexit__`, roll the UPDATE back, and leave a session that logs
    "revoked" but keeps answering `get_by_id` -- every later request then spending another refresh
    call on a dead token.
    """
    async with store.lock_session(session.id) as locked:
        current = locked.session
        if current is None:
            return _RefreshOutcome(ended=True)  # revoked while we waited
        if not force and not _needs_refresh(current):
            return _RefreshOutcome(access_token=current.kbc_access_token)  # another worker got there first
        try:
            refreshed = await refresh_tokens(cast(str, config.storage_api_url), refresh_token=current.kbc_refresh_token)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != HTTPStatus.UNAUTHORIZED:
                raise
            await locked.revoke()
            LOG.info(
                'Connection refused the provisioned session\'s refresh token (401); '
                f'session revoked: session_id={current.id}'
            )
            return _RefreshOutcome(ended=True)
        try:
            await locked.rotate_kbc_tokens(
                kbc_access_token=refreshed.access_token,
                kbc_refresh_token=refreshed.refresh_token,
                kbc_access_expires_at=datetime.fromtimestamp(refreshed.expires_at, tz=timezone.utc),
            )
        except Exception as e:
            raise _RotationWriteFailed(refreshed) from e
        return _RefreshOutcome(access_token=refreshed.access_token)


async def refresh_session(
    config: Config,
    session: OAuthSession,
    store: SessionStore,
    project_id: int | None,
    *,
    force: bool = False,
) -> str:
    """Exchanges the session's refresh token for a fresh pair and persists the rotation.

    Serialized across every worker and replica by a lock on the session row, because Connection
    rotates the refresh token: two requests refreshing the same session concurrently would have one
    of them spend a token the other has already invalidated, and a late write-back could persist
    that dead pair and strand the session for good. The lock re-reads the row, so the loser of the
    race finds the winner's fresh credentials and makes no second call at all -- the same
    double-checked pattern `get_access_token` uses for the local credential file.

    A refresh Connection *refuses* (401) is final, and for a provisioned session it has one likely
    cause: the human claimed the project, which revokes the whole Keboola session. The row is
    revoked inside the lock and the caller gets the same "this session has ended" explanation as
    every other way of discovering it -- the convention `SimpleOAuthProvider._refresh_kbc_tokens`
    established for OAuth sessions.

    A failure *before* the refresh call spends anything (the lock, the row read, a transport error
    reaching Connection) is transient: the stored token is returned, it may still have minutes left,
    and the 401 handling deals with it properly if it does not. A failure *after* it has succeeded
    is not transient at all -- the stored refresh token is spent and only the new pair is valid --
    so the write is retried on a fresh connection and, failing that, raised rather than hidden
    behind the superseded token.

    ``force`` refreshes even when the stored token still looks fresh -- used by the 401 handler,
    where whether the refresh token is still accepted *is* the question being asked.
    """
    try:
        outcome = await _refresh_locked(config, session, store, force=force)
    except _RotationWriteFailed as e:
        return await _persist_outside_lock(store, session, e.refreshed, project_id)
    except ProvisionedSessionEnded:
        raise  # the session is over -- say so rather than handing back a dead token
    except Exception as e:
        LOG.warning(f'Could not refresh the provisioned session: {e}', exc_info=True)
        return session.kbc_access_token
    if outcome.ended:
        raise session_ended(project_id)
    return cast(str, outcome.access_token)


async def _persist_outside_lock(
    store: SessionStore, session: OAuthSession, refreshed: TokenSet, project_id: int | None
) -> str:
    """Second attempt at storing a rotation whose in-lock write failed.

    Safe to do without the lock, and the lock is gone by now anyway: the refresh token this pair
    replaces is already spent, so no concurrent refresh can produce a competing newer pair -- any
    other worker reaching Connection with it gets a 401, not a second rotation.
    """
    try:
        await store.rotate_kbc_tokens(
            session.id,
            kbc_access_token=refreshed.access_token,
            kbc_refresh_token=refreshed.refresh_token,
            kbc_access_expires_at=datetime.fromtimestamp(refreshed.expires_at, tz=timezone.utc),
        )
    except Exception as e:
        LOG.exception(
            'Refreshed the provisioned session but could not store the new credentials; the session can no '
            f'longer be refreshed: session_id={session.id}'
        )
        raise ProvisionedSessionRotationLost(
            'Could not store the refreshed credentials of the temporary session that created project '
            f'{project_id}. Retry the call; if it keeps failing, the session cannot be recovered and the '
            'project has to be claimed through its confirm_url.'
        ) from e
    LOG.info(f'Stored the refreshed credentials outside the lock after a failed in-lock write: {session.id}')
    return refreshed.access_token


async def handle_unauthorized(config: Config, scope: 'SessionScope', store: SessionStore) -> None:
    """Decides whether a 401 on a provisioned session means the session is over.

    The expected cause is the happy one: the human opened the confirm link and claimed the project,
    which revokes the agent session that created it. Then the caller should be told that, not handed
    a bare 401.

    The authority is the **refresh token**, not the access token. An access token can 401 for
    reasons that leave the session perfectly alive -- most obviously it simply expired after an idle
    hour, or an earlier refresh hit a Connection blip and this request carried the stale token on
    purpose. Revoking on that evidence would destroy the only handle to an unclaimed project over a
    transient failure. So this re-reads the row and tries a refresh: Connection refusing it is proof
    (and revokes, inside the lock), a refresh that succeeds says the session was merely stale and the
    original error stands, and anything else is transient.
    """
    try:
        session = await store.get_by_id(cast(str, scope.provisioned_session_id))
    except Exception as e:
        LOG.warning(f'Could not load the provisioned session while handling a 401: {e}', exc_info=True)
        return
    if session is None:
        raise session_ended(scope.active_project_id)

    refreshed = await refresh_session(config, session, store, scope.active_project_id, force=True)
    if refreshed != session.kbc_access_token:
        LOG.info(
            'The provisioned session was refreshed while handling a 401; the credential is still valid: '
            f'session_id={session.id}'
        )
