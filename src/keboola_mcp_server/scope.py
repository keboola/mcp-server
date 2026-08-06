"""In-conversation multi-project scope (PSGO-261 increment 2): the ``SessionScope`` model, its
``scope_token`` JWT round-trip, and the associated session-state keys.

Split out of ``mcp.py`` so that module can stay focused on the middleware/server wiring itself
(``mcp.py``'s ``SessionStateMiddleware``/``MultiProjectMiddleware`` both depend on this).
"""

import dataclasses
import secrets
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Annotated, Optional

from pydantic import Field

from keboola_mcp_server.config import Config
from keboola_mcp_server.jwt_utils import decode_jwt, encode_jwt

if TYPE_CHECKING:
    from keboola_mcp_server.session_store.repository import SessionStore

SCOPE_KEY = 'project_scope'

# Declared on every write/modify/delete tool; consumed by MultiProjectMiddleware.on_call_tool to
# pick which scoped project the call targets (see multiproject.py's write branch). Optional only
# when the scope resolves the target unambiguously (a single scoped project).
PROJECT_ID_ARG = 'project_id'
ProjectIdArg = Annotated[
    Optional[str],
    Field(
        description=(
            'Target Keboola project id for this write. Required when the session is scoped to 2+ '
            'projects; optional (defaults to the single scoped project) otherwise.'
        )
    ),
]

# The OAuth session's DB row id (see session_store.repository.OAuthSession), stashed on
# ctx.session.state so set_project_scope can persist a newly-confirmed scope back to Postgres
# instead of only returning a scope_token. Absent for non-OAuth (PAT/header-token) sessions, which
# have no session row to persist against -- those keep relying on scope_token.
OAUTH_SESSION_ID_KEY = 'oauth_session_id'

# Per-call argument that carries the confirmed multi-project scope forward (consumed and stripped
# by SessionStateMiddleware.on_request). See SessionScope.to_token/from_token: under the server's
# default stateless-HTTP transport a fresh, empty session is built for every request (the mcp
# 2026-07-28 RC formalizes this across the spec, dropping Mcp-Session-Id/session pinning entirely),
# so nothing survives in ctx.session.state between one tool call and the next -- on one replica or
# many, even within a single process. A scope set via "set_project_scope" only persists if the
# caller resends the token it returned.
SCOPE_TOKEN_ARG = 'scope_token'

# Process-local fallback signing key for scope_token, used when no shared KBC_JWT_SECRET is
# configured (e.g. local stdio/login sessions). A per-process secret is enough there since a stdio
# process serves exactly one conversation end-to-end; deployed multi-replica setups already require
# a shared jwt_secret for the OAuth-provider JWTs (see oauth.py), which this reuses.
_FALLBACK_SCOPE_SECRET = secrets.token_hex(32)


def resolve_scope_secret(config: Config) -> str:
    """The HMAC key used to sign/verify ``scope_token`` -- shared across replicas when
    ``config.jwt_secret`` (``KBC_JWT_SECRET``) is configured, otherwise a process-local fallback."""
    return config.jwt_secret or _FALLBACK_SCOPE_SECRET


@dataclasses.dataclass(frozen=True)
class SessionScope:
    """In-conversation multi-project scope (PSGO-261 increment 2).

    Persisted on the session across the per-request state rebuild. ``project_ids`` is the
    user-selected set; ``scoped_token`` is the child access token minted by /v1/auth/pat/exchange
    and narrowed to those projects (re-minted from the parent when near expiry).
    """

    project_ids: list[int]
    read_only: bool = False
    scoped_token: str | None = None
    scoped_expires_at: float | None = None
    confirmed: bool = False
    """True once the user has explicitly chosen a scope via ``set_project_scope``. The default
    auto-leased scope is unconfirmed, which gates data tools until the user decides."""

    @property
    def active_project_id(self) -> int | None:
        return self.project_ids[0] if self.project_ids else None

    @property
    def is_near_expiry(self) -> bool:
        if self.scoped_expires_at is None:
            return False
        return time.time() >= (self.scoped_expires_at - 60)

    def to_token(self, secret: str) -> str:
        """Signs this scope into the opaque ``scope_token`` a caller resends on later calls."""
        return encode_jwt(dataclasses.asdict(self), secret)

    @classmethod
    def from_token(cls, token: str, secret: str) -> 'SessionScope':
        """Inverse of ``to_token``. Raises on a missing/invalid/tampered token -- callers should
        treat any exception as "no scope" rather than fail the request."""
        return cls(**decode_jwt(token, secret))


async def persist_scope(session_store: 'SessionStore', session_id: str, scope: SessionScope) -> None:
    """Writes ``scope`` onto the OAuth session row ``session_id`` -- shared by ``set_project_scope``
    (a fresh confirmation) and ``SessionStateMiddleware.on_request`` (a near-expiry re-mint), so both
    persist a refreshed ``scoped_token`` the same way."""
    await session_store.update_scope(
        session_id,
        project_ids=scope.project_ids,
        read_only=scope.read_only,
        confirmed=scope.confirmed,
        scoped_token=scope.scoped_token,
        scoped_expires_at=(
            datetime.fromtimestamp(scope.scoped_expires_at, tz=timezone.utc)
            if scope.scoped_expires_at is not None
            else None
        ),
    )
