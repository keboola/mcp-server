"""In-conversation multi-project scope (PSGO-261 increment 2): the ``SessionScope`` model, its
``scope_token`` round-trip, and the associated session-state keys.

Split out of ``mcp.py`` so that module can stay focused on the middleware/server wiring itself
(``mcp.py``'s ``SessionStateMiddleware``/``MultiProjectMiddleware`` both depend on this).
"""

import base64
import dataclasses
import gzip
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Annotated

from pydantic import Field

from keboola_mcp_server.config import Config
from keboola_mcp_server.session_store.crypto import decrypt, encrypt, resolve_encryption_key

if TYPE_CHECKING:
    from keboola_mcp_server.session_store.repository import SessionStore

SCOPE_KEY = 'project_scope'

# Declared on every write/modify/delete tool; consumed by MultiProjectMiddleware.on_call_tool to
# pick which scoped project the call targets (see multiproject.py's write branch). Optional only
# when the scope resolves the target unambiguously (a single scoped project).
PROJECT_ID_ARG = 'project_id'
ProjectIdArg = Annotated[
    str | None,
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


def resolve_scope_key(config: Config) -> bytes:
    """The AES-256 key used to encrypt/decrypt ``scope_token`` -- the same
    ``KBC_SESSION_ENCRYPTION_KEY`` OAuth sessions already encrypt their stored credentials with
    (shared across replicas when configured, otherwise a process-local fallback -- see
    ``session_store.crypto.resolve_encryption_key``). ``scope_token`` may carry a live
    ``scoped_token`` bearer credential, so it needs the same at-rest protection OAuth sessions
    get, not just a signature -- see the "Security hardening" RFC increment.
    """
    return resolve_encryption_key(config.session_encryption_key)


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

    def to_token(self, key: bytes) -> str:
        """Encrypts this scope into the opaque ``scope_token`` a caller resends on later calls.

        AES-GCM (authenticated encryption), not a bare signature: this may carry a live
        ``scoped_token`` bearer credential, which must not be recoverable by anyone without the
        key -- unlike a JWS, whose payload is trivially base64+gunzip-recoverable regardless of
        whether the signature itself can be forged. See the "Security hardening" RFC increment.
        """
        plaintext = gzip.compress(json.dumps(dataclasses.asdict(self)).encode('utf-8'))
        return base64.urlsafe_b64encode(encrypt(plaintext, key)).decode('ascii').rstrip('=')

    @classmethod
    def from_token(cls, token: str, key: bytes) -> 'SessionScope':
        """Inverse of ``to_token``. Raises on a missing/invalid/tampered/wrong-key token --
        callers should treat any exception as "no scope" rather than fail the request.
        """
        padded = token + '=' * (-len(token) % 4)
        plaintext = decrypt(base64.urlsafe_b64decode(padded), key)
        data = json.loads(gzip.decompress(plaintext).decode('utf-8'))
        # Ignore any unknown keys rather than raising -- forward-compat if a future field is added
        # to SessionScope after this token was minted.
        known_fields = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known_fields})


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
