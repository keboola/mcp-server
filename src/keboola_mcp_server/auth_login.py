"""Local browser PKCE login for the MCP server (PSGO-261, Part B).

Lets a user authenticate the locally-run (stdio) MCP server with only the stack URL:
a browser PKCE flow leases a whole-stack session (access + refresh token), which is
stored to a mode-600 file and refreshed during usage. The leased ``kbc_at_*`` access
token is then forwarded downstream as the bearer credential.

The interactive browser/loopback orchestration lives in ``perform_login``; the HTTP
calls (``exchange_code``, ``refresh_tokens``) are split out so they can be tested with
an injected httpx transport.
"""

import asyncio
import base64
import contextlib
import dataclasses
import hashlib
import json
import logging
import os
import secrets
import sys
import time
import urllib.parse
import webbrowser
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import ClassVar, cast
from urllib.parse import urlparse

import httpx

from keboola_mcp_server.clients.base import normalize_storage_api_url

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX; the cross-process lock degrades to a no-op
    fcntl = None  # type: ignore[assignment]

LOG = logging.getLogger(__name__)

DEFAULT_CLIENT_ID = 'keboola-cli-demo'
_AUTHORIZE_PATH = 'admin/auth/pkce/authorize'
_TOKEN_PATH = 'v1/auth/pkce/token'
_REFRESH_PATH = 'v1/auth/token/refresh'
_INTROSPECT_PATH = 'v1/auth/token/introspect'
_EXCHANGE_PATH = 'v1/auth/pat/exchange'
_SUDO_PATH = 'v1/auth/sudo'
_PAT_PATH = 'v1/auth/pat'
_REFRESH_SKEW_SECONDS = 60
# Max time to wait for the browser to hit the loopback /callback. Generous enough for SSO/MFA, but
# bounded so a closed tab or blocked browser can't hang `login` (and stdio auto-login) forever.
_LOGIN_CALLBACK_TIMEOUT_SECONDS = 300
_PAT_DEFAULT_EXPIRES_SECONDS = 30 * 24 * 60 * 60  # ~1 month
_CREDENTIALS_PATH = Path.home() / '.keboola' / 'mcp' / 'credentials.json'
# Names which local interface (Claude Desktop, Cursor, a terminal `login`) a stored session
# belongs to, so two interfaces logged in to the *same* stack never share one entry (and its
# rotating refresh token) -- see the "Security hardening" RFC increment. Each interface's MCP
# client config sets this to a distinct value; a single-interface setup needs nothing set.
_PROFILE_ENV_VAR = 'KBC_LOGIN_PROFILE'
_DEFAULT_PROFILE = 'default'
# Cross-process insurance only (see `_credentials_lock`); the actual fix for the credential race
# is per-profile keying above. Non-blocking poll, never a blocking flock -- this runs inside
# `get_access_token`, which must never stall the event loop / MCP handshake.
_LOCK_POLL_INTERVAL_SECONDS = 0.05
_LOCK_TIMEOUT_SECONDS = 10.0
# One asyncio.Lock per (hostname, profile), serializing concurrent refreshes *within this
# process* -- the flock below only ever guards the on-disk file, not in-memory races.
_refresh_locks: dict[str, asyncio.Lock] = {}


def _resolve_profile(profile: str | None) -> str:
    return profile or os.environ.get(_PROFILE_ENV_VAR) or _DEFAULT_PROFILE


# Short connect timeout so an unreachable stack (e.g. VPN off — internal `.dev` stacks resolve to a
# private 10.x IP) fails in a few seconds with a clear ConnectTimeout instead of blocking the full
# window. A longer read timeout still tolerates a slow-but-reachable Connection.
_AUTH_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)


def _client_id() -> str:
    # Configurable so the real MCP client id can replace the demo value via a secret.
    return os.environ.get('KBC_PKCE_CLIENT_ID') or DEFAULT_CLIENT_ID


def _base_url(storage_api_url: str) -> str:
    return normalize_storage_api_url(storage_api_url)


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode('ascii').rstrip('=')


@dataclass(frozen=True)
class TokenSet:
    """A leased session: the access token plus what's needed to refresh it.

    ``project_ids``/``read_only`` are the scope chosen at `login` time (None only for a
    credential predating this choice, or one never run through `login`'s prompt/flags) -- see
    the "Security hardening" RFC increment: a local session is scoped before it's ever usable,
    rather than auto-leased to everything with an unenforceable ask-first gate.
    """

    access_token: str
    refresh_token: str
    expires_at: float  # epoch seconds
    session_id: str | None = None
    project_ids: list[int] | None = None
    read_only: bool = False

    @property
    def is_near_expiry(self) -> bool:
        return time.time() >= (self.expires_at - _REFRESH_SKEW_SECONDS)


def parse_token_response(body: dict, *, now: float | None = None) -> TokenSet:
    now = time.time() if now is None else now
    return TokenSet(
        access_token=cast(str, body['accessToken']),
        refresh_token=cast(str, body['refreshToken']),
        expires_at=now + float(body.get('expiresIn') or 0),
        session_id=cast('str | None', body.get('sessionId')),
    )


@dataclass(frozen=True)
class ProjectAccess:
    """A project the introspected token can reach."""

    id: int
    name: str | None = None
    role: str | None = None


@dataclass(frozen=True)
class Introspection:
    """Identity + the set of projects a programmatic token can reach (token introspection)."""

    user_id: int | None
    user_email: str | None
    user_name: str | None
    projects: list[ProjectAccess]


@dataclass(frozen=True)
class ScopedToken:
    """A child access token minted by /v1/auth/pat/exchange, narrowed to a set of projects."""

    access_token: str
    expires_at: float  # epoch seconds
    project_ids: list[int]
    read_only: bool

    @property
    def is_near_expiry(self) -> bool:
        return time.time() >= (self.expires_at - _REFRESH_SKEW_SECONDS)


async def introspect_token(
    storage_api_url: str,
    *,
    subject_token: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> Introspection:
    """Enumerates the projects a programmatic token can reach via /v1/auth/token/introspect."""
    async with httpx.AsyncClient(timeout=_AUTH_TIMEOUT, transport=transport) as client:
        response = await client.get(
            f'{_base_url(storage_api_url)}/{_INTROSPECT_PATH}',
            headers={'Authorization': f'Bearer {subject_token}'},
        )
        response.raise_for_status()
        body = cast(dict, response.json())
    user = body.get('user') or {}
    projects = [
        ProjectAccess(id=int(p['id']), name=p.get('name'), role=p.get('role'))
        for p in body.get('projects', [])
        if p.get('id') is not None
    ]
    return Introspection(
        user_id=user.get('id'),
        user_email=user.get('email'),
        user_name=user.get('name'),
        projects=projects,
    )


async def exchange_scoped_token(
    storage_api_url: str,
    *,
    subject_token: str,
    project_ids: list[int],
    read_only: bool = False,
    expires_in: int | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> ScopedToken:
    """
    Mints a child access token scoped to ``project_ids`` via /v1/auth/pat/exchange.

    The child token has no refresh token of its own; it is re-minted from the (refreshable)
    parent token when it nears expiry. ``read_only=True`` mints a read-only token.
    """
    # The exchange API expects project ids as strings (and rejects integers with a 400).
    payload = {
        'expiresIn': expires_in,
        'scope': {'projects': [str(p) for p in project_ids], 'readOnly': read_only or None},
    }
    async with httpx.AsyncClient(timeout=_AUTH_TIMEOUT, transport=transport) as client:
        response = await client.post(
            f'{_base_url(storage_api_url)}/{_EXCHANGE_PATH}',
            headers={'Authorization': f'Bearer {subject_token}'},
            json=payload,
        )
        response.raise_for_status()
        body = cast(dict, response.json())
    return ScopedToken(
        access_token=cast(str, body['accessToken']),
        expires_at=time.time() + float(body.get('expiresIn') or 0),
        project_ids=list(project_ids),
        read_only=bool(body.get('readOnly')),
    )


async def elevate_session(
    storage_api_url: str,
    *,
    subject_token: str,
    totp_code: str | None = None,
    recovery_code: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """Elevates (``sudo``) the session with an MFA code via POST /v1/auth/sudo.

    ``totp_code`` and ``recovery_code`` are mutually exclusive — pass exactly one. The ``type``
    field is intentionally omitted (empty). Returns the elevated bearer token to authorize
    sensitive operations such as PAT creation; if the endpoint elevates the session in place and
    returns no token, falls back to the original ``subject_token``.

    NOTE: response field name assumed (``token``/``accessToken``) — confirm against the auth API.
    """
    if bool(totp_code) == bool(recovery_code):
        raise ValueError('Provide exactly one of totp_code or recovery_code.')
    payload = {'totpCode': totp_code} if totp_code else {'recoveryCode': recovery_code}
    async with httpx.AsyncClient(timeout=_AUTH_TIMEOUT, transport=transport) as client:
        response = await client.post(
            f'{_base_url(storage_api_url)}/{_SUDO_PATH}',
            headers={'Authorization': f'Bearer {subject_token}'},
            json=payload,
        )
        if response.is_error:
            # The response body may echo back request details; never surface it directly to the
            # caller (it can end up in a CLI transcript/bug report) -- full detail goes to debug
            # logs only.
            LOG.debug(f'POST /{_SUDO_PATH} failed ({response.status_code}): {response.text}')
            raise RuntimeError(f'POST /{_SUDO_PATH} failed ({response.status_code}). See debug logs for details.')
        body = cast(dict, response.json()) if response.content else {}
    return cast(str, body.get('token') or body.get('accessToken') or subject_token)


async def create_pat(
    storage_api_url: str,
    *,
    subject_token: str,
    project_ids: list[int],
    name: str,
    expires_in: int = _PAT_DEFAULT_EXPIRES_SECONDS,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """Creates a Personal Access Token (``kbc_pat_*``) via POST /v1/auth/pat.

    ``subject_token`` must be an elevated (sudo) bearer. ``project_ids`` are sent as strings (the
    auth service rejects integers, per the exchange endpoint). Requires a prior ``elevate_session``.

    Projects are nested under ``scope`` (mirroring /v1/auth/pat/exchange); a top-level ``projects``
    field is rejected by the API. Response token field assumed (``token``/``pat``/``accessToken``).
    """
    payload = {
        'name': name,
        'expiresIn': expires_in,
        'scope': {'projects': [str(p) for p in project_ids]},
    }
    async with httpx.AsyncClient(timeout=_AUTH_TIMEOUT, transport=transport) as client:
        response = await client.post(
            f'{_base_url(storage_api_url)}/{_PAT_PATH}',
            headers={'Authorization': f'Bearer {subject_token}'},
            json=payload,
        )
        if response.is_error:
            # Full detail (request payload + response body) to debug logs only -- never surfaced
            # directly, since it can end up in a CLI transcript/bug report.
            LOG.debug(f'POST /{_PAT_PATH} failed ({response.status_code}) with {payload=}: {response.text}')
            raise RuntimeError(f'POST /{_PAT_PATH} failed ({response.status_code}). See debug logs for details.')
        body = cast(dict, response.json())
    pat = body.get('token') or body.get('pat') or body.get('accessToken')
    if not pat:
        raise RuntimeError(f'PAT creation response did not contain a token: keys={sorted(body)}')
    return cast(str, pat)


async def lease_pat(
    storage_api_url: str,
    *,
    subject_token: str,
    project_ids: list[int] | None = None,
    totp_code: str | None = None,
    recovery_code: str | None = None,
    name: str = 'keboola-mcp-server',
    expires_in: int = _PAT_DEFAULT_EXPIRES_SECONDS,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """Leases a PAT: introspect (or use the caller's explicit ``project_ids``) → sudo (MFA) →
    create PAT.

    ``subject_token`` is the whole-stack session access token (``kbc_at_*``) from the PKCE login.
    ``project_ids=None`` means "every project the token can currently reach" -- callers making an
    explicit choice (e.g. `login --pat`'s scoping prompt) should always pass it explicitly instead
    of relying on this default, per the "Security hardening" RFC increment.
    """
    if project_ids is None:
        introspection = await introspect_token(storage_api_url, subject_token=subject_token, transport=transport)
        project_ids = [p.id for p in introspection.projects]
    if not project_ids:
        raise RuntimeError('The session token can not reach any projects; cannot create a PAT.')
    elevated = await elevate_session(
        storage_api_url,
        subject_token=subject_token,
        totp_code=totp_code,
        recovery_code=recovery_code,
        transport=transport,
    )
    return await create_pat(
        storage_api_url,
        subject_token=elevated,
        project_ids=project_ids,
        name=name,
        expires_in=expires_in,
        transport=transport,
    )


async def exchange_code(
    storage_api_url: str,
    *,
    code: str,
    state: str,
    code_verifier: str,
    redirect_uri: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> TokenSet:
    """Exchanges a PKCE authorization code for a session token set."""
    payload = {
        'clientId': _client_id(),
        'code': code,
        'state': state,
        'redirectUri': redirect_uri,
        'codeVerifier': code_verifier,
    }
    async with httpx.AsyncClient(timeout=_AUTH_TIMEOUT, transport=transport) as client:
        response = await client.post(f'{_base_url(storage_api_url)}/{_TOKEN_PATH}', json=payload)
        response.raise_for_status()
        return parse_token_response(cast(dict, response.json()))


async def refresh_tokens(
    storage_api_url: str,
    *,
    refresh_token: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> TokenSet:
    """Exchanges a refresh token for a new (rotated) session token set."""
    async with httpx.AsyncClient(timeout=_AUTH_TIMEOUT, transport=transport) as client:
        response = await client.post(
            f'{_base_url(storage_api_url)}/{_REFRESH_PATH}', json={'refreshToken': refresh_token}
        )
        response.raise_for_status()
        return parse_token_response(cast(dict, response.json()))


# --- credential storage (mode-600 file, keyed by stack host + interface profile) ---


def _store_key(storage_api_url: str, profile: str | None = None) -> str:
    hostname = cast(str, urlparse(storage_api_url).hostname)
    return f'{hostname}::{_resolve_profile(profile)}'


def _read_store() -> dict:
    if not _CREDENTIALS_PATH.is_file():
        return {}
    try:
        return cast(dict, json.loads(_CREDENTIALS_PATH.read_text()))
    except (ValueError, OSError):
        LOG.warning('Could not read MCP credentials file; treating as empty.')
        return {}


def _write_store(store: dict) -> None:
    """Writes the credential store with restrictive permissions, never widening them.

    The file is created 0600 atomically (no world-readable window between create and
    chmod) and its parent directory 0700.
    """
    _CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(_CREDENTIALS_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        # O_CREAT only applies the mode when creating; a pre-existing file could be world-readable.
        # fchmod BEFORE writing any token material so there is no exposure window (O_TRUNC already
        # emptied the file, so nothing sensitive exists until json.dump runs after this).
        os.fchmod(f.fileno(), 0o600)
        json.dump(store, f, indent=2, ensure_ascii=False)


@contextlib.asynccontextmanager
async def _credentials_lock():
    """Cross-process insurance around the on-disk read-modify-write (defense in depth; the
    primary fix for the credential race is per-profile keying, see `_store_key`). Non-blocking
    poll of a sibling `.lock` file -- never a blocking `flock`, which would stall the event loop
    and could hang the MCP initialize handshake. Degrades to a no-op (with a warning) on timeout
    or on a non-POSIX platform where `fcntl` is unavailable.
    """
    if fcntl is None:
        yield
        return
    lock_path = _CREDENTIALS_PATH.parent / (_CREDENTIALS_PATH.name + '.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        deadline = time.time() + _LOCK_TIMEOUT_SECONDS
        locked = False
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                break
            except BlockingIOError:
                if time.time() >= deadline:
                    LOG.warning('Timed out waiting for the credentials file lock; proceeding without it.')
                    break
                await asyncio.sleep(_LOCK_POLL_INTERVAL_SECONDS)
        try:
            yield
        finally:
            if locked:
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def load_tokens(storage_api_url: str, *, profile: str | None = None) -> TokenSet | None:
    entry = _read_store().get(_store_key(storage_api_url, profile))
    if not entry:
        return None
    return TokenSet(**entry)


def save_tokens(storage_api_url: str, tokens: TokenSet, *, profile: str | None = None) -> None:
    store = _read_store()
    store[_store_key(storage_api_url, profile)] = asdict(tokens)
    _write_store(store)


async def get_access_token(
    storage_api_url: str,
    *,
    profile: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """
    Returns a valid access token for the stack+profile, refreshing (and persisting the rotated
    pair) when near expiry. Raises if there are no stored credentials (run ``login``).

    Double-checked locking: an `asyncio.Lock` per (stack, profile) serializes concurrent
    refreshes from within this process; a cross-process `flock` (see `_credentials_lock`) is
    layered on top as insurance. After acquiring both, the stored tokens are re-read -- another
    caller may have already refreshed while this one was waiting, in which case no network call
    is made at all.
    """
    tokens = load_tokens(storage_api_url, profile=profile)
    if not tokens:
        raise RuntimeError(
            f'No stored credentials for {storage_api_url}. Run "keboola-mcp-server login --api-url <url>" first.'
        )
    if not tokens.is_near_expiry:
        return tokens.access_token

    key = _store_key(storage_api_url, profile)
    lock = _refresh_locks.setdefault(key, asyncio.Lock())
    async with lock:
        async with _credentials_lock():
            tokens = load_tokens(storage_api_url, profile=profile)
            if not tokens:
                raise RuntimeError(
                    f'No stored credentials for {storage_api_url}. '
                    'Run "keboola-mcp-server login --api-url <url>" first.'
                )
            if not tokens.is_near_expiry:
                return tokens.access_token
            try:
                refreshed = await refresh_tokens(
                    storage_api_url, refresh_token=tokens.refresh_token, transport=transport
                )
            except httpx.HTTPStatusError as e:
                # Dead token (refresh rejected). Only forget it if it's still the same refresh
                # token we just tried -- another caller may have already rotated it, in which
                # case dropping the (now newer) entry would just force an unnecessary re-login.
                current = load_tokens(storage_api_url, profile=profile)
                if current is not None and current.refresh_token == tokens.refresh_token:
                    _forget(storage_api_url, profile=profile)
                raise RuntimeError(
                    f'Session for {storage_api_url} has expired; run "keboola-mcp-server login --api-url <url>" again.'
                ) from e
            # The refresh response carries no scope -- carry the previously-persisted choice
            # forward so a rotation never silently drops it.
            refreshed = dataclasses.replace(refreshed, project_ids=tokens.project_ids, read_only=tokens.read_only)
            save_tokens(storage_api_url, refreshed, profile=profile)
        return refreshed.access_token


async def ensure_access_token(
    storage_api_url: str,
    *,
    profile: str | None = None,
    allow_interactive: bool = True,
    open_browser=webbrowser.open,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """Return a valid access token, running the browser PKCE login when needed and allowed.

    Convenience for the locally-run (stdio) server so it can be started with only the stack URL
    and no separate ``login`` step: if no session is stored, or the stored one can no longer be
    refreshed, this logs in interactively (opens a browser + loopback callback), persists the
    session, and returns the fresh token.

    ``allow_interactive`` MUST be false unless a real terminal is attached. When the stdio server
    is launched by an MCP client its stdout is the JSON-RPC channel and there is no TTY, so an
    interactive login would both corrupt the protocol stream and block the initialize handshake
    (and the loopback wait, though bounded by ``_LOGIN_CALLBACK_TIMEOUT_SECONDS``, would still stall
    the handshake for its duration). In that case this raises the same "run login" guidance as
    ``get_access_token`` instead of attempting a browser login. Remote/deployed servers must use
    client-driven OAuth regardless.
    """
    try:
        return await get_access_token(storage_api_url, profile=profile, transport=transport)
    except RuntimeError as exc:
        if not allow_interactive:
            raise
        LOG.info(f'No usable stored session for {storage_api_url} ({exc}); starting browser login.')
        await perform_login(storage_api_url, profile=profile, open_browser=open_browser)
        return await get_access_token(storage_api_url, profile=profile, transport=transport)


def _forget(storage_api_url: str, *, profile: str | None = None) -> None:
    store = _read_store()
    if store.pop(_store_key(storage_api_url, profile), None) is not None:
        _write_store(store)


def forget_tokens(storage_api_url: str | None = None, *, profile: str | None = None) -> bool:
    """Deletes the stored PKCE session — for one stack+profile, or every stack/profile when
    ``storage_api_url`` is None.

    Returns True if anything was removed. Used by the ``logout`` command so the next ``login`` starts
    a fresh browser flow (e.g. to switch user/token) instead of refreshing the old session.
    """
    store = _read_store()
    if not store:
        return False
    if storage_api_url is None:
        _write_store({})
        return True
    if store.pop(_store_key(storage_api_url, profile), None) is not None:
        _write_store(store)
        return True
    return False


# --- interactive browser login (not unit-tested; exercises a real browser + loopback) ---


class _CallbackHandler(BaseHTTPRequestHandler):
    result: ClassVar[dict] = {}

    def do_GET(self) -> None:
        query = urllib.parse.parse_qs(urlparse(self.path).query)
        type(self).result = {k: v[0] for k, v in query.items()}
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Login complete. You can close this tab and return to the terminal.')

    def log_message(self, *args) -> None:  # silence the default stderr logging
        pass


async def perform_login(storage_api_url: str, *, profile: str | None = None, open_browser=webbrowser.open) -> TokenSet:
    """Runs the interactive PKCE browser login and persists the resulting tokens."""
    verifier = _b64url(secrets.token_bytes(48))  # 64 url-safe chars
    challenge = _b64url(hashlib.sha256(verifier.encode('ascii')).digest())
    state = _b64url(secrets.token_bytes(32))

    server = HTTPServer(('127.0.0.1', 0), _CallbackHandler)
    redirect_uri = f'http://127.0.0.1:{server.server_address[1]}/callback'
    params = {
        'responseType': 'code',
        'clientId': _client_id(),
        'redirectUri': redirect_uri,
        'codeChallenge': challenge,
        'codeChallengeMethod': 'S256',
        'state': state,
    }
    authorize_url = f'{_base_url(storage_api_url)}/{_AUTHORIZE_PATH}?{urllib.parse.urlencode(params)}'
    # Never write to stdout: under the stdio transport stdout is the JSON-RPC channel. Use stderr.
    print(f'Open this URL in your browser to authenticate:\n\n  {authorize_url}\n', file=sys.stderr, flush=True)
    open_browser(authorize_url)

    _CallbackHandler.result = {}
    # Bound the wait: handle_request() returns after `timeout` seconds even if no callback arrives,
    # so a closed tab / blocked browser fails with a clear error instead of hanging indefinitely.
    server.timeout = _LOGIN_CALLBACK_TIMEOUT_SECONDS
    server.handle_request()  # blocks until the browser hits /callback or the timeout elapses
    server.server_close()
    result = _CallbackHandler.result

    if not result:
        raise RuntimeError(
            f'Timed out after {_LOGIN_CALLBACK_TIMEOUT_SECONDS}s waiting for the browser sign-in callback. '
            'Re-run the login and complete authentication in the opened browser window.'
        )
    if result.get('error'):
        raise RuntimeError(f'Authorization failed: {result.get("error")} {result.get("errorDescription", "")}'.strip())
    if not secrets.compare_digest(result.get('state', ''), state):
        raise RuntimeError('Authorization state mismatch; aborting login.')
    code = result.get('code')
    if not code:
        raise RuntimeError('Authorization callback did not return a code.')

    print('Exchanging authorization code for tokens…', file=sys.stderr, flush=True)
    tokens = await exchange_code(
        storage_api_url, code=code, state=state, code_verifier=verifier, redirect_uri=redirect_uri
    )
    save_tokens(storage_api_url, tokens, profile=profile)
    return tokens
