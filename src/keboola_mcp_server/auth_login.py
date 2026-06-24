"""Local browser PKCE login for the MCP server (PSGO-261, Part B).

Lets a user authenticate the locally-run (stdio) MCP server with only the stack URL:
a browser PKCE flow leases a whole-stack session (access + refresh token), which is
stored to a mode-600 file and refreshed during usage. The leased ``kbc_at_*`` access
token is then forwarded downstream as the bearer credential.

The interactive browser/loopback orchestration lives in ``perform_login``; the HTTP
calls (``exchange_code``, ``refresh_tokens``) are split out so they can be tested with
an injected httpx transport.
"""

import base64
import hashlib
import json
import logging
import os
import secrets
import time
import urllib.parse
import webbrowser
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import cast
from urllib.parse import urlparse, urlunparse

import httpx

LOG = logging.getLogger(__name__)

DEFAULT_CLIENT_ID = 'keboola-cli-demo'
_AUTHORIZE_PATH = 'admin/auth/pkce/authorize'
_TOKEN_PATH = 'v1/auth/pkce/token'
_REFRESH_PATH = 'v1/auth/token/refresh'
_REFRESH_SKEW_SECONDS = 60
_CREDENTIALS_PATH = Path.home() / '.keboola' / 'mcp' / 'credentials.json'


def _client_id() -> str:
    # Configurable so the real MCP client id can replace the demo value via a secret.
    return os.environ.get('KBC_PKCE_CLIENT_ID') or DEFAULT_CLIENT_ID


def _base_url(storage_api_url: str) -> str:
    parsed = urlparse(storage_api_url)
    if not parsed.hostname or not parsed.hostname.startswith('connection.'):
        raise ValueError(f'Invalid Keboola Storage API URL: {storage_api_url}')
    return urlunparse(('https', parsed.hostname, '', '', '', ''))


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode('ascii').rstrip('=')


@dataclass(frozen=True)
class TokenSet:
    """A leased session: the access token plus what's needed to refresh it."""

    access_token: str
    refresh_token: str
    expires_at: float  # epoch seconds
    session_id: str | None = None

    @property
    def is_near_expiry(self) -> bool:
        return time.time() >= (self.expires_at - _REFRESH_SKEW_SECONDS)


def _parse_token_response(body: dict, *, now: float | None = None) -> TokenSet:
    now = time.time() if now is None else now
    return TokenSet(
        access_token=cast(str, body['accessToken']),
        refresh_token=cast(str, body['refreshToken']),
        expires_at=now + float(body.get('expiresIn') or 0),
        session_id=cast('str | None', body.get('sessionId')),
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
    async with httpx.AsyncClient(timeout=30.0, transport=transport) as client:
        response = await client.post(f'{_base_url(storage_api_url)}/{_TOKEN_PATH}', json=payload)
        response.raise_for_status()
        return _parse_token_response(cast(dict, response.json()))


async def refresh_tokens(
    storage_api_url: str,
    *,
    refresh_token: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> TokenSet:
    """Exchanges a refresh token for a new (rotated) session token set."""
    async with httpx.AsyncClient(timeout=30.0, transport=transport) as client:
        response = await client.post(
            f'{_base_url(storage_api_url)}/{_REFRESH_PATH}', json={'refreshToken': refresh_token}
        )
        response.raise_for_status()
        return _parse_token_response(cast(dict, response.json()))


# --- credential storage (mode-600 file, keyed by stack host) ---


def _store_key(storage_api_url: str) -> str:
    return cast(str, urlparse(storage_api_url).hostname)


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
        json.dump(store, f, indent=2)
    # O_CREAT honors the mode only when creating; chmod covers a pre-existing file.
    _CREDENTIALS_PATH.chmod(0o600)


def load_tokens(storage_api_url: str) -> TokenSet | None:
    entry = _read_store().get(_store_key(storage_api_url))
    if not entry:
        return None
    return TokenSet(**entry)


def save_tokens(storage_api_url: str, tokens: TokenSet) -> None:
    store = _read_store()
    store[_store_key(storage_api_url)] = asdict(tokens)
    _write_store(store)


async def get_access_token(
    storage_api_url: str,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str:
    """
    Returns a valid access token for the stack, refreshing (and persisting the rotated
    pair) when near expiry. Raises if there are no stored credentials (run ``login``).
    """
    tokens = load_tokens(storage_api_url)
    if not tokens:
        raise RuntimeError(
            f'No stored credentials for {storage_api_url}. Run "keboola-mcp-server login --api-url <url>" first.'
        )
    if tokens.is_near_expiry:
        try:
            tokens = await refresh_tokens(storage_api_url, refresh_token=tokens.refresh_token, transport=transport)
        except httpx.HTTPStatusError as e:
            # Dead token (refresh rejected). Drop the stale credentials and force a re-login.
            _forget(storage_api_url)
            raise RuntimeError(
                f'Session for {storage_api_url} has expired; run "keboola-mcp-server login --api-url <url>" again.'
            ) from e
        save_tokens(storage_api_url, tokens)
    return tokens.access_token


def _forget(storage_api_url: str) -> None:
    store = _read_store()
    if store.pop(_store_key(storage_api_url), None) is not None:
        _write_store(store)


# --- interactive browser login (not unit-tested; exercises a real browser + loopback) ---


class _CallbackHandler(BaseHTTPRequestHandler):
    result: dict = {}

    def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
        query = urllib.parse.parse_qs(urlparse(self.path).query)
        type(self).result = {k: v[0] for k, v in query.items()}
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Login complete. You can close this tab and return to the terminal.')

    def log_message(self, *args) -> None:  # silence the default stderr logging
        pass


async def perform_login(storage_api_url: str, *, open_browser=webbrowser.open) -> TokenSet:
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
    print(f'Open this URL in your browser to authenticate:\n\n  {authorize_url}\n', flush=True)
    open_browser(authorize_url)

    _CallbackHandler.result = {}
    server.handle_request()  # blocks until the browser hits /callback
    server.server_close()
    result = _CallbackHandler.result

    if result.get('error'):
        raise RuntimeError(f'Authorization failed: {result.get("error")} {result.get("errorDescription", "")}'.strip())
    if not secrets.compare_digest(result.get('state', ''), state):
        raise RuntimeError('Authorization state mismatch; aborting login.')
    code = result.get('code')
    if not code:
        raise RuntimeError('Authorization callback did not return a code.')

    print('Exchanging authorization code for tokens…', flush=True)
    tokens = await exchange_code(
        storage_api_url, code=code, state=state, code_verifier=verifier, redirect_uri=redirect_uri
    )
    save_tokens(storage_api_url, tokens)
    return tokens
