"""Auth-bridge exchange against Connection's internal endpoint (PSGO-261).

`OAuthSessionExchanger` exchanges a league OAuth access token from the remote/HTTP OAuth
login flow (`oauth.py`) for a whole-stack Keboola programmatic session (`kbc_at_*`),
authenticating to Connection with the MCP server's own projected Kubernetes ServiceAccount
JWT (`X-Kubernetes-Authorization`); the user's token travels as `X-Subject-Token`. The
resulting `kbc_at_*` session feeds into the same downstream pipe as a directly-supplied
one -- forwarded as a Bearer to every service `KeboolaClient` wraps (Storage, Queue, AI,
etc.), narrowed to a project via `X-KBC-ProjectId` once known. No further exchange into a
legacy per-project Storage token is needed or performed.

The SA token file is read per call so kubelet rotation is honored. No token material is
ever logged or placed in exception messages.
"""

import logging
from http import HTTPStatus
from typing import cast

import httpx

from keboola_mcp_server.clients.base import normalize_storage_api_url, read_service_account_jwt

LOG = logging.getLogger(__name__)

_ACCESS_TOKEN_PREFIX = 'kbc_at_'
_PAT_PREFIX = 'kbc_pat_'
_EXCHANGE_OAUTH_ENDPOINT = 'manage/internal/auth-bridge/exchange-oauth-token'
# Resolver statuses passed through to the client verbatim; anything else (incl. 5xx,
# timeouts, network failures) is mapped to 502 Bad Gateway.
_PASS_THROUGH_STATUSES = frozenset(
    {int(HTTPStatus.BAD_REQUEST), int(HTTPStatus.UNAUTHORIZED), int(HTTPStatus.FORBIDDEN)}
)


def strip_bearer(token: str) -> str:
    """Removes a leading case-insensitive ``Bearer `` scheme from a token, if present."""
    if token[:7].lower() == 'bearer ':
        return token[7:].strip()
    return token


def is_programmatic_token(token: str | None) -> bool:
    """True if ``token`` is a Keboola programmatic bearer token (``kbc_at_`` / ``kbc_pat_``)."""
    if not token:
        return False
    bare = strip_bearer(token)
    return bare.startswith(_ACCESS_TOKEN_PREFIX) or bare.startswith(_PAT_PREFIX)


class OAuthTokenExchangeError(RuntimeError):
    """Raised when the auth-bridge fails to exchange a league OAuth token for a programmatic session.

    :ivar status_code: The client-facing HTTP status (resolver 400/401/403 pass through;
        5xx/timeout/network map to 502).
    """

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message, status_code)
        self.status_code = status_code

    def __str__(self) -> str:
        return self.args[0]


class OAuthSessionExchanger:
    """Exchanges a league OAuth access token (``claudai projectless`` scope) for a whole-stack
    Keboola programmatic session (PSGO-261 oauth_session_exchange RFC)."""

    def __init__(
        self,
        *,
        storage_api_url: str,
        kubernetes_token_path: str,
        timeout: httpx.Timeout | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """
        :param storage_api_url: Connection Storage API URL (``https://connection.<stack>``).
        :param kubernetes_token_path: Path to the projected ServiceAccount token file.
        :param timeout: Optional HTTP timeout override.
        :param transport: Optional httpx transport (for testing).
        """
        self._base_url = normalize_storage_api_url(storage_api_url)
        self._kubernetes_token_path = kubernetes_token_path
        self._timeout = timeout or httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
        self._transport = transport

    def _read_sa_jwt(self) -> str:
        # Read per call — the kubelet rotates the projected token in place.
        return read_service_account_jwt(self._kubernetes_token_path)

    async def exchange(self, *, oauth_access_token: str) -> dict:
        """
        Exchanges ``oauth_access_token`` for a ``CliTokenResponse`` (same shape as a PKCE login).

        :return: The raw response body (``accessToken``/``refreshToken``/``expiresIn``/``sessionId``).
        :raises OAuthTokenExchangeError: On any exchange failure (status carried on the error).
        """
        # X-KBC-ManageApiToken is a DIFFERENT, mutually-exclusive authenticator (a real Manage
        # token lookup) -- confirmed against Connection's source that it must never be sent
        # alongside X-Kubernetes-Authorization; the k8s JWT alone authorizes this endpoint.
        sa_jwt = self._read_sa_jwt()
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Kubernetes-Authorization': f'Bearer {sa_jwt}',
            'X-Subject-Token': f'Bearer {strip_bearer(oauth_access_token)}',
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
                response = await client.post(f'{self._base_url}/{_EXCHANGE_OAUTH_ENDPOINT}', headers=headers, json={})
        except httpx.HTTPError as e:
            raise OAuthTokenExchangeError(
                f'OAuth-token exchange could not reach Connection ({type(e).__name__}).',
                status_code=int(HTTPStatus.BAD_GATEWAY),
            ) from None

        if response.status_code != HTTPStatus.OK:
            status = response.status_code
            mapped = status if status in _PASS_THROUGH_STATUSES else int(HTTPStatus.BAD_GATEWAY)
            LOG.error(f'OAuth-token exchange failed: resolver status {status}, mapped to {mapped}.')
            raise OAuthTokenExchangeError(
                f'OAuth-token exchange was rejected (resolver status {status}).',
                status_code=mapped,
            )

        try:
            body = response.json()
        except ValueError:
            raise OAuthTokenExchangeError(
                'OAuth-token exchange returned a non-JSON body.',
                status_code=int(HTTPStatus.BAD_GATEWAY),
            ) from None
        if not isinstance(body, dict) or not body.get('accessToken') or not body.get('refreshToken'):
            raise OAuthTokenExchangeError(
                'OAuth-token exchange returned an incomplete response.',
                status_code=int(HTTPStatus.BAD_GATEWAY),
            )
        return cast(dict, body)
