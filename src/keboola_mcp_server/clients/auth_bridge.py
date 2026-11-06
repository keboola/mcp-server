"""Exchange Keboola auth-bridge tokens against Connection's internal endpoints (PSGO-261).

The exchange authenticates to Connection with the MCP server's own projected Kubernetes ServiceAccount JWT
(`X-Kubernetes-Authorization`); the caller's token travels as `X-Subject-Token`. The SA token file is read per call
so kubelet rotation is honored. No token material is ever logged or placed in exception messages.

`OAuthSessionExchanger` exchanges a league OAuth access token from the remote/HTTP OAuth login flow (`oauth.py`) for a
whole-stack Keboola programmatic session (`kbc_at_*`). That token is forwarded as a Bearer to every Keboola service,
narrowed to a project via `X-KBC-ProjectId` once known.
"""

import logging
from http import HTTPStatus
from typing import cast

import httpx

from keboola_mcp_server.clients.base import normalize_storage_api_url, read_service_account_jwt
from keboola_mcp_server.config import strip_bearer

LOG = logging.getLogger(__name__)

_EXCHANGE_OAUTH_ENDPOINT = 'manage/internal/auth-bridge/exchange-oauth-token'
# Exchange statuses passed through to the client verbatim; anything else (incl. 5xx,
# timeouts, network failures) is mapped to 502 Bad Gateway.
_PASS_THROUGH_STATUSES = frozenset(
    {int(HTTPStatus.BAD_REQUEST), int(HTTPStatus.UNAUTHORIZED), int(HTTPStatus.FORBIDDEN)}
)


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
    """Exchanges a league OAuth access token for a Keboola programmatic session (PSGO-261
    oauth_session_exchange RFC): whole-stack for ``claudai projectless`` scope (every registered
    client since AI-2883, RFC Decision §10), or pinned to a single project for ``claudai`` alone
    (only a session persisted by an earlier build of AI-2883 for a dynamically-approved client)."""

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

    async def exchange(
        self,
        *,
        oauth_access_token: str,
        app_client_id: str | None = None,
        reported_client_name: str | None = None,
    ) -> dict:
        """
        Exchanges ``oauth_access_token`` for a ``CliTokenResponse`` (same shape as a PKCE login).

        :param app_client_id: The Connection client row the login is for; sent only with ``reported_client_name``.
        :param reported_client_name: The name a local tool reported for itself (DMD-2600). Display-only: Connection
            stores it on the session only for its ``local-mcp-client`` row and never fails the exchange over it.
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
        body: dict[str, str] = {}
        if app_client_id and reported_client_name:
            body = {'app_client_id': app_client_id, 'reported_client_name': reported_client_name}
        try:
            async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
                response = await client.post(f'{self._base_url}/{_EXCHANGE_OAUTH_ENDPOINT}', headers=headers, json=body)
        except httpx.HTTPError as e:
            raise OAuthTokenExchangeError(
                f'OAuth-token exchange could not reach Connection ({type(e).__name__}).',
                status_code=int(HTTPStatus.BAD_GATEWAY),
            ) from None

        if response.status_code != HTTPStatus.OK:
            status = response.status_code
            mapped = status if status in _PASS_THROUGH_STATUSES else int(HTTPStatus.BAD_GATEWAY)
            # A pass-through status is the caller's own token being rejected, not a server fault -- only
            # ERROR when mapped to 502 (Connection itself unreachable/erroring).
            log = LOG.error if mapped == int(HTTPStatus.BAD_GATEWAY) else LOG.warning
            log(f'OAuth-token exchange failed: resolver status {status}, mapped to {mapped}.')
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
