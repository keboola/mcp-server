"""Exchange Keboola programmatic tokens for legacy Storage tokens (PSGO-261).

Implements the decentralized auth-bridge exchange: a programmatic bearer token
(`kbc_at_*` access token or `kbc_pat_*` personal access token) presented to the MCP
server is exchanged at Connection for a legacy Storage token, which is then used for
all downstream Storage-token APIs exactly as before.

The MCP server authenticates to the resolver with its own projected Kubernetes
ServiceAccount JWT (`X-Kubernetes-Authorization`); the user's token travels as
`X-Subject-Token`. The SA token file is read per call so kubelet rotation is honored.
No token material is ever logged or placed in exception messages.
"""

import logging
from http import HTTPStatus
from pathlib import Path
from typing import cast
from urllib.parse import urlparse, urlunparse

import httpx

LOG = logging.getLogger(__name__)

_ACCESS_TOKEN_PREFIX = 'kbc_at_'
_PAT_PREFIX = 'kbc_pat_'
_RESOLVE_ENDPOINT = 'manage/internal/auth-bridge/resolve-storage-token'
# Resolver statuses passed through to the client verbatim; anything else (incl. 5xx,
# timeouts, network failures) is mapped to 502 Bad Gateway.
_PASS_THROUGH_STATUSES = (int(HTTPStatus.BAD_REQUEST), int(HTTPStatus.UNAUTHORIZED), int(HTTPStatus.FORBIDDEN))


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
    return bare.startswith((_ACCESS_TOKEN_PREFIX, _PAT_PREFIX))


class StorageTokenExchangeError(RuntimeError):
    """Raised when the auth-bridge resolver fails to exchange a programmatic token.

    :ivar status_code: The client-facing HTTP status (resolver 400/401/403 pass through;
        5xx/timeout/network map to 502).
    """

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message, status_code)
        self.status_code = status_code

    def __str__(self) -> str:
        return self.args[0]


class StorageTokenResolver:
    """Exchanges a programmatic token for a legacy Storage token via the Connection resolver."""

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
        parsed = urlparse(storage_api_url)
        if not parsed.hostname or not parsed.hostname.startswith('connection.'):
            raise ValueError(f'Invalid Keboola Storage API URL: {storage_api_url}')
        self._base_url = urlunparse(('https', parsed.hostname, '', '', '', ''))
        self._kubernetes_token_path = kubernetes_token_path
        self._timeout = timeout or httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
        self._transport = transport

    def _read_sa_jwt(self) -> str:
        # Read per call — the kubelet rotates the projected token in place.
        jwt = Path(self._kubernetes_token_path).read_text().strip()
        if not jwt:
            raise ValueError(f'Kubernetes ServiceAccount token file is empty: {self._kubernetes_token_path}')
        return jwt

    async def resolve(self, *, subject_token: str, project_id: int) -> str:
        """
        Exchanges ``subject_token`` for the legacy Storage token of ``project_id``.

        :return: The legacy Storage token.
        :raises StorageTokenExchangeError: On any resolver failure (status carried on the error).
        """
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Kubernetes-Authorization': f'Bearer {self._read_sa_jwt()}',
            'X-Subject-Token': f'Bearer {strip_bearer(subject_token)}',
        }
        try:
            async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
                response = await client.post(
                    f'{self._base_url}/{_RESOLVE_ENDPOINT}',
                    headers=headers,
                    json={'projectId': project_id},
                )
        except httpx.HTTPError as e:
            # Network / timeout failure. Raise without chaining so no request (and thus no
            # token material) can surface in a traceback.
            raise StorageTokenExchangeError(
                f'Auth-bridge token exchange could not reach Connection ({type(e).__name__}).',
                status_code=int(HTTPStatus.BAD_GATEWAY),
            ) from None

        if response.status_code != HTTPStatus.OK:
            status = response.status_code
            mapped = status if status in _PASS_THROUGH_STATUSES else int(HTTPStatus.BAD_GATEWAY)
            LOG.error(f'Auth-bridge token exchange failed: resolver status {status}, mapped to {mapped}.')
            raise StorageTokenExchangeError(
                f'Auth-bridge token exchange was rejected (resolver status {status}).',
                status_code=mapped,
            )

        body = cast(dict, response.json())
        storage_token = body.get('storageToken')
        if not storage_token:
            raise StorageTokenExchangeError(
                'Auth-bridge token exchange returned no storageToken.',
                status_code=int(HTTPStatus.BAD_GATEWAY),
            )
        return cast(str, storage_token)
