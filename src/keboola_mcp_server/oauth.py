import logging
import secrets
import time
from http.client import HTTPException

from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    OAuthAuthorizationServerProvider,
    RefreshToken,
    construct_redirect_uri,
)
from mcp.shared._httpx_utils import create_mcp_http_client
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from pydantic import AnyHttpUrl

LOG = logging.getLogger(__name__)


class SimpleOAuthProvider(OAuthAuthorizationServerProvider):
    """Simple GitHub OAuth provider with essential functionality."""

    # TODO: GitHub OAuth URLs and scope. Change to Keboola OAuth server values.
    _OAUTH_SERVER_AUTH_URL: str = 'https://github.com/login/oauth/authorize'
    _OAUTH_SERVER_TOKEN_URL: str = 'https://github.com/login/oauth/access_token'
    _OAUTH_SERVER_SCOPE: str = 'read:user'

    MCP_SERVER_SCOPE: str = 'mcp'

    def __init__(
            self, *, oauth_callback_url: str, client_id: str, client_secret: str
    ) -> None:
        """
        Creates OAuth provider implementation for GitHub.

        :param oauth_callback_url: The URL where the OAuth server redirects to after the user authorizes.
        :param client_id: The client ID registered with the OAuth server.
        :param client_secret: The client secret registered with the OAuth server.
        """
        self._oauth_callback_url = oauth_callback_url
        self._oauth_client_id = client_id
        self._oauth_client_secret = client_secret

        LOG.debug(f'oauth_callback_url={self._oauth_callback_url}')
        LOG.debug(f'oauth_client_id={self._oauth_client_id}')
        LOG.debug(f'oauth_client_secret={self._oauth_client_secret}')

        self.clients: dict[str, OAuthClientInformationFull] = {}

        self.auth_codes: dict[str, AuthorizationCode] = {}
        self.tokens: dict[str, AccessToken] = {}
        self.state_mapping: dict[str, dict[str, str]] = {}

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        """Get OAuth client information."""
        client =  self.clients.get(client_id)
        LOG.debug(f"[get_client] client_id={client_id}, client={client}")
        return client

    async def register_client(self, client_info: OAuthClientInformationFull):
        """Register a new OAuth client."""
        LOG.debug(f"[register_client] client_info={client_info}")
        self.clients[client_info.client_id] = client_info

    async def authorize(
            self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        """Generate an authorization URL for GitHub OAuth flow."""
        state = params.state or secrets.token_hex(16)

        # Store the state mapping
        self.state_mapping[state] = {
            "redirect_uri": str(params.redirect_uri),
            "code_challenge": params.code_challenge,
            "redirect_uri_provided_explicitly": str(
                params.redirect_uri_provided_explicitly
            ),
            "client_id": client.client_id,
        }

        # Build GitHub authorization URL
        auth_url = (
            f"{self._OAUTH_SERVER_AUTH_URL}"
            f"?client_id={self._oauth_client_id}"
            f"&redirect_uri={self._oauth_callback_url}"
            f"&scope={self._OAUTH_SERVER_SCOPE}"
            f"&state={state}"
        )

        LOG.debug(f"[authorize] client={client}, params={params}, {auth_url}")

        return auth_url

    async def handle_oauth_callback(self, code: str, state: str) -> str:
        """Handle GitHub OAuth callback."""
        state_data = self.state_mapping.get(state)
        if not state_data:
            LOG.exception(f"[handle_github_callback] Invalid state: {state}")
            raise HTTPException(400, "Invalid state parameter")

        redirect_uri = state_data["redirect_uri"]
        code_challenge = state_data["code_challenge"]
        redirect_uri_provided_explicitly = (
                state_data["redirect_uri_provided_explicitly"] == "True"
        )
        client_id = state_data["client_id"]

        # Exchange code for token with GitHub
        # TODO: Don't use create_mcp_http_client from a private module.
        async with create_mcp_http_client() as client:
            response = await client.post(
                self._OAUTH_SERVER_TOKEN_URL,
                data={
                    "client_id": self._oauth_client_id,
                    "client_secret": self._oauth_client_secret,
                    "code": code,
                    # TODO: Why is the redirect_uri here? The POST /token endpoint does not redirect anywhere.
                    "redirect_uri": self._oauth_callback_url,
                },
                headers={"Accept": "application/json"},
            )

            if response.status_code != 200:
                LOG.exception(f"[handle_github_callback] Failed to exchange code for token")
                raise HTTPException(400, "Failed to exchange code for token")

            data = response.json()

            if "error" in data:
                LOG.exception(f"[handle_github_callback] GitHub error: data={data}")
                raise HTTPException(400, data.get("error_description", data["error"]))

            github_token = data["access_token"]
            LOG.debug(f"[handle_github_callback] github_token={github_token}")

            # Create MCP authorization code
            new_code = f"mcp_{secrets.token_hex(16)}"
            auth_code = AuthorizationCode(
                code=new_code,
                client_id=client_id,
                redirect_uri=AnyHttpUrl(redirect_uri),
                redirect_uri_provided_explicitly=redirect_uri_provided_explicitly,
                expires_at=time.time() + 300,
                scopes=[self.MCP_SERVER_SCOPE],
                code_challenge=code_challenge,
            )
            self.auth_codes[new_code] = auth_code

            # Store GitHub token - we'll map the MCP token to this later
            self.tokens[github_token] = AccessToken(
                token=github_token,
                client_id=client_id,
                scopes=[self._OAUTH_SERVER_SCOPE],
                expires_at=None,
            )

        del self.state_mapping[state]

        mcp_redirect_uri = construct_redirect_uri(redirect_uri, code=new_code, state=state)
        LOG.debug(f"[handle_github_callback] mcp_redirect_uri={mcp_redirect_uri}")

        return mcp_redirect_uri

    async def load_authorization_code(
            self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        """Load an authorization code."""
        mcp_auth_code = self.auth_codes.get(authorization_code)
        LOG.debug(f"[load_authorization_code] client={client}, authorization_code={authorization_code}, mcp_auth_code={mcp_auth_code}")
        return mcp_auth_code

    async def exchange_authorization_code(
            self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        """Exchange authorization code for tokens."""
        if authorization_code.code not in self.auth_codes:
            LOG.exception(f"[exchange_authorization_code] Invalid authorization code: "
                          f"client={client}, authorization_code={authorization_code}")
            raise ValueError("Invalid authorization code")

        # Generate MCP access token
        mcp_token = f"mcp_{secrets.token_hex(32)}"

        # Store MCP token
        mcp_access_token = AccessToken(
            token=mcp_token,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=int(time.time()) + 3600,
        )
        self.tokens[mcp_token] = mcp_access_token

        # Find GitHub token for this client
        github_token = next(
            (
                token
                for token, data in self.tokens.items()
                # see https://github.blog/engineering/platform-security/behind-githubs-new-authentication-token-formats/
                # which you get depends on your GH app setup.
                if (token.startswith("ghu_") or token.startswith("gho_"))
                   and data.client_id == client.client_id
            ),
            None,
        )

        del self.auth_codes[authorization_code.code]

        mcp_oauth_token = OAuthToken(
            access_token=mcp_token,
            token_type="bearer",
            expires_in=3600,
            scope=" ".join(authorization_code.scopes),
        )

        LOG.debug(f"[exchange_authorization_code] mcp_access_token={mcp_oauth_token}, "
                  f"mcp_oauth_token={mcp_oauth_token}, github_token={github_token}")

        return mcp_oauth_token

    async def load_access_token(self, token: str) -> AccessToken | None:
        """Load and validate an access token."""
        access_token = self.tokens.get(token)
        LOG.debug(f"[load_access_token] token={token}, access_token={access_token}")
        if not access_token:
            return None

        # Check if expired
        now = time.time()
        if access_token.expires_at and access_token.expires_at < now:
            del self.tokens[token]
            LOG.debug(f"[load_access_token] Expired token: access_token.expires_at={access_token.expires_at}, "
                      f"now={now}")
            return None

        return access_token

    async def load_refresh_token(
            self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        """Load a refresh token - not supported."""
        LOG.debug(f"[load_refresh_token] client={client}, refresh_token={refresh_token}")
        return None

    async def exchange_refresh_token(
            self,
            client: OAuthClientInformationFull,
            refresh_token: RefreshToken,
            scopes: list[str],
    ) -> OAuthToken:
        """Exchange refresh token"""
        LOG.debug(f"[exchange_refresh_token] client={client}, refresh_token={refresh_token}, scopes={scopes}")
        raise NotImplementedError("Not supported")

    async def revoke_token(
            self, token: str, token_type_hint: str | None = None
    ) -> None:
        """Revoke a token."""
        LOG.debug(f"[revoke_token] token={token}, token_type_hint={token_type_hint}")
        if token in self.tokens:
            del self.tokens[token]
