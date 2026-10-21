"""The router's own configuration, read from the process environment only (never from a request)."""

import dataclasses
from collections.abc import Mapping

from keboola_mcp_server.router.stacks import Stack, parse_stacks, router_issuer

# The defaults of RFC Security Considerations, "Abuse / DoS". The stack picker is answered by a person's browser, so its
# own address is what is counted; a person logs in a few times a day at most. A code is redeemed once and a refresh
# token used a few times an hour, so a credential sent more often than this is being guessed or replayed.
_DEFAULT_AUTHORIZE_RATE_LIMIT_PER_IP = 60
_DEFAULT_TOKEN_RATE_LIMIT_PER_CREDENTIAL = 10


def _env(env: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        if value := env.get(name):
            return value
    return None


def _positive_int(name: str, raw: str | None) -> int | None:
    if raw is None or raw == '':
        return None
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f'{name} must be a positive integer, got {raw!r}.') from None
    if value <= 0:
        raise ValueError(f'{name} must be a positive integer, got {raw!r}.')
    return value


@dataclasses.dataclass(frozen=True)
class RouterSettings:
    router_url: str
    """The router's public origin; its issuer and MCP resource are derived from it (`router.stacks`)."""
    stacks: Mapping[str, Stack]
    openai_apps_challenge: str | None = None
    """The domain verification token OpenAI asks the plugin's MCP host to serve; None serves a 404."""
    authorize_rate_limit_per_ip: int | None = _DEFAULT_AUTHORIZE_RATE_LIMIT_PER_IP
    """Requests per minute one browser address may send to `/authorize` (the stack picker)."""
    token_rate_limit_per_credential: int = _DEFAULT_TOKEN_RATE_LIMIT_PER_CREDENTIAL
    """Requests per minute with one code or refresh token to `/token`, and with one token to `/revoke`, counted by its
    SHA-256."""
    api_rate_limit_per_ip: int | None = None
    """Requests per minute one client address may send to `/register`. Unset by default: registrations come from the
    AI client's servers, so one address carries many users (every ChatGPT user shares OpenAI's egress addresses), and
    the ceiling must be sized from observed traffic."""
    trusted_proxy_hops: int = 1
    """How many proxies in front of the router append to `X-Forwarded-For` (see `oauth.caller_address`)."""
    upstream_timeout_seconds: float = 30.0
    """Connecting to a stack, and its whole answer on `/token` and `/revoke`."""
    mcp_read_timeout_seconds: float = 300.0
    """How long a stack may stay silent on `/mcp`, before its response starts (a long tool call) and between chunks.
    Whole seconds in the environment."""
    mcp_max_connections: int = 1000
    """Connections to the stacks that `/mcp` may hold at once, per replica; each request holds one until its answer
    ends. Above it a request waits up to `upstream_timeout_seconds` for a free one, then gets 503."""

    @property
    def issuer(self) -> str:
        return router_issuer(self.router_url)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> 'RouterSettings':
        """Reads the `KBC_ROUTER_*` variables (and `OPENAI_APPS_CHALLENGE`); raises ValueError when one is invalid or
        the router URL is missing."""
        router_url = _env(env, 'KBC_ROUTER_URL', 'ROUTER_URL')
        if not router_url:
            raise ValueError('Router mode needs its public URL: set KBC_ROUTER_URL (e.g. https://<global-host>).')
        router_issuer(router_url)  # validates it
        authorize_limit = _positive_int(
            'KBC_ROUTER_AUTHORIZE_RATE_LIMIT_PER_IP', _env(env, 'KBC_ROUTER_AUTHORIZE_RATE_LIMIT_PER_IP')
        )
        return cls(
            router_url=router_url,
            stacks=parse_stacks(_env(env, 'KBC_ROUTER_STACKS')),
            openai_apps_challenge=_env(env, 'OPENAI_APPS_CHALLENGE', 'KBC_OPENAI_APPS_CHALLENGE'),
            authorize_rate_limit_per_ip=authorize_limit or _DEFAULT_AUTHORIZE_RATE_LIMIT_PER_IP,
            token_rate_limit_per_credential=_positive_int(
                'KBC_ROUTER_TOKEN_RATE_LIMIT_PER_CREDENTIAL', _env(env, 'KBC_ROUTER_TOKEN_RATE_LIMIT_PER_CREDENTIAL')
            )
            or _DEFAULT_TOKEN_RATE_LIMIT_PER_CREDENTIAL,
            api_rate_limit_per_ip=_positive_int(
                'KBC_ROUTER_API_RATE_LIMIT_PER_IP', _env(env, 'KBC_ROUTER_API_RATE_LIMIT_PER_IP')
            ),
            mcp_read_timeout_seconds=_positive_int(
                'KBC_ROUTER_MCP_READ_TIMEOUT_SECONDS', _env(env, 'KBC_ROUTER_MCP_READ_TIMEOUT_SECONDS')
            )
            or cls.mcp_read_timeout_seconds,
            mcp_max_connections=_positive_int(
                'KBC_ROUTER_MCP_MAX_CONNECTIONS', _env(env, 'KBC_ROUTER_MCP_MAX_CONNECTIONS')
            )
            or cls.mcp_max_connections,
            trusted_proxy_hops=_positive_int(
                'KBC_OAUTH_TRUSTED_PROXY_HOPS', _env(env, 'KBC_OAUTH_TRUSTED_PROXY_HOPS', 'OAUTH_TRUSTED_PROXY_HOPS')
            )
            or 1,
        )
