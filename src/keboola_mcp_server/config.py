"""Configuration handling for the Keboola MCP server."""

import dataclasses
import importlib.metadata
import logging
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal
from urllib.parse import urlparse, urlunparse

LOG = logging.getLogger(__name__)
_NO_VALUE_MARKER = '__NO_VALUE_MARKER__'
Transport = Literal['stdio', 'streamable-http', 'http-compat/streamable-http']


def deployed_sa_token_path() -> str | None:
    """
    Path to the deployed server's projected Kubernetes ServiceAccount token, or None when running locally.

    The presence of the ``KBC_KUBERNETES_TOKEN_PATH`` env var is the single signal that this process is
    the Keboola-deployed MCP server (able to reach the auth-bridge resolver) rather than a local session.
    Read from the process environment only, never from per-request config.
    """
    return os.environ.get('KBC_KUBERNETES_TOKEN_PATH')


@dataclass(frozen=True)
class Config:
    """Server configuration."""

    storage_api_url: str | None = None
    """The URL to the Storage API."""
    storage_token: str | None = field(default=None, metadata={'aliases': ['storage_api_token']})
    """The token to access the storage API using the MCP tools."""
    branch_id: str | None = None
    """The branch ID to access the storage API using the MCP tools."""
    workspace_schema: str | None = None
    """Workspace schema to access the buckets, tables and execute sql queries."""
    workspace_id: str | None = field(default=None, metadata={'empty_means_absent': True, 'require_prefix': True})
    """Workspace ID to access the buckets, tables and execute sql queries (e.g. a Data App's own
    workspace, supplied per-request via the 'X-Workspace-Id' header). Takes precedence over
    `workspace_schema` when both are set.

    `require_prefix` is set because the bare `WORKSPACE_ID` env var is what Keboola injects into
    Data App containers -- without it, that variable would pin every session on such a server.
    `empty_means_absent` is set so an unset header template (`X-Workspace-Id:`) forwarded as an
    empty string is not mistaken for an explicit pin to override a server-side default with. This
    intentionally does NOT apply to `workspace_schema` (an empty `X-Workspace-Schema` header must
    keep clearing it back to the MCP-managed workspace, same as `branch_id` below) nor to most
    other fields -- it is opt-in per field precisely to avoid that kind of regression."""
    oauth_client_id: str | None = None
    """OAuth client ID registered in the Keboola OAuth Server."""
    oauth_client_secret: str | None = None
    """OAuth client secret registered in the Keboola OAuth Server."""
    oauth_server_url: str | None = None
    """The URL of the OAuth server to authenticate with."""
    oauth_scope: str | None = None
    """The OAuth scope to request from the OAuth server."""
    mcp_server_url: str | None = None
    """The URL where the MCP server si reachable."""
    jwt_secret: str | None = None
    """The secret key for encoding and decoding JWT tokens."""
    postgres_dsn: str | None = field(default=None, metadata={'aliases': ['mcp_db_url']})
    """Connection string for the Postgres-backed OAuth session store (oauth_session_persistence RFC).
    Required to enable OAuth login when oauth_client_id/oauth_client_secret are set.

    Maps the `MCP_DB_URL` / `KBC_MCP_DB_URL` env var (via the alias) as well as `KBC_POSTGRES_DSN`."""
    session_encryption_key: str | None = None
    """Base64-encoded 32-byte AES-256 key used to encrypt OAuth session credentials at rest."""
    bearer_token: str | None = None
    """The access-token issued by Keboola OAuth server to be sent in 'Authorization: Bearer <access-token>' header."""
    conversation_id: str | None = None
    """The ID of the ongoing conversation with the MCP server. This is supplied only by the HTTP header."""
    project_id: str | None = field(default=None, metadata={'aliases': ['kbc_project_id']})
    """Project id used to scope a programmatic-token (kbc_at_/kbc_pat_) exchange.

    Maps the `X-KBC-ProjectId` HTTP header (via the alias) and the `KBC_PROJECT_ID` env var.
    Only consulted when the inbound Storage token is a Keboola programmatic token; the legacy
    project-bound Storage token derives its project from the token itself."""

    # Fields a per-request HTTP header may legitimately set (see `replace_by_headers`). Everything
    # else -- jwt_secret, postgres_dsn, session_encryption_key, oauth_client_id/secret,
    # oauth_server_url, mcp_server_url -- is deployment-level configuration and must only ever come
    # from the process environment or CLI args, never a caller-supplied header. Without this
    # allowlist, a header literally named (in any of the exact/`KBC_`/`X-` spellings `_read_options`
    # accepts) e.g. `Jwt-Secret` would let a caller choose the HMAC key that verifies their own
    # `scope_token`, forging arbitrary `project_ids` -- see the "Security hardening" RFC increment.
    # `workspace_id` is included since `X-Workspace-Id` is a legitimate per-request pin (see its
    # field docstring above).
    _HEADER_ELIGIBLE_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {
            'storage_api_url',
            'storage_token',
            'branch_id',
            'workspace_schema',
            'workspace_id',
            'bearer_token',
            'conversation_id',
            'project_id',
        }
    )

    def __post_init__(self) -> None:
        for f in dataclasses.fields(self):
            if 'url' not in f.name:
                continue
            value = getattr(self, f.name)
            if value:
                orig_value = value
                url_value = urlparse(value)
                if url_value.netloc:
                    if (scheme := url_value.scheme) not in ['http', 'https']:
                        scheme = 'http' if url_value.netloc.startswith('localhost') else 'https'
                    value = urlunparse((scheme, url_value.netloc, '', '', '', ''))
                elif url_value.path:
                    value = urlunparse(('https', url_value.path.split('/', maxsplit=1)[0], '', '', '', ''))
                else:
                    raise ValueError(f'Invalid URL: {value}')
                if value != orig_value:
                    LOG.warning(f'Amended "{f.name}" value from "{orig_value}" to "{value}".')
                    object.__setattr__(self, f.name, value)

        if self.branch_id is not None and self.branch_id.lower() in ['', 'none', 'null', 'default', 'production']:
            object.__setattr__(self, 'branch_id', None)

        if self.workspace_id is not None and not self.workspace_id.isdigit():
            raise ValueError(f'Invalid workspace_id: {self.workspace_id!r}')

    @staticmethod
    def _normalize(name: str) -> str:
        """Removes dashes and underscores from the input string and turns it into lowercase."""
        return name.lower().replace('_', '').replace('-', '')

    @classmethod
    def _read_options(cls, d: Mapping[str, str], *, allowed_fields: frozenset[str] | None = None) -> Mapping[str, Any]:
        """:param allowed_fields: When given, only these field names are ever set -- fields
        outside it are skipped entirely, under every naming convention (`X-{name}` and `KBC_{name}`
        headers included). Used by `replace_by_headers` to keep deployment-level fields
        unreachable from a request; `None` (the default, for env/CLI-derived input) leaves every
        field reachable, since that input is already operator-trusted.
        """
        data = {cls._normalize(k): v for k, v in d.items()}
        options: dict[str, Any] = {}
        for f in dataclasses.fields(cls):
            if allowed_fields is not None and f.name not in allowed_fields:
                continue
            field_names = [f.name] + f.metadata.get('aliases', [])

            require_prefix = f.metadata.get('require_prefix', False)
            empty_means_absent = f.metadata.get('empty_means_absent', False)

            for name in field_names:
                value: str | None = _NO_VALUE_MARKER
                # `require_prefix` skips the bare field name -- only the KBC_/X- prefixed forms
                # count as "provided". Needed for fields whose bare name collides with a
                # variable set by something other than this server's own config (see
                # `workspace_id`'s docstring).
                candidates = (f'KBC_{name}', f'X-{name}') if require_prefix else (name, f'KBC_{name}', f'X-{name}')

                for candidate in candidates:
                    if (dict_name := cls._normalize(candidate)) in data:
                        candidate_value = data[dict_name]
                        # An empty value means "not provided" for opted-in fields only -- an
                        # unset header template (e.g. `X-Workspace-Id:`) must not be mistaken
                        # for an explicit request to override a server-side default with ''.
                        if empty_means_absent and candidate_value == '':
                            continue
                        value = candidate_value
                        break

                if value is not _NO_VALUE_MARKER:
                    if f.type == (bool | None):
                        options[f.name] = value.lower() in ('true', 'yes', '1')
                    elif f.type == (str | None):
                        options[f.name] = value
                    else:
                        raise ValueError(f'Unsupported type {f.type} for field {f.name}')
                    break

        return options

    @classmethod
    def from_dict(cls, d: Mapping[str, str]) -> 'Config':
        """
        Creates new `Config` instance with values read from the input mapping.
        The keys in the input mapping can either be the names of the fields in `Config` class
        or their uppercase variant prefixed with 'KBC_'.
        """
        return cls(**cls._read_options(d))

    def replace_by(self, d: Mapping[str, str]) -> 'Config':
        """
        Creates new `Config` instance from the existing one by replacing the values from the input mapping.
        The keys in the input mapping can either be the names of the fields in `Config` class
        or their uppercase variant prefixed with 'KBC_'.

        For a per-request HTTP request's headers (untrusted caller input), use
        `replace_by_headers` instead -- this method leaves every field reachable, which is only
        safe for operator-trusted input (the process environment, CLI args).

        A malformed `workspace_id` (or any other invalid value `__post_init__` rejects) always
        raises -- same as every other field. A per-request caller (e.g.
        `SessionStateMiddleware.apply_request_config`) that degrades this into "not provided"
        would silently widen an unpinned multi-tenant session's scope instead of rejecting the
        bad request; a trusted caller (the startup env merge in `create_server()`) needs it to
        fail loudly, the same way a malformed `--workspace-id` CLI flag already does.
        """
        return dataclasses.replace(self, **self._read_options(d))

    def replace_by_headers(self, headers: Mapping[str, str]) -> 'Config':
        """Like `replace_by`, but only ever sets fields in `_HEADER_ELIGIBLE_FIELDS` -- every
        other field (`jwt_secret`, `postgres_dsn`, `session_encryption_key`, `oauth_client_id`/
        `oauth_client_secret`, `oauth_server_url`, `mcp_server_url`) is deployment-level
        configuration and must never be settable by a caller-supplied header, under any of the
        exact/`KBC_`/`X-` name spellings `_read_options` accepts -- see the "Security hardening"
        RFC increment.
        """
        return dataclasses.replace(self, **self._read_options(headers, allowed_fields=self._HEADER_ELIGIBLE_FIELDS))

    def __repr__(self) -> str:
        params: list[str] = []
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if value:
                if any(kw in f.name for kw in ('token', 'password', 'secret', 'key', 'dsn')):
                    params.append(f"{f.name}='****'")
                else:
                    if isinstance(value, str):
                        params.append(f"{f.name}='{value}'")
                    else:
                        params.append(f'{f.name}={value}')
            else:
                params.append(f'{f.name}=None')
        joined_params = ', '.join(params)
        return f'Config({joined_params})'


def get_env_storage_api_url(env: Mapping[str, str] | None = None) -> str | None:
    """
    Returns the Storage API URL of this server's own Keboola stack as described by the process
    environment ('KBC_STORAGE_API_URL', falling back to 'HOSTNAME_SUFFIX').

    This is only the environment-derived input; the authoritative "which stack is mine?" value is
    `Config.storage_api_url` of the server's own configuration, which `create_server()` builds from
    the '--api-url' CLI parameter and from this function (see `ServerState.own_stack_storage_api_url`,
    the single value that all the stack checks use). The URL is normalized the same way as
    `Config.storage_api_url`, so the two are directly comparable.

    :param env: The environment mapping to read from; defaults to `os.environ`.
    :return: The Storage API URL configured in the environment, or None when there is none.
    """
    env = os.environ if env is None else env
    if storage_api_url := env.get('KBC_STORAGE_API_URL'):
        return Config(storage_api_url=storage_api_url).storage_api_url
    if hostname_suffix := env.get('HOSTNAME_SUFFIX'):
        return f'https://connection.{hostname_suffix}'
    return None


# The ports that are implied by a scheme, so that 'https://connection.keboola.com' and
# 'https://connection.keboola.com:443' are recognized as the very same endpoint.
_DEFAULT_PORTS: Mapping[str, int] = {'http': 80, 'https': 443}


def _stack_identity(url: str) -> tuple[str, int | None] | None:
    """
    Returns the (host, port) pair that identifies the Keboola stack addressed by the input URL,
    or None when the URL cannot identify a stack.

    The port is dropped when it is the default port of the URL's scheme, so that the two spellings
    of the same endpoint ('https://host' and 'https://host:443') yield the same identity, while a
    genuinely different port ('https://host:8443') does not. This mirrors the normalization that
    `KeboolaClient.__init__()` applies when it builds its own Storage API URL.

    :param url: The URL to identify.
    :return: The (host, port) pair, or None for a URL with no host or with user info in it.
    """
    parsed = urlparse(url)
    if parsed.username or parsed.password:
        # A URL such as 'https://connection.keboola.com@example.com' addresses 'example.com', and
        # 'https://attacker@connection.keboola.com' carries credentials that our URLs never have.
        return None
    if not parsed.hostname:
        return None
    port = parsed.port  # raises ValueError when the port is not a number
    if port is not None and port == _DEFAULT_PORTS.get(parsed.scheme.lower()):
        port = None
    return parsed.hostname, port


def is_same_stack(url: str | None, other_url: str | None) -> bool:
    """
    Tells whether two Keboola URLs point to the very same host.

    The comparison is an exact host (and port) match, with the scheme's default port normalized
    away. No prefix, suffix or pattern matching is involved, so hosts that merely look alike
    (e.g. 'connection.keboola.com.example.com' or 'connection.example.com') never compare equal.
    URLs carrying user info are never considered equal to anything, and a missing or unparsable
    URL always compares as different.

    :param url: The first URL to compare.
    :param other_url: The second URL to compare.
    :return: True if both URLs are set and address the same host and port.
    """
    if not url or not other_url:
        return False
    try:
        identity = _stack_identity(url)
        other_identity = _stack_identity(other_url)
    except ValueError:
        # Raised by urlparse() when e.g. the port is not a number.
        return False
    if identity is None or other_identity is None:
        return False
    return identity == other_identity


@dataclass(frozen=True)
class ServerRuntimeInfo:
    """Server runtime Information."""

    transport: Transport
    """Transport used by the MCP server (e.g., 'stdio', 'streamable-http')."""
    server_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    """The ID of the MCP server."""
    app_env: str = field(default_factory=lambda: os.getenv('APP_ENV') or 'local')
    """The environment of the MCP server application."""
    app_version: str = field(default_factory=lambda: os.getenv('APP_VERSION') or 'DEV')
    """The version of the MCP server application."""
    server_version: str = importlib.metadata.version('keboola_mcp_server')
    """The version of the Keboola MCP server library."""
    mcp_library_version: str = importlib.metadata.version('mcp')
    """The version of the MCP library."""
    fastmcp_library_version: str = importlib.metadata.version('fastmcp')
    """The version of the FastMCP library."""
    stateless_http: bool = True
    """Only meaningful for streamable-http: whether the transport was started with the default
    stateless session mode (a fresh session per request -- required for scaled/deployed servers
    where any replica may handle any request) or `--no-stateless-http` (session pinned by
    Mcp-Session-Id, for a single local server). Ignored for stdio, which is inherently
    single-session -- see `session_state_persists`."""

    @property
    def session_state_persists(self) -> bool:
        """True when the same `ctx.session` object (and thus its `.state` dict) is reused across
        requests within one conversation: always for stdio (one process, one session, for the
        whole conversation), and for streamable-http only when started with
        `--no-stateless-http`. False for the deployed default (`--stateless-http`), where FastMCP
        hands every request a fresh session object regardless of what this server does."""
        return self.transport == 'stdio' or not self.stateless_http


def build_tracing_headers(runtime_info: ServerRuntimeInfo) -> dict[str, Any]:
    """Additional headers for requests made to Connection/downstream services, identifying this
    MCP server for tracing. Depends only on ServerRuntimeInfo, so it lives here rather than in
    mcp.py -- shared by SessionStateMiddleware and MultiProjectMiddleware's per-project client
    construction, which live in separate modules."""
    return {
        'User-Agent': (
            f'Keboola MCP Server/{runtime_info.server_version} app_env={runtime_info.app_env} '
            f'transport={runtime_info.transport}'
        ),
        'MCP-Server-Transport': runtime_info.transport or 'NA',
        'MCP-Server-Versions': (
            f'keboola-mcp-server/{runtime_info.server_version} mcp/{runtime_info.mcp_library_version} '
            f'fastmcp/{runtime_info.fastmcp_library_version}'
        ),
    }


class MetadataField:
    """
    Predefined names of Keboola metadata fields.
    """

    DESCRIPTION = 'KBC.description'
    PROJECT_DESCRIPTION = 'KBC.projectDescription'
    SHARED_DESCRIPTION = 'KBC.sharedDescription'  # set when sharing a bucket via Data Catalog

    # set for configurations created by MCP tools;
    # expected value: 'true'
    CREATED_BY_MCP = 'KBC.MCP.createdBy'

    # set for configurations updated by MCP tools;
    # the full key should end by a version number;
    # expected value: 'true'
    UPDATED_BY_MCP_PREFIX = 'KBC.MCP.updatedBy.version.'

    # Branch filtering works only for "fake development branches"
    FAKE_DEVELOPMENT_BRANCH = 'KBC.createdBy.branch.id'

    # Component lineage metadata for created/updated configuration sources
    CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id'
    CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id'
    CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id'
    UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id'
    UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id'
    UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id'

    # Folder name for organizing configurations in the UI
    CONFIGURATION_FOLDER_NAME = 'KBC.configuration.folderName'

    # Data type metadata fields
    DATATYPE_TYPE = 'KBC.datatype.type'
    DATATYPE_NULLABLE = 'KBC.datatype.nullable'
    DATATYPE_BASETYPE = 'KBC.datatype.basetype'
