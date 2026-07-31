"""Configuration handling for the Keboola MCP server."""

import dataclasses
import importlib.metadata
import logging
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlparse, urlunparse

LOG = logging.getLogger(__name__)
_NO_VALUE_MARKER = '__NO_VALUE_MARKER__'
Transport = Literal['stdio', 'streamable-http', 'http-compat/streamable-http']


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
    bearer_token: str | None = None
    """The access-token issued by Keboola OAuth server to be sent in 'Authorization: Bearer <access-token>' header."""
    conversation_id: str | None = None
    """The ID of the ongoing conversation with the MCP server. This is supplied only by the HTTP header."""

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

    @staticmethod
    def _normalize(name: str) -> str:
        """Removes dashes and underscores from the input string and turns it into lowercase."""
        return name.lower().replace('_', '').replace('-', '')

    @classmethod
    def _read_options(cls, d: Mapping[str, str]) -> Mapping[str, Any]:
        data = {cls._normalize(k): v for k, v, in d.items()}
        options: dict[str, Any] = {}
        for f in dataclasses.fields(cls):
            field_names = [f.name] + f.metadata.get('aliases', [])

            for name in field_names:
                value: str | None = _NO_VALUE_MARKER

                if (dict_name := cls._normalize(name)) in data:
                    value = data[dict_name]

                elif (dict_name := cls._normalize(f'KBC_{name}')) in data:
                    # environment variables start with KBC_
                    value = data[dict_name]

                elif (dict_name := cls._normalize(f'X-{name}')) in data:
                    # HTTP headers start with X-
                    value = data[dict_name]

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
        """
        return dataclasses.replace(self, **self._read_options(d))

    def __repr__(self) -> str:
        params: list[str] = []
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if value:
                if 'token' in f.name or 'password' in f.name or 'secret' in f.name:
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
