"""Configuration handling for the Keboola MCP server."""

import dataclasses
import importlib.metadata
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional
from urllib.parse import urlparse, urlunparse

LOG = logging.getLogger(__name__)
_NO_VALUE_MARKER = '__NO_VALUE_MARKER__'
Transport = Literal['stdio', 'streamable-http', 'http-compat/streamable-http']


@dataclass(frozen=True)
class Config:
    """Server configuration."""

    storage_api_url: Optional[str] = None
    """The URL to the Storage API."""
    storage_token: Optional[str] = field(default=None, metadata={'aliases': ['storage_api_token']})
    """The token to access the storage API using the MCP tools."""
    branch_id: Optional[str] = None
    """The branch ID to access the storage API using the MCP tools."""
    workspace_schema: Optional[str] = None
    """Workspace schema to access the buckets, tables and execute sql queries."""
    oauth_client_id: Optional[str] = None
    """OAuth client ID registered in the Keboola OAuth Server."""
    oauth_client_secret: Optional[str] = None
    """OAuth client secret registered in the Keboola OAuth Server."""
    oauth_server_url: Optional[str] = None
    """The URL of the OAuth server to authenticate with."""
    oauth_scope: Optional[str] = None
    """The OAuth scope to request from the OAuth server."""
    mcp_server_url: Optional[str] = None
    """The URL where the MCP server si reachable."""
    jwt_secret: Optional[str] = None
    """The secret key for encoding and decoding JWT tokens."""
    bearer_token: Optional[str] = None
    """The access-token issued by Keboola OAuth server to be sent in 'Authorization: Bearer <access-token>' header."""
    conversation_id: Optional[str] = None
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
                value: Optional[str] = _NO_VALUE_MARKER

                if (dict_name := cls._normalize(name)) in data:
                    value = data[dict_name]

                elif (dict_name := cls._normalize(f'KBC_{name}')) in data:
                    # environment variables start with KBC_
                    value = data[dict_name]

                elif (dict_name := cls._normalize(f'X-{name}')) in data:
                    # HTTP headers start with X-
                    value = data[dict_name]

                if value is not _NO_VALUE_MARKER:
                    if f.type is Optional[bool]:
                        options[f.name] = value.lower() in ('true', 'yes', '1')
                    elif f.type is Optional[str]:
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


def get_deployment_storage_api_url(env: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """
    Returns the Storage API URL of the Keboola stack that this server instance is deployed on.

    The value is derived exclusively from the process environment ('KBC_STORAGE_API_URL', falling
    back to 'HOSTNAME_SUFFIX') — the same sources that `create_server()` uses when starting the
    server. HTTP headers and other per-request inputs can never influence it, which makes it usable
    as the trusted answer to "which stack is mine?". The URL is normalized the same way as
    `Config.storage_api_url`, so the two are directly comparable.

    :param env: The environment mapping to read from; defaults to `os.environ`.
    :return: The Storage API URL of this server's own stack, or None when the server is not
        configured with a stack of its own (typically a locally run server).
    """
    env = os.environ if env is None else env
    if storage_api_url := env.get('KBC_STORAGE_API_URL'):
        return Config(storage_api_url=storage_api_url).storage_api_url
    if hostname_suffix := env.get('HOSTNAME_SUFFIX'):
        return f'https://connection.{hostname_suffix}'
    return None


def is_same_stack(url: Optional[str], other_url: Optional[str]) -> bool:
    """
    Tells whether two Keboola URLs point to the very same host.

    The comparison is an exact host (and port) match. No prefix, suffix or pattern matching is
    involved, so hosts that merely look alike (e.g. 'connection.keboola.com.example.com' or
    'connection.example.com') never compare equal. URLs carrying user info are never considered
    equal to anything, and a missing or unparsable URL always compares as different.

    :param url: The first URL to compare.
    :param other_url: The second URL to compare.
    :return: True if both URLs are set and address the same host and port.
    """
    if not url or not other_url:
        return False
    try:
        parsed = urlparse(url)
        other_parsed = urlparse(other_url)
        if parsed.username or parsed.password or other_parsed.username or other_parsed.password:
            return False
        if not parsed.hostname or not other_parsed.hostname:
            return False
        return (parsed.hostname, parsed.port) == (other_parsed.hostname, other_parsed.port)
    except ValueError:
        # Raised by urlparse() when e.g. the port is not a number.
        return False


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
