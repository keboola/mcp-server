"""Configuration handling for the Keboola MCP server."""

import dataclasses
import importlib.metadata
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional

LOG = logging.getLogger(__name__)
_NO_VALUE_MARKER = '__NO_VALUE_MARKER__'
Transport = Literal['stdio', 'sse', 'streamable-http', 'http-compat/sse', 'http-compat/streamable-http']


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
    accept_secrets_in_url: Optional[bool] = None
    """If true, the configuration values are also read from the URL query parameters."""
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

    def __post_init__(self) -> None:
        for f in dataclasses.fields(self):
            if 'url' not in f.name or f.name == 'accept_secrets_in_url':
                continue
            value = getattr(self, f.name)
            if value and not value.startswith(('http://', 'https://')):
                value = f'https://{value}'
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


@dataclass(frozen=True)
class ServerRuntimeInfo:
    """Server runtime Information."""

    transport: Transport
    """Transport used by the MCP server (e.g., 'stdio', 'sse', 'streamable-http')."""
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

    # set for configurations created by MCP tools;
    # expected value: 'true'
    CREATED_BY_MCP = 'KBC.MCP.createdBy'

    # set for configurations updated by MCP tools;
    # the full key should end by a version number;
    # expected value: 'true'
    UPDATED_BY_MCP_PREFIX = 'KBC.MCP.updatedBy.version.'

    # Branch filtering works only for "fake development branches"
    FAKE_DEVELOPMENT_BRANCH = 'KBC.createdBy.branch.id'

    # Data type metadata fields
    DATATYPE_TYPE = 'KBC.datatype.type'
    DATATYPE_NULLABLE = 'KBC.datatype.nullable'
