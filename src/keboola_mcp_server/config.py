"""Configuration handling for the Keboola MCP server."""

import dataclasses
import importlib.metadata
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional
from urllib.parse import urlparse, urlunparse

LOG = logging.getLogger(__name__)
_NO_VALUE_MARKER = '__NO_VALUE_MARKER__'
Transport = Literal['stdio', 'streamable-http', 'http-compat/streamable-http']


@dataclass(frozen=True)
class ProjectConfig:
    """Configuration for a single project within MPA (Multi-Project Architecture) mode.

    Only the storage_token is required. The project_id and project_name are derived
    at runtime from verify_token(). The storage_api_url is shared from the parent Config.
    """

    storage_token: str
    """The Storage API token for this project."""
    branch_id: Optional[str] = None
    """The branch ID. If set, it is locked and cannot be changed per tool call."""
    workspace_schema: Optional[str] = None
    """Workspace schema for SQL queries."""

    def __post_init__(self) -> None:
        if self.branch_id is not None and self.branch_id.lower() in ['', 'none', 'null', 'default', 'production']:
            object.__setattr__(self, 'branch_id', None)

    @property
    def has_fixed_branch(self) -> bool:
        """Returns True if the branch_id is set (locked) in the config."""
        return self.branch_id is not None


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

    # MPA (Multi-Project Architecture) fields
    projects: tuple[ProjectConfig, ...] = field(default=(), metadata={'skip_options': True})
    """Project configurations for MPA mode. Empty tuple means legacy single-project mode."""
    forbid_main_branch_writes: bool = field(default=False, metadata={'skip_options': True})
    """Global default: if True, write operations on the main branch are forbidden."""

    @property
    def is_mpa_mode(self) -> bool:
        """Returns True if the server is running in Multi-Project Architecture mode."""
        return len(self.projects) > 0

    @property
    def show_project_id_param(self) -> bool:
        """Returns True if tools should show the project_id parameter."""
        return len(self.projects) > 1

    @property
    def show_branch_id_param(self) -> bool:
        """Returns True if tools should show the branch_id parameter."""
        return self.is_mpa_mode and any(not p.has_fixed_branch for p in self.projects)

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
            if f.metadata.get('skip_options'):
                continue
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

        Also detects numbered tokens (KBC_STORAGE_TOKEN_1, KBC_STORAGE_TOKEN_2, ...) for MPA mode.
        """
        options = dict(self._read_options(d))
        projects = self._read_numbered_projects(d)
        if projects:
            options['projects'] = tuple(projects)
        forbid = self._read_forbid_main_branch_writes(d)
        if forbid is not None:
            options['forbid_main_branch_writes'] = forbid
        return dataclasses.replace(self, **options)

    @staticmethod
    def _read_numbered_projects(d: Mapping[str, str]) -> list[ProjectConfig]:
        """
        Scans the input mapping for numbered storage tokens (KBC_STORAGE_TOKEN_1, KBC_STORAGE_TOKEN_2, ...)
        and builds ProjectConfig entries from them.

        For each numbered token N, also looks for optional KBC_BRANCH_ID_N and KBC_WORKSPACE_SCHEMA_N.
        """
        # Build a case-insensitive lookup
        lower_map = {k.lower(): v for k, v in d.items()}

        # Find all numbered tokens
        token_pattern = re.compile(r'^kbc_storage(?:_api)?_token_(\d+)$')
        numbered: dict[int, str] = {}
        for key_lower, value in lower_map.items():
            m = token_pattern.match(key_lower)
            if m and value:
                numbered[int(m.group(1))] = value

        if not numbered:
            return []

        projects: list[ProjectConfig] = []
        for n in sorted(numbered.keys()):
            token = numbered[n]
            branch_id = lower_map.get(f'kbc_branch_id_{n}')
            workspace_schema = lower_map.get(f'kbc_workspace_schema_{n}')
            projects.append(
                ProjectConfig(
                    storage_token=token,
                    branch_id=branch_id,
                    workspace_schema=workspace_schema,
                )
            )

        return projects

    @staticmethod
    def _read_forbid_main_branch_writes(d: Mapping[str, str]) -> bool | None:
        """Reads the KBC_FORBID_MAIN_BRANCH_WRITES env var."""
        lower_map = {k.lower(): v for k, v in d.items()}
        value = lower_map.get('kbc_forbid_main_branch_writes')
        if value is not None:
            return value.lower() in ('true', 'yes', '1')
        return None

    def __repr__(self) -> str:
        params: list[str] = []
        for f in dataclasses.fields(self):
            if f.metadata.get('skip_options'):
                continue
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
        if self.is_mpa_mode:
            params.append(f'projects=({len(self.projects)} projects)')
            params.append(f'forbid_main_branch_writes={self.forbid_main_branch_writes}')
        joined_params = ', '.join(params)
        return f'Config({joined_params})'


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

    # Data type metadata fields
    DATATYPE_TYPE = 'KBC.datatype.type'
    DATATYPE_NULLABLE = 'KBC.datatype.nullable'
    DATATYPE_BASETYPE = 'KBC.datatype.basetype'
