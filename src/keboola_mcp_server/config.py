"""Configuration handling for the Keboola MCP server."""

import dataclasses
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Optional

LOG = logging.getLogger(__name__)

STORAGE_TOKEN_KEY = 'storage_token'


@dataclass(frozen=True)
class Config:
    """Server configuration."""

    storage_token: Optional[str] = None
    """Storage token to access the storage API using the MCP tools, required.
    None when the server is accessed remotely."""
    workspace_schema: Optional[str] = None
    """Workspace schema to access the buckets, tables in the storage and execute sql queries over them, not required.
    None when the server is accessed remotely."""
    storage_api_url: str = 'https://connection.keboola.com'
    """Storage API URL to access the storage API of the current stack."""
    log_level: str = 'INFO'
    """Logging level to use for the server."""
    transport: str = 'stdio'
    """Transport to use for the server. Possible values: `stdio`, `sse`, `streamable-http`."""

    @classmethod
    def _read_options(cls, d: Mapping[str, str]) -> Mapping[str, str]:
        options: dict[str, str] = {}
        for f in dataclasses.fields(cls):
            if f.name in d:
                options[f.name] = d[f.name]
            elif (dict_name := f'KBC_{f.name.upper()}') in d:
                options[f.name] = d[dict_name]
        return options

    @classmethod
    def from_dict(cls, d: Mapping[str, str]) -> 'Config':
        """
        Creates new `Config` instance with values read from the input mapping.
        The keys in the input mapping can either be the names of the fields in `Config` class
        or their uppercase variant prefixed with 'KBC_'.
        """
        return cls(**cls._read_options(d))

    @staticmethod
    def required_fields() -> list[str]:
        required_fields = [STORAGE_TOKEN_KEY]
        return required_fields

    @staticmethod
    def contains_required_fields(params: Mapping[str, str]) -> bool:
        required_fields = Config.required_fields()
        kbc_required_fields = [f'KBC_{field.upper()}' for field in required_fields]
        return all(
            any(field in params for field in disjunctive_fields)
            for disjunctive_fields in zip(required_fields, kbc_required_fields)
        )

    def replace_by(self, d: Mapping[str, str]) -> 'Config':
        """
        Creates new `Config` instance from the existing one by replacing the values from the input mapping.
        The keys in the input mapping can either be the names of the fields in `Config` class
        or their uppercase variant prefixed with 'KBC_'.
        """
        return dataclasses.replace(self, **self._read_options(d))

    def __repr__(self):
        params: list[str] = []
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if value:
                if 'token' in f.name or 'password' in f.name:
                    params.append(f"{f.name}='****'")
                else:
                    params.append(f"{f.name}='{value}'")
            else:
                params.append(f'{f.name}=None')
        joined_params = ', '.join(params)
        return f'Config({joined_params})'


class MetadataField(str, Enum):
    """
    Enum to hold predefined names of Keboola metadata fields
    Add others as needed
    """

    DESCRIPTION = 'KBC.description'
