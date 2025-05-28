"""Configuration handling for the Keboola MCP server."""

import dataclasses
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional

LOG = logging.getLogger(__name__)
_NO_VALUE_MARKER = '__NO_VALUE_MARKER__'


@dataclass(frozen=True)
class Config:
    """Server configuration."""

    storage_api_url: Optional[str] = None
    """The URL to the Storage API."""
    storage_token: Optional[str] = field(default=None, metadata={'aliases': ['storage_api_token']})
    """The token to access the storage API using the MCP tools."""
    workspace_schema: Optional[str] = None
    """Workspace schema to access the buckets, tables and execute sql queries."""
    accept_secrets_in_url: Optional[bool] = None
    """If true, the configuration values are also read from the URL query parameters."""

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

    def __repr__(self):
        params: list[str] = []
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if value:
                if 'token' in f.name or 'password' in f.name:
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


class MetadataField(str, Enum):
    """
    Enum to hold predefined names of Keboola metadata fields
    Add others as needed
    """

    DESCRIPTION = 'KBC.description'
