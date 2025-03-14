"""Configuration handling for the Keboola MCP server."""

import dataclasses
import logging
from dataclasses import dataclass
from typing import Mapping, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    """Server configuration."""

    storage_token: Optional[str] = None
    storage_api_url: str = "https://connection.keboola.com"
    log_level: str = "INFO"
    # Add Snowflake credentials
    snowflake_account: Optional[str] = None
    snowflake_user: Optional[str] = None
    snowflake_password: Optional[str] = None
    snowflake_warehouse: Optional[str] = None
    snowflake_database: Optional[str] = None
    snowflake_schema: Optional[str] = None
    snowflake_role: Optional[str] = None

    @classmethod
    def _read_options(cls, d: Mapping[str, str]) -> Mapping[str, str]:
        options: dict[str, str] = {}
        for f in dataclasses.fields(cls):
            if f.name in d:
                options[f.name] = d.get(f.name)
            elif (dict_name := f"KBC_{f.name.upper()}") in d:
                options[f.name] = d.get(dict_name)
        return options

    @classmethod
    def from_dict(cls, d: Mapping[str, str]) -> "Config":
        """
        Creates new `Config` instance with values read from the input mapping.
        The keys in the input mapping can either be the names of the fields in `Config` class
        or their uppercase variant prefixed with 'KBC_'.
        """
        return cls(**cls._read_options(d))

    def replace_by(self, d: Mapping[str, str]) -> "Config":
        """
        Creates new `Config` instance from the existing one by replacing the values from the input mapping.
        The keys in the input mapping can either be the names of the fields in `Config` class
        or their uppercase variant prefixed with 'KBC_'.
        """
        return dataclasses.replace(self, **self._read_options(d))

    def has_storage_config(self) -> bool:
        """Check if Storage API configuration is complete."""
        return all(
            [
                self.storage_token,
                self.storage_api_url,
            ]
        )

    def has_snowflake_config(self) -> bool:
        """Check if Snowflake configuration is complete."""
        return all(
            [
                self.snowflake_account,
                self.snowflake_user,
                self.snowflake_password,
                self.snowflake_warehouse,
                self.snowflake_database,
            ]
        )
