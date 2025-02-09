"""Configuration handling for the Keboola MCP server."""

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Server configuration."""

    storage_token: str
    query_proxy_url: str
    storage_api_url: str = "https://connection.keboola.com"
    _log_level: str = "INFO"

    def __init__(
        self,
        storage_token: str,
        query_proxy_url: str,
        storage_api_url: str,
    ):
        self.storage_token = storage_token
        self.query_proxy_url = query_proxy_url
        self.storage_api_url = storage_api_url
        self._log_level = os.getenv("KBC_LOG_LEVEL", "INFO")

    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""
        storage_token = os.getenv("KBC_STORAGE_TOKEN")
        if not storage_token:
            raise ValueError("KBC_STORAGE_TOKEN environment variable is required")

        query_proxy_url = os.getenv("KBC_QUERY_PROXY_URL")
        if not query_proxy_url:
            raise ValueError("KBC_QUERY_PROXY_URL environment variable is required")

        return cls(
            storage_token=storage_token,
            query_proxy_url=query_proxy_url,
            storage_api_url=os.getenv("KBC_STORAGE_API_URL", "https://connection.keboola.com"),
        )

    def validate(self) -> None:
        """Validate the configuration."""
        if not self.storage_token:
            raise ValueError("Storage token not configured")
        if not self.storage_api_url:
            raise ValueError("Storage API URL is required")
        if not self.query_proxy_url:
            raise ValueError("Query proxy URL is required")
        if self._log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"Invalid log level: {self._log_level}")

    @property
    def log_level(self) -> str:
        """Get the configured log level."""
        return self._log_level

    @log_level.setter
    def log_level(self, value: str) -> None:
        """Set the log level."""
        if value not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"Invalid log level: {value}")
        self._log_level = value
