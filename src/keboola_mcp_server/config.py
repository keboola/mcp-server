"""Configuration handling for the Keboola MCP server."""

import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Config:
    """Server configuration."""
    storage_token: str
    storage_api_url: str = "https://connection.keboola.com"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""
        token = os.getenv("KBC_STORAGE_TOKEN")
        if not token:
            raise ValueError("KBC_STORAGE_TOKEN environment variable is required")
        
        api_url = os.getenv("KBC_STORAGE_API_URL", "https://connection.keboola.com")
        if not api_url.startswith(('http://', 'https://')):
            api_url = f"https://{api_url}"
            
        log_level = os.getenv("KBC_LOG_LEVEL", "INFO")
        
        return cls(
            storage_token=token,
            storage_api_url=api_url,
            log_level=log_level
        )

    def validate(self) -> None:
        """Validate the configuration."""
        if not self.storage_token:
            raise ValueError("Storage token is required")
        if not self.storage_api_url:
            raise ValueError("Storage API URL is required")
        if self.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"Invalid log level: {self.log_level}") 