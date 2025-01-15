"""Tests for the config module."""

import os
import pytest

from keboola_mcp_server.config import Config

def test_config_from_env(monkeypatch):
    """Test creating config from environment variables."""
    monkeypatch.setenv("KBC_STORAGE_TOKEN", "test-token")
    monkeypatch.setenv("KBC_STORAGE_API_URL", "https://test.keboola.com")
    monkeypatch.setenv("KBC_LOG_LEVEL", "DEBUG")
    
    config = Config.from_env()
    assert config.storage_token == "test-token"
    assert config.storage_api_url == "https://test.keboola.com"
    assert config.log_level == "DEBUG"

def test_config_missing_token(monkeypatch):
    """Test error when storage token is missing."""
    monkeypatch.delenv("KBC_STORAGE_TOKEN", raising=False)
    
    with pytest.raises(ValueError, match="KBC_STORAGE_TOKEN environment variable is required"):
        Config.from_env()

def test_config_defaults(monkeypatch):
    """Test default values are used when optional vars not set."""
    monkeypatch.setenv("KBC_STORAGE_TOKEN", "test-token")
    monkeypatch.delenv("KBC_STORAGE_API_URL", raising=False)
    monkeypatch.delenv("KBC_LOG_LEVEL", raising=False)
    
    config = Config.from_env()
    assert config.storage_token == "test-token"
    assert config.storage_api_url == "https://connection.keboola.com"
    assert config.log_level == "INFO"

def test_config_validate():
    """Test config validation."""
    # Valid config
    config = Config(
        storage_token="test-token",
        storage_api_url="https://test.keboola.com",
        log_level="INFO"
    )
    config.validate()  # Should not raise
    
    # Invalid log level
    config.log_level = "INVALID"
    with pytest.raises(ValueError, match="Invalid log level"):
        config.validate()
        
    # Missing token
    config = Config(storage_token="", storage_api_url="https://test.keboola.com")
    with pytest.raises(ValueError, match="Storage token is required"):
        config.validate() 