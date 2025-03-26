from typing import Mapping

import pytest

from keboola_mcp_server.config import Config


class TestConfig:
    @pytest.mark.parametrize(
        "d, expected",
        [
            (
                {"storage_token": "foo", "log_level": "DEBUG"},
                Config(storage_token="foo", log_level="DEBUG"),
            ),
            (
                {"KBC_STORAGE_TOKEN": "foo", "KBC_LOG_LEVEL": "DEBUG"},
                Config(storage_token="foo", log_level="DEBUG"),
            ),
            (
                {"foo": "bar", "storage_api_url": "http://nowhere"},
                Config(storage_api_url="http://nowhere"),
            ),
        ],
    )
    def test_from_dict(self, d: Mapping[str, str], expected: Config) -> None:
        assert Config.from_dict(d) == expected

    @pytest.mark.parametrize(
        "orig, d, expected",
        [
            (
                Config(),
                {"storage_token": "foo", "log_level": "DEBUG"},
                Config(storage_token="foo", log_level="DEBUG"),
            ),
            (
                Config(),
                {"KBC_STORAGE_TOKEN": "foo", "KBC_LOG_LEVEL": "DEBUG"},
                Config(storage_token="foo", log_level="DEBUG"),
            ),
            (
                Config(storage_token="bar"),
                {"storage_token": "foo", "log_level": "DEBUG"},
                Config(storage_token="foo", log_level="DEBUG"),
            ),
            (
                Config(storage_token="bar"),
                {"storage_token": None, "log_level": "DEBUG"},
                Config(log_level="DEBUG"),
            ),
        ],
    )
    def test_replace_by(self, orig: Config, d: Mapping[str, str], expected: Config) -> None:
        assert orig.replace_by(d) == expected

    def test_defaults(self) -> None:
        config = Config()
        assert config.storage_token is None
        assert config.storage_api_url == "https://connection.keboola.com"
        assert config.workspace_user is None
        assert config.log_level == "INFO"

    def test_no_token_password_in_repr(self) -> None:
        config = Config(storage_token="foo")
        assert str(config) == (
            "Config("
            "storage_token='****', "
            "storage_api_url='https://connection.keboola.com', "
            "workspace_user=None, "
            "log_level='INFO')"
        )
