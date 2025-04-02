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
            (
                {"queue_api_url": "http://nowhere"},
                Config(queue_api_url="http://nowhere"),
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
        assert config.queue_api_url == "https://queue.keboola.com"
        assert config.log_level == "INFO"
        assert config.snowflake_account is None
        assert config.snowflake_user is None
        assert config.snowflake_password is None
        assert config.snowflake_warehouse is None
        assert config.snowflake_database is None
        assert config.snowflake_schema is None
        assert config.snowflake_role is None

    @pytest.mark.parametrize(
        "d, expected",
        [
            ({}, False),
            ({"storage_token": "foo"}, True),  # relies on the default value of storage_api_url
            ({"storage_token": "foo", "storage_api_url": ""}, False),
            ({"storage_token": "foo", "storage_api_url": "bar"}, True),
        ],
    )
    def test_has_storage_config(self, d: Mapping[str, str], expected: bool) -> None:
        assert Config.from_dict(d).has_storage_config() == expected

    @pytest.mark.parametrize(
        "d, expected",
        [
            ({}, False),
            ({"snowflake_account": "foo"}, False),
            ({"snowflake_account": "foo", "snowflake_user": "bar"}, False),
            (
                {"snowflake_account": "foo", "snowflake_user": "bar", "snowflake_password": "baz"},
                False,
            ),
            (
                {
                    "snowflake_account": "foo",
                    "snowflake_user": "bar",
                    "snowflake_password": "baz",
                    "snowflake_warehouse": "baf",
                },
                False,
            ),
            (
                {
                    "snowflake_account": "foo",
                    "snowflake_user": "bar",
                    "snowflake_password": "baz",
                    "snowflake_warehouse": "baf",
                    "snowflake_database": "bam",
                },
                True,
            ),
        ],
    )
    def test_has_snowflake_config(self, d: Mapping[str, str], expected: bool) -> None:
        assert Config.from_dict(d).has_snowflake_config() == expected

    def test_no_token_password_in_repr(self) -> None:
        config = Config(storage_token="foo", snowflake_password="bar", snowflake_user="baz")
        assert str(config) == (
            "Config("
            "storage_token='****', "
            "storage_api_url='https://connection.keboola.com', "
            "queue_api_url='https://queue.keboola.com', "
            "log_level='INFO', "
            "snowflake_account=None, "
            "snowflake_user='baz', "
            "snowflake_password='****', "
            "snowflake_warehouse=None, "
            "snowflake_database=None, "
            "snowflake_schema=None, "
            "snowflake_role=None)"
        )
