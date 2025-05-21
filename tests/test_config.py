from typing import Mapping

import pytest

from keboola_mcp_server.config import Config


class TestConfig:
    @pytest.mark.parametrize(
        ('d', 'expected'),
        [
            (
                {'storage_token': 'foo', 'workspace_schema': 'bar'},
                Config(storage_token='foo', workspace_schema='bar'),
            ),
            (
                {'KBC_STORAGE_TOKEN': 'foo', 'KBC_WORKSPACE_SCHEMA': 'bar'},
                Config(storage_token='foo', workspace_schema='bar'),
            ),
            (
                {'foo': 'bar', 'storage_api_url': 'http://nowhere'},
                Config(storage_api_url='http://nowhere'),
            ),
        ],
    )
    def test_from_dict(self, d: Mapping[str, str], expected: Config) -> None:
        assert Config.from_dict(d) == expected

    @pytest.mark.parametrize(
        ('orig', 'd', 'expected'),
        [
            (
                Config(),
                {'storage_token': 'foo', 'workspace_schema': 'bar'},
                Config(storage_token='foo', workspace_schema='bar'),
            ),
            (
                Config(),
                {'KBC_STORAGE_TOKEN': 'foo', 'KBC_WORKSPACE_SCHEMA': 'bar'},
                Config(storage_token='foo', workspace_schema='bar'),
            ),
            (
                Config(storage_token='bar'),
                {'storage_token': 'foo', 'workspace_schema': 'bar'},
                Config(storage_token='foo', workspace_schema='bar'),
            ),
            (
                Config(storage_token='bar'),
                {'storage_token': None, 'workspace_schema': 'bar'},
                Config(workspace_schema='bar'),
            ),
        ],
    )
    def test_replace_by(self, orig: Config, d: Mapping[str, str], expected: Config) -> None:
        assert orig.replace_by(d) == expected

    def test_defaults(self) -> None:
        config = Config()
        assert config.storage_token is None
        assert config.storage_api_url == 'https://connection.keboola.com'
        assert config.workspace_schema is None
        assert config.transport == 'stdio'
        assert config.log_level == 'INFO'

    def test_no_token_password_in_repr(self) -> None:
        config = Config(storage_token='foo')
        assert str(config) == (
            'Config('
            "storage_token='****', "
            'workspace_schema=None, '
            "storage_api_url='https://connection.keboola.com', "
            "log_level='INFO', "
            "transport='stdio')"
        )

    @pytest.mark.parametrize(
        ('required_fields', 'input_params', 'expected_result'),
        [
            (['storage_token'], ['storage_token', 'KBC_STORAGE_TOKEN'], True),
            (['foo'], ['foo', 'KBC_FOO'], True),
            (['storage_token', 'foo'], ['KBC_STORAGE_TOKEN', 'foo'], True),
            (['storage_token', 'foo'], ['storage_token'], False),
        ],
    )
    def test_contains_required_fields(
        self, mocker, required_fields: list[str], input_params: list[str], expected_result: bool
    ) -> None:
        mocker.patch('keboola_mcp_server.config.Config.required_fields', return_value=required_fields)
        params = {param: 'value' for param in input_params}
        assert Config.contains_required_fields(params) == expected_result
