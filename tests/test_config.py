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
                {'X-Storage_Token': 'foo', 'KBC_WORKSPACE_SCHEMA': 'bar'},
                Config(storage_token='foo', workspace_schema='bar'),
            ),
            (
                {'X-StorageApi_Token': 'foo', 'KBC_WORKSPACE_SCHEMA': 'bar'},
                Config(storage_token='foo', workspace_schema='bar'),
            ),
            (
                {'foo': 'bar', 'storage_api_url': 'http://nowhere'},
                Config(storage_api_url='http://nowhere'),
            ),
            (
                {'accept_secrets_in_url': 'true'},
                Config(accept_secrets_in_url=True),
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
        assert config.storage_api_url is None
        assert config.workspace_schema is None
        assert config.accept_secrets_in_url is None

    def test_no_token_password_in_repr(self) -> None:
        config = Config(storage_token='foo')
        assert str(config) == ("Config(storage_api_url=None, storage_token='****', workspace_schema=None, "
                               'accept_secrets_in_url=None, oauth_client_id=None, oauth_client_secret=None, '
                               'oauth_server_url=None, oauth_scope=None, mcp_server_url=None, '
                               'jwt_secret=None, bearer_token=None)')
