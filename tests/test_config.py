import dataclasses
from typing import Mapping

import pytest

from keboola_mcp_server.config import Config, get_deployment_storage_api_url, is_same_stack


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
                {'X-Conversation-ID': '1234'},
                Config(conversation_id='1234'),
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
            (Config(branch_id='foo'), {'branch-id': ''}, Config()),
            (Config(branch_id='foo'), {'branch-id': 'none'}, Config()),
            (Config(branch_id='foo'), {'branch-id': 'Null'}, Config()),
            (Config(branch_id='foo'), {'branch-id': 'Default'}, Config()),
            (Config(branch_id='foo'), {'branch-id': 'pRoDuCtIoN'}, Config()),
        ],
    )
    def test_replace_by(self, orig: Config, d: Mapping[str, str], expected: Config) -> None:
        assert orig.replace_by(d) == expected

    def test_defaults(self) -> None:
        config = Config()
        for f in dataclasses.fields(Config):
            assert getattr(config, f.name) is None, f'Expected default value for {f.name} to be None'

    def test_no_token_password_in_repr(self) -> None:
        config = Config(storage_token='foo')
        assert str(config) == (
            "Config(storage_api_url=None, storage_token='****', branch_id=None, workspace_schema=None, "
            'oauth_client_id=None, oauth_client_secret=None, '
            'oauth_server_url=None, oauth_scope=None, mcp_server_url=None, '
            'jwt_secret=None, bearer_token=None, conversation_id=None)'
        )

    @pytest.mark.parametrize(
        ('url', 'expected'),
        [
            ('foo.bar', 'https://foo.bar'),
            ('ftp://foo.bar', 'https://foo.bar'),
            ('foo.bar/v2/storage', 'https://foo.bar'),
            ('test:foo.bar/v2/storage', 'https://foo.bar'),
            ('https://foo.bar/v2/storage', 'https://foo.bar'),
            ('https://foo.bar', 'https://foo.bar'),
            ('http://localhost:8000', 'http://localhost:8000'),
            ('https://localhost:8000/foo/bar', 'https://localhost:8000'),
        ],
    )
    def test_url_field(self, url: str, expected: str) -> None:
        config = Config(
            storage_api_url=url,
            oauth_server_url=url,
            mcp_server_url=url,
        )
        assert config.storage_api_url == expected
        assert config.oauth_server_url == expected
        assert config.mcp_server_url == expected


class TestDeploymentStorageApiUrl:
    @pytest.mark.parametrize(
        ('env', 'expected'),
        [
            ({}, None),
            (
                {'HOSTNAME_SUFFIX': 'north-europe.azure.keboola.com'},
                'https://connection.north-europe.azure.keboola.com',
            ),
            ({'KBC_STORAGE_API_URL': 'https://connection.keboola.com'}, 'https://connection.keboola.com'),
            # The URL is normalized just like `Config.storage_api_url` is.
            ({'KBC_STORAGE_API_URL': 'connection.keboola.com/v2/storage'}, 'https://connection.keboola.com'),
            # An explicit URL wins over the hostname suffix.
            (
                {'KBC_STORAGE_API_URL': 'https://connection.keboola.com', 'HOSTNAME_SUFFIX': 'keboola.dev'},
                'https://connection.keboola.com',
            ),
        ],
        ids=['not_deployed', 'hostname_suffix', 'explicit_url', 'explicit_url_normalized', 'explicit_url_wins'],
    )
    def test_get_deployment_storage_api_url(self, env: Mapping[str, str], expected: str | None) -> None:
        assert get_deployment_storage_api_url(env) == expected


class TestIsSameStack:
    @pytest.mark.parametrize(
        ('url', 'other_url', 'expected'),
        [
            ('https://connection.keboola.com', 'https://connection.keboola.com', True),
            # The scheme is irrelevant, the host is what identifies the stack.
            ('http://connection.keboola.com', 'https://connection.keboola.com', True),
            # Host names are case-insensitive.
            ('https://CONNECTION.Keboola.com', 'https://connection.keboola.com', True),
            ('http://localhost:8000', 'http://localhost:8000', True),
            # Another Keboola stack is not this stack.
            ('https://connection.north-europe.azure.keboola.com', 'https://connection.keboola.com', False),
            # Look-alike host names must not match ...
            ('https://connection.keboola.com.example.com', 'https://connection.keboola.com', False),
            ('https://connection.example.com', 'https://connection.keboola.com', False),
            ('https://xconnection.keboola.com', 'https://connection.keboola.com', False),
            # ... nor may user info smuggle a foreign host in.
            ('https://connection.keboola.com@example.com', 'https://connection.keboola.com', False),
            # Different ports are different endpoints.
            ('http://localhost:8000', 'http://localhost:8001', False),
            # Missing or unusable URLs never match.
            (None, 'https://connection.keboola.com', False),
            ('https://connection.keboola.com', None, False),
            ('', '', False),
            ('not-a-url', 'https://connection.keboola.com', False),
            ('https://connection.keboola.com:not-a-port', 'https://connection.keboola.com', False),
        ],
    )
    def test_is_same_stack(self, url: str | None, other_url: str | None, expected: bool) -> None:
        assert is_same_stack(url, other_url) is expected
        # The comparison is symmetric.
        assert is_same_stack(other_url, url) is expected
