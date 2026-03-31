import dataclasses
from typing import Mapping

import pytest

from keboola_mcp_server.config import Config, ProjectConfig


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
            if f.metadata.get('skip_options'):
                continue
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


class TestProjectConfig:
    def test_basic_creation(self) -> None:
        pc = ProjectConfig(storage_token='my-token')
        assert pc.branch_id is None
        assert not pc.has_fixed_branch

    def test_fixed_branch(self) -> None:
        pc = ProjectConfig(storage_token='my-token', branch_id='456')
        assert pc.has_fixed_branch
        assert pc.branch_id == '456'

    @pytest.mark.parametrize(
        'branch_id',
        ['', 'none', 'Null', 'Default', 'pRoDuCtIoN'],
    )
    def test_branch_id_normalization(self, branch_id: str) -> None:
        pc = ProjectConfig(storage_token='my-token', branch_id=branch_id)
        assert pc.branch_id is None
        assert not pc.has_fixed_branch


class TestConfigMPA:
    def test_legacy_mode(self) -> None:
        config = Config(storage_token='foo')
        assert not config.is_mpa_mode
        assert not config.show_project_id_param
        assert not config.show_branch_id_param

    def test_single_project_no_branch(self) -> None:
        config = Config(projects=(ProjectConfig(storage_token='tok'),))
        assert config.is_mpa_mode
        assert not config.show_project_id_param
        assert config.show_branch_id_param

    def test_single_project_with_branch(self) -> None:
        config = Config(projects=(ProjectConfig(storage_token='tok', branch_id='456'),))
        assert config.is_mpa_mode
        assert not config.show_project_id_param
        assert not config.show_branch_id_param

    def test_multi_project(self) -> None:
        config = Config(
            projects=(
                ProjectConfig(storage_token='tok1'),
                ProjectConfig(storage_token='tok2'),
            ),
        )
        assert config.is_mpa_mode
        assert config.show_project_id_param
        assert config.show_branch_id_param

    def test_multi_project_all_fixed_branches(self) -> None:
        config = Config(
            projects=(
                ProjectConfig(storage_token='tok1', branch_id='b1'),
                ProjectConfig(storage_token='tok2', branch_id='b2'),
            ),
        )
        assert config.show_project_id_param
        assert not config.show_branch_id_param

    def test_numbered_tokens_from_env(self) -> None:
        env = {
            'KBC_STORAGE_API_URL': 'https://connection.keboola.com',
            'KBC_STORAGE_TOKEN_1': 'tok1',
            'KBC_BRANCH_ID_1': '456',
            'KBC_STORAGE_TOKEN_2': 'tok2',
            'KBC_FORBID_MAIN_BRANCH_WRITES': 'true',
        }
        config = Config().replace_by(env)
        assert config.is_mpa_mode
        assert len(config.projects) == 2
        assert config.storage_api_url == 'https://connection.keboola.com'
        assert config.projects[0].storage_token == 'tok1'
        assert config.projects[0].branch_id == '456'
        assert config.projects[1].storage_token == 'tok2'
        assert config.projects[1].branch_id is None
        assert config.forbid_main_branch_writes is True

    def test_numbered_tokens_with_workspace_schema(self) -> None:
        env = {
            'KBC_STORAGE_TOKEN_1': 'tok1',
            'KBC_WORKSPACE_SCHEMA_1': 'schema1',
            'KBC_STORAGE_TOKEN_2': 'tok2',
        }
        config = Config().replace_by(env)
        assert config.projects[0].workspace_schema == 'schema1'
        assert config.projects[1].workspace_schema is None

    def test_no_numbered_tokens_stays_legacy(self) -> None:
        env = {
            'KBC_STORAGE_TOKEN': 'single-token',
            'KBC_STORAGE_API_URL': 'https://connection.keboola.com',
        }
        config = Config().replace_by(env)
        assert not config.is_mpa_mode
        assert config.storage_token == 'single-token'

    def test_numbered_tokens_sorted(self) -> None:
        env = {
            'KBC_STORAGE_TOKEN_3': 'tok3',
            'KBC_STORAGE_TOKEN_1': 'tok1',
            'KBC_STORAGE_TOKEN_2': 'tok2',
        }
        config = Config().replace_by(env)
        assert len(config.projects) == 3
        assert config.projects[0].storage_token == 'tok1'
        assert config.projects[1].storage_token == 'tok2'
        assert config.projects[2].storage_token == 'tok3'
