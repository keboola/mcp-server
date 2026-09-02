import dataclasses
from collections.abc import Mapping

import pytest

from keboola_mcp_server.config import Config, ServerRuntimeInfo, get_env_storage_api_url, is_same_stack


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
                # workspace_id requires the KBC_/X- prefix -- a bare `workspace_id`/`WORKSPACE_ID`
                # key must NOT be picked up (it collides with the variable Keboola injects into
                # Data App containers).
                {'storage_token': 'foo', 'workspace_id': '123'},
                Config(storage_token='foo'),
            ),
            (
                {'storage_token': 'foo', 'KBC_WORKSPACE_ID': '123'},
                Config(storage_token='foo', workspace_id='123'),
            ),
            (
                {'X-Workspace-Id': '123'},
                Config(workspace_id='123'),
            ),
            (
                # An empty value means "not provided" for workspace_id.
                {'X-Workspace-Id': ''},
                Config(),
            ),
            (
                {'foo': 'bar', 'storage_api_url': 'http://nowhere'},
                Config(storage_api_url='http://nowhere'),
            ),
            (
                {'X-Conversation-ID': '1234'},
                Config(conversation_id='1234'),
            ),
            (
                {'KBC_PROJECT_ID': '1888'},
                Config(project_id='1888'),
            ),
            (
                {'X-KBC-ProjectId': '1888'},
                Config(project_id='1888'),
            ),
            (
                {'MCP_DB_URL': 'postgresql://u:p@host/db'},
                Config(postgres_dsn='postgresql://u:p@host/db'),
            ),
            (
                {'KBC_MCP_DB_URL': 'postgresql://u:p@host/db'},
                Config(postgres_dsn='postgresql://u:p@host/db'),
            ),
            (
                {'KBC_POSTGRES_DSN': 'postgresql://u:p@host/db'},
                Config(postgres_dsn='postgresql://u:p@host/db'),
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
            (
                Config(),
                {'storage_token': 'foo', 'workspace_id': '123'},
                Config(storage_token='foo'),
            ),
            (
                Config(),
                {'storage_token': 'foo', 'KBC_WORKSPACE_ID': '123'},
                Config(storage_token='foo', workspace_id='123'),
            ),
            (
                # An empty header must not un-pin a server-configured workspace_id.
                Config(workspace_id='999'),
                {'X-Workspace-Id': ''},
                Config(workspace_id='999'),
            ),
            (
                # Unlike workspace_id, an empty workspace_schema header must still clear a
                # server default (to the falsy '', same as pre-existing main behavior) -- this is
                # the multi-user opt-out the README describes for X-Workspace-Schema, and must
                # not regress into keeping the server's pin.
                Config(workspace_schema='SERVER'),
                {'X-Workspace-Schema': ''},
                Config(workspace_schema=''),
            ),
        ],
    )
    def test_replace_by(self, orig: Config, d: Mapping[str, str], expected: Config) -> None:
        assert orig.replace_by(d) == expected

    @pytest.mark.parametrize(
        ('orig', 'd'),
        [
            # A malformed workspace_id always raises, same as any other invalid field -- it is
            # a per-request caller's job (e.g. `apply_request_config`) to decide how to turn that
            # into a clean rejection, not `replace_by`'s job to silently drop the pin (which would
            # widen an unpinned multi-tenant session's scope instead of rejecting the request).
            (Config(), {'X-Workspace-Id': 'abc'}),
            # A malformed header must not be treated any differently when it would have
            # overridden an existing server-configured pin.
            (Config(workspace_id='999'), {'X-Workspace-Id': 'abc'}),
        ],
    )
    def test_replace_by_reraises_malformed_workspace_id(self, orig: Config, d: Mapping[str, str]) -> None:
        with pytest.raises(ValueError, match='Invalid workspace_id'):
            orig.replace_by(d)

    def test_replace_by_reraises_other_errors(self) -> None:
        """A malformed value for an unrelated field (e.g. a header that fails the URL check)
        must also raise, exactly like a malformed `workspace_id` does."""
        with pytest.raises(ValueError, match='Invalid URL'):
            Config().replace_by({'X-Storage-Api-Url': '???'})

    def test_defaults(self) -> None:
        config = Config()
        for f in dataclasses.fields(Config):
            assert getattr(config, f.name) is None, f'Expected default value for {f.name} to be None'

    def test_no_token_password_in_repr(self) -> None:
        config = Config(storage_token='foo', postgres_dsn='postgresql://u:p@host/db', session_encryption_key='abc')
        assert str(config) == (
            "Config(storage_api_url=None, storage_token='****', branch_id=None, workspace_schema=None, "
            'workspace_id=None, '
            'oauth_client_id=None, oauth_client_secret=None, '
            'oauth_server_url=None, oauth_scope=None, mcp_server_url=None, '
            "jwt_secret=None, postgres_dsn='****', session_encryption_key='****', "
            'bearer_token=None, conversation_id=None, project_id=None, rls_rules_path=None)'
        )

    def test_workspace_id_must_be_numeric(self) -> None:
        with pytest.raises(ValueError, match='Invalid workspace_id'):
            Config(workspace_id='not-a-valid-id')

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


class TestReplaceByHeaders:
    """Deployment-level fields must never be settable by a per-request header, under any of the
    exact/`KBC_`/`X-` name spellings `_read_options` accepts -- see the "Security hardening" RFC
    increment (a caller-controlled `Jwt-Secret` header would otherwise let them forge their own
    `scope_token`)."""

    @pytest.mark.parametrize(
        'headers',
        [
            {'Jwt-Secret': 'attacker-chosen'},
            {'X-Jwt-Secret': 'attacker-chosen'},
            {'KBC-Jwt-Secret': 'attacker-chosen'},
            {'X-Postgres-Dsn': 'postgresql://evil'},
            {'X-Session-Encryption-Key': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='},
            {'X-Oauth-Client-Id': 'evil'},
            {'X-Oauth-Client-Secret': 'evil'},
            {'X-Oauth-Server-Url': 'https://evil.example'},
            {'X-Mcp-Server-Url': 'https://evil.example'},
            {'X-Rls-Rules-Path': '/tmp/evil.yaml'},
        ],
        ids=[
            'jwt_secret_bare',
            'jwt_secret_x_prefixed',
            'jwt_secret_kbc_prefixed',
            'postgres_dsn',
            'session_encryption_key',
            'oauth_client_id',
            'oauth_client_secret',
            'oauth_server_url',
            'mcp_server_url',
            'rls_rules_path',
        ],
    )
    def test_deployment_level_fields_are_unreachable(self, headers: Mapping[str, str]) -> None:
        config = Config(jwt_secret='real-secret', postgres_dsn='postgresql://real')
        out = config.replace_by_headers(headers)
        assert out == config  # nothing changed -- every one of these headers was ignored

    def test_rls_rules_path_from_env_and_cli(self) -> None:
        # Deployment-level: reachable from env / CLI (trusted), never from a header (Task 2 above).
        assert Config().replace_by({'KBC_RLS_RULES_PATH': '/etc/rls.yaml'}).rls_rules_path == '/etc/rls.yaml'
        assert Config(rls_rules_path='/opt/rls.yaml').rls_rules_path == '/opt/rls.yaml'

    def test_allowlisted_fields_still_work(self) -> None:
        config = Config()
        out = config.replace_by_headers(
            {
                'X-Storage-Api-Url': 'https://connection.keboola.com',
                'X-Branch-Id': '123',
                'X-Conversation-Id': 'conv-1',
            }
        )
        assert out.storage_api_url == 'https://connection.keboola.com'
        assert out.branch_id == '123'
        assert out.conversation_id == 'conv-1'

    def test_replace_by_is_unrestricted_for_trusted_input(self) -> None:
        # replace_by (env/CLI, operator-trusted) is deliberately NOT subject to the same
        # allowlist -- only replace_by_headers (untrusted per-request input) is restricted.
        config = Config()
        out = config.replace_by({'jwt_secret': 'ops-configured'})
        assert out.jwt_secret == 'ops-configured'


class TestServerRuntimeInfoSessionStatePersists:
    def test_stdio_always_persists_regardless_of_stateless_http(self) -> None:
        # stdio is one process/one session for the whole conversation -- the flag is meaningless there.
        assert ServerRuntimeInfo(transport='stdio', stateless_http=True).session_state_persists is True
        assert ServerRuntimeInfo(transport='stdio', stateless_http=False).session_state_persists is True

    def test_streamable_http_follows_stateless_http_flag(self) -> None:
        assert ServerRuntimeInfo(transport='streamable-http', stateless_http=True).session_state_persists is False
        assert ServerRuntimeInfo(transport='streamable-http', stateless_http=False).session_state_persists is True

    def test_defaults_to_stateless(self) -> None:
        # Matches the CLI's --stateless-http default (scaled/deployed-safe).
        assert ServerRuntimeInfo(transport='streamable-http').session_state_persists is False


class TestEnvStorageApiUrl:
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
    def test_get_env_storage_api_url(self, env: Mapping[str, str], expected: str | None) -> None:
        assert get_env_storage_api_url(env) == expected


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
            # ... nor may user info smuggle a foreign host in ...
            ('https://connection.keboola.com@example.com', 'https://connection.keboola.com', False),
            # ... and a URL with user info is not this stack even when its host is: our own URLs
            # never carry credentials.
            ('https://attacker@connection.keboola.com', 'https://connection.keboola.com', False),
            ('https://user:password@connection.keboola.com', 'https://connection.keboola.com', False),
            # The scheme's default port is the same endpoint as no port at all. `KeboolaClient`
            # builds its Storage API URL without a port, so 'KBC_STORAGE_API_URL=...:443' must
            # still be recognized as this server's own stack.
            ('https://connection.keboola.com:443', 'https://connection.keboola.com', True),
            ('http://localhost:80', 'http://localhost', True),
            # Different ports are different endpoints.
            ('https://connection.keboola.com:8443', 'https://connection.keboola.com', False),
            ('https://connection.keboola.com:8443', 'https://connection.keboola.com:443', False),
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
