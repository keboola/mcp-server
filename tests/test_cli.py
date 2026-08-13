import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from keboola_mcp_server import auth_login
from keboola_mcp_server.auth_login import TokenSet
from keboola_mcp_server.cli import (
    _local_login_fallback,
    _run_gc_sessions,
    _run_login,
    _run_logout,
    _run_migrate,
    parse_args,
)
from keboola_mcp_server.config import Config

STACK = 'https://connection.keboola.com'


@pytest.fixture
def creds_file(tmp_path, monkeypatch):
    path = tmp_path / 'creds' / 'credentials.json'
    monkeypatch.setattr(auth_login, '_CREDENTIALS_PATH', path)
    return path


def test_parse_args_migrate() -> None:
    args = parse_args(['migrate'])
    assert args.command == 'migrate'


def test_parse_args_gc_sessions() -> None:
    args = parse_args(['gc-sessions'])
    assert args.command == 'gc-sessions'


class TestLocalLoginFallback:
    """Both stdio and streamable-http go through this so a locally-run server picks up a prior
    `login`'s stored credentials instead of requiring --storage-token/KBC_STORAGE_TOKEN."""

    @pytest.mark.asyncio
    async def test_fills_in_token_from_login_store(self, monkeypatch) -> None:
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        config = Config(storage_api_url=STACK)

        result = await _local_login_fallback(config, allow_interactive=False, required=True)

        assert result.storage_token == 'kbc_at_x'
        auth_login.ensure_access_token.assert_awaited_once_with(STACK, allow_interactive=False)

    @pytest.mark.asyncio
    async def test_noop_when_token_already_set(self, monkeypatch) -> None:
        ensure = AsyncMock()
        monkeypatch.setattr(auth_login, 'ensure_access_token', ensure)
        config = Config(storage_api_url=STACK, storage_token='kbc_at_already_set')

        result = await _local_login_fallback(config, allow_interactive=False, required=True)

        assert result is config
        ensure.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_without_storage_api_url(self, monkeypatch) -> None:
        ensure = AsyncMock()
        monkeypatch.setattr(auth_login, 'ensure_access_token', ensure)
        config = Config()

        result = await _local_login_fallback(config, allow_interactive=False, required=True)

        assert result is config
        ensure.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_when_oauth_configured(self, monkeypatch) -> None:
        # Deployed server: authenticates per-session via OAuth, not a locally stored token.
        ensure = AsyncMock()
        monkeypatch.setattr(auth_login, 'ensure_access_token', ensure)
        config = Config(storage_api_url=STACK, oauth_client_id='id', oauth_client_secret='secret')

        result = await _local_login_fallback(config, allow_interactive=False, required=True)

        assert result is config
        ensure.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_required_raises_when_no_stored_session(self, monkeypatch) -> None:
        # stdio has no other way to get a token (no per-request headers) -- a missing local
        # credential there must fail server startup with the "run login" guidance.
        monkeypatch.setattr(
            auth_login, 'ensure_access_token', AsyncMock(side_effect=RuntimeError('no stored credentials'))
        )
        config = Config(storage_api_url=STACK)

        with pytest.raises(RuntimeError, match='no stored credentials'):
            await _local_login_fallback(config, allow_interactive=False, required=True)

    @pytest.mark.asyncio
    async def test_not_required_starts_without_a_token_when_no_stored_session(self, monkeypatch) -> None:
        # streamable-http/http-compat can still get a token per request via a header -- a missing
        # local credential there is a legitimate, unconfigured-on-purpose state, not a startup error
        # (regression: this used to crash the server subprocess before it could even start
        # listening, e.g. in integtests that deliberately run streamable-http with no token at all).
        monkeypatch.setattr(
            auth_login, 'ensure_access_token', AsyncMock(side_effect=RuntimeError('no stored credentials'))
        )
        config = Config(storage_api_url=STACK)

        result = await _local_login_fallback(config, allow_interactive=False, required=False)

        assert result is config
        assert result.storage_token is None


class TestRunMigrate:
    @pytest.mark.asyncio
    async def test_requires_postgres_dsn(self, monkeypatch) -> None:
        monkeypatch.delenv('MCP_DB_URL', raising=False)
        monkeypatch.delenv('KBC_POSTGRES_DSN', raising=False)
        monkeypatch.delenv('KBC_MCP_DB_URL', raising=False)
        with pytest.raises(RuntimeError, match='Postgres DSN'):
            await _run_migrate()

    @pytest.mark.asyncio
    async def test_applies_migrations_and_closes_pool(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)) as create_pool,
            patch(
                'keboola_mcp_server.session_store.migrator.apply_migrations',
                AsyncMock(return_value=['0001_oauth_sessions.sql']),
            ),
            patch(
                'keboola_mcp_server.session_store.retention.ensure_partitions',
                AsyncMock(return_value={'created': ['oauth_sessions_2026_07'], 'dropped': []}),
            ),
        ):
            await _run_migrate()

        create_pool.assert_awaited_once_with('postgresql://u:p@host/db')
        pool.close.assert_awaited_once()
        out = capsys.readouterr().out
        assert '0001_oauth_sessions.sql' in out
        assert 'oauth_sessions_2026_07' in out

    @pytest.mark.asyncio
    async def test_no_pending_migrations_still_closes_pool(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)),
            patch('keboola_mcp_server.session_store.migrator.apply_migrations', AsyncMock(return_value=[])),
            patch(
                'keboola_mcp_server.session_store.retention.ensure_partitions',
                AsyncMock(return_value={'created': [], 'dropped': []}),
            ),
        ):
            await _run_migrate()

        pool.close.assert_awaited_once()
        assert 'up to date' in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_closes_pool_even_if_migration_fails(self, monkeypatch) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)),
            patch(
                'keboola_mcp_server.session_store.migrator.apply_migrations',
                AsyncMock(side_effect=RuntimeError('boom')),
            ),
            pytest.raises(RuntimeError, match='boom'),
        ):
            await _run_migrate()

        pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_closes_pool_even_if_partition_ensure_fails(self, monkeypatch) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)),
            patch('keboola_mcp_server.session_store.migrator.apply_migrations', AsyncMock(return_value=[])),
            patch(
                'keboola_mcp_server.session_store.retention.ensure_partitions',
                AsyncMock(side_effect=RuntimeError('boom')),
            ),
            pytest.raises(RuntimeError, match='boom'),
        ):
            await _run_migrate()

        pool.close.assert_awaited_once()


class TestRunGcSessions:
    @pytest.mark.asyncio
    async def test_requires_postgres_dsn(self, monkeypatch) -> None:
        monkeypatch.delenv('MCP_DB_URL', raising=False)
        monkeypatch.delenv('KBC_POSTGRES_DSN', raising=False)
        monkeypatch.delenv('KBC_MCP_DB_URL', raising=False)
        with pytest.raises(RuntimeError, match='Postgres DSN'):
            await _run_gc_sessions()

    @pytest.mark.asyncio
    async def test_reports_created_and_dropped_partitions_and_closes_pool(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)) as create_pool,
            patch(
                'keboola_mcp_server.session_store.retention.ensure_partitions',
                AsyncMock(return_value={'created': ['oauth_sessions_2026_09'], 'dropped': ['oauth_sessions_2026_06']}),
            ),
        ):
            await _run_gc_sessions()

        create_pool.assert_awaited_once_with('postgresql://u:p@host/db')
        pool.close.assert_awaited_once()
        out = capsys.readouterr().out
        assert 'oauth_sessions_2026_09' in out
        assert 'oauth_sessions_2026_06' in out

    @pytest.mark.asyncio
    async def test_reports_none_when_nothing_changed(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)),
            patch(
                'keboola_mcp_server.session_store.retention.ensure_partitions',
                AsyncMock(return_value={'created': [], 'dropped': []}),
            ),
        ):
            await _run_gc_sessions()

        assert 'none' in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_closes_pool_even_if_it_fails(self, monkeypatch) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)),
            patch(
                'keboola_mcp_server.session_store.retention.ensure_partitions',
                AsyncMock(side_effect=RuntimeError('boom')),
            ),
            pytest.raises(RuntimeError, match='boom'),
        ):
            await _run_gc_sessions()

        pool.close.assert_awaited_once()


def _introspection(project_ids: list[int]):
    from keboola_mcp_server.auth_login import Introspection, ProjectAccess

    return Introspection(
        user_id=1, user_email='m@k.com', user_name='M', projects=[ProjectAccess(id=p) for p in project_ids]
    )


def _seed_unscoped_session(access_token: str = 'kbc_at_x') -> None:
    """Simulates what `ensure_access_token`/`perform_login` normally persist -- a session with
    no project scope chosen yet -- so `_run_login` (mocked past the actual network calls) has
    something to `load_tokens` back."""
    auth_login.save_tokens(STACK, TokenSet(access_token, 'kbc_rt', expires_at=time.time() + 3600))


class TestRunLogin:
    """`login` scopes a session at login time (Security hardening RFC increment) -- never leaves
    a local session auto-leased to everything with only a prompt-text ask-first gate."""

    @pytest.mark.asyncio
    async def test_project_ids_flag_persists_scope_without_prompting(self, creds_file, monkeypatch) -> None:
        _seed_unscoped_session()
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        with patch('builtins.input', side_effect=AssertionError('must not prompt when --project-ids is given')):
            await _run_login(STACK, project_ids_arg='18,83')

        tokens = auth_login.load_tokens(STACK)
        assert tokens.project_ids == [18, 83]
        assert tokens.read_only is False

    @pytest.mark.asyncio
    async def test_all_flag_introspects_and_scopes_to_everything(self, creds_file, monkeypatch) -> None:
        _seed_unscoped_session()
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        monkeypatch.setattr(auth_login, 'introspect_token', AsyncMock(return_value=_introspection([18, 83, 95])))
        await _run_login(STACK, all_projects=True, read_only=True)

        tokens = auth_login.load_tokens(STACK)
        assert tokens.project_ids == [18, 83, 95]
        assert tokens.read_only is True

    @pytest.mark.asyncio
    async def test_interactive_prompt_scopes_selection(self, creds_file, monkeypatch) -> None:
        _seed_unscoped_session()
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        monkeypatch.setattr(auth_login, 'introspect_token', AsyncMock(return_value=_introspection([18, 83, 95])))
        monkeypatch.setattr('sys.stdin.isatty', lambda: True)
        with patch('builtins.input', side_effect=['18,83', 'y']):
            await _run_login(STACK)

        tokens = auth_login.load_tokens(STACK)
        assert tokens.project_ids == [18, 83]
        assert tokens.read_only is True

    @pytest.mark.asyncio
    async def test_interactive_prompt_skips_project_question_with_only_one_project(
        self, creds_file, monkeypatch
    ) -> None:
        # No real choice to make with a single accessible project -- don't ask which project(s),
        # just auto-scope to it; still ask read-only.
        _seed_unscoped_session()
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        monkeypatch.setattr(auth_login, 'introspect_token', AsyncMock(return_value=_introspection([18])))
        monkeypatch.setattr('sys.stdin.isatty', lambda: True)
        with patch('builtins.input', side_effect=['y']) as mocked_input:
            await _run_login(STACK)
        assert mocked_input.call_count == 1  # only the read-only question, not a project-choice one

        tokens = auth_login.load_tokens(STACK)
        assert tokens.project_ids == [18]
        assert tokens.read_only is True

    @pytest.mark.asyncio
    async def test_non_interactive_without_scope_raises(self, creds_file, monkeypatch) -> None:
        _seed_unscoped_session()
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        monkeypatch.setattr('sys.stdin.isatty', lambda: False)
        with pytest.raises(RuntimeError, match='--project-ids'):
            await _run_login(STACK)

    @pytest.mark.asyncio
    async def test_plain_rerun_keeps_existing_persisted_scope(self, creds_file, monkeypatch) -> None:
        auth_login.save_tokens(
            STACK, TokenSet('kbc_at_old', 'kbc_rt', expires_at=time.time() + 3600, project_ids=[18], read_only=True)
        )
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_old'))
        with patch('builtins.input', side_effect=AssertionError('must not re-prompt on a plain re-run')):
            await _run_login(STACK)

        tokens = auth_login.load_tokens(STACK)
        assert tokens.project_ids == [18]
        assert tokens.read_only is True

    @pytest.mark.asyncio
    async def test_force_reruns_the_prompt_even_with_an_existing_scope(self, creds_file, monkeypatch) -> None:
        auth_login.save_tokens(
            STACK, TokenSet('kbc_at_old', 'kbc_rt', expires_at=time.time() + 3600, project_ids=[18], read_only=True)
        )
        monkeypatch.setattr(auth_login, 'forget_tokens', MagicMock(return_value=True))
        monkeypatch.setattr(
            auth_login,
            'perform_login',
            AsyncMock(return_value=TokenSet('kbc_at_new', 'kbc_rt_new', expires_at=time.time() + 3600)),
        )
        await _run_login(STACK, project_ids_arg='83', force=True)

        tokens = auth_login.load_tokens(STACK)
        assert tokens.project_ids == [83]

    @pytest.mark.asyncio
    async def test_pat_prompts_for_mfa_when_neither_given(self, creds_file, monkeypatch) -> None:
        _seed_unscoped_session()
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        lease_pat = AsyncMock(return_value='kbc_pat_new')
        monkeypatch.setattr(auth_login, 'lease_pat', lease_pat)
        with patch('getpass.getpass', side_effect=['123456']):
            await _run_login(STACK, project_ids_arg='18,83', pat=True)

        lease_pat.assert_awaited_once()
        assert lease_pat.await_args.kwargs['totp_code'] == '123456'
        assert lease_pat.await_args.kwargs['recovery_code'] is None
        assert lease_pat.await_args.kwargs['project_ids'] == [18, 83]

    @pytest.mark.asyncio
    async def test_pat_explicit_totp_skips_prompt(self, creds_file, monkeypatch) -> None:
        _seed_unscoped_session()
        monkeypatch.setattr(auth_login, 'ensure_access_token', AsyncMock(return_value='kbc_at_x'))
        lease_pat = AsyncMock(return_value='kbc_pat_new')
        monkeypatch.setattr(auth_login, 'lease_pat', lease_pat)
        with patch('getpass.getpass', side_effect=AssertionError('must not prompt when --totp is given')):
            await _run_login(STACK, project_ids_arg='18,83', pat=True, totp='654321')

        assert lease_pat.await_args.kwargs['totp_code'] == '654321'


class TestRunLogout:
    @pytest.mark.asyncio
    async def test_forgets_only_the_given_profile(self, creds_file) -> None:
        auth_login.save_tokens(STACK, TokenSet('a', 'r', expires_at=time.time() + 3600), profile='desktop')
        auth_login.save_tokens(STACK, TokenSet('b', 'r', expires_at=time.time() + 3600), profile='terminal')

        await _run_logout(STACK, profile='desktop')

        assert auth_login.load_tokens(STACK, profile='desktop') is None
        assert auth_login.load_tokens(STACK, profile='terminal') is not None
