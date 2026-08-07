from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from keboola_mcp_server.cli import _run_gc_sessions, _run_migrate, parse_args


def test_parse_args_migrate() -> None:
    args = parse_args(['migrate'])
    assert args.command == 'migrate'


def test_parse_args_gc_sessions() -> None:
    args = parse_args(['gc-sessions'])
    assert args.command == 'gc-sessions'


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
