from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from keboola_mcp_server.cli import _run_migrate, parse_args


def test_parse_args_migrate() -> None:
    args = parse_args(['migrate'])
    assert args.command == 'migrate'


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
        ):
            await _run_migrate()

        create_pool.assert_awaited_once_with('postgresql://u:p@host/db')
        pool.close.assert_awaited_once()
        assert '0001_oauth_sessions.sql' in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_no_pending_migrations_still_closes_pool(self, monkeypatch, capsys) -> None:
        monkeypatch.setenv('MCP_DB_URL', 'postgresql://u:p@host/db')
        pool = MagicMock()
        pool.close = AsyncMock()
        with (
            patch('asyncpg.create_pool', AsyncMock(return_value=pool)),
            patch('keboola_mcp_server.session_store.migrator.apply_migrations', AsyncMock(return_value=[])),
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
        ):
            with pytest.raises(RuntimeError, match='boom'):
                await _run_migrate()

        pool.close.assert_awaited_once()
