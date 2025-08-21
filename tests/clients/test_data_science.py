from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from keboola_mcp_server.clients.data_science import DataScienceClient


@pytest.mark.asyncio
async def test_tail_app_logs_with_lines_calls_get_text_with_lines() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    result = await client.tail_app_logs('app-123', since=None, lines=5)

    assert result == 'LOGS'
    client.get_text.assert_awaited_once_with(endpoint='apps/app-123/logs/tail', params={'lines': 5})


@pytest.mark.asyncio
async def test_tail_app_logs_with_lines_minimum_enforced() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    _ = await client.tail_app_logs('app-123', since=None, lines=0)

    client.get_text.assert_awaited_once_with(endpoint='apps/app-123/logs/tail', params={'lines': 1})


@pytest.mark.asyncio
async def test_tail_app_logs_with_since_calls_get_text_with_since_param() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    since = datetime.now(timezone.utc) - timedelta(days=1)
    result = await client.tail_app_logs('app-xyz', since=since, lines=None)

    assert result == 'LOGS'
    client.get_text.assert_awaited_once_with(
        endpoint='apps/app-xyz/logs/tail', params={'since': since.isoformat(timespec='microseconds')}
    )


@pytest.mark.asyncio
async def test_tail_app_logs_raises_when_both_since_and_lines_provided() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)

    with pytest.raises(ValueError, match='You cannot use both "since" and "lines"'):
        await client.tail_app_logs('app-123', since=datetime.now(timezone.utc), lines=10)


@pytest.mark.asyncio
async def test_tail_app_logs_raises_when_neither_param_provided() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)

    with pytest.raises(ValueError, match='Either "since" or "lines" must be provided.'):
        await client.tail_app_logs('app-123', since=None, lines=None)
