"""Tests for schema.py (Developer Portal API calls mocked via httpx)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from keboola_mcp_server.tools.local.schema import (
    ComponentSchemaResult,
    ComponentSearchResult,
    find_component_id,
    get_component_schema,
)


def _make_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


@pytest.mark.asyncio
async def test_get_component_schema_basic():
    payload = {
        'id': 'keboola.ex-http',
        'name': 'HTTP Extractor',
        'configurationSchema': {'type': 'object', 'properties': {'url': {'type': 'string'}}},
        'imageTag': 'keboola/ex-http:latest',
    }

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=_make_response(payload))

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient', return_value=mock_client):
        result = await get_component_schema('keboola.ex-http')

    assert isinstance(result, ComponentSchemaResult)
    assert result.component_id == 'keboola.ex-http'
    assert result.name == 'HTTP Extractor'
    assert result.image == 'keboola/ex-http:latest'
    assert result.config_schema == {'type': 'object', 'properties': {'url': {'type': 'string'}}}
    assert result.config_row_schema is None


@pytest.mark.asyncio
async def test_get_component_schema_fallback_uri():
    payload = {
        'id': 'keboola.ex-http',
        'name': 'HTTP Extractor',
        'uri': 'keboola/ex-http:fallback',
    }

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=_make_response(payload))

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient', return_value=mock_client):
        result = await get_component_schema('keboola.ex-http')

    assert result.image == 'keboola/ex-http:fallback'


@pytest.mark.asyncio
async def test_get_component_schema_no_image():
    payload = {'id': 'keboola.ex-http', 'name': 'HTTP Extractor'}

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=_make_response(payload))

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient', return_value=mock_client):
        result = await get_component_schema('keboola.ex-http')

    assert result.image is None


@pytest.mark.asyncio
async def test_find_component_id_list_response():
    payload = [
        {
            'id': 'keboola.ex-http',
            'name': 'HTTP Extractor',
            'type': 'extractor',
            'imageTag': 'keboola/ex-http:1.0',
            'shortDescription': 'Download files via HTTP',
        },
        {'id': 'keboola.ex-ftp', 'name': 'FTP Extractor', 'type': 'extractor'},
    ]

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=_make_response(payload))

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient', return_value=mock_client):
        results = await find_component_id('extractor', limit=5)

    assert len(results) == 2
    assert all(isinstance(r, ComponentSearchResult) for r in results)
    assert results[0].component_id == 'keboola.ex-http'
    assert results[0].image == 'keboola/ex-http:1.0'
    assert results[0].description == 'Download files via HTTP'
    assert results[1].image is None


@pytest.mark.asyncio
async def test_find_component_id_dict_response_with_apps_key():
    payload = {
        'apps': [
            {'id': 'keboola.ex-aws-s3', 'name': 'AWS S3', 'type': 'extractor'},
        ]
    }

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=_make_response(payload))

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient', return_value=mock_client):
        results = await find_component_id('s3')

    assert len(results) == 1
    assert results[0].component_id == 'keboola.ex-aws-s3'
    assert results[0].name == 'AWS S3'


@pytest.mark.asyncio
async def test_find_component_id_skips_missing_id():
    payload = [
        {'name': 'No ID Component'},
        {'id': 'keboola.valid', 'name': 'Valid'},
    ]

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=_make_response(payload))

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient', return_value=mock_client):
        results = await find_component_id('valid')

    assert len(results) == 1
    assert results[0].component_id == 'keboola.valid'


@pytest.mark.asyncio
async def test_find_component_id_empty_response():
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=_make_response([]))

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient', return_value=mock_client):
        results = await find_component_id('nonexistent')

    assert results == []
