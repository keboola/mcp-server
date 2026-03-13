from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from keboola_mcp_server.clients.metastore import MetastoreClient


def _jsonapi_item(name: str, resource_id: str, object_type: str = 'semantic-model') -> dict:
    return {
        'type': object_type,
        'id': resource_id,
        'attributes': {
            'uuid': resource_id,
            'name': name,
            'data': {'name': name},
            'revision': 1,
            'revisionCreatedAt': '2026-01-01T00:00:00Z',
        },
    }


@pytest.mark.asyncio
async def test_list_objects_maps_jsonapi() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value={'data': [_jsonapi_item('finance-core', 'u1')]}
    )

    first = await client.list_objects('semantic-model')

    assert len(first) == 1
    assert first[0].id == 'u1'
    assert first[0].attributes.get('name') == 'finance-core'
    client.raw_client.get.assert_awaited_once_with(  # type: ignore[attr-defined]
        endpoint='api/v1/repository/semantic-model',
        params=None,
    )


@pytest.mark.asyncio
async def test_create_object_calls_post() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.post = AsyncMock(  # type: ignore[assignment]
        return_value={'data': _jsonapi_item('new-metric', 'm1', 'semantic-metric')}
    )

    created = await client.create_object(
        'semantic-metric',
        name='new-metric',
        data={'name': 'new-metric', 'modelUUID': 'u1', 'sql': 'SUM("amount")'},
    )

    assert created.id == 'm1'
    assert created.attributes.get('name') == 'new-metric'
    client.raw_client.post.assert_awaited_once()  # type: ignore[attr-defined]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('version', 'response', 'expected_endpoint', 'expected_title', 'expected_version'),
    [
        (
            None,
            {'schema': {'title': 'semantic-model'}},
            'api/v1/schema/semantic-model',
            'semantic-model',
            None,
        ),
        (
            '1.0.0',
            {'schema': {'version': '1.0.0'}},
            'api/v1/schema/semantic-model/1.0.0',
            None,
            '1.0.0',
        ),
    ],
    ids=['latest', 'versioned'],
)
async def test_get_schema(
    version: str | None,
    response: dict,
    expected_endpoint: str,
    expected_title: str | None,
    expected_version: str | None,
) -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(return_value=response)  # type: ignore[assignment]

    schema = await client.get_schema('semantic-model', version=version)

    assert schema.title == expected_title
    assert schema.version == expected_version
    client.raw_client.get.assert_awaited_once_with(  # type: ignore[attr-defined]
        endpoint=expected_endpoint, params=None
    )


@pytest.mark.asyncio
async def test_health_check_true_when_status_ok() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value={'status': 'ok'}
    )

    health = await client.health_check()
    assert health.status == 'ok'
    assert health.is_ok is True
    client.raw_client.get.assert_awaited_once_with(endpoint='health-check', params=None)  # type: ignore[attr-defined]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('response_data', 'expected_id', 'expected_attributes'),
    [
        (
            {
                'type': 'semantic-model',
                'id': 'id-123',
                'attributes': {
                    'name': 'flat-model',
                    'sql_dialect': 'Snowflake',
                    'description': 'Flat payload',
                },
            },
            'id-123',
            {'sql_dialect': 'Snowflake', 'description': 'Flat payload'},
        ),
        (
            {
                'type': 'semantic-model',
                'id': 'id-123',
                'attributes': {
                    'uuid': 'uuid-456',
                    'name': 'bad-model',
                    'sql_dialect': 'Snowflake',
                },
            },
            'id-123',
            {'uuid': 'uuid-456'},
        ),
        (
            {
                'type': 'semantic-model',
                'attributes': {
                    'uuid': 'uuid-456',
                    'name': 'bad-model',
                    'sql_dialect': 'Snowflake',
                },
            },
            None,
            {'uuid': 'uuid-456'},
        ),
    ],
    ids=['flat-attributes', 'mismatched-id-and-uuid', 'missing-id'],
)
async def test_jsonapi_object_mapping_variants(
    response_data: dict,
    expected_id: str | None,
    expected_attributes: dict[str, str],
) -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.post = AsyncMock(return_value={'data': response_data})  # type: ignore[assignment]

    created = await client.create_object(
        'semantic-model',
        name='flat-model',
        data={'name': 'flat-model', 'sql_dialect': 'Snowflake'},
    )

    assert created.id == expected_id
    for key, value in expected_attributes.items():
        assert created.attributes.get(key) == value
