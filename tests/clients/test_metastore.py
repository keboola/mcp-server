from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from keboola_mcp_server.clients.metastore import MetastoreClient


def _jsonapi_object(
    name: str,
    uuid: str,
    object_type: str = 'semantic-model',
    revision: int = 1,
    deleted_at: str | None = None,
    relationships: dict[str, Any] | None = None,
    **extra_attrs: Any,
) -> dict:
    """Build a single JSON:API resource object (inside the 'data' envelope)."""
    return {
        'type': object_type,
        'id': uuid,
        'attributes': {'name': name, **extra_attrs},
        'meta': {
            'branch': 'main',
            'name': name,
            'revision': revision,
            'schemaVersion': '1.0.0',
            'projectId': 123,
            'organizationId': '456',
            'createdAt': '2026-01-01T00:00:00Z',
            'lastUpdated': '2026-01-01T00:00:00Z',
            'deletedAt': deleted_at,
            'revisionCreatedAt': '2026-01-01T00:00:00Z',
        },
        'relationships': relationships,
    }


def _list_response(*objects: dict) -> dict:
    """Wrap objects in a JSON:API list envelope."""
    return {'data': list(objects)}


def _single_response(obj: dict) -> dict:
    """Wrap a single object in a JSON:API envelope."""
    return {'data': obj}


@pytest.mark.asyncio
async def test_list_objects_returns_meta_objects() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_list_response(_jsonapi_object('finance-core', 'u1')),
    )

    result = await client.list_objects('semantic-model')

    assert len(result) == 1
    assert result[0].id == 'u1'
    assert result[0].type == 'semantic-model'
    assert result[0].attributes['name'] == 'finance-core'
    assert result[0].meta is not None
    assert result[0].meta.revision == 1
    assert result[0].meta.project_id == 123
    client.raw_client.get.assert_awaited_once_with(  # type: ignore[attr-defined]
        endpoint='api/v1/repository/semantic-model',
        params=None,
    )


@pytest.mark.asyncio
async def test_list_objects_with_filter() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_list_response(_jsonapi_object('filtered', 'u2')),
    )

    result = await client.list_objects('semantic-model', filter_by='name=filtered')

    assert len(result) == 1
    client.raw_client.get.assert_awaited_once_with(  # type: ignore[attr-defined]
        endpoint='api/v1/repository/semantic-model',
        params={'filter': 'name=filtered'},
    )


@pytest.mark.asyncio
async def test_list_objects_with_limit_offset() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_list_response(_jsonapi_object('ds', 'd1', 'semantic-dataset')),
    )

    result = await client.list_objects('semantic-dataset', limit=10, offset=5)

    assert len(result) == 1
    client.raw_client.get.assert_awaited_once_with(  # type: ignore[attr-defined]
        endpoint='api/v1/repository/semantic-dataset',
        params={'limit': 10, 'offset': 5},
    )


@pytest.mark.asyncio
async def test_list_objects_organization_scope() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_list_response(_jsonapi_object('org-model', 'u3')),
    )

    result = await client.list_objects('semantic-model', organization_scope=True)

    assert len(result) == 1
    client.raw_client.get.assert_awaited_once_with(  # type: ignore[attr-defined]
        endpoint='api/v1/repository/semantic-model/organization',
        params=None,
    )


@pytest.mark.asyncio
async def test_create_object_calls_post_with_branch() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token', branch_id='dev')
    client.raw_client.post = AsyncMock(  # type: ignore[assignment]
        return_value=_single_response(_jsonapi_object('new-metric', 'm1', 'semantic-metric')),
    )

    created = await client.create_object(
        'semantic-metric',
        name='new-metric',
        data={'name': 'new-metric', 'modelUUID': 'u1', 'sql': 'SUM("amount")'},
    )

    assert created.id == 'm1'
    assert created.attributes['name'] == 'new-metric'
    call_args = client.raw_client.post.call_args  # type: ignore[attr-defined]
    assert call_args.kwargs['data']['branch'] == 'dev'


@pytest.mark.asyncio
async def test_create_object_default_branch() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.post = AsyncMock(  # type: ignore[assignment]
        return_value=_single_response(_jsonapi_object('obj', 'o1')),
    )

    await client.create_object('semantic-model', data={'name': 'obj'})

    call_args = client.raw_client.post.call_args  # type: ignore[attr-defined]
    assert call_args.kwargs['data']['branch'] == 'main'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('version', 'response', 'expected_endpoint'),
    [
        (
            None,
            {
                'type': 'object',
                'title': 'semantic-model',
                '$schema': 'https://json-schema.org/draft/2020-12/schema',
                'version': '1.0.0',
                'required': ['name', 'sql_dialect'],
                'properties': {'name': {'type': 'string'}},
            },
            'api/v1/schema/semantic-model',
        ),
        (
            '1.0.0',
            {
                'type': 'object',
                'title': 'semantic-model',
                'version': '1.0.0',
                'properties': {'name': {'type': 'string'}},
            },
            'api/v1/schema/semantic-model/1.0.0',
        ),
    ],
    ids=['latest', 'versioned'],
)
async def test_get_schema(
    version: str | None,
    response: dict,
    expected_endpoint: str,
) -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(return_value=response)  # type: ignore[assignment]

    schema = await client.get_schema('semantic-model', version=version)

    assert isinstance(schema, dict)
    assert schema['title'] == 'semantic-model'
    client.raw_client.get.assert_awaited_once_with(  # type: ignore[attr-defined]
        endpoint=expected_endpoint, params=None
    )


@pytest.mark.asyncio
async def test_get_object() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    obj = _jsonapi_object('my-model', 'u1', sql_dialect='Snowflake')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_single_response(obj),
    )

    result = await client.get_object('semantic-model', 'u1')

    assert result.id == 'u1'
    assert result.attributes['name'] == 'my-model'
    assert result.attributes['sql_dialect'] == 'Snowflake'


@pytest.mark.asyncio
async def test_put_object() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.put = AsyncMock(  # type: ignore[assignment]
        return_value=_single_response(_jsonapi_object('updated', 'u1', revision=2)),
    )

    result = await client.put_object('semantic-model', 'u1', name='updated', data={'name': 'updated'})

    assert result.id == 'u1'
    assert result.meta is not None
    assert result.meta.revision == 2


@pytest.mark.asyncio
async def test_patch_object() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.patch = AsyncMock(  # type: ignore[assignment]
        return_value=_single_response(_jsonapi_object('patched', 'u1', revision=2)),
    )

    result = await client.patch_object('semantic-model', 'u1', name='patched')

    assert result.id == 'u1'
    assert result.attributes['name'] == 'patched'


@pytest.mark.asyncio
async def test_list_revisions() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_list_response(
            _jsonapi_object('m', 'u1', revision=1),
            _jsonapi_object('m', 'u1', revision=2),
        ),
    )

    result = await client.list_revisions('semantic-model', filter_by='id=u1')

    assert len(result) == 2
    assert result[0].meta is not None
    assert result[1].meta is not None
    assert result[0].meta.revision == 1
    assert result[1].meta.revision == 2


@pytest.mark.asyncio
async def test_get_revision() -> None:
    client = MetastoreClient.create('https://metastore.example.com', token='test-token')
    client.raw_client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_single_response(_jsonapi_object('m', 'u1', revision=3)),
    )

    result = await client.get_revision('semantic-model', 'u1', 3)

    assert result.id == 'u1'
    assert result.meta is not None
    assert result.meta.revision == 3


def test_model_validate_allows_optional_fields_to_be_missing() -> None:
    obj = MetastoreClient._parse_object({'data': {}})

    assert obj.id is None
    assert obj.type is None
    assert obj.attributes is None
    assert obj.relationships is None
    assert obj.meta is None


def test_model_validate_maps_deleted_at_and_relationships() -> None:
    result = MetastoreClient._parse_object(
        _single_response(
            _jsonapi_object(
                'my-model',
                'u1',
                deleted_at='2026-01-02T00:00:00Z',
                relationships={'dataset': {'data': {'type': 'semantic-dataset', 'id': 'd1'}}},
            )
        )
    )

    assert result.relationships == {'dataset': {'data': {'type': 'semantic-dataset', 'id': 'd1'}}}
    assert result.meta is not None
    assert result.meta.deleted_at == '2026-01-02T00:00:00Z'
