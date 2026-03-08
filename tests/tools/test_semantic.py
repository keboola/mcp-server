from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp import Context
from pydantic import ValidationError

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import JsonApiResource, SchemaDocument
from keboola_mcp_server.tools.semantic.model import (
    SemanticDefineAction,
    SemanticEntityType,
    SemanticFilter,
    SemanticScope,
)
from keboola_mcp_server.tools.semantic.tools import (
    semantic_define,
    semantic_discover,
    semantic_get_definition,
    semantic_query_plan,
)


def _resource(
    object_type: str,
    resource_id: str,
    data: dict,
    *,
    revision: int = 1,
    project_id: int = 22,
) -> JsonApiResource:
    return JsonApiResource.model_validate(
        {
            'type': object_type,
            'id': resource_id,
            'attributes': data,
            'meta': {
                'revision': revision,
                'projectId': project_id,
            },
        }
    )


@pytest.mark.asyncio
async def test_semantic_discover_returns_model_inventory(mcp_context_client: Context) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)

    async def list_objects(
        object_type: str,
        *,
        filter_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        simplified: bool | None = None,
        organization_scope: bool = False,
    ) -> list[JsonApiResource]:
        assert organization_scope is True
        assert filter_by is None
        assert limit is None
        assert offset is None
        assert simplified is None

        if object_type == 'semantic-model':
            return [_resource('semantic-model', 'model-1', {'name': 'finance', 'sql_dialect': 'Snowflake'})]
        if object_type == 'semantic-dataset':
            return [_resource('semantic-dataset', 'dataset-1', {'name': 'orders', 'modelUUID': 'model-1'})]
        if object_type == 'semantic-metric':
            return [_resource('semantic-metric', 'metric-1', {'name': 'revenue', 'modelUUID': 'model-1'})]
        return []

    client.metastore_client.list_objects = AsyncMock(side_effect=list_objects)

    result = await semantic_discover(mcp_context_client, scope=SemanticScope.ORGANIZATION)

    assert result.status == 'ok'
    assert len(result.models) == 1
    assert result.models[0].dataset_count == 1
    assert result.models[0].metric_count == 1
    assert result.matches == []


@pytest.mark.asyncio
async def test_semantic_get_definition_found_by_uuid(mcp_context_client: Context) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.metastore_client.get_object = AsyncMock(
        return_value=_resource(
            'semantic-metric', 'metric-1', {'name': 'revenue', 'modelUUID': 'model-1', 'sql': 'SUM(x)'}
        )
    )

    result = await semantic_get_definition(
        mcp_context_client,
        entity_type=SemanticEntityType.METRIC,
        uuid='metric-1',
    )

    assert result.defined is True
    assert result.definition is not None
    assert result.definition.name == 'revenue'
    assert result.source is not None
    assert result.source.uuid == 'metric-1'


@pytest.mark.asyncio
async def test_semantic_get_definition_missing_returns_defined_false(mcp_context_client: Context) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.metastore_client.list_objects = AsyncMock(return_value=[])

    result = await semantic_get_definition(
        mcp_context_client,
        entity_type=SemanticEntityType.METRIC,
        name='unknown_metric',
    )

    assert result.status == 'ok'
    assert result.defined is False
    assert result.next_action == 'semantic_discover'


@pytest.mark.asyncio
async def test_semantic_query_plan_missing_metric(mcp_context_client: Context) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.metastore_client.list_objects = AsyncMock(return_value=[])

    result = await semantic_query_plan(mcp_context_client, metric_name='undefined_metric')

    assert result.status == 'ok'
    assert result.defined is False
    assert result.valid is False
    assert result.next_action == 'semantic_define'


@pytest.mark.asyncio
async def test_semantic_query_plan_valid_metric(mcp_context_client: Context) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)

    metric = _resource(
        'semantic-metric',
        'metric-1',
        {'name': 'revenue', 'modelUUID': 'model-1', 'sql': 'SUM(amount)', 'dataset': 'out.c-finance.orders'},
    )
    dataset = _resource(
        'semantic-dataset',
        'dataset-1',
        {
            'name': 'orders',
            'modelUUID': 'model-1',
            'tableId': 'out.c-finance.orders',
            'fields': [{'name': 'region'}, {'name': 'amount'}],
        },
    )
    relationship = _resource(
        'semantic-relationship',
        'rel-1',
        {
            'name': 'orders_to_customers',
            'modelUUID': 'model-1',
            'from': 'out.c-finance.orders',
            'to': 'out.c-finance.customers',
            'type': 'left',
            'on': 'orders.customer_id = customers.id',
        },
    )
    joined_dataset = _resource(
        'semantic-dataset',
        'dataset-2',
        {
            'name': 'customers',
            'modelUUID': 'model-1',
            'tableId': 'out.c-finance.customers',
            'fields': [{'name': 'customer_segment'}],
        },
    )

    async def list_objects(
        object_type: str,
        *,
        filter_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        simplified: bool | None = None,
        organization_scope: bool = False,
    ) -> list[JsonApiResource]:
        assert filter_by is None
        assert limit is None
        assert offset is None
        assert simplified is None
        assert organization_scope is False

        if object_type == 'semantic-metric':
            return [metric]
        if object_type == 'semantic-dataset':
            return [dataset, joined_dataset]
        if object_type == 'semantic-relationship':
            return [relationship]
        if object_type == 'semantic-constraint':
            return []
        return []

    client.metastore_client.list_objects = AsyncMock(side_effect=list_objects)

    result = await semantic_query_plan(
        mcp_context_client,
        metric_name='revenue',
        dimensions=['region', 'customer_segment'],
        filters=[SemanticFilter(field='region', operator='=', value='EMEA')],
    )

    assert result.defined is True
    assert result.valid is True
    assert result.plan is not None
    assert sorted(result.plan.resolved_dimensions) == ['customer_segment', 'region']
    assert result.plan.unresolved_dimensions == []
    assert len(result.plan.joins) == 1


@pytest.mark.asyncio
async def test_semantic_define_dry_run_create(mcp_context_client: Context) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.metastore_client.get_schema = AsyncMock(
        return_value=SchemaDocument.model_validate(
            {
                'type': 'object',
                'required': ['name', 'modelUUID', 'sql'],
                'properties': {
                    'name': {'type': 'string'},
                    'modelUUID': {'type': 'string'},
                    'sql': {'type': 'string'},
                },
                'additionalProperties': True,
            }
        )
    )
    client.metastore_client.create_object = AsyncMock()

    result = await semantic_define(
        mcp_context_client,
        action=SemanticDefineAction.CREATE,
        entity_type=SemanticEntityType.METRIC,
        name='gross_margin',
        model_id='model-1',
        data={'name': 'gross_margin', 'sql': 'SUM(revenue) - SUM(cogs)'},
        dry_run=True,
    )

    assert result.status == 'ok'
    assert result.review_required is True
    client.metastore_client.create_object.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('action', 'payload', 'expect_call', 'expected_flag'),
    [
        (
            SemanticDefineAction.PATCH,
            {'sql': 'SUM(amount) + 1'},
            'patch_object',
            'updated',
        ),
        (
            SemanticDefineAction.REPLACE,
            {'name': 'revenue', 'modelUUID': 'model-1', 'sql': 'SUM(amount)'},
            'put_object',
            'updated',
        ),
        (
            SemanticDefineAction.DELETE,
            None,
            'delete_object',
            'deleted',
        ),
        (
            SemanticDefineAction.PUBLISH,
            None,
            'patch_object',
            'published',
        ),
    ],
)
async def test_semantic_define_actions(
    mcp_context_client: Context,
    action: SemanticDefineAction,
    payload: dict | None,
    expect_call: str,
    expected_flag: str,
) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)

    metric_schema = SchemaDocument.model_validate(
        {
            'type': 'object',
            'required': ['name', 'modelUUID', 'sql'],
            'properties': {
                'name': {'type': 'string'},
                'modelUUID': {'type': 'string'},
                'sql': {'type': 'string'},
                'status': {'type': 'string', 'enum': ['draft', 'published']},
            },
            'additionalProperties': True,
        }
    )

    current = _resource(
        'semantic-metric',
        'metric-1',
        {'name': 'revenue', 'modelUUID': 'model-1', 'sql': 'SUM(amount)', 'status': 'draft'},
    )

    client.metastore_client.get_schema = AsyncMock(return_value=metric_schema)
    client.metastore_client.get_object = AsyncMock(return_value=current)
    client.metastore_client.patch_object = AsyncMock(return_value=current)
    client.metastore_client.put_object = AsyncMock(return_value=current)
    client.metastore_client.delete_object = AsyncMock(return_value=None)

    result = await semantic_define(
        mcp_context_client,
        action=action,
        entity_type=SemanticEntityType.METRIC,
        uuid='metric-1',
        data=payload,
    )

    assert getattr(result, expected_flag) is True
    assert getattr(client.metastore_client, expect_call).called


@pytest.mark.asyncio
async def test_semantic_define_schema_validation_error(mcp_context_client: Context) -> None:
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.metastore_client.get_schema = AsyncMock(
        return_value=SchemaDocument.model_validate(
            {
                'type': 'object',
                'required': ['name', 'modelUUID', 'sql'],
                'properties': {
                    'name': {'type': 'string'},
                    'modelUUID': {'type': 'string'},
                    'sql': {'type': 'string'},
                },
                'additionalProperties': True,
            }
        )
    )

    with pytest.raises((ValueError, ValidationError), match='Schema validation failed'):
        await semantic_define(
            mcp_context_client,
            action=SemanticDefineAction.CREATE,
            entity_type=SemanticEntityType.METRIC,
            data={'name': 'broken_metric'},
        )
