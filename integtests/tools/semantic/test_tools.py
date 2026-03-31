from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urljoin

import httpx
import pytest
import pytest_asyncio
from fastmcp import Client

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.semantic.model import SemanticObjectType, SemanticSchemaDefinition
from keboola_mcp_server.tools.semantic.tools import (
    SemanticObjectTypeContext,
    SemanticSearchModelGroup,
    ValidateSemanticQueryOutput,
)
from keboola_mcp_server.workspace import WorkspaceManager

SEMANTIC_TOOLING_FEATURE = 'mcp-semantic-tooling'


@dataclass(frozen=True)
class SemanticTestSetup:
    slug: str
    model_id: str
    model_name: str
    primary_dataset_id: str
    secondary_dataset_id: str
    primary_table_id: str
    secondary_table_id: str
    primary_fqn: str
    metric_id: str
    metric_name: str
    relationship_id: str
    constraint_id: str


async def _delete_metastore_object(client: KeboolaClient, object_type: str, object_id: str) -> None:
    try:
        await client.metastore_client.delete_object(object_type, object_id)
        # Delete the soft deleted object
        await client.metastore_client.delete_object(object_type, object_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code not in (401, 403, 404):
            raise


@pytest.fixture(scope='module')
def metastore_url(storage_api_url: str) -> str:
    return storage_api_url.replace('connection.', 'metastore.', 1)


@pytest.fixture(scope='module', autouse=True)
def _require_metastore_available(
    storage_api_token: str,
    metastore_url: str,
) -> None:
    try:
        probe_url = urljoin(metastore_url, '/health-check')
        with httpx.Client(
            headers={'X-StorageApi-Token': storage_api_token},
            timeout=httpx.Timeout(3.0, connect=1.0),
        ) as client:
            response = client.get(probe_url)
        response.raise_for_status()
    except httpx.ConnectError as exc:
        pytest.skip(f'Metastore endpoint is not reachable in this environment: {exc}')
    except httpx.TimeoutException as exc:
        pytest.skip(f'Metastore endpoint timed out in this environment: {exc}')


@pytest_asyncio.fixture(autouse=True)
async def semantic_tools_enabled(keboola_client: KeboolaClient) -> None:
    token_info = await keboola_client.storage_client.verify_token()
    owner = token_info.get('owner', {})
    features = owner.get('features', []) if isinstance(owner, dict) else []
    if SEMANTIC_TOOLING_FEATURE not in features:
        pytest.skip(f'Semantic tooling feature "{SEMANTIC_TOOLING_FEATURE}" is not enabled in this environment.')


@pytest_asyncio.fixture
async def semantic_test_setup(
    keboola_client: KeboolaClient,
    unique_id: str,
    workspace_manager: WorkspaceManager,
) -> SemanticTestSetup:
    sql_dialect = await workspace_manager.get_sql_dialect()
    slug = f'it-semantic-{unique_id}'
    primary_table_id = f'in.c-it-semantic.{slug}_orders'
    secondary_table_id = f'in.c-it-semantic.{slug}_orders_aux'
    primary_fqn = f'{slug}_orders'
    secondary_fqn = f'{slug}_orders_aux'

    created_objects: list[tuple[str, str]] = []

    try:
        model_name = f'{slug} model'
        constraint_name = f'it_semantic_{unique_id}_constraint'
        model = await keboola_client.metastore_client.create_object(
            SemanticObjectType.SEMANTIC_MODEL.value,
            name=model_name,
            data={
                'name': model_name,
                'description': f'Semantic walkthrough model {slug}',
                'sql_dialect': sql_dialect,
            },
        )
        created_objects.append((SemanticObjectType.SEMANTIC_MODEL.value, model.id))

        primary_dataset = await keboola_client.metastore_client.create_object(
            SemanticObjectType.SEMANTIC_DATASET.value,
            name=f'{slug} orders',
            data={
                'name': f'{slug} orders',
                'description': f'Primary walkthrough dataset {slug}',
                'tableId': primary_table_id,
                'fqn': primary_fqn,
                'modelUUID': model.id,
            },
        )
        created_objects.append((SemanticObjectType.SEMANTIC_DATASET.value, primary_dataset.id))

        secondary_dataset = await keboola_client.metastore_client.create_object(
            SemanticObjectType.SEMANTIC_DATASET.value,
            name=f'{slug} orders aux',
            data={
                'name': f'{slug} orders aux',
                'description': f'Secondary walkthrough dataset {slug}',
                'tableId': secondary_table_id,
                'fqn': secondary_fqn,
                'modelUUID': model.id,
            },
        )
        created_objects.append((SemanticObjectType.SEMANTIC_DATASET.value, secondary_dataset.id))

        metric_name = f'{slug} total items'
        metric = await keboola_client.metastore_client.create_object(
            SemanticObjectType.SEMANTIC_METRIC.value,
            name=metric_name,
            data={
                'name': metric_name,
                'description': f'Walkthrough metric {slug}',
                'sql': 'SUM(item_count)',
                'dataset': primary_table_id,
                'modelUUID': model.id,
            },
        )
        created_objects.append((SemanticObjectType.SEMANTIC_METRIC.value, metric.id))

        relationship = await keboola_client.metastore_client.create_object(
            SemanticObjectType.SEMANTIC_RELATIONSHIP.value,
            name=f'{slug} relationship',
            data={
                'name': f'{slug} relationship',
                'modelUUID': model.id,
                'from': primary_table_id,
                'to': secondary_table_id,
                'type': 'left',
                'on': 'orders.id = orders_aux.id',
            },
        )
        created_objects.append((SemanticObjectType.SEMANTIC_RELATIONSHIP.value, relationship.id))

        constraint = await keboola_client.metastore_client.create_object(
            SemanticObjectType.SEMANTIC_CONSTRAINT.value,
            name=constraint_name,
            data={
                'name': constraint_name,
                'description': f'Walkthrough exclusion rule {slug}',
                'modelUUID': model.id,
                'constraintType': 'exclusion',
                'severity': 'warning',
                'rule': 'Do not combine both walkthrough datasets in one query.',
                'metrics': [metric_name],
                'datasets': [primary_table_id, secondary_table_id],
            },
        )
        created_objects.append((SemanticObjectType.SEMANTIC_CONSTRAINT.value, constraint.id))

        yield SemanticTestSetup(
            slug=slug,
            model_id=model.id,
            model_name=model_name,
            primary_dataset_id=primary_dataset.id,
            secondary_dataset_id=secondary_dataset.id,
            primary_table_id=primary_table_id,
            secondary_table_id=secondary_table_id,
            primary_fqn=primary_fqn,
            metric_id=metric.id,
            metric_name=metric_name,
            relationship_id=relationship.id,
            constraint_id=constraint.id,
        )
    finally:
        for object_type, object_id in reversed(created_objects):
            await _delete_metastore_object(keboola_client, object_type, object_id)


@pytest.mark.asyncio
async def test_search_semantic_context(
    mcp_client: Client,
    semantic_test_setup: SemanticTestSetup,
) -> None:
    search_result = await mcp_client.call_tool(
        'search_semantic_context',
        {
            'patterns': [semantic_test_setup.slug],
            'max_results': 20,
        },
    )
    search_payload = cast(dict[str, Any], search_result.structured_content)['result']
    search_groups = [
        SemanticSearchModelGroup.model_validate(item) for item in cast(list[dict[str, Any]], search_payload)
    ]

    assert len(search_groups) == 1
    assert search_groups[0].semantic_model_id == semantic_test_setup.model_id
    match_counts = Counter(match.object_type for match in search_groups[0].matches)
    assert match_counts == Counter(
        {
            SemanticObjectType.SEMANTIC_MODEL: 1,
            SemanticObjectType.SEMANTIC_DATASET: 2,
            SemanticObjectType.SEMANTIC_METRIC: 1,
            SemanticObjectType.SEMANTIC_RELATIONSHIP: 1,
            SemanticObjectType.SEMANTIC_CONSTRAINT: 1,
        }
    )


@pytest.mark.asyncio
async def test_get_semantic_context(
    mcp_client: Client,
    semantic_test_setup: SemanticTestSetup,
) -> None:
    context_result = await mcp_client.call_tool(
        'get_semantic_context',
        {
            'semantic_objects': [
                {'object_type': SemanticObjectType.SEMANTIC_MODEL.value, 'ids': [semantic_test_setup.model_id]},
                {
                    'object_type': SemanticObjectType.SEMANTIC_DATASET.value,
                    'ids': [semantic_test_setup.primary_dataset_id, semantic_test_setup.secondary_dataset_id],
                },
                {'object_type': SemanticObjectType.SEMANTIC_METRIC.value, 'ids': [semantic_test_setup.metric_id]},
                {
                    'object_type': SemanticObjectType.SEMANTIC_RELATIONSHIP.value,
                    'ids': [semantic_test_setup.relationship_id],
                },
                {
                    'object_type': SemanticObjectType.SEMANTIC_CONSTRAINT.value,
                    'ids': [semantic_test_setup.constraint_id],
                },
            ],
            'semantic_model_ids': [semantic_test_setup.model_id],
        },
    )
    context_payload = cast(dict[str, Any], context_result.structured_content)['result']
    contexts = [SemanticObjectTypeContext.model_validate(item) for item in cast(list[dict[str, Any]], context_payload)]
    contexts_by_type = {context.object_type: context for context in contexts}

    assert contexts_by_type[SemanticObjectType.SEMANTIC_MODEL].objects[0].id == semantic_test_setup.model_id
    dataset_context = contexts_by_type[SemanticObjectType.SEMANTIC_DATASET]
    assert {item.id for item in dataset_context.objects} == {
        semantic_test_setup.primary_dataset_id,
        semantic_test_setup.secondary_dataset_id,
    }
    assert all(hasattr(item, 'attributes') for item in dataset_context.objects)


@pytest.mark.asyncio
async def test_get_semantic_schema(
    mcp_client: Client,
) -> None:
    schema_result = await mcp_client.call_tool(
        'get_semantic_schema',
        {
            'semantic_types': [
                SemanticObjectType.SEMANTIC_DATASET.value,
                SemanticObjectType.SEMANTIC_METRIC.value,
            ]
        },
    )
    schema_payload = cast(dict[str, Any], schema_result.structured_content)['result']
    schemas = [SemanticSchemaDefinition.model_validate(item) for item in cast(list[dict[str, Any]], schema_payload)]
    schemas_by_type = {item.semantic_type: item for item in schemas}

    assert set(schemas_by_type) == {
        SemanticObjectType.SEMANTIC_DATASET,
        SemanticObjectType.SEMANTIC_METRIC,
    }
    assert isinstance(schemas_by_type[SemanticObjectType.SEMANTIC_DATASET].schema_definition, dict)
    assert isinstance(schemas_by_type[SemanticObjectType.SEMANTIC_METRIC].schema_definition, dict)
    assert schemas_by_type[SemanticObjectType.SEMANTIC_DATASET].schema_definition
    assert schemas_by_type[SemanticObjectType.SEMANTIC_METRIC].schema_definition


@pytest.mark.asyncio
async def test_validate_semantic_query(
    mcp_client: Client,
    semantic_test_setup: SemanticTestSetup,
) -> None:
    validate_result = await mcp_client.call_tool(
        'validate_semantic_query',
        {
            'sql_query': f'SELECT SUM(item_count) AS total_items FROM {semantic_test_setup.primary_fqn}',
            'semantic_model_ids': [semantic_test_setup.model_id],
            'expected_semantic_objects': [
                {
                    'object_type': SemanticObjectType.SEMANTIC_DATASET.value,
                    'ids': [semantic_test_setup.primary_dataset_id],
                },
                {
                    'object_type': SemanticObjectType.SEMANTIC_METRIC.value,
                    'ids': [semantic_test_setup.metric_id],
                },
            ],
        },
    )
    validation = ValidateSemanticQueryOutput.model_validate(validate_result.structured_content)

    assert validation.valid is True
    assert len(validation.semantic_models) == 1
    assert validation.semantic_models[0].id == semantic_test_setup.model_id
    assert validation.semantic_models[0].name == semantic_test_setup.model_name
    assert {(item.object_type, item.id) for item in validation.matched_expected_objects} == {
        (SemanticObjectType.SEMANTIC_DATASET, semantic_test_setup.primary_dataset_id),
        (SemanticObjectType.SEMANTIC_METRIC, semantic_test_setup.metric_id),
    }
    assert validation.missing_expected_objects == []
    assert validation.unexpected_detected_objects == []
    assert [dataset.id for dataset in validation.used_datasets] == [semantic_test_setup.primary_dataset_id]
    assert [metric.id for metric in validation.used_metrics] == [semantic_test_setup.metric_id]
    assert validation.matched_relationships == []
    assert validation.violations == []
    assert validation.post_execution_checks == []
