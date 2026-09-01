from __future__ import annotations

from collections.abc import Mapping

import pytest

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import MetastoreObject
from keboola_mcp_server.tools.semantic.model import SemanticObjectType


def _metastore_object(
    object_type: SemanticObjectType,
    object_id: str,
    *,
    name: str,
    attributes: Mapping[str, object] | None = None,
    meta: Mapping[str, object] | None = None,
) -> MetastoreObject:
    return MetastoreObject.model_validate(
        {
            'type': object_type.value,
            'id': object_id,
            'attributes': dict(attributes or {}),
            'meta': {'name': name, **(meta or {})},
        }
    )


@pytest.fixture
def semantic_api_objects() -> dict[SemanticObjectType, list[MetastoreObject]]:
    model_id = 'model-1'
    orders_table_id = 'in.c-main.orders'
    customers_table_id = 'in.c-main.customers'

    return {
        SemanticObjectType.SEMANTIC_MODEL: [
            _metastore_object(
                SemanticObjectType.SEMANTIC_MODEL,
                model_id,
                name='Revenue Semantic Model',
                attributes={
                    'name': 'Revenue Semantic Model',
                    'description': 'Semantic model for revenue analytics',
                    'sql_dialect': 'snowflake',
                },
            )
        ],
        SemanticObjectType.SEMANTIC_DATASET: [
            _metastore_object(
                SemanticObjectType.SEMANTIC_DATASET,
                'dataset-orders',
                name='Orders',
                attributes={
                    'name': 'Orders',
                    'tableId': orders_table_id,
                    'fqn': 'analytics.orders',
                    'description': 'Fact table with order level data',
                    'modelUUID': model_id,
                },
            ),
            _metastore_object(
                SemanticObjectType.SEMANTIC_DATASET,
                'dataset-customers',
                name='Customers',
                attributes={
                    'name': 'Customers',
                    'tableId': customers_table_id,
                    'fqn': 'analytics.customers',
                    'description': 'Customer dimension',
                    'modelUUID': model_id,
                },
            ),
        ],
        SemanticObjectType.SEMANTIC_METRIC: [
            _metastore_object(
                SemanticObjectType.SEMANTIC_METRIC,
                'metric-revenue',
                name='Revenue',
                attributes={
                    'name': 'Revenue',
                    'sql': 'SUM(order_amount)',
                    'dataset': orders_table_id,
                    'description': 'Total revenue',
                    'modelUUID': model_id,
                },
            ),
            _metastore_object(
                SemanticObjectType.SEMANTIC_METRIC,
                'metric-order-count',
                name='Order Count',
                attributes={
                    'name': 'Order Count',
                    'sql': 'COUNT(*)',
                    'dataset': orders_table_id,
                    'description': 'Count of orders',
                    'modelUUID': model_id,
                },
            ),
        ],
        SemanticObjectType.SEMANTIC_RELATIONSHIP: [
            _metastore_object(
                SemanticObjectType.SEMANTIC_RELATIONSHIP,
                'relationship-orders-customers',
                name='Orders to Customers',
                attributes={
                    'name': 'Orders to Customers',
                    'from': orders_table_id,
                    'to': customers_table_id,
                    'type': 'many_to_one',
                    'on': 'orders.customer_id = customers.id',
                    'modelUUID': model_id,
                },
            )
        ],
        SemanticObjectType.SEMANTIC_CONSTRAINT: [
            _metastore_object(
                SemanticObjectType.SEMANTIC_CONSTRAINT,
                'constraint-composition',
                name='Revenue requires order count',
                attributes={
                    'name': 'Revenue requires order count',
                    'constraintType': 'composition',
                    'severity': 'warning',
                    'metrics': ['Revenue', 'Order Count'],
                    'modelUUID': model_id,
                },
            ),
            _metastore_object(
                SemanticObjectType.SEMANTIC_CONSTRAINT,
                'constraint-exclusion',
                name='Orders and Customers combination',
                attributes={
                    'name': 'Orders and Customers combination',
                    'constraintType': 'exclusion',
                    'severity': 'error',
                    'datasets': [orders_table_id, customers_table_id],
                    'modelUUID': model_id,
                },
            ),
            _metastore_object(
                SemanticObjectType.SEMANTIC_CONSTRAINT,
                'constraint-pre-query',
                name='Revenue freshness',
                attributes={
                    'name': 'Revenue freshness',
                    'constraintType': 'conditional',
                    'severity': 'warning',
                    'datasets': [orders_table_id],
                    'modelUUID': model_id,
                    'errorMessage': 'Revenue must be checked against fresh source data.',
                    'remediation': 'Compare the report with the operational source before sharing it.',
                    'ai': {'preQueryCheck': True},
                    'validationQuery': {'default': 'SELECT 1'},
                },
            ),
            _metastore_object(
                SemanticObjectType.SEMANTIC_CONSTRAINT,
                'constraint-post-query',
                name='Revenue threshold',
                attributes={
                    'name': 'Revenue threshold',
                    'constraintType': 'inequality',
                    'severity': 'warning',
                    'metrics': ['Revenue'],
                    'modelUUID': model_id,
                    'validationQuery': {'snowflake': 'SELECT * FROM revenue_threshold_check'},
                },
            ),
        ],
        SemanticObjectType.SEMANTIC_GLOSSARY: [
            _metastore_object(
                SemanticObjectType.SEMANTIC_GLOSSARY,
                'glossary-revenue',
                name='Revenue glossary',
                attributes={
                    'term': 'Revenue',
                    'definition': 'Revenue recognized from completed orders',
                    'modelUUID': model_id,
                },
            )
        ],
    }


@pytest.fixture
def mock_semantic_api(
    keboola_client: KeboolaClient,
    semantic_api_objects: dict[SemanticObjectType, list[MetastoreObject]],
) -> dict[SemanticObjectType, list[MetastoreObject]]:
    async def list_objects_side_effect(
        object_type: SemanticObjectType | str,
        *,
        limit: int | None = None,
        offset: int | None = None,
        **_: object,
    ) -> list[MetastoreObject]:
        semantic_type = object_type if isinstance(object_type, SemanticObjectType) else SemanticObjectType(object_type)
        items = semantic_api_objects.get(semantic_type, [])
        start = offset or 0
        if limit is None:
            return items[start:]
        return items[start : start + limit]

    keboola_client.metastore_client.list_objects.side_effect = list_objects_side_effect
    return semantic_api_objects
