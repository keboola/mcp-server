from __future__ import annotations

import logging

import httpx
import jsonschema
import pytest

from keboola_mcp_server.clients.metastore import MetastoreClient
from keboola_mcp_server.tools.semantic.model import SemanticObjectType
from keboola_mcp_server.tools.semantic.schema import (
    SemanticConstraint,
    SemanticDataset,
    SemanticDatasetField,
    SemanticGlossary,
    SemanticMetric,
    SemanticRelationship,
)

LOG = logging.getLogger(__name__)

MODEL_UUID = '123e4567-e89b-12d3-a456-426614174000'


@pytest.fixture(scope='session')
def metastore_url(storage_api_url: str) -> str:
    return storage_api_url.replace('connection.', 'metastore.', 1)


@pytest.fixture
def metastore_client(storage_api_token: str, metastore_url: str) -> MetastoreClient:
    return MetastoreClient.create(root_url=metastore_url, token=storage_api_token)


def _skip_unauthorized(exc: httpx.HTTPStatusError) -> None:
    if exc.response.status_code == 401:
        details = ''
        try:
            details = exc.response.text[:300]
        except Exception:
            details = '<no response text>'
        LOG.warning(f'Metastore unauthorized (401) for {exc.request.url}: {details}')
        pytest.skip('Token is not authorized for configured Metastore.')


@pytest.mark.parametrize(
    ('object_type', 'instance'),
    [
        (
            SemanticObjectType.SEMANTIC_DATASET,
            SemanticDataset(
                table_id='in.c-sales.orders',
                name='orders',
                fqn='"DB"."PUBLIC"."ORDERS"',
                model_uuid=MODEL_UUID,
                grain='One row per order',
                fields=[
                    SemanticDatasetField(name='order_id', role='key', type='string'),
                    SemanticDatasetField(name='amount', role='measure', type='decimal'),
                ],
                primary_key=['order_id'],
                description='Orders fact table.',
            ),
        ),
        (
            SemanticObjectType.SEMANTIC_METRIC,
            SemanticMetric(
                model_uuid=MODEL_UUID,
                name='revenue',
                sql='SUM("amount")',
                dataset='in.c-sales.orders',
                description='Revenue from orders.',
            ),
        ),
        (
            SemanticObjectType.SEMANTIC_RELATIONSHIP,
            SemanticRelationship(
                model_uuid=MODEL_UUID,
                from_='in.c-sales.orders',
                to='in.c-sales.customers',
                on='orders.customer_id = customers.id',
                name='orders_to_customers',
                type='left',
            ),
        ),
        (
            SemanticObjectType.SEMANTIC_GLOSSARY,
            SemanticGlossary(
                model_uuid=MODEL_UUID,
                term='ARR',
                definition='Annual recurring revenue.',
                see_also=['in.c-sales.orders'],
            ),
        ),
        (
            SemanticObjectType.SEMANTIC_CONSTRAINT,
            SemanticConstraint(
                model_uuid=MODEL_UUID,
                name='profit_lte_revenue',
                constraint_type='inequality',
                rule='profit <= revenue',
                metrics=['profit', 'revenue'],
                severity='error',
                is_active=True,
                description='Profit must never exceed revenue.',
                rule_expression={
                    'left': 'profit',
                    'right': 'revenue',
                    'operator': '<=',
                },
                validation_query={'default': 'SELECT 1'},
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_semantic_schema_models_validate_against_live_metastore_schema(
    metastore_client: MetastoreClient,
    object_type: SemanticObjectType,
    instance: SemanticDataset | SemanticMetric | SemanticRelationship | SemanticGlossary | SemanticConstraint,
) -> None:

    schema = await metastore_client.get_schema(object_type.value)
    payload = instance.model_dump(mode='json', by_alias=True, exclude_none=True)

    assert schema.get('title') == object_type.value
    jsonschema.validate(payload, schema)
