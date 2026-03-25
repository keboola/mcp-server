from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import MetastoreObject
from keboola_mcp_server.tools.semantic.model import SemanticObjectType
from keboola_mcp_server.tools.semantic.service import (
    SemanticServiceDataTypeGroup,
    SemanticValidationServiceOutput,
    _constraint_is_relevant,
    _extract_join_columns,
    _extract_metric_column,
    _matches_sql,
    _to_semantic_service_data,
    detect_used_objects_from_context,
    evaluate_constraints_from_context,
    search_semantic_context,
    validate_semantic_query,
)


def _metastore_object(
    object_type: SemanticObjectType,
    object_id: str,
    *,
    name: str,
    attributes: Mapping[str, object] | None = None,
) -> MetastoreObject:
    return MetastoreObject.model_validate(
        {
            'type': object_type.value,
            'id': object_id,
            'attributes': dict(attributes or {}),
            'meta': {'name': name},
        }
    )


def _group_objects(
    result: SemanticValidationServiceOutput,
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    return {group.object_type: group for group in result.used_object_groups}


def _service_group(
    object_type: SemanticObjectType,
    objects: Sequence[MetastoreObject],
) -> SemanticServiceDataTypeGroup:
    return SemanticServiceDataTypeGroup(
        object_type=object_type,
        objects=[_to_semantic_service_data(object_type, item) for item in objects],
    )


def _detect_context(
    *,
    datasets: Sequence[MetastoreObject] = (),
    metrics: Sequence[MetastoreObject] = (),
    relationships: Sequence[MetastoreObject] = (),
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    context_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup] = {}
    if datasets:
        context_by_type[SemanticObjectType.SEMANTIC_DATASET] = _service_group(
            SemanticObjectType.SEMANTIC_DATASET,
            datasets,
        )
    if metrics:
        context_by_type[SemanticObjectType.SEMANTIC_METRIC] = _service_group(
            SemanticObjectType.SEMANTIC_METRIC,
            metrics,
        )
    if relationships:
        context_by_type[SemanticObjectType.SEMANTIC_RELATIONSHIP] = _service_group(
            SemanticObjectType.SEMANTIC_RELATIONSHIP,
            relationships,
        )
    return context_by_type


def _build_metastore_objects(
    object_type: SemanticObjectType,
    specs: Sequence[tuple[str, str, Mapping[str, object]]],
) -> list[MetastoreObject]:
    return [
        _metastore_object(
            object_type,
            object_id,
            name=name,
            attributes=attributes,
        )
        for object_id, name, attributes in specs
    ]


def _evaluate_context(
    *,
    model_specs: Sequence[tuple[str, str, Mapping[str, object]]] = (),
    constraint_specs: Sequence[tuple[str, str, Mapping[str, object]]] = (),
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    context_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup] = {}
    if model_specs:
        context_by_type[SemanticObjectType.SEMANTIC_MODEL] = _service_group(
            SemanticObjectType.SEMANTIC_MODEL,
            _build_metastore_objects(SemanticObjectType.SEMANTIC_MODEL, model_specs),
        )
    if constraint_specs:
        context_by_type[SemanticObjectType.SEMANTIC_CONSTRAINT] = _service_group(
            SemanticObjectType.SEMANTIC_CONSTRAINT,
            _build_metastore_objects(SemanticObjectType.SEMANTIC_CONSTRAINT, constraint_specs),
        )
    return context_by_type


def _used_object_groups(
    *,
    dataset_specs: Sequence[tuple[str, str, Mapping[str, object]]] = (),
    metric_specs: Sequence[tuple[str, str, Mapping[str, object]]] = (),
    relationship_specs: Sequence[tuple[str, str, Mapping[str, object]]] = (),
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    used_groups: dict[SemanticObjectType, SemanticServiceDataTypeGroup] = {}
    if dataset_specs:
        used_groups[SemanticObjectType.SEMANTIC_DATASET] = _service_group(
            SemanticObjectType.SEMANTIC_DATASET,
            _build_metastore_objects(SemanticObjectType.SEMANTIC_DATASET, dataset_specs),
        )
    if metric_specs:
        used_groups[SemanticObjectType.SEMANTIC_METRIC] = _service_group(
            SemanticObjectType.SEMANTIC_METRIC,
            _build_metastore_objects(SemanticObjectType.SEMANTIC_METRIC, metric_specs),
        )
    if relationship_specs:
        used_groups[SemanticObjectType.SEMANTIC_RELATIONSHIP] = _service_group(
            SemanticObjectType.SEMANTIC_RELATIONSHIP,
            _build_metastore_objects(SemanticObjectType.SEMANTIC_RELATIONSHIP, relationship_specs),
        )
    return used_groups


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


@pytest.mark.asyncio
async def test_validate_semantic_query_detects_used_objects_and_relevant_validations(
    keboola_client: KeboolaClient,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
) -> None:
    result = await validate_semantic_query(
        keboola_client,
        (
            'SELECT SUM(order_amount) AS revenue '
            'FROM analytics.orders orders '
            'JOIN analytics.customers customers ON orders.customer_id = customers.id'
        ),
        'model-1',
    )

    assert result.valid is False
    assert result.matched_relationships == ['Orders to Customers']

    groups = _group_objects(result)
    assert [dataset.id for dataset in groups[SemanticObjectType.SEMANTIC_DATASET].objects] == [
        'dataset-orders',
        'dataset-customers',
    ]
    assert [metric.id for metric in groups[SemanticObjectType.SEMANTIC_METRIC].objects] == ['metric-revenue']
    assert [relationship.id for relationship in groups[SemanticObjectType.SEMANTIC_RELATIONSHIP].objects] == [
        'relationship-orders-customers'
    ]

    findings_by_id = {finding.constraint_id: finding for finding in result.violations + result.post_execution_checks}

    composition_finding = findings_by_id['constraint-composition']
    assert composition_finding.status == 'missing_metrics'
    assert 'Order Count' in composition_finding.message

    exclusion_finding = findings_by_id['constraint-exclusion']
    assert exclusion_finding.status == 'excluded_combination'
    assert exclusion_finding.severity == 'error'

    pre_query_finding = findings_by_id['constraint-pre-query']
    assert pre_query_finding.status == 'pre_query_check'
    assert pre_query_finding.validation_query == 'SELECT 1'
    assert 'Revenue must be checked against fresh source data.' in pre_query_finding.message
    assert 'Compare the report with the operational source before sharing it.' in pre_query_finding.message

    post_query_finding = findings_by_id['constraint-post-query']
    assert post_query_finding.status == 'post_query_check'
    assert post_query_finding.validation_query == 'SELECT * FROM revenue_threshold_check'


@pytest.mark.parametrize(
    ('sql_query', 'candidate', 'expected'),
    [
        ('SELECT revenue FROM analytics.orders', 'revenue', True),
        ('SELECT Revenue FROM analytics.orders', 'revenue', True),
        ('SELECT order_count FROM analytics.orders', 'order', False),
        ('SELECT preorders FROM analytics.orders', 'order', False),
        ('SELECT customer_id FROM analytics.orders', 'customer_id', True),
        ('SELECT customer_id_2 FROM analytics.orders', 'customer_id', False),
        ('SELECT analytics.orders.id FROM analytics.orders', 'analytics.orders', True),
        ('SELECT analytics.orders_backup.id FROM analytics.orders_backup', 'analytics.orders', True),
        ('SELECT SUM(order_amount) FROM analytics.orders', 'SUM(order_amount)', True),
        ('SELECT SUM(other_amount) FROM analytics.orders', 'SUM(order_amount)', False),
        ('SELECT * FROM analytics.orders', '', False),
        ('SELECT * FROM analytics.orders', '   ', False),
    ],
)
def test_matches_sql_handles_identifiers_and_substrings(
    sql_query: str,
    candidate: str,
    expected: bool,
) -> None:
    assert _matches_sql(sql_query, candidate) is expected


@pytest.mark.parametrize(
    ('sql', 'expected'),
    [
        # Simple double-quoted column.
        ('SUM("REVENUE_YTD")', 'REVENUE_YTD'),
        # Unquoted column.
        ('AVG(margin_pct)', 'margin_pct'),
        # SUM without quotes, uppercase.
        ('SUM(AMOUNT)', 'AMOUNT'),
        # COUNT(*) has no column name.
        ('COUNT(*)', None),
        # Complex expression — no match.
        ('SUM(CASE WHEN x = 1 THEN amount ELSE 0 END)', None),
        # Multi-argument — no match.
        ('COALESCE(a, b)', None),
    ],
)
def test_extract_metric_column(sql: str, expected: str | None) -> None:
    assert _extract_metric_column(sql) == expected


@pytest.mark.parametrize(
    ('on_clause', 'expected'),
    [
        # Standard Snowflake-style uppercase ON clause with alias prefixes.
        (
            'fact.FK_BUSINESS_SUBUNIT = dim.PK_BUSINESS_SUBUNIT',
            ['FK_BUSINESS_SUBUNIT', 'PK_BUSINESS_SUBUNIT'],
        ),
        # Multi-condition clause.
        (
            'fact.FK_BUSINESS_SUBUNIT = dim.FK_BUSINESS_SUBUNIT AND fact.CODE_FIN_STAT = dim.CODE_FIN_STAT',
            ['FK_BUSINESS_SUBUNIT', 'CODE_FIN_STAT'],
        ),
        # Function call + string literal — LEFT and AVG (keyword) are filtered; string literal stripped.
        (
            'fact.DIM_CURRENCY = dim.CURRENCY_FROM AND LEFT(dim.CODE_PERIOD_VALUE, 6) = fact.CODE_PERIOD_VALUE '
            "AND dim.RATE_TYPE = 'AVG'",
            ['DIM_CURRENCY', 'CURRENCY_FROM', 'CODE_PERIOD_VALUE', 'RATE_TYPE'],
        ),
        # All-lowercase on-clause returns empty list (triggers full-string fallback).
        ('orders.customer_id = customers.id', []),
        # Short tokens (< 3 chars after first letter) are excluded.
        ('fact.FK = dim.PK', []),
    ],
)
def test_extract_join_columns(on_clause: str, expected: list[str]) -> None:
    assert _extract_join_columns(on_clause) == expected


@pytest.mark.parametrize(
    ('constraint_attributes', 'used_metric_names', 'used_dataset_ids', 'expected'),
    [
        ({'metrics': ['Revenue']}, {'Revenue'}, set(), True),
        ({'metrics': ['Revenue']}, {'Order Count'}, set(), False),
        ({'datasets': ['in.c-main.orders']}, set(), {'in.c-main.orders'}, True),
        ({'datasets': ['in.c-main.orders']}, set(), {'in.c-main.customers'}, False),
        ({'metrics': [' Revenue ', '   ']}, {'Revenue'}, set(), True),
        ({'datasets': [' in.c-main.orders ', '   ']}, set(), {'in.c-main.orders'}, True),
        ({'metrics': ['Revenue'], 'datasets': ['in.c-main.orders']}, set(), {'in.c-main.orders'}, True),
        ({'metrics': ['Revenue'], 'datasets': ['in.c-main.orders']}, {'Revenue'}, set(), True),
        ({'metrics': ['Revenue'], 'datasets': ['in.c-main.orders']}, {'Other'}, {'other'}, False),
        ({}, set(), set(), True),
        ({'metrics': ['   '], 'datasets': ['   ']}, set(), set(), True),
    ],
)
def test_constraint_is_relevant_edge_cases(
    constraint_attributes: Mapping[str, object],
    used_metric_names: set[str],
    used_dataset_ids: set[str],
    expected: bool,
) -> None:
    constraint = _service_group(
        SemanticObjectType.SEMANTIC_CONSTRAINT,
        [
            _metastore_object(
                SemanticObjectType.SEMANTIC_CONSTRAINT,
                'constraint-test',
                name='Constraint Test',
                attributes={
                    'name': 'Constraint Test',
                    'modelUUID': 'model-1',
                    **constraint_attributes,
                },
            )
        ],
    ).objects[0]

    assert _constraint_is_relevant(constraint, used_metric_names, used_dataset_ids) is expected


@pytest.mark.parametrize(
    ('sql_query', 'dataset_specs', 'metric_specs', 'relationship_specs', 'expected_group_ids'),
    [
        # Nothing from the semantic context should be detected when the SQL does not reference any object.
        (
            'SELECT 1',
            [
                (
                    'dataset-orders',
                    'Orders',
                    {
                        'name': 'Orders',
                        'tableId': 'in.c-main.orders',
                        'fqn': 'analytics.orders',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [
                (
                    'metric-revenue',
                    'Revenue',
                    {
                        'name': 'Revenue',
                        'sql': 'SUM(order_amount)',
                        'dataset': 'in.c-main.orders',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [
                (
                    'relationship-orders-customers',
                    'Orders to Customers',
                    {
                        'name': 'Orders to Customers',
                        'from': 'in.c-main.orders',
                        'to': 'in.c-main.customers',
                        'on': 'orders.customer_id = customers.id',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            {},
        ),
        # Metrics are only considered after their source dataset was detected, so this metric must be skipped.
        (
            'SELECT SUM(order_amount) FROM analytics.orders',
            [
                (
                    'dataset-orders',
                    'Orders',
                    {
                        'name': 'Orders',
                        'tableId': 'in.c-main.orders',
                        'fqn': 'analytics.orders',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [
                (
                    'metric-revenue',
                    'Revenue',
                    {
                        'name': 'Revenue',
                        'sql': 'SUM(order_amount)',
                        'dataset': 'in.c-main.other',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [],
            {
                SemanticObjectType.SEMANTIC_DATASET: ['dataset-orders'],
            },
        ),
        # Both datasets are present, but the join predicate differs from the relationship definition, so no
        # relationship should be reported.
        (
            (
                'SELECT * FROM analytics.orders orders '
                'JOIN analytics.customers customers ON orders.account_id = customers.id'
            ),
            [
                (
                    'dataset-orders',
                    'Orders',
                    {
                        'name': 'Orders',
                        'tableId': 'in.c-main.orders',
                        'fqn': 'analytics.orders',
                        'modelUUID': 'model-1',
                    },
                ),
                (
                    'dataset-customers',
                    'Customers',
                    {
                        'name': 'Customers',
                        'tableId': 'in.c-main.customers',
                        'fqn': 'analytics.customers',
                        'modelUUID': 'model-1',
                    },
                ),
            ],
            [],
            [
                (
                    'relationship-orders-customers',
                    'Orders to Customers',
                    {
                        'name': 'Orders to Customers',
                        'from': 'in.c-main.orders',
                        'to': 'in.c-main.customers',
                        'on': 'orders.customer_id = customers.id',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            {
                SemanticObjectType.SEMANTIC_DATASET: ['dataset-orders', 'dataset-customers'],
            },
        ),
        # Metric SQL uses a quoted column (SUM("REVENUE_YTD")); the query writes the column with a
        # table-alias prefix (ep."REVENUE_YTD").  The old full-string match failed; the new
        # column-extraction path should detect the metric.
        (
            'SELECT SUM(ep."REVENUE_YTD") FROM "DB"."schema"."FACT_PERFORMANCE" ep',
            [
                (
                    'dataset-fact',
                    'Fact Performance',
                    {
                        'name': 'Fact Performance',
                        'tableId': 'out.c-main.FACT_PERFORMANCE',
                        'fqn': '"DB"."schema"."FACT_PERFORMANCE"',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [
                (
                    'metric-revenue-ytd',
                    'Revenue YTD',
                    {
                        'name': 'Revenue YTD',
                        'sql': 'SUM("REVENUE_YTD")',
                        'dataset': 'out.c-main.FACT_PERFORMANCE',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [],
            {
                SemanticObjectType.SEMANTIC_DATASET: ['dataset-fact'],
                SemanticObjectType.SEMANTIC_METRIC: ['metric-revenue-ytd'],
            },
        ),
        # Relationship ON clause uses uppercase Snowflake-style column names with template aliases
        # (fact./dim.); the SQL uses different aliases (o./c.).  Column-extraction should detect
        # the relationship because all column names are present in the SQL.
        (
            (
                'SELECT * FROM "DB"."s"."ORDERS" o '
                'JOIN "DB"."s"."CUSTOMERS" c ON o."FK_CUSTOMER_ID" = c."PK_CUSTOMER_ID"'
            ),
            [
                (
                    'dataset-orders',
                    'Orders',
                    {
                        'name': 'Orders',
                        'tableId': 'out.c-main.ORDERS',
                        'fqn': '"DB"."s"."ORDERS"',
                        'modelUUID': 'model-1',
                    },
                ),
                (
                    'dataset-customers',
                    'Customers',
                    {
                        'name': 'Customers',
                        'tableId': 'out.c-main.CUSTOMERS',
                        'fqn': '"DB"."s"."CUSTOMERS"',
                        'modelUUID': 'model-1',
                    },
                ),
            ],
            [],
            [
                (
                    'rel-orders-customers',
                    'Orders to Customers',
                    {
                        'name': 'Orders to Customers',
                        'from': 'out.c-main.ORDERS',
                        'to': 'out.c-main.CUSTOMERS',
                        'on': 'fact.FK_CUSTOMER_ID = dim.PK_CUSTOMER_ID',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            {
                SemanticObjectType.SEMANTIC_DATASET: ['dataset-orders', 'dataset-customers'],
                SemanticObjectType.SEMANTIC_RELATIONSHIP: ['rel-orders-customers'],
            },
        ),
        # Relationship with uppercase ON clause columns — but the actual SQL uses DIFFERENT columns
        # in the join (FK_ORDER_ID instead of FK_CUSTOMER_ID).  Should NOT detect the relationship.
        (
            ('SELECT * FROM "DB"."s"."ORDERS" o ' 'JOIN "DB"."s"."CUSTOMERS" c ON o."FK_ORDER_ID" = c."PK_ORDER_ID"'),
            [
                (
                    'dataset-orders',
                    'Orders',
                    {
                        'name': 'Orders',
                        'tableId': 'out.c-main.ORDERS',
                        'fqn': '"DB"."s"."ORDERS"',
                        'modelUUID': 'model-1',
                    },
                ),
                (
                    'dataset-customers',
                    'Customers',
                    {
                        'name': 'Customers',
                        'tableId': 'out.c-main.CUSTOMERS',
                        'fqn': '"DB"."s"."CUSTOMERS"',
                        'modelUUID': 'model-1',
                    },
                ),
            ],
            [],
            [
                (
                    'rel-orders-customers',
                    'Orders to Customers',
                    {
                        'name': 'Orders to Customers',
                        'from': 'out.c-main.ORDERS',
                        'to': 'out.c-main.CUSTOMERS',
                        'on': 'fact.FK_CUSTOMER_ID = dim.PK_CUSTOMER_ID',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            {
                SemanticObjectType.SEMANTIC_DATASET: ['dataset-orders', 'dataset-customers'],
            },
        ),
    ],
)
def test_detect_used_objects_from_context_edge_cases(
    sql_query: str,
    dataset_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    metric_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    relationship_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    expected_group_ids: dict[SemanticObjectType, list[str]],
) -> None:
    context_by_type = _detect_context(
        datasets=_build_metastore_objects(SemanticObjectType.SEMANTIC_DATASET, dataset_specs),
        metrics=_build_metastore_objects(SemanticObjectType.SEMANTIC_METRIC, metric_specs),
        relationships=_build_metastore_objects(SemanticObjectType.SEMANTIC_RELATIONSHIP, relationship_specs),
    )
    result = detect_used_objects_from_context(sql_query, context_by_type)

    assert {
        object_type: [item.id for item in group.objects] for object_type, group in result.items()
    } == expected_group_ids


@pytest.mark.parametrize(
    (
        'model_specs',
        'constraint_specs',
        'used_dataset_specs',
        'used_metric_specs',
        'used_relationship_specs',
        'expected_valid',
        'expected_violation_statuses',
        'expected_post_check_statuses',
        'expected_matched_relationships',
        'expected_post_check_queries',
        'expected_post_check_severities',
    ),
    [
        # No constraints means the output should stay valid and contain no findings.
        (
            [('model-1', 'Model', {'name': 'Model', 'sql_dialect': 'snowflake'})],
            [],
            [
                (
                    'dataset-orders',
                    'Orders',
                    {
                        'name': 'Orders',
                        'tableId': 'in.c-main.orders',
                        'fqn': 'analytics.orders',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [],
            [],
            True,
            [],
            [],
            [],
            [],
            [],
        ),
        # The constraint references a different metric than the one used by the query, so it is irrelevant.
        (
            [('model-1', 'Model', {})],
            [
                (
                    'constraint-irrelevant',
                    'Irrelevant Constraint',
                    {
                        'name': 'Irrelevant Constraint',
                        'constraintType': 'inequality',
                        'metrics': ['Revenue'],
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [],
            [
                (
                    'metric-orders',
                    'Orders',
                    {'name': 'Orders', 'dataset': 'in.c-main.orders', 'modelUUID': 'model-1'},
                )
            ],
            [],
            True,
            [],
            [],
            [],
            [],
            [],
        ),
        # Unknown constraint types without a validation query are ignored even when they match the used dataset.
        (
            [('model-1', 'Model', {})],
            [
                (
                    'constraint-unknown',
                    'Unknown Constraint',
                    {
                        'name': 'Unknown Constraint',
                        'constraintType': 'custom',
                        'datasets': ['in.c-main.orders'],
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [
                (
                    'dataset-orders',
                    'Orders',
                    {'name': 'Orders', 'tableId': 'in.c-main.orders', 'modelUUID': 'model-1'},
                )
            ],
            [],
            [],
            True,
            [],
            [],
            [],
            [],
            [],
        ),
        # A pre-query check with error severity should make the validation fail immediately.
        (
            [('model-1', 'Model', {})],
            [
                (
                    'constraint-pre-query-error',
                    'Pre Query Error',
                    {
                        'name': 'Pre Query Error',
                        'constraintType': 'conditional',
                        'severity': 'error',
                        'datasets': ['in.c-main.orders'],
                        'ai': {'preQueryCheck': True},
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [
                (
                    'dataset-orders',
                    'Orders',
                    {'name': 'Orders', 'tableId': 'in.c-main.orders', 'modelUUID': 'model-1'},
                )
            ],
            [],
            [],
            False,
            ['pre_query_check'],
            [],
            [],
            [],
            [],
        ),
        # Relationship names should fall back to object IDs when no display name is available, and constraints
        # without severity should default to "error".
        (
            [('model-1', 'Model', {'sql_dialect': 'bigquery'})],
            [
                (
                    'constraint-defaults',
                    'Constraint Defaults',
                    {
                        'name': 'Constraint Defaults',
                        'constraintType': 'range',
                        'datasets': ['in.c-main.orders'],
                        'validationQuery': {'default': 'SELECT default_check'},
                        'modelUUID': 'model-1',
                    },
                )
            ],
            [
                (
                    'dataset-orders',
                    'Orders',
                    {'name': 'Orders', 'tableId': 'in.c-main.orders', 'modelUUID': 'model-1'},
                )
            ],
            [],
            [
                (
                    'relationship-1',
                    '',
                    {
                        'from': 'in.c-main.orders',
                        'to': 'in.c-main.customers',
                        'modelUUID': 'model-1',
                    },
                )
            ],
            True,
            [],
            ['post_query_check'],
            ['relationship-1'],
            ['SELECT default_check'],
            ['error'],
        ),
    ],
)
def test_evaluate_constraints_from_context_edge_cases(
    model_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    constraint_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    used_dataset_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    used_metric_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    used_relationship_specs: Sequence[tuple[str, str, Mapping[str, object]]],
    expected_valid: bool,
    expected_violation_statuses: list[str],
    expected_post_check_statuses: list[str],
    expected_matched_relationships: list[str],
    expected_post_check_queries: list[str],
    expected_post_check_severities: list[str],
) -> None:
    context_by_type = _evaluate_context(
        model_specs=model_specs,
        constraint_specs=constraint_specs,
    )
    used_object_groups_by_type = _used_object_groups(
        dataset_specs=used_dataset_specs,
        metric_specs=used_metric_specs,
        relationship_specs=used_relationship_specs,
    )

    result = evaluate_constraints_from_context(context_by_type, used_object_groups_by_type)

    assert result.valid is expected_valid
    assert [finding.status for finding in result.violations] == expected_violation_statuses
    assert [finding.status for finding in result.post_execution_checks] == expected_post_check_statuses
    assert result.matched_relationships == expected_matched_relationships
    assert [finding.validation_query for finding in result.post_execution_checks] == expected_post_check_queries
    assert [finding.severity for finding in result.post_execution_checks] == expected_post_check_severities


@pytest.mark.parametrize(
    ('sql_query', 'semantic_model_id', 'message'),
    [
        ('   ', 'model-1', 'sql_query must not be empty.'),
        ('SELECT 1', '   ', 'semantic_model_id must not be empty.'),
    ],
)
@pytest.mark.asyncio
async def test_validate_semantic_query_requires_non_empty_inputs(
    keboola_client: KeboolaClient,
    sql_query: str,
    semantic_model_id: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        await validate_semantic_query(keboola_client, sql_query, semantic_model_id)


@pytest.mark.parametrize(
    ('patterns', 'semantic_types', 'case_sensitive', 'expected_ids', 'expected_paths'),
    [
        (
            ['orders'],
            [SemanticObjectType.SEMANTIC_DATASET],
            False,
            ['dataset-orders'],
            ['fqn', 'meta.name', 'name', 'tableId'],
        ),
        (
            ['SUM\\(ORDER_AMOUNT\\)'],
            [SemanticObjectType.SEMANTIC_METRIC],
            False,
            ['metric-revenue'],
            ['sql'],
        ),
        (
            ['customer_id'],
            [SemanticObjectType.SEMANTIC_RELATIONSHIP],
            False,
            ['relationship-orders-customers'],
            ['on'],
        ),
        (
            ['revenue'],
            [SemanticObjectType.SEMANTIC_GLOSSARY],
            True,
            [],
            [],
        ),
    ],
)
@pytest.mark.asyncio
async def test_search_semantic_context_returns_expected_matches(
    keboola_client: KeboolaClient,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
    patterns: list[str],
    semantic_types: list[SemanticObjectType],
    case_sensitive: bool,
    expected_ids: list[str],
    expected_paths: list[str],
) -> None:
    hits = await search_semantic_context(
        keboola_client,
        patterns,
        semantic_types=semantic_types,
        case_sensitive=case_sensitive,
    )

    assert [hit.object.id for hit in hits] == expected_ids
    assert [hit.matched_paths for hit in hits] == ([expected_paths] if expected_paths else [])


@pytest.mark.parametrize(
    ('patterns', 'max_results', 'message'),
    [
        ([], 10, 'At least one regex pattern must be provided.'),
        (['   '], 10, 'At least one regex pattern must be provided.'),
        (['orders'], 0, 'max_results must be a positive integer.'),
    ],
)
@pytest.mark.asyncio
async def test_search_semantic_context_validates_inputs(
    keboola_client: KeboolaClient,
    patterns: Sequence[str],
    max_results: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        await search_semantic_context(keboola_client, patterns, max_results=max_results)
