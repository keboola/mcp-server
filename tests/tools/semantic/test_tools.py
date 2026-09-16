from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastmcp import Context

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import MetastoreObject
from keboola_mcp_server.tools.semantic.data_location import DatasetLocationStatus
from keboola_mcp_server.tools.semantic.model import SemanticObjectType, SemanticObjectTypeSelection
from keboola_mcp_server.tools.semantic.service import _to_semantic_service_data
from keboola_mcp_server.tools.semantic.tools import (
    SemanticModelCompact,
    SemanticObject,
    _compact_semantic_object,
    get_semantic_context,
    validate_semantic_query,
)
from tests.tools.semantic.conftest import _metastore_object


def test_compact_object_carries_project_scope_and_pending_elevation() -> None:
    """A "project"-scope object may have a pending request to promote it to "organization"."""
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_MODEL,
        'm1',
        name='Shared Revenue Model',
        meta={
            'scope': 'project',
            'projectId': 123,
            'scopeElevationRequestedAt': '2026-01-03T00:00:00Z',
        },
    )

    compact = _compact_semantic_object(_to_semantic_service_data(SemanticObjectType.SEMANTIC_MODEL, obj))

    assert isinstance(compact, SemanticModelCompact)
    assert compact.scope == 'project'
    assert compact.project_id == 123
    assert compact.scope_elevation_requested_at == '2026-01-03T00:00:00Z'
    # Elevation is still pending, so neither of the "arrived" scopes' fields apply yet.
    assert compact.source_project_id is None
    assert compact.target_project_ids is None


def test_compact_object_carries_organization_scope_and_source_project() -> None:
    """An "organization"-scope object keeps its originating `project_id` and gains `source_project_id`."""
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_MODEL,
        'm1',
        name='Shared Revenue Model',
        meta={
            'scope': 'organization',
            'projectId': 123,
            'sourceProjectId': 456,
        },
    )

    compact = _compact_semantic_object(_to_semantic_service_data(SemanticObjectType.SEMANTIC_MODEL, obj))

    assert isinstance(compact, SemanticModelCompact)
    assert compact.scope == 'organization'
    # project_id is present regardless of scope -- it does not mean the object is private to it.
    assert compact.project_id == 123
    assert compact.source_project_id == 456
    assert compact.target_project_ids is None
    assert compact.scope_elevation_requested_at is None


def test_compact_object_carries_targeted_scope_and_target_projects() -> None:
    """A "targeted"-scope object carries the sibling project ids granted read access."""
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_MODEL,
        'm1',
        name='Shared Revenue Model',
        meta={
            'scope': 'targeted',
            'projectId': 123,
            'targetProjectIds': [999999999],
        },
    )

    compact = _compact_semantic_object(_to_semantic_service_data(SemanticObjectType.SEMANTIC_MODEL, obj))

    assert isinstance(compact, SemanticModelCompact)
    assert compact.scope == 'targeted'
    assert compact.project_id == 123
    assert compact.target_project_ids == (999999999,)
    assert compact.source_project_id is None
    assert compact.scope_elevation_requested_at is None


def test_compact_object_leaves_scope_fields_absent_when_meta_has_none() -> None:
    obj = _metastore_object(SemanticObjectType.SEMANTIC_MODEL, 'm1', name='Plain Model')

    compact = _compact_semantic_object(_to_semantic_service_data(SemanticObjectType.SEMANTIC_MODEL, obj))

    assert compact.scope is None
    assert compact.project_id is None
    assert compact.source_project_id is None
    assert compact.target_project_ids is None
    assert compact.scope_elevation_requested_at is None


def test_full_semantic_object_also_carries_scope() -> None:
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_DATASET,
        'd1',
        name='Checkins',
        meta={'scope': 'organization', 'projectId': 123, 'sourceProjectId': 456},
    )

    full = SemanticObject.from_semantic_service_data(
        _to_semantic_service_data(SemanticObjectType.SEMANTIC_DATASET, obj)
    )

    assert full.scope == 'organization'
    assert full.source_project_id == 456
    # project_id is the originating project and is NOT cleared on promotion to "organization" --
    # pinning this so the field's description (and any future change to it) stays honest.
    assert full.project_id == 123


@pytest.mark.asyncio
async def test_get_semantic_context_resolves_data_location_when_requested(
    keboola_client: KeboolaClient,
    mcp_context_client: Context,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
) -> None:
    keboola_client.storage_client.bucket_list = AsyncMock(return_value=[{'id': 'in.c-main', 'name': 'c-main'}])
    keboola_client.storage_client.shared_bucket_list = AsyncMock(return_value=[])

    contexts = await get_semantic_context(
        mcp_context_client,
        [SemanticObjectTypeSelection(object_type=SemanticObjectType.SEMANTIC_DATASET)],
        resolve_data_location=True,
    )

    datasets = contexts[0].objects
    assert len(datasets) == 2
    assert all(d.data_location is not None and d.data_location.status == DatasetLocationStatus.LOCAL for d in datasets)
    # Both datasets share the same bucket ("in.c-main") -- fetched once for the pair, not once per
    # dataset (2 datasets calling the same two endpoints twice each would be pure waste).
    keboola_client.storage_client.bucket_list.assert_awaited_once()
    keboola_client.storage_client.shared_bucket_list.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_semantic_context_disambiguates_by_the_parent_models_source_project(
    keboola_client: KeboolaClient,
    mcp_context_client: Context,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
) -> None:
    """Both fixture datasets share modelUUID "model-1" -- the parent model's sourceProjectId (not
    either dataset's own, which they don't carry) must be what picks the right shared bucket."""
    keboola_client.storage_client.bucket_list = AsyncMock(return_value=[])
    keboola_client.storage_client.shared_bucket_list = AsyncMock(
        return_value=[
            {'id': 'in.c-main', 'displayName': 'main', 'stage': 'in', 'project': {'id': 555}},
            {'id': 'in.c-main', 'displayName': 'main (unrelated)', 'stage': 'in', 'project': {'id': 999}},
        ]
    )
    keboola_client.metastore_client.get_object = AsyncMock(
        return_value=_metastore_object(
            SemanticObjectType.SEMANTIC_MODEL, 'model-1', name='Revenue Semantic Model', meta={'sourceProjectId': 555}
        )
    )

    contexts = await get_semantic_context(
        mcp_context_client,
        [SemanticObjectTypeSelection(object_type=SemanticObjectType.SEMANTIC_DATASET)],
        resolve_data_location=True,
    )

    keboola_client.metastore_client.get_object.assert_awaited_once_with(
        SemanticObjectType.SEMANTIC_MODEL.value, 'model-1'
    )
    datasets = contexts[0].objects
    assert len(datasets) == 2
    for dataset in datasets:
        assert dataset.data_location is not None
        assert dataset.data_location.status == DatasetLocationStatus.SHARED_NOT_LINKED
        assert dataset.data_location.source_project_id == 555
        assert dataset.data_location.ambiguous is False


@pytest.mark.asyncio
async def test_get_semantic_context_skips_data_location_by_default(
    keboola_client: KeboolaClient,
    mcp_context_client: Context,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
) -> None:
    keboola_client.storage_client.bucket_list = AsyncMock(return_value=[{'id': 'in.c-main', 'name': 'c-main'}])

    contexts = await get_semantic_context(
        mcp_context_client,
        [SemanticObjectTypeSelection(object_type=SemanticObjectType.SEMANTIC_DATASET)],
    )

    keboola_client.storage_client.bucket_list.assert_not_awaited()
    assert all(d.data_location is None for d in contexts[0].objects)


@pytest.mark.asyncio
async def test_validate_semantic_query_surfaces_unreachable_dataset_location(
    keboola_client: KeboolaClient,
    mcp_context_client: Context,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
) -> None:
    keboola_client.metastore_client.get_object = AsyncMock(
        return_value=mock_semantic_api[SemanticObjectType.SEMANTIC_MODEL][0]
    )
    keboola_client.storage_client.bucket_list = AsyncMock(return_value=[])
    keboola_client.storage_client.shared_bucket_list = AsyncMock(return_value=[])

    result = await validate_semantic_query(
        mcp_context_client,
        (
            'SELECT SUM(order_amount) AS revenue '
            'FROM analytics.orders orders '
            'JOIN analytics.customers customers ON orders.customer_id = customers.id'
        ),
        ['model-1'],
        resolve_data_location=True,
    )

    findings = {f.constraint_id: f for f in result.validation_auto_detected.violations}
    assert findings['data-location:dataset-orders'].status == DatasetLocationStatus.UNREACHABLE.value
    assert findings['data-location:dataset-orders'].severity == 'warning'
    assert findings['data-location:dataset-customers'].status == DatasetLocationStatus.UNREACHABLE.value
    # Both used datasets resolve against the same two Storage lists -- fetched once, not once per
    # dataset, and reusing the semantic-model already fetched for semantic_model_ids rather than
    # a redundant per-dataset model lookup.
    keboola_client.storage_client.bucket_list.assert_awaited_once()
    keboola_client.storage_client.shared_bucket_list.assert_awaited_once()
    keboola_client.metastore_client.get_object.assert_awaited_once()


@pytest.mark.asyncio
async def test_validate_semantic_query_surfaces_ambiguous_dataset_location(
    keboola_client: KeboolaClient,
    mcp_context_client: Context,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
) -> None:
    """When two projects share the same bucket id and the parent model carries no
    sourceProjectId, the finding must tell the caller the source is ambiguous and needs
    resolving -- not hand it `source_project_id=None`/`source_bucket_id=None` as though those
    were usable link_shared_bucket arguments."""
    keboola_client.metastore_client.get_object = AsyncMock(
        return_value=mock_semantic_api[SemanticObjectType.SEMANTIC_MODEL][0]
    )
    keboola_client.storage_client.bucket_list = AsyncMock(return_value=[])
    keboola_client.storage_client.shared_bucket_list = AsyncMock(
        return_value=[
            {'id': 'in.c-main', 'displayName': 'main', 'stage': 'in', 'project': {'id': 555}},
            {'id': 'in.c-main', 'displayName': 'main (unrelated)', 'stage': 'in', 'project': {'id': 999}},
        ]
    )

    result = await validate_semantic_query(
        mcp_context_client,
        (
            'SELECT SUM(order_amount) AS revenue '
            'FROM analytics.orders orders '
            'JOIN analytics.customers customers ON orders.customer_id = customers.id'
        ),
        ['model-1'],
        resolve_data_location=True,
    )

    findings = {f.constraint_id: f for f in result.validation_auto_detected.violations}
    finding = findings['data-location:dataset-orders']
    assert finding.status == DatasetLocationStatus.SHARED_NOT_LINKED.value
    assert finding.severity == 'warning'
    assert 'None' not in finding.message
    assert 'ambiguous' in finding.message.lower()
    assert 'link_shared_bucket' in finding.message


@pytest.mark.asyncio
async def test_validate_semantic_query_skips_data_location_by_default(
    keboola_client: KeboolaClient,
    mcp_context_client: Context,
    mock_semantic_api: dict[SemanticObjectType, list[MetastoreObject]],
) -> None:
    keboola_client.metastore_client.get_object = AsyncMock(
        return_value=mock_semantic_api[SemanticObjectType.SEMANTIC_MODEL][0]
    )

    result = await validate_semantic_query(
        mcp_context_client,
        (
            'SELECT SUM(order_amount) AS revenue '
            'FROM analytics.orders orders '
            'JOIN analytics.customers customers ON orders.customer_id = customers.id'
        ),
        ['model-1'],
    )

    keboola_client.storage_client.bucket_list.assert_not_awaited()
    assert not any(f.constraint_id.startswith('data-location:') for f in result.validation_auto_detected.violations)
