from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.semantic.data_location import DatasetLocationStatus, resolve_dataset_location
from keboola_mcp_server.tools.semantic.model import SemanticObjectType
from keboola_mcp_server.tools.semantic.service import SemanticDatasetData, _to_semantic_service_data
from tests.tools.semantic.conftest import _metastore_object


def _dataset(table_id: str | None, *, meta: dict[str, Any] | None = None) -> SemanticDatasetData:
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_DATASET,
        'dataset-1',
        name='Checkins',
        attributes={'tableId': table_id} if table_id is not None else {},
        meta=meta,
    )
    result = _to_semantic_service_data(SemanticObjectType.SEMANTIC_DATASET, obj)
    assert isinstance(result, SemanticDatasetData)
    return result


def _client(*, bucket_list: list[dict] | None = None, shared_bucket_list: list[dict] | None = None) -> KeboolaClient:
    client = AsyncMock(KeboolaClient)
    client.storage_client = AsyncMock()
    client.storage_client.bucket_list = AsyncMock(return_value=bucket_list or [])
    client.storage_client.shared_bucket_list = AsyncMock(return_value=shared_bucket_list or [])
    return client


@pytest.mark.asyncio
async def test_unreachable_when_table_id_has_no_bucket_separator() -> None:
    dataset = _dataset('no-dots-here')
    client = _client()

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.UNREACHABLE
    assert location.bucket_id is None
    client.storage_client.bucket_list.assert_not_awaited()


@pytest.mark.asyncio
async def test_unreachable_when_table_id_missing() -> None:
    dataset = _dataset(None)
    client = _client()

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.UNREACHABLE


@pytest.mark.asyncio
async def test_local_when_bucket_owned_outright() -> None:
    dataset = _dataset('out.c-RGP-Global.checkins')
    client = _client(bucket_list=[{'id': 'out.c-RGP-Global', 'name': 'c-RGP-Global'}])

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.LOCAL
    assert location.bucket_id == 'out.c-RGP-Global'
    assert location.source_project_id is None


@pytest.mark.asyncio
async def test_linked_when_bucket_has_a_source_project() -> None:
    dataset = _dataset('out.c-RGP-Global.checkins')
    client = _client(
        bucket_list=[
            {
                'id': 'out.c-RGP-Global',
                'name': 'c-RGP-Global',
                'sourceBucket': {'project': {'id': 123, 'name': 'Source Project'}},
            }
        ]
    )

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.LINKED
    assert location.source_project_id == 123


@pytest.mark.asyncio
async def test_shared_not_linked_when_matched_by_source_project() -> None:
    dataset = _dataset('out.c-RGP-Global.checkins', meta={'scope': 'organization', 'sourceProjectId': 123})
    client = _client(
        shared_bucket_list=[
            {'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 123}}
        ]
    )

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.SHARED_NOT_LINKED
    assert location.source_project_id == 123
    assert location.source_bucket_id == 'out.c-RGP-Global'


@pytest.mark.asyncio
async def test_shared_not_linked_falls_back_to_bucket_id_when_dataset_has_no_source_project() -> None:
    # targeted-scope datasets never carry sourceProjectId (metastore only sets it for organization
    # scope), so this is the common real case, not an edge case.
    dataset = _dataset('out.c-RGP-Global.checkins', meta={'scope': 'targeted'})
    client = _client(
        shared_bucket_list=[
            {'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 999}}
        ]
    )

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.SHARED_NOT_LINKED
    assert location.source_project_id == 999


@pytest.mark.asyncio
async def test_unreachable_when_shared_bucket_belongs_to_a_different_project() -> None:
    # The dataset claims sourceProjectId=123, but the only bucket sharing this id in the catalog
    # is from a different project (456) -- must not be treated as a match.
    dataset = _dataset('out.c-RGP-Global.checkins', meta={'scope': 'organization', 'sourceProjectId': 123})
    client = _client(
        shared_bucket_list=[
            {'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 456}}
        ]
    )

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.UNREACHABLE


@pytest.mark.asyncio
async def test_unreachable_when_bucket_is_neither_local_nor_shared() -> None:
    dataset = _dataset('out.c-RGP-Global.checkins')
    client = _client()

    location = await resolve_dataset_location(client, dataset)

    assert location.status == DatasetLocationStatus.UNREACHABLE
    assert location.bucket_id == 'out.c-RGP-Global'
