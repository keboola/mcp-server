from __future__ import annotations

import pytest

from keboola_mcp_server.tools.semantic.data_location import DatasetLocationStatus, resolve_dataset_location
from keboola_mcp_server.tools.semantic.model import SemanticObjectType
from keboola_mcp_server.tools.semantic.service import SemanticDatasetData, _to_semantic_service_data
from tests.tools.semantic.conftest import _metastore_object


def _dataset(table_id: str | None) -> SemanticDatasetData:
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_DATASET,
        'dataset-1',
        name='Checkins',
        attributes={'tableId': table_id} if table_id is not None else {},
    )
    result = _to_semantic_service_data(SemanticObjectType.SEMANTIC_DATASET, obj)
    assert isinstance(result, SemanticDatasetData)
    return result


async def _resolve(
    dataset: SemanticDatasetData,
    *,
    local_buckets: list[dict] | None = None,
    shared_buckets: list[dict] | None = None,
    model_source_project_id: int | str | None = None,
):
    return await resolve_dataset_location(
        dataset,
        local_buckets=local_buckets or [],
        shared_buckets=shared_buckets or [],
        model_source_project_id=model_source_project_id,
    )


@pytest.mark.asyncio
async def test_unreachable_when_table_id_has_no_bucket_separator() -> None:
    location = await _resolve(_dataset('no-dots-here'))

    assert location.status == DatasetLocationStatus.UNREACHABLE
    assert location.bucket_id is None


@pytest.mark.asyncio
async def test_unreachable_when_table_id_missing() -> None:
    location = await _resolve(_dataset(None))

    assert location.status == DatasetLocationStatus.UNREACHABLE


@pytest.mark.asyncio
async def test_local_when_bucket_owned_outright() -> None:
    location = await _resolve(
        _dataset('out.c-RGP-Global.checkins'),
        local_buckets=[{'id': 'out.c-RGP-Global', 'name': 'c-RGP-Global'}],
    )

    assert location.status == DatasetLocationStatus.LOCAL
    assert location.bucket_id == 'out.c-RGP-Global'
    assert location.source_project_id is None


@pytest.mark.asyncio
async def test_linked_when_bucket_has_a_source_project() -> None:
    location = await _resolve(
        _dataset('out.c-RGP-Global.checkins'),
        local_buckets=[
            {
                'id': 'out.c-RGP-Global',
                'name': 'c-RGP-Global',
                'sourceBucket': {'project': {'id': 123, 'name': 'Source Project'}},
            }
        ],
    )

    assert location.status == DatasetLocationStatus.LINKED
    assert location.source_project_id == 123


@pytest.mark.asyncio
async def test_shared_not_linked_when_matched_by_model_source_project() -> None:
    """Disambiguation reads the *parent model's* sourceProjectId, per the RFC -- not the dataset's own."""
    location = await _resolve(
        _dataset('out.c-RGP-Global.checkins'),
        shared_buckets=[{'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 123}}],
        model_source_project_id=123,
    )

    assert location.status == DatasetLocationStatus.SHARED_NOT_LINKED
    assert location.source_project_id == 123
    assert location.source_bucket_id == 'out.c-RGP-Global'
    assert location.ambiguous is False


@pytest.mark.asyncio
async def test_shared_not_linked_falls_back_to_single_match_when_model_source_project_unknown() -> None:
    # targeted-scope models never carry sourceProjectId (the metastore only sets it for
    # organization scope), so this is the common real case, not an edge case.
    location = await _resolve(
        _dataset('out.c-RGP-Global.checkins'),
        shared_buckets=[{'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 999}}],
        model_source_project_id=None,
    )

    assert location.status == DatasetLocationStatus.SHARED_NOT_LINKED
    assert location.source_project_id == 999
    assert location.ambiguous is False


@pytest.mark.asyncio
async def test_ambiguous_when_multiple_projects_share_the_same_bucket_id_and_model_source_unknown() -> None:
    """Two sibling projects both sharing e.g. out.c-RGP-Global, with no model provenance to pick between
    them, must be reported as ambiguous rather than silently resolved to whichever the API listed first."""
    location = await _resolve(
        _dataset('out.c-RGP-Global.checkins'),
        shared_buckets=[
            {'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 111}},
            {'id': 'out.c-RGP-Global', 'displayName': 'checkins-2', 'stage': 'out', 'project': {'id': 222}},
        ],
        model_source_project_id=None,
    )

    assert location.status == DatasetLocationStatus.SHARED_NOT_LINKED
    assert location.ambiguous is True
    assert location.source_project_id is None
    assert location.source_bucket_id is None


@pytest.mark.asyncio
async def test_unreachable_when_shared_bucket_belongs_to_a_different_project() -> None:
    # The parent model names source project 123, but the only bucket sharing this id in the
    # catalog is from a different project (456) -- must not be treated as a match.
    location = await _resolve(
        _dataset('out.c-RGP-Global.checkins'),
        shared_buckets=[{'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 456}}],
        model_source_project_id=123,
    )

    assert location.status == DatasetLocationStatus.UNREACHABLE


@pytest.mark.asyncio
async def test_unreachable_when_bucket_is_neither_local_nor_shared() -> None:
    location = await _resolve(_dataset('out.c-RGP-Global.checkins'))

    assert location.status == DatasetLocationStatus.UNREACHABLE
    assert location.bucket_id == 'out.c-RGP-Global'


@pytest.mark.asyncio
async def test_unrelated_malformed_shared_bucket_entry_does_not_abort_resolution() -> None:
    """A shared-bucket entry for a different id that fails SharedBucketDetail's schema (missing
    displayName/stage -- e.g. a specific-users share, or a shape variation on another stack) must
    never be validated at all when resolving *this* dataset, since only same-id entries are."""
    location = await _resolve(
        _dataset('out.c-RGP-Global.checkins'),
        shared_buckets=[
            {'id': 'out.c-some-other-bucket'},  # missing displayName/stage -- would fail validation
            {'id': 'out.c-RGP-Global', 'displayName': 'checkins', 'stage': 'out', 'project': {'id': 123}},
        ],
        model_source_project_id=123,
    )

    assert location.status == DatasetLocationStatus.SHARED_NOT_LINKED
    assert location.source_project_id == 123
