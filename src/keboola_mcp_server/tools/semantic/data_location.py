"""Resolves whether a semantic-dataset's underlying Storage table is actually reachable from the
calling project -- see feature_spec/semantic_dataset_data_location/RFC.md.

A semantic-dataset's `scope` (surfaced by the metastore read path) only describes whether the
*metastore record* is visible to the calling project. It says nothing about whether the Storage
bucket behind its `tableId` was ever shared and linked -- those are two independent, unenforced
mechanisms. This module joins them.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.components.utils import get_nested
from keboola_mcp_server.tools.semantic.service import SemanticDatasetData
from keboola_mcp_server.tools.storage.shared_buckets import SharedBucketDetail


class DatasetLocationStatus(str, Enum):
    LOCAL = 'local'
    LINKED = 'linked'
    SHARED_NOT_LINKED = 'shared_not_linked'
    UNREACHABLE = 'unreachable'

    def __str__(self) -> str:
        return self.value


class DatasetLocation(BaseModel):
    """Where a semantic-dataset's tableId physically lives, relative to the calling project."""

    status: DatasetLocationStatus = Field(
        description=(
            '"local": bucket already owned by this project. "linked": bucket already linked into this '
            'project from another one. "shared_not_linked": bucket exists and is shared with this project '
            'but not linked in yet -- use link_shared_bucket with source_project_id/source_bucket_id to fix '
            'it. "unreachable": the dataset\'s scope says it should be visible, but its bucket is neither '
            'owned, linked, nor shared here -- the metastore object and the underlying data disagree.'
        )
    )
    bucket_id: str | None = Field(default=None, description='Bucket id derived from the dataset\'s tableId.')
    source_project_id: int | str | None = Field(
        default=None, description='Project the bucket is linked from or shared from, when known.'
    )
    source_bucket_id: str | None = Field(
        default=None,
        description='The shared bucket\'s own id in its source project -- pass as source_bucket_id to link_shared_bucket.',
    )


def _bucket_id_from_table_id(table_id: str | None) -> str | None:
    if not table_id or '.' not in table_id:
        return None
    return table_id.rsplit('.', 1)[0]


async def resolve_dataset_location(client: KeboolaClient, dataset: SemanticDatasetData) -> DatasetLocation:
    """Resolves where a semantic-dataset's tableId physically lives, relative to the calling project."""
    bucket_id = _bucket_id_from_table_id(dataset.table_id)
    if bucket_id is None:
        return DatasetLocation(status=DatasetLocationStatus.UNREACHABLE)

    for raw_bucket in await client.storage_client.bucket_list():
        if raw_bucket.get('id') != bucket_id:
            continue
        source_project = get_nested(raw_bucket, 'sourceBucket.project')
        if isinstance(source_project, dict) and source_project.get('id') is not None:
            return DatasetLocation(
                status=DatasetLocationStatus.LINKED, bucket_id=bucket_id, source_project_id=source_project['id']
            )
        return DatasetLocation(status=DatasetLocationStatus.LOCAL, bucket_id=bucket_id)

    # The dataset's own recorded source project, when the metastore tracked one (organization
    # scope only -- absent for project/targeted scope, or when the creator opted out via
    # dropSourceProject). Used to disambiguate a bucket_id that could plausibly exist in more
    # than one project; when absent, fall back to a bucket_id-only match rather than refusing to
    # resolve anything, matching this tool's existing "best-effort" framing elsewhere.
    expected_source_project_id = dataset.data.meta.source_project_id if dataset.data.meta else None

    fallback_match: SharedBucketDetail | None = None
    for raw_shared in await client.storage_client.shared_bucket_list():
        shared = SharedBucketDetail.model_validate(raw_shared)
        if shared.id != bucket_id:
            continue
        if expected_source_project_id is not None and str(shared.project_id) == str(expected_source_project_id):
            return DatasetLocation(
                status=DatasetLocationStatus.SHARED_NOT_LINKED,
                bucket_id=bucket_id,
                source_project_id=shared.project_id,
                source_bucket_id=shared.id,
            )
        fallback_match = fallback_match or shared

    if fallback_match is not None and expected_source_project_id is None:
        return DatasetLocation(
            status=DatasetLocationStatus.SHARED_NOT_LINKED,
            bucket_id=bucket_id,
            source_project_id=fallback_match.project_id,
            source_bucket_id=fallback_match.id,
        )

    return DatasetLocation(status=DatasetLocationStatus.UNREACHABLE, bucket_id=bucket_id)
