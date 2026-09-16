"""Resolves whether a semantic-dataset's underlying Storage table is actually reachable from the
calling project -- see feature_spec/semantic_dataset_data_location/RFC.md.

A semantic-dataset's `scope` (surfaced by the metastore read path) only describes whether the
*metastore record* is visible to the calling project. It says nothing about whether the Storage
bucket behind its `tableId` was ever shared and linked -- those are two independent, unenforced
mechanisms. This module joins them.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict
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
            'it (unless `ambiguous` is set -- see that field). "unreachable": the dataset\'s scope says it '
            'should be visible, but its bucket is neither owned, linked, nor shared here -- the metastore '
            'object and the underlying data disagree.'
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
    ambiguous: bool = Field(
        default=False,
        description=(
            'True when more than one project shares a bucket with this id and the dataset\'s parent semantic '
            'model carries no sourceProjectId to disambiguate between them (only ever set for "organization" '
            'scope). source_project_id/source_bucket_id are left unset rather than guessing -- resolve the '
            'ambiguity with the user before calling link_shared_bucket.'
        ),
    )


def _bucket_id_from_table_id(table_id: str | None) -> str | None:
    if not table_id or '.' not in table_id:
        return None
    return table_id.rsplit('.', 1)[0]


async def resolve_dataset_location(
    dataset: SemanticDatasetData,
    *,
    local_buckets: Sequence[JsonDict],
    shared_buckets: Sequence[JsonDict],
    model_source_project_id: int | str | None,
) -> DatasetLocation:
    """Resolves where a semantic-dataset's tableId physically lives, relative to the calling project.

    `local_buckets` and `shared_buckets` are the caller's own project's full bucket/shared-bucket
    listings (fetched once per outer call, not once per dataset -- both endpoints return the same
    payload regardless of which dataset is being resolved). `model_source_project_id` is the
    dataset's *parent semantic-model's* `sourceProjectId` (per the RFC, this is model-level
    provenance, not dataset-level -- a dataset only ever carries its own when it was itself
    directly created/promoted at "organization" scope, which targeted-scope sharing never does).
    """
    bucket_id = _bucket_id_from_table_id(dataset.table_id)
    if bucket_id is None:
        return DatasetLocation(status=DatasetLocationStatus.UNREACHABLE)

    for raw_bucket in local_buckets:
        if raw_bucket.get('id') != bucket_id:
            continue
        source_project = get_nested(raw_bucket, 'sourceBucket.project')
        if isinstance(source_project, dict) and source_project.get('id') is not None:
            return DatasetLocation(
                status=DatasetLocationStatus.LINKED, bucket_id=bucket_id, source_project_id=source_project['id']
            )
        return DatasetLocation(status=DatasetLocationStatus.LOCAL, bucket_id=bucket_id)

    # Only entries whose raw id matches are validated -- an unrelated shared bucket elsewhere in
    # the catalog (a different share type, a shape variation on another stack) must never abort
    # resolving *this* dataset just because it happens to fail SharedBucketDetail's schema.
    matches = [
        SharedBucketDetail.model_validate(raw_shared)
        for raw_shared in shared_buckets
        if raw_shared.get('id') == bucket_id
    ]

    if model_source_project_id is not None:
        match = next((m for m in matches if str(m.project_id) == str(model_source_project_id)), None)
        if match is not None:
            return DatasetLocation(
                status=DatasetLocationStatus.SHARED_NOT_LINKED,
                bucket_id=bucket_id,
                source_project_id=match.project_id,
                source_bucket_id=match.id,
            )
        # The parent model names a specific source project and no candidate matches it -- the
        # bucket is shared from somewhere else entirely, not just unresolvably ambiguous.
    elif len(matches) == 1:
        match = matches[0]
        return DatasetLocation(
            status=DatasetLocationStatus.SHARED_NOT_LINKED,
            bucket_id=bucket_id,
            source_project_id=match.project_id,
            source_bucket_id=match.id,
        )
    elif len(matches) > 1:
        return DatasetLocation(status=DatasetLocationStatus.SHARED_NOT_LINKED, bucket_id=bucket_id, ambiguous=True)

    return DatasetLocation(status=DatasetLocationStatus.UNREACHABLE, bucket_id=bucket_id)
