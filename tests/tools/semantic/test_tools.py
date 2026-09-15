from __future__ import annotations

from keboola_mcp_server.tools.semantic.model import SemanticObjectType
from keboola_mcp_server.tools.semantic.service import _to_semantic_service_data
from keboola_mcp_server.tools.semantic.tools import (
    SemanticModelCompact,
    SemanticObject,
    _compact_semantic_object,
)
from tests.tools.semantic.test_service import _metastore_object


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
