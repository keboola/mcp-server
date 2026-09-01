from __future__ import annotations

from collections.abc import Mapping

from keboola_mcp_server.clients.metastore import MetastoreObject
from keboola_mcp_server.tools.semantic.model import SemanticObjectType
from keboola_mcp_server.tools.semantic.service import _to_semantic_service_data
from keboola_mcp_server.tools.semantic.tools import (
    SemanticModelCompact,
    SemanticObject,
    _compact_semantic_object,
)


def _metastore_object(
    object_type: SemanticObjectType,
    object_id: str,
    *,
    name: str,
    meta: Mapping[str, object] | None = None,
    attributes: Mapping[str, object] | None = None,
) -> MetastoreObject:
    return MetastoreObject.model_validate(
        {
            'type': object_type.value,
            'id': object_id,
            'attributes': dict(attributes or {}),
            'meta': {'name': name, **(meta or {})},
        }
    )


def test_compact_object_carries_scope_and_visibility_fields() -> None:
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_MODEL,
        'm1',
        name='Shared Revenue Model',
        meta={
            'scope': 'targeted',
            'projectId': 123,
            'sourceProjectId': 456,
            'targetProjectIds': [999999999],
        },
    )

    compact = _compact_semantic_object(_to_semantic_service_data(SemanticObjectType.SEMANTIC_MODEL, obj))

    assert isinstance(compact, SemanticModelCompact)
    assert compact.scope == 'targeted'
    assert compact.project_id == 123
    assert compact.source_project_id == 456
    assert compact.target_project_ids == (999999999,)


def test_compact_object_leaves_scope_fields_absent_when_meta_has_none() -> None:
    obj = _metastore_object(SemanticObjectType.SEMANTIC_MODEL, 'm1', name='Plain Model')

    compact = _compact_semantic_object(_to_semantic_service_data(SemanticObjectType.SEMANTIC_MODEL, obj))

    assert compact.scope is None
    assert compact.project_id is None
    assert compact.source_project_id is None
    assert compact.target_project_ids is None


def test_full_semantic_object_also_carries_scope() -> None:
    obj = _metastore_object(
        SemanticObjectType.SEMANTIC_DATASET,
        'd1',
        name='Checkins',
        meta={'scope': 'organization', 'sourceProjectId': 456},
    )

    full = SemanticObject.from_semantic_service_data(
        _to_semantic_service_data(SemanticObjectType.SEMANTIC_DATASET, obj)
    )

    assert full.scope == 'organization'
    assert full.source_project_id == 456
