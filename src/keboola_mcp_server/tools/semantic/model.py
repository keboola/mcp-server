"""Models shared by the current semantic read tools."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.metastore import MetastoreObject


class SemanticObjectType(str, Enum):
    SEMANTIC_MODEL = 'semantic-model'
    SEMANTIC_DATASET = 'semantic-dataset'
    SEMANTIC_METRIC = 'semantic-metric'
    SEMANTIC_RELATIONSHIP = 'semantic-relationship'
    SEMANTIC_GLOSSARY = 'semantic-glossary'
    SEMANTIC_CONSTRAINT = 'semantic-constraint'


# Shared input models


class SemanticObjectTypeSelection(BaseModel):
    """Semantic object type selection used by semantic tools."""

    object_type: SemanticObjectType = Field(description='Semantic object type to load.')
    ids: tuple[str, ...] = Field(
        default=tuple(),
        description='Specific object UUIDs to include. Empty list [] means include all objects of this type.',
    )


# Service-only models


class SemanticSearchHit(BaseModel):
    """Raw semantic search hit returned by the service layer."""

    object_type: SemanticObjectType = Field(description='Matched semantic object type.')
    object: MetastoreObject = Field(description='Matched metastore object.')
    semantic_model_id: str = Field(description='Parent semantic model UUID.')
    matched_patterns: list[str] = Field(default_factory=list, description='Regex patterns that matched.')
    matched_paths: list[str] = Field(default_factory=list, description='Search sources where the match happened.')


class SemanticObjectTypeGroup(BaseModel):
    """Raw semantic objects grouped by semantic object type."""

    object_type: SemanticObjectType = Field(description='Semantic object type.')
    objects: list[MetastoreObject] = Field(
        default_factory=list,
        description='Raw semantic metastore objects of the requested type.',
    )


class ConstraintValidationFinding(BaseModel):
    """Semantic constraint finding produced by validation."""

    constraint_id: str = Field(description='Constraint UUID.')
    constraint_name: str = Field(description='Constraint name.')
    severity: str = Field(description='Constraint severity.')
    status: str = Field(description='Validation status.')
    message: str = Field(description='Human-readable validation finding.')
    validation_query: str | None = Field(
        default=None,
        description='Optional SQL validation query suggested by the semantic constraint.',
    )


class RawSemanticValidationResult(BaseModel):
    """Raw validation result returned by the service layer."""

    valid: bool = Field(description='False when an error-severity pre-execution finding was detected.')
    used_object_groups: list[SemanticObjectTypeGroup] = Field(
        default_factory=list,
        description='Used semantic objects grouped by semantic object type.',
    )
    matched_relationships: list[str] = Field(
        default_factory=list,
        description='Relationship names heuristically detected in the SQL.',
    )
    violations: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='Pre-execution semantic violations.',
    )
    post_execution_checks: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='Checks that should be verified against query results.',
    )


# Tool-only compact and output models


class CompactSemanticObject(BaseModel):
    id: str
    name: str | None = None


class SemanticModelCompact(CompactSemanticObject):
    description: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> 'SemanticModelCompact':
        attributes = obj.attributes
        return cls(
            id=obj.id,
            name=attributes.get('name') or obj.meta.name,
            description=attributes.get('description'),
        )


class SemanticDatasetCompact(CompactSemanticObject):
    table_id: str | None = Field(default=None, serialization_alias='tableId')
    description: str | None = None
    model_uuid: str | None = None
    fqn: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> 'SemanticDatasetCompact':
        attributes = obj.attributes
        return cls(
            id=obj.id,
            name=attributes.get('name') or obj.meta.name,
            table_id=attributes.get('tableId'),
            description=attributes.get('description'),
            model_uuid=attributes.get('modelUUID'),
            fqn=attributes.get('fqn'),
        )


class SemanticMetricCompact(CompactSemanticObject):
    description: str | None = None
    dataset: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> 'SemanticMetricCompact':
        attributes = obj.attributes
        return cls(
            id=obj.id,
            name=attributes.get('name') or obj.meta.name,
            description=attributes.get('description'),
            dataset=attributes.get('dataset'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticRelationshipCompact(CompactSemanticObject):
    from_: str | None = Field(default=None, serialization_alias='from')
    to: str | None = None
    type: str | None = None
    on: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> 'SemanticRelationshipCompact':
        attributes = obj.attributes
        return cls(
            id=obj.id,
            name=attributes.get('name') or obj.meta.name,
            from_=attributes.get('from'),
            to=attributes.get('to'),
            type=attributes.get('type'),
            on=attributes.get('on'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticGlossaryCompact(CompactSemanticObject):
    term: str | None = None
    definition: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> 'SemanticGlossaryCompact':
        attributes = obj.attributes
        return cls(
            id=obj.id,
            name=attributes.get('term') or obj.meta.name,
            term=attributes.get('term') or obj.meta.name,
            definition=attributes.get('definition'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticConstraintCompact(CompactSemanticObject):
    description: str | None = None
    type: str | None = None
    rule: str | None = None
    severity: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> 'SemanticConstraintCompact':
        attributes = obj.attributes
        return cls(
            id=obj.id,
            name=attributes.get('name') or obj.meta.name,
            description=attributes.get('description'),
            type=attributes.get('constraintType'),
            rule=attributes.get('rule'),
            severity=attributes.get('severity'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticObject(CompactSemanticObject):
    attributes: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> 'SemanticObject':
        return cls(
            id=obj.id,
            name=obj.meta.name,
            attributes=obj.attributes,
        )


class SemanticObjectMatch(BaseModel):
    """Matched semantic object returned by semantic search."""

    object_type: SemanticObjectType = Field(description='Matched semantic object type.')
    matched_paths: list[str] = Field(default_factory=list, description='Matched paths inside the semantic object.')
    data: CompactSemanticObject = Field(description='Compact matched semantic object detail.')


class SemanticModelSearchResult(BaseModel):
    """Search matches grouped by semantic model."""

    semantic_model_id: str = Field(description='Semantic model UUID.')
    matches: list[SemanticObjectMatch] = Field(default_factory=list, description='Matched objects for this model.')


class SearchSemanticContextOutput(BaseModel):
    """Output for the semantic search tool."""

    models: list[SemanticModelSearchResult] = Field(
        default_factory=list,
        description='Matched objects grouped by semantic model.',
    )


class SemanticObjectTypeContext(BaseModel):
    """Tool output context for a single semantic object type."""

    object_type: SemanticObjectType = Field(description='Semantic object type.')
    objects: list[CompactSemanticObject] = Field(
        default_factory=list,
        description='Semantic objects of the requested type.',
    )


class GetSemanticContextOutput(BaseModel):
    """Output for semantic context loading."""

    semantic_objects: list[SemanticObjectTypeContext] = Field(
        default_factory=list,
        description='Requested semantic contexts grouped by semantic object type.',
    )


class SemanticUsedDataset(BaseModel):
    """Dataset referenced by the validated SQL."""

    name: str = Field(description='Dataset name.')
    table_id: str = Field(description='Keboola table ID.', serialization_alias='tableId')
    description: str = Field(description='Dataset description.')
    fqn: str = Field(description='Dataset fully qualified SQL name.')


class SemanticUsedMetric(BaseModel):
    """Metric referenced by the validated SQL."""

    name: str = Field(description='Metric name.')
    description: str = Field(description='Metric description.')
    sql: str = Field(description='Metric SQL expression.')
    dataset: str = Field(description='Source dataset table ID.')


class SemanticValidationModelResult(BaseModel):
    """Validation result for a single semantic model."""

    semantic_model_id: str = Field(description='Semantic model UUID.')
    semantic_model_name: str | None = Field(default=None, description='Semantic model name.')
    sql_dialect: str | None = Field(default=None, description='SQL dialect of the semantic model.')
    selected_object_ids: list[str] = Field(default_factory=list, description='Selected semantic object UUIDs.')
    used_datasets: list[SemanticUsedDataset] = Field(
        default_factory=list,
        description='Semantic datasets referenced by the SQL.',
    )
    used_metrics: list[SemanticUsedMetric] = Field(
        default_factory=list,
        description='Semantic metrics used or approximated by the SQL.',
    )
    matched_relationships: list[str] = Field(default_factory=list, description='Relationship names detected in SQL.')
    violations: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='Pre-execution semantic violations.',
    )
    post_execution_checks: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='Checks that should be verified against query results.',
    )


class ValidateSemanticQueryOutput(BaseModel):
    """Output for semantic SQL validation."""

    valid: bool = Field(description='False when an error-severity pre-execution finding was detected.')
    used_datasets: list[SemanticUsedDataset] = Field(
        default_factory=list,
        description='All semantic datasets referenced by the SQL across selected models.',
    )
    used_metrics: list[SemanticUsedMetric] = Field(
        default_factory=list,
        description='All semantic metrics referenced by the SQL across selected models.',
    )
    violations: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='All pre-execution violations across selected models.',
    )
    post_execution_checks: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='All post-execution checks across selected models.',
    )
    semantic_models: list[SemanticValidationModelResult] = Field(
        default_factory=list,
        description='Validation result grouped by semantic model.',
    )
    summary: str = Field(description='Short validation summary.')
