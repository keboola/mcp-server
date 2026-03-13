"""Pydantic models for semantic layer MCP tools."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ToolStatus(str, Enum):
    OK = 'ok'
    ERROR = 'error'


class SemanticEntityType(str, Enum):
    MODEL = 'model'
    DATASET = 'dataset'
    METRIC = 'metric'
    RELATIONSHIP = 'relationship'
    GLOSSARY = 'glossary'
    CONSTRAINT = 'constraint'


class SemanticObjectType(str, Enum):
    SEMANTIC_MODEL = 'semantic-model'
    SEMANTIC_DATASET = 'semantic-dataset'
    SEMANTIC_METRIC = 'semantic-metric'
    SEMANTIC_RELATIONSHIP = 'semantic-relationship'
    SEMANTIC_GLOSSARY = 'semantic-glossary'
    SEMANTIC_CONSTRAINT = 'semantic-constraint'


ENTITY_TO_OBJECT_TYPE: dict[SemanticEntityType, SemanticObjectType] = {
    SemanticEntityType.MODEL: SemanticObjectType.SEMANTIC_MODEL,
    SemanticEntityType.DATASET: SemanticObjectType.SEMANTIC_DATASET,
    SemanticEntityType.METRIC: SemanticObjectType.SEMANTIC_METRIC,
    SemanticEntityType.RELATIONSHIP: SemanticObjectType.SEMANTIC_RELATIONSHIP,
    SemanticEntityType.GLOSSARY: SemanticObjectType.SEMANTIC_GLOSSARY,
    SemanticEntityType.CONSTRAINT: SemanticObjectType.SEMANTIC_CONSTRAINT,
}
OBJECT_TO_ENTITY_TYPE: dict[SemanticObjectType, SemanticEntityType] = {
    object_type: entity_type for entity_type, object_type in ENTITY_TO_OBJECT_TYPE.items()
}


class SemanticScope(str, Enum):
    PROJECT = 'project'
    ORGANIZATION = 'organization'


class SemanticDefineAction(str, Enum):
    CREATE = 'create'
    PATCH = 'patch'
    REPLACE = 'replace'
    DELETE = 'delete'
    PUBLISH = 'publish'


class SemanticSource(BaseModel):
    object_type: str = Field(description='Semantic object type, e.g. semantic-metric.')
    uuid: str | None = Field(default=None, description='Object UUID in Metastore.')
    model_id: str | None = Field(default=None, description='Referenced semantic model UUID.')
    revision: int | None = Field(default=None, description='Metastore revision number.')


class SemanticToolResponse(BaseModel):
    status: ToolStatus = Field(default=ToolStatus.OK, description='Tool execution status.')
    reason: str | None = Field(default=None, description='Reason for non-standard states or errors.')
    source: SemanticSource | None = Field(default=None, description='Primary source reference.')
    next_action: str | None = Field(default=None, description='Recommended next semantic tool call.')


class SemanticModelSummary(BaseModel):
    model_id: str = Field(description='Semantic model UUID.')
    name: str = Field(description='Semantic model name.')
    scope: SemanticScope = Field(description='Model scope where it was discovered.')
    project_id: int | None = Field(default=None, description='Owning Keboola project ID.')
    status: str | None = Field(default=None, description='Model lifecycle status.')
    revision: int | None = Field(default=None, description='Current model revision.')
    dataset_count: int = Field(default=0, description='Number of datasets in model.')
    metric_count: int = Field(default=0, description='Number of metrics in model.')


class SemanticDiscoverMatch(BaseModel):
    entity_type: SemanticEntityType = Field(description='Resolved semantic entity type.')
    object_type: SemanticObjectType = Field(description='Underlying metastore object type.')
    uuid: str | None = Field(default=None, description='Matched object UUID.')
    model_id: str | None = Field(default=None, description='Resolved semantic model UUID.')
    name: str = Field(description='Matched entity name.')
    match_score: float = Field(description='Simple deterministic lexical ranking score.')
    revision: int | None = Field(default=None, description='Metastore revision for the object.')


class SemanticDiscoverOutput(SemanticToolResponse):
    models: list[SemanticModelSummary] = Field(default_factory=list, description='Discovered semantic models.')
    matches: list[SemanticDiscoverMatch] = Field(default_factory=list, description='Ranked semantic matches.')


class SemanticObjectDefinition(BaseModel):
    entity_type: SemanticEntityType = Field(description='Entity type from MCP contract.')
    object_type: SemanticObjectType = Field(description='Object type in metastore repository.')
    uuid: str | None = Field(default=None, description='Object UUID.')
    name: str = Field(default='', description='Object name.')
    data: dict[str, Any] = Field(default_factory=dict, description='Canonical semantic definition payload.')


class SemanticGetDefinitionOutput(SemanticToolResponse):
    defined: bool = Field(description='Whether requested definition was found.')
    definition: SemanticObjectDefinition | None = Field(
        default=None, description='Canonical semantic object definition.'
    )
    definition_schema: dict[str, Any] | None = Field(
        default=None,
        description='Optional JSON schema for this object type.',
        serialization_alias='schema',
    )


class SemanticFilter(BaseModel):
    field: str = Field(description='Filter field name.')
    operator: str = Field(default='=', description='Filter operator.')
    value: str | int | float | bool = Field(description='Filter value.')


class QueryPlanJoin(BaseModel):
    from_table_id: str = Field(description='Source tableId.')
    to_table_id: str = Field(description='Joined tableId.')
    join_type: str = Field(default='left', description='Join type.')
    on: str = Field(default='', description='Join condition from semantic relationship.')


class SemanticQueryPlan(BaseModel):
    metric_name: str = Field(description='Resolved metric name.')
    sql_expression: str = Field(default='', description='Canonical metric SQL expression.')
    source_dataset_table_id: str | None = Field(
        default=None, description='Primary dataset tableId for the metric.'
    )
    requested_dimensions: list[str] = Field(
        default_factory=list, description='Requested dimensions from user input.'
    )
    resolved_dimensions: list[str] = Field(
        default_factory=list, description='Dimensions resolved in source/joined datasets.'
    )
    unresolved_dimensions: list[str] = Field(
        default_factory=list, description='Dimensions not found in semantic model.'
    )
    joins: list[QueryPlanJoin] = Field(default_factory=list, description='Relationships used by the planner.')
    time_grain: str | None = Field(default=None, description='Requested time granularity.')
    filters: list[SemanticFilter] = Field(default_factory=list, description='Normalized filter list.')


class ConstraintCheck(BaseModel):
    name: str = Field(description='Constraint name.')
    constraint_type: str = Field(description='Constraint type from semantic-constraint schema.')
    severity: str = Field(default='warning', description='Constraint severity.')
    rule: str = Field(default='', description='Constraint rule expression.')
    status: str = Field(description='Planner status for this check.')
    note: str = Field(description='Human-readable validation note.')


class SemanticQueryPlanOutput(SemanticToolResponse):
    defined: bool = Field(description='Whether metric definition exists.')
    valid: bool = Field(description='Whether plan is valid under requested strictness.')
    plan: SemanticQueryPlan | None = Field(default=None, description='Structured semantic query plan.')
    warnings: list[ConstraintCheck] = Field(default_factory=list, description='Pre-execution warnings.')
    post_execution_checks: list[ConstraintCheck] = Field(
        default_factory=list, description='Post-execution validations.'
    )


class SemanticDefineOutput(SemanticToolResponse):
    created: bool = Field(default=False, description='Whether object was created.')
    updated: bool = Field(default=False, description='Whether object was patched/replaced.')
    deleted: bool = Field(default=False, description='Whether object was deleted.')
    published: bool = Field(default=False, description='Whether object was marked as published.')
    review_required: bool = Field(default=False, description='Whether manual review is recommended.')
    definition: SemanticObjectDefinition | None = Field(
        default=None, description='Resulting semantic object, if available.'
    )
