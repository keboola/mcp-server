"""Typed semantic object models backed by Metastore JSON schemas."""

from __future__ import annotations

from enum import Enum
from typing import TypeAlias
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from keboola_mcp_server.tools.semantic.model import SemanticObjectType


class SemanticSchemaBaseModel(BaseModel):
    """Base model for semantic schema objects."""

    model_config = ConfigDict(populate_by_name=True, extra='ignore')


class SemanticAiGuidance(SemanticSchemaBaseModel):
    """Shared AI guidance block used by semantic dataset and field definitions."""

    hints: list[str] | None = Field(default=None, description='SQL generation tips')
    keywords: list[str] | None = Field(default=None, description='Words that should trigger selection')
    synonyms: list[str] | None = Field(default=None, description='Alternative names users might say')
    warnings: list[str] | None = Field(default=None, description='Common mistakes to avoid')
    anti_keywords: list[str] | None = Field(
        default=None,
        description="Words meaning DON'T select this",
        validation_alias=AliasChoices('antiKeywords', 'anti_keywords'),
        serialization_alias='antiKeywords',
    )


class SemanticDatasetFieldRole(str, Enum):
    KEY = 'key'
    DIMENSION = 'dimension'
    MEASURE = 'measure'
    TIMESTAMP = 'timestamp'


class SemanticDatasetFieldType(str, Enum):
    STRING = 'string'
    INTEGER = 'integer'
    DECIMAL = 'decimal'
    BOOLEAN = 'boolean'
    DATE = 'date'
    DATETIME = 'datetime'
    JSON = 'json'


class SemanticDatasetField(SemanticSchemaBaseModel):
    """Semantic field definition inside a dataset."""

    name: str = Field(description='Column name')
    ai: SemanticAiGuidance | None = Field(default=None, description='AI guidance for this field')
    role: SemanticDatasetFieldRole | None = Field(default=None, description="Column's role in analytics")
    type: SemanticDatasetFieldType | None = Field(default=None, description='Semantic data type')
    values: list[str] | None = Field(default=None, description='Valid/common values for categorical fields')
    description: str | None = Field(default=None, description='Field description')


class SemanticDataset(SemanticSchemaBaseModel):
    """Schema-backed semantic dataset object."""

    table_id: str = Field(
        description="Keboola table ID (e.g., 'in.c-bucket.table')",
        validation_alias=AliasChoices('tableId', 'table_id'),
        serialization_alias='tableId',
    )
    name: str = Field(description='Display name')
    fqn: str = Field(description='Fully qualified name for SQL')
    model_uuid: UUID = Field(
        description='UUID of the parent semantic-model',
        validation_alias=AliasChoices('modelUUID', 'model_uuid'),
        serialization_alias='modelUUID',
    )
    ai: SemanticAiGuidance | None = Field(default=None, description='AI guidance for better object selection')
    grain: str | None = Field(default=None, description='What one row represents')
    fields: list[SemanticDatasetField] | None = Field(default=None, description='Dataset fields')
    primary_key: list[str] | None = Field(
        default=None,
        description='Primary key column(s)',
        validation_alias=AliasChoices('primaryKey', 'primary_key'),
        serialization_alias='primaryKey',
    )
    description: str | None = Field(default=None, description='What this table contains and when to use it')


class SemanticMetric(SemanticSchemaBaseModel):
    """Schema-backed semantic metric object."""

    model_uuid: UUID = Field(
        description='UUID of the parent semantic-model',
        validation_alias=AliasChoices('modelUUID', 'model_uuid'),
        serialization_alias='modelUUID',
    )
    name: str = Field(description='Metric name')
    sql: str = Field(description='SQL expression')
    dataset: str | None = Field(default=None, description='Primary dataset ID (tableId) this metric uses')
    description: str | None = Field(default=None, description='What this metric measures')


class SemanticRelationshipType(str, Enum):
    LEFT = 'left'
    INNER = 'inner'


class SemanticRelationship(SemanticSchemaBaseModel):
    """Schema-backed semantic relationship object."""

    model_uuid: UUID = Field(
        description='UUID of the parent semantic-model',
        validation_alias=AliasChoices('modelUUID', 'model_uuid'),
        serialization_alias='modelUUID',
    )
    from_: str = Field(
        description='Source dataset ID (tableId)',
        validation_alias=AliasChoices('from', 'from_'),
        serialization_alias='from',
    )
    to: str = Field(description='Target dataset ID (tableId)')
    on: str = Field(description='Join condition')
    name: str | None = Field(default=None, description='Relationship name')
    type: SemanticRelationshipType = Field(default=SemanticRelationshipType.LEFT, description='Join type')


class SemanticGlossary(SemanticSchemaBaseModel):
    """Schema-backed semantic glossary object."""

    model_uuid: UUID = Field(
        description='UUID of the parent semantic-model',
        validation_alias=AliasChoices('modelUUID', 'model_uuid'),
        serialization_alias='modelUUID',
    )
    term: str = Field(description='Business term')
    definition: str = Field(description='What this term means in the business context')
    see_also: list[str] | None = Field(
        default=None,
        description='Related dataset IDs (tableId)',
        validation_alias=AliasChoices('seeAlso', 'see_also'),
        serialization_alias='seeAlso',
    )


class SemanticConstraintAiGuidance(SemanticSchemaBaseModel):
    """AI guidance block for semantic constraints."""

    synopsis: str | None = Field(default=None, max_length=500, description='Brief explanation for AI understanding')
    auto_correct: bool | None = Field(
        default=None,
        description='Whether AI can automatically adjust calculations to satisfy this constraint',
        validation_alias=AliasChoices('autoCorrect', 'auto_correct'),
        serialization_alias='autoCorrect',
    )
    enforcement: str | None = Field(
        default=None,
        max_length=1000,
        description='How AI should handle this constraint',
    )
    pre_query_check: bool | None = Field(
        default=None,
        description='Whether AI should validate this constraint before returning results',
        validation_alias=AliasChoices('preQueryCheck', 'pre_query_check'),
        serialization_alias='preQueryCheck',
    )


class SemanticConstraintScope(str, Enum):
    GLOBAL = 'global'
    PER_LOCATION = 'per_location'
    PER_DATE = 'per_date'
    PER_CATEGORY = 'per_category'


class SemanticConstraintSeverity(str, Enum):
    ERROR = 'error'
    WARNING = 'warning'
    INFO = 'info'


class SemanticConstraintType(str, Enum):
    INEQUALITY = 'inequality'
    EQUALITY = 'equality'
    RANGE = 'range'
    COMPOSITION = 'composition'
    EXCLUSION = 'exclusion'
    TEMPORAL = 'temporal'
    CONDITIONAL = 'conditional'


class SemanticConstraintOperator(str, Enum):
    LT = '<'
    LTE = '<='
    EQ = '='
    GTE = '>='
    GT = '>'
    NEQ = '!='
    BETWEEN = 'between'
    IN = 'in'
    SUM_EQUALS = 'sum_equals'
    RATIO_BETWEEN = 'ratio_between'


class SemanticConstraintRuleBounds(SemanticSchemaBaseModel):
    """Bounds for range-like semantic constraints."""

    max: float | int | None = Field(default=None)
    min: float | int | None = Field(default=None)


class SemanticConstraintRuleExpression(SemanticSchemaBaseModel):
    """Structured rule definition for programmatic evaluation."""

    left: str | None = Field(default=None, description='Left side metric or expression')
    right: str | None = Field(default=None, description='Right side metric, expression, or value')
    bounds: SemanticConstraintRuleBounds | None = Field(default=None, description='For range constraints')
    operator: SemanticConstraintOperator | None = Field(default=None)


class SemanticConstraintValidationQuery(SemanticSchemaBaseModel):
    """Dialect-specific SQL validation queries."""

    default: str | None = Field(default=None, description='Default SQL validation query')
    bigquery: str | None = Field(default=None, description='BigQuery-specific validation query')
    snowflake: str | None = Field(default=None, description='Snowflake-specific validation query')


class SemanticConstraint(SemanticSchemaBaseModel):
    """Schema-backed semantic constraint object."""

    model_config = ConfigDict(populate_by_name=True, extra='forbid')

    model_uuid: UUID = Field(
        description='References the parent semantic-model',
        validation_alias=AliasChoices('modelUUID', 'model_uuid'),
        serialization_alias='modelUUID',
    )
    name: str = Field(
        description='Unique identifier for this constraint within the model',
        min_length=1,
        max_length=255,
        pattern=r'^[a-z][a-z0-9_]*$',
    )
    constraint_type: SemanticConstraintType = Field(
        description='Category of constraint relationship',
        validation_alias=AliasChoices('constraintType', 'constraint_type'),
        serialization_alias='constraintType',
    )
    rule: str = Field(
        description='Logical expression defining the constraint using metric names',
        max_length=1000,
    )
    metrics: list[str] = Field(
        description='List of metric names involved in this constraint',
        min_length=1,
        max_length=20,
    )
    ai: SemanticConstraintAiGuidance | None = Field(default=None, description='AI-specific guidance')
    tags: list[str] | None = Field(default=None, description='Tags for categorization', max_length=20)
    owner: str | None = Field(default=None, max_length=255, description='Team or person responsible')
    scope: SemanticConstraintScope = Field(default=SemanticConstraintScope.GLOBAL, description='Validation scope')
    datasets: list[str] | None = Field(default=None, description='Optional list of datasets involved')
    is_active: bool = Field(
        default=True,
        description='Whether this constraint is currently enforced',
        validation_alias=AliasChoices('isActive', 'is_active'),
        serialization_alias='isActive',
    )
    severity: SemanticConstraintSeverity = Field(default=SemanticConstraintSeverity.ERROR, description='Violation severity')
    description: str | None = Field(default=None, max_length=2000, description='Business explanation')
    display_name: str | None = Field(
        default=None,
        max_length=255,
        description='Human-friendly display name',
        validation_alias=AliasChoices('displayName', 'display_name'),
        serialization_alias='displayName',
    )
    remediation: str | None = Field(default=None, max_length=1000, description='Remediation guidance')
    error_message: str | None = Field(
        default=None,
        max_length=500,
        description='User-friendly violation message',
        validation_alias=AliasChoices('errorMessage', 'error_message'),
        serialization_alias='errorMessage',
    )
    rule_expression: SemanticConstraintRuleExpression | None = Field(
        default=None,
        description='Structured rule definition for programmatic evaluation',
        validation_alias=AliasChoices('ruleExpression', 'rule_expression'),
        serialization_alias='ruleExpression',
    )
    validation_query: SemanticConstraintValidationQuery | None = Field(
        default=None,
        description='SQL queries to validate the constraint holds true',
        validation_alias=AliasChoices('validationQuery', 'validation_query'),
        serialization_alias='validationQuery',
    )


SemanticSchemaObjectModel: TypeAlias = (
    SemanticDataset | SemanticMetric | SemanticRelationship | SemanticGlossary | SemanticConstraint
)


SEMANTIC_OBJECT_TYPE_TO_SCHEMA_MODEL: dict[SemanticObjectType, type[SemanticSchemaBaseModel]] = {
    SemanticObjectType.SEMANTIC_DATASET: SemanticDataset,
    SemanticObjectType.SEMANTIC_METRIC: SemanticMetric,
    SemanticObjectType.SEMANTIC_RELATIONSHIP: SemanticRelationship,
    SemanticObjectType.SEMANTIC_GLOSSARY: SemanticGlossary,
    SemanticObjectType.SEMANTIC_CONSTRAINT: SemanticConstraint,
}


def get_semantic_schema_model(object_type: SemanticObjectType) -> type[SemanticSchemaBaseModel]:
    """Return the typed schema model for the given semantic object type."""

    try:
        return SEMANTIC_OBJECT_TYPE_TO_SCHEMA_MODEL[object_type]
    except KeyError as exc:
        raise ValueError(f'No typed semantic schema model is defined for "{object_type.value}".') from exc
