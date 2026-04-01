"""Semantic read tools backed by the semantic service layer."""

from collections.abc import Sequence
from typing import Annotated, Any

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import process_concurrently, toon_serializer_compact, unwrap_results
from keboola_mcp_server.tools.constants import SEMANTIC_TOOLS_TAG
from keboola_mcp_server.tools.semantic import service as semantic_service
from keboola_mcp_server.tools.semantic.model import (
    SemanticObjectRef,
    SemanticObjectType,
    SemanticObjectTypeSelection,
    SemanticSchemaDefinition,
)


class ConstraintValidationFinding(BaseModel):
    """Tool-facing semantic constraint finding."""

    constraint_id: str = Field(description='Constraint UUID.')
    constraint_name: str = Field(description='Constraint name.')
    severity: str = Field(description='Constraint severity.')
    status: str = Field(description='Validation status.')
    message: str = Field(description='Human-readable validation finding.')
    validation_query: str | None = Field(
        default=None,
        description='Optional SQL validation query suggested by the semantic constraint.',
    )


class CompactSemanticObject(BaseModel):
    id: str
    name: str | None = None


class SemanticModelCompact(CompactSemanticObject):
    description: str | None = None
    sql_dialect: str | None = None

    @classmethod
    def from_semantic_service_data(cls, obj: semantic_service.SemanticServiceData) -> 'SemanticModelCompact':
        attributes = obj.data.attributes or {}
        return cls(
            id=obj.id,
            name=obj.display_name,
            description=attributes.get('description'),
            sql_dialect=attributes.get('sql_dialect'),
        )


class SemanticDatasetCompact(CompactSemanticObject):
    model_config = ConfigDict(populate_by_name=True)

    table_id: str | None = Field(default=None, validation_alias='tableId', serialization_alias='tableId')
    description: str | None = None
    model_uuid: str | None = None
    fqn: str | None = None

    @classmethod
    def from_semantic_service_data(cls, obj: semantic_service.SemanticServiceData) -> 'SemanticDatasetCompact':
        attributes = obj.data.attributes or {}
        return cls(
            id=obj.id,
            name=obj.display_name,
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
    def from_semantic_service_data(cls, obj: semantic_service.SemanticServiceData) -> 'SemanticMetricCompact':
        attributes = obj.data.attributes or {}
        return cls(
            id=obj.id,
            name=obj.display_name,
            description=attributes.get('description'),
            dataset=attributes.get('dataset'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticRelationshipCompact(CompactSemanticObject):
    from_dataset: str | None = None
    to_dataset: str | None = None
    type: str | None = None
    on: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_semantic_service_data(cls, obj: semantic_service.SemanticServiceData) -> 'SemanticRelationshipCompact':
        attributes = obj.data.attributes or {}
        return cls(
            id=obj.id,
            name=obj.display_name,
            from_dataset=attributes.get('from'),
            to_dataset=attributes.get('to'),
            type=attributes.get('type'),
            on=attributes.get('on'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticGlossaryCompact(CompactSemanticObject):
    term: str | None = None
    definition: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_semantic_service_data(cls, obj: semantic_service.SemanticServiceData) -> 'SemanticGlossaryCompact':
        attributes = obj.data.attributes or {}
        return cls(
            id=obj.id,
            name=obj.display_name,
            term=attributes.get('term'),
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
    def from_semantic_service_data(cls, obj: semantic_service.SemanticServiceData) -> 'SemanticConstraintCompact':
        attributes = obj.data.attributes or {}
        return cls(
            id=obj.id,
            name=obj.display_name,
            description=attributes.get('description'),
            type=attributes.get('constraintType'),
            rule=attributes.get('rule'),
            severity=attributes.get('severity'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticObject(CompactSemanticObject):
    attributes: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_semantic_service_data(cls, obj: semantic_service.SemanticServiceData) -> 'SemanticObject':
        return cls(
            id=obj.id,
            name=obj.display_name,
            attributes=obj.data.attributes or {},
        )


SemanticCompactObject = (
    SemanticModelCompact
    | SemanticDatasetCompact
    | SemanticMetricCompact
    | SemanticRelationshipCompact
    | SemanticGlossaryCompact
    | SemanticConstraintCompact
)


SemanticContextObject = SemanticCompactObject | SemanticObject


class SemanticObjectMatchOutput(BaseModel):
    """Matched semantic object returned by semantic search."""

    object_type: SemanticObjectType = Field(description='Matched semantic object type.')
    matched_paths: list[str] = Field(default_factory=list, description='Matched paths inside the semantic object.')
    data: SemanticCompactObject = Field(description='Compact matched semantic object detail.')


class SemanticSearchModelGroup(BaseModel):
    """Search matches grouped by semantic model."""

    semantic_model_id: str = Field(description='Semantic model UUID.')
    matches: list[SemanticObjectMatchOutput] = Field(
        default_factory=list,
        description='Matched objects for this model.',
    )


class SemanticObjectTypeContext(BaseModel):
    """Tool output context for a single semantic object type."""

    object_type: SemanticObjectType = Field(description='Semantic object type.')
    objects: list[SemanticContextObject] = Field(
        default_factory=list,
        description='Semantic objects of the requested type.',
    )


class SemanticUsedDataset(BaseModel):
    """Dataset referenced by the validated SQL."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description='Dataset UUID.')
    name: str = Field(description='Dataset name.')
    table_id: str = Field(description='Keboola table ID.', validation_alias='tableId', serialization_alias='tableId')
    description: str = Field(description='Dataset description.')
    fqn: str = Field(description='Dataset fully qualified SQL name.')

    @classmethod
    def from_semantic_service_data(cls, obj: semantic_service.SemanticDatasetData) -> 'SemanticUsedDataset':
        return cls(
            id=obj.id,
            name=obj.name or '',
            table_id=obj.table_id or '',
            description=obj.description or '',
            fqn=obj.fqn or '',
        )


class SemanticUsedMetric(BaseModel):
    """Metric referenced by the validated SQL."""

    id: str = Field(description='Metric UUID.')
    name: str = Field(description='Metric name.')
    description: str = Field(description='Metric description.')
    sql: str = Field(description='Metric SQL expression.')
    dataset: str = Field(description='Source dataset table ID.')

    @classmethod
    def from_semantic_service_data(cls, obj: semantic_service.SemanticMetricData) -> 'SemanticUsedMetric':
        return cls(
            id=obj.id,
            name=obj.name or '',
            description=obj.description or '',
            sql=obj.sql or '',
            dataset=obj.dataset or '',
        )


class SemanticQueryValidationResult(BaseModel):
    """One semantic validation result view."""

    valid: bool = Field(description='False when an error-severity pre-execution finding was detected.')
    semantic_models: list[SemanticModelCompact] = Field(
        default_factory=list,
        description='Semantic models against which the SQL was validated.',
    )
    sql_dialects: list[str] = Field(
        default_factory=list,
        description=(
            'SQL dialects of the semantic models. '
            'Contains more than one entry when models use different dialects, which is a sign of incompatibility.'
        ),
    )
    used_datasets: list[SemanticUsedDataset] = Field(
        default_factory=list,
        description='Semantic datasets referenced by the SQL.',
    )
    used_metrics: list[SemanticUsedMetric] = Field(
        default_factory=list,
        description='Semantic metrics referenced by the SQL.',
    )
    matched_relationships: list[str] = Field(
        default_factory=list,
        description='Relationship names detected in SQL.',
    )
    violations: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='Pre-execution semantic violations.',
    )
    post_execution_checks: list[ConstraintValidationFinding] = Field(
        default_factory=list,
        description='Checks that should be verified against query results.',
    )
    summary: str = Field(description='Short validation summary.')


class ValidateSemanticQueryOutput(BaseModel):
    """Output for semantic SQL validation."""

    validation_auto_detected: SemanticQueryValidationResult = Field(
        description='Validation result built from semantic objects auto-detected from the SQL query.',
    )
    validation_detected_from_expected: SemanticQueryValidationResult | None = Field(
        default=None,
        description='Validation result built only from explicitly provided expected semantic object IDs.',
    )
    matched_expected_objects: list[SemanticObjectRef] = Field(
        default_factory=list,
        description='Expected semantic objects that were also detected in the SQL.',
    )
    missing_expected_objects: list[SemanticObjectRef] = Field(
        default_factory=list,
        description='Expected semantic objects that were not detected in the SQL.',
    )
    unexpected_detected_objects: list[SemanticObjectTypeContext] = Field(
        default_factory=list,
        description='Detected semantic objects that fall outside the expected semantic scope.',
    )


def _compact_semantic_object(obj: semantic_service.SemanticServiceData) -> CompactSemanticObject:
    if obj.semantic_type == SemanticObjectType.SEMANTIC_MODEL:
        return SemanticModelCompact.from_semantic_service_data(obj)
    elif obj.semantic_type == SemanticObjectType.SEMANTIC_DATASET:
        return SemanticDatasetCompact.from_semantic_service_data(obj)
    elif obj.semantic_type == SemanticObjectType.SEMANTIC_METRIC:
        return SemanticMetricCompact.from_semantic_service_data(obj)
    elif obj.semantic_type == SemanticObjectType.SEMANTIC_RELATIONSHIP:
        return SemanticRelationshipCompact.from_semantic_service_data(obj)
    elif obj.semantic_type == SemanticObjectType.SEMANTIC_GLOSSARY:
        return SemanticGlossaryCompact.from_semantic_service_data(obj)
    elif obj.semantic_type == SemanticObjectType.SEMANTIC_CONSTRAINT:
        return SemanticConstraintCompact.from_semantic_service_data(obj)
    raise ValueError(f'Unsupported semantic object type "{obj.semantic_type.value}"')


def _compare_expected_and_detected_objects(
    expected_semantic_objects: Sequence[SemanticObjectTypeSelection],
    used_object_groups: Sequence[semantic_service.SemanticServiceDataTypeGroup],
) -> tuple[list[SemanticObjectRef], list[SemanticObjectRef], list[SemanticObjectTypeContext]]:
    if not expected_semantic_objects:
        return [], [], []
    expected_ids_by_type: dict[SemanticObjectType, set[str]] = {}
    for selection in expected_semantic_objects:
        if selection.ids:
            expected_ids_by_type.setdefault(selection.object_type, set()).update(selection.ids)
    expected_types = {selection.object_type for selection in expected_semantic_objects}

    matched_expected_objects: list[SemanticObjectRef] = []
    missing_expected_objects: list[SemanticObjectRef] = []
    unexpected_detected_objects: list[SemanticObjectTypeContext] = []

    for object_type, expected_ids in expected_ids_by_type.items():
        detected_ids = {
            obj.id for group in used_object_groups if group.object_type == object_type for obj in group.objects
        }
        matched_expected_objects.extend(
            SemanticObjectRef(object_type=object_type, id=object_id)
            for object_id in sorted(expected_ids & detected_ids)
        )
        missing_expected_objects.extend(
            SemanticObjectRef(object_type=object_type, id=object_id)
            for object_id in sorted(expected_ids - detected_ids)
        )

    for group in used_object_groups:
        selection_ids = expected_ids_by_type.get(group.object_type)
        if group.object_type not in expected_types:
            unexpected_objects = [_compact_semantic_object(obj) for obj in group.objects]
        elif selection_ids:
            unexpected_objects = [_compact_semantic_object(obj) for obj in group.objects if obj.id not in selection_ids]
        else:
            unexpected_objects = []

        if unexpected_objects:
            unexpected_detected_objects.append(
                SemanticObjectTypeContext(object_type=group.object_type, objects=unexpected_objects)
            )

    return (
        matched_expected_objects,
        missing_expected_objects,
        unexpected_detected_objects,
    )


def _to_tool_finding(finding: semantic_service.ConstraintValidationFinding) -> ConstraintValidationFinding:
    return ConstraintValidationFinding(
        constraint_id=finding.constraint_id,
        constraint_name=finding.constraint_name,
        severity=finding.severity,
        status=finding.status,
        message=finding.message,
        validation_query=finding.validation_query,
    )


def _format_validation_result(
    raw_result: semantic_service.SemanticValidationServiceOutput,
    *,
    models: Sequence[semantic_service.SemanticModelData] = tuple(),
    summary_notes: Sequence[str] = tuple(),
) -> SemanticQueryValidationResult:
    used_dataset_objects = []
    used_metric_objects = []
    for group in raw_result.used_object_groups:
        if group.object_type == SemanticObjectType.SEMANTIC_DATASET:
            used_dataset_objects = [item for item in group.objects]
        elif group.object_type == SemanticObjectType.SEMANTIC_METRIC:
            used_metric_objects = [item for item in group.objects]

    used_datasets = [SemanticUsedDataset.from_semantic_service_data(item) for item in used_dataset_objects]
    used_metrics = [SemanticUsedMetric.from_semantic_service_data(item) for item in used_metric_objects]

    semantic_model_outputs = [SemanticModelCompact.from_semantic_service_data(m) for m in models]
    sql_dialects = sorted({m.sql_dialect for m in models if m.sql_dialect})

    summary_parts: list[str] = []
    if len(sql_dialects) > 1:
        summary_parts.append(
            f'Warning: semantic models use different SQL dialects ({", ".join(sql_dialects)}). '
            'The query may not be portable across all models.'
        )
    if raw_result.violations:
        summary_parts.append('Semantic validation found pre-execution issues that should be fixed before running.')
    if raw_result.post_execution_checks:
        summary_parts.append('Some checks should be verified after execution.')
    summary_parts.extend(summary_notes)

    if summary_parts:
        summary = '\n'.join(summary_parts)
    else:
        summary = 'Semantic validation finished without relevant findings.'

    return SemanticQueryValidationResult(
        valid=raw_result.valid,
        semantic_models=semantic_model_outputs,
        sql_dialects=sql_dialects,
        used_datasets=used_datasets,
        used_metrics=used_metrics,
        matched_relationships=raw_result.matched_relationships,
        violations=[_to_tool_finding(finding) for finding in raw_result.violations],
        post_execution_checks=[_to_tool_finding(finding) for finding in raw_result.post_execution_checks],
        summary=summary,
    )


async def _load_expected_object_groups(
    client: KeboolaClient,
    expected_semantic_objects: Sequence[SemanticObjectTypeSelection],
    semantic_model_ids: Sequence[str],
) -> list[semantic_service.SemanticServiceDataTypeGroup]:
    expected_requests = [
        (selection.object_type, object_id.strip())
        for selection in expected_semantic_objects
        for object_id in selection.ids
        if object_id.strip()
    ]
    if not expected_requests:
        return []

    results = await process_concurrently(
        expected_requests,
        lambda item: semantic_service.get_object_by_id(client, item[0], item[1]),
        max_concurrency=min(len(expected_requests), 10),
    )
    objects = unwrap_results(results, 'Failed to fetch expected semantic objects.')

    grouped_objects: dict[SemanticObjectType, list[semantic_service.SemanticServiceData]] = {}
    seen_ids_by_type: dict[SemanticObjectType, set[str]] = {}
    allowed_model_ids = set(semantic_model_ids)

    for (expected_type, object_id), obj in zip(expected_requests, objects, strict=True):
        if obj.semantic_type != expected_type:
            raise ValueError(
                f'Expected semantic object "{object_id}" to be of type "{expected_type.value}", '
                f'got "{obj.semantic_type.value}".'
            )

        object_model_id = (
            obj.id if obj.semantic_type == SemanticObjectType.SEMANTIC_MODEL else getattr(obj, 'model_uuid', None)
        )
        if object_model_id not in allowed_model_ids:
            raise ValueError(
                f'Expected semantic object "{object_id}" does not belong to the provided semantic_model_ids.'
            )

        seen_ids = seen_ids_by_type.setdefault(obj.semantic_type, set())
        if obj.id in seen_ids:
            continue

        seen_ids.add(obj.id)
        grouped_objects.setdefault(obj.semantic_type, []).append(obj)

    return [
        semantic_service.SemanticServiceDataTypeGroup(object_type=object_type, objects=objects)
        for object_type, objects in grouped_objects.items()
    ]


def add_semantic_tools(mcp: FastMCP) -> None:
    """Register semantic read tools."""
    mcp.add_tool(
        FunctionTool.from_function(
            search_semantic_context,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEMANTIC_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_semantic_context,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEMANTIC_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_semantic_schema,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEMANTIC_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            validate_semantic_query,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEMANTIC_TOOLS_TAG},
        )
    )


@tool_errors()
async def search_semantic_context(
    ctx: Context,
    patterns: Annotated[
        Sequence[str],
        Field(
            description=(
                'One or more regex patterns used to search semantic metadata. '
                'The search checks semantic model names plus semantic object names and nested attribute values. '
                'Use multiple patterns when you need to find objects related to several business terms at once.'
            )
        ),
    ],
    semantic_types: Annotated[
        Sequence[SemanticObjectType],
        Field(
            description=(
                'Optional semantic object types to search. '
                'Empty list [] means ALL semantic object types are searched. '
                'Use this to narrow the search when you already know whether you want datasets, metrics, '
                'relationships, glossary terms, constraints, or models.'
            )
        ),
    ] = tuple(),
    semantic_model_ids: Annotated[
        Sequence[str],
        Field(
            description=(
                'Optional list of semantic model IDs to restrict the search to specific models. '
                'Empty list [] means search across all semantic models.'
            )
        ),
    ] = tuple(),
    case_sensitive: Annotated[
        bool,
        Field(
            description=(
                'Whether regex matching should be case-sensitive. '
                'Leave false for normal discovery; set true only when exact casing matters.'
            )
        ),
    ] = False,
    max_results: Annotated[
        int,
        Field(
            description=(
                'Maximum number of matched semantic objects to return. '
                'Use a smaller value for quick discovery and a larger value only when you need a broader result set.'
            )
        ),
    ] = 100,
) -> list[SemanticSearchModelGroup]:
    """
    Searches semantic models and semantic objects using regex patterns matched against their names, descriptions and
    stringified JSON attributes.

    Returns compact matches grouped by semantic model. Each match includes the semantic object type,
    the paths where the patterns matched, and compact object view.

    CONSIDERATIONS:
    - The search is case-insensitive by default. Use `case_sensitive=True` when exact casing matters.
    - The search is performed against semantic object names and data attributes which are stringified JSON objects
    following their corresponding JSON schema.
    - The search can be scoped to specific semantic models or semantic object types but prefer broader search without
    scoping unless required by the context.

    WHEN TO USE:
    - When you need to discover which semantic objects are relevant to a user request.
    - When you know business terms, column names, metric fragments, or rule names, but not exact object UUIDs.
    - When you need to find semantic objects by keyword or values used in their attributes.

    WHEN NOT TO USE:
    - When you know the exact IDs.

    EXAMPLES:
    - Find semantic objects by business concepts for revenue or sales:
      `patterns=["revenue", "sales"]`
    - Find semantic objects using a Keboola table ID:
      `patterns=["out.c-sales-main.fact_orders"]`
    - Find semantic dataset for a certain table:
      `patterns=["in.c-sales-main.fact_orders"], semantic_types=["semantic-dataset"]`
    - Find semantic datasets that mention a column name:
      `patterns=["column_name"], semantic_types=["semantic-dataset"]`
    - Search semantic objects e.g. semantic metrics, relationships, and constraints using a certain semantic dataset:
      `patterns=["table-id-of-the-dataset"], semantic_types=["semantic-metric",`
      `"semantic-relationship", "semantic-constraint"]`
    - Search semantic constraints using e.g. certain semantic metrics and certain semantic datasets:
      `patterns=["metric-name-1", "metric-name-2", "table-id-from-the-dataset"],`
      `semantic_types=["semantic-metric", "semantic-relationship"]`
    - Search something within specific semantic models only:
      `patterns=["something"], semantic_model_ids=["<semantic-model-uuid-1>", "<semantic-model-uuid-2>"]`
    """
    cleaned_patterns = [pattern.strip() for pattern in patterns if pattern and pattern.strip()]
    if not cleaned_patterns:
        raise ValueError('At least one regex pattern must be provided.')
    if max_results <= 0:
        raise ValueError('max_results must be a positive integer.')

    client = KeboolaClient.from_state(ctx.session.state)
    hits = await semantic_service.search_semantic_context(
        client,
        cleaned_patterns,
        semantic_types=semantic_types,
        semantic_model_ids=semantic_model_ids or None,
        case_sensitive=case_sensitive,
        max_results=max_results,
    )

    grouped_matches: dict[str, list[SemanticObjectMatchOutput]] = {}

    for hit in hits:
        grouped_matches.setdefault(hit.semantic_model_id, []).append(
            SemanticObjectMatchOutput(
                object_type=hit.object_type,
                matched_paths=list(hit.matched_paths),
                data=_compact_semantic_object(hit.object),
            )
        )

    model_results = [
        SemanticSearchModelGroup(
            semantic_model_id=model_id,
            matches=sorted(grouped, key=lambda item: item.data.name or item.data.id),
        )
        for model_id, grouped in grouped_matches.items()
    ]
    model_results.sort(key=lambda item: item.semantic_model_id)

    return model_results


@tool_errors()
async def get_semantic_context(
    ctx: Context,
    semantic_objects: Annotated[
        Sequence[SemanticObjectTypeSelection],
        Field(
            description=(
                'List of semantic object selections to load. '
                'Each item contains "object_type" and optional "ids". '
                'If "ids" is empty, all objects of that type are returned in compact form. '
                'If "ids" is non-empty, only those objects are returned with full attributes.'
            )
        ),
    ],
    semantic_model_ids: Annotated[
        Sequence[str],
        Field(
            description=(
                'Optional list of semantic model IDs to restrict loading to specific models. '
                'Empty list [] means load across all semantic models.'
            )
        ),
    ] = tuple(),
) -> list[SemanticObjectTypeContext]:
    """
    Loads semantic objects grouped by semantic object type.

    CONSIDERATIONS:
    - If a selection has empty `ids`, the tool returns all objects of that type in compact form.
    - If a selection has non-empty `ids`, the tool returns only those specific objects with full attributes.
    - `semantic_model_ids` optionally narrows the lookup to specific semantic models.

    WHEN TO USE:
    - When you already know IDs of the semantic objects you want to load and want to inspect them in detail.
    - When you want to list all semantic objects of certain types or specific semantic models.
    - When you want to list semantic models.

    WHEN NOT TO USE:
    - When you need to discover semantic objects.

    EXAMPLES:
    - List all semantic models:
      `semantic_objects=[{"object_type": "semantic-model"}]`
    - List semantic datasets and metrics for specific semantic models:
      `semantic_objects=[{"object_type": "semantic-dataset"}, {"object_type": "semantic-metric"}],`
      `semantic_model_ids=["model-uuid-1", "model-uuid-2"]`
    - Get detailed context for specific semantic objects by their id:
      `semantic_objects=[{"object_type": "semantic-dataset", "ids": ["dataset-uuid-1"]},`
      `{"object_type": "semantic-metric", "ids": ["metric-uuid-1", "metric-uuid-2"]}]`
    - List all constraints for specific semantic models:
      `semantic_objects=[{"object_type": "semantic-constraint"}], semantic_model_ids=["model-uuid-1"]`
    """
    if not semantic_objects:
        raise ValueError('At least one semantic object type must be provided.')

    client = KeboolaClient.from_state(ctx.session.state)

    results = await process_concurrently(
        semantic_objects,
        lambda selection: semantic_service.load_semantic_context_for_semantic_type(
            client, selection.object_type, semantic_model_ids=semantic_model_ids or None, ids=selection.ids
        ),
        max_concurrency=min(len(semantic_objects), 10),
    )
    groups = unwrap_results(results, 'Failed to fetch semantic context.')

    # Normalize the contexts to the SemanticObjectTypeContext format
    normalized_contexts: list[SemanticObjectTypeContext] = []
    for selection, context in zip(semantic_objects, groups, strict=True):
        assert isinstance(
            context, semantic_service.SemanticServiceDataTypeGroup
        ), f'Expected SemanticServiceDataTypeGroup, got {type(context)}'
        assert (
            selection.object_type == context.object_type
        ), f'Semantic object type mismatch: {selection.object_type} != {context.object_type}'
        if selection.ids:
            # Detail context with specific IDs
            normalized_contexts.append(
                SemanticObjectTypeContext(
                    object_type=context.object_type,
                    objects=[SemanticObject.from_semantic_service_data(obj) for obj in context.objects],
                )
            )
        else:
            normalized_contexts.append(
                SemanticObjectTypeContext(
                    object_type=context.object_type,
                    objects=[_compact_semantic_object(obj) for obj in context.objects],
                )
            )

    return normalized_contexts


@tool_errors()
async def get_semantic_schema(
    ctx: Context,
    semantic_types: Annotated[
        Sequence[SemanticObjectType],
        Field(
            description=(
                'List of semantic object types for which JSON schemas should be returned. '
                'Each returned item contains the requested semantic type and its metastore schema.'
            )
        ),
    ],
) -> list[SemanticSchemaDefinition]:
    """
    Returns JSON schemas for the requested semantic object types.

    WHEN TO USE:
    - When you want to know the JSON schema of a semantic object type, e.g. before searching something specific.

    """
    if not semantic_types:
        raise ValueError('At least one semantic type must be provided.')

    client = KeboolaClient.from_state(ctx.session.state)
    results = await process_concurrently(
        semantic_types,
        lambda semantic_type: client.metastore_client.get_schema(semantic_type.value),
        max_concurrency=min(len(semantic_types), 10),
    )
    schemas = unwrap_results(results, 'Failed to fetch one or more semantic schemas.')

    return [
        SemanticSchemaDefinition(semantic_type=semantic_type, schema_definition=schema)
        for semantic_type, schema in zip(semantic_types, schemas, strict=True)
    ]


@tool_errors()
async def validate_semantic_query(
    ctx: Context,
    sql_query: Annotated[
        str,
        Field(
            description=(
                'SQL query that should be checked against the semantic layer. '
                'The query is not executed; the tool performs best-effort semantic detection and rule validation '
                'using heuristic string matching, so the detected objects may be incomplete or imperfect.'
            )
        ),
    ],
    semantic_model_ids: Annotated[
        Sequence[str],
        Field(
            description=(
                'One or more semantic model IDs against which the SQL should be validated. '
                'Contexts from all models are merged into a single universe for object detection. '
                'Constraint evaluation is performed per model to avoid cross-model rule contamination.'
            )
        ),
    ],
    expected_semantic_objects: Annotated[
        Sequence[SemanticObjectTypeSelection],
        Field(
            description=(
                'Optional semantic object selections that define the expected semantic scope of the query. '
                'These expectations are compared with the objects actually detected in the SQL. '
                'Use `ids` when you want to assert that specific semantic objects should be present.'
            )
        ),
    ] = tuple(),
) -> ValidateSemanticQueryOutput:
    """
    Performs best-effort semantic validation of an SQL query against one or more semantic models and compares it with
    the expected semantic objects provided.

    RETURNS:
    - `validation_auto_detected`: semantic validation built from objects heuristically detected in the SQL
    - `validation_detected_from_expected`: semantic validation built only from explicitly provided expected object IDs
    - expected semantic objects that were matched or missing in the auto-detected result
    - unexpected auto-detected objects outside the expected semantic scope

    LIMITATIONS:
    - Detection is heuristic and based on string matching over SQL and semantic metadata.
    - The tool does not parse SQL semantically and does not execute the query.
    - Auto-detected objects, missing objects, and relationship matches may therefore be imperfect.
    - Use the result as a best-effort semantic check, not as a formal proof that the query is correct.

    CONSIDERATIONS:
    -  Prefer calling this tool before executing any SQL that touches semantic objects.
    - This tool confirms the SQL dialect, surfaces semantic constraint violations, and provides post-execution checks.
    - Only proceed to query_data once this tool returns valid=True and violations is empty. If violations are found,
    fix the query first or consider the limitations of this tool.

    WHEN TO USE:
    - Before generating or approving a query that should follow a semantic model.
    - When you want to validate a SQL query against the semantic objects before executing it using "query_data" tool
    or creating a new SQL transformation out of it, especially when investigating data quality issues.
    - When you want to verify that a query uses the intended semantic objects.
    - When you need to surface semantic business-rule violations or follow-up checks.

    EXAMPLES:
    - Validate a SQL query against one semantic model:
      `sql_query="SELECT SUM(\\"REVENUE\\") FROM ...", semantic_model_ids=["semantic-model-uuid"],`
      `expected_semantic_objects=[{"object_type": "semantic-dataset"}]`
    - Validate a cross-model query against two semantic models:
      `sql_query="SELECT * FROM ...", semantic_model_ids=["model-uuid-1", "model-uuid-2"],`
      `expected_semantic_objects=[{"object_type": "semantic-dataset", "ids": ["dataset-uuid-1"]}]`
    - Validate a query and compare it against expected objects:
      `sql_query="SELECT SUM(\\"REVENUE\\") FROM ...", semantic_model_ids=["semantic-model-uuid"],`
      `expected_semantic_objects=[{"object_type": "semantic-metric", "ids": ["metric-uuid-1"]}]`

    """
    if not sql_query.strip():
        raise ValueError('sql_query must not be empty.')
    cleaned_model_ids = list(dict.fromkeys(mid.strip() for mid in semantic_model_ids if mid and mid.strip()))
    if not cleaned_model_ids:
        raise ValueError('At least one semantic_model_id must be provided.')

    client = KeboolaClient.from_state(ctx.session.state)

    model_results = await process_concurrently(
        cleaned_model_ids,
        lambda model_id: semantic_service.get_object_by_id(client, SemanticObjectType.SEMANTIC_MODEL, model_id),
        max_concurrency=min(len(cleaned_model_ids), 10),
    )
    models = unwrap_results(model_results, 'Failed to fetch semantic models.')
    assert all(isinstance(m, semantic_service.SemanticModelData) for m in models)

    # Pre-load contexts once when both validation paths will run, avoiding a double round-trip.
    pre_loaded_contexts = None
    if expected_semantic_objects:
        pre_loaded_contexts = await semantic_service._load_validation_contexts(client, cleaned_model_ids)

    raw_auto_detected = await semantic_service.validate_semantic_query(
        client, sql_query, cleaned_model_ids, contexts_per_model=pre_loaded_contexts
    )
    expected_object_groups = await _load_expected_object_groups(client, expected_semantic_objects, cleaned_model_ids)
    raw_from_expected = None
    if expected_object_groups:
        raw_from_expected = await semantic_service.validate_semantic_used_objects(
            client, cleaned_model_ids, expected_object_groups, contexts_per_model=pre_loaded_contexts
        )

    matched_expected_objects, missing_expected_objects, unexpected_detected_objects = (
        _compare_expected_and_detected_objects(expected_semantic_objects, raw_auto_detected.used_object_groups)
    )
    auto_detected_summary_notes: list[str] = []
    if missing_expected_objects:
        auto_detected_summary_notes.append('Some expected semantic objects were not detected in the SQL query.')
    if unexpected_detected_objects:
        auto_detected_summary_notes.append('Some detected semantic objects fall outside the expected semantic scope.')

    return ValidateSemanticQueryOutput(
        validation_auto_detected=_format_validation_result(
            raw_auto_detected,
            models=models,
            summary_notes=auto_detected_summary_notes,
        ),
        validation_detected_from_expected=(
            _format_validation_result(raw_from_expected, models=models) if raw_from_expected is not None else None
        ),
        matched_expected_objects=matched_expected_objects,
        missing_expected_objects=missing_expected_objects,
        unexpected_detected_objects=unexpected_detected_objects,
    )
