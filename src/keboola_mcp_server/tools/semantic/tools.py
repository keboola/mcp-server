"""Semantic layer MCP tools backed by Metastore."""

import asyncio
from difflib import SequenceMatcher
from typing import Annotated, Any, Literal, Sequence

import jsonschema
from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ClientCapabilities, SamplingCapability, ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import INTERNAL_META_FIELDS, JsonApiResource
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.tools.constants import SEMANTIC_TOOLS_TAG
from keboola_mcp_server.tools.semantic.model import (
    ENTITY_TO_OBJECT_TYPE,
    OBJECT_TO_ENTITY_TYPE,
    ConstraintCheck,
    QueryPlanJoin,
    SemanticDefineAction,
    SemanticDefineOutput,
    SemanticDiscoverMatch,
    SemanticDiscoverOutput,
    SemanticEntityType,
    SemanticFilter,
    SemanticGetDefinitionOutput,
    SemanticModelSummary,
    SemanticObjectDefinition,
    SemanticObjectType,
    SemanticQueryPlan,
    SemanticQueryPlanOutput,
    SemanticScope,
    SemanticSource,
    ToolStatus,
)

SEARCH_ENTITY_TYPES: tuple[SemanticEntityType, ...] = (
    SemanticEntityType.METRIC,
    SemanticEntityType.DATASET,
    SemanticEntityType.GLOSSARY,
    SemanticEntityType.CONSTRAINT,
    SemanticEntityType.RELATIONSHIP,
)


def add_semantic_tools(mcp: KeboolaMcpServer) -> None:
    """Add semantic layer tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            semantic_discover,
            tags={SEMANTIC_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            semantic_get_definition,
            tags={SEMANTIC_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            semantic_query_plan,
            tags={SEMANTIC_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            semantic_define,
            tags={SEMANTIC_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
        )
    )


def _extract_semantic_data(attributes: dict[str, Any]) -> dict[str, Any]:
    nested_data = attributes.get('data')
    if isinstance(nested_data, dict):
        return dict(nested_data)
    ignored = INTERNAL_META_FIELDS - {'name'}
    return {key: value for key, value in attributes.items() if key not in ignored}


def _resource_to_definition(resource: JsonApiResource, object_type: SemanticObjectType) -> SemanticObjectDefinition:
    data = _extract_semantic_data(resource.attributes)
    name = str(data.get('name') or resource.attributes.get('name') or resource.id or '')
    return SemanticObjectDefinition(
        entity_type=OBJECT_TO_ENTITY_TYPE[object_type],
        object_type=object_type,
        uuid=resource.id,
        name=name,
        data=data,
    )


def _resource_source(resource: JsonApiResource, object_type: SemanticObjectType) -> SemanticSource:
    data = _extract_semantic_data(resource.attributes)
    revision_raw = resource.meta.get('revision') if isinstance(resource.meta, dict) else None
    if revision_raw is None:
        revision_raw = resource.attributes.get('revision')
    revision = int(revision_raw) if isinstance(revision_raw, int | str) and str(revision_raw).isdigit() else None

    model_id = data.get('modelUUID')
    if object_type == SemanticObjectType.SEMANTIC_MODEL:
        model_id = resource.id

    return SemanticSource(
        object_type=object_type.value,
        uuid=resource.id,
        model_id=model_id if isinstance(model_id, str) else None,
        revision=revision,
    )


def _get_project_id(resource: JsonApiResource) -> int | None:
    if isinstance(resource.meta.get('projectId'), int):
        return resource.meta.get('projectId')
    value = resource.attributes.get('projectId')
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _fuzzy_score(query: str, text: str) -> float:
    """Compute fuzzy similarity between query and text using difflib."""
    return SequenceMatcher(None, query.lower(), text.lower()).ratio()


class _SamplingRanking(BaseModel):
    ranked_indices: list[int]


async def _rank_with_sampling(ctx: Context, query: str, blobs: list[str]) -> list[float]:
    """Ask the LLM to rank candidates by semantic relevance; returns score per candidate."""
    numbered = '\n'.join(f'{i}: {blob}' for i, blob in enumerate(blobs))
    result = await ctx.sample(
        messages=f'Rank the following semantic objects by relevance to: "{query}"\n\n{numbered}',
        system_prompt='Return only the indices in descending relevance order.',
        result_type=_SamplingRanking,
        max_tokens=256,
    )
    scores = [0.0] * len(blobs)
    for rank, idx in enumerate(result.result.ranked_indices):
        if 0 <= idx < len(blobs):
            scores[idx] = 1.0 - rank / len(blobs)
    return scores


async def _rank_candidates(ctx: Context, query: str, blobs: list[str]) -> list[float]:
    """Rank candidates using LLM sampling if available, otherwise fuzzy lexical scoring."""
    supports_sampling = ctx.session.check_client_capability(ClientCapabilities(sampling=SamplingCapability()))
    if supports_sampling:
        try:
            return await _rank_with_sampling(ctx, query, blobs)
        except Exception:
            pass
    return [_fuzzy_score(query, blob) for blob in blobs]


def _build_search_blob(data: dict[str, Any]) -> str:
    fields = data.get('fields') if isinstance(data.get('fields'), list) else []
    field_names = ' '.join(str(field.get('name', '')) for field in fields if isinstance(field, dict))
    ai_blob = str(data.get('ai', ''))
    terms = [
        str(data.get('name', '')),
        str(data.get('term', '')),
        str(data.get('description', '')),
        str(data.get('rule', '')),
        str(data.get('tableId', '')),
        field_names,
        ai_blob,
    ]
    return ' '.join(terms)


async def _list_objects_by_scope(
    client: KeboolaClient,
    object_type: SemanticObjectType,
    scope: SemanticScope,
) -> list[JsonApiResource]:
    return await client.metastore_client.list_objects(
        object_type.value,
        organization_scope=(scope == SemanticScope.ORGANIZATION),
    )


@tool_errors()
async def semantic_discover(
    ctx: Context,
    query: Annotated[
        str | None,
        Field(description='Optional free-text query for semantic discovery.'),
    ] = None,
    entity_types: Annotated[
        Sequence[SemanticEntityType],
        Field(description='Optional entity type filters for discovery ranking.'),
    ] = tuple(),
    project_id: Annotated[
        int | None,
        Field(description='Optional project ID filter.'),
    ] = None,
    model_id: Annotated[
        str | None,
        Field(description='Optional semantic model UUID filter.'),
    ] = None,
    scope: Annotated[
        Literal['project', 'organization'],
        Field(description='Repository scope for list operations.'),
    ] = 'project',
    limit: Annotated[
        int,
        Field(description='Maximum number of ranked matches to return.'),
    ] = 10,
) -> SemanticDiscoverOutput:
    """
    Discovers semantic definitions relevant to user intent and returns ranked candidates.

    WHEN TO USE:
    - First step for analytical questions to find semantic entities before planning SQL.
    - Also for inventory mode (query omitted) to list available semantic models and model statistics.

    RULES:
    - If `query` is empty, only model inventory is returned (no ranked matches).
    - If `scope=\"organization\"`, search uses organization listing endpoints.
    - Ranking is deterministic lexical matching over semantic object content.

    OUTPUT CONTRACT:
    - Always returns `status`.
    - Returns `matches` sorted by descending score and deterministic tie-break by name.
    - Sets `next_action=\"semantic_get_definition\"` when at least one match exists.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    scope_enum = SemanticScope(scope)

    model_resources = await _list_objects_by_scope(client, SemanticObjectType.SEMANTIC_MODEL, scope_enum)
    if project_id is not None:
        model_resources = [resource for resource in model_resources if _get_project_id(resource) == project_id]

    dataset_resources, metric_resources = await asyncio.gather(
        _list_objects_by_scope(client, SemanticObjectType.SEMANTIC_DATASET, scope_enum),
        _list_objects_by_scope(client, SemanticObjectType.SEMANTIC_METRIC, scope_enum),
    )

    dataset_count_by_model: dict[str, int] = {}
    for resource in dataset_resources:
        data = _extract_semantic_data(resource.attributes)
        resource_model_id = data.get('modelUUID')
        if isinstance(resource_model_id, str):
            dataset_count_by_model[resource_model_id] = dataset_count_by_model.get(resource_model_id, 0) + 1

    metric_count_by_model: dict[str, int] = {}
    for resource in metric_resources:
        data = _extract_semantic_data(resource.attributes)
        resource_model_id = data.get('modelUUID')
        if isinstance(resource_model_id, str):
            metric_count_by_model[resource_model_id] = metric_count_by_model.get(resource_model_id, 0) + 1

    models: list[SemanticModelSummary] = []
    for resource in model_resources:
        model_data = _extract_semantic_data(resource.attributes)
        current_model_id = resource.id or ''
        if model_id and current_model_id != model_id:
            continue
        models.append(
            SemanticModelSummary(
                model_id=current_model_id,
                name=str(model_data.get('name') or resource.attributes.get('name') or current_model_id),
                scope=scope_enum,
                project_id=_get_project_id(resource),
                status=model_data.get('status') if isinstance(model_data.get('status'), str) else None,
                revision=_resource_source(resource, SemanticObjectType.SEMANTIC_MODEL).revision,
                dataset_count=dataset_count_by_model.get(current_model_id, 0),
                metric_count=metric_count_by_model.get(current_model_id, 0),
            )
        )

    if not query:
        return SemanticDiscoverOutput(status=ToolStatus.OK, models=models, matches=[])

    target_entity_types = tuple(entity_types) if entity_types else SEARCH_ENTITY_TYPES

    # Collect all candidate resources before scoring so ranking can be batched
    _candidates: list[tuple[SemanticEntityType, SemanticObjectType, JsonApiResource, dict[str, Any]]] = []
    for entity_type in target_entity_types:
        object_type = ENTITY_TO_OBJECT_TYPE[entity_type]
        resources = await _list_objects_by_scope(client, object_type, scope_enum)
        for resource in resources:
            data = _extract_semantic_data(resource.attributes)
            if model_id and data.get('modelUUID') != model_id:
                continue
            if project_id is not None and _get_project_id(resource) != project_id:
                continue
            _candidates.append((entity_type, object_type, resource, data))

    ranked_matches: list[SemanticDiscoverMatch] = []
    if _candidates:
        blobs = [_build_search_blob(data) for _, _, _, data in _candidates]
        scores = await _rank_candidates(ctx, query, blobs)
        for (entity_type, object_type, resource, data), score in zip(_candidates, scores):
            if score <= 0.0:
                continue
            source = _resource_source(resource, object_type)
            ranked_matches.append(
                SemanticDiscoverMatch(
                    entity_type=entity_type,
                    object_type=object_type,
                    uuid=resource.id,
                    model_id=source.model_id,
                    name=str(data.get('name') or data.get('term') or resource.id or ''),
                    match_score=score,
                    revision=source.revision,
                )
            )

    ranked_matches = sorted(ranked_matches, key=lambda item: (-item.match_score, item.name))[: max(limit, 0)]

    return SemanticDiscoverOutput(
        status=ToolStatus.OK,
        models=models,
        matches=ranked_matches,
        next_action='semantic_get_definition' if ranked_matches else None,
    )


async def _find_by_name(
    client: KeboolaClient,
    object_type: SemanticObjectType,
    name: str,
    model_id: str | None,
) -> JsonApiResource | None:
    resources = await client.metastore_client.list_objects(object_type.value)
    normalized_name = name.lower()

    candidates: list[JsonApiResource] = []
    for resource in resources:
        data = _extract_semantic_data(resource.attributes)
        candidate_name = str(data.get('name') or data.get('term') or resource.attributes.get('name') or '')
        if candidate_name.lower() != normalized_name:
            continue
        if model_id and data.get('modelUUID') != model_id:
            continue
        candidates.append(resource)

    return candidates[0] if candidates else None


@tool_errors()
async def semantic_get_definition(
    ctx: Context,
    entity_type: Annotated[
        Literal['model', 'dataset', 'metric', 'relationship', 'glossary', 'constraint'],
        Field(description='Semantic entity type to retrieve.'),
    ],
    name: Annotated[
        str | None,
        Field(description='Semantic object name (or glossary term) for lookup.'),
    ] = None,
    uuid: Annotated[
        str | None,
        Field(description='Semantic object UUID for direct retrieval.'),
    ] = None,
    model_id: Annotated[
        str | None,
        Field(description='Optional semantic model UUID to disambiguate name lookup.'),
    ] = None,
    include_schema: Annotated[
        bool,
        Field(description='Include metastore JSON schema in response.'),
    ] = False,
    revision: Annotated[
        int | None,
        Field(description='Optional revision number for historical retrieval.'),
    ] = None,
) -> SemanticGetDefinitionOutput:
    """
    Retrieves one canonical semantic definition by UUID or by name with optional model disambiguation.

    WHEN TO USE:
    - After `semantic_discover` to fetch the authoritative object used for reasoning or citation.
    - Whenever caller needs deterministic source metadata (uuid/model/revision).

    RULES:
    - Exactly one selector is required: `uuid` or `name`.
    - If `revision` is supplied, lookup is executed against revision endpoint (requires `uuid`).
    - Optional `include_schema` attaches current JSON schema for the selected object type.

    OUTPUT CONTRACT:
    - Returns `defined=true` with canonical `definition` when object is found.
    - Returns `defined=false` and `next_action=\"semantic_discover\"` when not found.
    """
    if not uuid and not name:
        raise ValueError('Provide either uuid or name to retrieve semantic definition.')
    if uuid and name:
        raise ValueError('Provide either uuid or name, not both.')

    client = KeboolaClient.from_state(ctx.session.state)
    entity_type_enum = SemanticEntityType(entity_type)
    object_type = ENTITY_TO_OBJECT_TYPE[entity_type_enum]

    resource: JsonApiResource | None
    if uuid and revision is not None:
        resource = await client.metastore_client.get_revision(object_type.value, uuid, revision)
    elif uuid:
        resource = await client.metastore_client.get_object(object_type.value, uuid)
    else:
        assert name is not None
        resource = await _find_by_name(client, object_type, name=name, model_id=model_id)

    if resource is None:
        return SemanticGetDefinitionOutput(
            status=ToolStatus.OK,
            defined=False,
            reason=f'{entity_type} definition not found.',
            next_action='semantic_discover',
        )

    definition = _resource_to_definition(resource, object_type)
    source = _resource_source(resource, object_type)

    schema: dict[str, Any] | None = None
    if include_schema:
        schema_document = await client.metastore_client.get_schema(object_type.value)
        schema = schema_document.model_dump(exclude_none=True)

    return SemanticGetDefinitionOutput(
        status=ToolStatus.OK,
        defined=True,
        definition=definition,
        definition_schema=schema,
        source=source,
        next_action='semantic_query_plan' if entity_type_enum == SemanticEntityType.METRIC else None,
    )


def _collect_field_names(dataset_data: dict[str, Any]) -> set[str]:
    fields = dataset_data.get('fields') if isinstance(dataset_data.get('fields'), list) else []
    names = set()
    for field in fields:
        if isinstance(field, dict) and isinstance(field.get('name'), str):
            names.add(field['name'])
    return names


@tool_errors()
async def semantic_query_plan(
    ctx: Context,
    metric_name: Annotated[
        str,
        Field(description='Metric name to plan query for.'),
    ],
    dimensions: Annotated[
        Sequence[str],
        Field(description='Optional list of requested dimensions.'),
    ] = tuple(),
    time_grain: Annotated[
        str | None,
        Field(description='Optional requested time grain, e.g. day/week/month.'),
    ] = None,
    filters: Annotated[
        Sequence[SemanticFilter],
        Field(description='Optional normalized filters for query planning.'),
    ] = tuple(),
    model_id: Annotated[
        str | None,
        Field(description='Optional semantic model UUID to disambiguate the metric.'),
    ] = None,
    strict: Annotated[
        bool,
        Field(description='If true, unresolved dimensions and severe constraints invalidate the plan.'),
    ] = True,
) -> SemanticQueryPlanOutput:
    """
    Builds a structured query plan from semantic metric definition and related model objects.

    WHEN TO USE:
    - Before SQL generation/execution for metric-based analytical questions.
    - To validate requested dimensions and constraints in strict semantic mode.

    RULES:
    - Metric must exist in semantic layer; otherwise returns `defined=false`.
    - Planner resolves source dataset, relationship joins, and relevant constraints.
    - `strict=true` marks plan invalid when error-severity warnings are present.

    OUTPUT CONTRACT:
    - Returns `defined`, `valid`, `plan`, `warnings`, and `post_execution_checks`.
    - Sets `next_action=\"query_data\"` for valid plans, otherwise `semantic_get_definition`.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    metric_resource = await _find_by_name(client, SemanticObjectType.SEMANTIC_METRIC, metric_name, model_id=model_id)
    if metric_resource is None:
        return SemanticQueryPlanOutput(
            status=ToolStatus.OK,
            defined=False,
            valid=False,
            reason=f"Metric '{metric_name}' is not defined in semantic layer.",
            next_action='semantic_define',
        )

    metric_data = _extract_semantic_data(metric_resource.attributes)
    source = _resource_source(metric_resource, SemanticObjectType.SEMANTIC_METRIC)
    effective_model_id = source.model_id

    dataset_resources, relationship_resources, constraint_resources = await asyncio.gather(
        client.metastore_client.list_objects(SemanticObjectType.SEMANTIC_DATASET.value),
        client.metastore_client.list_objects(SemanticObjectType.SEMANTIC_RELATIONSHIP.value),
        client.metastore_client.list_objects(SemanticObjectType.SEMANTIC_CONSTRAINT.value),
    )

    datasets_by_table_id: dict[str, dict[str, Any]] = {}
    for resource in dataset_resources:
        dataset_data = _extract_semantic_data(resource.attributes)
        table_id = dataset_data.get('tableId')
        if not isinstance(table_id, str):
            continue
        if effective_model_id and dataset_data.get('modelUUID') != effective_model_id:
            continue
        datasets_by_table_id[table_id] = dataset_data

    source_dataset_id = metric_data.get('dataset') if isinstance(metric_data.get('dataset'), str) else None
    source_dataset = datasets_by_table_id.get(source_dataset_id or '')

    source_field_names = _collect_field_names(source_dataset or {})
    resolved_dimensions = set(dimensions) & source_field_names

    joins: list[QueryPlanJoin] = []
    joined_field_names: set[str] = set()
    if source_dataset_id:
        for resource in relationship_resources:
            rel = _extract_semantic_data(resource.attributes)
            if effective_model_id and rel.get('modelUUID') != effective_model_id:
                continue
            from_table = rel.get('from')
            to_table = rel.get('to')
            if not isinstance(from_table, str) or not isinstance(to_table, str):
                continue
            if source_dataset_id not in {from_table, to_table}:
                continue

            target_table = to_table if from_table == source_dataset_id else from_table
            target_dataset = datasets_by_table_id.get(target_table)
            if target_dataset:
                joined_field_names.update(_collect_field_names(target_dataset))

            joins.append(
                QueryPlanJoin(
                    from_table_id=from_table,
                    to_table_id=to_table,
                    join_type=str(rel.get('type', 'left')),
                    on=str(rel.get('on', '')),
                )
            )

    for dimension in dimensions:
        if dimension in joined_field_names:
            resolved_dimensions.add(dimension)

    unresolved_dimensions = [dimension for dimension in dimensions if dimension not in resolved_dimensions]

    warnings: list[ConstraintCheck] = []
    post_execution_checks: list[ConstraintCheck] = []

    if unresolved_dimensions:
        warnings.append(
            ConstraintCheck(
                name='dimension-resolution',
                constraint_type='dimension',
                severity='error' if strict else 'warning',
                rule=', '.join(unresolved_dimensions),
                status='unresolved_dimensions',
                note=f'Unresolved dimensions: {", ".join(unresolved_dimensions)}',
            )
        )

    for resource in constraint_resources:
        constraint_data = _extract_semantic_data(resource.attributes)
        if effective_model_id and constraint_data.get('modelUUID') != effective_model_id:
            continue

        metrics = constraint_data.get('metrics') if isinstance(constraint_data.get('metrics'), list) else []
        metric_matches = metric_name in metrics
        datasets = constraint_data.get('datasets') if isinstance(constraint_data.get('datasets'), list) else []
        dataset_matches = source_dataset_id in datasets if source_dataset_id else False
        if not metric_matches and not dataset_matches:
            continue

        constraint_name = str(constraint_data.get('name', 'unnamed-constraint'))
        constraint_type = str(constraint_data.get('constraintType', 'generic'))
        severity = str(constraint_data.get('severity', 'warning'))
        rule = str(constraint_data.get('rule', ''))

        if constraint_type in {'range', 'inequality', 'equality', 'temporal', 'conditional'}:
            post_execution_checks.append(
                ConstraintCheck(
                    name=constraint_name,
                    constraint_type=constraint_type,
                    severity=severity,
                    rule=rule,
                    status='cannot_verify_pre_execution',
                    note='Requires result data for final validation.',
                )
            )
        elif constraint_type == 'composition':
            missing_metrics = [name for name in metrics if isinstance(name, str) and name != metric_name]
            if missing_metrics:
                warnings.append(
                    ConstraintCheck(
                        name=constraint_name,
                        constraint_type=constraint_type,
                        severity=severity,
                        rule=rule,
                        status='missing_components',
                        note=f'Composition references additional metrics: {", ".join(missing_metrics)}',
                    )
                )

    valid = True
    if strict:
        valid = not any(warning.severity.lower() == 'error' for warning in warnings)

    plan = SemanticQueryPlan(
        metric_name=metric_name,
        sql_expression=str(metric_data.get('sql', '')),
        source_dataset_table_id=source_dataset_id,
        requested_dimensions=list(dimensions),
        resolved_dimensions=sorted(resolved_dimensions),
        unresolved_dimensions=unresolved_dimensions,
        joins=joins,
        time_grain=time_grain,
        filters=list(filters),
    )

    return SemanticQueryPlanOutput(
        status=ToolStatus.OK,
        defined=True,
        valid=valid,
        plan=plan,
        warnings=warnings,
        post_execution_checks=post_execution_checks,
        source=source,
        next_action='query_data' if valid else 'semantic_get_definition',
    )


def _validate_payload(schema: dict[str, Any], payload: dict[str, Any]) -> None:
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.path))
    if not errors:
        return
    first_error = errors[0]
    path = '.'.join(str(item) for item in first_error.path)
    if path:
        raise ValueError(f'Schema validation failed at {path}: {first_error.message}')
    raise ValueError(f'Schema validation failed: {first_error.message}')


@tool_errors()
async def semantic_define(
    ctx: Context,
    action: Annotated[
        Literal['create', 'patch', 'replace', 'delete', 'publish'],
        Field(description='Define action: create, patch, replace, delete, publish.'),
    ],
    entity_type: Annotated[
        Literal['model', 'dataset', 'metric', 'relationship', 'glossary', 'constraint'],
        Field(description='Semantic entity type being modified.'),
    ],
    name: Annotated[
        str | None,
        Field(description='Optional object name (required for create when not present in data).'),
    ] = None,
    uuid: Annotated[
        str | None,
        Field(description='Object UUID for patch/replace/delete/publish actions.'),
    ] = None,
    data: Annotated[
        dict[str, Any] | None,
        Field(description='Semantic payload for create/patch/replace actions.'),
    ] = None,
    model_id: Annotated[
        str | None,
        Field(description='Optional model UUID injected into payload when absent.'),
    ] = None,
    dry_run: Annotated[
        bool,
        Field(description='Validate and plan action without persisting changes.'),
    ] = False,
) -> SemanticDefineOutput:
    """
    Performs semantic authoring actions over Metastore with schema validation guardrails.

    WHEN TO USE:
    - For create/patch/replace/delete/publish lifecycle over semantic objects.
    - As follow-up when analytical metric is undefined and authoring is requested.

    RULES:
    - `delete` requires `uuid`.
    - `patch`, `replace`, and `publish` require `uuid`.
    - `create` requires a resolvable name (`name` arg or `data.name`).
    - All write-like actions validate payload against object schema before persistence.
    - `dry_run=true` performs validation and returns review guidance without mutation.

    OUTPUT CONTRACT:
    - Returns action flags: `created`, `updated`, `deleted`, `published`.
    - Returns `source` metadata and optional `definition` for mutated objects.
    - Uses `next_action` to guide subsequent retrieval/discovery flow.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    action_enum = SemanticDefineAction(action)
    entity_type_enum = SemanticEntityType(entity_type)
    object_type = ENTITY_TO_OBJECT_TYPE[entity_type_enum]

    if action_enum == SemanticDefineAction.DELETE:
        if not uuid:
            raise ValueError('uuid is required for delete action.')

        if dry_run:
            return SemanticDefineOutput(
                status=ToolStatus.OK,
                deleted=False,
                review_required=True,
                reason='Dry run: delete not executed.',
                source=SemanticSource(object_type=object_type.value, uuid=uuid, model_id=model_id),
            )

        await client.metastore_client.delete_object(object_type.value, uuid)
        return SemanticDefineOutput(
            status=ToolStatus.OK,
            deleted=True,
            source=SemanticSource(object_type=object_type.value, uuid=uuid, model_id=model_id),
            next_action='semantic_discover',
        )

    schema_document = await client.metastore_client.get_schema(object_type.value)
    schema = schema_document.model_dump(exclude_none=True)

    payload = dict(data or {})
    if model_id and entity_type_enum != SemanticEntityType.MODEL and 'modelUUID' not in payload:
        payload['modelUUID'] = model_id

    if action_enum == SemanticDefineAction.CREATE:
        inferred_name = name or str(payload.get('name', ''))
        if not inferred_name:
            raise ValueError('name is required for create action.')
        if 'name' not in payload:
            payload['name'] = inferred_name
        _validate_payload(schema, payload)

        if dry_run:
            return SemanticDefineOutput(
                status=ToolStatus.OK,
                created=False,
                review_required=True,
                reason='Dry run: create not executed.',
                source=SemanticSource(object_type=object_type.value, uuid=None, model_id=model_id),
            )

        created = await client.metastore_client.create_object(object_type.value, name=inferred_name, data=payload)
        definition = _resource_to_definition(created, object_type)
        return SemanticDefineOutput(
            status=ToolStatus.OK,
            created=True,
            source=_resource_source(created, object_type),
            definition=definition,
            next_action='semantic_get_definition',
        )

    if not uuid:
        raise ValueError('uuid is required for patch, replace, and publish actions.')

    current = await client.metastore_client.get_object(object_type.value, uuid)
    current_data = _extract_semantic_data(current.attributes)

    if action_enum == SemanticDefineAction.PATCH:
        merged_payload = current_data | payload
        if name:
            merged_payload['name'] = name
        _validate_payload(schema, merged_payload)

        if dry_run:
            return SemanticDefineOutput(
                status=ToolStatus.OK,
                updated=False,
                review_required=True,
                reason='Dry run: patch not executed.',
                source=_resource_source(current, object_type),
            )

        patched = await client.metastore_client.patch_object(object_type.value, uuid, name=name, data=payload or None)
        return SemanticDefineOutput(
            status=ToolStatus.OK,
            updated=True,
            source=_resource_source(patched, object_type),
            definition=_resource_to_definition(patched, object_type),
            next_action='semantic_get_definition',
        )

    if action_enum == SemanticDefineAction.REPLACE:
        if not payload:
            raise ValueError('data payload is required for replace action.')
        replace_name = name or str(payload.get('name') or current_data.get('name') or '')
        if not replace_name:
            raise ValueError('name is required for replace action.')
        if 'name' not in payload:
            payload['name'] = replace_name

        _validate_payload(schema, payload)

        if dry_run:
            return SemanticDefineOutput(
                status=ToolStatus.OK,
                updated=False,
                review_required=True,
                reason='Dry run: replace not executed.',
                source=_resource_source(current, object_type),
            )

        replaced = await client.metastore_client.put_object(object_type.value, uuid, name=replace_name, data=payload)
        return SemanticDefineOutput(
            status=ToolStatus.OK,
            updated=True,
            source=_resource_source(replaced, object_type),
            definition=_resource_to_definition(replaced, object_type),
            next_action='semantic_get_definition',
        )

    if action_enum == SemanticDefineAction.PUBLISH:
        publish_payload = current_data | {'status': 'published'}
        _validate_payload(schema, publish_payload)

        if dry_run:
            return SemanticDefineOutput(
                status=ToolStatus.OK,
                published=False,
                review_required=True,
                reason='Dry run: publish not executed.',
                source=_resource_source(current, object_type),
            )

        published = await client.metastore_client.patch_object(
            object_type.value,
            uuid,
            data={'status': 'published'},
        )
        return SemanticDefineOutput(
            status=ToolStatus.OK,
            published=True,
            source=_resource_source(published, object_type),
            definition=_resource_to_definition(published, object_type),
            next_action='semantic_get_definition',
        )

    raise ValueError(f'Unsupported define action: {action}')
