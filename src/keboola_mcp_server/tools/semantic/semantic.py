"""Semantic read tools backed by the semantic service layer."""

from collections.abc import Sequence
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import MetastoreObject
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import process_concurrently, toon_serializer_compact, unwrap_results
from keboola_mcp_server.tools.constants import SEMANTIC_TOOLS_TAG
from keboola_mcp_server.tools.semantic.model import (
    CompactSemanticObject,
    GetSemanticContextOutput,
    RawSemanticValidationResult,
    SearchSemanticContextOutput,
    SemanticConstraintCompact,
    SemanticDatasetCompact,
    SemanticGlossaryCompact,
    SemanticMetricCompact,
    SemanticModelCompact,
    SemanticModelSearchResult,
    SemanticObject,
    SemanticObjectMatch,
    SemanticObjectType,
    SemanticObjectTypeContext,
    SemanticObjectTypeGroup,
    SemanticObjectTypeSelection,
    SemanticRelationshipCompact,
    SemanticUsedDataset,
    SemanticUsedMetric,
    SemanticValidationModelResult,
    ValidateSemanticQueryOutput,
)
from keboola_mcp_server.tools.semantic.service import (
    load_single_semantic_context,
)
from keboola_mcp_server.tools.semantic.service import search_semantic_context as search_semantic_context_service
from keboola_mcp_server.tools.semantic.service import validate_semantic_query as validate_semantic_query_service


def _compact_semantic_object(object_type: SemanticObjectType, obj: MetastoreObject) -> CompactSemanticObject:
    if object_type == SemanticObjectType.SEMANTIC_MODEL:
        return SemanticModelCompact.from_metastore(obj)
    elif object_type == SemanticObjectType.SEMANTIC_DATASET:
        return SemanticDatasetCompact.from_metastore(obj)
    elif object_type == SemanticObjectType.SEMANTIC_METRIC:
        return SemanticMetricCompact.from_metastore(obj)
    elif object_type == SemanticObjectType.SEMANTIC_RELATIONSHIP:
        return SemanticRelationshipCompact.from_metastore(obj)
    elif object_type == SemanticObjectType.SEMANTIC_GLOSSARY:
        return SemanticGlossaryCompact.from_metastore(obj)
    elif object_type == SemanticObjectType.SEMANTIC_CONSTRAINT:
        return SemanticConstraintCompact.from_metastore(obj)
    raise ValueError(f'No compact semantic object model is defined for "{object_type.value}".')


def add_semantic_tools(mcp: FastMCP) -> None:
    """Register semantic read tools."""
    mcp.add_tool(
        FunctionTool.from_function(
            search_semantic_context,
            name='searchSemanticContext',
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEMANTIC_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_semantic_context,
            name='getSemanticContext',
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEMANTIC_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            validate_semantic_query,
            name='validate',
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
                'Regex patterns used to search the semantic layer. '
                'The tool searches semantic model names, descriptions, and nested fields of all semantic objects.'
            )
        ),
    ],
    semantic_types: Annotated[
        Sequence[SemanticObjectType],
        Field(description='Optional semantic object types to search. Empty means all semantic object types.'),
    ] = tuple(),
    semantic_model_id: Annotated[
        str | None,
        Field(description='Semantic model ID for which to search semantic objects.'),
    ] = None,
    case_sensitive: Annotated[bool, Field(description='Whether regex matching should be case-sensitive.')] = False,
    max_results: Annotated[int, Field(description='Maximum number of matched semantic objects to return.')] = 100,
) -> SearchSemanticContextOutput:
    """
    Searches all semantic models and their child semantic objects using regex patterns.

    Use this tool when you need to discover which semantic models, datasets, metrics,
    glossary terms, relationships, or constraints are relevant to a user request.
    """
    cleaned_patterns = [pattern.strip() for pattern in patterns if pattern and pattern.strip()]
    if not cleaned_patterns:
        raise ValueError('At least one regex pattern must be provided.')
    if max_results <= 0:
        raise ValueError('max_results must be a positive integer.')

    client = KeboolaClient.from_state(ctx.session.state)
    hits = await search_semantic_context_service(
        client,
        cleaned_patterns,
        semantic_types=semantic_types,
        semantic_model_id=semantic_model_id,
        case_sensitive=case_sensitive,
        max_results=max_results,
    )

    grouped_matches: dict[str, list[SemanticObjectMatch]] = {}

    for hit in hits:
        grouped_matches.setdefault(hit.semantic_model_id, []).append(
            SemanticObjectMatch(
                object_type=hit.object_type,
                matched_paths=list(hit.matched_paths),
                data=_compact_semantic_object(hit.object_type, hit.object),
            )
        )

    model_results = [
        SemanticModelSearchResult(
            semantic_model_id=model_id,
            matches=sorted(grouped, key=lambda item: item.data.name or item.data.id),
        )
        for model_id, grouped in grouped_matches.items()
    ]
    model_results.sort(key=lambda item: item.semantic_model_id)

    return SearchSemanticContextOutput(models=model_results)


@tool_errors()
async def get_semantic_context(
    ctx: Context,
    semantic_objects: Annotated[
        Sequence[SemanticObjectTypeSelection],
        Field(
            description=(
                'List of semantic object type selections. '
                'Each item contains "object_type" and optional "ids". '
                'If "ids" is empty, the all objects of the given type are returned in compact form.'
            )
        ),
    ],
    semantic_model_id: Annotated[
        str | None,
        Field(description='Semantic model ID to filter the semantic objects by.'),
    ] = None,
) -> GetSemanticContextOutput:
    """
    Returns semantic context grouped by semantic object type.

    Use this when you already know which semantic object types
    and optional object UUIDs should be loaded in full detail.
    """
    if not semantic_objects:
        raise ValueError('At least one semantic object type must be provided.')

    client = KeboolaClient.from_state(ctx.session.state)

    async def provide_to_get_single_semantic_context(
        selection: SemanticObjectTypeSelection,
    ) -> SemanticObjectTypeGroup:
        """Wrapper to provide the client to the single-context fetch executed in parallel."""
        return await load_single_semantic_context(client, selection.object_type, selection.ids, semantic_model_id)

    contexts = unwrap_results(
        await process_concurrently(
            semantic_objects,
            provide_to_get_single_semantic_context,
            max_concurrency=len(semantic_objects),
        ),
        'Failed to fetch semantic context.',
    )

    # Normalize the contexts to the SemanticObjectTypeContext format
    normalized_contexts: list[SemanticObjectTypeContext] = []
    for selection, context in zip(semantic_objects, contexts, strict=True):
        assert selection.object_type == context.object_type, (
            f'Semantic object type mismatch: {selection.object_type} != {context.object_type}'
        )
        if selection.ids:
            # Detail context with specific IDs
            normalized_contexts.append(
                SemanticObjectTypeContext(
                    object_type=context.object_type,
                    objects=[SemanticObject.from_metastore(obj) for obj in context.objects],
                )
            )
        else:
            normalized_contexts.append(
                SemanticObjectTypeContext(
                    object_type=context.object_type,
                    objects=[_compact_semantic_object(context.object_type, obj) for obj in context.objects],
                )
            )

    return GetSemanticContextOutput(semantic_objects=normalized_contexts)


@tool_errors()
async def validate_semantic_query(
    sql_query: Annotated[str, Field(description='SQL query that should be checked against semantic context.')],
    semantic_model_id: Annotated[
        str,
        Field(description='Semantic model ID against which the SQL should be validated.'),
    ],
    expected_semantic_objects: Annotated[
        Sequence[SemanticObjectTypeSelection],
        Field(
            description=(
                'Semantic object selections that define which semantic objects are expected '
                'to be used in the SQL query. '
                'Missing required object types are loaded automatically.'
            )
        ),
    ],
    ctx: Context,
) -> ValidateSemanticQueryOutput:
    """
    Performs a best-effort semantic pre-validation of an SQL query.

    The tool does not execute the SQL. It checks whether the SQL seems aligned with
    selected semantic objects and surfaces relevant semantic constraints with optional
    validation SQL snippets from the metastore.
    """
    if not sql_query.strip():
        raise ValueError('sql_query must not be empty.')
    if not semantic_model_id.strip():
        raise ValueError('semantic_model_id must not be empty.')
    client = KeboolaClient.from_state(ctx.session.state)
    raw_result = await validate_semantic_query_service(client, sql_query, expected_semantic_objects, semantic_model_id)
    return _format_validation_output(raw_result, semantic_model_id, expected_semantic_objects)


def _format_validation_output(
    raw_result: RawSemanticValidationResult,
    semantic_model_id: str,
    semantic_objects: Sequence[SemanticObjectTypeSelection],
) -> ValidateSemanticQueryOutput:
    used_dataset_objects = []
    used_metric_objects = []
    for group in raw_result.used_object_groups:
        if group.object_type == SemanticObjectType.SEMANTIC_DATASET:
            used_dataset_objects = group.objects
        elif group.object_type == SemanticObjectType.SEMANTIC_METRIC:
            used_metric_objects = group.objects

    used_datasets = [
        SemanticUsedDataset(
            name=str(item.attributes.get('name') or item.meta.name or item.attributes.get('tableId') or ''),
            table_id=str(item.attributes.get('tableId') or ''),
            description=str(item.attributes.get('description') or ''),
            fqn=str(item.attributes.get('fqn') or ''),
        )
        for item in used_dataset_objects
    ]
    used_metrics = [
        SemanticUsedMetric(
            name=str(item.attributes.get('name') or item.meta.name or ''),
            description=str(item.attributes.get('description') or ''),
            sql=str(item.attributes.get('sql') or ''),
            dataset=str(item.attributes.get('dataset') or ''),
        )
        for item in used_metric_objects
    ]
    selected_object_ids = [object_id for selection in semantic_objects for object_id in selection.ids]

    if raw_result.violations:
        summary = 'Semantic validation found pre-execution issues that should be fixed before running the query.'
    elif raw_result.post_execution_checks:
        summary = (
            'Semantic validation found no pre-execution issues, but some checks should be verified after execution.'
        )
    else:
        summary = 'Semantic validation finished without relevant findings.'

    return ValidateSemanticQueryOutput(
        valid=raw_result.valid,
        used_datasets=used_datasets,
        used_metrics=used_metrics,
        violations=raw_result.violations,
        post_execution_checks=raw_result.post_execution_checks,
        semantic_models=[
            SemanticValidationModelResult(
                semantic_model_id=semantic_model_id,
                semantic_model_name=None,
                sql_dialect=None,
                selected_object_ids=selected_object_ids,
                used_datasets=used_datasets,
                used_metrics=used_metrics,
                matched_relationships=raw_result.matched_relationships,
                violations=raw_result.violations,
                post_execution_checks=raw_result.post_execution_checks,
            )
        ],
        summary=summary,
    )
