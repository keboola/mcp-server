"""Semantic service layer shared by semantic read tools."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence

import jsonpath_ng

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import MetastoreObject
from keboola_mcp_server.mcp import process_concurrently, unwrap_results
from keboola_mcp_server.tools.semantic.model import (
    ConstraintValidationFinding,
    RawSemanticValidationResult,
    SemanticObjectType,
    SemanticObjectTypeGroup,
    SemanticObjectTypeSelection,
    SemanticSearchHit,
)

SEMANTIC_OBJECT_TYPES: tuple[SemanticObjectType, ...] = (
    SemanticObjectType.SEMANTIC_MODEL,
    SemanticObjectType.SEMANTIC_DATASET,
    SemanticObjectType.SEMANTIC_METRIC,
    SemanticObjectType.SEMANTIC_RELATIONSHIP,
    SemanticObjectType.SEMANTIC_GLOSSARY,
    SemanticObjectType.SEMANTIC_CONSTRAINT,
)
VALIDATION_OBJECT_TYPES: tuple[SemanticObjectType, ...] = (
    SemanticObjectType.SEMANTIC_MODEL,
    SemanticObjectType.SEMANTIC_DATASET,
    SemanticObjectType.SEMANTIC_METRIC,
    SemanticObjectType.SEMANTIC_RELATIONSHIP,
    SemanticObjectType.SEMANTIC_CONSTRAINT,
)

# Some metastore endpoints return 500 for large responses unless paged aggressively.
DEFAULT_PAGE_LIMITS: dict[SemanticObjectType, int] = {
    SemanticObjectType.SEMANTIC_DATASET: 1,
    SemanticObjectType.SEMANTIC_METRIC: 1,
}
DEFAULT_PAGE_LIMIT = 20
ALL_ATTRIBUTE_NODES_EXPR = jsonpath_ng.parse('$..*')
POST_QUERY_CONSTRAINT_TYPES = {'inequality', 'equality', 'range', 'temporal', 'conditional'}


def _object_name(obj: MetastoreObject) -> str | None:
    for key in ('name', 'term', 'displayName', 'tableId', 'dataset'):
        value = obj.attributes.get(key)
        if isinstance(value, str) and value:
            return value
    return obj.meta.name or None


def _model_id_for_object(obj: MetastoreObject) -> str:
    if obj.type == SemanticObjectType.SEMANTIC_MODEL.value:
        return obj.id
    model_id = obj.attributes.get('modelUUID')
    return str(model_id) if model_id else ''


def _clean_jsonpath_path_str(path_str: str) -> str:
    """Normalize a jsonpath_ng full_path string across library versions."""
    result = path_str.replace('(', '').replace(')', '')
    result = re.sub(r"['\"]([^'\"]+)['\"]", r'\1', result)
    return re.sub(r'\.\[', '[', result)


def _stringify_value(value: object) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


def _find_matches(
    obj: MetastoreObject,
    compiled_patterns: Sequence[re.Pattern[str]],
    cleaned_patterns: Sequence[str],
) -> tuple[list[str], list[str]]:
    matched_paths: set[str] = set()
    matched_patterns: set[str] = set()

    if obj.meta.name:
        for pattern, compiled in zip(cleaned_patterns, compiled_patterns, strict=False):
            if compiled.search(obj.meta.name):
                matched_paths.add('meta.name')
                matched_patterns.add(pattern)

    if any(compiled.search(_stringify_value(obj.attributes)) for compiled in compiled_patterns):
        for jpath_match in ALL_ATTRIBUTE_NODES_EXPR.find(obj.attributes):
            value = jpath_match.value
            if isinstance(value, (dict, list)):
                continue

            haystack = _stringify_value(value)
            if not haystack:
                continue

            path = _clean_jsonpath_path_str(str(jpath_match.full_path))
            for pattern, compiled in zip(cleaned_patterns, compiled_patterns, strict=False):
                if compiled.search(haystack):
                    matched_paths.add(path)
                    matched_patterns.add(pattern)

    return sorted(matched_paths), sorted(matched_patterns)


async def _list_semantic_type_objects(
    client: KeboolaClient,
    object_type: SemanticObjectType,
    semantic_model_id: str | None = None,
) -> list[MetastoreObject]:
    """List all semantic objects of a given type, optionally filtered by a semantic model ID."""
    metastore = client.metastore_client
    limit = DEFAULT_PAGE_LIMITS.get(object_type, DEFAULT_PAGE_LIMIT)
    offset = 0
    items: list[MetastoreObject] = []

    while True:
        page = await metastore.list_objects(object_type, limit=limit, offset=offset)
        items.extend([
            obj for obj in page if semantic_model_id is None or _model_id_for_object(obj) == semantic_model_id
        ])
        if len(page) < limit:
            return items
        offset += limit


def _matches_sql(sql_query: str, candidate: str) -> bool:
    if not candidate:
        return False

    candidate_lower = candidate.lower()
    sql_lower = sql_query.lower()
    if re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', candidate):
        pattern = rf'(?<![a-zA-Z0-9_]){re.escape(candidate_lower)}(?![a-zA-Z0-9_])'
        return re.search(pattern, sql_lower) is not None
    return candidate_lower in sql_lower


def _pick_validation_query(constraint: MetastoreObject, sql_dialect: str | None) -> str | None:
    validation_query = constraint.attributes.get('validationQuery')
    if not isinstance(validation_query, dict):
        return None

    dialect_key = (sql_dialect or '').strip().lower()
    if dialect_key == 'snowflake' and isinstance(validation_query.get('snowflake'), str):
        return validation_query['snowflake']
    if dialect_key == 'bigquery' and isinstance(validation_query.get('bigquery'), str):
        return validation_query['bigquery']

    default_query = validation_query.get('default')
    return default_query if isinstance(default_query, str) else None


def _constraint_message(constraint: MetastoreObject, default_message: str) -> str:
    error_message = constraint.attributes.get('errorMessage')
    remediation = constraint.attributes.get('remediation')
    if isinstance(error_message, str) and error_message.strip():
        if isinstance(remediation, str) and remediation.strip():
            return f'{error_message.strip()} Remediation: {remediation.strip()}'
        return error_message.strip()
    if isinstance(remediation, str) and remediation.strip():
        return f'{default_message} Remediation: {remediation.strip()}'
    return default_message


def _dataset_identifiers(dataset: MetastoreObject) -> list[str]:
    candidates = [
        dataset.attributes.get('fqn'),
        dataset.attributes.get('tableId'),
        dataset.attributes.get('name'),
        dataset.meta.name,
    ]
    return [str(candidate).strip() for candidate in candidates if isinstance(candidate, str) and candidate.strip()]


def _metric_identifiers(metric: MetastoreObject) -> list[str]:
    candidates = [
        metric.attributes.get('sql'),
        metric.attributes.get('name'),
        metric.meta.name,
    ]
    return [str(candidate).strip() for candidate in candidates if isinstance(candidate, str) and candidate.strip()]


def _detect_used_datasets(sql_query: str, datasets: Sequence[MetastoreObject]) -> list[MetastoreObject]:
    return [
        dataset
        for dataset in datasets
        if any(_matches_sql(sql_query, candidate) for candidate in _dataset_identifiers(dataset))
    ]


def _detect_used_metrics_for_datasets(
    sql_query: str,
    metrics: Sequence[MetastoreObject],
    used_dataset_ids: set[str],
) -> list[MetastoreObject]:
    matches: list[MetastoreObject] = []
    for metric in metrics:
        metric_dataset = metric.attributes.get('dataset')
        if not isinstance(metric_dataset, str) or metric_dataset not in used_dataset_ids:
            continue
        if any(_matches_sql(sql_query, candidate) for candidate in _metric_identifiers(metric)):
            matches.append(metric)
    return matches


def _detect_used_relationships(
    sql_query: str,
    relationships: Sequence[MetastoreObject],
    used_dataset_ids: set[str],
) -> list[MetastoreObject]:
    matches: list[MetastoreObject] = []
    for relationship in relationships:
        from_dataset = relationship.attributes.get('from')
        to_dataset = relationship.attributes.get('to')
        join_condition = relationship.attributes.get('on')
        if not isinstance(from_dataset, str) or not isinstance(to_dataset, str):
            continue
        if from_dataset not in used_dataset_ids or to_dataset not in used_dataset_ids:
            continue
        if isinstance(join_condition, str) and join_condition.strip() and not _matches_sql(sql_query, join_condition):
            continue
        matches.append(relationship)
    return matches


def _get_group_objects(
    used_object_groups: Sequence[SemanticObjectTypeGroup],
    object_type: SemanticObjectType,
) -> list[MetastoreObject]:
    for group in used_object_groups:
        if group.object_type == object_type:
            return group.objects
    return []


def _constraint_is_relevant(
    constraint: MetastoreObject,
    used_metric_names: set[str],
    used_dataset_ids: set[str],
) -> bool:
    constraint_metrics = {
        metric.strip()
        for metric in constraint.attributes.get('metrics', [])
        if isinstance(metric, str) and metric.strip()
    }
    constraint_datasets = {
        dataset.strip()
        for dataset in constraint.attributes.get('datasets', [])
        if isinstance(dataset, str) and dataset.strip()
    }
    if constraint_metrics and used_metric_names.intersection(constraint_metrics):
        return True
    if constraint_datasets and used_dataset_ids.intersection(constraint_datasets):
        return True
    return not constraint_metrics and not constraint_datasets


async def search_semantic_context(
    client: KeboolaClient,
    patterns: Sequence[str],
    *,
    semantic_types: Sequence[SemanticObjectType] = tuple(),
    semantic_model_id: str | None = None,
    case_sensitive: bool = False,
    max_results: int = 50,
) -> list[SemanticSearchHit]:
    """Search semantic objects by regex patterns for selected semantic object types."""
    cleaned_patterns = [pattern.strip() for pattern in patterns if pattern and pattern.strip()]
    if not cleaned_patterns:
        raise ValueError('At least one regex pattern must be provided.')
    if max_results <= 0:
        raise ValueError('max_results must be a positive integer.')

    target_types = tuple(semantic_types) if semantic_types else SEMANTIC_OBJECT_TYPES
    compiled_patterns = [re.compile(pattern, 0 if case_sensitive else re.IGNORECASE) for pattern in cleaned_patterns]

    matches: list[SemanticSearchHit] = []
    for object_type in target_types:
        if len(matches) >= max_results:
            break

        items = await _list_semantic_type_objects(client, object_type, semantic_model_id)
        for item in items:
            if len(matches) >= max_results:
                break

            field_hits, pattern_hits = _find_matches(item, compiled_patterns, cleaned_patterns)
            if not pattern_hits:
                continue

            matches.append(
                SemanticSearchHit(
                    object_type=object_type,
                    semantic_model_id=_model_id_for_object(item),
                    object=item,
                    matched_patterns=sorted(pattern_hits),
                    matched_paths=sorted(field_hits),
                )
            )
    return matches


async def load_single_semantic_context(
    client: KeboolaClient,
    object_type: SemanticObjectType,
    ids: Sequence[str] = tuple(),
    semantic_model_id: str | None = None,
) -> SemanticObjectTypeGroup:
    """Get semantic context for a single semantic object type."""
    if ids:
        results = await process_concurrently(
            ids,
            lambda object_id: client.metastore_client.get_object(object_type.value, object_id),
            max_concurrency=len(ids),
        )
        objects = unwrap_results(
            results,
            f'Failed to fetch semantic objects for type "{object_type.value}".',
        )
        if semantic_model_id is not None:
            objects = [obj for obj in objects if _model_id_for_object(obj) == semantic_model_id]
    else:
        objects = await _list_semantic_type_objects(client, object_type, semantic_model_id)

    return SemanticObjectTypeGroup(object_type=object_type, objects=objects)


async def load_all_semantic_context(
    client: KeboolaClient,
    semantic_model_id: str,
    *,
    required_types: Sequence[SemanticObjectType] | None = None,
) -> dict[SemanticObjectType, SemanticObjectTypeGroup]:
    """Load raw semantic context grouped by type for the given semantic model."""
    required_types = required_types or VALIDATION_OBJECT_TYPES
    selections = [
        SemanticObjectTypeSelection(
            object_type=object_type,
            ids=tuple(),
        )
        for object_type in required_types
    ]
    results = await process_concurrently(
        selections,
        lambda selection: load_single_semantic_context(client, selection.object_type, selection.ids, semantic_model_id),
        max_concurrency=len(selections),
    )
    groups = unwrap_results(results, 'Failed to fetch semantic context.')
    return {group.object_type: group for group in groups}


def detect_used_objects_from_context(
    sql_query: str,
    context_by_type: dict[SemanticObjectType, SemanticObjectTypeGroup],
) -> list[SemanticObjectTypeGroup]:
    """Detect semantic objects used by the SQL query from raw semantic context."""
    datasets = context_by_type.get(SemanticObjectType.SEMANTIC_DATASET, [])
    metrics = context_by_type.get(SemanticObjectType.SEMANTIC_METRIC, [])
    relationships = context_by_type.get(SemanticObjectType.SEMANTIC_RELATIONSHIP, [])

    used_dataset_objects = _detect_used_datasets(sql_query, datasets.objects)
    used_dataset_ids = {
        str(item.attributes.get('tableId')).strip()
        for item in used_dataset_objects
        if isinstance(item.attributes.get('tableId'), str) and item.attributes.get('tableId')
    }
    used_metric_objects = _detect_used_metrics_for_datasets(sql_query, metrics.objects, used_dataset_ids)
    used_relationship_objects = _detect_used_relationships(sql_query, relationships.objects, used_dataset_ids)

    used_groups: list[SemanticObjectTypeGroup] = []
    if used_dataset_objects:
        used_groups.append(
            SemanticObjectTypeGroup(
                object_type=SemanticObjectType.SEMANTIC_DATASET,
                objects=used_dataset_objects,
            )
        )
    if used_metric_objects:
        used_groups.append(
            SemanticObjectTypeGroup(
                object_type=SemanticObjectType.SEMANTIC_METRIC,
                objects=used_metric_objects,
            )
        )
    if used_relationship_objects:
        used_groups.append(
            SemanticObjectTypeGroup(
                object_type=SemanticObjectType.SEMANTIC_RELATIONSHIP,
                objects=used_relationship_objects,
            )
        )
    return used_groups


def evaluate_constraints_from_context(
    sql_query: str,
    context_by_type: dict[SemanticObjectType, list[MetastoreObject]],
    used_object_groups: Sequence[SemanticObjectTypeGroup],
) -> RawSemanticValidationResult:
    """Evaluate relevant semantic constraints for the used semantic objects."""
    model = next(iter(context_by_type.get(SemanticObjectType.SEMANTIC_MODEL, [])), None)
    constraints = context_by_type.get(SemanticObjectType.SEMANTIC_CONSTRAINT, [])
    used_dataset_objects = _get_group_objects(used_object_groups, SemanticObjectType.SEMANTIC_DATASET)
    used_metric_objects = _get_group_objects(used_object_groups, SemanticObjectType.SEMANTIC_METRIC)
    used_relationship_objects = _get_group_objects(used_object_groups, SemanticObjectType.SEMANTIC_RELATIONSHIP)

    used_dataset_ids = {
        str(item.attributes.get('tableId')).strip()
        for item in used_dataset_objects
        if isinstance(item.attributes.get('tableId'), str) and item.attributes.get('tableId')
    }
    used_metric_names = {
        str(item.attributes.get('name')).strip()
        for item in used_metric_objects
        if isinstance(item.attributes.get('name'), str) and item.attributes.get('name')
    }
    matched_relationships = sorted(
        str(item.attributes.get('name') or item.meta.name or item.id)
        for item in used_relationship_objects
    )

    sql_dialect = model.attributes.get('sql_dialect') if model is not None else None
    sql_dialect_str = str(sql_dialect) if isinstance(sql_dialect, str) else None
    violations: list[ConstraintValidationFinding] = []
    post_execution_checks: list[ConstraintValidationFinding] = []
    has_error = False

    for constraint in constraints:
        if not _constraint_is_relevant(constraint, used_metric_names, used_dataset_ids):
            continue

        constraint_name = str(constraint.attributes.get('name') or constraint.meta.name or constraint.id)
        severity = str(constraint.attributes.get('severity') or 'error')
        constraint_type = str(constraint.attributes.get('constraintType') or 'unknown')
        validation_query = _pick_validation_query(constraint, sql_dialect_str)
        constraint_metrics = [
            metric
            for metric in constraint.attributes.get('metrics', [])
            if isinstance(metric, str) and metric.strip()
        ]
        constraint_datasets = [
            dataset
            for dataset in constraint.attributes.get('datasets', [])
            if isinstance(dataset, str) and dataset.strip()
        ]
        pre_query_check = bool(
            isinstance(constraint.attributes.get('ai'), dict)
            and constraint.attributes['ai'].get('preQueryCheck') is True
        )

        if constraint_type == 'composition':
            missing_metrics = [metric for metric in constraint_metrics if metric not in used_metric_names]
            if missing_metrics:
                if severity == 'error':
                    has_error = True
                violations.append(
                    ConstraintValidationFinding(
                        constraint_id=constraint.id,
                        constraint_name=constraint_name,
                        severity=severity,
                        status='missing_metrics',
                        message=_constraint_message(
                            constraint,
                            (
                                f'Constraint "{constraint_name}" expects metrics present in the SQL: '
                                f'{", ".join(missing_metrics)}.'
                            ),
                        ),
                        validation_query=validation_query,
                    )
                )
            continue

        if constraint_type == 'exclusion':
            used_excluded_metrics = [metric for metric in constraint_metrics if metric in used_metric_names]
            used_excluded_datasets = [dataset for dataset in constraint_datasets if dataset in used_dataset_ids]
            if len(used_excluded_metrics) > 1 or len(used_excluded_datasets) > 1:
                if severity == 'error':
                    has_error = True
                violations.append(
                    ConstraintValidationFinding(
                        constraint_id=constraint.id,
                        constraint_name=constraint_name,
                        severity=severity,
                        status='excluded_combination',
                        message=_constraint_message(
                            constraint,
                            f'Constraint "{constraint_name}" forbids this combination of semantic objects.',
                        ),
                        validation_query=validation_query,
                    )
                )
            continue

        if pre_query_check:
            if severity == 'error':
                has_error = True
            violations.append(
                ConstraintValidationFinding(
                    constraint_id=constraint.id,
                    constraint_name=constraint_name,
                    severity=severity,
                    status='pre_query_check',
                    message=_constraint_message(
                        constraint,
                        (
                            f'Constraint "{constraint_name}" should be explicitly checked before trusting '
                            f'the query result.'
                        ),
                    ),
                    validation_query=validation_query,
                )
            )
            continue

        if constraint_type not in POST_QUERY_CONSTRAINT_TYPES and validation_query is None:
            continue

        post_execution_checks.append(
            ConstraintValidationFinding(
                constraint_id=constraint.id,
                constraint_name=constraint_name,
                severity=severity,
                status='post_query_check',
                message=_constraint_message(
                    constraint,
                    (
                        f'Constraint "{constraint_name}" is relevant for this SQL and should be verified '
                        f'against the result.'
                    ),
                ),
                validation_query=validation_query,
            )
        )

    return RawSemanticValidationResult(
        valid=not has_error,
        used_object_groups=list(used_object_groups),
        matched_relationships=matched_relationships,
        violations=violations,
        post_execution_checks=post_execution_checks,
    )


async def validate_semantic_query(
    client: KeboolaClient,
    sql_query: str,
    semantic_model_id: str,
) -> RawSemanticValidationResult:
    """Validate SQL against a semantic model without executing it."""
    if not sql_query.strip():
        raise ValueError('sql_query must not be empty.')
    if not semantic_model_id.strip():
        raise ValueError('semantic_model_id must not be empty.')

    context_by_type = await load_all_semantic_context(client, semantic_model_id)
    used_object_groups = detect_used_objects_from_context(sql_query, context_by_type)
    return evaluate_constraints_from_context(sql_query, context_by_type, used_object_groups)
