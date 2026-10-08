"""Semantic service layer shared by semantic read tools."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence

import jsonpath_ng
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import MetastoreObject
from keboola_mcp_server.mcp import process_concurrently, unwrap_results
from keboola_mcp_server.tools.semantic.model import SemanticObjectType

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
DEFAULT_PAGE_LIMIT = 20
DEFAULT_PAGE_LIMITS: dict[SemanticObjectType, int] = {
    SemanticObjectType.SEMANTIC_DATASET: 1,
    SemanticObjectType.SEMANTIC_METRIC: 5,
}

ALL_ATTRIBUTE_NODES_EXPR = jsonpath_ng.parse('$..*')
POST_QUERY_CONSTRAINT_TYPES = {'inequality', 'equality', 'range', 'temporal', 'conditional'}

# Regex that captures the single column name from a simple aggregate metric SQL expression,
# e.g.  SUM("REVENUE_YTD") → "REVENUE_YTD",  AVG(margin_pct) → "margin_pct".
# Complex expressions (CASE, arithmetic, multi-arg) do not match and return None.
_AGGREGATE_COLUMN_RE = re.compile(r'^\s*\w+\s*\(\s*"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*\)\s*$')

# SQL function names and keywords that should never be treated as column identifiers when
# parsing relationship ON clauses.  Upper-case only because _extract_join_columns only
# looks at uppercase tokens.
_SQL_KEYWORDS_UPPER = frozenset(
    {
        'AND',
        'OR',
        'NOT',
        'IN',
        'IS',
        'NULL',
        'TRUE',
        'FALSE',
        'LEFT',
        'RIGHT',
        'INNER',
        'OUTER',
        'FULL',
        'CROSS',
        'JOIN',
        'ON',
        'WHERE',
        'SELECT',
        'FROM',
        'AS',
        'BY',
        'GROUP',
        'AVG',
        'SUM',
        'COUNT',
        'MIN',
        'MAX',
        'COALESCE',
        'NULLIF',
        'CAST',
        'CONCAT',
        'TRIM',
        'LENGTH',
        'UPPER',
        'LOWER',
        'IFF',
        'CASE',
        'WHEN',
        'THEN',
        'ELSE',
        'END',
    }
)

# Words that can follow a table reference without being its alias.
_NON_ALIAS_KEYWORDS_UPPER = _SQL_KEYWORDS_UPPER | frozenset(
    {'ORDER', 'LIMIT', 'HAVING', 'UNION', 'QUALIFY', 'USING', 'NATURAL', 'LATERAL', 'EXCEPT', 'INTERSECT', 'MINUS'}
)
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")
_IDENTIFIER_PART = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)'
_QUALIFIED_COLUMN = rf'{_IDENTIFIER_PART}(?:\s*\.\s*{_IDENTIFIER_PART})+'
# `a.x = b.y` predicates between two qualified columns, the shape of a join condition.
_COLUMN_EQUALITY_RE = re.compile(rf'(?<![\w."$])({_QUALIFIED_COLUMN})\s*=\s*({_QUALIFIED_COLUMN})(?![\w"$(])')
_AGGREGATE_FUNCTIONS = (
    'APPROX_COUNT_DISTINCT',
    'AVG',
    'COUNT_IF',
    'COUNT',
    'MAX',
    'MEDIAN',
    'MIN',
    'STDDEV_POP',
    'STDDEV_SAMP',
    'STDDEV',
    'SUM',
    'VAR_POP',
    'VAR_SAMP',
    'VARIANCE',
)
_AGGREGATE_CALL_RE = re.compile(rf'(?<![\w."$])(?:{"|".join(_AGGREGATE_FUNCTIONS)})\s*\(', re.IGNORECASE)


class SemanticTypeData(BaseModel):
    """Minimal typed semantic object used by the service layer."""

    semantic_type: SemanticObjectType = Field(description='Semantic object type.')
    id: str = Field(description='Semantic object UUID.')
    data: MetastoreObject = Field(description='Raw metastore object backing this typed service model.')

    @property
    def display_name(self) -> str | None:
        name = getattr(self, 'name', None)
        if isinstance(name, str) and name:
            return name
        return getattr(self.data.meta, 'name', None) or None


class SemanticModelData(SemanticTypeData):
    name: str | None = None
    description: str | None = None
    sql_dialect: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> SemanticModelData:
        attributes = obj.attributes or {}
        return cls(
            semantic_type=SemanticObjectType.SEMANTIC_MODEL,
            id=obj.id,
            data=obj,
            name=attributes.get('name') or getattr(obj.meta, 'name', None),
            description=attributes.get('description'),
            sql_dialect=attributes.get('sql_dialect'),
        )


class SemanticDatasetData(SemanticTypeData):
    name: str | None = None
    table_id: str | None = None
    fqn: str | None = None
    description: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> SemanticDatasetData:
        attributes = obj.attributes or {}
        return cls(
            semantic_type=SemanticObjectType.SEMANTIC_DATASET,
            id=obj.id,
            data=obj,
            name=attributes.get('name') or getattr(obj.meta, 'name', None),
            table_id=attributes.get('tableId'),
            fqn=attributes.get('fqn'),
            description=attributes.get('description'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticMetricData(SemanticTypeData):
    name: str | None = None
    sql: str | None = None
    dataset: str | None = None
    description: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> SemanticMetricData:
        attributes = obj.attributes or {}
        return cls(
            semantic_type=SemanticObjectType.SEMANTIC_METRIC,
            id=obj.id,
            data=obj,
            name=attributes.get('name') or getattr(obj.meta, 'name', None),
            sql=attributes.get('sql'),
            dataset=attributes.get('dataset'),
            description=attributes.get('description'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticRelationshipData(SemanticTypeData):
    name: str | None = None
    from_dataset: str | None = None
    to_dataset: str | None = None
    relationship_type: str | None = None
    on: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> SemanticRelationshipData:
        attributes = obj.attributes or {}
        return cls(
            semantic_type=SemanticObjectType.SEMANTIC_RELATIONSHIP,
            id=obj.id,
            data=obj,
            name=attributes.get('name') or getattr(obj.meta, 'name', None),
            from_dataset=attributes.get('from'),
            to_dataset=attributes.get('to'),
            relationship_type=attributes.get('type'),
            on=attributes.get('on'),
            model_uuid=attributes.get('modelUUID'),
        )


class SemanticGlossaryData(SemanticTypeData):
    term: str | None = None
    definition: str | None = None
    model_uuid: str | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> SemanticGlossaryData:
        attributes = obj.attributes or {}
        return cls(
            semantic_type=SemanticObjectType.SEMANTIC_GLOSSARY,
            id=obj.id,
            data=obj,
            term=attributes.get('term'),
            definition=attributes.get('definition'),
            model_uuid=attributes.get('modelUUID'),
        )

    @property
    def display_name(self) -> str | None:
        return self.term or super().display_name


class SemanticConstraintData(SemanticTypeData):
    name: str | None = None
    description: str | None = None
    constraint_type: str | None = None
    severity: str | None = None
    rule: str | None = None
    metrics: tuple[str, ...] = ()
    datasets: tuple[str, ...] = ()
    model_uuid: str | None = None
    error_message: str | None = None
    remediation: str | None = None
    pre_query_check: bool = False
    validation_query: dict[str, str] | None = None

    @classmethod
    def from_metastore(cls, obj: MetastoreObject) -> SemanticConstraintData:
        attributes = obj.attributes or {}
        ai = attributes.get('ai')
        validation_query = attributes.get('validationQuery')
        return cls(
            semantic_type=SemanticObjectType.SEMANTIC_CONSTRAINT,
            id=obj.id,
            data=obj,
            name=attributes.get('name') or getattr(obj.meta, 'name', None),
            description=attributes.get('description'),
            constraint_type=attributes.get('constraintType'),
            severity=attributes.get('severity'),
            rule=attributes.get('rule'),
            metrics=tuple(metric for metric in attributes.get('metrics', []) if isinstance(metric, str) and metric),
            datasets=tuple(
                dataset for dataset in attributes.get('datasets', []) if isinstance(dataset, str) and dataset
            ),
            model_uuid=attributes.get('modelUUID'),
            error_message=attributes.get('errorMessage'),
            remediation=attributes.get('remediation'),
            pre_query_check=isinstance(ai, dict) and ai.get('preQueryCheck') is True,
            validation_query=validation_query if isinstance(validation_query, dict) else None,
        )


SemanticServiceData = (
    SemanticModelData
    | SemanticDatasetData
    | SemanticMetricData
    | SemanticRelationshipData
    | SemanticGlossaryData
    | SemanticConstraintData
)


class SemanticServiceDataTypeGroup(BaseModel):
    """Semantic service objects grouped by semantic object type."""

    object_type: SemanticObjectType = Field(description='Semantic object type.')
    objects: list[SemanticServiceData] = Field(
        default_factory=list,
        description='Typed semantic objects of the requested type.',
    )


class SemanticSearchHit(BaseModel):
    """Raw semantic search hit returned by the service layer."""

    object_type: SemanticObjectType = Field(description='Matched semantic object type.')
    object: SemanticServiceData = Field(description='Matched semantic object.')
    semantic_model_id: str = Field(description='Parent semantic model UUID.')
    matched_patterns: list[str] = Field(default_factory=list, description='Regex patterns that matched.')
    matched_paths: list[str] = Field(default_factory=list, description='Search sources where the match happened.')


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


class SemanticValidationServiceOutput(BaseModel):
    """Output for semantic SQL validation."""

    valid: bool = Field(description='False when an error-severity pre-execution finding was detected.')
    used_object_groups: list[SemanticServiceDataTypeGroup] = Field(
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


def _to_semantic_service_data(object_type: SemanticObjectType, obj: MetastoreObject) -> SemanticServiceData:
    if object_type == SemanticObjectType.SEMANTIC_MODEL:
        return SemanticModelData.from_metastore(obj)
    if object_type == SemanticObjectType.SEMANTIC_DATASET:
        return SemanticDatasetData.from_metastore(obj)
    if object_type == SemanticObjectType.SEMANTIC_METRIC:
        return SemanticMetricData.from_metastore(obj)
    if object_type == SemanticObjectType.SEMANTIC_RELATIONSHIP:
        return SemanticRelationshipData.from_metastore(obj)
    if object_type == SemanticObjectType.SEMANTIC_GLOSSARY:
        return SemanticGlossaryData.from_metastore(obj)
    if object_type == SemanticObjectType.SEMANTIC_CONSTRAINT:
        return SemanticConstraintData.from_metastore(obj)
    raise ValueError(f'Unsupported semantic object type "{object_type.value}".')


def _get_semantic_model_id(obj: SemanticTypeData | MetastoreObject) -> str:
    if isinstance(obj, SemanticTypeData):
        if isinstance(obj, SemanticModelData):
            return obj.id
        else:
            return obj.model_uuid or ''
    elif isinstance(obj, MetastoreObject):
        if obj.type == SemanticObjectType.SEMANTIC_MODEL.value:
            return obj.id
        else:
            model_id = (obj.attributes or {}).get('modelUUID')
            return str(model_id) if model_id else ''
    raise ValueError(f'Unsupported object type "{type(obj)}".')


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
    semantic_object: SemanticServiceData,
    compiled_patterns: Sequence[re.Pattern[str]],
    cleaned_patterns: Sequence[str],
) -> tuple[list[str], list[str]]:
    matched_paths: set[str] = set()
    matched_patterns: set[str] = set()

    if semantic_object.display_name:
        for pattern, compiled in zip(cleaned_patterns, compiled_patterns, strict=False):
            if compiled.search(semantic_object.display_name):
                matched_paths.add('meta.name')
                matched_patterns.add(pattern)

    attrs = semantic_object.data.attributes or {}
    if any(compiled.search(_stringify_value(attrs)) for compiled in compiled_patterns):
        for jpath_match in ALL_ATTRIBUTE_NODES_EXPR.find(attrs):
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
    semantic_model_ids: Sequence[str] | None = None,
) -> list[SemanticServiceData]:
    """List all semantic objects of a given type, optionally filtered by a set of semantic model IDs."""
    if object_type == SemanticObjectType.SEMANTIC_MODEL:
        models = await _list_semantic_type_pages(client, object_type)
        wanted_ids = set(semantic_model_ids or ())
        return [model for model in models if not wanted_ids or model.id in wanted_ids]

    # Child objects are filtered by `modelUUID` server-side: the unfiltered list also holds children of
    # deleted models, and paging through it one small page at a time costs a request per object.
    # Without model IDs, the models the project lists stand in for them.
    model_ids = list(dict.fromkeys(semantic_model_ids)) if semantic_model_ids else await list_semantic_model_ids(client)
    if not model_ids:
        return []
    results = await process_concurrently(
        model_ids,
        lambda model_id: _list_semantic_type_pages(client, object_type, model_uuid=model_id),
        max_concurrency=min(len(model_ids), 10),
    )
    pages = unwrap_results(results, f'Failed to list semantic objects for type "{object_type.value}".')
    return [item for page in pages for item in page]


async def list_semantic_model_ids(client: KeboolaClient) -> list[str]:
    """IDs of the semantic models the project lists."""
    models = await _list_semantic_type_pages(client, SemanticObjectType.SEMANTIC_MODEL)
    return [model.id for model in models]


async def _list_semantic_type_pages(
    client: KeboolaClient,
    object_type: SemanticObjectType,
    *,
    model_uuid: str | None = None,
) -> list[SemanticServiceData]:
    """Page through one metastore listing, narrowed to the `model_uuid` model when given.

    The same id is checked client-side as well, since the metastore ignores filter keys a schema lacks.
    """
    metastore = client.metastore_client
    limit = DEFAULT_PAGE_LIMITS.get(object_type, DEFAULT_PAGE_LIMIT)
    offset = 0
    data: list[SemanticServiceData] = []

    while True:
        page = await metastore.list_objects(object_type, model_uuid=model_uuid, limit=limit, offset=offset)
        data.extend(
            _to_semantic_service_data(object_type, obj)
            for obj in page
            if model_uuid is None or _get_semantic_model_id(obj) == model_uuid
        )
        if len(page) < limit:
            return data
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


def _pick_validation_query(constraint: SemanticConstraintData, sql_dialect: str | None) -> str | None:
    validation_query = constraint.validation_query
    if validation_query is None:
        return None

    dialect_key = (sql_dialect or '').strip().lower()
    if dialect_key == 'snowflake' and isinstance(validation_query.get('snowflake'), str):
        return validation_query['snowflake']
    if dialect_key == 'bigquery' and isinstance(validation_query.get('bigquery'), str):
        return validation_query['bigquery']

    default_query = validation_query.get('default')
    return default_query if isinstance(default_query, str) else None


def _constraint_message(constraint: SemanticConstraintData, default_message: str) -> str:
    if constraint.error_message and constraint.error_message.strip():
        if constraint.remediation and constraint.remediation.strip():
            return f'{constraint.error_message.strip()} Remediation: {constraint.remediation.strip()}'
        return constraint.error_message.strip()
    if constraint.remediation and constraint.remediation.strip():
        return f'{default_message} Remediation: {constraint.remediation.strip()}'
    return default_message


def _dataset_identifiers(dataset: SemanticDatasetData) -> list[str]:
    candidates = [dataset.fqn]
    return [str(candidate).strip() for candidate in candidates if isinstance(candidate, str) and candidate.strip()]


def _extract_metric_column(sql: str) -> str | None:
    """Extract the bare column name from a simple aggregate metric SQL expression.

    Handles forms like ``SUM("REVENUE_YTD")``, ``AVG(margin_pct)``, ``SUM(AMOUNT)``.
    Returns *None* for complex expressions (CASE, arithmetic, multi-argument, ``COUNT(*)``).

    The extracted column is added as a secondary match candidate so that the metric is
    detected even when the column appears with a table-alias prefix in the SQL query
    (e.g. ``ep."REVENUE_YTD"``), which would otherwise defeat full-string matching.
    """
    m = _AGGREGATE_COLUMN_RE.match(sql)
    return m.group(1) if m else None


def _metric_identifiers(metric: SemanticMetricData) -> list[str]:
    candidates: list[str] = []
    if metric.sql:
        candidates.append(metric.sql)
        # Also add the bare column name so that `SUM("REVENUE_YTD")` matches even when
        # the SQL writes `SUM(ep."REVENUE_YTD")` — the alias prefix breaks substring
        # matching but word-boundary matching on the column name still works.
        col = _extract_metric_column(metric.sql)
        if col:
            candidates.append(col)
    return [c.strip() for c in candidates if c.strip()]


def _detect_used_datasets(sql_query: str, datasets: Sequence[SemanticDatasetData]) -> list[SemanticDatasetData]:
    return [
        dataset
        for dataset in datasets
        if any(_matches_sql(sql_query, candidate) for candidate in _dataset_identifiers(dataset))
    ]


def _detect_used_metrics_for_datasets(
    sql_query: str,
    metrics: Sequence[SemanticMetricData],
    used_dataset_ids: set[str],
) -> list[SemanticMetricData]:
    matches: list[SemanticMetricData] = []
    for metric in metrics:
        # Metric SQL snippets such as SUM("AMOUNT") are often reused across different datasets.
        # We therefore only accept a metric match when its source dataset was already detected
        # in the query; otherwise the metric match would be too noisy.
        if metric.dataset is None or metric.dataset not in used_dataset_ids:
            continue
        if any(_matches_sql(sql_query, candidate) for candidate in _metric_identifiers(metric)):
            matches.append(metric)
    return matches


def _extract_join_columns(on_clause: str) -> list[str]:
    """Extract bare column identifiers from a relationship ON clause.

    Strategy:
    1. Strip single-quoted string literals so constant values like ``'AVG'`` or ``'USD'``
       are not mistaken for column names.
    2. Find all uppercase identifiers of three or more characters (the convention used in
       Snowflake/BigQuery schemas for column names), de-duplicated and in order of appearance.
    3. Drop known SQL function names and keywords from ``_SQL_KEYWORDS_UPPER``.

    Returns an empty list when no uppercase identifiers are found — this happens for
    all-lowercase on-clauses (test fixtures, BigQuery style), which signals the caller to
    fall back to the original full-string match.
    """
    cleaned = re.sub(r"'[^']*'", '', on_clause)
    tokens = re.findall(r'\b([A-Z][A-Z0-9_]{2,})\b', cleaned)
    seen: set[str] = set()
    result: list[str] = []
    for token in tokens:
        if token not in _SQL_KEYWORDS_UPPER and token not in seen:
            seen.add(token)
            result.append(token)
    return result


def _detect_used_relationships(
    sql_query: str,
    relationships: Sequence[SemanticRelationshipData],
    used_dataset_ids: set[str],
) -> list[SemanticRelationshipData]:
    matches: list[SemanticRelationshipData] = []
    for relationship in relationships:
        if relationship.from_dataset is None or relationship.to_dataset is None:
            continue
        # Relationships are only considered when both datasets were already detected.
        # This keeps relationship matching conservative and avoids claiming a join path just
        # because an "on" fragment happens to appear in an unrelated query.
        if relationship.from_dataset not in used_dataset_ids or relationship.to_dataset not in used_dataset_ids:
            continue
        if relationship.on and relationship.on.strip():
            col_names = _extract_join_columns(relationship.on)
            if col_names:
                # Column-based matching: require ALL column names from the ON clause to
                # appear in the SQL.  Word-boundary matching handles quoted identifiers
                # (e.g. "FK_COL") and tolerates different table-alias conventions
                # (fact./dim. in the definition vs. bsu./coa. in the actual query).
                if not all(_matches_sql(sql_query, col) for col in col_names):
                    continue
            else:
                # No uppercase columns found (all-lowercase on-clause): fall back to
                # the original full-string match to preserve existing behaviour.
                if not _matches_sql(sql_query, relationship.on):
                    continue
        matches.append(relationship)
    return matches


def _blank_string_literals(sql: str) -> str:
    """Replaces the content of single-quoted literals with spaces, keeping every character offset intact."""
    return _STRING_LITERAL_RE.sub(lambda match: "'" + ' ' * (len(match.group()) - 2) + "'", sql)


def _identifier_parts(qualified_name: str) -> list[str]:
    return [part.strip('"').lower() for part in re.findall(_IDENTIFIER_PART, qualified_name)]


def _column_equalities(sql: str) -> list[tuple[tuple[str, str], tuple[str, str]]]:
    """Returns `((qualifier, column), (qualifier, column))` for every `a.x = b.y` predicate, lower-cased."""
    equalities = []
    for match in _COLUMN_EQUALITY_RE.finditer(_blank_string_literals(sql)):
        left, right = _identifier_parts(match.group(1)), _identifier_parts(match.group(2))
        equalities.append(((left[-2], left[-1]), (right[-2], right[-1])))
    return equalities


def _table_names(dataset: SemanticDatasetData) -> set[str]:
    """The bare table name of a dataset, the qualifier relationship ON clauses use."""
    names = (dataset.table_id, dataset.fqn)
    return {name.strip().rsplit('.', 1)[-1].strip('"').lower() for name in names if name and name.strip()}


def _dataset_references(sql_query: str, dataset: SemanticDatasetData) -> set[str]:
    """Names the SQL can qualify the dataset's columns with: its bare table name and every alias given to its fqn."""
    references = _table_names(dataset)
    if dataset.fqn and dataset.fqn.strip():
        alias_re = re.compile(
            rf'{re.escape(dataset.fqn.strip())}\s+(?:AS\s+)?("?)([A-Za-z_][A-Za-z0-9_$]*)\1(?![\w.])', re.IGNORECASE
        )
        for match in alias_re.finditer(sql_query):
            if match.group(2).upper() not in _NON_ALIAS_KEYWORDS_UPPER:
                references.add(match.group(2).lower())
    return references


def _join_keys_between(
    sql_query: str,
    equalities: Sequence[tuple[tuple[str, str], tuple[str, str]]],
    source: SemanticDatasetData,
    target: SemanticDatasetData,
) -> set[tuple[str, str]]:
    """Returns `(source column, target column)` for every SQL predicate equating a source column with a target one."""
    source_refs = _dataset_references(sql_query, source)
    target_refs = _dataset_references(sql_query, target)
    keys: set[tuple[str, str]] = set()
    for (left_qualifier, left_column), (right_qualifier, right_column) in equalities:
        if left_qualifier in source_refs and right_qualifier in target_refs:
            keys.add((left_column, right_column))
        elif left_qualifier in target_refs and right_qualifier in source_refs:
            keys.add((right_column, left_column))
    return keys


def _relationship_join_matches(
    sql_query: str,
    sql_equalities: Sequence[tuple[tuple[str, str], tuple[str, str]]],
    relationship: SemanticRelationshipData,
    datasets_by_table_id: dict[str, SemanticDatasetData],
) -> bool | None:
    """Whether the SQL joins the relationship's datasets on its keys.

    - `None`: the SQL has no predicate between the two datasets, or the ON clause has no `a.x = b.y` key.
    - `True`: every relationship key is among the SQL's join keys; extra SQL predicates are allowed.
    - `False`: the SQL joins the two datasets on other keys.
    """
    relationship_equalities = _column_equalities(relationship.on or '')
    source = datasets_by_table_id[relationship.from_dataset or '']
    target = datasets_by_table_id[relationship.to_dataset or '']
    sql_keys = _join_keys_between(sql_query, sql_equalities, source, target)
    if not relationship_equalities or not sql_keys:
        return None

    source_names, target_names = _table_names(source), _table_names(target)
    for (left_qualifier, left_column), (right_qualifier, right_column) in relationship_equalities:
        if left_qualifier in source_names and right_qualifier in target_names:
            accepted = {(left_column, right_column)}
        elif left_qualifier in target_names and right_qualifier in source_names:
            accepted = {(right_column, left_column)}
        else:
            accepted = {(left_column, right_column), (right_column, left_column)}
        if not accepted & sql_keys:
            return False
    return True


def _classify_relationship_joins(
    sql_query: str,
    relationships: Sequence[SemanticRelationshipData],
    used_datasets: Sequence[SemanticDatasetData],
) -> tuple[list[SemanticRelationshipData], list[SemanticRelationshipData]]:
    """Splits relationships between used datasets by how the SQL joins those datasets.

    - Matched: the SQL joins the datasets on the relationship's keys.
    - Conflicting: the SQL joins the datasets on other keys, and no relationship between them matches the join.
    - Relationships whose datasets the SQL does not visibly join are in neither list.
    """
    datasets_by_table_id = {
        dataset.table_id.strip(): dataset for dataset in used_datasets if dataset.table_id and dataset.table_id.strip()
    }
    sql_equalities = _column_equalities(sql_query)
    if not sql_equalities:
        return [], []

    relationships_by_pair: dict[frozenset[str], list[SemanticRelationshipData]] = {}
    for relationship in relationships:
        source, target = relationship.from_dataset, relationship.to_dataset
        if source in datasets_by_table_id and target in datasets_by_table_id and source != target:
            relationships_by_pair.setdefault(frozenset((source, target)), []).append(relationship)

    matched: list[SemanticRelationshipData] = []
    conflicts: list[SemanticRelationshipData] = []
    for pair_relationships in relationships_by_pair.values():
        verdicts = [
            (relationship, _relationship_join_matches(sql_query, sql_equalities, relationship, datasets_by_table_id))
            for relationship in pair_relationships
        ]
        matched.extend(relationship for relationship, verdict in verdicts if verdict is True)
        if not any(verdict is True for _, verdict in verdicts):
            conflicts.extend(relationship for relationship, verdict in verdicts if verdict is False)
    return matched, conflicts


def _normalize_sql_expression(expression: str) -> str:
    """Lower-cases, unquotes, drops table qualifiers and insignificant whitespace so that
    `SUM(o."AMOUNT")` and `sum("AMOUNT")` compare equal."""
    text = expression.replace('"', '').lower()
    text = re.sub(r'(?<![\w$])[a-z_][a-z0-9_$]*\s*\.\s*(?=[a-z_*])', '', text)
    text = re.sub(r'\s+', ' ', text)
    return re.sub(r'\s*([(),*/+=<>-])\s*', r'\1', text).strip()


def _aggregate_expressions(sql_query: str) -> list[str]:
    """Every aggregate function call in the SQL, e.g. `SUM("orders"."quantity")`, de-duplicated."""
    searchable = _blank_string_literals(sql_query)
    expressions: list[str] = []
    for match in _AGGREGATE_CALL_RE.finditer(searchable):
        depth = 0
        for index in range(match.end() - 1, len(searchable)):
            if searchable[index] == '(':
                depth += 1
            elif searchable[index] == ')':
                depth -= 1
                if depth == 0:
                    expressions.append(sql_query[match.start() : index + 1])
                    break
    return list(dict.fromkeys(expressions))


def _detect_undefined_aggregates(sql_query: str, metrics: Sequence[SemanticMetricData]) -> list[str]:
    """Aggregates in the SQL that are not part of any metric's SQL expression."""
    metric_expressions = [
        _normalize_sql_expression(metric.sql) for metric in metrics if metric.sql and metric.sql.strip()
    ]
    return [
        expression
        for expression in _aggregate_expressions(sql_query)
        if not any(_normalize_sql_expression(expression) in metric_sql for metric_sql in metric_expressions)
    ]


def evaluate_sql_against_model(
    sql_query: str,
    context_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup],
    used_object_groups_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup],
) -> list[ConstraintValidationFinding]:
    """Pre-execution findings where the SQL contradicts the model itself rather than one of its constraints.

    - `join_key_mismatch` (error): the SQL joins two datasets on keys no relationship between them defines.
    - `undefined_metric` (warning): an aggregate matches no defined metric; only checked when a dataset is used.
    """
    used_datasets = [
        dataset
        for dataset in used_object_groups_by_type.get(
            SemanticObjectType.SEMANTIC_DATASET,
            SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_DATASET),
        ).objects
        if isinstance(dataset, SemanticDatasetData)
    ]
    if not used_datasets:
        return []
    relationships = [
        relationship
        for relationship in context_by_type.get(
            SemanticObjectType.SEMANTIC_RELATIONSHIP,
            SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_RELATIONSHIP),
        ).objects
        if isinstance(relationship, SemanticRelationshipData)
    ]
    metrics = [
        metric
        for metric in context_by_type.get(
            SemanticObjectType.SEMANTIC_METRIC,
            SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_METRIC),
        ).objects
        if isinstance(metric, SemanticMetricData)
    ]

    findings: list[ConstraintValidationFinding] = []
    _, conflicts = _classify_relationship_joins(sql_query, relationships, used_datasets)
    for relationship in conflicts:
        relationship_name = relationship.display_name or relationship.id
        findings.append(
            ConstraintValidationFinding(
                constraint_id=f'relationship:{relationship.id}',
                constraint_name='relationship_join_keys',
                severity='error',
                status='join_key_mismatch',
                message=(
                    f'The SQL joins {relationship.from_dataset} and {relationship.to_dataset} on keys that do not '
                    f'match relationship "{relationship_name}" ({relationship.on}). Join on the relationship keys.'
                ),
            )
        )
    for expression in _detect_undefined_aggregates(sql_query, metrics):
        findings.append(
            ConstraintValidationFinding(
                constraint_id=f'undefined-metric:{expression}',
                constraint_name='undefined_metric',
                severity='warning',
                status='undefined_metric',
                message=(
                    f'Aggregate {expression} matches no metric defined in the semantic model, so the model does '
                    'not back its business meaning. Use a defined metric, or confirm the calculation with the user.'
                ),
            )
        )
    return findings


def _constraint_is_relevant(
    constraint: SemanticConstraintData,
    used_metric_names: set[str],
    used_dataset_ids: set[str],
) -> bool:
    constraint_metrics = {metric.strip() for metric in constraint.metrics if metric.strip()}
    constraint_datasets = {dataset.strip() for dataset in constraint.datasets if dataset.strip()}
    # Check if the constraint references any used metrics or datasets.
    # If it does, the constraint is relevant.
    if constraint_metrics and used_metric_names.intersection(constraint_metrics):
        return True
    if constraint_datasets and used_dataset_ids.intersection(constraint_datasets):
        return True
    # Scope-less constraints are currently treated as model-global constraints. This is a
    # pragmatic default so such constraints are not silently ignored, but it may over-match
    # if the semantic model contains broad or underspecified rules.
    return not constraint_metrics and not constraint_datasets


async def search_semantic_context(
    client: KeboolaClient,
    patterns: Sequence[str],
    *,
    semantic_types: Sequence[SemanticObjectType] = (),
    semantic_model_ids: Sequence[str] | None = None,
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
    flags = 0 if case_sensitive else re.IGNORECASE
    compiled_patterns: list[re.Pattern[str]] = []
    for pattern in cleaned_patterns:
        try:
            compiled_patterns.append(re.compile(pattern, flags))
        except re.error as e:
            raise ValueError(f'Invalid regex pattern "{pattern}": {e}') from e

    # The types are listed one after another, so resolve the models once rather than once per type.
    model_ids = semantic_model_ids or await list_semantic_model_ids(client)
    if not model_ids:
        return []

    matches: list[SemanticSearchHit] = []
    for object_type in target_types:
        if len(matches) >= max_results:
            break

        objects = await _list_semantic_type_objects(client, object_type, model_ids)
        for semantic_object in objects:
            if len(matches) >= max_results:
                break

            field_hits, pattern_hits = _find_matches(semantic_object, compiled_patterns, cleaned_patterns)
            if not pattern_hits:
                continue

            matches.append(
                SemanticSearchHit(
                    object_type=object_type,
                    semantic_model_id=_get_semantic_model_id(semantic_object),
                    object=semantic_object,
                    matched_patterns=sorted(pattern_hits),
                    matched_paths=sorted(field_hits),
                )
            )
    return matches[:max_results]


async def load_semantic_context_for_semantic_type(
    client: KeboolaClient,
    object_type: SemanticObjectType,
    *,
    ids: Sequence[str] = (),
    semantic_model_ids: Sequence[str] | None = None,
) -> SemanticServiceDataTypeGroup:
    """Get semantic context for a semantic object type, optionally filtered by semantic model IDs or object IDs."""
    if ids:
        results = await process_concurrently(
            ids,
            lambda object_id: client.metastore_client.get_object(object_type.value, object_id),
            max_concurrency=min(len(ids), 10),
        )
        raw_objects = unwrap_results(
            results,
            f'Failed to fetch semantic objects for type "{object_type.value}".',
        )
        objects = [_to_semantic_service_data(object_type, obj) for obj in raw_objects]
    else:
        objects = await _list_semantic_type_objects(client, object_type, semantic_model_ids)

    return SemanticServiceDataTypeGroup(object_type=object_type, objects=objects)


async def load_semantic_context_for_semantic_model(
    client: KeboolaClient,
    semantic_model_id: str,
    *,
    required_types: Sequence[SemanticObjectType] | None = None,
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    """Load semantic context grouped by type for the given semantic model."""
    required_types = required_types or VALIDATION_OBJECT_TYPES

    results = await process_concurrently(
        required_types,
        lambda object_type: load_semantic_context_for_semantic_type(
            client,
            object_type,
            semantic_model_ids=[semantic_model_id],
        ),
        max_concurrency=min(len(required_types), 10),
    )
    groups = unwrap_results(results, 'Failed to fetch semantic context.')
    return {group.object_type: group for group in groups}


def detect_used_objects_from_context(
    sql_query: str,
    context_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup],
    *,
    used_objects_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup] | None = None,
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    """Detect semantic objects used by the SQL query from raw semantic context. If used objects are provided,
    they will be combined with the detected objects if not detected by the heuristics"""
    used_objects_by_type = used_objects_by_type or {}
    datasets = context_by_type.get(
        SemanticObjectType.SEMANTIC_DATASET,
        SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_DATASET),
    )
    metrics = context_by_type.get(
        SemanticObjectType.SEMANTIC_METRIC,
        SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_METRIC),
    )
    relationships = context_by_type.get(
        SemanticObjectType.SEMANTIC_RELATIONSHIP,
        SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_RELATIONSHIP),
    )
    # Detection is intentionally layered:
    # 1. detect datasets first
    # 2. detect metrics only within those datasets
    # 3. detect relationships only between those detected datasets
    # This keeps later detections narrower and reduces false positives.

    used_dataset_objects = _detect_used_datasets(sql_query, datasets.objects)
    if expected := used_objects_by_type.get(SemanticObjectType.SEMANTIC_DATASET):
        expected_objects = expected.objects
        ids = {obj.id for obj in used_dataset_objects}
        used_dataset_objects = used_dataset_objects + [obj for obj in expected_objects if obj.id not in ids]
    used_dataset_ids = {
        item.table_id.strip() for item in used_dataset_objects if item.table_id and item.table_id.strip()
    }

    used_metric_objects = _detect_used_metrics_for_datasets(sql_query, metrics.objects, used_dataset_ids)
    if expected := used_objects_by_type.get(SemanticObjectType.SEMANTIC_METRIC):
        expected_objects = expected.objects
        ids = {obj.id for obj in used_metric_objects}
        used_metric_objects = used_metric_objects + [obj for obj in expected_objects if obj.id not in ids]

    used_relationship_objects = _detect_used_relationships(sql_query, relationships.objects, used_dataset_ids)
    joined_relationships, conflicting_relationships = _classify_relationship_joins(
        sql_query, relationships.objects, used_dataset_objects
    )
    ids = {obj.id for obj in used_relationship_objects}
    used_relationship_objects = used_relationship_objects + [obj for obj in joined_relationships if obj.id not in ids]
    if expected := used_objects_by_type.get(SemanticObjectType.SEMANTIC_RELATIONSHIP):
        expected_objects = expected.objects
        ids = {obj.id for obj in used_relationship_objects}
        used_relationship_objects = used_relationship_objects + [obj for obj in expected_objects if obj.id not in ids]
    conflicting_relationship_ids = {relationship.id for relationship in conflicting_relationships}
    used_relationship_objects = [
        relationship
        for relationship in used_relationship_objects
        if relationship.id not in conflicting_relationship_ids
    ]

    used_groups: dict[SemanticObjectType, SemanticServiceDataTypeGroup] = {}
    if used_dataset_objects:
        used_groups[SemanticObjectType.SEMANTIC_DATASET] = SemanticServiceDataTypeGroup(
            object_type=SemanticObjectType.SEMANTIC_DATASET,
            objects=used_dataset_objects,
        )
    if used_metric_objects:
        used_groups[SemanticObjectType.SEMANTIC_METRIC] = SemanticServiceDataTypeGroup(
            object_type=SemanticObjectType.SEMANTIC_METRIC,
            objects=used_metric_objects,
        )
    if used_relationship_objects:
        used_groups[SemanticObjectType.SEMANTIC_RELATIONSHIP] = SemanticServiceDataTypeGroup(
            object_type=SemanticObjectType.SEMANTIC_RELATIONSHIP,
            objects=used_relationship_objects,
        )
    return used_groups


def evaluate_constraints_from_context(
    context_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup],
    used_object_groups_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup],
) -> SemanticValidationServiceOutput:
    """Evaluate relevant semantic constraints for the used semantic objects."""
    model_group = context_by_type.get(
        SemanticObjectType.SEMANTIC_MODEL,
        SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_MODEL),
    )
    model = next(iter(model_group.objects), None)
    constraint_group = context_by_type.get(
        SemanticObjectType.SEMANTIC_CONSTRAINT,
        SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_CONSTRAINT),
    )
    constraints = constraint_group.objects
    used_dataset_objects = (
        used_object_groups_by_type[SemanticObjectType.SEMANTIC_DATASET].objects
        if SemanticObjectType.SEMANTIC_DATASET in used_object_groups_by_type
        else []
    )
    used_metric_objects = (
        used_object_groups_by_type[SemanticObjectType.SEMANTIC_METRIC].objects
        if SemanticObjectType.SEMANTIC_METRIC in used_object_groups_by_type
        else []
    )
    used_relationship_objects = (
        used_object_groups_by_type[SemanticObjectType.SEMANTIC_RELATIONSHIP].objects
        if SemanticObjectType.SEMANTIC_RELATIONSHIP in used_object_groups_by_type
        else []
    )

    used_dataset_ids = {
        item.table_id.strip() for item in used_dataset_objects if item.table_id and item.table_id.strip()
    }
    used_metric_names = {item.name.strip() for item in used_metric_objects if item.name and item.name.strip()}
    matched_relationships = sorted(
        item.name or getattr(item.data.meta, 'name', None) or item.id for item in used_relationship_objects
    )

    sql_dialect_str = model.sql_dialect if model is not None else None
    violations: list[ConstraintValidationFinding] = []
    post_execution_checks: list[ConstraintValidationFinding] = []
    has_error = False

    for constraint in constraints:
        assert isinstance(constraint, SemanticConstraintData)
        if not _constraint_is_relevant(constraint, used_metric_names, used_dataset_ids):
            continue

        constraint_name = constraint.name or getattr(constraint.data.meta, 'name', None) or constraint.id
        severity = constraint.severity or 'error'
        constraint_type = constraint.constraint_type or 'unknown'
        validation_query = _pick_validation_query(constraint, sql_dialect_str)
        constraint_metrics = [metric.strip() for metric in constraint.metrics if metric.strip()]
        constraint_datasets = [dataset.strip() for dataset in constraint.datasets if dataset.strip()]
        pre_query_check = constraint.pre_query_check

        if constraint_type == 'composition':
            # Composition constraints are the one class we can reliably check before execution:
            # they usually declare that if one semantic metric family is used, other metrics
            # must also be present in the same SQL.
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
            # Exclusion constraints model forbidden combinations. We only flag them when the
            # query appears to use more than one excluded item from the same constraint scope.
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
            # Some constraints explicitly ask for manual pre-query review even when we cannot
            # mechanically prove a violation from SQL text alone.
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

        # Everything else is treated as a post-query concern unless the constraint has no
        # recognized post-query semantics and does not provide its own validation SQL.
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

    return SemanticValidationServiceOutput(
        valid=not has_error,
        used_object_groups=list(used_object_groups_by_type.values()),
        matched_relationships=matched_relationships,
        violations=violations,
        post_execution_checks=post_execution_checks,
    )


def _merge_contexts(
    contexts: list[dict[SemanticObjectType, SemanticServiceDataTypeGroup]],
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    """Merge semantic contexts from multiple models into a single combined context."""
    merged: dict[SemanticObjectType, list[SemanticServiceData]] = {}
    for context in contexts:
        for object_type, group in context.items():
            merged.setdefault(object_type, []).extend(group.objects)
    return {
        object_type: SemanticServiceDataTypeGroup(object_type=object_type, objects=objects)
        for object_type, objects in merged.items()
    }


def _filter_used_objects_by_model(
    used_object_groups_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup],
    model_id: str,
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    """Filter used object groups to only objects belonging to the given model."""
    filtered: dict[SemanticObjectType, SemanticServiceDataTypeGroup] = {}
    for object_type, group in used_object_groups_by_type.items():
        model_objects = [obj for obj in group.objects if _get_semantic_model_id(obj) == model_id]
        if model_objects:
            filtered[object_type] = SemanticServiceDataTypeGroup(object_type=object_type, objects=model_objects)
    return filtered


async def _load_validation_contexts(
    client: KeboolaClient,
    semantic_model_ids: Sequence[str],
) -> list[dict[SemanticObjectType, SemanticServiceDataTypeGroup]]:
    if not semantic_model_ids:
        raise ValueError('At least one semantic_model_id must be provided.')

    results = await process_concurrently(
        semantic_model_ids,
        lambda model_id: load_semantic_context_for_semantic_model(client, model_id),
        max_concurrency=min(len(semantic_model_ids), 10),
    )
    return unwrap_results(results, 'Failed to fetch semantic context.')


def _merge_used_object_groups(
    used_object_groups: Sequence[SemanticServiceDataTypeGroup],
) -> dict[SemanticObjectType, SemanticServiceDataTypeGroup]:
    merged: dict[SemanticObjectType, list[SemanticServiceData]] = {}
    for group in used_object_groups:
        merged.setdefault(group.object_type, []).extend(group.objects)

    return {
        object_type: SemanticServiceDataTypeGroup(object_type=object_type, objects=objects)
        for object_type, objects in merged.items()
    }


def _evaluate_used_objects_for_contexts(
    semantic_model_ids: Sequence[str],
    contexts_per_model: Sequence[dict[SemanticObjectType, SemanticServiceDataTypeGroup]],
    used_object_groups_by_type: dict[SemanticObjectType, SemanticServiceDataTypeGroup],
) -> SemanticValidationServiceOutput:
    all_violations: list[ConstraintValidationFinding] = []
    all_post_checks: list[ConstraintValidationFinding] = []
    has_error = False
    for model_id, context_by_type in zip(semantic_model_ids, contexts_per_model, strict=True):
        model_used_objects = _filter_used_objects_by_model(used_object_groups_by_type, model_id)
        per_model_result = evaluate_constraints_from_context(context_by_type, model_used_objects)
        all_violations.extend(per_model_result.violations)
        all_post_checks.extend(per_model_result.post_execution_checks)
        if not per_model_result.valid:
            has_error = True

    used_relationships = used_object_groups_by_type.get(
        SemanticObjectType.SEMANTIC_RELATIONSHIP,
        SemanticServiceDataTypeGroup(object_type=SemanticObjectType.SEMANTIC_RELATIONSHIP),
    )
    matched_relationships = sorted(
        item.name or getattr(item.data.meta, 'name', None) or item.id for item in used_relationships.objects
    )

    return SemanticValidationServiceOutput(
        valid=not has_error,
        used_object_groups=list(used_object_groups_by_type.values()),
        matched_relationships=matched_relationships,
        violations=all_violations,
        post_execution_checks=all_post_checks,
    )


async def validate_semantic_query_with_used_objects(
    client: KeboolaClient,
    sql_query: str,
    semantic_model_ids: Sequence[str],
    *,
    used_object_groups: Sequence[SemanticServiceDataTypeGroup] | None = None,
    contexts_per_model: list[dict[SemanticObjectType, SemanticServiceDataTypeGroup]] | None = None,
) -> SemanticValidationServiceOutput:
    """Validate SQL against one or more semantic models without executing it.
    If used objects are provided, they will be combined with the detected objects if not detected from sql query
    by the heuristics.
    Contexts from all requested models are merged into a single universe for object detection.
    Constraint evaluation is performed per model to avoid cross-model rule contamination.
    """
    if not sql_query.strip():
        raise ValueError('sql_query must not be empty.')

    cleaned_model_ids = list(dict.fromkeys(mid.strip() for mid in semantic_model_ids if mid and mid.strip()))
    if not cleaned_model_ids:
        raise ValueError('At least one semantic_model_id must be provided.')

    if contexts_per_model is None:
        contexts_per_model = await _load_validation_contexts(client, cleaned_model_ids)
    used_object_groups = used_object_groups or []

    merged_context = _merge_contexts(contexts_per_model)
    used_object_groups_by_type = _merge_used_object_groups(used_object_groups)
    used_object_groups_by_type = detect_used_objects_from_context(
        sql_query, merged_context, used_objects_by_type=used_object_groups_by_type
    )
    result = _evaluate_used_objects_for_contexts(cleaned_model_ids, contexts_per_model, used_object_groups_by_type)
    model_findings = evaluate_sql_against_model(sql_query, merged_context, used_object_groups_by_type)
    return result.model_copy(
        update={
            'valid': result.valid and not any(finding.severity == 'error' for finding in model_findings),
            'violations': [*result.violations, *model_findings],
        }
    )


async def get_object_by_id(
    client: KeboolaClient,
    object_type: SemanticObjectType,
    object_id: str,
) -> SemanticServiceData:
    raw_obj = await client.metastore_client.get_object(object_type.value, object_id)
    if raw_obj.type != object_type.value:
        raise ValueError(
            f'Expected object "{object_id}" to be of type "{object_type.value}", '
            f'got "{raw_obj.type}" from the Metastore API.'
        )
    return _to_semantic_service_data(object_type, raw_obj)
