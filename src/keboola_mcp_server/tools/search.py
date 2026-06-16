import asyncio
import json
import logging
import re
from collections import defaultdict
from typing import Annotated, Any, AsyncGenerator, Iterable, Literal, Mapping, Sequence, cast

import jsonpath_ng
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.tools import FunctionTool
from jsonpath_ng.jsonpath import JSONPath
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field, PrivateAttr, model_validator

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    DATA_APP_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.clients.storage import GlobalSearchResponse
from keboola_mcp_server.clients.storage import ItemType as ApiItemType
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import toon_serializer_compact
from keboola_mcp_server.tools.components.utils import _normalize_jsonpath, get_nested
from keboola_mcp_server.tools.storage_helpers import merged_bucket_list, merged_bucket_table_list

LOG = logging.getLogger(__name__)

SEARCH_TOOL_NAME = 'search'
MAX_GLOBAL_SEARCH_LIMIT = 100
DEFAULT_GLOBAL_SEARCH_LIMIT = 50
SEARCH_TOOLS_TAG = 'search'

SearchItemType = Literal[
    'bucket',
    'table',
    'data-app',
    'flow',
    'transformation',
    'component',
    'configuration',
    'configuration-row',
    'workspace',
    'shared-code',
    'rows',
    'state',
]


SearchComponentItemType = Literal[
    'flow',
    'transformation',
    'component',
    'configuration',
    'configuration-row',
    'workspace',
]


SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES: Mapping[SearchItemType, Sequence[str]] = {
    'data-app': ['other'],
    'flow': ['other'],
    'transformation': ['transformation'],
    'configuration': ['extractor', 'writer', 'application'],
    'configuration-row': ['extractor', 'writer', 'application'],
    'component': ['extractor', 'writer', 'application'],
    'workspace': ['other'],
}

GLOBAL_SEARCH_FEATURE = 'global-search'
WORKSPACE_COMPONENT_ID = 'keboola.sandboxes'

# Maps the tool's item types to the API types requested from the global-search endpoint. Some tool
# types (data-app, flow, workspace) exist server-side as 'configuration' items distinguished only
# by their component ID, so 'configuration' is over-fetched and narrowed client-side after re-typing.
SEARCH_ITEM_TYPE_TO_API_TYPES: Mapping[SearchItemType, Sequence[ApiItemType]] = {
    'bucket': ('bucket',),
    'table': ('table',),
    'transformation': ('transformation',),
    'configuration': ('configuration',),
    'configuration-row': ('configuration-row',),
    'component': ('configuration', 'configuration-row'),
    'flow': ('flow', 'configuration'),
    'data-app': ('configuration',),
    'workspace': ('workspace', 'configuration'),
    'shared-code': ('shared-code',),
    'rows': ('rows',),
    'state': ('state',),
}

SearchType = Literal['textual', 'config-based']
SearchPatternMode = Literal['regex', 'literal']
SearchBranchScope = Literal['current-branch', 'all-branches']


def add_search_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    LOG.info(f'Adding tool {find_component_id.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            find_component_id,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEARCH_TOOLS_TAG},
        )
    )

    LOG.info(f'Adding tool {search.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            search,
            name=SEARCH_TOOL_NAME,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={SEARCH_TOOLS_TAG},
        )
    )

    LOG.info('Search tools initialized.')


class PatternMatch(BaseModel):
    scope: str | None
    patterns: list[str]


class SearchHit(BaseModel):
    bucket_id: str | None = Field(default=None, description='The ID of the bucket.')
    table_id: str | None = Field(default=None, description='The ID of the table.')
    component_id: str | None = Field(default=None, description='The ID of the component.')
    configuration_id: str | None = Field(default=None, description='The ID of the configuration.')
    configuration_row_id: str | None = Field(default=None, description='The ID of the configuration row.')

    item_type: SearchItemType = Field(description='The type of the item (e.g. table, bucket, configuration, etc.).')
    updated: str = Field(
        description='The date and time the item was last updated (or created, when the update time is not '
        'available) in ISO 8601 format.'
    )

    name: str | None = Field(default=None, description='Name of the item.')
    display_name: str | None = Field(default=None, description='Display name of the item.')
    description: str | None = Field(default=None, description='Description of the item.')
    branch_id: str | None = Field(
        default=None, description='ID of the branch the item belongs to, when reported by the search backend.'
    )
    branch_name: str | None = Field(
        default=None, description='Name of the branch the item belongs to, when reported by the search backend.'
    )
    matches: list[PatternMatch] = Field(
        default_factory=list,
        description='Most specific JSONPath scopes with grouped matched patterns (config-based search only).',
    )
    links: list[Link] = Field(default_factory=list, description='Links to the item.')

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SearchHit):
            return self.model_dump() == other.model_dump()
        return False

    @model_validator(mode='after')
    def check_id_fields(self) -> 'SearchHit':
        id_fields = [
            self.bucket_id,
            self.table_id,
            self.component_id,
            self.configuration_id,
            self.configuration_row_id,
        ]

        if not any(field for field in id_fields if field):
            raise ValueError('At least one ID field must be filled.')

        if self.configuration_row_id and not all([self.component_id, self.configuration_id]):
            raise ValueError(
                'If configuration_row_id is filled, ' 'both component_id and configuration_id must be filled.'
            )

        if self.configuration_id and not self.component_id:
            raise ValueError('If configuration_id is filled, component_id must be filled.')

        return self

    def set_matches(self, matches: list['PatternMatch']) -> 'SearchHit':
        """Assign pattern matches to this search hit and return self for chaining."""
        patterns_by_scope: dict[str, set[str]] = defaultdict(set)
        for match in matches:
            if not match.scope:
                continue
            patterns_by_scope[match.scope].update(match.patterns)

        unique_scopes = list(patterns_by_scope)
        most_specific_scopes = [
            scope
            for scope in unique_scopes
            if not any(
                other.startswith(scope) and len(other) > len(scope) and other[len(scope)] in ('.', '[')
                for other in unique_scopes
            )
        ]
        self.matches = [
            PatternMatch(scope=scope, patterns=list(patterns_by_scope[scope])) for scope in most_specific_scopes
        ]
        return self


class SearchOutput(BaseModel):
    """Paginated search results with total counts."""

    hits: list[SearchHit] = Field(description='The matching items (paginated).')
    total: int = Field(
        description='Approximate total number of matching items before pagination; treat it as an upper bound on '
        'the items reachable via pagination. With multiple patterns, an item matching more than one pattern is '
        'counted once per pattern; for textual search the count may also include items later removed by client-side '
        'type narrowing (e.g. configurations re-typed to data-apps/flows/workspaces).'
    )
    by_type: dict[str, int] = Field(
        default_factory=dict,
        description='Number of matching items per item type (before pagination and client-side narrowing).',
    )
    branch_scope: SearchBranchScope = Field(
        default='current-branch',
        description="Branch scope the hits come from. 'all-branches' means nothing was found in the current "
        "branch context and the search was widened to the whole project; check each hit's branch_id/branch_name "
        'to see where it lives.',
    )


class SearchSpec(BaseModel):
    patterns: Sequence[str]
    item_types: Sequence[SearchItemType]
    pattern_mode: SearchPatternMode = 'regex'
    case_sensitive: bool = False
    search_scopes: Sequence[str] = tuple()
    search_type: SearchType = 'textual'
    return_all_matched_patterns: bool = False

    _component_types: Sequence[str] = PrivateAttr(default_factory=tuple)
    _compiled_patterns: list[re.Pattern] = PrivateAttr(default_factory=list)
    _clean_patterns: list[str] = PrivateAttr(default_factory=list)
    _all_nodes_expr: JSONPath | None = PrivateAttr(default=None)
    # Tuple fields: (original_scope, parsed_scope_expr, parsed_descendants_expr)
    _scope_exprs: list[tuple[str, JSONPath, JSONPath]] = PrivateAttr(default_factory=list)

    @model_validator(mode='after')
    def _compile_patterns(self) -> 'SearchSpec':
        cleaned_patterns = [str(item).strip() for item in self.patterns if item is not None and str(item).strip()]
        if not cleaned_patterns:
            raise ValueError('At least one search pattern must be provided.')

        self.patterns = cleaned_patterns
        flags = 0 if self.case_sensitive else re.IGNORECASE
        if self.pattern_mode == 'literal':
            self._compiled_patterns = [re.compile(re.escape(pattern), flags) for pattern in cleaned_patterns]
        else:
            self._compiled_patterns = [re.compile(pattern, flags) for pattern in cleaned_patterns]

        self._clean_patterns = cleaned_patterns
        return self

    @model_validator(mode='after')
    def _validate_component_args(self) -> 'SearchSpec':
        if not self._component_types:
            self._component_types = list(
                set(
                    component_type
                    for item in self.item_types
                    for component_type in SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES.get(item, [])
                )
            )
        return self

    @model_validator(mode='after')
    def _validate_item_types(self) -> 'SearchSpec':
        if 'component' in self.item_types:
            self.item_types = list({*self.item_types, 'configuration', 'configuration-row'})
        return self

    @model_validator(mode='after')
    def _compile_jsonpath_exprs(self) -> 'SearchSpec':
        # Compile commonly used expressions once per SearchSpec instance.
        self._all_nodes_expr = jsonpath_ng.parse('$..*')
        self._scope_exprs = []
        for scope in self.search_scopes:
            normalized = _normalize_jsonpath(scope if scope.startswith('$') else f'$.{scope}')
            try:
                self._scope_exprs.append((scope, jsonpath_ng.parse(normalized), jsonpath_ng.parse(f'{normalized}..*')))
            except Exception as e:
                LOG.warning(f'Invalid JSONPath scope "{scope}": {e}')
        return self

    @staticmethod
    def _stringify(value: Any) -> str:
        try:
            return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(value)

    def match_patterns(self, value: str | JsonDict | None) -> list[str]:
        """
        Matches a string or dictionary value against the patterns.

        :param value: The value to match against the patterns.
        :return: A list of patterns that matched the value; empty list if no matches.
        """
        if value is None:
            return []
        haystack = value if isinstance(value, str) else self._stringify(value)
        if not haystack:
            return []

        matches: list[str] = []
        for pattern, compiled in zip(self._clean_patterns, self._compiled_patterns):
            if compiled.search(haystack):
                matches.append(pattern)
                if not self.return_all_matched_patterns:
                    break

        return matches

    def _find_matches_for_expr(
        self, configuration: JsonDict, parsed_expr: JSONPath, scalar_only: bool = False
    ) -> list[PatternMatch]:
        """Find pattern matches on JSON nodes matched by a JSONPath expression. If scalar_only is True, only scalar
        nodes are matched."""
        matches: list[PatternMatch] = []
        for jpath_match in parsed_expr.find(configuration):
            value = jpath_match.value
            if scalar_only and isinstance(value, (dict, list)):
                continue
            if matched := self.match_patterns(value):
                matches.append(
                    PatternMatch(
                        scope=_clean_jsonpath_path_str(str(jpath_match.full_path)),
                        patterns=matched,
                    )
                )
                if not self.return_all_matched_patterns:
                    return matches
        return matches

    def match_configuration_scopes(self, configuration: JsonDict | None) -> list[PatternMatch]:
        """
        Checks configuration fields within specified JSONPath scopes for pattern matches.
        Walks matching nodes within each scope and returns the exact path where the match
        was found. When no scopes are specified, walks the entire configuration.

        :param configuration: The configuration to match against the patterns.
        :return: List of PatternMatch with matching JSONPath scopes; empty list if no matches.
        """
        if configuration is None:
            return []

        if self.search_scopes:
            all_matches: list[PatternMatch] = []
            # Deduplicate hits when scopes overlap (e.g. "parameters" + "parameters.query")
            # or the same logical scope is provided multiple times.
            seen: set[str | None] = set()
            for _scope, self_expr, desc_expr in self._scope_exprs:
                # Search in self expression node for scalar matches first
                self_matches = self._find_matches_for_expr(configuration, self_expr, scalar_only=True)
                # If no scalar matches, search in descendants nodes
                desc_matches: list[PatternMatch] = []
                if not self_matches:
                    desc_matches = self._find_matches_for_expr(configuration, desc_expr)
                for match in self_matches or desc_matches:
                    if match.scope in seen:
                        continue
                    seen.add(match.scope)
                    all_matches.append(match)
                    if not self.return_all_matched_patterns:
                        return all_matches
            return all_matches
        else:
            # No scope provided – search all descendants and return exact match paths.
            return self._find_matches_for_expr(configuration, self._all_nodes_expr)

    def match_texts(self, texts: Iterable[str]) -> list[PatternMatch]:
        """
        Matches a sequence of strings against the patterns.

        :param texts: The sequence of strings to match against the patterns.
        :return: A list of PatternMatch objects.
        """
        matches: list[PatternMatch] = []
        for text in texts:
            if matched := self.match_patterns(text):
                matches.append(PatternMatch(scope=None, patterns=matched))
                if not self.return_all_matched_patterns:
                    break
        return matches


def _clean_jsonpath_path_str(path_str: str) -> str:
    """Normalize a jsonpath_ng full_path string across library versions.

    jsonpath_ng >= 1.8.0 wraps Child nodes in parentheses and single-quotes field names
    with special characters, e.g. "(authorization.'#apiKey')" instead of "authorization.#apiKey".
    """
    # Strip parentheses added by jsonpath_ng >= 1.8.0
    result = path_str.replace('(', '').replace(')', '')
    # Remove surrounding quotes from field name segments, e.g. "'#apiKey'" -> "#apiKey"
    result = re.sub(r"['\"]([^'\"]+)['\"]", r'\1', result)
    # Normalize .[N] -> [N]
    return re.sub(r'\.\[', '[', result)


def _get_field_value(item: JsonDict, fields: Sequence[str]) -> Any | None:
    for field in fields:
        if value := get_nested(item, field):
            return value
    return None


def _check_column_match(table: JsonDict, cfg: SearchSpec) -> list[PatternMatch]:
    """Check if any column name or description matches the patterns."""
    # Check column names (list of strings)
    if col_names := table.get('columns', []):
        if matched := cfg.match_texts(col_names):
            return matched

    if col_metadata := table.get('columnMetadata', {}):
        col_descs = (get_metadata_property(col_meta, MetadataField.DESCRIPTION) for col_meta in col_metadata.values())
        if matched := cfg.match_texts(filter(None, col_descs)):
            return matched
    return []


async def _fetch_buckets(client: KeboolaClient, spec: SearchSpec) -> list[SearchHit]:
    """Fetches and filters buckets."""
    hits = []
    for bucket in await merged_bucket_list(client):
        if not (bucket_id := bucket.get('id')):
            continue

        bucket_name = bucket.get('name')
        bucket_display_name = bucket.get('displayName')
        bucket_description = get_metadata_property(bucket.get('metadata', []), MetadataField.DESCRIPTION)

        if matches := spec.match_texts([bucket_id, bucket_name, bucket_display_name, bucket_description]):
            hits.append(
                SearchHit(
                    bucket_id=bucket_id,
                    item_type='bucket',
                    updated=_get_field_value(bucket, ['lastChangeDate', 'updated', 'created']) or '',
                    name=bucket_name,
                    display_name=bucket_display_name,
                    description=bucket_description,
                ).set_matches(matches)
            )
    return hits


async def _fetch_tables(client: KeboolaClient, spec: SearchSpec) -> list[SearchHit]:
    """Fetches and filters tables from all buckets."""
    hits = []
    for bucket in await merged_bucket_list(client):
        if not (bucket_id := bucket.get('id')):
            continue

        tables = await merged_bucket_table_list(client, bucket_id, include=['columns', 'columnMetadata'])
        for table in tables:
            if not (table_id := table.get('id')):
                continue

            table_name = table.get('name')
            table_display_name = table.get('displayName')
            table_description = get_metadata_property(table.get('metadata', []), MetadataField.DESCRIPTION)

            matches = spec.match_texts([table_id, table_name, table_display_name, table_description])
            matches.extend(_check_column_match(table, spec))
            if matches:
                hits.append(
                    SearchHit(
                        table_id=table_id,
                        item_type='table',
                        updated=_get_field_value(table, ['lastChangeDate', 'created']) or '',
                        name=table_name,
                        display_name=table_display_name,
                        description=table_description,
                    ).set_matches(matches)
                )
    return hits


async def fetch_configurations(client: KeboolaClient, spec: SearchSpec) -> list[SearchHit]:
    """Fetches and filters configurations and configuration rows from all component types."""
    hits = []

    if spec._component_types:
        for component_type in spec._component_types:
            async for hit in _fetch_configs(client, spec, component_type=component_type):
                hits.append(hit)

    else:
        async for hit in _fetch_configs(client, spec, component_type=None):
            hits.append(hit)

    return hits


async def _fetch_configs(
    client: KeboolaClient, spec: SearchSpec, component_type: str | None = None
) -> AsyncGenerator[SearchHit, None]:
    components = await client.storage_client.component_list(component_type, include=['configuration', 'rows'])

    allowed_transformations = 'transformation' in spec.item_types or component_type is None
    allowed_components = (
        'configuration' in spec.item_types or 'configuration-row' in spec.item_types or component_type is None
    )
    allowed_flows = 'flow' in spec.item_types or component_type is None
    allowed_workspaces = 'workspace' in spec.item_types or component_type is None
    allowed_data_apps = 'data-app' in spec.item_types or component_type is None

    for component in components:
        if not (component_id := component.get('id')):
            continue

        current_component_type = component.get('type')
        if component_id in [ORCHESTRATOR_COMPONENT_ID, CONDITIONAL_FLOW_COMPONENT_ID]:
            item_type: SearchItemType = 'flow'
            if not allowed_flows:
                continue
        elif current_component_type == 'transformation':
            item_type: SearchItemType = 'transformation'
            if not allowed_transformations:
                continue
        elif component_id == WORKSPACE_COMPONENT_ID:
            item_type: SearchItemType = 'workspace'
            if not allowed_workspaces:
                continue
        elif component_id == DATA_APP_COMPONENT_ID:
            item_type: SearchItemType = 'data-app'
            if not allowed_data_apps:
                continue
        elif current_component_type in ['extractor', 'writer', 'application']:
            item_type: SearchItemType = 'configuration'
            if not allowed_components:
                continue
        else:
            item_type: SearchItemType = 'configuration'

        for config in component.get('configurations', []):
            if not (config_id := config.get('id')):
                continue

            config_name = config.get('name')
            config_description = config.get('description')
            config_updated = _get_field_value(config, ['currentVersion.created', 'created']) or ''

            if spec.search_type == 'textual':
                if matches := spec.match_texts([config_id, config_name, config_description]):
                    yield SearchHit(
                        component_id=component_id,
                        configuration_id=config_id,
                        item_type=item_type,
                        updated=config_updated,
                        name=config_name,
                        description=config_description,
                    ).set_matches(matches)
            elif spec.search_type == 'config-based':
                if matches := spec.match_configuration_scopes(config.get('configuration')):
                    yield SearchHit(
                        component_id=component_id,
                        configuration_id=config_id,
                        item_type=item_type,
                        updated=config_updated,
                        name=config_name,
                        description=config_description,
                    ).set_matches(matches)

            for row in config.get('rows', []):
                if not (row_id := row.get('id')):
                    continue

                row_name = row.get('name')
                row_description = row.get('description')

                if spec.search_type == 'textual':
                    if matches := spec.match_texts([row_id, row_name, row_description]):
                        yield SearchHit(
                            component_id=component_id,
                            configuration_id=config_id,
                            configuration_row_id=row_id,
                            item_type='configuration-row',
                            updated=config_updated or _get_field_value(row, ['created']),
                            name=row_name,
                            description=row_description,
                        ).set_matches(matches)

                elif spec.search_type == 'config-based':
                    if matches := spec.match_configuration_scopes(row.get('configuration')):
                        yield SearchHit(
                            component_id=component_id,
                            configuration_id=config_id,
                            configuration_row_id=row_id,
                            item_type='configuration-row',
                            updated=config_updated or _get_field_value(row, ['created']),
                            name=row_name,
                            description=row_description,
                        ).set_matches(matches)


def _api_types_for(item_types: Sequence[SearchItemType]) -> list[ApiItemType]:
    """Maps the tool's item types to a deduplicated list of API types for the global-search endpoint."""
    api_types: list[ApiItemType] = []
    for item_type in item_types:
        for api_type in SEARCH_ITEM_TYPE_TO_API_TYPES.get(item_type, ()):
            if api_type not in api_types:
                api_types.append(api_type)
    return api_types


def _retype_configuration(component_id: str | None) -> SearchItemType:
    """Maps a 'configuration' global-search item to the tool's more specific item type by its component."""
    if component_id in (ORCHESTRATOR_COMPONENT_ID, CONDITIONAL_FLOW_COMPONENT_ID):
        return 'flow'
    if component_id == DATA_APP_COMPONENT_ID:
        return 'data-app'
    if component_id == WORKSPACE_COMPONENT_ID:
        return 'workspace'
    return 'configuration'


def _global_search_hit(item: GlobalSearchResponse.Item) -> SearchHit | None:
    """Maps a global-search item to a SearchHit; returns None for items that cannot be mapped."""
    common: dict[str, Any] = {
        'updated': item.created.isoformat(),
        'name': item.name,
        'branch_id': item.branch_id,
        'branch_name': item.branch_name,
    }

    if item.type == 'bucket':
        return SearchHit(bucket_id=item.id, item_type='bucket', **common)

    if item.type == 'table':
        bucket = item.full_path.get('bucket')
        bucket_id = str(bucket['id']) if isinstance(bucket, dict) and bucket.get('id') else None
        return SearchHit(table_id=item.id, bucket_id=bucket_id, item_type='table', **common)

    if item.type in ('configuration-row', 'rows'):
        configuration = item.full_path.get('configuration')
        configuration_id = (
            str(configuration['id']) if isinstance(configuration, dict) and configuration.get('id') else None
        )
        if not (item.component_id and configuration_id):
            LOG.warning(f'Skipping global-search row hit with no parent configuration in fullPath: {item.id}')
            return None
        return SearchHit(
            component_id=item.component_id,
            configuration_id=configuration_id,
            configuration_row_id=item.id,
            item_type='configuration-row',
            **common,
        )

    # The remaining types (configuration, transformation, flow, workspace, shared-code, state) are all
    # configuration-like items whose id is the configuration ID.
    component_id = item.component_id or (WORKSPACE_COMPONENT_ID if item.type == 'workspace' else None)
    if not component_id:
        LOG.warning(f'Skipping global-search hit with no component id: {item.type} {item.id}')
        return None
    item_type = _retype_configuration(component_id) if item.type == 'configuration' else cast(SearchItemType, item.type)
    return SearchHit(component_id=component_id, configuration_id=item.id, item_type=item_type, **common)


async def _global_textual_search(
    client: KeboolaClient,
    spec: SearchSpec,
    limit: int,
    offset: int,
) -> SearchOutput:
    """
    Searches item names server-side via the SAPI global-search endpoint, scoped to the current project.

    Runs one request per pattern (patterns are OR-ed, mirroring the legacy behavior) against the current
    branch context first; when nothing is found, widens the search to the whole project (all branches).
    """
    api_types = _api_types_for(spec.item_types)
    # 'rows' hits are reported as 'configuration-row' and 'component' expands to configuration (rows);
    # normalize the requested types accordingly for the client-side narrowing.
    requested_types = {'configuration-row' if t == 'rows' else t for t in spec.item_types if t != 'component'}

    # The 'configuration' API type is lossy: a single server-side 'configuration' item may re-type to
    # configuration/data-app/flow/workspace, so when the caller filters by type the server can fill a page
    # with items that are dropped during client-side narrowing, under-filling the page. Over-fetch up to the
    # server max in that case so the narrowed page is more likely to reach `limit`. Deep pagination (large
    # offset) remains approximate because the server paginates in un-narrowed space.
    needs_overfetch = bool(requested_types) and 'configuration' in api_types
    fetch_limit = MAX_GLOBAL_SEARCH_LIMIT if needs_overfetch else limit

    async def query(branch_scope: Literal['current', 'all']) -> list[GlobalSearchResponse]:
        return list(
            await asyncio.gather(
                *(
                    client.storage_client.global_search(
                        query=pattern, types=api_types, limit=fetch_limit, offset=offset, branch_scope=branch_scope
                    )
                    for pattern in spec.patterns
                )
            )
        )

    def collect(responses: list[GlobalSearchResponse]) -> list[SearchHit]:
        hits_by_key: dict[tuple[str, str], SearchHit] = {}
        for response in responses:
            for item in response.items:
                if (hit := _global_search_hit(item)) is None:
                    continue
                if requested_types and hit.item_type not in requested_types:
                    continue
                hits_by_key.setdefault((item.type, item.id), hit)
        return list(hits_by_key.values())

    branch_scope: Literal['current', 'all'] = 'current'
    responses = await query(branch_scope)
    hits = collect(responses)
    if not hits and offset == 0:
        # Nothing in the current branch context — widen to the whole project so that items living
        # in other branches can be discovered. Hits carry branch_id/branch_name for attribution.
        branch_scope = 'all'
        responses = await query(branch_scope)
        hits = collect(responses)

    hits.sort(
        key=lambda x: (
            x.updated,
            x.bucket_id or x.table_id or x.component_id or x.configuration_id or x.configuration_row_id,
        ),
        reverse=True,
    )

    by_type: dict[str, int] = defaultdict(int)
    for response in responses:
        for type_name, count in response.by_type.items():
            by_type[type_name] += count

    return SearchOutput(
        hits=hits[:limit],
        total=sum(response.all for response in responses),
        by_type=dict(by_type),
        branch_scope='current-branch' if branch_scope == 'current' else 'all-branches',
    )


@tool_errors()
async def search(
    ctx: Context,
    patterns: Annotated[
        list[str],
        Field(
            description='One or more search patterns. For textual search they match item names (server-side, '
            'tokenized full-text); for config-based search they match the configuration JSON content. '
            'Case-insensitive by default. Examples: ["customer"], ["sales", "revenue"], ["my_bucket"]. '
            'Do not use empty strings or empty lists.'
        ),
    ],
    item_types: Annotated[
        Sequence[SearchItemType],
        Field(
            description='Filter for specific Keboola item types. '
            'Common values: "table" (data tables), "bucket" (table containers), "transformation" '
            '(SQL/Python transformations), "component" (extractor/writer/application components), '
            '"data-app" (data apps), "flow" (orchestration flows). '
            "Use when you know what type of item you're looking for or leave empty to search all types."
        ),
    ] = tuple(),
    search_type: Annotated[
        SearchType,
        Field(
            description='Search mode: "textual" (name/id/description) or "config-based" (stringified configuration '
            'payloads). (default: "textual")'
        ),
    ] = 'textual',
    scopes: Annotated[
        Sequence[str],
        Field(
            description='JSONPath expressions to narrow config-based search to specific parts of the configuration. '
            'Simple dot-notation (e.g. "parameters", "storage.input") and full JSONPath (e.g. "$.tasks[*]") are both '
            'supported (e.g. "parameters.host", "storage.input[0].source"). '
            'Leave empty to search the whole configuration.'
        ),
    ] = tuple(),
    mode: Annotated[
        SearchPatternMode,
        Field(
            description='How to interpret patterns. Applies to config-based search only: "regex" for regular '
            'expressions or "literal" for exact text (default: "literal"). Ignored by textual search, which is '
            'always a tokenized full-text name query (not typo-corrected) and rejects "regex".'
        ),
    ] = 'literal',
    limit: Annotated[
        int,
        Field(
            description=f'Maximum number of items to return (default: {DEFAULT_GLOBAL_SEARCH_LIMIT}, max: '
            f'{MAX_GLOBAL_SEARCH_LIMIT}).'
        ),
    ] = DEFAULT_GLOBAL_SEARCH_LIMIT,
    offset: Annotated[int, Field(description='Number of matching items to skip for pagination (default: 0).')] = 0,
) -> SearchOutput:
    """
    Searches for Keboola items (tables, buckets, components, configurations, transformations, flows, data-apps, etc.)
    in the current project and returns matching ID + metadata.

    This tool supports two complementary search types:

    1) textual
    - Searches items by name, server-side (fast, independent of project size).
    - Tokenized full-text name matching, case- and diacritics-insensitive. Pass the plain name; do NOT build
      regex (rejected). It is NOT typo-corrected — misspellings may not match.
    - Prefers the current branch context; when nothing is found there, automatically widens the search to all
      branches of the project — such hits carry `branch_id`/`branch_name` so you can tell where they live.

    2) config-based
    - Searches item configurations (JSON objects) by matching patterns against the configuration values ​​converted
      to a string, optionally narrowed by JSON path `scopes`.
    - Returns also `match_scopes` with JSON paths and matched patterns per scope.

    THIS IS THE PRIMARY DISCOVERY TOOL. Always use it BEFORE any get_* tool when you need to find items
    by name or specific configuration content. Do NOT enumerate items with get_buckets, get_tables, get_configs,
    get_flows, or get_data_apps just to locate a specific item — use this tool instead.

    WHEN TO USE:
    - User asks to "find", "locate", or "search for" something by name, keyword, text pattern, configuration content or
    value
    - User mentions a partial name and you need to find the full item (e.g., "find the customer table")
    - User asks "what tables/configs/flows do I have with X in the name?"
    - You need to discover items before performing operations on them
    - User asks to "list all items with [name] or [configuration value/part] in it"
    - User asks where a value, table, component, specific configuration ID, or specific settings is used in components,
    data-apps, flows, or transformations
    - You need to trace lineage by searching for IDs referenced in configurations, or to find flows using a
      specific component, or find usage of a bucket/table in transformations or components, or to find items with
      specific parameters.
    - User asks to "what is the genesis of this item?" or "explain me business logic of this item?"

    HOW IT WORKS:
    - Supports two types:
      - search_type="textual": tokenized full-text name search, server-side. Names only — descriptions, column
        names, IDs and configuration contents are NOT searched (use config-based search for configuration contents,
        or get_tables for columns). Matching is case- and diacritics-insensitive but NOT typo-corrected.
      - search_type="config-based": matches inside configuration JSON objects, optionally narrowed by JSON path `scopes`
    - case-insensitive search
    - mode for pattern search: applies to config-based only — `literal` (default) or `regex`. Textual search ignores
      `mode` (always full-text) and rejects `regex`.
    - Multiple patterns work as OR condition - matches items containing ANY of the patterns
    - Each result includes the item's ID, name, creation date, and relevant metadata; the response also carries
      `total` and `by_type` counts and the `branch_scope` the hits come from
    - textual search prefers the current branch; on zero hits it automatically retries across all branches of the
      project and marks the response with branch_scope="all-branches"
    - scopes (config-based) narrow matching to specific JSONPath areas within configurations; matching is performed
      against the stringified JSON node content in those areas.
    - config-based always returns all matched paths per item in `match_scopes` (including matched patterns)

    IMPORTANT:
    - Always use this tool when the user mentions a name but you don't have the exact ID
    - The search returns IDs that you can use with other tools (e.g., get_tables, get_configs, get_flows)
    - Results are ordered by the `updated` field, most recent first. `updated` is the item's last update time
      when available, or its creation time otherwise (textual/global-search hits expose only the creation time).
    - Textual search matches names only, with tokenized full-text matching (case/diacritics-insensitive; not
      typo-corrected; no regex). It may not return every item the legacy enumeration did. To find items by
      description or by table column, use get_tables; to find items by configuration content, use config-based search.
    - For exact ID lookups, use specific tools like get_tables, get_configs, get_flows instead
    - Use specific `scopes` only when you know the config structure (schema or real example); otherwise run config-based
      search without scopes.
    - Use find_component_id and get_configs tools to find configurations related to a specific component
    - If results are too numerous or empty, ask the user to refine their query rather than enumerating all items.

    USAGE EXAMPLES:
    1) textual search examples:
    - user_input: "Find all tables with 'customer' in the name"
        → patterns=["customer"], item_types=["table"]
        → Returns all tables whose name matches "customer"

    - user_input: "Search for the sales transformation"
        → patterns=["sales"], item_types=["transformation"]
        → Returns transformations with "sales" in the name

    - user_input: "Find items named 'daily report' or 'weekly summary'"
        → patterns=["daily report", "weekly summary"], item_types=[]
        → Returns all items matching any of these patterns

    - user_input: "Show me all configurations related to Google Analytics"
        → patterns=["google analytics"], item_types=["configuration"]
        → Returns configurations with matching names

    2) config-based search examples:
    - user_input: "Find transformations/configs/components referencing table in.c-prod.customers"
        -> patterns=["in.c-prod.customers"], item_types=["transformation", "configuration"],
        search_type="config-based"
        -> No scopes = search whole stringified config; result includes `match_scopes` with exact paths + patterns

    - user_input: "Find configurations/transformations (etc.) using specific setting / id anywhere"
        -> patterns=["setting", "id"], item_types=["configuration", "transformations"], search_type="config-based",

    - user_input: "Find configurations/transformations (etc.) using specific setting / id in parameters"
    -> patterns=["setting", "id"], item_types=["configuration", "transformations"], search_type="config-based",
    scopes=["parameters"]

    - user_input: "Find configurations/transformations (etc.) using specific setting / id in storage"
    -> patterns=["setting", "id"], item_types=["configuration", "transformations"], search_type="config-based",
    scopes=["storage"]

    - user_input: "Find configurations/transformations (etc.) using specific setting / id in authorization"
        -> patterns=["setting", "id"], item_types=["configuration", "transformations"], search_type="config-based",
        scopes=["parameters.authorization", "authorization"]

    - user_input: "Find components/transformations using my_bucket in input or output mappings"
        -> patterns=["my_bucket"], item_types=["configuration", "transformation"], search_type="config-based",
        scopes=["storage.input", "storage.output"]
        -> Returns matches with paths like `storage.input.tables[0].source`, `storage.input.files[0].source`,
        or `storage.output.tables[0].destination`

    - user_input: "Find flows using configuration ID 01k9cz233cvd1rga3zzx40g8qj"
        -> patterns=["01k9cz233cvd1rga3zzx40g8qj"], item_types=["flow"], search_type="config-based",
        scopes=["tasks", "phases"]

    - user_input: "Find transformations using this table / column / specific code in its script"
        -> patterns=["element"], item_types=["transformation"], search_type="config-based",
        scopes=["parameters", "storage"]

    - user_input: "Find data apps using something in its config / python code / setting"
        -> patterns=["something"], item_types=["data-app"], search_type="config-based"
        -> Returns data apps where script/config sections contain the keyword and includes `match_scopes`
    """

    spec = SearchSpec(
        patterns=patterns,
        item_types=item_types,
        pattern_mode=mode,
        search_type=search_type,
        search_scopes=scopes,
        return_all_matched_patterns=(search_type == 'config-based'),
    )

    offset = max(0, offset)
    if not 0 < limit <= MAX_GLOBAL_SEARCH_LIMIT:
        LOG.warning(
            f'The "limit" parameter is out of range (0, {MAX_GLOBAL_SEARCH_LIMIT}], setting to default value '
            f'{DEFAULT_GLOBAL_SEARCH_LIMIT}.'
        )
        limit = DEFAULT_GLOBAL_SEARCH_LIMIT

    client = KeboolaClient.from_state(ctx.session.state)

    if search_type == 'textual' and await client.storage_client.is_enabled(GLOBAL_SEARCH_FEATURE):
        if mode == 'regex':
            raise ToolError(
                'Regex patterns are not supported for textual search — it is a tokenized full-text name search. '
                'Pass the plain name as the pattern, or use search_type="config-based" for regex matching inside '
                'configurations.'
            )
        # The global-search feature flag does not guarantee the project's index is populated (the bulk
        # backfill is asynchronous) and the endpoint can fail transiently, so global search is a fast path
        # with a safety net: fall back to client-side enumeration on any error, or when it finds nothing.
        try:
            output = await _global_textual_search(client, spec, limit=limit, offset=offset)
        except Exception:
            LOG.warning('Global search failed; falling back to client-side enumeration.', exc_info=True)
            output = await _enumeration_search(client, spec, limit=limit, offset=offset)
        else:
            if not output.hits and offset == 0:
                LOG.info('Global search returned no hits; falling back to client-side enumeration.')
                output = await _enumeration_search(client, spec, limit=limit, offset=offset)
    else:
        # Projects without the global-search feature use the legacy client-side enumeration;
        # config-based search has no server-side equivalent and always runs client-side.
        output = await _enumeration_search(client, spec, limit=limit, offset=offset)

    # Get links for the hits
    links_manager = await ProjectLinksManager.from_client(client)
    for hit in output.hits:
        hit.links.extend(
            links_manager.get_links(
                bucket_id=hit.bucket_id,
                table_id=hit.table_id,
                component_id=hit.component_id,
                configuration_id=hit.configuration_id,
                name=hit.name,
            )
        )

    return output


async def _enumeration_search(client: KeboolaClient, spec: SearchSpec, limit: int, offset: int) -> SearchOutput:
    """
    Searches by enumerating the project's items client-side. Used for config-based search (which has no
    server-side equivalent) and as the legacy fallback for textual search in projects without the
    global-search feature.
    """
    # Determine which types to fetch
    types_to_fetch = set(spec.item_types) if spec.item_types else set()

    # Fetch items concurrently based on requested types
    tasks = []
    all_hits: list[SearchHit] = []

    if not types_to_fetch or 'bucket' in types_to_fetch:
        tasks.append(_fetch_buckets(client, spec))

    if not types_to_fetch or 'table' in types_to_fetch:
        tasks.append(_fetch_tables(client, spec))

    if not types_to_fetch:
        tasks.append(fetch_configurations(client, spec))
    elif types_to_fetch & {
        'configuration',
        'transformation',
        'flow',
        'configuration-row',
        'workspace',
        'data-app',
    }:
        tasks.append(fetch_configurations(client, spec))

    # Gather all results
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    for result in results:
        if isinstance(result, Exception):
            # TODO: report this somehow to the AI assistant
            LOG.warning(f'Error fetching items: {result}')
            continue
        else:
            all_hits.extend(result)

    # The configuration endpoint returns every config type at once, so narrow to the requested types to match
    # the global-search path (e.g. item_types=['configuration-row'] must not leak 'configuration' hits).
    if types_to_fetch:
        all_hits = [hit for hit in all_hits if hit.item_type in types_to_fetch]

    # TODO: Should we sort by the item type too?
    all_hits.sort(
        key=lambda x: (
            x.updated,
            x.bucket_id or x.table_id or x.component_id or x.configuration_id or x.configuration_row_id,
        ),
        reverse=True,
    )

    by_type: dict[str, int] = defaultdict(int)
    for hit in all_hits:
        by_type[hit.item_type] += 1

    return SearchOutput(
        hits=all_hits[offset : offset + limit],
        total=len(all_hits),
        by_type=dict(by_type),
        branch_scope='current-branch',
    )


class SuggestedComponentOutput(BaseModel):
    """Output of find_component_id tool."""

    component_id: str = Field(description='The component ID.')
    score: float = Field(description='Score of the component suggestion.')
    links: list[Link] = Field(description='Links to the component.', default_factory=list)


@tool_errors()
async def find_component_id(
    ctx: Context,
    query: Annotated[str, Field(description='Natural language query to find the requested component.')],
) -> list[SuggestedComponentOutput]:
    """
    Returns list of component IDs that match the given query.

    WHEN TO USE:
    - Use when you want to find the component for a specific purpose.

    USAGE EXAMPLES:
    - user_input: "I am looking for a salesforce extractor component"
      → Returns a list of component IDs that match the query, ordered by relevance/best match.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    suggestion_response = await client.ai_service_client.suggest_component(query)

    components = []
    for component in suggestion_response.components:
        links = [links_manager.get_config_dashboard_link(component_id=component.component_id, component_name=None)]
        components.append(
            SuggestedComponentOutput(component_id=component.component_id, score=component.score, links=links)
        )
    return components
