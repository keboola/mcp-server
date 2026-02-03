import asyncio
import json
import logging
import re
from typing import Annotated, Any, AsyncGenerator, Literal, Mapping, Sequence

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field, PrivateAttr, model_validator

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.clients.storage import ItemType
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import toon_serializer_compact
from keboola_mcp_server.tools.components.utils import get_nested

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
    'configuration',
    'configuration-row',
    'component',
    'workspace',
    'shared-code',
    'rows',
    'state',
]

SearchComponentItemType = Literal[
    'flow',
    'transformation',
    'configuration',
    'configuration-row',
    'workspace',
]

ITEM_TYPE_TO_COMPONENT_TYPES: Mapping[ItemType, Sequence[str]] = {
    'flow': ['other'],
    'transformation': ['transformation'],
    'configuration': ['extractor', 'writer'],
    'configuration-row': ['extractor', 'writer'],
    'workspace': ['other'],
}

SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES: Mapping[SearchItemType, Sequence[str]] = {
    'data-app': ['other'],
    'flow': ['other'],
    'transformation': ['transformation'],
    'component': ['extractor', 'writer', 'application'],
    'configuration': ['extractor', 'writer'],
    'configuration-row': ['extractor', 'writer'],
    'workspace': ['other'],
}

SearchType = Literal['textual', 'config-based']
SearchPatternMode = Literal['regex', 'literal']


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

    item_type: ItemType = Field(description='The type of the item (e.g. table, bucket, configuration, etc.).')
    updated: str = Field(description='The date and time the item was created in ISO 8601 format.')

    name: str | None = Field(default=None, description='Name of the item.')
    display_name: str | None = Field(default=None, description='Display name of the item.')
    description: str | None = Field(default=None, description='Description of the item.')
    links: list[Link] = Field(default_factory=list, description='Links to the item.')
    _matches: list[PatternMatch] = PrivateAttr(default_factory=list)

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

    def with_matches(self, matches: list['PatternMatch']) -> 'SearchHit':
        """Assign pattern matches to this search hit and return self for chaining."""
        self._matches = matches
        return self


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

    @staticmethod
    def _stringify(value: JsonDict) -> str:
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

    def match_configuration_scopes(self, configuration: JsonDict | None) -> list[PatternMatch]:
        """
        Checks configuration fields within specified scopes for pattern matches.

        :param configuration: The configuration to match against the patterns.
        :return: A tuple of scopes and patterns that matched the configuration; empty patterns if no matches.
        """
        if self.search_scopes:
            matches: list[PatternMatch] = []
            for scope in self.search_scopes:
                if matched := self.match_patterns(get_nested(configuration, scope, default=None)):
                    matches.append(PatternMatch(scope=scope, patterns=matched))
                    if not self.return_all_matched_patterns:
                        break
            return matches

        if matched := self.match_patterns(configuration):
            return [PatternMatch(scope=None, patterns=matched)]
        return []

    def match_texts(self, texts: Sequence[str]) -> list[PatternMatch]:
        matches: list[PatternMatch] = []
        for text in texts:
            if matched := self.match_patterns(text):
                matches.append(PatternMatch(scope=None, patterns=matched))
                if not self.return_all_matched_patterns:
                    break
        return matches


def _get_field_value(item: JsonDict, fields: Sequence[str]) -> Any | None:
    for field in fields:
        if value := get_nested(item, field):
            return value
    return None


def _check_column_match(table: JsonDict, spec: SearchSpec) -> bool:
    """Check if any column name or description matches the patterns."""
    # Check column names (list of strings)
    for col_name in table.get('columns', []):
        if spec.match_patterns(col_name):
            return True

    # Check column descriptions (from columnMetadata)
    column_metadata = table.get('columnMetadata', {})
    for col_meta in column_metadata.values():
        col_description = get_metadata_property(col_meta, MetadataField.DESCRIPTION)
        if spec.match_patterns(col_description):
            return True

    return False


async def _fetch_buckets(client: KeboolaClient, spec: SearchSpec) -> list[SearchHit]:
    """Fetches and filters buckets."""
    hits = []
    for bucket in await client.storage_client.bucket_list():
        if not (bucket_id := bucket.get('id')):
            continue

        bucket_name = bucket.get('name')
        bucket_display_name = bucket.get('displayName')
        bucket_description = get_metadata_property(bucket.get('metadata', []), MetadataField.DESCRIPTION)

        if (
            spec.match_patterns(bucket_id)
            or spec.match_patterns(bucket_name)
            or spec.match_patterns(bucket_display_name)
            or spec.match_patterns(bucket_description)
        ):
            hits.append(
                SearchHit(
                    bucket_id=bucket_id,
                    item_type='bucket',
                    updated=_get_field_value(bucket, ['lastChangeDate', 'updated', 'created']) or '',
                    name=bucket_name,
                    display_name=bucket_display_name,
                    description=bucket_description,
                )
            )
    return hits


async def _fetch_tables(client: KeboolaClient, spec: SearchSpec) -> list[SearchHit]:
    """Fetches and filters tables from all buckets."""
    hits = []
    for bucket in await client.storage_client.bucket_list():
        if not (bucket_id := bucket.get('id')):
            continue

        tables = await client.storage_client.bucket_table_list(bucket_id, include=['columns', 'columnMetadata'])
        for table in tables:
            if not (table_id := table.get('id')):
                continue

            table_name = table.get('name')
            table_display_name = table.get('displayName')
            table_description = get_metadata_property(table.get('metadata', []), MetadataField.DESCRIPTION)

            if (
                spec.match_patterns(table_id)
                or spec.match_patterns(table_name)
                or spec.match_patterns(table_display_name)
                or spec.match_patterns(table_description)
                or _check_column_match(table, spec)
            ):
                hits.append(
                    SearchHit(
                        table_id=table_id,
                        item_type='table',
                        updated=_get_field_value(table, ['lastChangeDate', 'created']) or '',
                        name=table_name,
                        display_name=table_display_name,
                        description=table_description,
                    )
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
    for component in components:
        if not (component_id := component.get('id')):
            continue

        current_component_type = component.get('type')
        if component_id in [ORCHESTRATOR_COMPONENT_ID, CONDITIONAL_FLOW_COMPONENT_ID]:
            item_type = 'flow'
        elif current_component_type == 'transformation':
            item_type = 'transformation'
        elif component_id == 'keboola.sandboxes':
            item_type = 'workspace'
        else:
            item_type = 'configuration'

        for config in component.get('configurations', []):
            if not (config_id := config.get('id')):
                continue

            config_name = config.get('name')
            config_description = config.get('description')
            config_updated = _get_field_value(config, ['currentVersion.created', 'created']) or ''

            if spec.search_type == 'textual':
                if (
                    spec.match_patterns(config_id)
                    or spec.match_patterns(config_name)
                    or spec.match_patterns(config_description)
                ):
                    yield SearchHit(
                        component_id=component_id,
                        configuration_id=config_id,
                        item_type=item_type,
                        updated=config_updated,
                        name=config_name,
                        description=config_description,
                    )
            elif spec.search_type == 'config-based':
                if matches := spec.match_configuration_scopes(config.get('configuration')):
                    yield SearchHit(
                        component_id=component_id,
                        configuration_id=config_id,
                        item_type=item_type,
                        updated=config_updated,
                        name=config_name,
                        description=config_description,
                    ).with_matches(matches)

            for row in config.get('rows', []):
                if not (row_id := row.get('id')):
                    continue

                row_name = row.get('name')
                row_description = row.get('description')

                if spec.search_type == 'textual':
                    if (
                        spec.match_patterns(row_id)
                        or spec.match_patterns(row_name)
                        or spec.match_patterns(row_description)
                    ):
                        yield SearchHit(
                            component_id=component_id,
                            configuration_id=config_id,
                            configuration_row_id=row_id,
                            item_type='configuration-row',
                            updated=config_updated or _get_field_value(row, ['created']),
                            name=row_name,
                            description=row_description,
                        )

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
                        ).with_matches(matches)


@tool_errors()
async def search(
    ctx: Context,
    patterns: Annotated[
        list[str],
        Field(
            description='One or more search patterns to match against item ID, name, display name, or description. '
            'Supports regex patterns. Case-insensitive. Examples: ["customer"], ["sales", "revenue"], '
            '["test.*table"]. Do not use empty strings or empty lists.'
        ),
    ],
    item_types: Annotated[
        Sequence[ItemType],
        Field(
            description='Optional filter for specific Keboola item types. Leave empty to search all types. '
            'Common values: "table" (data tables), "bucket" (table containers), "transformation" '
            '(SQL/Python transformations), "configuration" (extractor/writer configs), "flow" (orchestration flows). '
            "Use when you know what type of item you're looking for."
        ),
    ] = tuple(),
    limit: Annotated[
        int,
        Field(
            description=f'Maximum number of items to return (default: {DEFAULT_GLOBAL_SEARCH_LIMIT}, max: '
            f'{MAX_GLOBAL_SEARCH_LIMIT}).'
        ),
    ] = DEFAULT_GLOBAL_SEARCH_LIMIT,
    offset: Annotated[int, Field(description='Number of matching items to skip for pagination (default: 0).')] = 0,
) -> list[SearchHit]:
    """
    Searches for Keboola items (tables, buckets, configurations, transformations, flows, etc.) in the current project
    by matching patterns against item ID, name, display name, or description. Returns matching items grouped by type
    with their IDs and metadata.

    WHEN TO USE:
    - User asks to "find", "locate", or "search for" something by name
    - User mentions a partial name and you need to find the full item (e.g., "find the customer table")
    - User asks "what tables/configs/flows do I have with X in the name?"
    - You need to discover items before performing operations on them
    - User asks to "list all items with [name] in it"
    - DO NOT use for listing all items of a specific type. Use get_configs, list_tables, get_flows, etc instead.

    HOW IT WORKS:
    - Searches by regex pattern matching against id, name, displayName, and description fields
    - For tables, also searches column names and column descriptions
    - Case-insensitive search
    - Multiple patterns work as OR condition - matches items containing ANY of the patterns
    - Returns grouped results by item type (tables, buckets, configurations, flows, etc.)
    - Each result includes the item's ID, name, creation date, and relevant metadata

    IMPORTANT:
    - Always use this tool when the user mentions a name but you don't have the exact ID
    - The search returns IDs that you can use with other tools (e.g., get_table, get_configs, get_flows)
    - Results are ordered by update time. The most recently updated items are returned first.
    - For exact ID lookups, use specific tools like get_table, get_configs, get_flows instead
    - Use find_component_id and get_configs tools to find configurations related to a specific component

    USAGE EXAMPLES:
    - user_input: "Find all tables with 'customer' in the name"
      → patterns=["customer"], item_types=["table"]
      → Returns all tables whose id, name, displayName, or description contains "customer"

    - user_input: "Find tables with 'email' column"
      → patterns=["email"], item_types=["table"]
      → Returns all tables that have a column named "email" or with "email" in column description

    - user_input: "Search for the sales transformation"
      → patterns=["sales"], item_types=["transformation"]
      → Returns transformations with "sales" in any searchable field

    - user_input: "Find items named 'daily report' or 'weekly summary'"
      → patterns=["daily.*report", "weekly.*summary"], item_types=[]
      → Returns all items matching any of these patterns

    - user_input: "Show me all configurations related to Google Analytics"
      → patterns=["google.*analytics"], item_types=["configuration"]
      → Returns configurations with matching patterns
    """

    spec = SearchSpec(
        patterns=patterns,
        item_types=item_types,
        search_type='textual',
    )

    offset = max(0, offset)
    if not 0 < limit <= MAX_GLOBAL_SEARCH_LIMIT:
        LOG.warning(
            f'The "limit" parameter is out of range (0, {MAX_GLOBAL_SEARCH_LIMIT}], setting to default value '
            f'{DEFAULT_GLOBAL_SEARCH_LIMIT}.'
        )
        limit = DEFAULT_GLOBAL_SEARCH_LIMIT

    # Determine which types to fetch
    types_to_fetch = set(spec.item_types) if spec.item_types else set()

    # Fetch items concurrently based on requested types
    tasks = []
    all_hits: list[SearchHit] = []
    client = KeboolaClient.from_state(ctx.session.state)

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

    # Filter by item_types if specified
    if types_to_fetch:
        all_hits = [item for item in all_hits if item.item_type in types_to_fetch]

    # TODO: Should we sort by the item type too?
    all_hits.sort(
        key=lambda x: (
            x.updated,
            x.bucket_id or x.table_id or x.component_id or x.configuration_id or x.configuration_row_id,
        ),
        reverse=True,
    )
    paginated_hits = all_hits[offset : offset + limit]

    # Get links for the hits
    links_manager = await ProjectLinksManager.from_client(client)
    for hit in paginated_hits:
        hit.links.extend(
            links_manager.get_links(
                bucket_id=hit.bucket_id,
                table_id=hit.table_id,
                component_id=hit.component_id,
                configuration_id=hit.configuration_id,
                name=hit.name,
            )
        )

    # TODO: Should we report the total number of hits?
    return paginated_hits


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
