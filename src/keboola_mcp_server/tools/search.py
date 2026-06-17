import asyncio
import logging
from collections import defaultdict
from typing import Annotated, Any, AsyncGenerator, Sequence

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    DATA_APP_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import toon_serializer_compact
from keboola_mcp_server.tools.components.utils import get_nested
from keboola_mcp_server.tools.search_global import _global_textual_search
from keboola_mcp_server.tools.search_models import (
    DEFAULT_GLOBAL_SEARCH_LIMIT,
    GLOBAL_SEARCH_FEATURE,
    MAX_GLOBAL_SEARCH_LIMIT,
    WORKSPACE_COMPONENT_ID,
    PatternMatch,
    SearchComponentItemType,
    SearchHit,
    SearchItemType,
    SearchOutput,
    SearchPatternMode,
    SearchSpec,
    SearchType,
)
from keboola_mcp_server.tools.storage_helpers import merged_bucket_list, merged_bucket_table_list

LOG = logging.getLogger(__name__)

# Re-exported for backwards compatibility — models/aliases moved to search_models, the global-search
# path to search_global. Importers (server.py, generate_tool_docs, tools/storage/usage.py, tests) keep
# importing these names from `keboola_mcp_server.tools.search`.
__all__ = [
    'SEARCH_TOOL_NAME',
    'SEARCH_TOOLS_TAG',
    'PatternMatch',
    'SearchComponentItemType',
    'SearchHit',
    'SearchItemType',
    'SearchOutput',
    'SearchSpec',
    'SuggestedComponentOutput',
    'add_search_tools',
    'fetch_configurations',
    'find_component_id',
    'search',
]

SEARCH_TOOL_NAME = 'search'
SEARCH_TOOLS_TAG = 'search'


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
