import asyncio
import logging
import re
from typing import Annotated, Any, AsyncGenerator, Iterable, Mapping, Sequence

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field, model_validator

from keboola_mcp_server.clients.ai_service import SuggestedComponent
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.clients.storage import ItemType
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.tools.components.utils import get_nested

LOG = logging.getLogger(__name__)

SEARCH_TOOL_NAME = 'search'
MAX_GLOBAL_SEARCH_LIMIT = 100
DEFAULT_GLOBAL_SEARCH_LIMIT = 50
SEARCH_TOOLS_TAG = 'search'

ITEM_TYPE_TO_COMPONENT_TYPES: Mapping[ItemType, Sequence[str]] = {
    'flow': ['other'],
    'transformation': ['transformation'],
    'configuration': ['extractor', 'writer'],
    'configuration-row': ['extractor', 'writer'],
    'workspace': ['other'],
}


def add_search_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    LOG.info(f'Adding tool {find_component_id.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            find_component_id,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SEARCH_TOOLS_TAG},
        )
    )

    LOG.info(f'Adding tool {search.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            search,
            name=SEARCH_TOOL_NAME,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SEARCH_TOOLS_TAG},
        )
    )

    LOG.info('Search tools initialized.')


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


def _matches_pattern(text: str | None, patterns: list[re.Pattern]) -> bool:
    """Checks if text matches any of the regex patterns."""
    return text and any(pattern.search(text) for pattern in patterns)


def _get_field_value(item: JsonDict, fields: Sequence[str]) -> Any | None:
    for field in fields:
        if value := get_nested(item, field):
            return value
    return None


async def _fetch_buckets(client: KeboolaClient, patterns: list[re.Pattern]) -> list[SearchHit]:
    """Fetches and filters buckets."""
    hits = []
    for bucket in await client.storage_client.bucket_list():
        if not (bucket_id := bucket.get('id')):
            continue

        bucket_name = bucket.get('name')
        bucket_display_name = bucket.get('displayName')
        bucket_description = get_metadata_property(bucket.get('metadata', []), MetadataField.DESCRIPTION)

        if (
            _matches_pattern(bucket_id, patterns)
            or _matches_pattern(bucket_name, patterns)
            or _matches_pattern(bucket_display_name, patterns)
            or _matches_pattern(bucket_description, patterns)
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


async def _fetch_tables(client: KeboolaClient, patterns: list[re.Pattern]) -> list[SearchHit]:
    """Fetches and filters tables from all buckets."""
    hits = []
    for bucket in await client.storage_client.bucket_list():
        if not (bucket_id := bucket.get('id')):
            continue

        tables = await client.storage_client.bucket_table_list(bucket_id)
        for table in tables:
            if not (table_id := table.get('id')):
                continue

            table_name = table.get('name')
            table_display_name = table.get('displayName')
            table_description = get_metadata_property(table.get('metadata', []), MetadataField.DESCRIPTION)

            if (
                _matches_pattern(table_id, patterns)
                or _matches_pattern(table_name, patterns)
                or _matches_pattern(table_display_name, patterns)
                or _matches_pattern(table_description, patterns)
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


async def _fetch_configurations(
    client: KeboolaClient, patterns: list[re.Pattern], item_types: Iterable[ItemType] | None = None
) -> list[SearchHit]:
    """Fetches and filters configurations and configuration rows from all component types."""
    hits = []

    component_types: set[str] = set()
    for item_type in item_types or []:
        if ctypes := ITEM_TYPE_TO_COMPONENT_TYPES.get(item_type):
            component_types.update(ctypes)

    if component_types:
        for component_type in component_types:
            async for hit in _fetch_configs(client, patterns, component_type=component_type):
                hits.append(hit)

    else:
        async for hit in _fetch_configs(client, patterns, component_type=None):
            hits.append(hit)

    return hits


async def _fetch_configs(
    client: KeboolaClient, patterns: list[re.Pattern], component_type: str | None = None
) -> AsyncGenerator[SearchHit, None]:
    components = await client.storage_client.component_list(component_type, include=['configuration', 'rows'])
    for component in components:
        if not (component_id := component.get('id')):
            continue

        item_type: ItemType
        if component_id in ['keboola.orchestrator', 'keboola.flow']:
            item_type = 'flow'
        elif component_type == 'transformation':
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

            if (
                _matches_pattern(config_id, patterns)
                or _matches_pattern(config_name, patterns)
                or _matches_pattern(config_description, patterns)
            ):
                yield SearchHit(
                    component_id=component_id,
                    configuration_id=config_id,
                    item_type=item_type,
                    updated=config_updated,
                    name=config_name,
                    description=config_description,
                )

            for row in config.get('rows', []):
                if not (row_id := row.get('id')):
                    continue

                row_name = row.get('name')
                row_description = row.get('description')

                if (
                    _matches_pattern(row_id, patterns)
                    or _matches_pattern(row_name, patterns)
                    or _matches_pattern(row_description, patterns)
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
    - DO NOT use for listing all items of a specific type. Use list_configs, list_tables, list_flows, etc instead.

    HOW IT WORKS:
    - Searches by regex pattern matching against id, name, displayName, and description fields
    - Case-insensitive search
    - Multiple patterns work as OR condition - matches items containing ANY of the patterns
    - Returns grouped results by item type (tables, buckets, configurations, flows, etc.)
    - Each result includes the item's ID, name, creation date, and relevant metadata

    IMPORTANT:
    - Always use this tool when the user mentions a name but you don't have the exact ID
    - The search returns IDs that you can use with other tools (e.g., get_table, get_config, get_flow)
    - Results are ordered by update time. The most recently updated items are returned first.
    - For exact ID lookups, use specific tools like get_table, get_config, get_flow instead
    - Use find_component_id and list_configs tools to find configurations related to a specific component

    USAGE EXAMPLES:
    - user_input: "Find all tables with 'customer' in the name"
      → patterns=["customer"], item_types=["table"]
      → Returns all tables whose id, name, displayName, or description contains "customer"

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
    patterns = list(filter(None, map(str.strip, filter(None, patterns))))
    if not patterns:
        raise ValueError('At least one search pattern must be provided.')

    offset = max(0, offset)
    if not 0 < limit <= MAX_GLOBAL_SEARCH_LIMIT:
        LOG.warning(
            f'The "limit" parameter is out of range (0, {MAX_GLOBAL_SEARCH_LIMIT}], setting to default value '
            f'{DEFAULT_GLOBAL_SEARCH_LIMIT}.'
        )
        limit = DEFAULT_GLOBAL_SEARCH_LIMIT

    # Compile regex patterns from patterns (case-insensitive)
    compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in patterns]

    # Determine which types to fetch
    types_to_fetch = set(item_types) if item_types else set()

    # Fetch items concurrently based on requested types
    tasks = []
    all_hits: list[SearchHit] = []
    client = KeboolaClient.from_state(ctx.session.state)

    if not types_to_fetch or 'bucket' in types_to_fetch:
        tasks.append(_fetch_buckets(client, compiled_patterns))

    if not types_to_fetch or 'table' in types_to_fetch:
        tasks.append(_fetch_tables(client, compiled_patterns))

    if not types_to_fetch:
        tasks.append(_fetch_configurations(client, compiled_patterns))
    elif config_types_to_fetch := types_to_fetch & {
        'configuration',
        'transformation',
        'flow',
        'configuration-row',
        'workspace',
    }:
        tasks.append(_fetch_configurations(client, compiled_patterns, item_types=config_types_to_fetch))

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

    # TODO: Should we report the total number of hits?
    return paginated_hits


@tool_errors()
async def find_component_id(
    ctx: Context,
    query: Annotated[str, Field(description='Natural language query to find the requested component.')],
) -> list[SuggestedComponent]:
    """
    Returns list of component IDs that match the given query.

    USAGE:
    - Use when you want to find the component for a specific purpose.

    EXAMPLES:
    - user_input: `I am looking for a salesforce extractor component`
        - returns a list of component IDs that match the query, ordered by relevance/best match.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    suggestion_response = await client.ai_service_client.suggest_component(query)
    return suggestion_response.components
