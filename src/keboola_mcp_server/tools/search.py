import logging
from collections import defaultdict
from datetime import datetime
from typing import Annotated, Any, Sequence

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.ai_service import SuggestedComponent
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import GlobalSearchResponse, ItemType
from keboola_mcp_server.errors import tool_errors

LOG = logging.getLogger(__name__)

SEARCH_TOOL_NAME = 'search'
MAX_GLOBAL_SEARCH_LIMIT = 100
DEFAULT_GLOBAL_SEARCH_LIMIT = 50
SEARCH_TOOLS_TAG = 'search'


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
    # The search tool is disabled for now as the underlying search API is not working as expected.
    #
    # LOG.info(f'Adding tool {search.__name__} to the MCP server.')
    # mcp.add_tool(
    #     FunctionTool.from_function(
    #         search,
    #         name=SEARCH_TOOL_NAME,
    #         annotations=ToolAnnotations(readOnlyHint=True),
    #         tags={SEARCH_TOOLS_TAG},
    #     )
    # )

    LOG.info('Search tools initialized.')


class ItemsGroup(BaseModel):
    """Group of items of the same type found in the global search."""

    class Item(BaseModel):
        """An item corresponding to its group type found in the global search."""

        name: str = Field(description='The name of the item.')
        id: str = Field(description='The id of the item.')
        created: datetime = Field(description='The date and time the item was created.')
        additional_info: dict[str, Any] = Field(description='Additional information about the item.')

        @classmethod
        def from_api_response(cls, item: GlobalSearchResponse.Item) -> 'ItemsGroup.Item':
            """Creates an Item from the item API response."""
            add_info = {}
            if item.type == 'table':
                bucket_info = item.full_path['bucket']
                add_info['bucket_id'] = bucket_info['id']
                add_info['bucket_name'] = bucket_info['name']
            elif item.type in ['configuration', 'configuration-row', 'transformation', 'flow']:
                component_info = item.full_path['component']
                add_info['component_id'] = component_info['id']
                add_info['component_name'] = component_info['name']
                if item.type == 'configuration-row':
                    # as row_config is identified by root_config id and component id.
                    configuration_info = item.full_path['configuration']
                    add_info['configuration_id'] = configuration_info['id']
                    add_info['configuration_name'] = configuration_info['name']
            return cls.model_construct(name=item.name, id=item.id, created=item.created, additional_info=add_info)

    type: ItemType = Field(description='The type of the items in the group.')
    count: int = Field(description='Number of items in the group.')
    items: list[Item] = Field(
        description=('List of items for the type found in the global search, sorted by relevance and creation time.')
    )

    @classmethod
    def from_api_response(cls, type: ItemType, items: list[GlobalSearchResponse.Item]) -> 'ItemsGroup':
        """Creates a ItemsGroup from the API response items and a type."""
        # filter the items by the given type to be sure
        items = [item for item in items if item.type == type]
        return cls.model_construct(
            type=type,
            count=len(items),
            items=[ItemsGroup.Item.from_api_response(item) for item in items],
        )


class GlobalSearchOutput(BaseModel):
    """A result of a global search query for multiple name substrings."""

    counts: dict[str, int] = Field(description='Number of items in total and for each type.')
    groups: dict[ItemType, ItemsGroup] = Field(description='Search results.')

    @classmethod
    def from_api_responses(cls, response: GlobalSearchResponse) -> 'GlobalSearchOutput':
        """Creates a GlobalSearchOutput from the API responses."""
        items_by_type: defaultdict[ItemType, list[GlobalSearchResponse.Item]] = defaultdict(list)
        for item in response.items:
            items_by_type[item.type].append(item)
        return cls.model_construct(
            counts=response.by_type,  # contains counts for "total", and for each found type.
            groups={
                type: ItemsGroup.from_api_response(type=type, items=items) for type, items in items_by_type.items()
            },
        )


@tool_errors()
async def search(
    ctx: Context,
    name_prefixes: Annotated[
        list[str],
        Field(
            description='One or more name prefixes to search for. An item matches if its name (or any word in the '
            'name) starts with any of these prefixes. Case-insensitive. Examples: ["customer"], ["sales", "revenue"], '
            '["test"]. Do not use empty strings or empty lists.'
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
) -> GlobalSearchOutput:
    """
    Searches for Keboola items (tables, buckets, configurations, transformations, flows, etc.) in the current project
    by name. Returns matching items grouped by type with their IDs and metadata.

    WHEN TO USE:
    - User asks to "find", "locate", or "search for" something by name
    - User mentions a partial name and you need to find the full item (e.g., "find the customer table")
    - User asks "what tables/configs/flows do I have with X in the name?"
    - You need to discover items before performing operations on them
    - User asks to "list all items with [name] in it"
    - DO NOT use for listing all items of a specific type. Use list_configs, list_tables, list_flows, etc instead.

    HOW IT WORKS:
    - Searches by name prefix matching: an item matches if its name or any word in the name starts with the search term
    - Case-insensitive search
    - Returns grouped results by item type (tables, buckets, configurations, flows, etc.)
    - Each result includes the item's ID, name, creation date, and relevant metadata

    IMPORTANT:
    - Always use this tool when the user mentions a name but you don't have the exact ID
    - The search returns IDs that you can use with other tools (e.g., get_table, get_config, get_flow)
    - Results are ordered by relevance, then creation time

    USAGE EXAMPLES:
    - user_input: "Find all tables with 'customer' in the name"
      → name_prefixes=["customer"], item_types=["table"]
      → Returns all tables whose name contains a word starting with "customer"

    - user_input: "Search for the sales transformation"
      → name_prefixes=["sales"], item_types=["transformation"]
      → Returns transformations with "sales" in the name

    - user_input: "Find items named 'daily report' or 'weekly summary'"
      → name_prefixes=["daily", "report", "weekly", "summary"], item_types=[]
      → Returns all items matching any of these name parts

    - user_input: "Show me all configurations related to Google Analytics"
      → name_prefixes=["google", "analytics"], item_types=["configuration"]
      → Returns configurations with "google" or "analytics" in the name

    CONSIDERATIONS:
    - Search is purely name-based (does not search descriptions or other metadata)
    - Multiple prefixes work as OR condition - matches items containing ANY of the prefixes
    - For exact ID lookups, use specific tools like get_table, get_config, get_flow instead
    - Use find_component_id and list_configs tools to find configurations related to a specific component
    """

    client = KeboolaClient.from_state(ctx.session.state)
    # check if global search is enabled
    if not await client.storage_client.is_enabled('global-search'):
        raise ValueError('Global search is not enabled in the project. Please enable it in your project settings.')

    offset = max(0, offset)
    if not 0 < limit <= MAX_GLOBAL_SEARCH_LIMIT:
        LOG.warning(
            f'The "limit" parameter is out of range (0, {MAX_GLOBAL_SEARCH_LIMIT}], setting to default value '
            f'{DEFAULT_GLOBAL_SEARCH_LIMIT}.'
        )
        limit = DEFAULT_GLOBAL_SEARCH_LIMIT

    # Join the name prefixes to make the search more efficient as the API conducts search for each prefix split by space
    # separately.
    joined_prefixes = ' '.join(name_prefixes)
    response = await client.storage_client.global_search(
        query=joined_prefixes, types=item_types, limit=limit, offset=offset
    )
    return GlobalSearchOutput.from_api_responses(response)


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
