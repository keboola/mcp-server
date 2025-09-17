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
    name_prefixes: Annotated[list[str], Field(description='Name prefixes to match against item names.')],
    item_types: Annotated[
        Sequence[ItemType], Field(description='Optional list of keboola item types to filter by.')
    ] = tuple(),
    limit: Annotated[
        int,
        Field(
            description=f'Maximum number of items to return (default: {DEFAULT_GLOBAL_SEARCH_LIMIT}, max: '
            f'{MAX_GLOBAL_SEARCH_LIMIT}).'
        ),
    ] = DEFAULT_GLOBAL_SEARCH_LIMIT,
    offset: Annotated[int, Field(description='Number of matching items to skip, pagination.')] = 0,
) -> GlobalSearchOutput:
    """
    Searches for Keboola items in the production branch of the current project whose names match the given prefixes,
    potentially narrowed down by item type, limited and paginated. Results are ordered by relevance, then creation time.

    Considerations:
    - The search is purely name-based, and an item is returned when its name or any word in the name starts with any
      of the "name_prefixes" parameter.
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
