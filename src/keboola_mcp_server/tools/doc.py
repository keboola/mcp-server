import logging
from collections import defaultdict
from datetime import datetime
from typing import Annotated, Any, Sequence

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from pydantic import BaseModel, Field

from keboola_mcp_server.client import GlobalSearchResponse, GlobalSearchTypes, KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import with_session_state

LOG = logging.getLogger(__name__)

MAX_GLOBAL_SEARCH_LIMIT = 100
DEFAULT_GLOBAL_SEARCH_LIMIT = 50


def add_doc_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    doc_tools = [
        docs_query,
        global_search,
    ]
    for tool in doc_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(FunctionTool.from_function(tool))

    LOG.info('Doc tools initialized.')


class DocsAnswer(BaseModel):
    """An answer to a documentation query."""

    text: str = Field(description='Text of the answer to a documentation query.')
    source_urls: list[str] = Field(description='List of URLs to the sources of the answer.')


class GlobalSearchGroupItems(BaseModel):
    """Group of items of the same type found in the global search."""

    class GroupTypeItem(BaseModel):
        """An item corresponding to its group type found in the global search."""

        name: str = Field(description='The name of the item.')
        id: str = Field(description='The id of the item.')
        created: datetime = Field(description='The date and time the entity was created.')
        additional_info: dict[str, Any] = Field(description='Additional information about the item.')

        @classmethod
        def from_api_response(cls, item: GlobalSearchResponse.Item) -> 'GlobalSearchGroupItems.GroupTypeItem':
            """Creates an Item from the item API response."""
            add_info = {}
            if item.type == 'table':
                bucket_info = item.full_path.get('bucket', {})
                add_info['bucket_id'] = bucket_info.get('id', 'unknown')
                add_info['bucket_name'] = bucket_info.get('name', 'unknown')
            elif item.type in ['configuration', 'configuration-row', 'transformation', 'flow']:
                component_info = item.full_path.get('component', {})
                add_info['component_id'] = component_info.get('id', 'unknown')
                add_info['component_name'] = component_info.get('name', 'unknown')
                if item.type == 'configuration-row':
                    # as row_config is identified by root_config id and component id.
                    configuration_info = item.full_path.get('configuration', {})
                    add_info['configuration_id'] = configuration_info.get('id', 'unknown')
                    add_info['configuration_name'] = configuration_info.get('name', 'unknown')
            return cls(name=item.name, id=item.id, created=item.created, additional_info=add_info)

    group_type: GlobalSearchTypes = Field(description='The type of the items in the group.')
    group_count: int = Field(description='Number of items in the group.')
    group_items: list[GroupTypeItem] = Field(
        description=('List of items for the type found in the global search, sorted by relevance and creation time.')
    )

    @classmethod
    def from_api_response(
        cls, group_type: GlobalSearchTypes, group_items: list[GlobalSearchResponse.Item]
    ) -> 'GlobalSearchGroupItems':
        """Creates a GlobalSearchItemsGroupedByType from the API response items and a type."""
        # filter the items by the given type to be sure
        group_items = [item for item in group_items if item.type == group_type]
        return cls(
            group_type=group_type,
            group_count=len(group_items),
            group_items=[GlobalSearchGroupItems.GroupTypeItem.from_api_response(item) for item in group_items],
        )


class GlobalSearchAnswer(BaseModel):
    """An answer to a global search query for multiple name substrings."""

    counts: dict[str, int] = Field(description='Number of items found for each type.')
    type_groups: list[GlobalSearchGroupItems] = Field(
        description='List of results grouped by type.'
    )

    @classmethod
    def from_api_responses(cls, response: GlobalSearchResponse) -> 'GlobalSearchAnswer':
        """Creates a GlobalSearchAnswer from the API responses."""
        items_by_type = defaultdict(list)
        for item in response.items:
            items_by_type[item.type].append(item)
        return cls(
            counts=response.by_type, # contains counts for "total", and for each found type.
            type_groups=[
                GlobalSearchGroupItems.from_api_response(group_type=type, group_items=items)
                for type, items in sorted(items_by_type.items(), key=lambda x: x[0])
            ],
        )


@tool_errors()
@with_session_state()
async def docs_query(
    ctx: Context,
    query: Annotated[str, Field(description='Natural language query to search for in the documentation.')],
) -> Annotated[DocsAnswer, Field(description='The retrieved documentation.')]:
    """
    Answers a question using the Keboola documentation as a source.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    answer = await client.ai_service_client.docs_question(query)

    return DocsAnswer(text=answer.text, source_urls=answer.source_urls)


@tool_errors()
@with_session_state()
async def global_search(
    ctx: Context,
    name_prefixes: Annotated[list[str], Field(description='Name prefixes to look for inside entity name.')],
    entity_types: Annotated[
        Sequence[GlobalSearchTypes], Field(description='Optional list of keboola object types to search for.')
    ] = tuple(),
    limit: Annotated[
        int,
        Field(
            description=f'Maximum number of items to return (default: {DEFAULT_GLOBAL_SEARCH_LIMIT}, max: '
            f'{MAX_GLOBAL_SEARCH_LIMIT}).'
        ),
    ] = DEFAULT_GLOBAL_SEARCH_LIMIT,
    offset: Annotated[int, Field(description='How many matching items to skip, pagination.')] = 0,
) -> Annotated[GlobalSearchAnswer, Field(description='Search results ordered by relevance, then creation time.')]:
    """
    Searches for Keboola entities by name substrings in the production branch of the current project, potentially
    narrowed down by entity type, limited and paginated. Results are ordered by relevance, then creation time.

    Considerations:
    - The search is purely name-based, and an entity is returned when its name contains "name_substring".
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
        query=joined_prefixes, types=entity_types, limit=limit, offset=offset
    )
    return GlobalSearchAnswer.from_api_responses(response)
