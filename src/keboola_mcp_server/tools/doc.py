from datetime import datetime
import logging
from typing import Annotated, Any, Sequence

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from pydantic import BaseModel, Field

from keboola_mcp_server.client import GlobalSearchResponse, GlobalSearchTypes, KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import with_session_state

LOG = logging.getLogger(__name__)


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


class GlobalSearchAnswer(BaseModel):
    """An answer to a global search query."""

    class Item(BaseModel):
        """An item found in the global search."""
        name: str = Field(description='The name of the item.')
        id: str = Field(description='The id of the item.')
        type: GlobalSearchTypes = Field(description='The type of the item.')
        created: datetime = Field(description='The date and time the entity was created.')
        additional_info: dict[str, Any] = Field(description='Additional information about the item.')

        @classmethod
        def from_api_response(cls, item: GlobalSearchResponse.Item) -> 'GlobalSearchAnswer.Item':
            """Creates an Item from the API response."""
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
            return cls(name=item.name, type=item.type, id=item.id, created=item.created, additional_info=add_info)

    total_count: int = Field(description='Total number of items found in the global search.')
    counts_by_type: dict[str, int] = Field(description='Number of items found by type.')
    items: list[Item] = Field(description=(
        'List of items found in the global search, sorted by relevance and creation time.'
    ))

    @classmethod
    def from_api_response(cls, response: GlobalSearchResponse) -> 'GlobalSearchAnswer':
        """Creates a GlobalSearchAnswer from the API response."""
        return cls(
            items=[GlobalSearchAnswer.Item.from_api_response(item) for item in response.items],
            total_count=response.all,
            counts_by_type=response.by_type,
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
    query: Annotated[str, Field(description='name-based search query')],
    types: Annotated[Sequence[GlobalSearchTypes], Field(description='Which types of objects to search for.')] = tuple(),
    limit: Annotated[int, Field(description='The maximum number of items to return.')] = 100,
    offset: Annotated[int, Field(description='The offset to start from.')] = 0,
) -> Annotated[GlobalSearchAnswer, Field(description='The search results.')]:
    """
    Performs a name-based search for Keboola entities within the current project and only in the production branch.
    It supports filtering by entity type and returns results ordered by relevance and creation time.
    """

    client = KeboolaClient.from_state(ctx.session.state)

    if not await client.storage_client.is_enabled('global-search'):
        raise ValueError('Global search is not enabled in the project. Please enable it in the project settings.')

    response = await client.storage_client.global_search(query=query, types=types, limit=limit, offset=offset)
    return GlobalSearchAnswer.from_api_response(response)
