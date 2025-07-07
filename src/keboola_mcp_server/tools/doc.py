import logging
from typing import Annotated, Sequence

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from pydantic import BaseModel, Field

from keboola_mcp_server.client import GlobalSearchTypes, KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import with_session_state

LOG = logging.getLogger(__name__)


def add_doc_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    doc_tools = [
        docs_query,
    ]
    for tool in doc_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(FunctionTool.from_function(tool))

    LOG.info('Doc tools initialized.')


class DocsAnswer(BaseModel):
    """An answer to a documentation query."""

    text: str = Field(description='Text of the answer to a documentation query.')
    source_urls: list[str] = Field(description='List of URLs to the sources of the answer.')


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
) -> Annotated[str, Field(description='The search results.')]:
    """
    Performs a name-based search for Keboola entities within the current project and only in the production branch.
    It supports filtering by entity type and returns results ordered by relevance and creation time.
    """

    client = KeboolaClient.from_state(ctx.session.state)

    if not await client.storage_client.is_enabled('global-search'):
        raise ValueError('Global search is not enabled in the project. Please enable it in the project settings.')

    ret = await client.storage_client.global_search(query=query, types=types, limit=limit, offset=offset)
    return ret.model_dump_json()
