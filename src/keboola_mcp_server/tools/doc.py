import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
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
    query: Annotated[str, Field(description='to be added')],
    types: Annotated[list[str], Field(description='Which types of objects to search for.')] = [],
    limit: Annotated[int, Field(description='The maximum number of items to return.')] = 100,
    offset: Annotated[int, Field(description='The offset to start from.')] = 0,
) -> Annotated[str, Field(description='The search results.')]:
    """
    Searches for items in the current project storage only for production branch.

    :param query: The query to search for.
    :param types: The types of items to search for.
    :param limit: The maximum number of items to return.
    :param offset: The offset to start from.
    """
    return ''