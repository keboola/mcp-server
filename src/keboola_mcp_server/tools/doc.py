import logging
from typing import Annotated

from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors

LOG = logging.getLogger(__name__)


def add_doc_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    doc_tools = [
        docs_query,
    ]
    for tool in doc_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(tool)

    LOG.info('Doc tools initialized.')


class DocsAnswer(BaseModel):
    """An answer to a documentation query."""

    text: str = Field(description='Text of the answer to a documentation query.')
    source_urls: list[str] = Field(description='List of URLs to the sources of the answer.')


@tool_errors()
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
