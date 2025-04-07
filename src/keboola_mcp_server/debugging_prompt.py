"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.prompts.base import AssistantMessage, Message, Prompt, UserMessage

logger = logging.getLogger(__name__)


def add_debugging_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_prompt(
        Prompt.from_function(debug_job_failure)
    )


def debug_job_failure(
    job_id: str
) -> list[Message]:  # Ensure the return type is correct
    return [
        UserMessage(f"I have a failed job with ID `{job_id}`."),
        UserMessage("The job failed and I need help understanding why."),
        AssistantMessage("I'll help you debug it. What information do you have so far?"),
    ]
