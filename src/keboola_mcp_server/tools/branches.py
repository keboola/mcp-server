"""Branch management tools for the MCP server."""

import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors

LOG = logging.getLogger(__name__)

BRANCH_TOOLS_TAG = 'branches'


class BranchInfo(BaseModel):
    id: int = Field(description='The branch ID.')
    name: str = Field(description='The branch name.')
    is_default: bool = Field(description='Whether this is the default (main/production) branch.')
    description: str = Field(default='', description='The branch description.')
    created: str = Field(default='', description='The branch creation timestamp.')


class BranchListResult(BaseModel):
    branches: list[BranchInfo] = Field(description='List of branches in the project.')


class BranchCreateResult(BaseModel):
    id: int = Field(description='The ID of the created branch.')
    name: str = Field(description='The name of the created branch.')
    description: str = Field(default='', description='The description of the created branch.')


def add_branch_tools(mcp: FastMCP) -> None:
    """Add branch management tools to the MCP server."""
    LOG.info(f'Adding tool {list_branches.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            list_branches,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={BRANCH_TOOLS_TAG},
        )
    )

    LOG.info(f'Adding tool {create_branch.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            create_branch,
            annotations=ToolAnnotations(destructiveHint=False),
            tags={BRANCH_TOOLS_TAG},
        )
    )

    LOG.info('Branch tools initialized.')


@tool_errors()
async def list_branches(
    ctx: Context,
) -> BranchListResult:
    """
    Lists all branches (main and development) in the current project.

    Use this tool to discover available branches before specifying a branch_id
    in other tool calls.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    raw_branches = await client.storage_client.branches_list()

    branches = []
    for b in raw_branches:
        branches.append(
            BranchInfo(
                id=b.get('id', 0),
                name=b.get('name', ''),
                is_default=b.get('isDefault', False),
                description=b.get('description', ''),
                created=b.get('created', ''),
            )
        )

    return BranchListResult(branches=branches)


@tool_errors()
async def create_branch(
    ctx: Context,
    name: Annotated[str, Field(description='The name for the new development branch.')],
    description: Annotated[str, Field(description='Optional description for the branch.')] = '',
) -> BranchCreateResult:
    """
    Creates a new development branch in the current project.

    Development branches allow you to make changes without affecting the main/production branch.
    After creating a branch, use its ID as the branch_id parameter in other tool calls.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    result = await client.storage_client.dev_branch_create(name=name, description=description)

    return BranchCreateResult(
        id=result.get('id', 0),
        name=result.get('name', name),
        description=result.get('description', description),
    )
