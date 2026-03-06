"""
MCP App tool for configuration diff preview.

The ``preview_config_diff`` tool is model-visible -- the LLM calls it with
a mutation tool name and its parameters, and the MCP App renders a side-by-side
diff of the original vs updated configuration before the mutation is applied.
"""

import importlib.resources
import logging
from typing import Any

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations

from keboola_mcp_server.apps import APP_RESOURCE_MIME_TYPE, build_app_resource_meta, build_app_tool_meta
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.preview import compute_config_diff
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

CONFIG_DIFF_TAG = 'config_diff'
CONFIG_DIFF_RESOURCE_URI = 'ui://keboola/config-diff'

SUPPORTED_TOOLS = frozenset(
    {
        'update_config',
        'update_config_row',
        'update_sql_transformation',
        'update_flow',
        'modify_flow',
        'modify_data_app',
    }
)


def add_config_diff_tools(mcp: KeboolaMcpServer) -> None:
    """Register the Config Diff MCP App tool and resource."""
    html_content = importlib.resources.read_text('keboola_mcp_server.apps', 'config_diff.html')

    resource_meta = build_app_resource_meta(
        csp_resource_domains=['https://unpkg.com'],
    )

    @mcp.resource(
        CONFIG_DIFF_RESOURCE_URI,
        name='Config Diff',
        description='Side-by-side configuration diff viewer.',
        mime_type=APP_RESOURCE_MIME_TYPE,
        meta=resource_meta,
    )
    def config_diff_resource() -> str:
        return html_content

    app_meta = build_app_tool_meta(
        resource_uri=CONFIG_DIFF_RESOURCE_URI,
    )

    mcp.add_tool(
        FunctionTool.from_function(
            preview_config_diff,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={CONFIG_DIFF_TAG},
            meta=app_meta,
        )
    )

    LOG.info('Config Diff MCP App tool registered.')


@tool_errors()
async def preview_config_diff(
    ctx: Context,
    tool_name: str,
    tool_params: dict[str, Any],
) -> dict[str, Any]:
    """
    Preview configuration changes before applying a mutation.

    Shows a side-by-side diff of the original and updated configuration.
    Call this BEFORE calling any mutation tool (update_config, update_config_row,
    update_sql_transformation, update_flow, modify_flow, modify_data_app) to
    let the user review changes before they are applied.

    Pass the same tool_name and tool_params you would use for the mutation tool.

    EXAMPLES:
    - tool_name="update_config", tool_params={"component_id": "keboola.ex-aws-s3",
      "configuration_id": "123", "change_description": "Update bucket",
      "parameter_updates": [{"op": "set", "path": "bucket", "value": "new-bucket"}]}
    - tool_name="modify_flow", tool_params={"configuration_id": "456",
      "flow_type": "keboola.orchestrator", "change_description": "Update phases", ...}
    """
    if tool_name not in SUPPORTED_TOOLS:
        return {
            'coordinates': {},
            'originalConfig': {},
            'updatedConfig': {},
            'isValid': False,
            'validationErrors': [
                f'Unsupported tool_name "{tool_name}". ' f'Supported: {", ".join(sorted(SUPPORTED_TOOLS))}'
            ],
        }

    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)

    result = await compute_config_diff(
        tool_name=tool_name,
        tool_params=tool_params,
        client=client,
        workspace_manager=workspace_manager,
    )

    return result.model_dump(by_alias=True, exclude_none=True)
