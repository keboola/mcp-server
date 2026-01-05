import logging
from typing import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP
from fastmcp.server.middleware import CallNext, MiddlewareContext
from mcp import types as mt

from integtests.conftest import ConfigDef
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    KeboolaClient,
)
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.flow.tools import FlowToolOutput

LOG = logging.getLogger(__name__)


@pytest.fixture
def mcp_server(storage_api_url: str, storage_api_token: str, workspace_schema: str, mocker) -> FastMCP:
    # allow all tool calls regardless the testing project features
    async def on_call_tool(
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        return await call_next(context)

    mocker.patch(
        'keboola_mcp_server.server.ToolsFilteringMiddleware.on_call_tool', new=AsyncMock(side_effect=on_call_tool)
    )

    config = Config(storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema)
    return create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))


@pytest_asyncio.fixture
async def mcp_client(mcp_server: FastMCP) -> AsyncGenerator[Client, None]:
    async with Client(mcp_server) as client:
        yield client


@pytest_asyncio.fixture
async def initial_lf(
    mcp_client: Client, configs: list[ConfigDef], keboola_client: KeboolaClient
) -> AsyncGenerator[FlowToolOutput, None]:
    # Create the initial component configuration test data
    tool_result = await mcp_client.call_tool(
        name='create_flow',
        arguments={
            'name': 'Initial Test Flow',
            'description': 'Initial test flow created by automated test',
            'phases': [{'name': 'Phase1', 'dependsOn': [], 'description': 'First phase'}],
            'tasks': [
                {
                    'id': 20001,
                    'name': 'Task1',
                    'phase': 1,
                    'continueOnFailure': False,
                    'enabled': False,
                    'task': {
                        'componentId': configs[0].component_id,
                        'configId': configs[0].configuration_id,
                        'mode': 'run',
                    },
                }
            ],
        },
    )
    try:
        yield FlowToolOutput.model_validate(tool_result.structured_content)
    finally:
        # Clean up: Delete the configuration
        await keboola_client.storage_client.configuration_delete(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=tool_result.structured_content['configuration_id'],
            skip_trash=True,
        )


@pytest_asyncio.fixture
async def initial_cf(
    mcp_client: Client, configs: list[ConfigDef], keboola_client: KeboolaClient
) -> AsyncGenerator[FlowToolOutput, None]:
    # Create the initial component configuration test data
    tool_result = await mcp_client.call_tool(
        name='create_conditional_flow',
        arguments={
            'name': 'Initial Test Flow',
            'description': 'Initial test flow created by automated test',
            'phases': [
                {
                    'id': 'phase1',
                    'name': 'Phase1',
                    'description': 'First phase',
                    'next': [{'id': 'phase1_end', 'name': 'End Flow', 'goto': None}],
                },
            ],
            'tasks': [
                {
                    'id': 'task1',
                    'name': 'Task1',
                    'phase': 'phase1',
                    'task': {
                        'type': 'job',
                        'componentId': configs[0].component_id,
                        'configId': configs[0].configuration_id,
                        'mode': 'run',
                    },
                },
            ],
        },
    )
    try:
        yield FlowToolOutput.model_validate(tool_result.structured_content)
    finally:
        # Clean up: Delete the configuration
        await keboola_client.storage_client.configuration_delete(
            component_id=CONDITIONAL_FLOW_COMPONENT_ID,
            configuration_id=tool_result.structured_content['configuration_id'],
            skip_trash=True,
        )
