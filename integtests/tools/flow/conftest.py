import logging
from typing import Any, AsyncGenerator, Generator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP
from fastmcp.server.middleware import CallNext, MiddlewareContext
from kbcstorage.client import Client as SyncStorageClient
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


@pytest.fixture(scope='session')
def _ensure_clean_flows(storage_api_token: str, storage_api_url: str) -> Generator[None, Any, None]:
    """
    Ensure the project has no flows before and after flow tests.
    This prevents leftover flows from failed tests affecting new test runs.
    """
    client = SyncStorageClient(storage_api_url, storage_api_token)

    # Check for and clean up any leftover flows before tests
    orchestrator_configs = client.configurations.list(component_id=ORCHESTRATOR_COMPONENT_ID)
    if orchestrator_configs:
        LOG.warning(f'Found {len(orchestrator_configs)} leftover orchestrator flows. Cleaning up...')
        for config in orchestrator_configs:
            LOG.info(f'Deleting leftover orchestrator flow: {config["id"]}')
            # Call delete twice for permanent deletion (first to trash, second to remove from trash)
            client.configurations.delete(ORCHESTRATOR_COMPONENT_ID, config['id'])
            client.configurations.delete(ORCHESTRATOR_COMPONENT_ID, config['id'])

    conditional_configs = client.configurations.list(component_id=CONDITIONAL_FLOW_COMPONENT_ID)
    if conditional_configs:
        LOG.warning(f'Found {len(conditional_configs)} leftover conditional flows. Cleaning up...')
        for config in conditional_configs:
            LOG.info(f'Deleting leftover conditional flow: {config["id"]}')
            # Call delete twice for permanent deletion (first to trash, second to remove from trash)
            client.configurations.delete(CONDITIONAL_FLOW_COMPONENT_ID, config['id'])
            client.configurations.delete(CONDITIONAL_FLOW_COMPONENT_ID, config['id'])

    yield

    # Clean up after all tests complete
    orchestrator_configs = client.configurations.list(component_id=ORCHESTRATOR_COMPONENT_ID)
    for config in orchestrator_configs:
        LOG.info(f'Cleaning up orchestrator flow: {config["id"]}')
        # Call delete twice for permanent deletion (first to trash, second to remove from trash)
        client.configurations.delete(ORCHESTRATOR_COMPONENT_ID, config['id'])
        client.configurations.delete(ORCHESTRATOR_COMPONENT_ID, config['id'])

    conditional_configs = client.configurations.list(component_id=CONDITIONAL_FLOW_COMPONENT_ID)
    for config in conditional_configs:
        LOG.info(f'Cleaning up conditional flow: {config["id"]}')
        # Call delete twice for permanent deletion (first to trash, second to remove from trash)
        client.configurations.delete(CONDITIONAL_FLOW_COMPONENT_ID, config['id'])
        client.configurations.delete(CONDITIONAL_FLOW_COMPONENT_ID, config['id'])


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
    mcp_client: Client,
    configs: list[ConfigDef],
    keboola_client: KeboolaClient,
    _ensure_clean_flows: None,
) -> AsyncGenerator[FlowToolOutput, None]:
    configuration_id: str | None = None

    try:
        LOG.debug('Creating initial test flow (orchestrator)')
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
        flow_output = FlowToolOutput.model_validate(tool_result.structured_content)
        configuration_id = flow_output.configuration_id
        yield flow_output

    except Exception:
        # If tool creation fails but returned a configuration_id, try to extract it
        if 'tool_result' in locals() and hasattr(tool_result, 'structured_content'):
            try:
                configuration_id = tool_result.structured_content.get('configuration_id')
            except Exception:
                pass
        raise

    finally:
        # Clean up if we have a configuration_id
        if configuration_id:
            try:
                LOG.debug(f'Cleaning up flow configuration: {configuration_id}')
                await keboola_client.storage_client.configuration_delete(
                    component_id=ORCHESTRATOR_COMPONENT_ID,
                    configuration_id=configuration_id,
                    skip_trash=True,
                )
            except Exception as cleanup_error:
                LOG.error(f'Failed to clean up flow configuration {configuration_id}: {cleanup_error}')


@pytest_asyncio.fixture
async def initial_cf(
    mcp_client: Client,
    configs: list[ConfigDef],
    keboola_client: KeboolaClient,
    _ensure_clean_flows: None,
) -> AsyncGenerator[FlowToolOutput, None]:
    configuration_id: str | None = None

    try:
        LOG.debug('Creating initial test flow (conditional)')
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
        flow_output = FlowToolOutput.model_validate(tool_result.structured_content)
        configuration_id = flow_output.configuration_id
        yield flow_output

    except Exception:
        # If tool creation fails but returned a configuration_id, try to extract it
        if 'tool_result' in locals() and hasattr(tool_result, 'structured_content'):
            try:
                configuration_id = tool_result.structured_content.get('configuration_id')
            except Exception:
                pass
        raise

    finally:
        # Clean up if we have a configuration_id
        if configuration_id:
            try:
                LOG.debug(f'Cleaning up conditional flow configuration: {configuration_id}')
                await keboola_client.storage_client.configuration_delete(
                    component_id=CONDITIONAL_FLOW_COMPONENT_ID,
                    configuration_id=configuration_id,
                    skip_trash=True,
                )
            except Exception as cleanup_error:
                LOG.error(f'Failed to clean up conditional flow configuration {configuration_id}: {cleanup_error}')
