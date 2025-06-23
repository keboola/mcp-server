import pytest
from fastmcp import Context

from integtests.conftest import ConfigDef
from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.flow.model import FlowConfigurationResponse
from keboola_mcp_server.tools.flow.tools import (
    FlowToolResponse,
    RetrieveFlowsOutput,
    create_flow,
    get_flow_detail,
    retrieve_flows,
    update_flow,
)


@pytest.mark.asyncio
async def test_create_and_retrieve_flow(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a flow and retrieve it using retrieve_flows.
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    :return: None
    """
    assert configs
    assert configs[0].configuration_id is not None
    phases = [
        {'name': 'Extract', 'dependsOn': [], 'description': 'Extract data'},
        {'name': 'Transform', 'dependsOn': [1], 'description': 'Transform data'},
    ]
    tasks = [
        {
            'name': 'Extract Task',
            'phase': 1,
            'task': {
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
            },
        },
        {
            'name': 'Transform Task',
            'phase': 2,
            'task': {
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
            },
        },
    ]
    flow_name = 'Integration Test Flow'
    flow_description = 'Flow created by integration test.'

    created = await create_flow(
        mcp_context,
        name=flow_name,
        description=flow_description,
        phases=phases,
        tasks=tasks,
    )
    flow_id = created.flow_id
    client = KeboolaClient.from_state(mcp_context.session.state)
    try:
        assert isinstance(created, FlowToolResponse)
        assert created.description == flow_description
        assert created.success is True
        assert len(created.links) == 3

        result = await retrieve_flows(mcp_context)
        assert any(f.name == flow_name for f in result.flows)
        found = [f for f in result.flows if f.id == flow_id][0]
        detail = await get_flow_detail(mcp_context, configuration_id=found.id)
        assert isinstance(detail, FlowConfigurationResponse)
        assert detail.configuration.phases[0].name == 'Extract'
        assert detail.configuration.phases[1].name == 'Transform'
        assert detail.configuration.tasks[0].task['componentId'] == configs[0].component_id
        assert detail.links is not None
        assert len(detail.links) == 3

        # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=flow_id
        )

        # Convert metadata list to dictionary for easier checking
        # metadata is a list of dicts with 'key' and 'value' keys
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        assert MetadataField.CREATED_BY_MCP in metadata_dict
        assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'
    finally:
        await client.storage_client.flow_delete(flow_id, skip_trash=True)


@pytest.mark.asyncio
async def test_update_flow(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Update a flow and verify the update.
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    :return: None
    """
    assert configs
    assert configs[0].configuration_id is not None
    phases = [
        {'name': 'Phase1', 'dependsOn': [], 'description': 'First phase'},
    ]
    tasks = [
        {
            'name': 'Task1',
            'phase': 1,
            'task': {
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
            },
        },
    ]
    flow_name = 'Flow to Update'
    flow_description = 'Initial description.'
    created = await create_flow(
        mcp_context,
        name=flow_name,
        description=flow_description,
        phases=phases,
        tasks=tasks,
    )
    flow_id = created.flow_id
    client = KeboolaClient.from_state(mcp_context.session.state)
    try:
        new_name = 'Updated Flow Name'
        new_description = 'Updated description.'
        updated = await update_flow(
            mcp_context,
            configuration_id=created.flow_id,
            name=new_name,
            description=new_description,
            phases=phases,
            tasks=tasks,
            change_description='Integration test update',
        )
        assert isinstance(updated, FlowToolResponse)
        assert created.flow_id == updated.flow_id
        assert updated.description == new_description
        assert updated.success is True
        assert len(updated.links) == 3

        # Verify the metadata - check that KBC.MCP.updatedBy.version.{version} is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=flow_id
        )

        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        sync_flow = await client.storage_client.flow_detail(flow_id)
        updated_by_md_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{sync_flow["version"]}'
        assert updated_by_md_key in metadata_dict
        assert metadata_dict[updated_by_md_key] == 'true'

    finally:
        await client.storage_client.flow_delete(flow_id, skip_trash=True)


@pytest.mark.asyncio
async def test_retrieve_flows_empty(mcp_context: Context) -> None:
    """
    Retrieve flows when none exist (should not error, may return empty list).
    :param mcp_context: The test context fixture.
    :return: None
    """
    flows = await retrieve_flows(mcp_context)
    assert isinstance(flows, RetrieveFlowsOutput)


@pytest.mark.asyncio
async def test_flow_invalid_structure(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a flow with invalid structure (should raise ValueError).
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    :return: None
    """
    assert configs
    assert configs[0].configuration_id is not None
    phases = [
        {'name': 'Phase1', 'dependsOn': [99], 'description': 'Depends on non-existent phase'},
    ]
    tasks = [
        {
            'name': 'Task1',
            'phase': 1,
            'task': {
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
            },
        },
    ]
    with pytest.raises(ValueError, match='depends on non-existent phase'):
        await create_flow(
            mcp_context,
            name='Invalid Flow',
            description='Should fail',
            phases=phases,
            tasks=tasks,
        )
