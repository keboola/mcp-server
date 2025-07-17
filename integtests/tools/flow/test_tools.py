import json

import pytest
from fastmcp import Context

from integtests.conftest import ConfigDef
from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.tools.flow.model import Flow
from keboola_mcp_server.tools.flow.tools import (
    FlowToolResponse,
    ListFlowsOutput,
    create_flow,
    get_flow,
    get_flow_schema,
    list_flows,
    update_flow,
)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_create_and_retrieve_flow(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a flow and retrieve it using list_flows.
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
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
        ctx=mcp_context,
        name=flow_name,
        description=flow_description,
        phases=phases,
        tasks=tasks,
    )
    flow_id = created.flow_id
    client = KeboolaClient.from_state(mcp_context.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    expected_links = [
        links_manager.get_flow_detail_link(flow_id, flow_name),
        links_manager.get_flows_dashboard_link(),
        links_manager.get_flows_docs_link(),
    ]
    try:
        assert isinstance(created, FlowToolResponse)
        assert created.description == flow_description
        # Verify the links of created flow
        assert created.success is True
        assert set(created.links) == set(expected_links)

        # Verify the flow is listed in the list_flows tool
        result = await list_flows(mcp_context)
        assert any(f.name == flow_name for f in result.flows)
        found = [f for f in result.flows if f.configuration_id == flow_id][0]
        flow = await get_flow(mcp_context, configuration_id=found.configuration_id)

        assert isinstance(flow, Flow)
        assert flow.component_id == ORCHESTRATOR_COMPONENT_ID
        assert flow.configuration_id == found.configuration_id
        assert flow.configuration.phases[0].name == 'Extract'
        assert flow.configuration.phases[1].name == 'Transform'
        assert flow.configuration.tasks[0].task['componentId'] == configs[0].component_id
        assert set(flow.links) == set(expected_links)

        # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=ORCHESTRATOR_COMPONENT_ID, configuration_id=flow_id
        )

        # Convert metadata list to dictionary for easier checking
        # metadata is a list of dicts with 'key' and 'value' keys
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        assert MetadataField.CREATED_BY_MCP in metadata_dict
        assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'
    finally:
        await client.storage_client.flow_delete(flow_id, skip_trash=True)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_update_flow(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Update a flow and verify the update.
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
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
        ctx=mcp_context,
        name=flow_name,
        description=flow_description,
        phases=phases,
        tasks=tasks,
    )
    flow_id = created.flow_id
    client = KeboolaClient.from_state(mcp_context.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    try:
        new_name = 'Updated Flow Name'
        new_description = 'Updated description.'
        expected_links = [
            links_manager.get_flow_detail_link(flow_id, new_name),
            links_manager.get_flows_dashboard_link(),
            links_manager.get_flows_docs_link(),
        ]
        updated = await update_flow(
            ctx=mcp_context,
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
        assert set(updated.links) == set(expected_links)

        # Verify the metadata - check that KBC.MCP.updatedBy.version.{version} is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=ORCHESTRATOR_COMPONENT_ID, configuration_id=flow_id
        )

        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        sync_flow = await client.storage_client.flow_detail(flow_type=ORCHESTRATOR_COMPONENT_ID, config_id=flow_id)
        updated_by_md_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{sync_flow["version"]}'
        assert updated_by_md_key in metadata_dict
        assert metadata_dict[updated_by_md_key] == 'true'

    finally:
        await client.storage_client.flow_delete(flow_id, skip_trash=True)


@pytest.mark.asyncio
async def test_list_flows_empty(mcp_context: Context) -> None:
    """
    Retrieve flows when none exist (should not error, may return empty list).
    :param mcp_context: The test context fixture.
    """
    flows = await list_flows(mcp_context)
    assert isinstance(flows, ListFlowsOutput)


@pytest.mark.asyncio
async def test_get_flow_schema(mcp_context: Context) -> None:
async def test_get_flow_schema(mcp_context: Context) -> None:
    """
    Test that get_flow_schema returns the flow configuration JSON schema.
    """
    schema_result = await get_flow_schema(mcp_context, ORCHESTRATOR_COMPONENT_ID)

    assert isinstance(schema_result, str)
    assert schema_result.startswith('```json\n')
    assert schema_result.endswith('\n```')

    # Extract and parse the JSON content to verify it's valid
    json_content = schema_result[8:-4]  # Remove ```json\n and \n```
    parsed_schema = json.loads(json_content)

    # Verify basic schema structure
    assert isinstance(parsed_schema, dict)
    assert '$schema' in parsed_schema
    assert 'properties' in parsed_schema
    assert 'phases' in parsed_schema['properties']
    assert 'tasks' in parsed_schema['properties']


@pytest.mark.asyncio
async def test_create_flow_invalid_structure(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a flow with invalid structure (should raise ValueError).
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
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
            ctx=mcp_context,
            name='Invalid Flow',
            description='Should fail',
            phases=phases,
            tasks=tasks,
        )
