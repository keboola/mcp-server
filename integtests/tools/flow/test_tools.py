import json
import logging
from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio
from fastmcp import Client, Context, FastMCP
from pydantic import ValidationError

from integtests.conftest import ConfigDef, ProjectDef
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    FlowType,
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.config import Config, MetadataField
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.flow.model import Flow
from keboola_mcp_server.tools.flow.tools import (
    FlowToolResponse,
    ListFlowsOutput,
    create_conditional_flow,
    create_flow,
    get_flow,
    get_flow_schema,
    list_flows,
)
from keboola_mcp_server.tools.project import get_project_info

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_create_and_retrieve_flow(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a flow and retrieve it using list_flows.
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    """
    assert configs
    assert configs[0].configuration_id is not None
    flow_type = ORCHESTRATOR_COMPONENT_ID
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
    flow_id = created.id
    client = KeboolaClient.from_state(mcp_context.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    expected_links = [
        links_manager.get_flow_detail_link(flow_id=flow_id, flow_name=flow_name, flow_type=flow_type),
        links_manager.get_flows_dashboard_link(flow_type=flow_type),
        links_manager.get_flows_docs_link(),
    ]
    try:
        assert isinstance(created, FlowToolResponse)
        assert created.description == flow_description
        # Verify the links of created flow
        assert created.success is True
        assert set(created.links) == set(expected_links)
        assert created.version is not None

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
        await client.storage_client.configuration_delete(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=flow_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_create_and_retrieve_conditional_flow(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a conditional flow and retrieve it using list_flows.
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    """
    assert configs
    assert configs[0].configuration_id is not None
    flow_type = CONDITIONAL_FLOW_COMPONENT_ID

    phases = [
        {
            'id': 'extract_phase',
            'name': 'Extract',
            'description': 'Extract data',
            'next': [{'id': 'extract_to_transform', 'name': 'Extract to Transform', 'goto': 'transform_phase'}],
        },
        {
            'id': 'transform_phase',
            'name': 'Transform',
            'description': 'Transform data',
            'next': [{'id': 'transform_end', 'name': 'End Flow', 'goto': None}],
        },
    ]
    tasks = [
        {
            'id': 'extract_task',
            'name': 'Extract Task',
            'phase': 'extract_phase',
            'task': {
                'type': 'job',
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
                'mode': 'run',
            },
        },
        {
            'id': 'transform_task',
            'name': 'Transform Task',
            'phase': 'transform_phase',
            'task': {
                'type': 'job',
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
                'mode': 'run',
            },
        },
    ]
    flow_name = 'Integration Test Conditional Flow'
    flow_description = 'Conditional flow created by integration test.'

    created = await create_conditional_flow(
        ctx=mcp_context,
        name=flow_name,
        description=flow_description,
        phases=phases,
        tasks=tasks,
    )
    flow_id = created.id
    client = KeboolaClient.from_state(mcp_context.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    expected_links = [
        links_manager.get_flow_detail_link(flow_id=flow_id, flow_name=flow_name, flow_type=flow_type),
        links_manager.get_flows_dashboard_link(flow_type=flow_type),
        links_manager.get_flows_docs_link(),
    ]
    try:
        assert isinstance(created, FlowToolResponse)
        assert created.description == flow_description
        assert created.success is True
        assert set(created.links) == set(expected_links)
        assert created.version is not None

        # Verify the flow is listed in the list_flows tool
        result = await list_flows(mcp_context)
        assert any(f.name == flow_name for f in result.flows)
        found = [f for f in result.flows if f.configuration_id == flow_id][0]
        flow = await get_flow(mcp_context, configuration_id=found.configuration_id)

        assert isinstance(flow, Flow)
        assert flow.component_id == CONDITIONAL_FLOW_COMPONENT_ID
        assert flow.configuration_id == found.configuration_id
        assert flow.configuration.phases[0].name == 'Extract'
        assert flow.configuration.phases[1].name == 'Transform'
        assert flow.configuration.tasks[0].task.component_id == configs[0].component_id
        assert set(flow.links) == set(expected_links)

        # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=CONDITIONAL_FLOW_COMPONENT_ID, configuration_id=flow_id
        )

        # Convert metadata list to dictionary for easier checking
        # metadata is a list of dicts with 'key' and 'value' keys
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        assert MetadataField.CREATED_BY_MCP in metadata_dict
        assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'
    finally:
        await client.storage_client.configuration_delete(
            component_id=CONDITIONAL_FLOW_COMPONENT_ID,
            configuration_id=flow_id,
            skip_trash=True,
        )


@pytest.fixture
def mcp_server(storage_api_url: str, storage_api_token: str, workspace_schema: str) -> FastMCP:
    config = Config(storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema)
    return create_server(config)


@pytest_asyncio.fixture
async def mcp_client(mcp_server: FastMCP) -> AsyncGenerator[Client, None]:
    async with Client(mcp_server) as client:
        yield client


@pytest_asyncio.fixture
async def initial_lf(
    mcp_client: Client, configs: list[ConfigDef], keboola_client: KeboolaClient
) -> AsyncGenerator[FlowToolResponse, None]:
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
        yield FlowToolResponse.model_validate(tool_result.structured_content)
    finally:
        # Clean up: Delete the configuration
        await keboola_client.storage_client.configuration_delete(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=tool_result.structured_content['id'],
            skip_trash=True,
        )


@pytest_asyncio.fixture
async def initial_cf(
    mcp_client: Client, configs: list[ConfigDef], keboola_client: KeboolaClient
) -> AsyncGenerator[FlowToolResponse, None]:
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
        yield FlowToolResponse.model_validate(tool_result.structured_content)
    finally:
        # Clean up: Delete the configuration
        await keboola_client.storage_client.configuration_delete(
            component_id=CONDITIONAL_FLOW_COMPONENT_ID,
            configuration_id=tool_result.structured_content['id'],
            skip_trash=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('flow_type', 'updates'),
    [
        (
            ORCHESTRATOR_COMPONENT_ID,
            {
                'phases': [
                    {'id': 1, 'name': 'Phase1', 'dependsOn': [], 'description': 'First phase updated'},
                    {'id': 2, 'name': 'Phase2', 'dependsOn': [], 'description': 'Second phase added'},
                ],
                'tasks': [
                    {
                        'id': 20001,
                        'name': 'Task1 - Updated',
                        'phase': 1,
                        'continueOnFailure': False,
                        'enabled': False,
                        'task': {'componentId': 'ex-generic-v2', 'configId': 'test_config_001', 'mode': 'run'},
                    },
                    {
                        'id': 20002,
                        'name': 'Task2 - Added',
                        'phase': 2,
                        'continueOnFailure': False,
                        'enabled': False,
                        'task': {'componentId': 'ex-generic-v2', 'configId': 'test_config_002', 'mode': 'run'},
                    },
                ],
                'name': 'Updated Test Flow',
                'description': 'The test flow updated by an automated test.',
            },
        ),
        (
            ORCHESTRATOR_COMPONENT_ID,
            {
                'phases': [
                    {'id': 1, 'name': 'Phase1', 'dependsOn': [], 'description': 'First phase updated'},
                    {'id': 2, 'name': 'Phase2', 'dependsOn': [], 'description': 'Second phase added'},
                ]
            },
        ),
        (
            ORCHESTRATOR_COMPONENT_ID,
            {
                'tasks': [
                    {
                        'id': 20001,
                        'name': 'Task1 - Updated',
                        'phase': 1,
                        'continueOnFailure': False,
                        'enabled': False,
                        'task': {'componentId': 'ex-generic-v2', 'configId': 'test_config_001', 'mode': 'run'},
                    },
                    {
                        'id': 20002,
                        'name': 'Task2 - Added',
                        'phase': 1,
                        'continueOnFailure': False,
                        'enabled': False,
                        'task': {'componentId': 'ex-generic-v2', 'configId': 'test_config_002', 'mode': 'run'},
                    },
                ]
            },
        ),
        (ORCHESTRATOR_COMPONENT_ID, {'name': 'Updated just name'}),
        (ORCHESTRATOR_COMPONENT_ID, {'description': 'Updated just description'}),
        (
            CONDITIONAL_FLOW_COMPONENT_ID,
            {
                'phases': [
                    {
                        'id': 'phase1',
                        'name': 'Phase1',
                        'description': 'First phase updated',
                        'next': [{'id': 'phase1_end', 'name': 'End Flow', 'goto': None}],
                    },
                    {
                        'id': 'phase2',
                        'name': 'Phase2',
                        'description': 'Second phase added',
                        'next': [{'id': 'phase1_end', 'name': 'End Flow', 'goto': None}],
                    },
                ],
                'tasks': [
                    {
                        'id': 'task1',
                        'name': 'Task1 - Updated',
                        'phase': 'phase1',
                        'task': {
                            'type': 'job',
                            'componentId': 'ex-generic-v2',
                            'configId': 'test_config_001',
                            'mode': 'run',
                        },
                    },
                    {
                        'id': 'task2',
                        'name': 'Task2 - Added',
                        'phase': 'phase2',
                        'task': {
                            'type': 'job',
                            'componentId': 'ex-generic-v2',
                            'configId': 'test_config_002',
                            'mode': 'run',
                        },
                    },
                ],
            },
        ),
        (
            CONDITIONAL_FLOW_COMPONENT_ID,
            {
                'phases': [
                    {
                        'id': 'phase1',
                        'name': 'Phase1',
                        'description': 'First phase updated',
                        'next': [{'id': 'phase1_end', 'name': 'End Flow', 'goto': None}],
                    },
                    {
                        'id': 'phase2',
                        'name': 'Phase2',
                        'description': 'Second phase added',
                        'next': [{'id': 'phase1_end', 'name': 'End Flow', 'goto': None}],
                    },
                ]
            },
        ),
        (
            CONDITIONAL_FLOW_COMPONENT_ID,
            {
                'tasks': [
                    {
                        'id': 'task1',
                        'name': 'Task1 - Updated',
                        'phase': 'phase1',
                        'task': {
                            'type': 'job',
                            'componentId': 'ex-generic-v2',
                            'configId': 'test_config_001',
                            'mode': 'run',
                        },
                    },
                    {
                        'id': 'task2',
                        'name': 'Task2 - Added',
                        'phase': 'phase1',
                        'task': {
                            'type': 'job',
                            'componentId': 'ex-generic-v2',
                            'configId': 'test_config_002',
                            'mode': 'run',
                        },
                    },
                ]
            },
        ),
        (CONDITIONAL_FLOW_COMPONENT_ID, {'name': 'Updated just name'}),
        (CONDITIONAL_FLOW_COMPONENT_ID, {'description': 'Updated just description'}),
    ],
)
async def test_update_flow(
    flow_type: FlowType,
    updates: dict[str, Any],
    initial_lf: FlowToolResponse,
    initial_cf: FlowToolResponse,
    mcp_client: Client,
    keboola_project: ProjectDef,
    keboola_client: KeboolaClient,
) -> None:
    """Tests that 'update_flow' tool works as expected."""
    initial_flow = initial_lf if flow_type == ORCHESTRATOR_COMPONENT_ID else initial_cf
    project_id = keboola_project.project_id
    flow_id = initial_flow.id
    tool_result = await mcp_client.call_tool(
        name='update_flow',
        arguments={
            'configuration_id': flow_id,
            'flow_type': flow_type,
            'change_description': 'Integration test update',
            **updates,
        },
    )

    # Check the tool's output
    updated_flow = FlowToolResponse.model_validate(tool_result.structured_content)
    assert updated_flow.id == flow_id
    assert updated_flow.success is True
    assert updated_flow.timestamp is not None
    assert updated_flow.version is not None

    expected_name = updates.get('name') or 'Initial Test Flow'
    expected_description = updates.get('description') or initial_flow.description
    assert updated_flow.description == expected_description
    if flow_type == ORCHESTRATOR_COMPONENT_ID:
        flow_path = 'flows'
        flow_label = 'Flows'
    else:
        flow_path = 'flows-v2'
        flow_label = 'Conditional Flows'
    assert frozenset(updated_flow.links) == frozenset(
        [
            Link(
                type='ui-detail',
                title=f'Flow: {expected_name}',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/{flow_path}/{flow_id}',
            ),
            Link(
                type='ui-dashboard',
                title=f'{flow_label} in the project',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/{flow_path}',
            ),
            Link(type='docs', title='Documentation for Keboola Flows', url='https://help.keboola.com/flows/'),
        ]
    )

    # Verify the configuration was updated
    flow_detail = await keboola_client.storage_client.configuration_detail(
        component_id=flow_type, configuration_id=updated_flow.id
    )

    assert flow_detail['name'] == expected_name
    assert flow_detail['description'] == expected_description

    flow_data = flow_detail.get('configuration')
    assert isinstance(flow_data, dict), f'Expecting dict, got: {type(flow_data)}'

    if (expected_phases := updates.get('phases')) is not None:
        assert flow_data['phases'] == expected_phases

    if (expected_tasks := updates.get('tasks')) is not None:
        assert flow_data['tasks'] == expected_tasks

    current_version = flow_detail['version']
    assert isinstance(current_version, int), f'Expecting int, got: {type(current_version)}'
    assert current_version == 2

    # Check that KBC.MCP.updatedBy.version.{version} is set to 'true'
    metadata = await keboola_client.storage_client.configuration_metadata_get(
        component_id=flow_type, configuration_id=updated_flow.id
    )
    assert isinstance(metadata, list), f'Expecting list, got: {type(metadata)}'

    meta_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{current_version}'
    meta_value = get_metadata_property(metadata, meta_key)
    assert meta_value == 'true'
    # Check that the original creation metadata is still there
    assert get_metadata_property(metadata, MetadataField.CREATED_BY_MCP) == 'true'


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
    """
    Test that get_flow_schema returns the flow configuration JSON schema.
    Tests the conditional behavior where the tool might return a different schema
    than requested based on project settings.
    """
    project_info = await get_project_info(mcp_context)

    # Test 1: Request orchestrator schema (should always work)
    legacy_flow_schema = await get_flow_schema(mcp_context, ORCHESTRATOR_COMPONENT_ID)

    assert isinstance(legacy_flow_schema, str)
    assert legacy_flow_schema.startswith('```json\n')
    assert legacy_flow_schema.endswith('\n```')
    assert 'dependsOn' in legacy_flow_schema

    # Extract and parse the JSON content to verify it's valid
    json_content = legacy_flow_schema[8:-4]  # Remove ```json\n and \n```
    parsed_legacy_schema = json.loads(json_content)

    # Verify basic schema structure for legacy flow
    assert isinstance(parsed_legacy_schema, dict)
    assert '$schema' in parsed_legacy_schema
    assert 'properties' in parsed_legacy_schema
    assert 'phases' in parsed_legacy_schema['properties']
    assert 'tasks' in parsed_legacy_schema['properties']

    # Test 2: Request conditional flow schema (behavior depends on project settings)
    conditional_schema = await get_flow_schema(mcp_context, CONDITIONAL_FLOW_COMPONENT_ID)

    assert isinstance(conditional_schema, str)
    assert conditional_schema.startswith('```json\n')
    assert conditional_schema.endswith('\n```')

    # Extract and parse the JSON content
    json_content = conditional_schema[8:-4]  # Remove ```json\n and \n```
    parsed_conditional_schema = json.loads(json_content)

    # Test 3: Verify the conditional behavior
    if not project_info.conditional_flows:
        # If the project does not support conditional flows, both requests should return the same schema
        assert legacy_flow_schema == conditional_schema
        LOG.info('Project has conditional flows disabled - both schemas are identical')
    else:
        # If conditional flows are enabled, the schemas should be different
        assert legacy_flow_schema != conditional_schema
        LOG.info('Project has conditional flows enabled - schemas are different')

        # Verify that the conditional schema has conditional-specific properties
        conditional_phases = parsed_conditional_schema['properties']['phases']['items']['properties']
        assert 'next' in conditional_phases  # Conditional flows use 'next' instead of 'dependsOn'

        conditional_tasks = parsed_conditional_schema['properties']['tasks']['items']['properties']['task']
        assert 'oneOf' in conditional_tasks  # Conditional flows have structured task types


@pytest.mark.asyncio
async def test_create_legacy_flow_invalid_structure(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a legacy flow with invalid structure (should raise ValueError).
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
            name='Invalid Legacy Flow',
            description='Should fail',
            phases=phases,
            tasks=tasks,
        )


@pytest.mark.asyncio
async def test_create_conditional_flow_invalid_structure(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a conditional flow with invalid structure (should raise ValueError).
    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    """
    assert configs
    assert configs[0].configuration_id is not None

    # Test invalid conditional flow structure - missing required fields and invalid types
    phases = [
        {
            'id': 123,  # Invalid: should be string, not integer
            'name': '',  # Invalid: empty string not allowed
            'next': [{'id': 'transition-1', 'goto': 'phase-2'}],
        }
    ]

    tasks = [
        {
            'id': 'task-1',
            'name': 'Task1',
            'phase': 'phase-1',
            'enabled': True,
            'task': {
                'type': 'invalid_type',  # Invalid: not one of job, notification, variable
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
                'mode': 'invalid_mode',  # Invalid: should be 'run'
            },
        }
    ]

    with pytest.raises(ValidationError):
        await create_conditional_flow(
            ctx=mcp_context,
            name='Invalid Conditional Flow',
            description='Should fail',
            phases=phases,
            tasks=tasks,
        )


@pytest.mark.asyncio
async def test_flow_lifecycle_integration(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Test complete flow lifecycle for both legacy and conditional flows.
    Creates flows, retrieves them individually, and lists all flows.
    Tests project-aware behavior based on conditional flows setting.
    """
    assert configs
    assert configs[0].configuration_id is not None

    project_info = await get_project_info(mcp_context)

    # Test data for legacy flow
    legacy_phases = [
        {'id': 1, 'name': 'Extract', 'description': 'Extract data from source', 'dependsOn': []},
        {'id': 2, 'name': 'Load', 'description': 'Load data to destination', 'dependsOn': [1]},
    ]

    legacy_tasks = [
        {
            'id': 20001,
            'name': 'Extract from API',
            'phase': 1,
            'enabled': True,
            'continueOnFailure': False,
            'task': {'componentId': configs[0].component_id, 'configId': configs[0].configuration_id, 'mode': 'run'},
        },
        {
            'id': 20002,
            'name': 'Load to Warehouse',
            'phase': 2,
            'enabled': True,
            'continueOnFailure': False,
            'task': {'componentId': configs[0].component_id, 'configId': configs[0].configuration_id, 'mode': 'run'},
        },
    ]

    # Test data for conditional flow
    conditional_phases = [
        {
            'id': 'phase-1',
            'name': 'Extract',
            'description': 'Extract data from source',
            'next': [{'id': 'transition-1', 'goto': 'phase-2'}],
        },
        {'id': 'phase-2', 'name': 'Load', 'description': 'Load data to destination', 'next': []},
    ]

    conditional_tasks = [
        {
            'id': 'task-1',
            'name': 'Extract from API',
            'phase': 'phase-1',
            'enabled': True,
            'task': {
                'type': 'job',
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
                'mode': 'run',
            },
        },
        {
            'id': 'task-2',
            'name': 'Load to Warehouse',
            'phase': 'phase-2',
            'enabled': True,
            'task': {
                'type': 'job',
                'componentId': configs[0].component_id,
                'configId': configs[0].configuration_id,
                'mode': 'run',
            },
        },
    ]

    created_flows = []

    # Step 1: Create orchestrator flow (should always work)
    orchestrator_flow_name = 'Integration Test Orchestrator Flow'
    orchestrator_flow_description = 'Orchestrator flow created by integration test'

    orchestrator_result = await create_flow(
        ctx=mcp_context,
        name=orchestrator_flow_name,
        description=orchestrator_flow_description,
        phases=legacy_phases,
        tasks=legacy_tasks,
    )

    assert isinstance(orchestrator_result, FlowToolResponse)
    assert orchestrator_result.success is True
    assert orchestrator_result.description == orchestrator_flow_description
    assert orchestrator_result.version is not None
    created_flows.append((ORCHESTRATOR_COMPONENT_ID, orchestrator_result.id))

    # Step 2: Try to create conditional flow (only if project allows it)
    conditional_flow_name = 'Integration Test Conditional Flow'
    conditional_flow_description = 'Conditional flow created by integration test'

    if project_info.conditional_flows:
        conditional_result = await create_conditional_flow(
            ctx=mcp_context,
            name=conditional_flow_name,
            description=conditional_flow_description,
            phases=conditional_phases,
            tasks=conditional_tasks,
        )

        assert isinstance(conditional_result, FlowToolResponse)
        assert conditional_result.success is True
        assert conditional_result.description == conditional_flow_description
        assert conditional_result.version is not None
        created_flows.append((CONDITIONAL_FLOW_COMPONENT_ID, conditional_result.id))
    else:
        LOG.info('Conditional flows are disabled in this project, skipping conditional flow creation')

    # Step 3: Get individual flows
    for flow_type, flow_id in created_flows:
        flow = await get_flow(mcp_context, configuration_id=flow_id)

        assert isinstance(flow, Flow)
        assert flow.configuration_id == flow_id

        if flow_type == ORCHESTRATOR_COMPONENT_ID:
            assert flow.name == orchestrator_flow_name
            assert flow.component_id == ORCHESTRATOR_COMPONENT_ID
            assert len(flow.configuration.phases) == 2
            assert len(flow.configuration.tasks) == 2
            assert flow.configuration.phases[0].name == 'Extract'
            assert flow.configuration.phases[1].name == 'Load'
        else:
            assert flow.name == conditional_flow_name
            assert flow.component_id == CONDITIONAL_FLOW_COMPONENT_ID
            assert len(flow.configuration.phases) == 2
            assert len(flow.configuration.tasks) == 2
            assert flow.configuration.phases[0].name == 'Extract'
            assert flow.configuration.phases[1].name == 'Load'

    # Step 4: List all flows and verify our created flows are there
    flows_list = await list_flows(mcp_context)

    assert isinstance(flows_list, ListFlowsOutput)
    assert len(flows_list.flows) >= len(created_flows)

    # Verify our created flows are in the list
    flow_ids = [flow.configuration_id for flow in flows_list.flows]
    for flow_type, flow_id in created_flows:
        assert flow_id in flow_ids, f'Created {flow_type} flow {flow_id} not found in flows list'

    # Step 5: Clean up - delete all created flows
    client = KeboolaClient.from_state(mcp_context.session.state)
    for flow_type, flow_id in created_flows:
        try:
            await client.storage_client.configuration_delete(
                component_id=flow_type,
                configuration_id=flow_id,
                skip_trash=True,
            )
            LOG.info(f'Successfully deleted {flow_type} flow {flow_id}')
        except Exception as e:
            LOG.warning(f'Failed to delete {flow_type} flow {flow_id}: {e}')
