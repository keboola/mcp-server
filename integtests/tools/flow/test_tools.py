import json
import logging
from typing import Any, cast

import pytest
import toon_format
import yaml
from fastmcp import Client, Context
from pydantic import ValidationError

from integtests.conftest import ConfigDef, ProjectDef
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    FlowType,
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import ToolError
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.tools.constants import MODIFY_FLOW_TOOL_NAME, UPDATE_FLOW_TOOL_NAME
from keboola_mcp_server.tools.flow.model import ConditionalFlowPhase, Flow, GetFlowsDetailOutput, GetFlowsListOutput
from keboola_mcp_server.tools.flow.tools import (
    FlowToolOutput,
    create_conditional_flow,
    create_flow,
    get_flow_schema,
    get_flows,
)
from keboola_mcp_server.tools.project import get_project_info

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_create_and_retrieve_flow(mcp_context: Context, configs: list[ConfigDef]) -> None:
    """
    Create a flow and retrieve it using get_flows.
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
    flow_id = created.configuration_id
    client = KeboolaClient.from_state(mcp_context.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    expected_links = [
        links_manager.get_flow_detail_link(flow_id=flow_id, flow_name=flow_name, flow_type=flow_type),
        links_manager.get_flows_dashboard_link(flow_type=flow_type),
        links_manager.get_flows_docs_link(),
    ]
    try:
        assert isinstance(created, FlowToolOutput)
        assert created.component_id == ORCHESTRATOR_COMPONENT_ID
        assert created.description == flow_description
        # Verify the links of created flow
        assert created.success is True
        assert set(created.links) == set(expected_links)
        assert created.version is not None

        # Verify the flow is listed in the get_flows tool
        result = await get_flows(mcp_context)
        assert isinstance(result, GetFlowsListOutput)
        assert any(f.name == flow_name for f in result.flows)
        found = [f for f in result.flows if f.configuration_id == flow_id][0]
        flow_detail_result = await get_flows(mcp_context, flow_ids=[found.configuration_id])
        assert isinstance(flow_detail_result, GetFlowsDetailOutput)
        flow = flow_detail_result.flows[0]

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
    Create a conditional flow and retrieve it using get_flows.
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
    flow_id = created.configuration_id
    client = KeboolaClient.from_state(mcp_context.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    expected_links = [
        links_manager.get_flow_detail_link(flow_id=flow_id, flow_name=flow_name, flow_type=flow_type),
        links_manager.get_flows_dashboard_link(flow_type=flow_type),
        links_manager.get_flows_docs_link(),
    ]
    try:
        assert isinstance(created, FlowToolOutput)
        assert created.component_id == CONDITIONAL_FLOW_COMPONENT_ID
        assert created.description == flow_description
        assert created.success is True
        assert set(created.links) == set(expected_links)
        assert created.version is not None

        # Verify the flow is listed in the get_flows tool
        result = await get_flows(mcp_context)
        assert isinstance(result, GetFlowsListOutput)
        assert any(f.name == flow_name for f in result.flows)
        found = [f for f in result.flows if f.configuration_id == flow_id][0]
        flow_detail_result = await get_flows(mcp_context, flow_ids=[found.configuration_id])
        assert isinstance(flow_detail_result, GetFlowsDetailOutput)
        flow = flow_detail_result.flows[0]

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
                        'next': [{'id': 'phase1_phase2', 'name': 'End Flow', 'goto': 'phase2'}],
                    },
                    {
                        'id': 'phase2',
                        'name': 'Phase2',
                        'description': 'Second phase added',
                        'next': [{'id': 'phase2_end', 'name': 'End Flow', 'goto': None}],
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
                        'next': [{'id': 'phase1_phase2', 'name': 'End Flow', 'goto': 'phase2'}],
                    },
                    {
                        'id': 'phase2',
                        'name': 'Phase2',
                        'description': 'Second phase added',
                        'next': [{'id': 'phase2_end', 'name': 'End Flow', 'goto': None}],
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
    initial_lf: FlowToolOutput,
    initial_cf: FlowToolOutput,
    mcp_client: Client,
    keboola_project: ProjectDef,
    keboola_client: KeboolaClient,
) -> None:
    """Tests that 'update_flow' tool works as expected."""
    flow_id = initial_lf.configuration_id if flow_type == ORCHESTRATOR_COMPONENT_ID else initial_cf.configuration_id
    tool_call_result = await mcp_client.call_tool(name='get_flows', arguments={'flow_ids': [flow_id]})
    struct_call_result = cast(dict[str, Any], tool_call_result.structured_content)
    initial_flow_result = GetFlowsDetailOutput.model_validate(struct_call_result['result'])
    initial_flow = initial_flow_result.flows[0]

    # Determine the tool name to use based on the token role, should not break if not using schedulers
    token_info = await keboola_client.storage_client.verify_token()
    token_role = (token_info.get('admin', {}) or {}).get('role')
    if token_role == 'admin':
        tool_name = MODIFY_FLOW_TOOL_NAME
    else:
        tool_name = UPDATE_FLOW_TOOL_NAME

    project_id = keboola_project.project_id
    tool_result = await mcp_client.call_tool(
        name=tool_name,
        arguments={
            'configuration_id': flow_id,
            'flow_type': flow_type,
            'change_description': 'Integration test update',
            **updates,
        },
    )

    # Check the tool's output
    updated_flow = FlowToolOutput.model_validate(tool_result.structured_content)
    assert updated_flow.configuration_id == flow_id
    assert updated_flow.component_id == flow_type
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
    tool_call_result = await mcp_client.call_tool(name='get_flows', arguments={'flow_ids': [flow_id]})
    struct_call_result = cast(dict[str, Any], tool_call_result.structured_content)
    flow_detail_result = GetFlowsDetailOutput.model_validate(struct_call_result['result'])
    flow_detail = flow_detail_result.flows[0]

    assert flow_detail.name == expected_name
    assert flow_detail.description == expected_description

    flow_data = flow_detail.configuration.model_dump(exclude_unset=True, by_alias=True)

    # Check that ids, names, and transitions match for phases using assert all
    if updates.get('phases'):
        # Convert the phases to get the expected format.
        if flow_type == ORCHESTRATOR_COMPONENT_ID:
            expected_phases = updates['phases']
        else:
            expected_phases = [
                ConditionalFlowPhase.model_validate(phase).model_dump(exclude_unset=True, by_alias=True)
                for phase in updates['phases']
            ]
    else:
        expected_phases = [
            phase.model_dump(exclude_unset=True, by_alias=True) for phase in initial_flow.configuration.phases
        ]
    assert len(flow_data['phases']) == len(
        expected_phases
    ), f"Phases count mismatch: {len(flow_data['phases'])} vs {len(expected_phases)}"
    assert all(
        actual['id'] == expected['id']
        and actual['name'] == expected['name']
        and len(actual.get('next', [])) == len(expected.get('next', []))
        and all(
            act_tr['id'] == exp_tr['id'] and act_tr['goto'] == exp_tr['goto']
            for act_tr, exp_tr in zip(actual.get('next', []), expected.get('next', []))
        )
        for actual, expected in zip(flow_data['phases'], expected_phases)
    ), f"Phase id, name, or transitions do not match!\nExpected: {expected_phases}\nGot: {flow_data['phases']}"

    # Check that all task ids and names match between actual and expected using all()
    if updates.get('tasks'):
        expected_tasks = updates['tasks']
    else:
        expected_tasks = [
            task.model_dump(exclude_unset=True, by_alias=True) for task in initial_flow.configuration.tasks
        ]
    assert all(
        actual_task['id'] == expected_task['id'] and actual_task['name'] == expected_task['name']
        for actual_task, expected_task in zip(flow_data['tasks'], expected_tasks)
    ), f"Task id or name mismatch!\nExpected: {expected_tasks}\nGot: {flow_data['tasks']}"

    current_version = flow_detail.version
    assert current_version == 2

    # Check that KBC.MCP.updatedBy.version.{version} is set to 'true'
    metadata = await keboola_client.storage_client.configuration_metadata_get(
        component_id=flow_type, configuration_id=updated_flow.configuration_id
    )
    assert isinstance(metadata, list), f'Expecting list, got: {type(metadata)}'

    meta_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{current_version}'
    meta_value = get_metadata_property(metadata, meta_key)
    assert meta_value == 'true'
    # Check that the original creation metadata is still there
    assert get_metadata_property(metadata, MetadataField.CREATED_BY_MCP) == 'true'


@pytest.mark.asyncio
async def test_get_flows_empty(mcp_context: Context) -> None:
    """
    Retrieve flows when none exist (should not error, may return empty list).
    :param mcp_context: The test context fixture.
    """
    flows = await get_flows(mcp_context)
    assert isinstance(flows, GetFlowsListOutput)
    assert len(flows.flows) == 0


@pytest.mark.asyncio
async def test_get_flows_list(
    keboola_project: ProjectDef, mcp_client: Client, initial_lf: FlowToolOutput, initial_cf: FlowToolOutput
) -> None:
    """Tests that `get_flows` tool works as expected when listing all flows."""
    tool_call_result = await mcp_client.call_tool(name='get_flows', arguments={})
    struct_call_result = cast(dict[str, Any], tool_call_result.structured_content)
    flows = GetFlowsListOutput.model_validate(struct_call_result['result'])
    assert len(flows.flows) == 2
    assert frozenset(flows.links) == frozenset(
        [
            Link(
                type='ui-dashboard',
                title='Flows in the project',
                url=f'https://connection.keboola.com/admin/projects/{keboola_project.project_id}/flows',
            ),
            Link(
                type='ui-dashboard',
                title='Conditional Flows in the project',
                url=f'https://connection.keboola.com/admin/projects/{keboola_project.project_id}/flows-v2',
            ),
        ]
    )
    assert flows.flows[0].configuration_id == initial_cf.configuration_id
    assert flows.flows[1].configuration_id == initial_lf.configuration_id
    assert tool_call_result.content is not None
    assert len(tool_call_result.content) == 1
    assert tool_call_result.content[0].type == 'text'
    toon_decoded = toon_format.decode(tool_call_result.content[0].text)
    assert GetFlowsListOutput.model_validate(toon_decoded) == flows


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

    with pytest.raises(ToolError) as exc_info:
        await create_conditional_flow(
            ctx=mcp_context,
            name='Invalid Conditional Flow',
            description='Should fail',
            phases=phases,
            tasks=tasks,
        )

    err = exc_info.value
    assert isinstance(err.__cause__, ValidationError)

    lines = str(err).splitlines()
    assert len(lines) > 0, 'Empty error message'
    assert lines[0] == 'Found 2 validation error(s) for ConditionalFlowPhase'
    assert yaml.safe_load('\n'.join(lines[1:])) == {
        'errors': [
            {
                'field': 'id',
                'message': 'Input should be a valid string',
                'extra': {
                    'type': 'string_type',
                    'input': '123',
                    'url': 'https://errors.pydantic.dev/2.12/v/string_type',
                },
            },
            {
                'field': 'name',
                'message': 'String should have at least 1 character',
                'extra': {
                    'type': 'string_too_short',
                    'input': '',
                    'ctx': "{'min_length': 1}",
                    'url': 'https://errors.pydantic.dev/2.12/v/string_too_short',
                },
            },
        ]
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('new_config', 'expected_error_message'),
    [
        (
            {
                'phases': [
                    {
                        'id': 'phase-1',
                        'name': 'Phase1',
                        'next': [{'id': 'transition-1', 'goto': None}],
                    },
                    {
                        'id': 'phase-2',
                        'name': 'Phase2',
                        'next': [{'id': 'transition-2', 'goto': None}],
                    },
                ],
                'tasks': [
                    {
                        'id': 'task-1',
                        'name': 'Task1',
                        'phase': 'phase-1',
                        'task': {
                            'type': 'job',
                            'componentId': 'ex-generic-v2',
                            'configId': 'test_config_002',
                            'mode': 'run',
                        },
                    }
                ],
            },
            'Flow has multiple entry phases',
        ),
        (
            {
                'phases': [
                    {
                        'id': 'phase-1',
                        'name': 'Phase1',
                        'next': [{'id': 'transition-1', 'goto': 'phase-2'}],
                    },
                    {
                        'id': 'phase-2',
                        'name': 'Phase2',
                        'next': [{'id': 'transition-2', 'goto': 'phase-1'}],
                    },
                ],
                'tasks': [
                    {
                        'id': 'task-1',
                        'name': 'Task1',
                        'phase': 'phase-1',
                        'task': {
                            'type': 'job',
                            'componentId': 'ex-generic-v2',
                            'configId': 'test_config_002',
                            'mode': 'run',
                        },
                    },
                    {
                        'id': 'task-2',
                        'name': 'Task2',
                        'phase': 'phase-2',
                        'task': {
                            'type': 'job',
                            'componentId': 'ex-generic-v2',
                            'configId': 'test_config_002',
                            'mode': 'run',
                        },
                    },
                ],
            },
            'Flow has no ending phases',
        ),
    ],
)
async def test_create_conditional_flow_semantically_invalid_structure(
    mcp_context: Context, new_config: dict[str, list[dict]], expected_error_message: str
) -> None:
    # Test invalid conditional flow structure - missing required fields and invalid types
    phases = new_config['phases']
    tasks = new_config['tasks']

    with pytest.raises(ValueError, match=expected_error_message):
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

    assert isinstance(orchestrator_result, FlowToolOutput)
    assert orchestrator_result.success is True
    assert orchestrator_result.component_id == ORCHESTRATOR_COMPONENT_ID
    assert orchestrator_result.description == orchestrator_flow_description
    assert orchestrator_result.version is not None
    created_flows.append((ORCHESTRATOR_COMPONENT_ID, orchestrator_result.configuration_id))

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

        assert isinstance(conditional_result, FlowToolOutput)
        assert conditional_result.success is True
        assert conditional_result.component_id == CONDITIONAL_FLOW_COMPONENT_ID
        assert conditional_result.description == conditional_flow_description
        assert conditional_result.version is not None
        created_flows.append((CONDITIONAL_FLOW_COMPONENT_ID, conditional_result.configuration_id))
    else:
        LOG.info('Conditional flows are disabled in this project, skipping conditional flow creation')

    # Step 3: Get individual flows
    for flow_type, flow_id in created_flows:
        flow_result = await get_flows(mcp_context, flow_ids=[flow_id])
        assert isinstance(flow_result, GetFlowsDetailOutput)
        flow = flow_result.flows[0]

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
    flows_list = await get_flows(mcp_context)

    assert isinstance(flows_list, GetFlowsListOutput)
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
