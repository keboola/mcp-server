"""Unit tests for Flow management tools."""

from typing import Any, Dict, List

import pytest
from dateutil import parser
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.flow.model import (
    FlowConfiguration,
    ListFlowsOutput,
    ReducedFlow,
)
from keboola_mcp_server.tools.flow.tools import (
    FlowToolResponse,
    create_flow,
    get_flow,
    list_flows,
    update_flow,
)

# --- Test Flow Tools ---


class TestFlowTools:
    """Test flow management tools."""

    @pytest.mark.asyncio
    async def test_create_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        sample_phases: List[Dict[str, Any]],
        sample_tasks: List[Dict[str, Any]],
        mock_raw_flow_config: Dict[str, Any],
        mock_project_id: str,
    ):
        """Test flow creation."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_create = mocker.AsyncMock(return_value=mock_raw_flow_config)
        keboola_client.storage_client.project_id = mocker.AsyncMock(return_value=mock_project_id)

        result = await create_flow(
            ctx=mcp_context_client,
            name='Test Flow',
            description='Test flow description',
            phases=sample_phases,
            tasks=sample_tasks,
        )

        assert isinstance(result, FlowToolResponse)
        assert result.description == 'Test flow description'
        assert result.timestamp == parser.isoparse('2025-05-25T06:33:41+0200')
        assert result.success is True
        assert len(result.links) == 3

        keboola_client.storage_client.flow_create.assert_called_once()
        call_args = keboola_client.storage_client.flow_create.call_args

        assert call_args.kwargs['name'] == 'Test Flow'
        assert call_args.kwargs['description'] == 'Test flow description'
        assert 'flow_configuration' in call_args.kwargs

        flow_config = call_args.kwargs['flow_configuration']
        assert 'phases' in flow_config
        assert 'tasks' in flow_config
        assert len(flow_config['phases']) == 3
        assert len(flow_config['tasks']) == 3

    @pytest.mark.asyncio
    async def test_list_flows_all(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_raw_flow_config: Dict[str, Any],
        mock_empty_flow_config: Dict[str, Any],
    ):
        """Test listing all flows."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_list = mocker.AsyncMock(
            return_value=[mock_raw_flow_config, mock_empty_flow_config]
        )

        result = await list_flows(ctx=mcp_context_client)

        assert isinstance(result, ListFlowsOutput)
        assert len(result.flows) == 2
        assert all(isinstance(flow, ReducedFlow) for flow in result.flows)
        assert result.flows[0].id == '21703284'
        assert result.flows[1].id == '21703285'
        assert result.flows[0].phases_count == 2
        assert result.flows[1].phases_count == 0

    @pytest.mark.asyncio
    async def test_list_flows_specific_ids(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_raw_flow_config: Dict[str, Any]
    ):
        """Test listing specific flows by ID."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(return_value=mock_raw_flow_config)

        result = await list_flows(ctx=mcp_context_client, flow_ids=['21703284'])

        assert len(result.flows) == 1
        assert result.flows[0].id == '21703284'
        keboola_client.storage_client.flow_detail.assert_called_once_with('21703284')

    @pytest.mark.asyncio
    async def test_list_flows_with_missing_id(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_raw_flow_config: Dict[str, Any]
    ):
        """Test listing flows when some IDs don't exist."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        def mock_get_flow(flow_id):
            if flow_id == '21703284':
                return mock_raw_flow_config
            else:
                raise Exception(f'Flow {flow_id} not found')

        keboola_client.storage_client.flow_detail = mocker.AsyncMock(side_effect=mock_get_flow)

        result = await list_flows(ctx=mcp_context_client, flow_ids=['21703284', 'nonexistent'])

        assert len(result.flows) == 1
        assert result.flows[0].id == '21703284'

    @pytest.mark.asyncio
    async def test_get_flow(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_raw_flow_config: Dict[str, Any]
    ):
        """Test getting detailed flow configuration."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(return_value=mock_raw_flow_config)

        result = await get_flow(ctx=mcp_context_client, configuration_id='21703284')

        assert isinstance(result, FlowConfiguration)
        assert len(result.phases) == 2
        assert len(result.tasks) == 2
        assert result.phases[0].name == 'Data Extraction'
        assert result.tasks[0].name == 'Extract AWS S3'

    @pytest.mark.asyncio
    async def test_update_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        sample_phases: List[Dict[str, Any]],
        sample_tasks: List[Dict[str, Any]],
        mock_raw_flow_config: Dict[str, Any],
        mock_project_id: str,
    ):
        """Test flow update."""
        mock_raw_flow_config['description'] = 'Updated description'
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_update = mocker.AsyncMock(return_value=mock_raw_flow_config)
        keboola_client.storage_client.project_id = mocker.AsyncMock(return_value=mock_project_id)

        result = await update_flow(
            ctx=mcp_context_client,
            configuration_id='21703284',
            name='Updated Flow',
            description='Updated description',
            phases=sample_phases,
            tasks=sample_tasks,
            change_description='Updated flow structure',
        )

        assert isinstance(result, FlowToolResponse)
        assert result.description == 'Updated description'
        assert result.timestamp == parser.isoparse('2025-05-25T06:33:41+0200')
        assert result.success is True
        assert len(result.links) == 3

        keboola_client.storage_client.flow_update.assert_called_once()
        call_args = keboola_client.storage_client.flow_update.call_args

        assert call_args.kwargs['config_id'] == '21703284'
        assert call_args.kwargs['name'] == 'Updated Flow'
        assert call_args.kwargs['description'] == 'Updated description'
        assert call_args.kwargs['change_description'] == 'Updated flow structure'

        flow_config = call_args.kwargs['flow_configuration']
        assert 'phases' in flow_config
        assert 'tasks' in flow_config


# --- Test Edge Cases ---


class TestFlowEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_create_flow_with_invalid_structure(self, mcp_context_client: Context):
        """Test flow creation with invalid structure."""
        invalid_phases = [{'name': 'Phase 1', 'dependsOn': [999]}]  # Invalid dependency
        invalid_tasks = [{'name': 'Task 1', 'phase': 1, 'task': {'componentId': 'comp1'}}]

        with pytest.raises(ValueError, match='depends on non-existent phase'):
            await create_flow(
                ctx=mcp_context_client,
                name='Invalid Flow',
                description='Invalid flow',
                phases=invalid_phases,
                tasks=invalid_tasks,
            )


# --- Integration-style Tests ---


@pytest.mark.asyncio
async def test_complete_flow_workflow(mocker: MockerFixture, mcp_context_client: Context):
    """Test a complete flow workflow: create, list, update, get."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

    created_flow = {
        'id': '123456',
        'name': 'Integration Test Flow',
        'description': 'Flow for integration testing',
        'version': 1,
        'configuration': {'phases': [], 'tasks': []},
        'isDisabled': False,
        'isDeleted': False,
        'created': '2025-05-28T12:00:00Z',
    }

    updated_flow = created_flow.copy()
    updated_flow['version'] = 2
    updated_flow['configuration'] = {
        'phases': [{'id': 1, 'name': 'Test Phase', 'dependsOn': []}],
        'tasks': [
            {
                'id': 20001,
                'name': 'Test Task',
                'phase': 1,
                'enabled': True,
                'continueOnFailure': False,
                'task': {'componentId': 'test.component', 'mode': 'run'},
            }
        ],
    }

    keboola_client.storage_client.flow_create = mocker.AsyncMock(return_value=created_flow)
    keboola_client.storage_client.flow_list = mocker.AsyncMock(return_value=[created_flow])
    keboola_client.storage_client.flow_update = mocker.AsyncMock(return_value=updated_flow)
    keboola_client.storage_client.flow_detail = mocker.AsyncMock(return_value=updated_flow)

    created = await create_flow(
        ctx=mcp_context_client,
        name='Integration Test Flow',
        description='Flow for integration testing',
        phases=[],
        tasks=[],
    )
    assert isinstance(created, FlowToolResponse)

    result = await list_flows(ctx=mcp_context_client)
    assert len(result.flows) == 1
    assert result.flows[0].name == 'Integration Test Flow'

    updated = await update_flow(
        ctx=mcp_context_client,
        configuration_id='123456',
        name='Integration Test Flow',
        description='Updated flow for integration testing',
        phases=[{'name': 'Test Phase'}],
        tasks=[{'name': 'Test Task', 'phase': 1, 'task': {'componentId': 'test.component'}}],
        change_description='Added test phase and task',
    )
    assert isinstance(updated, FlowToolResponse)

    detail = await get_flow(ctx=mcp_context_client, configuration_id='123456')
    assert isinstance(detail, FlowConfiguration)
    assert len(detail.phases) == 1
    assert len(detail.tasks) == 1
