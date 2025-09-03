"""Unit tests for Flow management tools."""

from typing import Any, Dict, List

import httpx
import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.tools.flow.model import (
    ConditionalFlowPhase,
    ConditionalFlowTask,
    Flow,
    FlowPhase,
    FlowSummary,
    FlowTask,
    ListFlowsOutput,
)
from keboola_mcp_server.tools.flow.tools import (
    FlowToolResponse,
    create_conditional_flow,
    create_flow,
    get_flow,
    get_flow_examples,
    get_flow_schema,
    list_flows,
    update_flow,
)

# =============================================================================
# FLOW DATA FIXTURES
# =============================================================================


@pytest.fixture
def legacy_flow_phases() -> List[Dict[str, Any]]:
    """Sample legacy flow phases."""
    return [
        {'id': 1, 'name': 'Data Extraction', 'description': 'Extract data from various sources', 'dependsOn': []},
        {'id': 2, 'name': 'Data Transformation', 'description': 'Transform and process data', 'dependsOn': [1]},
        {'id': 3, 'name': 'Data Loading', 'description': 'Load data to destination', 'dependsOn': [2]},
    ]


@pytest.fixture
def legacy_flow_tasks() -> List[Dict[str, Any]]:
    """Sample legacy flow tasks."""
    return [
        {
            'id': 20001,
            'name': 'Extract from S3',
            'phase': 1,
            'enabled': True,
            'continueOnFailure': False,
            'task': {'componentId': 'keboola.ex-aws-s3', 'configId': '123456', 'mode': 'run'},
        },
        {
            'id': 20002,
            'name': 'Transform Data',
            'phase': 2,
            'enabled': True,
            'continueOnFailure': False,
            'task': {'componentId': 'keboola.snowflake-transformation', 'configId': '789012', 'mode': 'run'},
        },
        {
            'id': 20003,
            'name': 'Load to Warehouse',
            'phase': 3,
            'enabled': True,
            'continueOnFailure': False,
            'task': {'componentId': 'keboola.wr-snowflake', 'configId': '345678', 'mode': 'run'},
        },
    ]


@pytest.fixture
def mock_conditional_flow_phases() -> List[Dict[str, Any]]:
    """Sample conditional flow phases with simple configuration."""
    return [
        {
            'id': 'phase1',
            'name': 'Simple Phase',
            'description': 'A simple conditional flow phase',
            'next': [{'id': 'transition1', 'name': 'Simple Transition', 'goto': None}],
        }
    ]


@pytest.fixture
def mock_conditional_flow_tasks() -> List[Dict[str, Any]]:
    """Sample conditional flow tasks with simple configuration."""
    return [
        {
            'id': 'task1',
            'name': 'Simple Task',
            'phase': 'phase1',
            'enabled': True,
            'task': {
                'type': 'notification',
                'recipients': [{'channel': 'email', 'address': 'admin@company.com'}],
                'title': 'Simple Notification',
                'message': 'This is a simple notification task',
            },
        }
    ]


@pytest.fixture
def mock_conditional_flow(
    mock_conditional_flow_phases: List[Dict[str, Any]], mock_conditional_flow_tasks: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Mock conditional flow configuration response for get_flow endpoint."""
    return {
        'component_id': CONDITIONAL_FLOW_COMPONENT_ID,
        'configuration_id': 'conditional_flow_456',
        'name': 'Advanced Data Pipeline',
        'description': 'Advanced pipeline with conditional logic and error handling',
        'created': '2025-01-15T11:00:00Z',
        'updated': '2025-01-15T11:00:00Z',
        'creatorToken': {'id': 'test_token', 'description': 'Test token'},
        'version': 1,
        'changeDescription': 'Initial creation',
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {'phases': mock_conditional_flow_phases, 'tasks': mock_conditional_flow_tasks},
        'rows': [],
        'metadata': [],
    }


@pytest.fixture
def mock_conditional_flow_create_update(
    mock_conditional_flow_phases: List[Dict[str, Any]], mock_conditional_flow_tasks: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Mock conditional flow configuration response for create/update endpoints."""
    return {
        'id': 'conditional_flow_456',
        'name': 'Advanced Data Pipeline',
        'description': 'Advanced pipeline with conditional logic and error handling',
        'created': '2025-01-15T11:00:00Z',
        'creatorToken': {'id': 'test_token', 'description': 'Test token'},
        'version': 1,
        'changeDescription': 'Initial creation',
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {'phases': mock_conditional_flow_phases, 'tasks': mock_conditional_flow_tasks},
        'state': {},
        'currentVersion': {'version': 1},
    }


@pytest.fixture
def mock_legacy_flow_create_update(
    legacy_flow_phases: list[dict[str, Any]], legacy_flow_tasks: list[dict[str, Any]]
) -> dict[str, Any]:
    """Mock legacy flow configuration response for create/update endpoints."""
    return {
        'id': 'legacy_flow_123',
        'name': 'Legacy ETL Pipeline',
        'description': 'Traditional ETL pipeline using legacy flows',
        'created': '2025-01-15T10:30:00Z',
        'creatorToken': {'id': 'test_token', 'description': 'Test token'},
        'version': 1,
        'changeDescription': 'Initial creation',
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {'phases': legacy_flow_phases, 'tasks': legacy_flow_tasks},
        'state': {},
        'currentVersion': {'version': 1},
    }


@pytest.fixture
def mock_legacy_flow(
    legacy_flow_phases: list[dict[str, Any]], legacy_flow_tasks: list[dict[str, Any]]
) -> dict[str, Any]:
    """Mock legacy flow configuration response for get_flow endpoint."""
    return {
        'component_id': ORCHESTRATOR_COMPONENT_ID,
        'configuration_id': 'legacy_flow_123',
        'name': 'Legacy ETL Pipeline',
        'description': 'Traditional ETL pipeline using legacy flows',
        'created': '2025-01-15T10:30:00Z',
        'updated': '2025-01-15T10:30:00Z',
        'creatorToken': {'id': 'test_token', 'description': 'Test token'},
        'version': 1,
        'changeDescription': 'Initial creation',
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {'phases': legacy_flow_phases, 'tasks': legacy_flow_tasks},
        'rows': [],
        'metadata': [],
    }


# =============================================================================
# CREATE_FLOW TOOL TESTS
# =============================================================================


class TestCreateFlowTool:
    """Tests for the create_flow tool."""

    @pytest.mark.asyncio
    async def test_create_legacy_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        legacy_flow_phases: list[dict[str, Any]],
        legacy_flow_tasks: list[dict[str, Any]],
        mock_legacy_flow_create_update: dict[str, Any],
    ):
        """Should create a new legacy (orchestrator) flow with valid phases/tasks."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        mocker.patch.object(
            keboola_client.storage_client,
            'configuration_create',
            return_value=mock_legacy_flow_create_update,
        )

        result = await create_flow(
            ctx=mcp_context_client,
            name='Legacy ETL Pipeline',
            description='Traditional ETL pipeline using legacy flows',
            phases=legacy_flow_phases,
            tasks=legacy_flow_tasks,
        )

        assert isinstance(result, FlowToolResponse)
        assert result.success is True
        assert result.id == mock_legacy_flow_create_update['id']
        assert result.description == mock_legacy_flow_create_update['description']
        assert result.timestamp is not None
        assert len(result.links) == 3
        assert result.version == mock_legacy_flow_create_update['version']

        keboola_client.storage_client.configuration_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_conditional_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow_create_update: Dict[str, Any],
    ):
        """Test conditional flow creation."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.configuration_create = mocker.AsyncMock(
            return_value=mock_conditional_flow_create_update
        )

        result = await create_conditional_flow(
            ctx=mcp_context_client,
            name='Advanced Data Pipeline',
            description='Advanced pipeline with conditional logic and error handling',
            phases=mock_conditional_flow_create_update['configuration']['phases'],
            tasks=mock_conditional_flow_create_update['configuration']['tasks'],
        )

        assert isinstance(result, FlowToolResponse)
        assert result.success is True
        assert result.id == mock_conditional_flow_create_update['id']
        assert result.description == mock_conditional_flow_create_update['description']
        assert result.timestamp is not None
        assert len(result.links) == 3
        assert result.version == mock_conditional_flow_create_update['version']

        keboola_client.storage_client.configuration_create.assert_called_once()


# =============================================================================
# UPDATE_FLOW TOOL TESTS
# =============================================================================


class TestUpdateFlowTool:
    """Tests for the update_flow tool."""

    # TODO: The test_update_*() tests need to cover different variations of the tool's parameters
    #  and properly check that the original flow was correctly updated.

    @pytest.mark.asyncio
    async def test_update_legacy_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        legacy_flow_phases: List[Dict[str, Any]],
        legacy_flow_tasks: List[Dict[str, Any]],
        mock_legacy_flow_create_update: Dict[str, Any],
    ):
        """Test legacy flow update with new phases and tasks."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = True

        async def mock_get_project_info(ctx):
            return mock_project_info

        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', side_effect=mock_get_project_info)

        updated_config = mock_legacy_flow_create_update.copy()
        updated_config['version'] = 2
        updated_config['description'] = 'Updated legacy ETL pipeline'

        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value={})
        keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value=updated_config)

        result = await update_flow(
            ctx=mcp_context_client,
            configuration_id='legacy_flow_123',
            flow_type=ORCHESTRATOR_COMPONENT_ID,
            name='Updated Legacy ETL Pipeline',
            description='Updated legacy ETL pipeline',
            phases=legacy_flow_phases,
            tasks=legacy_flow_tasks,
            change_description='Added data validation phase and enhanced error handling',
        )

        assert isinstance(result, FlowToolResponse)
        assert result.success is True
        assert result.id == 'legacy_flow_123'
        assert result.description == 'Updated legacy ETL pipeline'
        assert result.timestamp is not None
        assert len(result.links) == 3
        assert result.version == updated_config['version']

        keboola_client.storage_client.configuration_update.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_conditional_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow_create_update: Dict[str, Any],
    ):
        """Test conditional flow update with enhanced conditions."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = True

        async def mock_get_project_info(ctx):
            return mock_project_info

        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', side_effect=mock_get_project_info)

        updated_config = mock_conditional_flow_create_update.copy()
        updated_config['version'] = 2
        updated_config['name'] = 'Enhanced Advanced Data Pipeline'
        updated_config['description'] = 'Enhanced pipeline with improved conditional logic'

        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value={})
        keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value=updated_config)

        result = await update_flow(
            ctx=mcp_context_client,
            configuration_id='conditional_flow_456',
            flow_type=CONDITIONAL_FLOW_COMPONENT_ID,
            name='Enhanced Advanced Data Pipeline',
            description='Enhanced pipeline with improved conditional logic',
            phases=mock_conditional_flow_create_update['configuration']['phases'],
            tasks=mock_conditional_flow_create_update['configuration']['tasks'],
            change_description='Enhanced error handling and added notification phase',
        )

        assert isinstance(result, FlowToolResponse)
        assert result.success is True
        assert result.id == updated_config['id']
        assert result.description == updated_config['description']
        assert result.timestamp is not None
        assert len(result.links) == 3
        assert result.version == updated_config['version']

        keboola_client.storage_client.configuration_update.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_conditional_flow_fails_when_conditional_flows_disabled(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test that updating conditional flow fails when conditional flows are disabled."""
        # Mock project info with conditional flows disabled
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = False
        mock_project_info.project_name = 'Test Project'

        async def mock_get_project_info(ctx):
            return mock_project_info

        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', side_effect=mock_get_project_info)

        # Should raise ValueError with proper error message
        with pytest.raises(ValueError, match='Conditional flows are not supported.') as exc_info:
            await update_flow(
                ctx=mcp_context_client,
                configuration_id='test-config-id',
                flow_type=CONDITIONAL_FLOW_COMPONENT_ID,
                name='Updated Conditional Flow',
                description='Updated description for conditional flow',
                phases=[],
                tasks=[],
                change_description='Test update',
            )

        error_message = str(exc_info.value)
        assert 'Conditional flows are not supported in this project' in error_message
        assert 'Test Project' in error_message
        assert 'conditional_flows=false' in error_message
        assert 'enable them in your project settings' in error_message


# =============================================================================
# GET_FLOW TOOL TESTS
# =============================================================================


class TestGetFlowTool:
    """Tests for the get_flow tool."""

    @pytest.mark.asyncio
    async def test_get_legacy_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_legacy_flow: dict[str, Any],
    ):
        """Should fall back to legacy flow when conditional flow is missing (404)."""

        async def mock_configuration_detail(component_id: str, configuration_id: str) -> dict[str, Any]:
            if component_id == CONDITIONAL_FLOW_COMPONENT_ID:
                response = mocker.Mock(status_code=404)
                raise httpx.HTTPStatusError('404 Not Found', request=None, response=response)
            if component_id == ORCHESTRATOR_COMPONENT_ID:
                return mock_legacy_flow
            raise ValueError(f'Unexpected component_id: {component_id}')

        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        mocker.patch.object(
            keboola_client.storage_client, 'configuration_detail', side_effect=mock_configuration_detail
        )

        result = await get_flow(
            ctx=mcp_context_client,
            configuration_id='legacy_flow_123',
        )

        assert isinstance(result, Flow)
        assert result.configuration_id == mock_legacy_flow['configuration_id']
        assert result.name == mock_legacy_flow['name']
        assert result.description == mock_legacy_flow['description']
        assert result.created == mock_legacy_flow['created']
        assert result.updated == mock_legacy_flow['updated']
        assert result.version == mock_legacy_flow['version']
        assert result.is_disabled == mock_legacy_flow['isDisabled']
        assert result.is_deleted == mock_legacy_flow['isDeleted']
        assert result.configuration.phases == [
            FlowPhase.model_validate(phase) for phase in mock_legacy_flow['configuration']['phases']
        ]
        assert result.configuration.tasks == [
            FlowTask.model_validate(task) for task in mock_legacy_flow['configuration']['tasks']
        ]
        assert len(result.links) == 3

    @pytest.mark.asyncio
    async def test_get_conditional_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow: Dict[str, Any],
    ):
        """Test retrieving conditional flow details."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value=mock_conditional_flow)

        result = await get_flow(ctx=mcp_context_client, configuration_id='conditional_flow_456')

        assert isinstance(result, Flow)
        assert result.component_id == mock_conditional_flow['component_id']
        assert result.configuration_id == mock_conditional_flow['configuration_id']
        assert result.name == mock_conditional_flow['name']
        assert result.description == mock_conditional_flow['description']
        assert result.created == mock_conditional_flow['created']
        assert result.updated == mock_conditional_flow['updated']
        assert result.version == mock_conditional_flow['version']
        assert result.is_disabled == mock_conditional_flow['isDisabled']
        assert result.is_deleted == mock_conditional_flow['isDeleted']
        assert result.configuration.phases == [
            ConditionalFlowPhase.model_validate(phase) for phase in mock_conditional_flow['configuration']['phases']
        ]
        assert result.configuration.tasks == [
            ConditionalFlowTask.model_validate(task) for task in mock_conditional_flow['configuration']['tasks']
        ]
        assert len(result.links) == 3


# =============================================================================
# LIST_FLOWS TOOL TESTS
# =============================================================================


class TestListFlowsTool:
    """Tests for the list_flows tool."""

    @pytest.mark.asyncio
    async def test_list_flows_no_params(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_legacy_flow: Dict[str, Any],
        mock_conditional_flow: Dict[str, Any],
    ):
        """Test listing flows of both types with comprehensive validation."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock different responses for different flow types
        def mock_configuration_list(component_id):
            if component_id == ORCHESTRATOR_COMPONENT_ID:
                return [mock_legacy_flow]
            elif component_id == CONDITIONAL_FLOW_COMPONENT_ID:
                return [mock_conditional_flow]
            return []

        keboola_client.storage_client.configuration_list = mocker.AsyncMock(side_effect=mock_configuration_list)

        result = await list_flows(ctx=mcp_context_client)

        assert isinstance(result, ListFlowsOutput)
        assert len(result.flows) == 2

        # Verify both flow types are present
        flow_types = {flow.component_id for flow in result.flows}
        assert ORCHESTRATOR_COMPONENT_ID in flow_types
        assert CONDITIONAL_FLOW_COMPONENT_ID in flow_types

        # Extract and validate individual flows
        legacy_flow = next(f for f in result.flows if f.component_id == ORCHESTRATOR_COMPONENT_ID)
        conditional_flow = next(f for f in result.flows if f.component_id == CONDITIONAL_FLOW_COMPONENT_ID)

        # Validate legacy flow summary structure
        assert isinstance(legacy_flow, FlowSummary)
        assert legacy_flow.component_id == ORCHESTRATOR_COMPONENT_ID
        assert legacy_flow.configuration_id == mock_legacy_flow['configuration_id']
        assert legacy_flow.name == mock_legacy_flow['name']
        assert legacy_flow.description == mock_legacy_flow['description']
        assert legacy_flow.created == mock_legacy_flow['created']
        assert legacy_flow.updated == mock_legacy_flow['updated']
        assert legacy_flow.version == mock_legacy_flow['version']
        assert legacy_flow.is_disabled == mock_legacy_flow['isDisabled']
        assert legacy_flow.is_deleted == mock_legacy_flow['isDeleted']

        # Validate conditional flow summary structure
        assert isinstance(conditional_flow, FlowSummary)
        assert conditional_flow.component_id == CONDITIONAL_FLOW_COMPONENT_ID
        assert conditional_flow.configuration_id == mock_conditional_flow['configuration_id']
        assert conditional_flow.name == mock_conditional_flow['name']
        assert conditional_flow.description == mock_conditional_flow['description']
        assert conditional_flow.created == mock_conditional_flow['created']
        assert conditional_flow.updated == mock_conditional_flow['updated']
        assert conditional_flow.version == mock_conditional_flow['version']
        assert conditional_flow.is_disabled == mock_conditional_flow['isDisabled']
        assert conditional_flow.is_deleted == mock_conditional_flow['isDeleted']

        assert keboola_client.storage_client.configuration_list.call_count == 2

    @pytest.mark.asyncio
    async def test_list_flows_specific_ids_mixed_types(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_legacy_flow: Dict[str, Any],
        mock_conditional_flow: Dict[str, Any],
    ):
        """Test listing specific flows by ID when they're different types with comprehensive validation."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        def mock_configuration_detail(component_id, configuration_id):
            if configuration_id == 'legacy_flow_123' and component_id == ORCHESTRATOR_COMPONENT_ID:
                return mock_legacy_flow
            elif configuration_id == 'conditional_flow_456' and component_id == CONDITIONAL_FLOW_COMPONENT_ID:
                return mock_conditional_flow
            raise Exception(f'Configuration {configuration_id} not found')

        keboola_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_configuration_detail)

        result = await list_flows(ctx=mcp_context_client, flow_ids=['legacy_flow_123', 'conditional_flow_456'])

        # Validate the overall response structure
        assert isinstance(result, ListFlowsOutput)
        assert len(result.flows) == 2

        # Verify both flow types are present
        flow_types = {flow.component_id for flow in result.flows}
        assert ORCHESTRATOR_COMPONENT_ID in flow_types
        assert CONDITIONAL_FLOW_COMPONENT_ID in flow_types

        # Extract and validate individual flows
        legacy_flow = next(f for f in result.flows if f.component_id == ORCHESTRATOR_COMPONENT_ID)
        conditional_flow = next(f for f in result.flows if f.component_id == CONDITIONAL_FLOW_COMPONENT_ID)

        # Validate legacy flow summary structure
        assert isinstance(legacy_flow, FlowSummary)
        assert legacy_flow.component_id == ORCHESTRATOR_COMPONENT_ID
        assert legacy_flow.configuration_id == mock_legacy_flow['configuration_id']
        assert legacy_flow.name == mock_legacy_flow['name']
        assert legacy_flow.description == mock_legacy_flow['description']
        assert legacy_flow.created == mock_legacy_flow['created']
        assert legacy_flow.updated == mock_legacy_flow['updated']
        assert legacy_flow.version == mock_legacy_flow['version']
        assert legacy_flow.is_disabled == mock_legacy_flow['isDisabled']
        assert legacy_flow.is_deleted == mock_legacy_flow['isDeleted']

        # Validate conditional flow summary structure
        assert isinstance(conditional_flow, FlowSummary)
        assert conditional_flow.component_id == CONDITIONAL_FLOW_COMPONENT_ID
        assert conditional_flow.configuration_id == mock_conditional_flow['configuration_id']
        assert conditional_flow.name == mock_conditional_flow['name']
        assert conditional_flow.description == mock_conditional_flow['description']
        assert conditional_flow.created == mock_conditional_flow['created']
        assert conditional_flow.updated == mock_conditional_flow['updated']
        assert conditional_flow.version == mock_conditional_flow['version']
        assert conditional_flow.is_disabled == mock_conditional_flow['isDisabled']
        assert conditional_flow.is_deleted == mock_conditional_flow['isDeleted']

        # Since we look up for both types (conditional flows first) we expect the calls to be 2 and 1, respectfully
        assert keboola_client.storage_client.configuration_detail.call_count == 3


# =============================================================================
# GET_FLOW_SCHEMA TOOL TESTS
# =============================================================================


class TestGetFlowSchemaTool:
    """Tests for the get_flow_schema tool."""

    @pytest.mark.asyncio
    async def test_get_legacy_flow_schema_when_conditional_flows_disabled(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting schema for legacy flow type when conditional flows are disabled."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = False
        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', return_value=mock_project_info)

        result = await get_flow_schema(ctx=mcp_context_client, flow_type=ORCHESTRATOR_COMPONENT_ID)

        assert isinstance(result, str)
        assert '```json' in result
        assert 'dependsOn' in result

    @pytest.mark.asyncio
    async def test_get_legacy_flow_schema_when_conditional_flows_enabled(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting schema for legacy flow type when conditional flows are enabled."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = True
        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', return_value=mock_project_info)

        result = await get_flow_schema(ctx=mcp_context_client, flow_type=ORCHESTRATOR_COMPONENT_ID)

        assert isinstance(result, str)
        assert '```json' in result
        assert 'dependsOn' in result

    @pytest.mark.asyncio
    async def test_get_conditional_flow_schema_when_conditional_flows_enabled(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting schema for conditional flow type when conditional flows are enabled."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = True
        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', return_value=mock_project_info)

        result = await get_flow_schema(ctx=mcp_context_client, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

        assert isinstance(result, str)
        assert '```json' in result
        assert 'keboola.flow' in result or 'conditional' in result.lower()
        assert 'next' in result

    @pytest.mark.asyncio
    async def test_get_conditional_flow_schema_fails_when_conditional_flows_disabled(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test that requesting conditional flow schema fails when conditional flows are disabled."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = False
        mock_project_info.project_name = 'Test Project'

        async def mock_get_project_info(ctx):
            return mock_project_info

        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', side_effect=mock_get_project_info)

        # Should raise ValueError with proper error message
        with pytest.raises(ValueError, match='Conditional flows are not supported.') as exc_info:
            await get_flow_schema(ctx=mcp_context_client, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

        error_message = str(exc_info.value)
        assert 'Conditional flows are not supported in this project' in error_message
        assert 'Test Project' in error_message
        assert 'conditional_flows=false' in error_message
        assert 'enable them in your project settings' in error_message


# =============================================================================
# GET_FLOW_EXAMPLES TOOL TESTS
# =============================================================================


class TestGetFlowExamplesTool:
    """Tests for the get_flow_examples tool."""

    @pytest.mark.asyncio
    async def test_get_legacy_flow_examples(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting examples for legacy flow type."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = True

        async def mock_get_project_info(ctx):
            return mock_project_info

        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', side_effect=mock_get_project_info)

        # Mock the file path and content properly - using actual structure from the real file
        mock_file_content = [
            (
                '{"tasks":[{"id":1,"name":"keboola.wr-google-bigquery-v2-28356142",'
                '"task":{"mode":"run","configId":"28356142","componentId":"keboola.wr-google-bigquery-v2"},'
                '"phase":1,"continueOnFailure":false,"enabled":true}],'
                '"phases":[{"id":1,"name":"Scheduledconfiguration","dependsOn":[]}]}'
            ),
            (
                '{"phases":[{"id":59812,"name":"Extraction","dependsOn":[],'
                '"description":"ExtractdatafromWhenIworkandPaychex"}],'
                '"tasks":[{"id":36614,"name":"ex-generic-v2-34446855","phase":59812,'
                '"task":{"componentId":"ex-generic-v2","configId":"34446855","mode":"run"},'
                '"continueOnFailure":false,"enabled":false}]}'
            ),
        ]

        # Mock the file path resolution
        mock_path = mocker.Mock()
        mock_path.__truediv__ = mocker.Mock(return_value=mock_path)
        mock_path.open = mocker.mock_open(read_data='\n'.join(mock_file_content))

        # Mock the importlib.resources.files function
        mocker.patch('importlib.resources.files', return_value=mock_path)

        result = await get_flow_examples(ctx=mcp_context_client, flow_type=ORCHESTRATOR_COMPONENT_ID)

        assert isinstance(result, str)
        assert 'Flow Configuration Examples for `keboola.orchestrator`' in result
        assert 'keboola.wr-google-bigquery-v2-28356142' in result
        assert 'ex-generic-v2-34446855' in result
        assert 'Scheduledconfiguration' in result
        assert 'Extraction' in result

    @pytest.mark.asyncio
    async def test_get_conditional_flow_examples(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting examples for conditional flow type."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = True

        async def mock_get_project_info(ctx):
            return mock_project_info

        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', side_effect=mock_get_project_info)

        # Mock the file path and content properly - using actual structure from the real file
        mock_file_content = [
            (
                '{"tasks":[{"id":"40fef978-7092-4d79-a5b4-ea3fb2e38d03",'
                '"name":"keboola.wr-azure-event-hub-92021091",'
                '"phase":"6afbf55b-782c-47d7-bf70-f0ef1be6505b",'
                '"task":{"type":"job","mode":"run","componentId":"keboola.wr-azure-event-hub","configId":"92021091"},'
                '"enabled":true}],'
                '"phases":[{"id":"7dd992b0-9ac5-495b-b277-d8bc0b7e15d5",'
                '"name":"Phase1",'
                '"next":[{"id":"a25a4e4a-3042-49a2-81d8-fb1103957ebe",'
                '"goto":"6afbf55b-782c-47d7-bf70-f0ef1be6505b"}]}]}'
            ),
            (
                '{"tasks":[{"id":"6bcc72d8-d9a5-4708-b0bd-53c4f6e839f7",'
                '"name":"keboola.python-transformation-v2-16550",'
                '"phase":"78c07164-0d1c-41d6-ba48-b821e781d830",'
                '"task":{"type":"job","mode":"run",'
                '"componentId":"keboola.python-transformation-v2","configId":"16550"},'
                '"enabled":true}],'
                '"phases":[{"id":"78c07164-0d1c-41d6-ba48-b821e781d830",'
                '"name":"Phase1",'
                '"next":[{"id":"e5dc7c43-d311-4e90-a2ca-6cac8d2eb5f5",'
                '"goto":"92649482-45d6-475d-aace-33466f37e381"}]}]}'
            ),
        ]

        # Mock the file path resolution
        mock_path = mocker.Mock()
        mock_path.__truediv__ = mocker.Mock(return_value=mock_path)
        mock_path.open = mocker.mock_open(read_data='\n'.join(mock_file_content))

        # Mock the importlib.resources.files function
        mocker.patch('importlib.resources.files', return_value=mock_path)

        result = await get_flow_examples(ctx=mcp_context_client, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

        assert isinstance(result, str)
        assert 'Flow Configuration Examples for `keboola.flow`' in result
        assert 'keboola.wr-azure-event-hub-92021091' in result
        assert 'keboola.python-transformation-v2-16550' in result
        assert 'Phase1' in result

    @pytest.mark.asyncio
    async def test_get_conditional_flow_examples_when_conditional_flows_disabled(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test that requesting conditional flow examples fails when conditional flows are disabled."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = False
        mock_project_info.project_name = 'Test Project'

        async def mock_get_project_info(ctx):
            return mock_project_info

        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', side_effect=mock_get_project_info)

        # Mock the file path resolution (should not be called due to early failure)
        mock_path = mocker.Mock()
        mock_path.__truediv__ = mocker.Mock(return_value=mock_path)
        mock_path.open = mocker.mock_open()
        mocker.patch('importlib.resources.files', return_value=mock_path)

        # Should raise ValueError with proper error message
        with pytest.raises(ValueError, match='Conditional flows are not supported.') as exc_info:
            await get_flow_examples(ctx=mcp_context_client, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        error_message = str(exc_info.value)
        assert 'Conditional flows are not supported in this project' in error_message
        assert 'Test Project' in error_message
        assert 'conditional_flows=false' in error_message
        assert 'enable them in your project settings' in error_message
