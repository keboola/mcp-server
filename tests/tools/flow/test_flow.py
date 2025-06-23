"""Unit tests for Flow management tools."""

from typing import Any, Dict, List

import pytest
from dateutil import parser
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.tools.flow.model import (
    FlowConfiguration,
    FlowConfigurationResponse,
    FlowPhase,
    FlowTask,
    ReducedFlow,
)
from keboola_mcp_server.tools.flow.tools import (
    FlowToolResponse,
    create_flow,
    get_flow_detail,
    retrieve_flows,
    update_flow,
)
from keboola_mcp_server.tools.flow.utils import (
    _check_circular_dependencies,
    ensure_phase_ids,
    ensure_task_ids,
    validate_flow_structure,
)

# --- Fixtures ---


@pytest.fixture
def mock_project_id() -> str:
    """Mocks a project id."""
    return '1'


@pytest.fixture
def mock_raw_flow_config() -> Dict[str, Any]:
    """Mock raw flow configuration as returned by Keboola API."""
    return {
        'id': '21703284',
        'name': 'Test Flow',
        'description': 'Test flow description',
        'version': 1,
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {
            'phases': [
                {'id': 1, 'name': 'Data Extraction', 'description': 'Extract data from sources', 'dependsOn': []},
                {'id': 2, 'name': 'Data Processing', 'description': 'Process extracted data', 'dependsOn': [1]},
            ],
            'tasks': [
                {
                    'id': 20001,
                    'name': 'Extract AWS S3',
                    'phase': 1,
                    'enabled': True,
                    'continueOnFailure': False,
                    'task': {'componentId': 'keboola.ex-aws-s3', 'configId': '12345', 'mode': 'run'},
                },
                {
                    'id': 20002,
                    'name': 'Process Data',
                    'phase': 2,
                    'enabled': True,
                    'continueOnFailure': False,
                    'task': {'componentId': 'keboola.snowflake-transformation', 'configId': '67890', 'mode': 'run'},
                },
            ],
        },
        'changeDescription': 'Initial creation',
        'metadata': [],
        'created': '2025-05-25T06:33:41+0200',
    }


@pytest.fixture
def mock_empty_flow_config() -> Dict[str, Any]:
    """Mock empty flow configuration."""
    return {
        'id': '21703285',
        'name': 'Empty Flow',
        'description': 'Empty test flow',
        'version': 1,
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {'phases': [], 'tasks': []},
        'changeDescription': None,
        'metadata': [],
        'created': '2025-05-25T07:00:00+0200',
    }


@pytest.fixture
def sample_phases() -> List[Dict[str, Any]]:
    """Sample phase definitions for testing."""
    return [
        {'name': 'Data Extraction', 'dependsOn': [], 'description': 'Extract data'},
        {'name': 'Data Processing', 'dependsOn': [1], 'description': 'Process data'},
        {'name': 'Data Output', 'dependsOn': [2], 'description': 'Output processed data'},
    ]


@pytest.fixture
def sample_tasks() -> List[Dict[str, Any]]:
    """Sample task definitions for testing."""
    return [
        {'name': 'Extract from S3', 'phase': 1, 'task': {'componentId': 'keboola.ex-aws-s3', 'configId': '12345'}},
        {
            'name': 'Transform Data',
            'phase': 2,
            'task': {'componentId': 'keboola.snowflake-transformation', 'configId': '67890'},
        },
        {
            'name': 'Export to BigQuery',
            'phase': 3,
            'task': {'componentId': 'keboola.wr-google-bigquery-v2', 'configId': '11111'},
        },
    ]


# --- Test Model Parsing ---


class TestFlowModels:
    """Test Flow Pydantic models."""

    def test_flow_configuration_response_model_validate(self, mock_raw_flow_config: Dict[str, Any]):
        """Test parsing raw API response into FlowConfigurationResponse without component_id of Orchestrator."""
        flow_response = FlowConfigurationResponse.model_validate(mock_raw_flow_config)

        assert flow_response.component_id == ORCHESTRATOR_COMPONENT_ID
        assert flow_response.configuration_id == mock_raw_flow_config['id']
        assert flow_response.configuration_name == mock_raw_flow_config['name']
        assert flow_response.configuration_description == mock_raw_flow_config['description']
        assert flow_response.version == mock_raw_flow_config['version']
        assert flow_response.is_disabled is mock_raw_flow_config['isDisabled']
        assert flow_response.is_deleted is mock_raw_flow_config['isDeleted']

        config = flow_response.configuration
        assert isinstance(config, FlowConfiguration)
        assert len(config.phases) == 2
        assert len(config.tasks) == 2

        phase1 = config.phases[0]
        assert isinstance(phase1, FlowPhase)
        assert phase1.id == 1
        assert phase1.name == 'Data Extraction'
        assert phase1.depends_on == []

        phase2 = config.phases[1]
        assert phase2.id == 2
        assert phase2.depends_on == [1]

        task1 = config.tasks[0]
        assert isinstance(task1, FlowTask)
        assert task1.id == 20001
        assert task1.name == 'Extract AWS S3'
        assert task1.phase == 1
        assert task1.task['componentId'] == 'keboola.ex-aws-s3'

    def test_reduced_flow_model_validate(self, mock_raw_flow_config: Dict[str, Any]):
        """Test parsing raw API response into ReducedFlow."""
        reduced_flow = ReducedFlow.model_validate(mock_raw_flow_config)

        assert reduced_flow.id == mock_raw_flow_config['id']
        assert reduced_flow.name == mock_raw_flow_config['name']
        assert reduced_flow.description == mock_raw_flow_config['description']
        assert reduced_flow.version == mock_raw_flow_config['version']
        assert reduced_flow.phases_count == 2
        assert reduced_flow.tasks_count == 2
        assert reduced_flow.is_disabled is mock_raw_flow_config['isDisabled']
        assert reduced_flow.is_deleted is mock_raw_flow_config['isDeleted']

    def test_empty_flow_parsing(self, mock_empty_flow_config):
        """Test parsing empty flow configuration."""
        flow_response = FlowConfigurationResponse.model_validate(mock_empty_flow_config)
        reduced_flow = ReducedFlow.model_validate(mock_empty_flow_config)

        assert len(flow_response.configuration.phases) == 0
        assert len(flow_response.configuration.tasks) == 0
        assert reduced_flow.phases_count == 0
        assert reduced_flow.tasks_count == 0


# --- Test Helper Functions ---


class TestFlowHelpers:
    """Test helper functions for flow processing."""

    def test_ensure_phase_ids_with_missing_ids(self):
        """Test phase ID generation when IDs are missing."""
        phases = [{'name': 'Phase 1'}, {'name': 'Phase 2', 'dependsOn': [1]}, {'id': 5, 'name': 'Phase 5'}]

        processed_phases = ensure_phase_ids(phases)

        assert len(processed_phases) == 3
        assert processed_phases[0].id == 1
        assert processed_phases[0].name == 'Phase 1'
        assert processed_phases[1].id == 2
        assert processed_phases[1].name == 'Phase 2'
        assert processed_phases[2].id == 5
        assert processed_phases[2].name == 'Phase 5'

    def test_ensure_phase_ids_with_existing_ids(self):
        """Test phase processing when IDs already exist."""
        phases = [
            {'id': 10, 'name': 'Custom Phase 1'},
            {'id': 'string-id', 'name': 'Custom Phase 2', 'dependsOn': [10]},
        ]

        processed_phases = ensure_phase_ids(phases)

        assert len(processed_phases) == 2
        assert processed_phases[0].id == 10
        assert processed_phases[1].id == 'string-id'
        assert processed_phases[1].depends_on == [10]

    def test_ensure_task_ids_with_missing_ids(self):
        """Test task ID generation using 20001+ pattern."""
        tasks = [
            {'name': 'Task 1', 'phase': 1, 'task': {'componentId': 'comp1'}},
            {'name': 'Task 2', 'phase': 2, 'task': {'componentId': 'comp2'}},
            {'id': 30000, 'name': 'Task 3', 'phase': 3, 'task': {'componentId': 'comp3'}},
        ]

        processed_tasks = ensure_task_ids(tasks)

        assert len(processed_tasks) == 3
        assert processed_tasks[0].id == 20001
        assert processed_tasks[1].id == 20002
        assert processed_tasks[2].id == 30000

    def test_ensure_task_ids_adds_default_mode(self):
        """Test that default mode 'run' is added to tasks."""
        tasks = [
            {'name': 'Task 1', 'phase': 1, 'task': {'componentId': 'comp1'}},
            {'name': 'Task 2', 'phase': 1, 'task': {'componentId': 'comp2', 'mode': 'debug'}},
        ]

        processed_tasks = ensure_task_ids(tasks)

        assert processed_tasks[0].task['mode'] == 'run'  # Default added
        assert processed_tasks[1].task['mode'] == 'debug'  # Existing preserved

    def test_ensure_task_ids_validates_required_fields(self):
        """Test validation of required task fields."""
        with pytest.raises(ValueError, match="missing 'task' configuration"):
            ensure_task_ids([{'name': 'Bad Task', 'phase': 1}])

        with pytest.raises(ValueError, match='missing componentId'):
            ensure_task_ids([{'name': 'Bad Task', 'phase': 1, 'task': {}}])

    def test_validate_flow_structure_success(self, sample_phases, sample_tasks):
        """Test successful flow structure validation."""
        phases = ensure_phase_ids(sample_phases)
        tasks = ensure_task_ids(sample_tasks)

        validate_flow_structure(phases, tasks)

    def test_validate_flow_structure_invalid_phase_dependency(self):
        """Test validation failure for invalid phase dependencies."""
        phases = ensure_phase_ids([{'id': 1, 'name': 'Phase 1', 'dependsOn': [999]}])  # Non-existent phase
        tasks = []

        with pytest.raises(ValueError, match='depends on non-existent phase 999'):
            validate_flow_structure(phases, tasks)

    def test_validate_flow_structure_invalid_task_phase(self):
        """Test validation failure for task referencing non-existent phase."""
        phases = ensure_phase_ids([{'id': 1, 'name': 'Phase 1'}])
        tasks = ensure_task_ids(
            [{'name': 'Bad Task', 'phase': 999, 'task': {'componentId': 'comp1'}}]  # Non-existent phase
        )

        with pytest.raises(ValueError, match='references non-existent phase 999'):
            validate_flow_structure(phases, tasks)


# --- Test Circular Dependency Detection ---


class TestCircularDependencies:
    """Test circular dependency detection."""

    def test_no_circular_dependencies(self):
        """Test flow with no circular dependencies."""
        phases = ensure_phase_ids(
            [
                {'id': 1, 'name': 'Phase 1'},
                {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]},
                {'id': 3, 'name': 'Phase 3', 'dependsOn': [2]},
            ]
        )

        _check_circular_dependencies(phases)

    def test_direct_circular_dependency(self):
        """Test detection of direct circular dependency."""
        phases = ensure_phase_ids(
            [{'id': 1, 'name': 'Phase 1', 'dependsOn': [2]}, {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]}]
        )

        with pytest.raises(ValueError, match='Circular dependency detected'):
            _check_circular_dependencies(phases)

    def test_indirect_circular_dependency(self):
        """Test detection of indirect circular dependency."""
        phases = ensure_phase_ids(
            [
                {'id': 1, 'name': 'Phase 1', 'dependsOn': [3]},
                {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]},
                {'id': 3, 'name': 'Phase 3', 'dependsOn': [2]},
            ]
        )

        with pytest.raises(ValueError, match='Circular dependency detected'):
            _check_circular_dependencies(phases)

    def test_self_referencing_dependency(self):
        """Test detection of self-referencing dependency."""
        phases = ensure_phase_ids([{'id': 1, 'name': 'Phase 1', 'dependsOn': [1]}])

        with pytest.raises(ValueError, match='Circular dependency detected'):
            _check_circular_dependencies(phases)

    def test_complex_valid_dependencies(self):
        """Test complex but valid dependency structure."""
        phases = ensure_phase_ids(
            [
                {'id': 1, 'name': 'Phase 1'},
                {'id': 2, 'name': 'Phase 2'},
                {'id': 3, 'name': 'Phase 3', 'dependsOn': [1, 2]},
                {'id': 4, 'name': 'Phase 4', 'dependsOn': [3]},
                {'id': 5, 'name': 'Phase 5', 'dependsOn': [1]},
            ]
        )

        _check_circular_dependencies(phases)


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
        mock_project_id: str
    ):
        """Test flow creation."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_create = mocker.AsyncMock(
            return_value=mock_raw_flow_config
        )
        keboola_client.storage_client.project_id = mocker.AsyncMock(
            return_value=mock_project_id
        )

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
    async def test_retrieve_flows_all(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_raw_flow_config: Dict[str, Any],
        mock_empty_flow_config: Dict[str, Any],
    ):
        """Test retrieving all flows."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_list = mocker.AsyncMock(
            return_value=[mock_raw_flow_config, mock_empty_flow_config]
        )

        result = await retrieve_flows(ctx=mcp_context_client)

        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(flow, ReducedFlow) for flow in result)
        assert result[0].id == '21703284'
        assert result[1].id == '21703285'
        assert result[0].phases_count == 2
        assert result[1].phases_count == 0

    @pytest.mark.asyncio
    async def test_retrieve_flows_specific_ids(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_raw_flow_config: Dict[str, Any]
    ):
        """Test retrieving specific flows by ID."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(
            return_value=mock_raw_flow_config
        )

        result = await retrieve_flows(ctx=mcp_context_client, flow_ids=['21703284'])

        assert len(result) == 1
        assert result[0].id == '21703284'
        keboola_client.storage_client.flow_detail.assert_called_once_with('21703284')

    @pytest.mark.asyncio
    async def test_retrieve_flows_with_missing_id(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_raw_flow_config: Dict[str, Any]
    ):
        """Test retrieving flows when some IDs don't exist."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        def mock_get_flow(flow_id):
            if flow_id == '21703284':
                return mock_raw_flow_config
            else:
                raise Exception(f'Flow {flow_id} not found')

        keboola_client.storage_client.flow_detail = mocker.AsyncMock(
            side_effect=mock_get_flow
        )

        result = await retrieve_flows(ctx=mcp_context_client, flow_ids=['21703284', 'nonexistent'])

        assert len(result) == 1
        assert result[0].id == '21703284'

    @pytest.mark.asyncio
    async def test_get_flow_detail(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_raw_flow_config: Dict[str, Any]
    ):
        """Test getting detailed flow configuration."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(
            return_value=mock_raw_flow_config
        )

        result = await get_flow_detail(ctx=mcp_context_client, configuration_id=mock_raw_flow_config['id'])

        assert isinstance(result, FlowConfigurationResponse)
        assert result.component_id == ORCHESTRATOR_COMPONENT_ID
        assert result.configuration_id == mock_raw_flow_config['id']
        assert result.configuration_name == mock_raw_flow_config['name']
        assert result.configuration_description == mock_raw_flow_config['description']
        assert isinstance(result.configuration, FlowConfiguration)
        assert len(result.configuration.phases) == 2
        assert len(result.configuration.tasks) == 2
        assert result.configuration.phases[0].name == 'Data Extraction'
        assert result.configuration.tasks[0].name == 'Extract AWS S3'

    @pytest.mark.asyncio
    async def test_update_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        sample_phases: List[Dict[str, Any]],
        sample_tasks: List[Dict[str, Any]],
        mock_raw_flow_config: Dict[str, Any],
        mock_project_id: str
    ):
        """Test flow update."""
        mock_raw_flow_config['description'] = 'Updated description'
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_update = mocker.AsyncMock(
            return_value=mock_raw_flow_config
        )
        keboola_client.storage_client.project_id = mocker.AsyncMock(
            return_value=mock_project_id
        )

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

    def test_phase_validation_with_missing_name(self):
        """Test phase validation when required name field is missing."""
        invalid_phases = [{'name': 'Valid Phase'}, {}]

        processed_phases = ensure_phase_ids(invalid_phases)
        assert len(processed_phases) == 2
        assert processed_phases[1].name == 'Phase 2'

    def test_task_validation_with_missing_name(self):
        """Test task validation when required name field is missing."""
        invalid_tasks = [{}]

        with pytest.raises(ValueError, match="missing 'task' configuration"):
            ensure_task_ids(invalid_tasks)

    def test_empty_flow_validation(self):
        """Test validation of completely empty flow."""
        phases = ensure_phase_ids([])
        tasks = ensure_task_ids([])

        validate_flow_structure(phases, tasks)


# --- Integration-style Tests ---


@pytest.mark.asyncio
async def test_complete_flow_workflow(mocker: MockerFixture, mcp_context_client: Context):
    """Test a complete flow workflow: create, retrieve, update, get detail."""
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

    flows = await retrieve_flows(ctx=mcp_context_client)
    assert len(flows) == 1
    assert flows[0].name == 'Integration Test Flow'

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

    detail = await get_flow_detail(ctx=mcp_context_client, configuration_id='123456')
    assert isinstance(detail, FlowConfigurationResponse)
    assert len(detail.configuration.phases) == 1
    assert len(detail.configuration.tasks) == 1
