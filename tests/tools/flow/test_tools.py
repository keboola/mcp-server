"""Unit tests for Flow management tools."""

from typing import Any, Dict, List

import httpx
import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.tools.flow.model import (
    Flow,
    FlowPhase,
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
        {
            'id': 1,
            'name': 'Data Extraction',
            'description': 'Extract data from various sources',
            'dependsOn': []
        },
        {
            'id': 2,
            'name': 'Data Transformation',
            'description': 'Transform and process data',
            'dependsOn': [1]
        },
        {
            'id': 3,
            'name': 'Data Loading',
            'description': 'Load data to destination',
            'dependsOn': [2]
        }
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
            'task': {
                'componentId': 'keboola.ex-aws-s3',
                'configId': '123456',
                'mode': 'run'
            }
        },
        {
            'id': 20002,
            'name': 'Transform Data',
            'phase': 2,
            'enabled': True,
            'continueOnFailure': False,
            'task': {
                'componentId': 'keboola.snowflake-transformation',
                'configId': '789012',
                'mode': 'run'
            }
        },
        {
            'id': 20003,
            'name': 'Load to Warehouse',
            'phase': 3,
            'enabled': True,
            'continueOnFailure': False,
            'task': {
                'componentId': 'keboola.wr-snowflake',
                'configId': '345678',
                'mode': 'run'
            }
        }
    ]


@pytest.fixture
def mock_conditional_flow_phases() -> List[Dict[str, Any]]:
    """Sample conditional flow phases with simple configuration."""
    return [
        {
            'id': 'phase1',
            'name': 'Simple Phase',
            'description': 'A simple conditional flow phase',
            'next': [
                {
                    'id': 'transition1',
                    'name': 'Simple Transition',
                    'goto': None
                }
            ]
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
                'recipients': [
                    {
                        'channel': 'email',
                        'address': 'admin@company.com'
                    }
                ],
                'title': 'Simple Notification',
                'message': 'This is a simple notification task'
            }
        }
    ]


@pytest.fixture
def mock_conditional_flow(
    mock_conditional_flow_phases: List[Dict[str, Any]],
    mock_conditional_flow_tasks: List[Dict[str, Any]]
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
        'configuration': {
            'phases': mock_conditional_flow_phases,
            'tasks': mock_conditional_flow_tasks
        },
        'rows': [],
        'metadata': []
    }


@pytest.fixture
def mock_conditional_flow_create_update(
    mock_conditional_flow_phases: List[Dict[str, Any]],
    mock_conditional_flow_tasks: List[Dict[str, Any]]
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
        'configuration': {
            'phases': mock_conditional_flow_phases,
            'tasks': mock_conditional_flow_tasks
        },
        'state': {},
        'currentVersion': {'version': 1}
    }


@pytest.fixture
def mock_legacy_flow_create_update(
    legacy_flow_phases: list[dict[str, Any]],
    legacy_flow_tasks: list[dict[str, Any]]
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
        'configuration': {
            'phases': legacy_flow_phases,
            'tasks': legacy_flow_tasks
        },
        'state': {},
        'currentVersion': {'version': 1}
    }


@pytest.fixture
def mock_legacy_flow(
    legacy_flow_phases: list[dict[str, Any]],
    legacy_flow_tasks: list[dict[str, Any]]
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
        'configuration': {
            'phases': legacy_flow_phases,
            'tasks': legacy_flow_tasks
        },
        'rows': [],
        'metadata': []
    }

# =============================================================================
# LEGACY FLOW TESTS
# =============================================================================


class TestLegacyFlowTools:
    """Tests for managing legacy (orchestrator) flows."""

    @pytest.mark.asyncio
    async def test_create_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        legacy_flow_phases: list[dict[str, Any]],
        legacy_flow_tasks: list[dict[str, Any]],
        mock_legacy_flow_create_update: dict[str, Any],
    ):
        """Should create a new legacy (orchestrator) flow with valid phases/tasks."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        mocker.patch.object(keboola_client.storage_client, 'flow_create', return_value=mock_legacy_flow_create_update)

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

        keboola_client.storage_client.flow_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        legacy_flow_phases: List[Dict[str, Any]],
        legacy_flow_tasks: List[Dict[str, Any]],
        mock_legacy_flow_create_update: Dict[str, Any],
    ):
        """Test legacy flow update with new phases and tasks."""
        updated_config = mock_legacy_flow_create_update.copy()
        updated_config['version'] = 2
        updated_config['description'] = 'Updated legacy ETL pipeline'

        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_update = mocker.AsyncMock(return_value=updated_config)

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

        keboola_client.storage_client.flow_update.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_legacy_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_legacy_flow: dict[str, Any],
    ):
        """Should fall back to legacy flow when conditional flow is missing (404)."""

        async def mock_flow_detail(config_id: str, flow_type: str) -> dict[str, Any]:
            if flow_type == CONDITIONAL_FLOW_COMPONENT_ID:
                response = mocker.Mock(status_code=404)
                raise httpx.HTTPStatusError('404 Not Found', request=None, response=response)
            if flow_type == ORCHESTRATOR_COMPONENT_ID:
                return mock_legacy_flow
            raise ValueError(f'Unexpected flow type: {flow_type}')

        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        mocker.patch.object(keboola_client.storage_client, 'flow_detail', side_effect=mock_flow_detail)

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
        assert result.configuration.phases == [FlowPhase.model_validate(phase) for phase in mock_legacy_flow['configuration']['phases']]
        assert result.configuration.tasks == [FlowTask.model_validate(task) for task in mock_legacy_flow['configuration']['tasks']]
        assert len(result.links) == 3

# =============================================================================
# CONDITIONAL FLOW TESTS
# =============================================================================


class TestConditionalFlowTools:
    """Test conditional flow management tools."""

    @pytest.mark.asyncio
    async def test_create_conditional_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow_create_update: Dict[str, Any],
    ):
        """Test conditional flow creation."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_create = mocker.AsyncMock(return_value=mock_conditional_flow_create_update)

        result = await create_conditional_flow(
            ctx=mcp_context_client,
            name='Advanced Data Pipeline',
            description='Advanced pipeline with conditional logic and error handling',
            phases=mock_conditional_flow_create_update['configuration']['phases'],
            tasks=mock_conditional_flow_create_update['configuration']['tasks'],
        )

        assert isinstance(result, FlowToolResponse)
        assert result.id == 'conditional_flow_456'
        assert result.description == 'Advanced pipeline with conditional logic and error handling'
        assert result.success is True

        # Verify the flow creation call
        keboola_client.storage_client.flow_create.assert_called_once()
        call_args = keboola_client.storage_client.flow_create.call_args

        assert call_args.kwargs['name'] == 'Advanced Data Pipeline'
        assert call_args.kwargs['flow_type'] == CONDITIONAL_FLOW_COMPONENT_ID

        # Verify conditional flow specific structure
        flow_config = call_args.kwargs['flow_configuration']
        assert len(flow_config['phases']) == 1  # simple phase
        assert len(flow_config['tasks']) == 1   # simple task

    @pytest.mark.asyncio
    async def test_update_conditional_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow_create_update: Dict[str, Any],
    ):
        """Test conditional flow update with enhanced conditions."""
        updated_config = mock_conditional_flow_create_update.copy()
        updated_config['version'] = 2

        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_update = mocker.AsyncMock(return_value=updated_config)

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

        # Verify the update call
        keboola_client.storage_client.flow_update.assert_called_once()
        call_args = keboola_client.storage_client.flow_update.call_args

        assert call_args.kwargs['config_id'] == 'conditional_flow_456'
        assert call_args.kwargs['flow_type'] == CONDITIONAL_FLOW_COMPONENT_ID

    @pytest.mark.asyncio
    async def test_get_conditional_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow: Dict[str, Any],
    ):
        """Test retrieving conditional flow details."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(return_value=mock_conditional_flow)

        result = await get_flow(
            ctx=mcp_context_client,
            configuration_id='conditional_flow_456'
        )

        assert isinstance(result, Flow)
        assert result.component_id == CONDITIONAL_FLOW_COMPONENT_ID
        assert result.configuration_id == 'conditional_flow_456'
        assert result.name == 'Advanced Data Pipeline'
        from keboola_mcp_server.tools.flow.model import ConditionalFlowPhase, ConditionalFlowTask
        assert result.configuration.phases == [ConditionalFlowPhase.model_validate(p) for p in mock_conditional_flow['configuration']['phases']]
        assert result.configuration.tasks == [ConditionalFlowTask.model_validate(t) for t in mock_conditional_flow['configuration']['tasks']]

# =============================================================================
# MIXED FLOW TYPE TESTS
# =============================================================================


class TestMixedFlowTypeTools:
    """Test tools that work with both flow types."""

    @pytest.mark.asyncio
    async def test_list_flows_mixed_types(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_legacy_flow_create_update: Dict[str, Any],
        mock_conditional_flow_create_update: Dict[str, Any],
    ):
        """Test listing flows of both types."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock different responses for different flow types
        def mock_flow_list(flow_type):
            if flow_type == ORCHESTRATOR_COMPONENT_ID:
                return [mock_legacy_flow_create_update]
            elif flow_type == CONDITIONAL_FLOW_COMPONENT_ID:
                return [mock_conditional_flow_create_update]
            return []

        keboola_client.storage_client.flow_list = mocker.AsyncMock(side_effect=mock_flow_list)

        result = await list_flows(ctx=mcp_context_client)

        assert isinstance(result, ListFlowsOutput)
        assert len(result.flows) == 2

        # Verify both flow types are present
        flow_types = {flow.component_id for flow in result.flows}
        assert ORCHESTRATOR_COMPONENT_ID in flow_types
        assert CONDITIONAL_FLOW_COMPONENT_ID in flow_types

        # Verify flow summaries have correct structure
        legacy_flow = next(f for f in result.flows if f.component_id == ORCHESTRATOR_COMPONENT_ID)
        conditional_flow = next(f for f in result.flows if f.component_id == CONDITIONAL_FLOW_COMPONENT_ID)

        assert legacy_flow.configuration_id == 'legacy_flow_123'
        assert conditional_flow.configuration_id == 'conditional_flow_456'

    @pytest.mark.asyncio
    async def test_list_flows_specific_ids_mixed_types(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_legacy_flow_create_update: Dict[str, Any],
        mock_conditional_flow_create_update: Dict[str, Any],
    ):
        """Test listing specific flows by ID when they're different types."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        def mock_flow_detail(flow_id, component_id=None):
            if flow_id == 'legacy_flow_123' and component_id == ORCHESTRATOR_COMPONENT_ID:
                return mock_legacy_flow_create_update
            elif flow_id == 'conditional_flow_456' and component_id == CONDITIONAL_FLOW_COMPONENT_ID:
                return mock_conditional_flow_create_update
            raise Exception(f'Flow {flow_id} not found')

        keboola_client.storage_client.flow_detail = mocker.AsyncMock(side_effect=mock_flow_detail)

        result = await list_flows(
            ctx=mcp_context_client,
            flow_ids=['legacy_flow_123', 'conditional_flow_456']
        )

        assert len(result.flows) == 2
        assert any(f.component_id == ORCHESTRATOR_COMPONENT_ID for f in result.flows)
        assert any(f.component_id == CONDITIONAL_FLOW_COMPONENT_ID for f in result.flows)

    @pytest.mark.asyncio
    async def test_get_flow_schema_both_types(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting schema for both flow types."""

        # Mock schema responses
        legacy_schema = {'type': 'object', 'properties': {'phases': {'type': 'array'}}}
        conditional_schema = {'type': 'object', 'properties': {'phases': {'type': 'array'}}}

        def mock_get_schema(flow_type):
            if flow_type == ORCHESTRATOR_COMPONENT_ID:
                return legacy_schema
            elif flow_type == CONDITIONAL_FLOW_COMPONENT_ID:
                return conditional_schema
            return {}

        mocker.patch('keboola_mcp_server.tools.flow.utils._load_schema', side_effect=mock_get_schema)

        # Test legacy flow schema
        legacy_result = await get_flow_schema(
            ctx=mcp_context_client,
            flow_type=ORCHESTRATOR_COMPONENT_ID
        )
        assert 'phases' in legacy_result

        # Test conditional flow schema
        conditional_result = await get_flow_schema(
            ctx=mcp_context_client,
            flow_type=CONDITIONAL_FLOW_COMPONENT_ID
        )
        assert 'phases' in conditional_result

    @pytest.mark.asyncio
    async def test_get_flow_examples_both_types(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting examples for both flow types."""

        # Mock example responses
        legacy_examples = 'Legacy flow examples...'
        conditional_examples = 'Conditional flow examples...'

        def mock_get_examples(flow_type):
            if flow_type == ORCHESTRATOR_COMPONENT_ID:
                return legacy_examples
            elif flow_type == CONDITIONAL_FLOW_COMPONENT_ID:
                return conditional_examples
            return ''

        mocker.patch('keboola_mcp_server.tools.flow.utils.get_schema_as_markdown', side_effect=mock_get_examples)

        # Test legacy flow examples
        legacy_result = await get_flow_examples(
            ctx=mcp_context_client,
            flow_type=ORCHESTRATOR_COMPONENT_ID
        )
        assert 'Legacy flow examples' in legacy_result

        # Test conditional flow examples
        conditional_result = await get_flow_examples(
            ctx=mcp_context_client,
            flow_type=CONDITIONAL_FLOW_COMPONENT_ID
        )
        assert 'Conditional flow examples' in conditional_result

# =============================================================================
# EDGE CASES AND ERROR HANDLING
# =============================================================================


class TestFlowEdgeCases:
    """Test edge cases and error conditions for both flow types."""

    @pytest.mark.asyncio
    async def test_create_flow_with_invalid_legacy_structure(
        self,
        mcp_context_client: Context
    ):
        """Test legacy flow creation with invalid structure."""
        invalid_phases = [
            {'name': 'Phase 1', 'dependsOn': [999]}  # Invalid dependency
        ]
        invalid_tasks = [
            {'name': 'Task 1', 'phase': 1, 'task': {'componentId': 'comp1'}}
        ]

        with pytest.raises(ValueError, match='depends on non-existent phase'):
            await create_flow(
                ctx=mcp_context_client,
                name='Invalid Legacy Flow',
                description='Invalid legacy flow',
                phases=invalid_phases,
                tasks=invalid_tasks,
            )

    @pytest.mark.asyncio
    async def test_create_conditional_flow_with_invalid_structure(
        self,
        mcp_context_client: Context
    ):
        """Test conditional flow creation with invalid structure."""
        invalid_phases = [
            {
                'id': 'phase_1',
                'name': 'Phase 1',
                'next': [
                    {
                        'id': 'invalid_transition',
                        'goto': 'non_existent_phase'  # Invalid target phase
                    }
                ]
            }
        ]
        invalid_tasks = [
            {
                'id': 'task_1',
                'name': 'Task 1',
                'phase': 'phase_1',
                'task': {
                    'type': 'job',
                    'componentId': 'comp1'
                }
            }
        ]

        with pytest.raises(ValueError, match='transition.*references non-existent phase'):
            await create_conditional_flow(
                ctx=mcp_context_client,
                name='Invalid Conditional Flow',
                description='Invalid conditional flow',
                phases=invalid_phases,
                tasks=invalid_tasks,
            )

    @pytest.mark.asyncio
    async def test_update_flow_wrong_type(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        legacy_flow_phases: List[Dict[str, Any]],
        legacy_flow_tasks: List[Dict[str, Any]],
    ):
        """Test updating a flow with the wrong flow type."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_update = mocker.AsyncMock(
            side_effect=Exception('Flow not found')
        )

        with pytest.raises(Exception, match='Flow not found'):
            await update_flow(
                ctx=mcp_context_client,
                configuration_id='legacy_flow_123',
                flow_type=CONDITIONAL_FLOW_COMPONENT_ID,  # Wrong type
                name='Updated Flow',
                description='Updated description',
                phases=legacy_flow_phases,
                tasks=legacy_flow_tasks,
                change_description='Update with wrong type',
            )

    @pytest.mark.asyncio
    async def test_get_flow_not_found(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ):
        """Test getting a non-existent flow."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(
            side_effect=Exception('Flow not found')
        )

        with pytest.raises(Exception, match='Flow not found'):
            await get_flow(
                ctx=mcp_context_client,
                configuration_id='non_existent_flow'
            )

# =============================================================================
# INTEGRATION TESTS
# =============================================================================


class TestFlowIntegration:
    """Integration tests for complete flow workflows."""

    @pytest.mark.asyncio
    async def test_complete_legacy_flow_workflow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        legacy_flow_phases: List[Dict[str, Any]],
        legacy_flow_tasks: List[Dict[str, Any]],
        mock_legacy_flow_create_update: Dict[str, Any],
    ):
        """Test complete legacy flow workflow: create, list, update, get."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock all necessary methods
        keboola_client.storage_client.flow_create = mocker.AsyncMock(return_value=mock_legacy_flow_create_update)
        keboola_client.storage_client.flow_list = mocker.AsyncMock(
            side_effect=lambda flow_type: [mock_legacy_flow_create_update] if flow_type == ORCHESTRATOR_COMPONENT_ID else []
        )
        keboola_client.storage_client.flow_update = mocker.AsyncMock(return_value=mock_legacy_flow_create_update)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(return_value=mock_legacy_flow_create_update)

        # 1. Create flow
        created = await create_flow(
            ctx=mcp_context_client,
            name='Integration Test Legacy Flow',
            description='Legacy flow for integration testing',
            phases=legacy_flow_phases,
            tasks=legacy_flow_tasks,
        )
        assert isinstance(created, FlowToolResponse)
        assert created.success is True

        # 2. List flows
        listed = await list_flows(ctx=mcp_context_client)
        assert len(listed.flows) == 1
        assert listed.flows[0].component_id == ORCHESTRATOR_COMPONENT_ID

        # 3. Update flow
        updated = await update_flow(
            ctx=mcp_context_client,
            configuration_id='legacy_flow_123',
            flow_type=ORCHESTRATOR_COMPONENT_ID,
            name='Updated Integration Test Legacy Flow',
            description='Updated legacy flow for integration testing',
            phases=legacy_flow_phases,
            tasks=legacy_flow_tasks,
            change_description='Updated for integration testing',
        )
        assert isinstance(updated, FlowToolResponse)
        assert updated.success is True

        # 4. Get flow details
        detail = await get_flow(
            ctx=mcp_context_client,
            configuration_id='legacy_flow_123'
        )
        assert isinstance(detail, Flow)
        assert detail.component_id == ORCHESTRATOR_COMPONENT_ID

    @pytest.mark.asyncio
    async def test_complete_conditional_flow_workflow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow_create_update: Dict[str, Any],
    ):
        """Test complete conditional flow workflow: create, list, update, get."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock all necessary methods
        keboola_client.storage_client.flow_create = mocker.AsyncMock(return_value=mock_conditional_flow_create_update)
        keboola_client.storage_client.flow_list = mocker.AsyncMock(
            side_effect=lambda ft: [mock_conditional_flow_create_update] if ft == CONDITIONAL_FLOW_COMPONENT_ID else []
        )
        keboola_client.storage_client.flow_update = mocker.AsyncMock(return_value=mock_conditional_flow_create_update)
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(return_value=mock_conditional_flow_create_update)

        # 1. Create conditional flow
        created = await create_conditional_flow(
            ctx=mcp_context_client,
            name='Integration Test Conditional Flow',
            description='Conditional flow for integration testing',
            phases=mock_conditional_flow_create_update['configuration']['phases'],
            tasks=mock_conditional_flow_create_update['configuration']['tasks'],
        )
        assert isinstance(created, FlowToolResponse)
        assert created.success is True

        # 2. List flows
        listed = await list_flows(ctx=mcp_context_client)
        assert len(listed.flows) == 1
        assert listed.flows[0].component_id == CONDITIONAL_FLOW_COMPONENT_ID

        # 3. Update conditional flow
        updated = await update_flow(
            ctx=mcp_context_client,
            configuration_id='conditional_flow_456',
            flow_type=CONDITIONAL_FLOW_COMPONENT_ID,
            name='Updated Integration Test Conditional Flow',
            description='Updated conditional flow for integration testing',
            phases=mock_conditional_flow_create_update['configuration']['phases'],
            tasks=mock_conditional_flow_create_update['configuration']['tasks'],
            change_description='Updated for integration testing',
        )
        assert isinstance(updated, FlowToolResponse)
        assert updated.success is True

        # 4. Get conditional flow details
        detail = await get_flow(
            ctx=mcp_context_client,
            configuration_id='conditional_flow_456'
        )
        assert isinstance(detail, Flow)
        assert detail.component_id == CONDITIONAL_FLOW_COMPONENT_ID
        # Conditional flows should have retry configuration
        assert hasattr(detail.configuration.phases[0], 'retry')

    @pytest.mark.asyncio
    async def test_mixed_flow_type_workflow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        legacy_flow_phases: List[Dict[str, Any]],
        legacy_flow_tasks: List[Dict[str, Any]],
        mock_conditional_flow_create_update: Dict[str, Any],
        mock_legacy_flow_create_update: Dict[str, Any],
    ):
        """Test workflow involving both flow types simultaneously."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock all necessary methods
        keboola_client.storage_client.flow_create = mocker.AsyncMock(
            side_effect=[mock_legacy_flow_create_update, mock_conditional_flow_create_update]
        )
        keboola_client.storage_client.flow_list = mocker.AsyncMock(
            side_effect=lambda flow_type: {
                ORCHESTRATOR_COMPONENT_ID: [mock_legacy_flow_create_update],
                CONDITIONAL_FLOW_COMPONENT_ID: [mock_conditional_flow_create_update]
            }[flow_type]
        )
        keboola_client.storage_client.flow_detail = mocker.AsyncMock(
            side_effect=lambda flow_id, component_id=None: {
                'legacy_flow_123': mock_legacy_flow_create_update,
                'conditional_flow_456': mock_conditional_flow_create_update
            }[flow_id]
        )

        # 1. Create both types of flows
        legacy_created = await create_flow(
            ctx=mcp_context_client,
            name='Mixed Test Legacy Flow',
            description='Legacy flow for mixed testing',
            phases=legacy_flow_phases,
            tasks=legacy_flow_tasks,
        )
        assert legacy_created.success is True

        conditional_created = await create_conditional_flow(
            ctx=mcp_context_client,
            name='Mixed Test Conditional Flow',
            description='Conditional flow for mixed testing',
            phases=mock_conditional_flow_create_update['configuration']['phases'],
            tasks=mock_conditional_flow_create_update['configuration']['tasks'],
        )
        assert conditional_created.success is True

        # 2. List all flows
        all_flows = await list_flows(ctx=mcp_context_client)
        assert len(all_flows.flows) == 2
        assert any(f.component_id == ORCHESTRATOR_COMPONENT_ID for f in all_flows.flows)
        assert any(f.component_id == CONDITIONAL_FLOW_COMPONENT_ID for f in all_flows.flows)

        # 3. Get specific flows by ID
        legacy_detail = await get_flow(
            ctx=mcp_context_client,
            configuration_id='legacy_flow_123'
        )
        assert legacy_detail.component_id == ORCHESTRATOR_COMPONENT_ID

        conditional_detail = await get_flow(
            ctx=mcp_context_client,
            configuration_id='conditional_flow_456'
        )
        assert conditional_detail.component_id == CONDITIONAL_FLOW_COMPONENT_ID
