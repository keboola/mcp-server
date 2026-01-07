from typing import Any

from keboola_mcp_server.clients.client import ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.clients.storage import APIFlowResponse
from keboola_mcp_server.tools.flow.model import (
    ConditionalFlowPhase,
    ConditionalFlowTransition,
    Flow,
    FlowConfiguration,
    FlowPhase,
    FlowSummary,
    FlowTask,
)

# --- Test Model Parsing ---


class TestFlowModels:
    """Test Flow models."""

    def test_flow_from_api_response(self, mock_raw_flow_config: dict[str, Any]):
        """Test Flow.from_api_response from a typical raw API response."""
        assert 'component_id' not in mock_raw_flow_config
        api_model = APIFlowResponse.model_validate(mock_raw_flow_config)
        flow = Flow.from_api_response(api_config=api_model, flow_component_id=ORCHESTRATOR_COMPONENT_ID)
        assert flow.component_id == ORCHESTRATOR_COMPONENT_ID
        assert flow.configuration_id == '21703284'
        assert flow.name == 'Test Flow'
        assert flow.description == 'Test flow description'
        assert flow.version == 1
        assert flow.is_disabled is False
        assert flow.is_deleted is False
        config = flow.configuration
        assert isinstance(config, FlowConfiguration)
        assert len(config.phases) == 2
        assert len(config.tasks) == 2
        # Check phase and task structure
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

    def test_flow_summary_from_api_response(self, mock_raw_flow_config: dict[str, Any]):
        """Test FlowSummary.from_api_response from a typical raw API response."""
        assert 'tasks_count' not in mock_raw_flow_config
        assert 'phases_count' not in mock_raw_flow_config
        api_model = APIFlowResponse.model_validate(mock_raw_flow_config)
        flow_summary = FlowSummary.from_api_response(api_config=api_model, flow_component_id=ORCHESTRATOR_COMPONENT_ID)
        assert flow_summary.component_id == ORCHESTRATOR_COMPONENT_ID
        assert flow_summary.configuration_id == '21703284'
        assert flow_summary.name == 'Test Flow'
        assert flow_summary.description == 'Test flow description'
        assert flow_summary.version == 1
        assert flow_summary.phases_count == 2
        assert flow_summary.tasks_count == 2
        assert flow_summary.is_disabled is False
        assert flow_summary.is_deleted is False

    def test_empty_flow_from_api_response(self, mock_empty_flow_config: dict[str, Any]):
        """Test Flow and FlowSummary from_api_response with an empty flow configuration."""
        assert 'component_id' not in mock_empty_flow_config
        assert 'tasks_count' not in mock_empty_flow_config
        assert 'phases_count' not in mock_empty_flow_config
        api_model = APIFlowResponse.model_validate(mock_empty_flow_config)
        flow = Flow.from_api_response(api_config=api_model, flow_component_id=ORCHESTRATOR_COMPONENT_ID)
        flow_summary = FlowSummary.from_api_response(api_config=api_model, flow_component_id=ORCHESTRATOR_COMPONENT_ID)
        assert len(flow.configuration.phases) == 0
        assert len(flow.configuration.tasks) == 0
        assert flow_summary.phases_count == 0
        assert flow_summary.tasks_count == 0


class TestConditionalFlowPhase:
    """Tests for conditional flow phase serialization helpers."""

    def test_next_defaults_to_empty_list(self):
        """Ensure default next is an empty list and serialized when requested."""
        phase = ConditionalFlowPhase(id='phase-1', name='Phase 1')

        assert phase.next == []

        serialized = phase.model_dump()
        assert 'next' in serialized
        assert serialized['next'] == []

    def test_model_dump_exclude_unset_omits_empty_next(self):
        """When exclude_unset=True, empty next should be removed from payload."""
        phase = ConditionalFlowPhase(id='phase-1', name='Phase 1')

        serialized = phase.model_dump(exclude_unset=True)

        assert 'next' not in serialized

    def test_model_dump_keeps_non_empty_next(self):
        """Non-empty next array should always be serialized."""
        transition = ConditionalFlowTransition(id='t1', name='Go to phase 2', goto='phase-2')
        phase = ConditionalFlowPhase(id='phase-1', name='Phase 1', next=[transition])

        serialized_default = phase.model_dump()
        assert serialized_default['next'][0]['id'] == 't1'

        serialized_excluding_unset = phase.model_dump(exclude_unset=True)
        assert serialized_excluding_unset['next'][0]['goto'] == 'phase-2'

    def test_model_dump_excludes_single_null_goto_transition(self):
        """Single transition with goto=None should be excluded when exclude_unset=True to prevent UI damage."""
        transition = ConditionalFlowTransition(id='t1', name='Go to end', goto=None)
        phase = ConditionalFlowPhase(id='phase-1', name='Phase 1', next=[transition])

        # Without exclude_unset, the next array should be serialized
        serialized_default = phase.model_dump()
        assert 'next' in serialized_default
        assert serialized_default['next'][0]['id'] == 't1'
        assert serialized_default['next'][0]['goto'] is None

        # With exclude_unset=True, the next array should be excluded
        serialized_excluding_unset = phase.model_dump(exclude_unset=True)
        assert 'next' not in serialized_excluding_unset

    def test_model_dump_keeps_multiple_transitions_with_null_goto(self):
        """Multiple transitions should be kept even if one has goto=None."""
        transition1 = ConditionalFlowTransition(id='t1', name='Go to phase 2', goto='phase-2')
        transition2 = ConditionalFlowTransition(id='t2', name='Go to end', goto=None)
        phase = ConditionalFlowPhase(id='phase-1', name='Phase 1', next=[transition1, transition2])

        serialized_default = phase.model_dump()
        assert 'next' in serialized_default
        assert len(serialized_default['next']) == 2

        serialized_excluding_unset = phase.model_dump(exclude_unset=True)
        assert 'next' in serialized_excluding_unset
        assert len(serialized_excluding_unset['next']) == 2
        assert serialized_excluding_unset['next'][0]['goto'] == 'phase-2'
        assert serialized_excluding_unset['next'][1]['goto'] is None
