from typing import Any

from keboola_mcp_server.clients.client import ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.clients.storage import APIFlowResponse
from keboola_mcp_server.tools.flow.model import (
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
