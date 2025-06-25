from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.tools.flow.model import (
    FlowConfiguration,
    FlowConfigurationResponse,
    FlowPhase,
    FlowTask,
    ReducedFlow,
)

# --- Test Model Parsing ---


class TestFlowModels:
    """Test Flow Pydantic models."""

    def test_flow_configuration_response_model_validate(self, mock_raw_flow_config):
        """Test parsing raw API response into FlowConfigurationResponse."""
        flow_response = FlowConfigurationResponse.model_validate(mock_raw_flow_config)

        assert flow_response.component_id == ORCHESTRATOR_COMPONENT_ID
        assert flow_response.configuration_id == '21703284'
        assert flow_response.configuration_name == 'Test Flow'
        assert flow_response.configuration_description == 'Test flow description'
        assert flow_response.version == 1
        assert flow_response.is_disabled is False
        assert flow_response.is_deleted is False

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

    def test_reduced_flow_model_validate(self, mock_raw_flow_config):
        """Test parsing raw API response into ReducedFlow."""
        reduced_flow = ReducedFlow.model_validate(mock_raw_flow_config)

        assert reduced_flow.id == '21703284'
        assert reduced_flow.name == 'Test Flow'
        assert reduced_flow.description == 'Test flow description'
        assert reduced_flow.version == 1
        assert reduced_flow.phases_count == 2
        assert reduced_flow.tasks_count == 2
        assert reduced_flow.is_disabled is False
        assert reduced_flow.is_deleted is False

    def test_empty_flow_model_validate(self, mock_empty_flow_config):
        """Test parsing empty flow configuration."""
        flow_response = FlowConfigurationResponse.model_validate(mock_empty_flow_config)
        reduced_flow = ReducedFlow.model_validate(mock_empty_flow_config)

        assert len(flow_response.configuration.phases) == 0
        assert len(flow_response.configuration.tasks) == 0
        assert reduced_flow.phases_count == 0
        assert reduced_flow.tasks_count == 0
