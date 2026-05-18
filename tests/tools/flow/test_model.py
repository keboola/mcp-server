from typing import Any

import pytest
from pydantic import ValidationError

from keboola_mcp_server.clients.client import CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.clients.storage import APIFlowResponse
from keboola_mcp_server.tools.flow.model import (
    ConditionalFlowConfiguration,
    ConditionalFlowPhase,
    ConditionalFlowTask,
    ConditionalFlowTransition,
    Flow,
    FlowConfiguration,
    FlowPhase,
    FlowSummary,
    FlowTask,
    FunctionCondition,
    JobTaskConfiguration,
    VariableTaskConfiguration,
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


class TestConditionalFlowValidationResilience:
    """Regression tests for AI-3216 — `get_flows` should not crash on unknown variants.

    The bug: a real conditional flow on stack `com-keboola-gcp-europe-west3` contained a
    `variable` task whose `source.function` was `'YEAR'`. The strict `Literal['COUNT','DATE']`
    on `FunctionCondition.function` failed, and the undiscriminated `TaskConfiguration` union
    surfaced 18 cascading validation errors that aborted the entire `get_flows` response.
    """

    @pytest.mark.parametrize('function_name', ['COUNT', 'DATE', 'YEAR', 'MONTH', 'DAY_OF_WEEK'])
    def test_function_condition_accepts_arbitrary_function_names(self, function_name: str):
        """The `function` field is now permissive — the backend evolves independently of MCP."""
        cond = FunctionCondition.model_validate(
            {'type': 'function', 'function': function_name, 'operands': [{'type': 'const', 'value': 'U'}]}
        )
        assert cond.function == function_name

    def test_variable_task_with_year_function_no_longer_raises(self):
        """The exact shape from the Datadog alert in AI-3216 must parse without raising."""
        raw_task = {
            'id': 'task-1',
            'name': 'compute year',
            'phase': 'phase-1',
            'task': {
                'type': 'variable',
                'name': 'current_year',
                'source': {
                    'type': 'function',
                    'function': 'YEAR',
                    'operands': [{'type': 'const', 'value': 'U'}],
                },
            },
        }
        task = ConditionalFlowTask.model_validate(raw_task)
        assert isinstance(task.task, VariableTaskConfiguration)
        assert isinstance(task.task.source, FunctionCondition)
        assert task.task.source.function == 'YEAR'

    def test_task_configuration_uses_discriminator(self):
        """Discriminator on `type` collapses the 18-error cascade to a single targeted error.

        Pre-fix: pydantic tried Job/Notification/Variable in turn and reported every literal
        mismatch — 18 errors for one bad source value. Post-fix: pydantic dispatches by `type`,
        so a `variable` task only triggers `VariableTaskConfiguration` validation.
        """
        with pytest.raises(ValidationError) as exc_info:
            ConditionalFlowTask.model_validate(
                {
                    'id': 'task-1',
                    'name': 'broken',
                    'phase': 'phase-1',
                    'task': {'type': 'variable'},  # missing required `name`
                }
            )
        errors = exc_info.value.errors()
        # All errors should be scoped to the matched variant only.
        assert all('VariableTaskConfiguration' in str(e.get('loc', ())) or e['loc'][-1] == 'name' for e in errors)
        # Confirm no cascading errors about Job/Notification literals.
        assert not any('JobTaskConfiguration' in str(e.get('loc', ())) for e in errors)
        assert not any('NotificationTaskConfiguration' in str(e.get('loc', ())) for e in errors)

    def test_unknown_task_type_falls_back_in_read_path(self, caplog: pytest.LogCaptureFixture):
        """`Flow.from_api_response` must keep returning the flow even when a task type is unknown.

        Pre-fix: one unknown task type took down the entire `get_flows` response. Post-fix:
        `_safe_validate` logs and falls back to `model_construct` for that task while keeping
        the rest of the flow intact.
        """
        raw = {
            'id': '99',
            'name': 'Flow with unknown task variant',
            'description': '',
            'version': 1,
            'isDisabled': False,
            'isDeleted': False,
            'configuration': {
                'phases': [{'id': 'p1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': None}]}],
                'tasks': [
                    {
                        'id': 'task-good',
                        'name': 'good',
                        'phase': 'p1',
                        'task': {'type': 'job', 'componentId': 'keboola.ex-aws-s3', 'mode': 'run'},
                    },
                    {
                        'id': 'task-bad',
                        'name': 'bad',
                        'phase': 'p1',
                        'task': {'type': 'unknown-future-variant', 'foo': 'bar'},
                    },
                ],
            },
            'metadata': [],
            'created': '2026-01-01T00:00:00+0000',
        }
        api_model = APIFlowResponse.model_validate(raw)
        with caplog.at_level('WARNING'):
            flow = Flow.from_api_response(api_config=api_model, flow_component_id=CONDITIONAL_FLOW_COMPONENT_ID)

        assert isinstance(flow.configuration, ConditionalFlowConfiguration)
        assert len(flow.configuration.tasks) == 2
        good, bad = flow.configuration.tasks
        assert isinstance(good.task, JobTaskConfiguration)
        # Bad task survives as raw passthrough; agent still sees the entry instead of nothing.
        assert bad.id == 'task-bad'
        assert any('failed strict validation' in m for m in caplog.messages)

    def test_unknown_task_type_still_strict_in_write_path(self):
        """Write paths (`utils.get_flow_configuration`) must remain strict to reject agent garbage.

        The fallback is intentionally scoped to `Flow.from_api_response` (the READ path). Agents
        constructing flows should still get loud failures so they can correct themselves.
        """
        from keboola_mcp_server.tools.flow.utils import get_flow_configuration

        with pytest.raises(ValidationError):
            get_flow_configuration(
                phases=[{'id': 'p1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': None}]}],
                tasks=[
                    {
                        'id': 'task-bad',
                        'name': 'bad',
                        'phase': 'p1',
                        'task': {'type': 'unknown-future-variant'},
                    }
                ],
                flow_type=CONDITIONAL_FLOW_COMPONENT_ID,
            )
