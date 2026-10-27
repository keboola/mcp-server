import copy
from typing import Any

import pytest
from httpx import ConnectError, HTTPStatusError, Request, Response

from keboola_mcp_server.clients.client import CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.clients.storage import APIFlowResponse
from keboola_mcp_server.errors import NotFoundError
from keboola_mcp_server.tools.flow.model import Flow
from keboola_mcp_server.tools.flow.utils import (
    _check_legacy_circular_dependencies,
    _reachable_ids,
    ensure_legacy_phase_ids,
    ensure_legacy_task_ids,
    get_flow_configuration,
    normalize_data_app_tasks,
    resolve_flow_by_id,
    resolve_flow_schema,
    validate_flow_structure,
)


def _notification_task(task_id: str, phase_id: str) -> dict[str, Any]:
    """Create a minimal notification task for conditional flow tests."""
    return {
        'id': task_id,
        'name': f'Task {task_id}',
        'phase': phase_id,
        'task': {
            'type': 'notification',
            'title': 'Notify',
            'message': 'Done',
            'recipients': [{'channel': 'email', 'address': 'ops@example.com'}],
        },
    }


# --- Test Helper Functions ---


class TestFlowHelpers:
    """Test helper functions for flow processing."""

    def test_ensure_phase_ids_with_missing_ids(self):
        """Test phase ID generation when IDs are missing."""
        phases = [{'name': 'Phase 1'}, {'name': 'Phase 2', 'dependsOn': [1]}, {'id': 5, 'name': 'Phase 5'}]

        processed_phases = ensure_legacy_phase_ids(phases)

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

        processed_phases = ensure_legacy_phase_ids(phases)

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

        processed_tasks = ensure_legacy_task_ids(tasks)

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

        processed_tasks = ensure_legacy_task_ids(tasks)

        assert processed_tasks[0].task['mode'] == 'run'  # Default added
        assert processed_tasks[1].task['mode'] == 'debug'  # Existing preserved

    def test_ensure_task_ids_validates_required_fields(self):
        """Test validation of required task fields."""
        with pytest.raises(ValueError, match="missing 'task' configuration"):
            ensure_legacy_task_ids([{'name': 'Bad Task', 'phase': 1}])

        with pytest.raises(ValueError, match='missing componentId'):
            ensure_legacy_task_ids([{'name': 'Bad Task', 'phase': 1, 'task': {}}])

    def test_validate_flow_structure_success(self, sample_phases, sample_tasks):
        """Test successful flow structure validation."""
        flow_configuration = get_flow_configuration(sample_phases, sample_tasks, ORCHESTRATOR_COMPONENT_ID)
        validate_flow_structure(flow_configuration, flow_type=ORCHESTRATOR_COMPONENT_ID)

    def test_validate_flow_structure_invalid_phase_dependency(self):
        """Test validation failure for invalid phase dependencies."""
        flow_configuration = get_flow_configuration(
            phases=[{'id': 1, 'name': 'Phase 1', 'dependsOn': [999]}], tasks=[], flow_type=ORCHESTRATOR_COMPONENT_ID
        )

        with pytest.raises(ValueError, match='depends on non-existent phase 999'):
            validate_flow_structure(flow_configuration, flow_type=ORCHESTRATOR_COMPONENT_ID)

    def test_validate_flow_structure_invalid_task_phase(self):
        """Test validation failure for task referencing non-existent phase."""
        flow_configuration = get_flow_configuration(
            phases=[{'id': 1, 'name': 'Phase 1'}],
            tasks=[{'name': 'Bad Task', 'phase': 999, 'task': {'componentId': 'comp1'}}],
            flow_type=ORCHESTRATOR_COMPONENT_ID,
        )

        with pytest.raises(ValueError, match='references non-existent phase 999'):
            validate_flow_structure(flow_configuration, flow_type=ORCHESTRATOR_COMPONENT_ID)


# --- Test Circular Dependency Detection ---


class TestCircularDependencies:
    """Test circular dependency detection."""

    @pytest.mark.parametrize(
        'phases',
        [
            pytest.param(
                [
                    {'id': 1, 'name': 'Phase 1'},
                    {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]},
                    {'id': 3, 'name': 'Phase 3', 'dependsOn': [2]},
                ],
                id='no_circular_dependencies',
            ),
            pytest.param(
                [
                    {'id': 1, 'name': 'Phase 1'},
                    {'id': 2, 'name': 'Phase 2'},
                    {'id': 3, 'name': 'Phase 3', 'dependsOn': [1, 2]},
                    {'id': 4, 'name': 'Phase 4', 'dependsOn': [3]},
                    {'id': 5, 'name': 'Phase 5', 'dependsOn': [1]},
                ],
                id='complex_valid_dependencies',
            ),
        ],
    )
    def test_no_circular_dependency_cases(self, phases: list[dict[str, Any]]):
        """Test cases where no circular dependencies should be detected."""
        phases = ensure_legacy_phase_ids(phases)
        ret = _check_legacy_circular_dependencies(phases)
        assert ret is None

    @pytest.mark.parametrize(
        'phases',
        [
            pytest.param(
                [{'id': 1, 'name': 'Phase 1', 'dependsOn': [2]}, {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]}],
                id='direct_circular_dependency',
            ),
            pytest.param(
                [
                    {'id': 1, 'name': 'Phase 1', 'dependsOn': [3]},
                    {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]},
                    {'id': 3, 'name': 'Phase 3', 'dependsOn': [2]},
                ],
                id='indirect_circular_dependency',
            ),
            pytest.param([{'id': 1, 'name': 'Phase 1', 'dependsOn': [1]}], id='self_referencing_dependency'),
        ],
    )
    def test_circular_dependency_errors(self, phases: list[dict[str, Any]]):
        """Test detection of direct, indirect, and self-referencing circular dependencies."""
        phases = ensure_legacy_phase_ids(phases)

        with pytest.raises(ValueError, match='Circular dependency detected'):
            _check_legacy_circular_dependencies(phases)


# --- Test Edge Cases ---


class TestFlowEdgeCases:
    """Test edge cases and error conditions."""

    def test_phase_validation_with_missing_name(self):
        """Test phase validation when required name field is missing."""
        invalid_phases = [{'name': 'Valid Phase'}, {}]

        processed_phases = ensure_legacy_phase_ids(invalid_phases)
        assert len(processed_phases) == 2
        assert processed_phases[1].name == 'Phase 2'

    def test_task_validation_with_missing_name(self):
        """Test task validation when required name field is missing."""
        invalid_tasks = [{}]

        with pytest.raises(ValueError, match="missing 'task' configuration"):
            ensure_legacy_task_ids(invalid_tasks)

    def test_empty_flow_validation(self):
        """Test validation of completely empty flow."""
        flow_configuration = get_flow_configuration([], [], ORCHESTRATOR_COMPONENT_ID)
        ret = validate_flow_structure(flow_configuration, flow_type=ORCHESTRATOR_COMPONENT_ID)
        assert ret is None


class TestFlowConfigurationBuilder:
    """Test flow configuration builder helper."""

    def test_get_flow_configuration_legacy_generates_ids_and_aliases(self):
        """Legacy builder should sanitize IDs and serialize aliases."""
        flow_configuration = get_flow_configuration(
            phases=[{'name': 'Generated Phase', 'depends_on': []}],
            tasks=[{'name': 'Legacy Task', 'phase': 1, 'task': {'componentId': 'keboola.component'}}],
            flow_type=ORCHESTRATOR_COMPONENT_ID,
        )

        phase = flow_configuration['phases'][0]
        task = flow_configuration['tasks'][0]

        assert phase['id'] == 1
        assert 'dependsOn' in phase
        assert 'depends_on' not in phase
        assert task['id'] == 20001
        assert task['task']['mode'] == 'run'
        assert 'continueOnFailure' in task

    def test_get_flow_configuration_conditional_excludes_unset_fields(self):
        """Conditional builder should drop unset optional fields including single goto=null transitions."""
        flow_configuration = get_flow_configuration(
            phases=[
                {
                    'id': 'phase1',
                    'name': 'Start',
                    'next': [{'id': 'transition1', 'goto': None}],
                }
            ],
            tasks=[_notification_task('task1', 'phase1')],
            flow_type=CONDITIONAL_FLOW_COMPONENT_ID,
        )

        phase = flow_configuration['phases'][0]
        task = flow_configuration['tasks'][0]

        assert 'description' not in phase
        # Single transition with goto=None should be dropped entirely
        assert 'next' not in phase
        assert 'enabled' not in task


class TestConditionalFlowValidation:
    """Test validation logic for conditional flows."""

    @pytest.mark.parametrize(
        'phases',
        [
            pytest.param(
                [
                    {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {'id': 'phase2', 'name': 'End', 'next': [{'id': 't2', 'goto': None}]},
                ],
                id='simple-start-end',
            ),
            pytest.param(
                [
                    {'id': 'phase1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {
                        'id': 'phase2',
                        'name': 'Phase 2',
                        'next': [{'id': 't2', 'goto': 'phase3'}, {'id': 't3', 'goto': 'phase4'}],
                    },
                    {'id': 'phase3', 'name': 'Phase 3', 'next': [{'id': 't4', 'goto': None}]},
                    {'id': 'phase4', 'name': 'Phase 4', 'next': [{'id': 't5', 'goto': None}]},
                ],
                id='complex-branched',
            ),
        ],
    )
    def test_validate_conditional_flow_valid_cases(self, phases: list[dict[str, Any]]):
        """Test valid conditional flow dependency structures (includes both simple and complex cases)."""
        tasks = [_notification_task(f'task{i}', phase['id']) for i, phase in enumerate(phases)]
        # Should not raise any errors
        ret = validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        assert ret is None

    @pytest.mark.parametrize(
        ('phases', 'task_specs', 'error_match'),
        [
            pytest.param(
                [
                    {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {'id': 'phase1', 'name': 'Duplicate', 'next': [{'id': 't2', 'goto': None}]},
                ],
                [('task1', 'phase1'), ('task2', 'phase1')],
                'duplicate phase IDs',
                id='duplicate_phase_ids',
            ),
            pytest.param(
                [{'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': None}]}],
                [('task1', 'phase1'), ('task1', 'phase1')],
                'duplicate task IDs',
                id='duplicate_task_ids',
            ),
            pytest.param(
                [{'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': None}]}],
                [('task1', 'missing-phase')],
                'references non-existent phase',
                id='task_references_missing_phase',
            ),
            pytest.param(
                [{'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'ghost-phase'}]}],
                [('task1', 'phase1')],
                'references non-existent phase',
                id='transition_references_missing_phase',
            ),
            pytest.param(
                [
                    {'id': 'phase0', 'name': 'Start', 'next': [{'id': 't0', 'goto': 'phase1'}]},
                    {'id': 'phase1', 'name': 'Loop', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {'id': 'phase2', 'name': 'Loop Again', 'next': [{'id': 't2', 'goto': 'phase1'}]},
                ],
                [('task1', 'phase1'), ('task2', 'phase2')],
                'has no ending phases',
                id='requires_ending_phase',
            ),
            pytest.param(
                [
                    {'id': 'phase1', 'name': 'One', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {
                        'id': 'phase2',
                        'name': 'Two',
                        'next': [{'id': 't2', 'goto': 'phase1'}, {'id': 't3', 'goto': None}],
                    },
                ],
                [('task1', 'phase1'), ('task2', 'phase2')],
                'has no entry phase',
                id='requires_entry_phase',
            ),
            pytest.param(
                [
                    {'id': 'phase1', 'name': 'Entry A', 'next': [{'id': 't1', 'goto': None}]},
                    {'id': 'phase2', 'name': 'Entry B', 'next': [{'id': 't2', 'goto': None}]},
                ],
                [('task1', 'phase1'), ('task2', 'phase2')],
                'multiple entry phases',
                id='single_entry_required',
            ),
            pytest.param(
                [
                    {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {'id': 'phase2', 'name': 'End', 'next': [{'id': 't2', 'goto': None}]},
                    {'id': 'phase3', 'name': 'Isolated', 'next': [{'id': 't3', 'goto': 'phase4'}]},
                    {'id': 'phase4', 'name': 'Isolated', 'next': [{'id': 't4', 'goto': 'phase3'}]},
                ],
                [('task1', 'phase1'), ('task2', 'phase2'), ('task3', 'phase3')],
                'not reachable',
                id='all_phases_reachable',
            ),
            pytest.param(
                [
                    {'id': 'phase0', 'name': 'Phase 0', 'next': [{'id': 't0', 'goto': 'phase1'}]},
                    {'id': 'phase1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {
                        'id': 'phase2',
                        'name': 'Phase 2',
                        'next': [{'id': 't2', 'goto': 'phase1'}, {'id': 't3', 'goto': None}],
                    },
                ],
                [('task1', 'phase1'), ('task2', 'phase2')],
                'Circular dependency detected',
                id='circular_dependency',
            ),
            pytest.param(
                [
                    {'id': 'phase0', 'name': 'Phase 0', 'next': [{'id': 't0', 'goto': 'phase1'}]},
                    {'id': 'phase1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': 'phase2'}]},
                    {'id': 'phase2', 'name': 'Phase 2', 'next': [{'id': 't2', 'goto': 'phase3'}]},
                    {
                        'id': 'phase3',
                        'name': 'Phase 3',
                        'next': [{'id': 't3', 'goto': 'phase1'}, {'id': 't4', 'goto': None}],
                    },
                ],
                [('task1', 'phase1'), ('task2', 'phase2'), ('task3', 'phase3')],
                'Circular dependency detected',
                id='indirect_circular_dependency',
            ),
            pytest.param(
                [
                    {'id': 'phase0', 'name': 'Phase 0', 'next': [{'id': 't0', 'goto': 'phase1'}]},
                    {
                        'id': 'phase1',
                        'name': 'Phase 1',
                        'next': [{'id': 't1', 'goto': 'phase1'}, {'id': 't2', 'goto': None}],
                    },
                ],
                [('task1', 'phase1')],
                'Circular dependency detected',
                id='self_referencing_dependency',
            ),
        ],
    )
    def test_validate_conditional_flow_error_cases(
        self, phases: list[dict[str, Any]], task_specs: list[tuple[str, str]], error_match: str
    ):
        """Parametrize conditional flow error cases to avoid repetitive tests."""
        tasks = [_notification_task(task_id, phase_id) for task_id, phase_id in task_specs]

        with pytest.raises(ValueError, match=error_match):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)


class TestReachableIds:
    """Test _reachable_ids function for finding reachable phases in a graph."""

    @pytest.mark.parametrize(
        (
            'start_id',
            'edges',
            'initial_visited',
            'expected',
            'expected_visited',
        ),
        [
            pytest.param(
                'A',
                {},
                set(),
                {'A'},
                {'A'},
                id='empty_graph_single_node',
            ),
            pytest.param(
                'A',
                {'A': set()},
                set(),
                {'A'},
                {'A'},
                id='single_node_no_outgoing_edges',
            ),
            pytest.param(
                'A',
                {'B': {'C'}, 'C': set()},
                set(),
                {'A'},
                {'A'},
                id='start_node_not_in_edges',
            ),
            pytest.param(
                'A',
                {'A': {'A'}},
                set(),
                {'A'},
                {'A'},
                id='single_node_self_loop',
            ),
            pytest.param(
                'A',
                {'A': {'B'}, 'B': {'C'}, 'C': set()},
                set(),
                {'A', 'B', 'C'},
                {'A', 'B', 'C'},
                id='linear_chain',
            ),
            pytest.param(
                'A',
                {'A': {'B', 'C'}, 'B': set(), 'C': set()},
                set(),
                {'A', 'B', 'C'},
                {'A', 'B', 'C'},
                id='branching_structure',
            ),
            pytest.param(
                'A',
                {'A': {'B'}, 'B': {'C'}, 'C': {'A'}},
                set(),
                {'A', 'B', 'C'},
                {'A', 'B', 'C'},
                id='cycle_handling',
            ),
            pytest.param(
                'A',
                {'A': {'B'}, 'B': set(), 'C': {'D'}, 'D': set()},
                set(),
                {'A', 'B'},
                {'A', 'B'},
                id='disconnected_graph',
            ),
            pytest.param(
                'A',
                {
                    'A': {'B', 'C'},
                    'B': {'D'},
                    'C': {'D', 'E'},
                    'D': {'F'},
                    'E': {'F'},
                    'F': set(),
                },
                set(),
                {'A', 'B', 'C', 'D', 'E', 'F'},
                {'A', 'B', 'C', 'D', 'E', 'F'},
                id='complex_graph_with_branches_and_merges',
            ),
            pytest.param(
                'A',
                {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': set()},
                set(),
                {'A', 'B', 'C', 'D'},
                {'A', 'B', 'C', 'D'},
                id='node_with_multiple_outgoing_edges',
            ),
            pytest.param(
                'A',
                {'A': {'B'}, 'B': {'C'}, 'C': {'B', 'D'}, 'D': {'A'}},
                set(),
                {'A', 'B', 'C', 'D'},
                {'A', 'B', 'C', 'D'},
                id='nested_cycles',
            ),
            pytest.param(
                'A',
                {'A': {'B', 'C'}, 'B': {'D'}, 'C': {'D'}, 'D': set()},
                {'C', 'D'},
                {'A', 'B', 'C', 'D'},
                {'A', 'B', 'C', 'D'},
                id='partial_visited_set',
            ),
            pytest.param(
                'A',
                {'A': {'B'}, 'B': {'A', 'C'}, 'C': set()},
                set('B'),
                {'A', 'B'},
                {'A', 'B'},
                id='visited_nodes_are_not_revisited',
            ),
        ],
    )
    def test_reachable_ids(
        self,
        start_id: str,
        edges: dict[str, set[str]],
        initial_visited: set[str],
        expected: set[str],
        expected_visited: set[str],
    ):
        """Parametrized coverage for _reachable_ids scenarios."""
        visited = set(initial_visited)
        result = _reachable_ids(start_id, edges, visited)

        assert result == expected
        assert visited == expected_visited


class TestResolveFlowSchema:
    """Tests for resolve_flow_schema."""

    @pytest.mark.asyncio
    async def test_returns_live_schema_for_conditional(self, mocker, conditional_flow_schema):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = None
        component = mocker.Mock()
        component.configuration_schema = conditional_flow_schema
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )

        result = await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

        assert result == conditional_flow_schema
        client.cache_flow_schema.assert_called_once_with(CONDITIONAL_FLOW_COMPONENT_ID, conditional_flow_schema)

    @pytest.mark.asyncio
    async def test_uses_cache_and_does_not_refetch(self, mocker, conditional_flow_schema):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = conditional_flow_schema
        fetch = mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(),
        )

        result = await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

        assert result == conditional_flow_schema
        fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_raises_on_empty_schema(self, mocker):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = None
        component = mocker.Mock()
        component.configuration_schema = None
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )

        with pytest.raises(ValueError, match='Could not retrieve the conditional flow'):
            await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'error',
        [
            # non-404 HTTP status error re-raised by fetch_component
            HTTPStatusError('boom', request=Request('GET', 'https://ai.keboola.com'), response=Response(500)),
            # transport/network error
            ConnectError('connection refused', request=Request('GET', 'https://ai.keboola.com')),
            # unexpected non-HTTP failure (e.g. a malformed AI Service payload failing model validation)
            RuntimeError('unexpected AI Service payload'),
        ],
        ids=['http_status_error', 'network_error', 'unexpected_error'],
    )
    async def test_raises_recoverable_error_on_fetch_failure(self, mocker, error):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = None
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(side_effect=error),
        )

        with pytest.raises(ValueError, match='Could not retrieve the conditional flow'):
            await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

    @pytest.mark.asyncio
    async def test_returns_bundled_schema_for_legacy(self, mocker):
        client = mocker.Mock()
        fetch = mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(),
        )

        result = await resolve_flow_schema(client, ORCHESTRATOR_COMPONENT_ID)

        assert result['properties']['phases']['items']['properties']  # bundled legacy schema
        assert 'dependsOn' in result['properties']['phases']['items']['properties']
        fetch.assert_not_called()


# --- Conditional flow variables (variableOverrides + JMESPath) round-trip ---

# JMESPath expression over a prior job's result; not one of the legacy enumerated `value` paths.
_JMESPATH_VALUE = "sum(job.result.output.tables[].metrics[?name=='importedRowsCount'][].value)"


def _variables_flow() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """A conditional flow that sets a flow variable from a JMESPath over a job result, consumes it via
    `variableOverrides`, and branches on a JMESPath-driven condition (mirrors a real CF-variables flow)."""
    phases = [
        {
            'id': 'extract',
            'name': 'Extract',
            'next': [
                {
                    'id': 'cond',
                    'name': 'Has rows',
                    'condition': {
                        'type': 'operator',
                        'operator': 'GREATER_THAN',
                        'operands': [
                            {'type': 'task', 'task': 'extract_task', 'value': _JMESPATH_VALUE},
                            {'type': 'const', 'value': 0},
                        ],
                    },
                    'goto': 'transform',
                },
                {'id': 'cond_default', 'goto': 'transform'},
            ],
        },
        {'id': 'transform', 'name': 'Transform', 'next': [{'id': 'end', 'goto': None}]},
    ]
    tasks = [
        {
            'id': 'extract_task',
            'name': 'Extract',
            'phase': 'extract',
            'task': {'type': 'job', 'mode': 'run', 'componentId': 'keboola.ex-google-drive', 'configId': '123'},
        },
        {
            'id': 'set_var',
            'name': 'importedRowsSum',
            'phase': 'transform',
            'task': {
                'type': 'variable',
                'name': 'importedRowsSum',
                'source': {'type': 'task', 'task': 'extract_task', 'value': _JMESPATH_VALUE},
            },
        },
        {
            'id': 'use_var',
            'name': 'Transform',
            'phase': 'transform',
            'task': {
                'type': 'job',
                'mode': 'run',
                'componentId': 'keboola.snowflake-transformation',
                'configId': 'abc',
                'variableOverrides': ['importedRowsSum'],
            },
        },
    ]
    return phases, tasks


class TestConditionalFlowVariablesRoundTrip:
    """get_flow_configuration must faithfully carry the CF-variables fields the live schema allows."""

    def test_variable_overrides_preserved_on_job_task(self):
        phases, tasks = _variables_flow()
        cfg = get_flow_configuration(phases=phases, tasks=tasks, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        job_task = next(t for t in cfg['tasks'] if t['id'] == 'use_var')['task']
        assert job_task['variableOverrides'] == ['importedRowsSum']

    def test_jmespath_value_preserved_in_variable_source(self):
        phases, tasks = _variables_flow()
        cfg = get_flow_configuration(phases=phases, tasks=tasks, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        var_task = next(t for t in cfg['tasks'] if t['id'] == 'set_var')['task']
        assert var_task['source']['value'] == _JMESPATH_VALUE

    def test_jmespath_value_preserved_in_phase_condition(self):
        phases, tasks = _variables_flow()
        cfg = get_flow_configuration(phases=phases, tasks=tasks, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        condition = cfg['phases'][0]['next'][0]['condition']
        assert condition['operands'][0]['value'] == _JMESPATH_VALUE

    def test_validate_flow_structure_accepts_variables_flow(self):
        phases, tasks = _variables_flow()
        cfg = get_flow_configuration(phases=phases, tasks=tasks, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        # Should not raise.
        validate_flow_structure(cfg, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_unknown_future_job_field_is_preserved(self):
        """extra='allow' keeps fields the live schema may add instead of silently dropping them."""
        phases, tasks = _variables_flow()
        tasks[0]['task']['someFutureField'] = {'k': 'v'}
        cfg = get_flow_configuration(phases=phases, tasks=tasks, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        extract_task = next(t for t in cfg['tasks'] if t['id'] == 'extract_task')['task']
        assert extract_task['someFutureField'] == {'k': 'v'}

    def test_unknown_future_condition_field_is_preserved(self):
        """extra='allow' applies to condition nodes too (not just task configs) via BaseExtraModel."""
        phases, tasks = _variables_flow()
        phases[0]['next'][0]['condition']['someFutureField'] = 'keep-me'
        cfg = get_flow_configuration(phases=phases, tasks=tasks, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)
        condition = cfg['phases'][0]['next'][0]['condition']
        assert condition['someFutureField'] == 'keep-me'

    def test_read_path_preserves_variables_via_from_api_response(self):
        """The READ/display path (Flow.from_api_response, used by get_flows) must also carry the
        variables fields — it routes through the same conditional-flow models as the write path."""
        phases, tasks = _variables_flow()
        api_config = APIFlowResponse.model_validate(
            {
                'id': 'flow-1',
                'name': 'Variables flow',
                'version': 1,
                'configuration': {'phases': phases, 'tasks': tasks},
            }
        )

        flow = Flow.from_api_response(api_config=api_config, flow_component_id=CONDITIONAL_FLOW_COMPONENT_ID)

        job_task = next(t for t in flow.configuration.tasks if t.id == 'use_var')
        assert job_task.task.variable_overrides == ['importedRowsSum']
        var_task = next(t for t in flow.configuration.tasks if t.id == 'set_var')
        assert var_task.task.source.value == _JMESPATH_VALUE
        assert flow.configuration.phases[0].next[0].condition.operands[0].value == _JMESPATH_VALUE


def _status_error(status: int) -> HTTPStatusError:
    request = Request('GET', 'https://connection.test.keboola.com/v2/storage/x')
    return HTTPStatusError('error', request=request, response=Response(status, request=request))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('probe_failures', 'expected'),
    [
        ([_status_error(404), _status_error(404)], NotFoundError),
        ([_status_error(403), _status_error(503)], 503),
        ([_status_error(503), _status_error(403)], 503),
        ([_status_error(404), _status_error(403)], 403),
        ([_status_error(403), _status_error(404)], 403),
    ],
    ids=['both_404', '403_then_503', '503_then_403', '404_then_403', '403_then_404'],
)
async def test_resolve_flow_by_id_reports_server_failure_over_caller_failure(
    probe_failures: list[Exception], expected: type[Exception] | int, mocker: Any
):
    client = mocker.Mock()
    client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=probe_failures)

    with pytest.raises(Exception) as exc_info:
        await resolve_flow_by_id(client, 'flow-1')

    if isinstance(expected, int):
        assert exc_info.value.response.status_code == expected
    else:
        assert isinstance(exc_info.value, expected)


def _data_app_task(task: dict[str, Any]) -> dict[str, Any]:
    return {
        'id': 't1',
        'name': 'Dashboard',
        'phase': 'p1',
        'task': {'type': 'job', 'mode': 'run', **copy.deepcopy(task)},
    }


_LEGACY_DATA_APP_TASK = {'componentId': 'keboola.data-apps', 'configId': 'cfg-1'}
_CONTROL_DATA_APP_TASK = {'componentId': 'keboola.data-app-control', 'configData': {'parameters': {'appId': 'app-1'}}}
_DATA_APP_CONFIG = {'id': 'cfg-1', 'configuration': {'parameters': {'id': 'app-1'}}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('task', 'control_component', 'expected_task', 'checks_stack'),
    [
        (_LEGACY_DATA_APP_TASK, {}, _CONTROL_DATA_APP_TASK, True),
        (_CONTROL_DATA_APP_TASK, {}, _CONTROL_DATA_APP_TASK, True),
        (
            {'componentId': 'keboola.data-app-control', 'configData': {'parameters': {'appId': 5823114}}},
            {},
            {'componentId': 'keboola.data-app-control', 'configData': {'parameters': {'appId': '5823114'}}},
            True,
        ),
        (
            {'componentId': 'keboola.data-app-control', 'configId': 'control-cfg'},
            {},
            {'componentId': 'keboola.data-app-control', 'configId': 'control-cfg'},
            True,
        ),
        (
            {'componentId': 'keboola.snowflake-transformation', 'configId': 'cfg-1'},
            {},
            {'componentId': 'keboola.snowflake-transformation', 'configId': 'cfg-1'},
            False,
        ),
        (_LEGACY_DATA_APP_TASK, _status_error(404), _LEGACY_DATA_APP_TASK, True),
    ],
    ids=[
        'legacy_rewritten',
        'control_kept',
        'control_int_app_id_stringified',
        'control_config_kept',
        'other_component_kept',
        'no_control_component',
    ],
)
async def test_normalize_data_app_tasks(
    task: dict[str, Any], control_component: Any, expected_task: dict[str, Any], checks_stack: bool, mocker: Any
):
    client = mocker.Mock()
    client.storage_client.component_detail = mocker.AsyncMock(side_effect=[control_component])
    client.storage_client.configuration_detail = mocker.AsyncMock(return_value=_DATA_APP_CONFIG)
    flow_configuration = {'phases': [], 'tasks': [_data_app_task(task)]}

    await normalize_data_app_tasks(client, flow_configuration)

    assert flow_configuration['tasks'] == [_data_app_task(expected_task)]
    assert client.storage_client.component_detail.await_count == int(checks_stack)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('task', 'control_component', 'data_app_config', 'expected'),
    [
        (_LEGACY_DATA_APP_TASK, {}, {'id': 'cfg-1', 'configuration': {'parameters': {}}}, 'has no data app ID'),
        (_LEGACY_DATA_APP_TASK, {}, _status_error(404), 'does not exist'),
        ({'componentId': 'keboola.data-apps'}, {}, _DATA_APP_CONFIG, 'has no "configId"'),
        ({'componentId': 'keboola.data-app-control', 'configData': {}}, {}, _DATA_APP_CONFIG, 'without "configData'),
        (_CONTROL_DATA_APP_TASK, _status_error(404), _DATA_APP_CONFIG, 'not available on this stack'),
        (_LEGACY_DATA_APP_TASK, _status_error(503), _DATA_APP_CONFIG, 503),
        (_LEGACY_DATA_APP_TASK, {}, _status_error(503), 503),
    ],
    ids=[
        'app_without_id',
        'missing_config',
        'legacy_without_config_id',
        'control_without_app_id',
        'control_without_component',
        'component_check_fails',
        'config_fetch_fails',
    ],
)
async def test_normalize_data_app_tasks_rejects_unresolvable_app(
    task: dict[str, Any], control_component: Any, data_app_config: Any, expected: str | int, mocker: Any
):
    client = mocker.Mock()
    client.storage_client.component_detail = mocker.AsyncMock(side_effect=[control_component])
    client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=[data_app_config])

    with pytest.raises(Exception) as exc_info:
        await normalize_data_app_tasks(client, {'phases': [], 'tasks': [_data_app_task(task)]})

    if isinstance(expected, int):
        assert exc_info.value.response.status_code == expected
    else:
        assert isinstance(exc_info.value, ValueError)
        assert expected in str(exc_info.value)
