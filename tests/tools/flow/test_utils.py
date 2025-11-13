from typing import Any

import pytest

from keboola_mcp_server.clients.client import CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.tools.flow.utils import (
    _check_legacy_circular_dependencies,
    _reachable_ids,
    ensure_legacy_phase_ids,
    ensure_legacy_task_ids,
    get_flow_configuration,
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

    def test_no_circular_dependencies(self):
        """Test flow with no circular dependencies."""
        phases = ensure_legacy_phase_ids(
            [
                {'id': 1, 'name': 'Phase 1'},
                {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]},
                {'id': 3, 'name': 'Phase 3', 'dependsOn': [2]},
            ]
        )

        _check_legacy_circular_dependencies(phases)

    def test_direct_circular_dependency(self):
        """Test detection of direct circular dependency."""
        phases = ensure_legacy_phase_ids(
            [{'id': 1, 'name': 'Phase 1', 'dependsOn': [2]}, {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]}]
        )

        with pytest.raises(ValueError, match='Circular dependency detected'):
            _check_legacy_circular_dependencies(phases)

    def test_indirect_circular_dependency(self):
        """Test detection of indirect circular dependency."""
        phases = ensure_legacy_phase_ids(
            [
                {'id': 1, 'name': 'Phase 1', 'dependsOn': [3]},
                {'id': 2, 'name': 'Phase 2', 'dependsOn': [1]},
                {'id': 3, 'name': 'Phase 3', 'dependsOn': [2]},
            ]
        )

        with pytest.raises(ValueError, match='Circular dependency detected'):
            _check_legacy_circular_dependencies(phases)

    def test_self_referencing_dependency(self):
        """Test detection of self-referencing dependency."""
        phases = ensure_legacy_phase_ids([{'id': 1, 'name': 'Phase 1', 'dependsOn': [1]}])

        with pytest.raises(ValueError, match='Circular dependency detected'):
            _check_legacy_circular_dependencies(phases)

    def test_complex_valid_dependencies(self):
        """Test complex but valid dependency structure."""
        phases = ensure_legacy_phase_ids(
            [
                {'id': 1, 'name': 'Phase 1'},
                {'id': 2, 'name': 'Phase 2'},
                {'id': 3, 'name': 'Phase 3', 'dependsOn': [1, 2]},
                {'id': 4, 'name': 'Phase 4', 'dependsOn': [3]},
                {'id': 5, 'name': 'Phase 5', 'dependsOn': [1]},
            ]
        )

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
        validate_flow_structure(flow_configuration, flow_type=ORCHESTRATOR_COMPONENT_ID)


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
        """Conditional builder should drop unset optional fields but goto=null remains."""
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
        assert phase['next'][0]['id'] == 'transition1'
        assert phase['next'][0]['goto'] is None
        assert 'enabled' not in task


class TestConditionalFlowValidation:
    """Test validation logic for conditional flows."""

    def test_validate_conditional_flow_success(self):
        phases = [
            {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {'id': 'phase2', 'name': 'End', 'next': [{'id': 't2', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'phase1'), _notification_task('task2', 'phase2')]

        validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_duplicate_phase_ids(self):
        phases = [
            {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {'id': 'phase1', 'name': 'Duplicate', 'next': [{'id': 't2', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'phase1'), _notification_task('task2', 'phase1')]

        with pytest.raises(ValueError, match='duplicate phase IDs'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_duplicate_task_ids(self):
        phases = [
            {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'phase1'), _notification_task('task1', 'phase1')]

        with pytest.raises(ValueError, match='duplicate task IDs'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_task_references_missing_phase(self):
        phases = [
            {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'missing-phase')]

        with pytest.raises(ValueError, match='references non-existent phase'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_transition_references_missing_phase(self):
        phases = [
            {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'ghost-phase'}]},
        ]
        tasks = [_notification_task('task1', 'phase1')]

        with pytest.raises(ValueError, match='references non-existent phase'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_requires_ending_phase(self):
        phases = [
            {'id': 'phase0', 'name': 'Start', 'next': [{'id': 't0', 'goto': 'phase1'}]},
            {'id': 'phase1', 'name': 'Loop', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {'id': 'phase2', 'name': 'Loop Again', 'next': [{'id': 't2', 'goto': 'phase1'}]},
        ]
        tasks = [_notification_task('task1', 'phase1'), _notification_task('task2', 'phase2')]

        with pytest.raises(ValueError, match='has no ending phases'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_requires_entry_phase(self):
        phases = [
            {'id': 'phase1', 'name': 'One', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {'id': 'phase2', 'name': 'Two', 'next': [{'id': 't2', 'goto': 'phase1'}, {'id': 't3', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'phase1'), _notification_task('task2', 'phase2')]

        with pytest.raises(ValueError, match='has no entry phase'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_single_entry_required(self):
        phases = [
            {'id': 'phase1', 'name': 'Entry A', 'next': [{'id': 't1', 'goto': None}]},
            {'id': 'phase2', 'name': 'Entry B', 'next': [{'id': 't2', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'phase1'), _notification_task('task2', 'phase2')]

        with pytest.raises(ValueError, match='multiple entry phases'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_all_phases_reachable(self):
        phases = [
            {'id': 'phase1', 'name': 'Start', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {'id': 'phase2', 'name': 'End', 'next': [{'id': 't2', 'goto': None}]},
            {'id': 'phase3', 'name': 'Isolated', 'next': [{'id': 't3', 'goto': 'phase4'}]},
            {'id': 'phase4', 'name': 'Isolated', 'next': [{'id': 't4', 'goto': 'phase3'}]},
        ]
        tasks = [
            _notification_task('task1', 'phase1'),
            _notification_task('task2', 'phase2'),
            _notification_task('task3', 'phase3'),
        ]

        with pytest.raises(ValueError, match='not reachable'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_circular_dependency(self):
        """Test detection of direct circular dependency in conditional flows."""
        phases = [
            {'id': 'phase0', 'name': 'Phase 0', 'next': [{'id': 't0', 'goto': 'phase1'}]},
            {'id': 'phase1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {'id': 'phase2', 'name': 'Phase 2', 'next': [{'id': 't2', 'goto': 'phase1'}, {'id': 't3', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'phase1'), _notification_task('task2', 'phase2')]

        with pytest.raises(ValueError, match='Circular dependency detected'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_indirect_circular_dependency(self):
        """Test detection of indirect circular dependency in conditional flows."""
        phases = [
            {'id': 'phase0', 'name': 'Phase 0', 'next': [{'id': 't0', 'goto': 'phase1'}]},
            {'id': 'phase1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {'id': 'phase2', 'name': 'Phase 2', 'next': [{'id': 't2', 'goto': 'phase3'}]},
            {'id': 'phase3', 'name': 'Phase 3', 'next': [{'id': 't3', 'goto': 'phase1'}, {'id': 't4', 'goto': None}]},
        ]
        tasks = [
            _notification_task('task1', 'phase1'),
            _notification_task('task2', 'phase2'),
            _notification_task('task3', 'phase3'),
        ]

        with pytest.raises(ValueError, match='Circular dependency detected'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_self_referencing_dependency(self):
        """Test detection of self-referencing dependency in conditional flows."""
        phases = [
            {'id': 'phase0', 'name': 'Phase 0', 'next': [{'id': 't0', 'goto': 'phase1'}]},
            {'id': 'phase1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': 'phase1'}, {'id': 't2', 'goto': None}]},
        ]
        tasks = [_notification_task('task1', 'phase1')]

        with pytest.raises(ValueError, match='Circular dependency detected'):
            validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

    def test_validate_conditional_flow_complex_valid_dependencies(self):
        """Test complex but valid dependency structure in conditional flows."""
        phases = [
            {'id': 'phase1', 'name': 'Phase 1', 'next': [{'id': 't1', 'goto': 'phase2'}]},
            {
                'id': 'phase2',
                'name': 'Phase 2',
                'next': [{'id': 't2', 'goto': 'phase3'}, {'id': 't3', 'goto': 'phase4'}],
            },
            {'id': 'phase3', 'name': 'Phase 3', 'next': [{'id': 't4', 'goto': None}]},
            {'id': 'phase4', 'name': 'Phase 4', 'next': [{'id': 't5', 'goto': None}]},
        ]
        tasks = [
            _notification_task('task1', 'phase1'),
            _notification_task('task2', 'phase2'),
            _notification_task('task3', 'phase3'),
            _notification_task('task4', 'phase4'),
        ]

        # Should not raise any errors
        validate_flow_structure({'phases': phases, 'tasks': tasks}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)


class TestReachableIds:
    """Test _reachable_ids function for finding reachable phases in a graph."""

    def test_empty_graph_single_node(self):
        """Test with a single node and no edges."""
        edges: dict[str, set[str]] = {}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A'}
        assert visited == {'A'}

    def test_single_node_no_outgoing_edges(self):
        """Test with a node that has no outgoing edges."""
        edges: dict[str, set[str]] = {'A': set()}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A'}
        assert visited == {'A'}

    def test_start_node_not_in_edges(self):
        """Test when start node is not in edges dictionary."""
        edges: dict[str, set[str]] = {'B': {'C'}, 'C': set()}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A'}
        assert visited == {'A'}

    def test_single_node_self_loop(self):
        """Test with a node that has a self-loop."""
        edges: dict[str, set[str]] = {'A': {'A'}}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A'}
        assert visited == {'A'}

    def test_linear_chain(self):
        """Test a simple linear chain: A -> B -> C."""
        edges: dict[str, set[str]] = {'A': {'B'}, 'B': {'C'}, 'C': set()}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B', 'C'}
        assert visited == {'A', 'B', 'C'}

    def test_branching_structure(self):
        """Test branching: A -> B, A -> C."""
        edges: dict[str, set[str]] = {'A': {'B', 'C'}, 'B': set(), 'C': set()}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B', 'C'}
        assert visited == {'A', 'B', 'C'}

    def test_cycle_handling(self):
        """Test that cycles are handled correctly: A -> B -> C -> A."""
        edges: dict[str, set[str]] = {'A': {'B'}, 'B': {'C'}, 'C': {'A'}}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B', 'C'}
        assert visited == {'A', 'B', 'C'}

    def test_disconnected_graph(self):
        """Test with disconnected components - only reachable nodes are returned."""
        edges: dict[str, set[str]] = {'A': {'B'}, 'B': set(), 'C': {'D'}, 'D': set()}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B'}
        assert 'C' not in result
        assert 'D' not in result

    def test_already_visited_nodes(self):
        """Test that already visited nodes are not revisited."""
        edges: dict[str, set[str]] = {'A': {'B'}, 'B': {'A', 'C'}, 'C': set()}
        visited: set[str] = {'B'}
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B'}
        assert visited == {'A', 'B'}
        assert 'C' not in result

    def test_complex_graph_with_branches_and_merges(self):
        """Test a complex graph with multiple branches and merges."""
        edges: dict[str, set[str]] = {
            'A': {'B', 'C'},
            'B': {'D'},
            'C': {'D', 'E'},
            'D': {'F'},
            'E': {'F'},
            'F': set(),
        }
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B', 'C', 'D', 'E', 'F'}
        assert visited == {'A', 'B', 'C', 'D', 'E', 'F'}

    def test_node_with_multiple_outgoing_edges(self):
        """Test node with multiple outgoing edges to different targets."""
        edges: dict[str, set[str]] = {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': set()}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B', 'C', 'D'}
        assert visited == {'A', 'B', 'C', 'D'}

    def test_nested_cycles(self):
        """Test graph with nested cycles."""
        edges: dict[str, set[str]] = {'A': {'B'}, 'B': {'C'}, 'C': {'B', 'D'}, 'D': {'A'}}
        visited: set[str] = set()
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B', 'C', 'D'}
        assert visited == {'A', 'B', 'C', 'D'}

    def test_partial_visited_set(self):
        """Test with some nodes already in visited set."""
        edges: dict[str, set[str]] = {'A': {'B', 'C'}, 'B': {'D'}, 'C': {'D'}, 'D': set()}
        visited: set[str] = {'C', 'D'}
        result = _reachable_ids('A', edges, visited)
        assert result == {'A', 'B', 'C', 'D'}
        assert visited == {'A', 'B', 'C', 'D'}
