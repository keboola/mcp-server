import pytest

from keboola_mcp_server.tools.flow.utils import (
    _check_legacy_circular_dependencies,
    ensure_legacy_phase_ids,
    ensure_legacy_task_ids,
    validate_legacy_flow_structure,
)

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
        phases = ensure_legacy_phase_ids(sample_phases)
        tasks = ensure_legacy_task_ids(sample_tasks)

        validate_legacy_flow_structure(phases, tasks)

    def test_validate_flow_structure_invalid_phase_dependency(self):
        """Test validation failure for invalid phase dependencies."""
        phases = ensure_legacy_phase_ids([{'id': 1, 'name': 'Phase 1', 'dependsOn': [999]}])  # Non-existent phase
        tasks = []

        with pytest.raises(ValueError, match='depends on non-existent phase 999'):
            validate_legacy_flow_structure(phases, tasks)

    def test_validate_flow_structure_invalid_task_phase(self):
        """Test validation failure for task referencing non-existent phase."""
        phases = ensure_legacy_phase_ids([{'id': 1, 'name': 'Phase 1'}])
        tasks = ensure_legacy_task_ids(
            [{'name': 'Bad Task', 'phase': 999, 'task': {'componentId': 'comp1'}}]  # Non-existent phase
        )

        with pytest.raises(ValueError, match='references non-existent phase 999'):
            validate_legacy_flow_structure(phases, tasks)


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
        phases = ensure_legacy_phase_ids([])
        tasks = ensure_legacy_task_ids([])

        validate_legacy_flow_structure(phases, tasks)
