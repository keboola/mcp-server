"""Utility functions for flow management."""

import json
import logging
from importlib import resources
from typing import Any, cast

from keboola_mcp_server.client import JsonDict
from keboola_mcp_server.tools.flows.model import FlowPhase, FlowTask
from keboola_mcp_server.tools.validation import validate_flow_configuration_against_schema

LOG = logging.getLogger(__name__)

RESOURCES = 'keboola_mcp_server.resources'
FLOW_SCHEMA_RESOURCE = 'flow-schema.json'


def _load_schema() -> JsonDict:
    """Load the flow schema from the resources."""
    with resources.open_text(RESOURCES, FLOW_SCHEMA_RESOURCE, encoding='utf-8') as f:
        return json.load(f)


def get_schema_as_markdown() -> str:
    """Return the flow schema as a markdown formatted string."""
    schema = _load_schema()
    return f'```json\n{json.dumps(schema, indent=2)}\n```'


def ensure_phase_ids(phases: list[dict[str, Any]]) -> list[FlowPhase]:
    """
    Ensure all phases have IDs, assigning sequential integers if missing.
    The function iterates through a list of phases. If a phase lacks an 'id',
    it assigns the next available integer ID, starting from the maximum existing
    ID plus one. This ensures all phases can be reliably referenced.
    """
    if not phases:
        return []

    max_id = 0
    # First pass: identify max existing ID to avoid collisions
    for phase in phases:
        if 'id' in phase and isinstance(phase['id'], int) and phase['id'] > max_id:
            max_id = phase['id']

    # Second pass: assign IDs to phases without one
    next_id = max_id + 1
    processed_phases = []
    for phase in phases:
        if 'id' not in phase:
            phase['id'] = next_id
            next_id += 1
        processed_phases.append(FlowPhase.model_validate(phase))

    return processed_phases


def ensure_task_ids(tasks: list[dict[str, Any]]) -> list[FlowTask]:
    """
    Ensure all tasks have IDs, assigning sequential integers if missing.
    Similar to _ensure_phase_ids, this function processes a list of tasks.
    It assigns a unique integer ID to any task missing one, preventing conflicts
    by starting from the maximum existing task ID plus one.
    """
    if not tasks:
        return []

    max_id = 0
    # First pass: find the maximum existing integer ID among tasks
    for task in tasks:
        if 'id' in task and isinstance(task['id'], int) and task['id'] > max_id:
            max_id = task['id']

    # Second pass: assign new IDs to tasks that are missing them
    next_id = max_id + 1
    processed_tasks = []
    for task in tasks:
        if 'id' not in task:
            task['id'] = next_id
            next_id += 1
        processed_tasks.append(FlowTask.model_validate(task))

    return processed_tasks


def validate_flow_structure(phases: list[FlowPhase], tasks: list[FlowTask]) -> None:
    """
    Validate the structural integrity of the flow, checking dependencies and phase references.
    This function performs critical structural checks:
    1. It verifies that every task references a valid phase.
    2. It checks for circular dependencies among phases.
    3. It ensures the overall flow configuration adheres to the defined JSON schema.
    An error is raised if any of these checks fail.
    """
    phase_ids = {phase.id for phase in phases}
    for task in tasks:
        if task.phase not in phase_ids:
            msg = f'Task "{task.name}" references non-existent phase "{task.phase}".'
            raise ValueError(msg)

    _check_circular_dependencies(phases)

    # Validate the full configuration against the schema after processing
    flow_configuration = {
        'phases': [phase.model_dump(by_alias=True) for phase in phases],
        'tasks': [task.model_dump(by_alias=True) for task in tasks],
    }
    flow_configuration = cast(JsonDict, flow_configuration)
    validate_flow_configuration_against_schema(flow_configuration)


def _check_circular_dependencies(phases: list[FlowPhase]) -> None:
    """Check for circular dependencies among phases."""
    phase_map = {phase.id: phase for phase in phases}
    # Track visited nodes for the current traversal and the recursion stack
    visited = set()
    recursion_stack = set()
    path: list[Any] = []

    def _has_cycle(phase_id: Any, _visited: set, rec_stack: set, path: list[Any]) -> list[Any] | None:
        """
        Recursively checks for cycles in the phase dependency graph.
        - `_visited`: keeps track of all visited nodes to avoid redundant checks.
        - `rec_stack`: tracks nodes in the current recursion path to detect back edges (cycles).
        - `path`: reconstructs the dependency chain to provide informative error messages.
        """
        _visited.add(phase_id)
        rec_stack.add(phase_id)
        path.append(phase_id)

        phase = phase_map.get(phase_id)
        if phase:
            for dep_id in phase.depends_on:
                if dep_id not in _visited:
                    # If a cycle is found in a deeper recursive call, propagate the result
                    if result := _has_cycle(dep_id, _visited, rec_stack, path):
                        return result
                # If a dependency is already in the recursion stack, a cycle is detected
                elif dep_id in rec_stack:
                    path.append(dep_id)
                    return path

        # Backtrack: remove the node from the recursion stack and path before returning
        rec_stack.remove(phase_id)
        path.pop()
        return None

    for phase in phases:
        if phase.id not in visited:
            if cycle_path := _has_cycle(phase.id, visited, recursion_stack, path):
                # Format the cycle path for a clear error message
                path_str = ' -> '.join(map(str, cycle_path))
                msg = f'Circular dependency detected in flow phases: {path_str}.'
                raise ValueError(msg)
