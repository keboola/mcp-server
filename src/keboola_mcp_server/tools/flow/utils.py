"""Utility functions for flow management."""

import json
import logging
from importlib import resources
from typing import Any

from keboola_mcp_server.client import JsonDict
from keboola_mcp_server.tools.flow.model import FlowPhase, FlowTask

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
    """Ensure all phases have unique IDs and proper structure using Pydantic validation"""
    processed_phases = []
    used_ids = set()

    for i, phase in enumerate(phases):
        phase_data = phase.copy()

        if 'id' not in phase_data or not phase_data['id']:
            phase_id = i + 1
            while phase_id in used_ids:
                phase_id += 1
            phase_data['id'] = phase_id

        if 'name' not in phase_data:
            phase_data['name'] = f"Phase {phase_data['id']}"

        try:
            validated_phase = FlowPhase.model_validate(phase_data)
            used_ids.add(validated_phase.id)
            processed_phases.append(validated_phase)
        except Exception as e:
            raise ValueError(f'Invalid phase configuration: {e}')

    return processed_phases


def ensure_task_ids(tasks: list[dict[str, Any]]) -> list[FlowTask]:
    """Ensure all tasks have unique IDs and proper structure using Pydantic validation"""
    processed_tasks = []
    used_ids = set()

    # Task ID pattern inspired by Kai-Bot implementation:
    # https://github.com/keboola/kai-bot/blob/main/src/keboola/kaibot/backend/flow_backend.py
    #
    # ID allocation strategy:
    # - Phase IDs: 1, 2, 3... (small sequential numbers)
    # - Task IDs: 20001, 20002, 20003... (high sequential numbers)
    #
    # This namespace separation technique ensures phase and task IDs never collide
    # while maintaining human-readable sequential numbering.
    task_counter = 20001

    for task in tasks:
        task_data = task.copy()

        if 'id' not in task_data or not task_data['id']:
            while task_counter in used_ids:
                task_counter += 1
            task_data['id'] = task_counter
            task_counter += 1

        if 'name' not in task_data:
            task_data['name'] = f"Task {task_data['id']}"

        if 'task' not in task_data:
            raise ValueError(f"Task {task_data['id']} missing 'task' configuration")

        if 'componentId' not in task_data.get('task', {}):
            raise ValueError(f"Task {task_data['id']} missing componentId in task configuration")

        task_obj = task_data.get('task', {})
        if 'mode' not in task_obj:
            task_obj['mode'] = 'run'
        task_data['task'] = task_obj

        try:
            validated_task = FlowTask.model_validate(task_data)
            used_ids.add(validated_task.id)
            processed_tasks.append(validated_task)
        except Exception as e:
            raise ValueError(f'Invalid task configuration: {e}')

    return processed_tasks


def validate_flow_structure(phases: list[FlowPhase], tasks: list[FlowTask]) -> None:
    """Validate that the flow structure is valid - now using Pydantic models"""
    phase_ids = {phase.id for phase in phases}

    for phase in phases:
        for dep_id in phase.depends_on:
            if dep_id not in phase_ids:
                raise ValueError(f'Phase {phase.id} depends on non-existent phase {dep_id}')

    for task in tasks:
        if task.phase not in phase_ids:
            raise ValueError(f'Task {task.id} references non-existent phase {task.phase}')

    _check_circular_dependencies(phases)


def _check_circular_dependencies(phases: list[FlowPhase]) -> None:
    """
    Optimized circular dependency check that:
    1. Uses O(n) dict lookup instead of O(n²) list search
    2. Returns detailed cycle path information for better debugging
    """

    # Build efficient lookup graph once - O(n) optimization
    graph = {phase.id: phase.depends_on for phase in phases}

    def _has_cycle(phase_id: Any, _visited: set, rec_stack: set, path: list[Any]) -> list[Any] | None:
        """
        Returns None if no cycle found, or List[phase_ids] representing the cycle path.
        """
        _visited.add(phase_id)
        rec_stack.add(phase_id)
        path.append(phase_id)

        dependencies = graph.get(phase_id, [])

        for dep_id in dependencies:
            if dep_id not in _visited:
                cycle = _has_cycle(dep_id, _visited, rec_stack, path)
                if cycle is not None:
                    return cycle

            elif dep_id in rec_stack:
                try:
                    cycle_start_index = path.index(dep_id)
                    return path[cycle_start_index:] + [dep_id]
                except ValueError:
                    return [phase_id, dep_id]

        path.pop()
        rec_stack.remove(phase_id)
        return None

    visited = set()
    for phase in phases:
        if phase.id not in visited:
            cycle_path = _has_cycle(phase.id, visited, set(), [])
            if cycle_path is not None:
                cycle_str = ' -> '.join(str(pid) for pid in cycle_path)
                raise ValueError(f'Circular dependency detected in phases: {cycle_str}')
