"""Utility functions for flow management."""

import json
import logging
from importlib import resources
from typing import Any, Mapping, Sequence

from keboola_mcp_server.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    FLOW_TYPE,
    FLOW_TYPES,
    ORCHESTRATOR_COMPONENT_ID,
    JsonDict,
    KeboolaClient,
)
from keboola_mcp_server.tools.flow.api_models import APIFlowResponse
from keboola_mcp_server.tools.flow.model import (
    FlowPhase,
    FlowSummary,
    FlowTask,
)

LOG = logging.getLogger(__name__)

RESOURCES = 'keboola_mcp_server.resources'
FLOW_SCHEMAS: Mapping[FLOW_TYPE, str] = {
    CONDITIONAL_FLOW_COMPONENT_ID: 'conditional-flow-schema.json',
    ORCHESTRATOR_COMPONENT_ID: 'flow-schema.json'
}


def _load_schema(flow_type: FLOW_TYPE) -> JsonDict:
    """Load a schema from the resources folder."""
    with resources.open_text(RESOURCES, FLOW_SCHEMAS[flow_type], encoding='utf-8') as f:
        return json.load(f)


def get_schema_as_markdown(flow_type: FLOW_TYPE) -> str:
    """Return the flow schema as a markdown formatted string."""
    schema = _load_schema(flow_type=flow_type)
    return f'```json\n{json.dumps(schema, indent=2)}\n```'


def validate_legacy_flow_structure(
    phases: list[FlowPhase],
    tasks: list[FlowTask],
) -> None:
    """Validate that the legacy flow structure is valid (phases exist and graph is not circular)"""
    phase_ids = {phase.id for phase in phases}

    for phase in phases:
        for dep_id in phase.depends_on:
            if dep_id not in phase_ids:
                raise ValueError(f'Phase {phase.id} depends on non-existent phase {dep_id}')

    for task in tasks:
        if task.phase not in phase_ids:
            raise ValueError(f'Task {task.id} references non-existent phase {task.phase}')

    _check_legacy_circular_dependencies(phases)


def _check_legacy_circular_dependencies(phases: list[FlowPhase]) -> None:
    """
    Check for circular dependencies in legacy flows using depends_on relationships.
    Uses optimized O(n) lookup.
    """
    # Build dependency graph
    graph = {phase.id: phase.depends_on for phase in phases}

    # Use DFS to detect cycles
    visited = set()
    rec_stack = set()

    def has_cycle(node: str | int) -> bool:
        if node in rec_stack:
            return True
        if node in visited:
            return False

        visited.add(node)
        rec_stack.add(node)

        for neighbor in graph.get(node, []):
            if has_cycle(neighbor):
                return True

        rec_stack.remove(node)
        return False

    # Check each phase for cycles
    for phase_id in graph:
        if phase_id not in visited:
            if has_cycle(phase_id):
                raise ValueError(f'Circular dependency detected involving phase {phase_id}')


def ensure_legacy_phase_ids(phases: list[dict[str, Any]]) -> list[FlowPhase]:
    """Ensure all phases have unique IDs and proper structure for legacy flows"""
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


def ensure_legacy_task_ids(tasks: list[dict[str, Any]]) -> list[FlowTask]:
    """Ensure all tasks have unique IDs and proper structure using Pydantic validation for legacy flows"""
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


async def _get_flows_by_ids(
    client: KeboolaClient,
    flow_ids: Sequence[str]
) -> list[FlowSummary]:
    flows = []

    for flow_id in flow_ids:
        raw_flow = None
        found_flow_type = None
        for flow_type in FLOW_TYPES:
            try:
                raw_flow = await client.storage_client.flow_detail(flow_id, flow_type)
                found_flow_type = flow_type
                break
            except Exception:
                continue

        if raw_flow and found_flow_type:
            api_flow = APIFlowResponse.model_validate(raw_flow)
            flows.append(FlowSummary.from_api_response(api_config=api_flow, flow_component_id=found_flow_type))
        else:
            LOG.warning(f'Failed to retrieve flow {flow_id}.')

    return flows


async def _get_flows_by_type(
    client: KeboolaClient,
    flow_type: FLOW_TYPE
) -> list[FlowSummary]:
    raw_flows = await client.storage_client.flow_list(flow_type=flow_type)
    return [
        FlowSummary.from_api_response(api_config=APIFlowResponse.model_validate(raw), flow_component_id=flow_type)
        for raw in raw_flows
    ]


async def _get_all_flows(client: KeboolaClient) -> list[FlowSummary]:
    all_flows = []
    for flow_type in FLOW_TYPES:
        flows = await _get_flows_by_type(client=client, flow_type=flow_type)
        all_flows.extend(flows)
    return all_flows
