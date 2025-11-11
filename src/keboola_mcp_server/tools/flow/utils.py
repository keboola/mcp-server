"""Utility functions for flow management."""

import json
import logging
from collections import Counter, defaultdict
from importlib import resources
from typing import Any, Mapping, Sequence

from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    FLOW_TYPES,
    ORCHESTRATOR_COMPONENT_ID,
    FlowType,
    KeboolaClient,
)
from keboola_mcp_server.clients.storage import APIFlowResponse, JsonDict
from keboola_mcp_server.tools.flow.model import (
    ConditionalFlowPhase,
    ConditionalFlowTask,
    FlowPhase,
    FlowSummary,
    FlowTask,
)

LOG = logging.getLogger(__name__)

RESOURCES = 'keboola_mcp_server.resources'
FLOW_SCHEMAS: Mapping[FlowType, str] = {
    CONDITIONAL_FLOW_COMPONENT_ID: 'conditional-flow-schema.json',
    ORCHESTRATOR_COMPONENT_ID: 'flow-schema.json',
}


def _load_schema(flow_type: FlowType) -> JsonDict:
    """Load a schema from the resources folder."""
    with resources.open_text(RESOURCES, FLOW_SCHEMAS[flow_type], encoding='utf-8') as f:
        return json.load(f)


def get_schema_as_markdown(flow_type: FlowType) -> str:
    """Return the flow schema as a markdown formatted string."""
    schema = _load_schema(flow_type=flow_type)
    return f'```json\n{json.dumps(schema, indent=2)}\n```'


def get_flow_configuration(
    phases: list[dict[str, Any]] | None, tasks: list[dict[str, Any]] | None, flow_type: FlowType
) -> JsonDict:
    """Get the flow configuration from tasks and phases. For legacy flows, apply necessary sanitization.

    :param phases: The list of phases.
    :param tasks: The list of tasks.
    :param flow_type: The type of flow to convert.
    :return: The dictionary containing the flow configuration (phases and tasks) serialized to JSON.
    """
    if flow_type == ORCHESTRATOR_COMPONENT_ID:
        processed_phases = ensure_legacy_phase_ids(phases or [])
        processed_tasks = ensure_legacy_task_ids(tasks or [])
        return {
            'phases': [phase.model_dump(by_alias=True) for phase in processed_phases],
            'tasks': [task.model_dump(by_alias=True) for task in processed_tasks],
        }
    else:
        processed_phases = [ConditionalFlowPhase.model_validate(phase) for phase in phases or []]
        processed_tasks = [ConditionalFlowTask.model_validate(task) for task in tasks or []]
        return {
            'phases': [phase.model_dump(exclude_unset=True, by_alias=True) for phase in processed_phases],
            'tasks': [task.model_dump(exclude_unset=True, by_alias=True) for task in processed_tasks],
        }


def validate_flow_structure(
    flow_configuration: JsonDict,
    flow_type: FlowType,
) -> None:
    """
    Validate that the flow structure is valid by checking logical and structural constraints.

    :param flow_configuration: The flow configuration to validate.
    :param flow_type: The type of flow to validate.
    :raises ValueError: If the flow structure is invalid.
    """
    if flow_type == ORCHESTRATOR_COMPONENT_ID:
        _validate_legacy_flow_structure(
            phases=[FlowPhase.model_validate(phase) for phase in flow_configuration.get('phases', [])],
            tasks=[FlowTask.model_validate(task) for task in flow_configuration.get('tasks', [])],
        )
    else:
        _validate_conditional_flow_structure(
            phases=[ConditionalFlowPhase.model_validate(phase) for phase in flow_configuration.get('phases', [])],
            tasks=[ConditionalFlowTask.model_validate(task) for task in flow_configuration.get('tasks', [])],
        )


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


async def resolve_flow_by_id(client: KeboolaClient, flow_id: str) -> tuple[APIFlowResponse, FlowType]:
    """
    Resolve a flow by ID across all flow types.

    :param client: Keboola client instance.
    :param flow_id: The flow configuration ID to resolve.
    :return: Tuple of (APIFlowResponse, flow_type) if found.
    :raises ValueError: If flow cannot be resolved in any flow type.
    """
    for flow_type in FLOW_TYPES:
        try:
            raw_flow = await client.storage_client.configuration_detail(
                component_id=flow_type, configuration_id=flow_id
            )
            api_flow = APIFlowResponse.model_validate(raw_flow)
            return api_flow, flow_type
        except Exception:
            continue

    raise ValueError(f'Flow configuration "{flow_id}" not found')


async def get_flows_by_ids(client: KeboolaClient, flow_ids: Sequence[str]) -> list[FlowSummary]:
    flows: list[FlowSummary] = []

    for flow_id in flow_ids:
        try:
            api_flow, flow_type = await resolve_flow_by_id(client, flow_id)
            flow_summary = FlowSummary.from_api_response(api_config=api_flow, flow_component_id=flow_type)
            flows.append(flow_summary)
        except ValueError as e:
            LOG.warning(f'Flow {flow_id} not found: {e}')
            continue

    return flows


async def get_flows_by_type(client: KeboolaClient, flow_type: FlowType) -> list[FlowSummary]:
    raw_flows = await client.storage_client.configuration_list(component_id=flow_type)
    return [
        FlowSummary.from_api_response(api_config=APIFlowResponse.model_validate(raw), flow_component_id=flow_type)
        for raw in raw_flows
    ]


async def get_all_flows(client: KeboolaClient) -> list[FlowSummary]:
    all_flows = []
    for flow_type in FLOW_TYPES:
        flows = await get_flows_by_type(client=client, flow_type=flow_type)
        all_flows.extend(flows)
    return all_flows


def _validate_legacy_flow_structure(
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


def _validate_conditional_flow_structure(
    phases: list[ConditionalFlowPhase],
    tasks: list[ConditionalFlowTask],
) -> None:
    """
    Validate that the conditional flow structure is valid by checking reachability, existence of entry phase and ending
    phase.
    :param phases: List of conditional flow phases to validate.
    :param tasks: List of conditional flow tasks to validate.
    :raises ValueError: If the flow structure is invalid.
    """

    # Validate that there are no duplicate phase or task IDs
    counter_phases = Counter([phase.id for phase in phases])
    phase_ids = set(counter_phases)
    if counter_phases.most_common(1)[0][1] > 1:
        duplicate_phase_ids = [pid for pid, count in counter_phases.most_common() if count > 1]
        raise ValueError(f'Flow contains duplicate phase IDs: {duplicate_phase_ids}.')
    counter_tasks = Counter([task.id for task in tasks])
    if counter_tasks.most_common(1)[0][1] > 1:
        duplicate_task_ids = [tid for tid, count in counter_tasks.most_common() if count > 1]
        raise ValueError(f'Flow contains duplicate task IDs: {duplicate_task_ids}.')

    # Validate that all tasks reference existing phases
    for task in tasks:
        if task.phase not in phase_ids:
            raise ValueError(f'Task {task.id} references non-existent phase {task.phase}')

    # Build graph of transitions: phase_id -> set of target phase IDs
    # Also track which phases have incoming transitions
    succ_phases = defaultdict[str, set[str]](set)
    pred_phases = defaultdict[str, set[str]](set)
    ending_phases = set[str]()

    for phase in phases:
        if not phase.next:
            ending_phases.add(phase.id)
        else:
            for transition in phase.next:
                if transition.goto is None:
                    ending_phases.add(phase.id)
                else:
                    if transition.goto not in phase_ids:
                        raise ValueError(
                            f'Phase {phase.id} has a transition that references non-existent phase {transition.goto}'
                        )
                    succ_phases[phase.id].add(transition.goto)
                    pred_phases[transition.goto].add(phase.id)

    # Check that we have at least one ending phase
    if not ending_phases:
        raise ValueError(
            'Flow has no ending phases. Each conditional flow must have at least one ending phase. Any ending phase '
            'has either no transitions at all or contains transition with goto: null referencing end of the flow.'
        )

    # Find entry phase (phase with no incoming transitions)
    entry_phase = [pid for pid in phase_ids if not pred_phases[pid]]

    if not entry_phase:
        raise ValueError(
            'Flow has no entry phase. Each conditional flow must have exactly one entry phase. An entry phase has no '
            'incoming transitions; no transition from another phase leads to it.'
        )
    if len(entry_phase) > 1:
        raise ValueError(
            f'Flow has multiple entry phases ({len(entry_phase)}): {entry_phase}. Each conditional flow must have '
            'exactly one entry phase. Either merge the entry phases into one or redefine the transitions to form a '
            'single entry phase.'
        )

    # All phases must be reachable from the entry point
    reachable_ids = _reachable_ids(entry_phase[0], succ_phases, set[str]())
    if reachable_ids != phase_ids:
        raise ValueError(
            f'Flow has phases that are not reachable from the entry phase ({entry_phase[0]}): '
            f'{phase_ids - reachable_ids}. All phases must be reachable from the entry phase by a valid path of '
            'transitions.'
        )


def _reachable_ids(start_id: str, edges: dict[str, set[str]], visited: set[str]) -> set[str]:
    """
    Find all phases reachable from a starting phase using DFS.

    :param start_id: The ID of the starting phase.
    :param edges: Dictionary mapping phase IDs to sets of target phase IDs.
    :param visited: Set of phase IDs that have been visited.
    :return: Set of IDs reachable from the starting ID.
    """

    visited.add(start_id)
    for target_id in edges.get(start_id, []):
        if target_id not in visited:
            visited.update(_reachable_ids(target_id, edges, visited))
    return visited
