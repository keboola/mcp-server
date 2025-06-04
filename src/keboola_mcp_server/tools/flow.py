"""Flow management tools for the MCP server (orchestrations/flows)."""

import json
import logging
from importlib import resources
from typing import Annotated, Any, Sequence, cast

from fastmcp import Context, FastMCP
from pydantic import Field

from keboola_mcp_server.client import JsonDict, KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.tools._validate import validate_flow_configuration_against_schema
from keboola_mcp_server.tools.components.model import (
    FlowConfiguration,
    FlowConfigurationResponse,
    FlowPhase,
    FlowTask,
    ReducedFlow,
)

LOG = logging.getLogger(__name__)

RESOURCES = 'keboola_mcp_server.resources'
FLOW_SCHEMA_RESOURCE = 'flow-schema.json'


def _load_schema() -> JsonDict:
    with resources.open_text(RESOURCES, FLOW_SCHEMA_RESOURCE, encoding='utf-8') as f:
        return json.load(f)


def get_schema_as_markdown() -> str:
    schema = _load_schema()
    return f'```json\n{json.dumps(schema, indent=2)}\n```'


def add_flow_tools(mcp: FastMCP) -> None:
    """Add flow tools to the MCP server."""
    flow_tools = [create_flow, retrieve_flows, update_flow, get_flow_detail, get_flow_schema]

    for tool in flow_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(tool)

    LOG.info('Flow tools initialized.')


@tool_errors()
@with_session_state()
async def get_flow_schema(ctx: Context) -> Annotated[str, Field(description='The configuration schema of Flow.')]:
    """Returns the JSON schema that defines the structure of Flow configurations."""

    LOG.info('Returning flow configuration schema')
    return get_schema_as_markdown()


@tool_errors()
@with_session_state()
async def create_flow(
    ctx: Context,
    name: Annotated[str, Field(description='A short, descriptive name for the flow.')],
    description: Annotated[str, Field(description='Detailed description of the flow purpose.')],
    phases: Annotated[list[dict[str, Any]], Field(description='List of phase definitions.')],
    tasks: Annotated[list[dict[str, Any]], Field(description='List of task definitions.')],
) -> Annotated[FlowConfiguration, Field(description='Created flow configuration.')]:
    """
    Creates a new flow configuration in Keboola.
    A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
    how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
    Each flow is composed of:
    - Tasks: individual component configurations (e.g., extractors, writers, transformations).
    - Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

    CONSIDERATIONS:
    - The `phases` and `tasks` parameters must conform to the Keboola Flow JSON schema.
    - Each task and phase must include at least: `id` and `name`.
    - Each task must reference an existing component configuration in the project.
    - Items in the `dependsOn` phase field reference ids of other phases.

    USAGE:
    Use this tool to automate multi-step data workflows. This is ideal for:
    - Creating ETL/ELT orchestration.
    - Coordinating dependencies between components.
    - Structuring parallel and sequential task execution.

    EXAMPLES:
    - user_input: Orchestrate all my JIRA extractors.
        - fill `tasks` parameter with the tasks for the JIRA extractors
        - determine dependencies between the JIRA extractors
        - fill `phases` parameter by grouping tasks into phases
    """

    processed_phases = _ensure_phase_ids(phases)
    processed_tasks = _ensure_task_ids(tasks)
    _validate_flow_structure(processed_phases, processed_tasks)

    flow_configuration = {
        'phases': [phase.model_dump(by_alias=True) for phase in processed_phases],
        'tasks': [task.model_dump(by_alias=True) for task in processed_tasks],
    }
    flow_configuration = cast(JsonDict, flow_configuration)
    validate_flow_configuration_against_schema(flow_configuration)

    client = KeboolaClient.from_state(ctx.session.state)
    LOG.info(f'Creating new flow: {name}')

    new_raw_configuration = await client.storage_client.flow_create(
        name=name, description=description, flow_configuration=flow_configuration  # Direct configuration
    )

    flow_response = FlowConfigurationResponse.from_raw_config(new_raw_configuration)

    LOG.info(f'Created flow "{name}" with configuration ID "{flow_response.configuration_id}"')
    return flow_response.configuration


@tool_errors()
@with_session_state()
async def update_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration to update.')],
    name: Annotated[str, Field(description='Updated flow name.')],
    description: Annotated[str, Field(description='Updated flow description.')],
    phases: Annotated[list[dict[str, Any]], Field(description='Updated list of phase definitions.')],
    tasks: Annotated[list[dict[str, Any]], Field(description='Updated list of task definitions.')],
    change_description: Annotated[str, Field(description='Description of changes made.')],
) -> Annotated[FlowConfiguration, Field(description='Updated flow configuration.')]:
    """
    Updates an existing flow configuration in Keboola.
    A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
    how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
    Each flow is composed of:
    - Tasks: individual component configurations (e.g., extractors, writers, transformations).
    - Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

    CONSIDERATIONS:
    - The `phases` and `tasks` parameters must conform to the Keboola Flow JSON schema.
    - Each task and phase must include at least: `id` and `name`.
    - Each task must reference an existing component configuration in the project.
    - Items in the `dependsOn` phase field reference ids of other phases.
    - The flow specified by `configuration_id` must already exist in the project.

    USAGE:
    Use this tool to update an existing flow.
    """

    processed_phases = _ensure_phase_ids(phases)
    processed_tasks = _ensure_task_ids(tasks)
    _validate_flow_structure(processed_phases, processed_tasks)

    flow_configuration = {
        'phases': [phase.model_dump(by_alias=True) for phase in processed_phases],
        'tasks': [task.model_dump(by_alias=True) for task in processed_tasks],
    }
    flow_configuration = cast(JsonDict, flow_configuration)
    validate_flow_configuration_against_schema(flow_configuration)

    client = KeboolaClient.from_state(ctx.session.state)
    LOG.info(f'Updating flow configuration: {configuration_id}')

    updated_raw_configuration = await client.storage_client.flow_update(
        config_id=configuration_id,
        name=name,
        description=description,
        change_description=change_description,
        flow_configuration=flow_configuration,  # Direct configuration
    )

    updated_flow_response = FlowConfigurationResponse.from_raw_config(updated_raw_configuration)

    LOG.info(f'Updated flow configuration: {configuration_id}')
    return updated_flow_response.configuration


@tool_errors()
@with_session_state()
async def retrieve_flows(
    ctx: Context,
    flow_ids: Annotated[
        Sequence[str], Field(default_factory=tuple, description='The configuration IDs of the flows to retrieve.')
    ] = tuple(),
) -> Annotated[list[ReducedFlow], Field(description='The retrieved flow configurations.')]:
    """Retrieves flow configurations from the project."""

    client = KeboolaClient.from_state(ctx.session.state)

    if flow_ids:
        flows = []
        for flow_id in flow_ids:
            try:
                raw_config = await client.storage_client.flow_detail(flow_id)
                flow = ReducedFlow.from_raw_config(raw_config)
                flows.append(flow)
            except Exception as e:
                LOG.warning(f'Could not retrieve flow {flow_id}: {e}')
        return flows
    else:
        raw_flows = await client.storage_client.flow_list()
        flows = [ReducedFlow.from_raw_config(raw_flow) for raw_flow in raw_flows]
        LOG.info(f'Found {len(flows)} flows in the project')
        return flows


@tool_errors()
@with_session_state()
async def get_flow_detail(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration to retrieve.')],
) -> Annotated[FlowConfiguration, Field(description='Detailed flow configuration.')]:
    """Gets detailed information about a specific flow configuration."""

    client = KeboolaClient.from_state(ctx.session.state)

    raw_config = await client.storage_client.flow_detail(configuration_id)

    flow_response = FlowConfigurationResponse.from_raw_config(raw_config)

    LOG.info(f'Retrieved flow details for configuration: {configuration_id}')
    return flow_response.configuration


def _ensure_phase_ids(phases: list[dict[str, Any]]) -> list[FlowPhase]:
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


def _ensure_task_ids(tasks: list[dict[str, Any]]) -> list[FlowTask]:
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


def _validate_flow_structure(phases: list[FlowPhase], tasks: list[FlowTask]) -> None:
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

    def has_cycle(phase_id: Any, _visited: set, rec_stack: set, path: list[Any]) -> list[Any] | None:
        """
        Returns None if no cycle found, or List[phase_ids] representing the cycle path.
        """
        _visited.add(phase_id)
        rec_stack.add(phase_id)
        path.append(phase_id)

        dependencies = graph.get(phase_id, [])

        for dep_id in dependencies:
            if dep_id not in _visited:
                cycle = has_cycle(dep_id, _visited, rec_stack, path)
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
            cycle_path = has_cycle(phase.id, visited, set(), [])
            if cycle_path is not None:
                cycle_str = ' -> '.join(str(pid) for pid in cycle_path)
                raise ValueError(f'Circular dependency detected in phases: {cycle_str}')
