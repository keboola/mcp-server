"""Flow management tools for the MCP server (orchestrations/flows)."""

import logging
from typing import Annotated, Any, Dict, List, Optional

from fastmcp import Context, FastMCP
from pydantic import Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.tools.components.model import ComponentRootConfiguration

LOG = logging.getLogger(__name__)

ORCHESTRATOR_COMPONENT_ID = 'keboola.orchestrator'


def add_flow_tools(mcp: FastMCP) -> None:
    """Add flow tools to the MCP server."""
    flow_tools = [
        create_flow,
        retrieve_flows,
        update_flow,
        get_flow_detail,
    ]

    for tool in flow_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(tool)

    LOG.info('Flow tools initialized.')


@tool_errors()
@with_session_state()
async def create_flow(
    ctx: Context,
    name: Annotated[str, Field(description='A short, descriptive name for the flow')],
    description: Annotated[str, Field(description='Detailed description of the flow purpose')],
    phases: Annotated[
        List[Dict[str, Any]],
        Field(description='List of phase definitions with id, name, description, dependsOn')
    ],
    tasks: Annotated[
        List[Dict[str, Any]],
        Field(description='List of task definitions with id, name, phase, task.componentId, task.configId')
    ],
) -> Annotated[ComponentRootConfiguration, Field(description='Created flow configuration')]:
    """
    Creates a new flow configuration in Keboola orchestrator.

    This handles the most common flow scenarios following the official flow schema:
    - Simple extractors with single configurations
    - Basic transformations
    - Standard writers
    - Sequential phases with parallel tasks within phases

    CONSIDERATIONS:
    - Phases and tasks will have IDs auto-generated if not provided (can be integers or strings)
    - Phase dependencies are validated before creation
    - Tasks must reference valid phase IDs
    - Component IDs in tasks must be valid Keboola components
    - Schema supports advanced features like configRowIds, mode, backend, variableValuesData

    ADVANCED TASK OPTIONS (optional):
    - configRowIds: List of row IDs for row-based components
    - mode: "run" or "debug" (defaults to "run")
    - backend: Backend configuration for transformations
    - variableValuesData: Dynamic variables for runtime
    - tag: Component version tag

    USAGE:
    - Use when you want to create a new data flow/pipeline

    EXAMPLES:
    - user_input: `Create a flow with my AWS S3 extractor`
        -> set phases and tasks to include the S3 component
        -> returns the created flow configuration
    """

    processed_phases = _ensure_phase_ids(phases)
    processed_tasks = _ensure_task_ids(tasks)
    _validate_flow_structure(processed_phases, processed_tasks)

    flow_parameters = {
        'phases': processed_phases,
        'tasks': processed_tasks
    }

    client = KeboolaClient.from_state(ctx.session.state)

    LOG.info(f'Creating new flow: {name}')

    new_raw_configuration = await client.storage_client.create_component_root_configuration(
        component_id=ORCHESTRATOR_COMPONENT_ID,
        data={
            'name': name,
            'description': description,
            'configuration': {
                'parameters': flow_parameters
            }
        }
    )

    new_configuration = ComponentRootConfiguration(
        **new_raw_configuration,
        component_id=ORCHESTRATOR_COMPONENT_ID,
        parameters=flow_parameters,
        storage=None  # Flows don't typically use storage mappings
    )

    LOG.info(f'Created flow "{name}" with configuration ID "{new_configuration.configuration_id}"')
    return new_configuration


@tool_errors()
@with_session_state()
async def retrieve_flows(
    ctx: Context,
    flow_ids: Annotated[
        Optional[List[str]],
        Field(description='Optional list of specific flow configuration IDs to retrieve')
    ] = None,
) -> Annotated[List[Dict[str, Any]], Field(description='List of flow configurations')]:
    """
    Retrieves flow configurations from the project.

    USAGE:
    - Use when you want to list existing flows in the project

    EXAMPLES:
    - user_input: `Show me all flows`
        -> returns all flow configurations
    - user_input: `Get flow with ID 12345`
        -> set flow_ids to ["12345"]
        -> returns the specific flow configuration
    """
    client = KeboolaClient.from_state(ctx.session.state)

    if flow_ids:
        flows = []
        for flow_id in flow_ids:
            try:
                raw_config = await client.storage_client.get(
                    endpoint=(
                        f'branch/{client.storage_client.branch_id}/components/'
                        f'{ORCHESTRATOR_COMPONENT_ID}/configs/{flow_id}'
                    )
                )
                flows.append(raw_config)
            except Exception as e:
                LOG.warning(f'Could not retrieve flow {flow_id}: {e}')
        return flows
    else:
        raw_flows = await client.storage_client.configuration_list(component_id=ORCHESTRATOR_COMPONENT_ID)
        LOG.info(f'Found {len(raw_flows)} flows in the project')
        return raw_flows


@tool_errors()
@with_session_state()
async def update_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration to update')],
    name: Annotated[str, Field(description='Updated flow name')],
    description: Annotated[str, Field(description='Updated flow description')],
    phases: Annotated[List[Dict[str, Any]], Field(description='Updated list of phase definitions')],
    tasks: Annotated[List[Dict[str, Any]], Field(description='Updated list of task definitions')],
    change_description: Annotated[str, Field(description='Description of changes made')],
) -> Annotated[ComponentRootConfiguration, Field(description='Updated flow configuration')]:
    """
    Updates an existing flow configuration.

    USAGE:
    - Use when you want to modify an existing flow

    EXAMPLES:
    - user_input: `Update flow 12345 to add a new transformation step`
        -> set configuration_id to "12345" and update phases/tasks accordingly
        -> returns the updated flow configuration
    """

    processed_phases = _ensure_phase_ids(phases)
    processed_tasks = _ensure_task_ids(tasks)
    _validate_flow_structure(processed_phases, processed_tasks)

    flow_parameters = {
        'phases': processed_phases,
        'tasks': processed_tasks
    }

    client = KeboolaClient.from_state(ctx.session.state)

    LOG.info(f'Updating flow configuration: {configuration_id}')

    updated_raw_configuration = await client.storage_client.update_component_root_configuration(
        component_id=ORCHESTRATOR_COMPONENT_ID,
        config_id=configuration_id,
        data={
            'name': name,
            'description': description,
            'changeDescription': change_description,
            'configuration': {
                'parameters': flow_parameters
            }
        }
    )

    updated_configuration = ComponentRootConfiguration(
        **updated_raw_configuration,
        component_id=ORCHESTRATOR_COMPONENT_ID,
        parameters=flow_parameters,
        storage=None
    )

    LOG.info(f'Updated flow configuration: {configuration_id}')
    return updated_configuration


@tool_errors()
@with_session_state()
async def get_flow_detail(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration to retrieve')],
) -> Annotated[Dict[str, Any], Field(description='Detailed flow configuration')]:
    """
    Gets detailed information about a specific flow configuration.

    USAGE:
    - Use when you want to see the full details of a specific flow

    EXAMPLES:
    - user_input: `Show me details of flow 12345`
        -> set configuration_id to "12345"
        -> returns the detailed flow configuration
    """
    client = KeboolaClient.from_state(ctx.session.state)

    raw_config = await client.storage_client.get(
        endpoint=(
            f'branch/{client.storage_client.branch_id}/components/'
            f'{ORCHESTRATOR_COMPONENT_ID}/configs/{configuration_id}'
        )
    )

    LOG.info(f'Retrieved flow details for configuration: {configuration_id}')
    return raw_config


def _ensure_phase_ids(phases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure all phases have unique IDs and proper structure according to schema"""
    processed_phases = []
    used_ids = set()

    for i, phase in enumerate(phases):
        phase_copy = phase.copy()

        if 'id' not in phase_copy or not phase_copy['id']:
            phase_id = i + 1
            while phase_id in used_ids:
                phase_id += 1
            phase_copy['id'] = phase_id

        if 'name' not in phase_copy:
            phase_copy['name'] = f"Phase {phase_copy['id']}"

        if 'dependsOn' not in phase_copy:
            phase_copy['dependsOn'] = []

        if 'behavior' not in phase_copy:
            phase_copy['behavior'] = {'onError': 'stop'}

        if 'childBehavior' not in phase_copy:
            phase_copy['childBehavior'] = {'onError': 'stop'}

        used_ids.add(phase_copy['id'])
        processed_phases.append(phase_copy)

    return processed_phases


def _ensure_task_ids(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure all tasks have unique IDs and proper structure according to schema"""
    processed_tasks = []
    used_ids = set()
    task_counter = 20001

    for task in tasks:
        task_copy = task.copy()

        if 'id' not in task_copy or not task_copy['id']:
            while task_counter in used_ids:
                task_counter += 1
            task_copy['id'] = task_counter
            task_counter += 1

        if 'name' not in task_copy:
            task_copy['name'] = f"Task {task_copy['id']}"

        if 'enabled' not in task_copy:
            task_copy['enabled'] = True

        if 'continueOnFailure' not in task_copy:
            task_copy['continueOnFailure'] = False

        if 'task' not in task_copy:
            raise ValueError(f"Task {task_copy['id']} missing 'task' configuration")

        if 'componentId' not in task_copy['task']:
            raise ValueError(f"Task {task_copy['id']} missing componentId in task configuration")

        task_obj = task_copy['task']
        if 'mode' not in task_obj:
            task_obj['mode'] = 'run'

        if 'behavior' not in task_copy:
            task_copy['behavior'] = {'onError': 'stop'}

        used_ids.add(task_copy['id'])
        processed_tasks.append(task_copy)

    return processed_tasks


def _validate_flow_structure(phases: List[Dict[str, Any]], tasks: List[Dict[str, Any]]) -> None:
    """Validate that the flow structure is valid"""
    phase_ids = {phase['id'] for phase in phases}

    for phase in phases:
        depends_on = phase.get('dependsOn', [])
        for dep_id in depends_on:
            if dep_id not in phase_ids:
                raise ValueError(f"Phase {phase['id']} depends on non-existent phase {dep_id}")

    for task in tasks:
        if task['phase'] not in phase_ids:
            raise ValueError(f"Task {task['id']} references non-existent phase {task['phase']}")

    _check_circular_dependencies(phases)


def _check_circular_dependencies(phases: List[Dict[str, Any]]) -> None:
    """Basic circular dependency check for phases"""
    def has_cycle(phase_id: int, visited: set, rec_stack: set) -> bool:
        visited.add(phase_id)
        rec_stack.add(phase_id)

        phase = next((p for p in phases if p['id'] == phase_id), None)
        if phase:
            for dep_id in phase.get('dependsOn', []):
                if dep_id not in visited:
                    if has_cycle(dep_id, visited, rec_stack):
                        return True
                elif dep_id in rec_stack:
                    return True

        rec_stack.remove(phase_id)
        return False

    visited = set()
    for phase in phases:
        if phase['id'] not in visited:
            if has_cycle(phase['id'], visited, set()):
                raise ValueError('Circular dependency detected in phases')
