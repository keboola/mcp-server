"""Flow management tools for the MCP server (orchestrations/flows)."""

import logging
from typing import Annotated, Any, Sequence, cast

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from pydantic import Field

from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID, JsonDict, KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.tools.components.tools import _set_cfg_creation_metadata, _set_cfg_update_metadata
from keboola_mcp_server.tools.flow.model import (
    FlowConfigurationResponse,
    FlowToolResponse,
    ListFlowsOutput,
    ReducedFlow,
)
from keboola_mcp_server.tools.flow.utils import (
    ensure_phase_ids,
    ensure_task_ids,
    get_schema_as_markdown,
    validate_flow_structure,
)
from keboola_mcp_server.tools.validation import validate_flow_configuration_against_schema

LOG = logging.getLogger(__name__)


def add_flow_tools(mcp: FastMCP) -> None:
    """Add flow tools to the MCP server."""
    flow_tools = [create_flow, list_flows, update_flow, get_flow, get_flow_schema]

    for tool in flow_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(FunctionTool.from_function(tool))

    LOG.info('Flow tools initialized.')


@tool_errors()
async def get_flow_schema() -> Annotated[str, Field(description='The configuration schema of Flow.')]:
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
) -> Annotated[FlowToolResponse, Field(description='Response object for flow creation.')]:
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
    - Links contained in the response should ALWAYS be presented to the user

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

    processed_phases = ensure_phase_ids(phases)
    processed_tasks = ensure_task_ids(tasks)
    validate_flow_structure(processed_phases, processed_tasks)
    flow_configuration = {
        'phases': [phase.model_dump(by_alias=True) for phase in processed_phases],
        'tasks': [task.model_dump(by_alias=True) for task in processed_tasks],
    }
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration))

    LOG.info(f'Creating new flow: {name}')
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    new_raw_configuration = await client.storage_client.flow_create(
        name=name, description=description, flow_configuration=flow_configuration  # Direct configuration
    )

    await _set_cfg_creation_metadata(
        client,
        component_id=ORCHESTRATOR_COMPONENT_ID,
        configuration_id=str(new_raw_configuration['id']),
    )

    flow_id = str(new_raw_configuration['id'])
    flow_name = str(new_raw_configuration['name'])
    flow_links = links_manager.get_flow_links(flow_id=flow_id, flow_name=flow_name)
    tool_response = FlowToolResponse.model_validate(new_raw_configuration | {'links': flow_links})

    LOG.info(f'Created flow "{name}" with configuration ID "{flow_id}"')
    return tool_response


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
) -> Annotated[FlowToolResponse, Field(description='Response object for flow update.')]:
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
    - Links contained in the response should ALWAYS be presented to the user

    USAGE:
    Use this tool to update an existing flow.
    """

    processed_phases = ensure_phase_ids(phases)
    processed_tasks = ensure_task_ids(tasks)
    validate_flow_structure(processed_phases, processed_tasks)
    flow_configuration = {
        'phases': [phase.model_dump(by_alias=True) for phase in processed_phases],
        'tasks': [task.model_dump(by_alias=True) for task in processed_tasks],
    }
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration))

    LOG.info(f'Updating flow configuration: {configuration_id}')
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    updated_raw_configuration = await client.storage_client.flow_update(
        config_id=configuration_id,
        name=name,
        description=description,
        change_description=change_description,
        flow_configuration=flow_configuration,  # Direct configuration
    )

    await _set_cfg_update_metadata(
        client,
        component_id=ORCHESTRATOR_COMPONENT_ID,
        configuration_id=str(updated_raw_configuration['id']),
        configuration_version=cast(int, updated_raw_configuration['version']),
    )

    flow_id = str(updated_raw_configuration['id'])
    flow_name = str(updated_raw_configuration['name'])
    flow_links = links_manager.get_flow_links(flow_id=flow_id, flow_name=flow_name)
    tool_response = FlowToolResponse.model_validate(updated_raw_configuration | {'links': flow_links})

    LOG.info(f'Updated flow configuration: {flow_id}')
    return tool_response


@tool_errors()
@with_session_state()
async def list_flows(
    ctx: Context,
    flow_ids: Annotated[
        Sequence[str], Field(default_factory=tuple, description='The configuration IDs of the flows to retrieve.')
    ] = tuple(),
) -> ListFlowsOutput:
    """Retrieves flow configurations from the project."""

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if flow_ids:
        flows = []
        for flow_id in flow_ids:
            try:
                raw_config = await client.storage_client.flow_detail(flow_id)
                flow = ReducedFlow.model_validate(raw_config)
                flows.append(flow)
            except Exception as e:
                LOG.warning(f'Could not retrieve flow {flow_id}: {e}')
    else:
        raw_flows = await client.storage_client.flow_list()
        flows = [ReducedFlow.model_validate(raw_flow) for raw_flow in raw_flows]
        LOG.info(f'Found {len(flows)} flows in the project')

    links = [links_manager.get_flows_dashboard_link()]

    return ListFlowsOutput(flows=flows, links=links)


@tool_errors()
@with_session_state()
async def get_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration to retrieve.')],
) -> Annotated[FlowConfigurationResponse, Field(description='Detailed flow configuration.')]:
    """Gets detailed information about a specific flow configuration."""

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    raw_config = await client.storage_client.flow_detail(configuration_id)

    flow_response = FlowConfigurationResponse.model_validate(raw_config)
    links = links_manager.get_flow_links(flow_response.configuration_id, flow_name=flow_response.configuration_name)
    flow_response.links = links
    LOG.info(f'Retrieved flow details for configuration: {configuration_id}')
    return flow_response
