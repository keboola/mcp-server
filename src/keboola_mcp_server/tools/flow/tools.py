"""Flow management tools for the MCP server (orchestrations/flows)."""

import importlib.resources as pkg_resources
import json
import logging
from datetime import datetime, timezone
from typing import Annotated, Any, Sequence, cast

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import Field

from keboola_mcp_server import resources
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    FlowType,
    KeboolaClient,
)
from keboola_mcp_server.clients.storage import CreateConfigurationAPIResponse
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.tools.components.utils import set_cfg_creation_metadata, set_cfg_update_metadata
from keboola_mcp_server.tools.flow.model import (
    ConditionalFlowPhase,
    ConditionalFlowTask,
    Flow,
    FlowPhase,
    FlowTask,
    FlowToolOutput,
    ListFlowsOutput,
)
from keboola_mcp_server.tools.flow.utils import (
    ensure_legacy_phase_ids,
    ensure_legacy_task_ids,
    get_all_flows,
    get_flows_by_ids,
    get_schema_as_markdown,
    resolve_flow_by_id,
    validate_legacy_flow_structure,
)
from keboola_mcp_server.tools.project import get_project_info
from keboola_mcp_server.tools.validation import validate_flow_configuration_against_schema

LOG = logging.getLogger(__name__)

FLOW_TOOLS_TAG = 'flows'


def add_flow_tools(mcp: FastMCP) -> None:
    """Add flow tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            create_flow,
            tags={FLOW_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            create_conditional_flow,
            tags={FLOW_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            list_flows,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={FLOW_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            update_flow,
            annotations=ToolAnnotations(destructiveHint=True),
            tags={FLOW_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_flow,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={FLOW_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_flow_examples,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={FLOW_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_flow_schema,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={FLOW_TOOLS_TAG},
        )
    )

    LOG.info('Flow tools initialized.')


@tool_errors()
async def get_flow_schema(
    ctx: Context,
    flow_type: Annotated[FlowType, Field(description='The type of flow for which to fetch schema.')],
) -> Annotated[str, Field(description='The configuration schema of the specified flow type.')]:
    """
    Returns the JSON schema for the given flow type in markdown format.
    `keboola.flow` = conditional flows
    `keboola.orchestrator` = legacy flows

    CONSIDERATIONS:
    - If the project has conditional flows disabled, this tool will fail when requesting conditional flow schema.
    - Otherwise, the returned schema matches the requested flow type.

    Usage:
        Use this tool to inspect the required structure of phases and tasks for `create_flow` or `update_flow`.
    """
    project_info = await get_project_info(ctx)

    if flow_type == CONDITIONAL_FLOW_COMPONENT_ID and not project_info.conditional_flows:
        raise ValueError(
            f'Conditional flows are not supported in this project. '
            f'Project "{project_info.project_name}" has conditional_flows=false. '
            f'If you want to use conditional flows, please enable them in your project settings. '
            f'Otherwise, use flow_type="{ORCHESTRATOR_COMPONENT_ID}" for legacy flows instead.'
        )

    LOG.info(f'Returning flow configuration schema for flow type: {flow_type}')
    return get_schema_as_markdown(flow_type=flow_type)


@tool_errors()
async def create_flow(
    ctx: Context,
    name: Annotated[str, Field(description='A short, descriptive name for the flow.')],
    description: Annotated[str, Field(description='Detailed description of the flow purpose.')],
    phases: Annotated[list[dict[str, Any]], Field(description='List of phase definitions.')],
    tasks: Annotated[list[dict[str, Any]], Field(description='List of task definitions.')],
) -> FlowToolOutput:
    """
    Creates a new flow configuration in Keboola.
    A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
    how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
    Each flow is composed of:
    - Tasks: individual component configurations (e.g., extractors, writers, transformations).
    - Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

    If you haven't already called it, always use the `get_flow_schema` tool using `keboola.orchestrator` flow type
    to see the latest schema for flows and also look at the examples under `get_flow_examples` tool.

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
    flow_type = ORCHESTRATOR_COMPONENT_ID
    processed_phases = ensure_legacy_phase_ids(phases)
    processed_tasks = ensure_legacy_task_ids(tasks)
    validate_legacy_flow_structure(processed_phases, processed_tasks)
    flow_configuration = {
        'phases': [phase.model_dump(by_alias=True) for phase in processed_phases],
        'tasks': [task.model_dump(by_alias=True) for task in processed_tasks],
    }
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type)

    LOG.info(f'Creating new flow: {name} (type: {ORCHESTRATOR_COMPONENT_ID})')
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    new_raw_configuration = await client.storage_client.configuration_create(
        component_id=flow_type,
        name=name,
        description=description,
        configuration=flow_configuration,
    )
    api_config = CreateConfigurationAPIResponse.model_validate(new_raw_configuration)
    await set_cfg_creation_metadata(
        client,
        component_id=flow_type,
        configuration_id=str(new_raw_configuration['id']),
    )

    flow_links = links_manager.get_flow_links(flow_id=api_config.id, flow_name=api_config.name, flow_type=flow_type)
    tool_response = FlowToolOutput(
        configuration_id=api_config.id,
        component_id=flow_type,
        description=api_config.description or '',
        version=api_config.version,
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=flow_links,
    )

    LOG.info(f'Created legacy flow "{name}" with configuration ID "{api_config.id}" (type: {flow_type})')
    return tool_response


@tool_errors()
async def create_conditional_flow(
    ctx: Context,
    name: Annotated[str, Field(description='A short, descriptive name for the flow.')],
    description: Annotated[str, Field(description='Detailed description of the flow purpose.')],
    phases: Annotated[list[dict[str, Any]], Field(description='List of phase definitions for conditional flows.')],
    tasks: Annotated[list[dict[str, Any]], Field(description='List of task definitions for conditional flows.')],
) -> FlowToolOutput:
    """
    Creates a new conditional flow configuration in Keboola.

    BEFORE USING THIS TOOL:
    - Call `get_flow_schema` with flow_type='keboola.flow' to see the required schema structure
    - Call `get_flow_examples` with flow_type='keboola.flow' to see working examples

    REQUIREMENTS:
    - All phase and task IDs must be unique strings
    - The `phases` list cannot be empty
    - The `phases` and `tasks` parameters must match the keboola.flow JSON schema structure
    - Only include conditions/retry logic if the user explicitly requests them
    - All phases must be connected: no dangling phases are allowed
    - The flow must have exactly one entry point (one phase with no incoming transitions)
    - Every phase must either transition to another phase or end the flow

    WHEN TO USE:
    - User asks to "create a flow" (conditional flows are the default flow type)
    - User requests conditional logic, retry mechanisms, or error handling
    - User needs a data pipeline with sophisticated branching or notifications
    """
    flow_type = CONDITIONAL_FLOW_COMPONENT_ID
    processed_phases = [ConditionalFlowPhase.model_validate(phase) for phase in phases]
    processed_tasks = [ConditionalFlowTask.model_validate(task) for task in tasks]
    flow_configuration = {
        'phases': [phase.model_dump(exclude_unset=True, by_alias=True) for phase in processed_phases],
        'tasks': [task.model_dump(exclude_unset=True, by_alias=True) for task in processed_tasks],
    }
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type)
    LOG.info(f'Creating new enhanced conditional flow: {name} (type: {flow_type})')
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    new_raw_configuration = await client.storage_client.configuration_create(
        component_id=flow_type,
        name=name,
        description=description,
        configuration=flow_configuration,
    )
    api_config = CreateConfigurationAPIResponse.model_validate(new_raw_configuration)

    await set_cfg_creation_metadata(
        client,
        component_id=flow_type,
        configuration_id=str(new_raw_configuration['id']),
    )

    flow_links = links_manager.get_flow_links(flow_id=api_config.id, flow_name=api_config.name, flow_type=flow_type)
    tool_response = FlowToolOutput(
        configuration_id=api_config.id,
        component_id=flow_type,
        description=api_config.description or '',
        version=api_config.version,
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=flow_links,
    )

    LOG.info(f'Created conditional flow "{name}" with configuration ID "{api_config.id}" (type: {flow_type})')
    return tool_response


@tool_errors()
async def update_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration to update.')],
    flow_type: Annotated[
        FlowType,
        Field(
            description=(
                'The type of flow to update. Use "keboola.flow" for conditional flows or '
                '"keboola.orchestrator" for legacy flows. This MUST match the existing flow type.'
            )
        ),
    ],
    change_description: Annotated[str, Field(description='Description of changes made.')],
    phases: Annotated[list[dict[str, Any]], Field(description='Updated list of phase definitions.')] = None,
    tasks: Annotated[list[dict[str, Any]], Field(description='Updated list of task definitions.')] = None,
    name: Annotated[str, Field(description='Updated flow name. Only updated if provided.')] = '',
    description: Annotated[str, Field(description='Updated flow description. Only updated if provided.')] = '',
) -> FlowToolOutput:
    """
    Updates an existing flow configuration in Keboola.

    A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
    how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
    Each flow is composed of:
    - Tasks: individual component configurations (e.g., extractors, writers, transformations).
    - Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

    PREREQUISITES:
    - The flow specified by `configuration_id` must already exist in the project
    - Use `get_flow` to retrieve the current flow configuration and determine its type
    - Use `get_flow_schema` with the correct flow type to understand the required structure
    - Ensure all referenced component configurations exist in the project

    CONSIDERATIONS:
    - The `flow_type` parameter **MUST** match the actual type of the flow being updated
    - The `phases` and `tasks` parameters must conform to the appropriate JSON schema
    - Each task and phase must include at least: `id` and `name`
    - Each task must reference an existing component configuration in the project
    - Items in the `dependsOn` phase field reference ids of other phases
    - If the project has conditional flows disabled, this tool will fail when trying to update conditional flows
    - Links contained in the response should ALWAYS be presented to the user

    USAGE:
    Use this tool to update an existing flow. You must specify the correct flow_type:
    - Use `"keboola.flow"` for conditional flows
    - Use `"keboola.orchestrator"` for legacy flows

    EXAMPLES:
    - user_input: "Add a new transformation phase to my existing flow"
        - First use `get_flow` to retrieve the current flow configuration
        - Determine the flow type from the response
        - Use `get_flow_schema` with the correct flow type
        - Update the phases and tasks arrays with the new transformation
        - Set `flow_type` to match the existing flow type
    - user_input: "Update my flow to include error handling"
        - For conditional flows: add retry configurations and error conditions
        - For legacy flows: adjust `continueOnFailure` settings
        - Ensure the `flow_type` matches the existing flow
    """

    project_info = await get_project_info(ctx)
    if flow_type == CONDITIONAL_FLOW_COMPONENT_ID and not project_info.conditional_flows:
        raise ValueError(
            f'Conditional flows are not supported in this project. '
            f'Project "{project_info.project_name}" has conditional_flows=false. '
            f'If you want to use conditional flows, please enable them in your project settings. '
            f'Otherwise, use flow_type="{ORCHESTRATOR_COMPONENT_ID}" for legacy flows instead.'
        )

    client = KeboolaClient.from_state(ctx.session.state)

    current_config = await client.storage_client.configuration_detail(
        component_id=flow_type, configuration_id=configuration_id
    )
    flow_configuration = current_config.get('configuration', {}).copy()

    if phases is not None:
        if flow_type == ORCHESTRATOR_COMPONENT_ID:
            processed_phases = ensure_legacy_phase_ids(phases)
            flow_configuration['phases'] = [phase.model_dump(by_alias=True) for phase in processed_phases]
        else:
            processed_phases = [ConditionalFlowPhase.model_validate(phase) for phase in phases]
            flow_configuration['phases'] = [
                phase.model_dump(exclude_unset=True, by_alias=True) for phase in processed_phases
            ]

    if tasks is not None:
        if flow_type == ORCHESTRATOR_COMPONENT_ID:
            processed_tasks = ensure_legacy_task_ids(tasks)
            flow_configuration['tasks'] = [task.model_dump(by_alias=True) for task in processed_tasks]
        else:
            processed_tasks = [ConditionalFlowTask.model_validate(task) for task in tasks]
            flow_configuration['tasks'] = [
                task.model_dump(exclude_unset=True, by_alias=True) for task in processed_tasks
            ]

    if flow_type == ORCHESTRATOR_COMPONENT_ID:
        validate_legacy_flow_structure(
            phases=[FlowPhase.model_validate(phase) for phase in flow_configuration.get('phases', [])],
            tasks=[FlowTask.model_validate(task) for task in flow_configuration.get('tasks', [])],
        )

    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type)

    LOG.info(f'Updating flow configuration: {configuration_id} (type: {flow_type})')
    links_manager = await ProjectLinksManager.from_client(client)
    updated_raw_configuration = await client.storage_client.configuration_update(
        component_id=flow_type,
        configuration_id=configuration_id,
        configuration=flow_configuration,
        change_description=change_description,
        updated_name=name,
        updated_description=description,
    )
    api_config = CreateConfigurationAPIResponse.model_validate(updated_raw_configuration)

    await set_cfg_update_metadata(
        client,
        component_id=flow_type,
        configuration_id=api_config.id,
        configuration_version=api_config.version,
    )

    flow_links = links_manager.get_flow_links(flow_id=api_config.id, flow_name=api_config.name, flow_type=flow_type)
    tool_response = FlowToolOutput(
        configuration_id=api_config.id,
        component_id=flow_type,
        description=api_config.description or '',
        version=api_config.version,
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=flow_links,
    )
    LOG.info(f'Updated flow configuration: {api_config.id}')
    return tool_response


@tool_errors()
async def list_flows(
    ctx: Context,
    flow_ids: Annotated[Sequence[str], Field(description='IDs of the flows to retrieve.')] = tuple(),
) -> ListFlowsOutput:
    """Retrieves flow configurations from the project. Optionally filtered by IDs."""

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if flow_ids:
        flows = await get_flows_by_ids(client, flow_ids)
        LOG.info(f'Retrieved {len(flows)} flows by ID.')
    else:
        flows = await get_all_flows(client)
        LOG.info(f'Retrieved {len(flows)} flows.')

    # For list_flows, we don't know the specific flow types, so we'll use both flow types
    links = [
        links_manager.get_flows_dashboard_link(ORCHESTRATOR_COMPONENT_ID),
        links_manager.get_flows_dashboard_link(CONDITIONAL_FLOW_COMPONENT_ID),
    ]
    return ListFlowsOutput(flows=flows, links=links)


@tool_errors()
async def get_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow to retrieve.')],
) -> Flow:
    """Gets detailed information about a specific flow configuration."""

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    api_flow, found_type = await resolve_flow_by_id(client, configuration_id)
    LOG.info(f'Found flow {configuration_id} under flow type {found_type}.')

    links = links_manager.get_flow_links(api_flow.configuration_id, flow_name=api_flow.name, flow_type=found_type)
    flow = Flow.from_api_response(api_config=api_flow, flow_component_id=found_type, links=links)

    LOG.info(f'Retrieved flow details for configuration: {configuration_id} (type: {found_type})')
    return flow


@tool_errors()
async def get_flow_examples(
    ctx: Context,
    flow_type: Annotated[FlowType, Field(description='The type of the flow to retrieve examples for.')],
) -> Annotated[str, Field(description='Examples of the flow configurations.')]:
    """
    Retrieves examples of valid flow configurations.

    CONSIDERATIONS:
    - If the project has conditional flows disabled, this tool will fail when requesting conditional flow examples.
    - Projects with conditional flows enabled can fetch examples for both flow types.
    - Projects with conditional flows disabled should use `keboola.orchestrator` for legacy flow examples.
    """
    project_info = await get_project_info(ctx)
    if flow_type == CONDITIONAL_FLOW_COMPONENT_ID and not project_info.conditional_flows:
        raise ValueError(
            f'Conditional flows are not supported in this project. '
            f'Project "{project_info.project_name}" has conditional_flows=false. '
            f'If you want to use conditional flows, please enable them in your project settings. '
            f'Otherwise, use flow_type="{ORCHESTRATOR_COMPONENT_ID}" for legacy flow examples instead.'
        )

    filename = (
        'conditional_flow_examples.jsonl'
        if flow_type == CONDITIONAL_FLOW_COMPONENT_ID
        else 'legacy_flow_examples.jsonl'
    )
    file_path = pkg_resources.files(resources) / 'flow_examples' / filename

    markdown = f'# Flow Configuration Examples for `{flow_type}`\n\n'

    with file_path.open('r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            data = json.loads(line)
            markdown += f'{i}. Flow Configuration:\n```json\n{json.dumps(data, indent=2)}\n```\n\n'

    return markdown
