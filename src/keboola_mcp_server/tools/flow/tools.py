"""Flow management tools for the MCP server (orchestrations/flows)."""

import importlib.resources as pkg_resources
import json
import logging
from datetime import datetime, timezone
from typing import Annotated, Any, Sequence, cast

import httpx
from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from pydantic import Field

from keboola_mcp_server import resources
from keboola_mcp_server.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    FLOW_TYPE,
    FLOW_TYPES,
    ORCHESTRATOR_COMPONENT_ID,
    JsonDict,
    KeboolaClient,
)
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.tools.components.api_models import CreateConfigurationAPIResponse
from keboola_mcp_server.tools.components.utils import set_cfg_creation_metadata, set_cfg_update_metadata
from keboola_mcp_server.tools.flow.api_models import APIFlowResponse
from keboola_mcp_server.tools.flow.model import (
    ConditionalFlowPhase,
    ConditionalFlowTask,
    Flow,
    FlowToolResponse,
    ListFlowsOutput,
)
from keboola_mcp_server.tools.flow.utils import (
    _get_all_flows,
    _get_flows_by_ids,
    ensure_legacy_phase_ids,
    ensure_legacy_task_ids,
    get_schema_as_markdown,
    validate_legacy_flow_structure,
)
from keboola_mcp_server.tools.project import get_project_info
from keboola_mcp_server.tools.validation import validate_flow_configuration_against_schema

LOG = logging.getLogger(__name__)


def add_flow_tools(mcp: FastMCP) -> None:
    """Add flow tools to the MCP server."""
    flow_tools = [create_flow, create_conditional_flow, list_flows,
                  update_flow, get_flow, get_flow_examples, get_flow_schema]

    for tool in flow_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(FunctionTool.from_function(tool))

    LOG.info('Flow tools initialized.')


@tool_errors()
@with_session_state()
async def get_flow_schema(
    ctx: Context,
    flow_type: Annotated[FLOW_TYPE, Field(description='The type of flow for which to fetch.')],
) -> Annotated[str, Field(description='The configuration schema of the specified flow type, formatted as markdown.')]:
    """
    Returns the JSON schema for the given flow type in markdown format.
    `keboola.flow` = conditional flows
    `keboola.orchestrator` = legacy flows

    CONSIDERATIONS:
    - If the project_info has conditional flows disabled, the legacy flow schema (orchestrator) is returned regardless
    of requested type.
    - Otherwise, the returned schema matches the requested flow type.

    Usage:
        Use this tool to inspect the required structure of phases and tasks for `create_flow` or `update_flow`.
    """
    project_info = await get_project_info(ctx)

    if project_info.conditional_flows_disabled or flow_type == ORCHESTRATOR_COMPONENT_ID:
        effective_type = ORCHESTRATOR_COMPONENT_ID
    else:
        effective_type = CONDITIONAL_FLOW_COMPONENT_ID

    LOG.info(f'Returning flow configuration schema for flow type: {effective_type}')
    return get_schema_as_markdown(flow_type=effective_type)


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
    new_raw_configuration = await client.storage_client.flow_create(
        name=name, description=description, flow_configuration=flow_configuration, flow_type=flow_type
    )
    api_config = CreateConfigurationAPIResponse.model_validate(new_raw_configuration)
    await set_cfg_creation_metadata(
        client,
        component_id=flow_type,
        configuration_id=str(new_raw_configuration['id']),
    )

    flow_links = links_manager.get_flow_links(flow_id=api_config.id, flow_name=api_config.name, flow_type=flow_type)
    tool_response = FlowToolResponse(
        id=api_config.id,
        description=api_config.description,
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=flow_links
    )

    LOG.info(f'Created legacy flow "{name}" with configuration ID "{api_config.id}" (type: {flow_type})')
    return tool_response


@tool_errors()
@with_session_state()
async def create_conditional_flow(
    ctx: Context,
    name: Annotated[str, Field(description='A short, descriptive name for the flow.')],
    description: Annotated[str, Field(description='Detailed description of the flow purpose.')],
    phases: Annotated[
        list[dict[str, Any]],
        Field(description='List of phase definitions for conditional flows.')
    ],
    tasks: Annotated[
        list[dict[str, Any]],
        Field(description='List of task definitions for conditional flows.')
    ],
) -> Annotated[FlowToolResponse, Field(description='Response object for enhanced flow creation.')]:
    """
    Creates a new **conditional flow** configuration in Keboola.
    Use this to create a flow always when the project has conditional flows enabled (in project_info tool)

    If you haven't already called it, always use the `get_flow_schema` tool using `keboola.flow` flow type
    to see the latest schema for conditional flows and also look at the examples under `get_flow_examples` tool.

    This enhanced version provides:
    - **Structured conditions**: Type-safe condition objects with proper validation
    - **Structured retry configurations**: Properly typed retry parameters and conditions
    - **Structured task configurations**: Type-safe job, notification, and variable task configurations
    - **Better validation**: Compile-time validation of flow structure and relationships
    - **Enhanced error messages**: More descriptive errors when validation fails

    Enhanced conditional flows use structured models for:
    - **Conditions**: TaskCondition, PhaseCondition, OperatorCondition, etc.
    - **Retry configurations**: RetryConfiguration with RetryStrategyParams and RetryOnCondition
    - **Task configurations**: JobTaskConfiguration, NotificationTaskConfiguration, VariableTaskConfiguration

    CONSIDERATIONS:
    - Do not create conditions, unless user asks for them explicitly
    - All IDs must be unique and clearly defined.
    - The `phases` and `tasks` parameters must conform to the keboola.flow JSON schema.
    - The phases cannot be empty.
    - Conditional flows are the default and recommended flow type in Keboola.

    USE CASES:
    - user_input: Create a flow.
    - user_input: Create a flow with complex conditional logic and retry mechanisms.
    - user_input: Build a data pipeline with sophisticated error handling and notifications.
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
    new_raw_configuration = await client.storage_client.flow_create(
        name=name, description=description, flow_configuration=flow_configuration, flow_type=flow_type
    )
    api_config = CreateConfigurationAPIResponse.model_validate(new_raw_configuration)

    await set_cfg_creation_metadata(
        client,
        component_id=flow_type,
        configuration_id=str(new_raw_configuration['id']),
    )

    flow_links = links_manager.get_flow_links(flow_id=api_config.id, flow_name=api_config.name, flow_type=flow_type)
    tool_response = FlowToolResponse(
        id=api_config.id,
        description=api_config.description,
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=flow_links
    )

    LOG.info(f'Created conditional flow "{name}" with configuration ID "{api_config.id}" (type: {flow_type})')
    return tool_response


@tool_errors()
@with_session_state()
async def update_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration to update.')],
    flow_type: Annotated[
        FLOW_TYPE,
        Field(
            description=(
                'The type of flow to update. Use "keboola.flow" for conditional flows or '
                '"keboola.orchestrator" for legacy flows.'
            )
        )
    ],
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
    - The `flow_type` parameter must match the actual type of the flow being updated.
    - Links contained in the response should ALWAYS be presented to the user

    USAGE:
    Use this tool to update an existing flow. You must specify the correct flow_type:
    - Use "keboola.flow" for conditional flows (advanced flows with conditional logic)
    - Use "keboola.orchestrator" for legacy flows (basic orchestration flows)
    """

    client = KeboolaClient.from_state(ctx.session.state)

    # Use flow-type specific utilities
    if flow_type == ORCHESTRATOR_COMPONENT_ID:
        processed_phases = ensure_legacy_phase_ids(phases)
        processed_tasks = ensure_legacy_task_ids(tasks)
        validate_legacy_flow_structure(processed_phases, processed_tasks)
        flow_configuration = {
            'phases': [phase.model_dump(by_alias=True) for phase in processed_phases],
            'tasks': [task.model_dump(by_alias=True) for task in processed_tasks],
        }
    else:
        processed_phases = [ConditionalFlowPhase.model_validate(phase) for phase in phases]
        processed_tasks = [ConditionalFlowTask.model_validate(task) for task in tasks]
        flow_configuration = {
            'phases': [phase.model_dump(exclude_unset=True, by_alias=True) for phase in processed_phases],
            'tasks': [task.model_dump(exclude_unset=True, by_alias=True) for task in processed_tasks],
            }

    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type)

    LOG.info(f'Updating flow configuration: {configuration_id} (type: {flow_type})')
    links_manager = await ProjectLinksManager.from_client(client)
    updated_raw_configuration = await client.storage_client.flow_update(
        config_id=configuration_id,
        name=name,
        description=description,
        change_description=change_description,
        flow_configuration=flow_configuration,  # Direct configuration
        flow_type=flow_type,
    )
    api_config = CreateConfigurationAPIResponse.model_validate(updated_raw_configuration)

    await set_cfg_update_metadata(
        client,
        component_id=flow_type,
        configuration_id=str(updated_raw_configuration['id']),
        configuration_version=cast(int, updated_raw_configuration['version']),
    )

    flow_links = links_manager.get_flow_links(flow_id=api_config.id, flow_name=api_config.name, flow_type=flow_type)
    tool_response = FlowToolResponse(
        id=api_config.id,
        description=api_config.description,
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=flow_links
    )
    LOG.info(f'Updated flow configuration: {api_config.id}')
    return tool_response


@tool_errors()
@with_session_state()
async def list_flows(
    ctx: Context,
    flow_ids: Annotated[Sequence[str], Field(description='IDs of the flows to retrieve.')] = tuple(),
) -> ListFlowsOutput:
    """Retrieves flow configurations from the project. Optionally filtered by IDs."""

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if flow_ids:
        flows = await _get_flows_by_ids(client, flow_ids)
        LOG.info(f'Retrieved {len(flows)} flows by ID.')
    else:
        flows = await _get_all_flows(client)
        LOG.info(f'Retrieved {len(flows)} flows.')

    # For list_flows, we don't know the specific flow types, so we'll use both flow types
    links = [
        links_manager.get_flows_dashboard_link(ORCHESTRATOR_COMPONENT_ID),
        links_manager.get_flows_dashboard_link(CONDITIONAL_FLOW_COMPONENT_ID),
    ]
    return ListFlowsOutput(flows=flows, links=links)


@tool_errors()
@with_session_state()
async def get_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow to retrieve.')],
) -> Annotated[Flow, Field(description='Detailed flow configuration.')]:
    """Gets detailed information about a specific flow configuration."""

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    raw_config = None
    found_type: FLOW_TYPE | None = None

    for type_ in FLOW_TYPES:
        try:
            raw_config = await client.storage_client.flow_detail(
                config_id=configuration_id,
                flow_type=type_,
            )
            found_type = type_

            LOG.info(f'Found flow {configuration_id} under flow type {type_}.')
            break
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                LOG.info(f'Could not find flow {configuration_id} under flow type {type_}.')
            else:
                raise

    if not found_type:
        raise ValueError(f'Flow configuration "{configuration_id}" not found.')

    api_flow = APIFlowResponse.model_validate(raw_config)
    links = links_manager.get_flow_links(api_flow.configuration_id, flow_name=api_flow.name, flow_type=found_type)
    flow = Flow.from_api_response(api_config=api_flow, flow_component_id=found_type, links=links)

    LOG.info(f'Retrieved flow details for configuration: {configuration_id} (type: {found_type})')
    return flow


@tool_errors()
@with_session_state()
async def get_flow_examples(
    ctx: Context,
    flow_type: Annotated[FLOW_TYPE, Field(description='The type of the flow to retrieve examples for.')],
) -> Annotated[str, Field(description='Examples of the flow configurations.')]:
    """
    Retrieves examples of valid flow configurations.

    Projects have conditional flows enabled by default (check the `conditional_flows_disabled` flag in
    get_project_info()) and should therefore fetch examples for type `keboola.flow`.
    Projects that have conditional flows disabled should fetch examples for type `keboola.orchestrator`.
    """
    filename = (
        'conditional_flow_examples.jsonl' if flow_type == CONDITIONAL_FLOW_COMPONENT_ID
        else 'legacy_flow_examples.jsonl'
    )
    file_path = pkg_resources.files(resources) / 'flow_examples' / filename

    markdown = f'# Flow Configuration Examples for `{flow_type}`\n\n'

    with file_path.open('r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            data = json.loads(line)
            markdown += f'{i}. Flow Configuration:\n```json\n{json.dumps(data, indent=2)}\n```\n\n'

    return markdown
