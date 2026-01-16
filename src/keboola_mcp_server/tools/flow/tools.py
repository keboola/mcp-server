"""Flow management tools for the MCP server (orchestrations/flows)."""

import copy
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
from keboola_mcp_server.mcp import process_concurrently, toon_serializer, unwrap_results
from keboola_mcp_server.tools.components.utils import set_cfg_creation_metadata, set_cfg_update_metadata
from keboola_mcp_server.tools.constants import (
    FLOW_TOOLS_TAG,
)
from keboola_mcp_server.tools.flow.model import (
    Flow,
    FlowToolOutput,
    GetFlowsDetailOutput,
    GetFlowsListOutput,
    GetFlowsOutput,
)
from keboola_mcp_server.tools.flow.scheduler import (
    fetch_schedules_for_flows,
    process_schedule_request,
)
from keboola_mcp_server.tools.flow.scheduler_model import ScheduleRequest
from keboola_mcp_server.tools.flow.utils import (
    get_all_flows,
    get_flow_configuration,
    get_schema_as_markdown,
    resolve_flow_by_id,
    validate_flow_structure,
)
from keboola_mcp_server.tools.project import get_project_info
from keboola_mcp_server.tools.validation import validate_flow_configuration_against_schema

LOG = logging.getLogger(__name__)


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
            get_flows,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer,
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
            modify_flow,
            annotations=ToolAnnotations(destructiveHint=True),
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
    Returns the JSON schema for the given flow type (markdown).

    PRE-REQUISITES:
    - Unknown schema for the target flow type: `keboola.flow` (conditional) or `keboola.orchestrator` (legacy)

    RULES:
    - Projects without conditional flows enabled cannot request `keboola.flow` schema
    - Use the returned schema to shape `phases` and `tasks` for `create_flow` / `create_conditional_flow` /
    `update_flow`
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
    Creates a new legacy (non-conditional) flow using `keboola.orchestrator`.

    PRE-REQUISITES:
    - Always use `get_flow_schema` with flow_type="keboola.orchestrator" and review `get_flow_examples` if unknown
    - Collect component configuration IDs for every task you include

    RULES:
    - `phases` and `tasks` must follow the orchestrator schema; each entry must include `id` and `name`
    - Phases run sequentially; tasks inside a phase run in parallel
    - Use `dependsOn` on phases to sequence them; reference other phase ids
    - Always share the returned links with the user

    WHEN TO USE:
    - Simple/linear orchestrations without branching or conditions
    - ETL/ELT pipelines where phases just need ordering and parallel task groups
    """
    flow_type = ORCHESTRATOR_COMPONENT_ID
    flow_configuration = get_flow_configuration(phases=phases, tasks=tasks, flow_type=flow_type)

    # Validate flow structure before to catch semantic errors in the structure
    validate_flow_structure(cast(JsonDict, flow_configuration), flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
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
    Creates a new conditional flow configuration using `keboola.flow`.

    PRE-REQUISITES:
    - Always use `get_flow_schema` with flow_type="keboola.flow" and review `get_flow_examples` if unknown
    - Gather component configuration IDs for all tasks you include

    RULES:
    - `phases` and `tasks` must follow the keboola.flow schema; each entry needs `id` and `name`
    - Exactly one entry phase (no incoming transitions); all phases must be reachable
    - Connect phases via `next` transitions; no cycles or dangling phases; empty `next` means flow end
    - Task/phase failures already stop the flow; add retries/conditions only if the user requests them
    - Always share the returned links with the user

    WHEN TO USE:
    - Flows needing branching, conditions, retries, or notifications
    - Default choice when user simply says “create a flow,” unless they explicitly want legacy orchestrator behavior
    """
    flow_type = CONDITIONAL_FLOW_COMPONENT_ID
    flow_configuration = get_flow_configuration(phases=phases, tasks=tasks, flow_type=flow_type)

    # Validate flow structure to catch semantic errors in the structure
    validate_flow_structure(flow_configuration=flow_configuration, flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
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
    configuration_id: Annotated[str, Field(description='ID of the flow configuration.')],
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
    Updates an existing flow configuration (either legacy `keboola.orchestrator` or conditional `keboola.flow`).

    PRE-REQUISITES:
    - Always use `get_flow_schema` (and `get_flow_examples`) for that flow type you want to update to follow the
    required structure and see the examples if unknown
    - Only pass `phases`/`tasks` when you want to replace them; omit to keep the existing ones unchanged

    RULES (ALL FLOWS):
    - `flow_type` must match the stored component id of the flow; do not switch flow types during update
    - `phases` and `tasks` must follow the schema for the selected flow type; include at least `id` and `name`
    - Tasks must reference existing component configurations; keep dependencies consistent
    - Always provide a clear `change_description` and surface any links returned in the response to the user

    CONDITIONAL FLOWS (`keboola.flow`):
    - Maintain a single entry phase and ensure every phase is reachable; connect phases via `next` transitions
    - No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

    LEGACY FLOWS (`keboola.orchestrator`):
    - Phases run sequentially; tasks inside a phase run in parallel; `dependsOn` references other phase ids
    - Use `continueOnFailure` or best-effort patterns only when the user explicitly asks for them

    WHEN TO USE:
    - Renaming a flow, updating descriptions, adding/removing phases or tasks, or adjusting dependencies
    """
    return await modify_flow(
        ctx=ctx,
        configuration_id=configuration_id,
        flow_type=flow_type,
        change_description=change_description,
        phases=phases,
        tasks=tasks,
        name=name,
        description=description,
        schedules=tuple(),
    )


@tool_errors()
async def modify_flow(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the flow configuration.')],
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
    schedules: Annotated[
        Sequence[ScheduleRequest],
        Field(
            description=(
                'Optional sequence of schedule requests to add/update/remove schedules for this flow. '
                'Each request must have "action": "add"|"update"|"remove". '
                'For add: include "cron_tab", "state" ("enabled"|"disabled"), "timezone". '
                'For update/remove: include "schedule_id". '
                'Example: [{"action": "add", "cron_tab": "0 8 * * 1-5", "state": "enabled", "timezone": "UTC"}]'
            )
        ),
    ] = tuple(),
) -> FlowToolOutput:
    """
    Updates an existing flow configuration (either legacy `keboola.orchestrator` or conditional `keboola.flow`) or
    manages schedules for this flow.

    PRE-REQUISITES:
    - Always use `get_flow_schema` (and `get_flow_examples`) for that flow type you want to update to follow the
    required structure and see the examples if unknown
    - Only pass `phases`/`tasks` when you want to replace them; omit to keep the existing ones unchanged

    RULES (ALL FLOWS):
    - `flow_type` must match the stored component id of the flow; do not switch flow types during update
    - `phases` and `tasks` must follow the schema for the selected flow type; include at least `id` and `name`
    - Tasks must reference existing component configurations; keep dependencies consistent
    - Always provide a clear `change_description` and surface any links returned in the response to the user
    - A flow can have multiple schedules for automation runs. Add/update/remove schedules only if requested.
    - When updating a flow or a schedule, specify only the fields you want to update, others will be kept unchanged.

    CONDITIONAL FLOWS (`keboola.flow`):
    - Maintain a single entry phase and ensure every phase is reachable; connect phases via `next` transitions
    - No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

    LEGACY FLOWS (`keboola.orchestrator`):
    - Phases run sequentially; tasks inside a phase run in parallel; `dependsOn` references other phase ids
    - Use `continueOnFailure` or best-effort patterns only when the user explicitly asks for them

    WHEN TO USE:
    - Renaming a flow, updating descriptions, adding/removing phases or tasks, updating schedules or
    adjusting dependencies
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

    response_message = None
    has_config_changes = bool(name) or bool(description) or phases is not None or tasks is not None

    if has_config_changes:
        LOG.info(f'Updating flow configuration: {configuration_id} (type: {flow_type})')
        _, flow_configuration = await update_flow_internal(
            client=client,
            configuration_id=configuration_id,
            flow_type=flow_type,
            change_description=change_description,
            phases=phases,
            tasks=tasks,
            name=name,
            description=description,
        )
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
    else:
        current_config = await client.storage_client.configuration_detail(
            component_id=flow_type,
            configuration_id=configuration_id,
        )
        api_config = CreateConfigurationAPIResponse.model_validate(current_config)

    links_manager = await ProjectLinksManager.from_client(client)
    flow_links = links_manager.get_flow_links(flow_id=api_config.id, flow_name=api_config.name, flow_type=flow_type)
    # Process schedule requests if provided
    if schedules is not None and len(schedules) > 0:
        responses = await process_schedule_request(
            client=client,
            target_component_id=flow_type,
            target_configuration_id=configuration_id,
            requests=schedules,
        )
        response_message = 'Schedules request processed successfully: \n' + '\n'.join(responses)
        LOG.info(f'Successfully processed {len(schedules)} schedule request(s) for flow {configuration_id}')
        flow_links.append(links_manager.get_scheduler_detail_link(configuration_id, flow_type))

    tool_response = FlowToolOutput(
        configuration_id=api_config.id,
        component_id=flow_type,
        description=api_config.description or '',
        version=api_config.version,
        timestamp=datetime.now(timezone.utc),
        response=response_message,
        success=True,
        links=flow_links,
    )
    LOG.info(f'Updated flow configuration: {api_config.id}')
    return tool_response


async def update_flow_internal(
    *,
    client: KeboolaClient,
    configuration_id: str,
    flow_type: FlowType,
    change_description: str,
    phases: list[dict[str, Any]] | None = None,
    tasks: list[dict[str, Any]] | None = None,
    name: str = '',
    description: str = '',
) -> tuple[JsonDict, JsonDict]:
    current_config = await client.storage_client.configuration_detail(
        component_id=flow_type, configuration_id=configuration_id
    )
    flow_configuration = cast(JsonDict, current_config.get('configuration', {}))
    flow_configuration = copy.deepcopy(flow_configuration)

    updated_configuration = get_flow_configuration(phases=phases, tasks=tasks, flow_type=flow_type)
    if updated_configuration.get('phases'):
        flow_configuration['phases'] = updated_configuration['phases']
    if updated_configuration.get('tasks'):
        flow_configuration['tasks'] = updated_configuration['tasks']

    # Validate flow structure to catch semantic errors in the structure
    validate_flow_structure(flow_configuration=flow_configuration, flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type)

    return current_config, flow_configuration


@tool_errors()
async def get_flows(
    ctx: Context,
    flow_ids: Annotated[
        Sequence[str],
        Field(
            description=(
                'IDs of flows to retrieve full details for. '
                'When provided (non-empty), returns full flow configurations including phases and tasks. '
                'When empty [], lists all flows in the project as summaries.'
            )
        ),
    ] = tuple(),
) -> GetFlowsOutput:
    """
    Lists flows or retrieves full details for specific flows.

    OPTIONS:
    - `flow_ids=[]` → summaries of all flows in the project
    - `flow_ids=["id1", ...]` → full details (including phases/tasks) for those flows
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    # Case 1: flow_ids provided - return full details for those flows
    if flow_ids:

        async def fetch_flow_detail(flow_id: str) -> Flow:
            api_flow, found_type = await resolve_flow_by_id(client, flow_id)
            LOG.info(f'Found flow {flow_id} under flow type {found_type}.')
            links = links_manager.get_flow_links(
                api_flow.configuration_id, flow_name=api_flow.name, flow_type=found_type
            )
            return Flow.from_api_response(api_config=api_flow, flow_component_id=found_type, links=links)

        results = await process_concurrently(flow_ids, fetch_flow_detail)
        flows = unwrap_results(results, 'Failed to fetch one or more flows')

        LOG.info(f'Retrieved full details for {len(flows)} flows.')
        flows = await fetch_schedules_for_flows(client=client, links_manager=links_manager, list_of_flows=flows)
        return GetFlowsDetailOutput(flows=flows)

    # Case 2: no flow_ids - list all flows as summaries
    flows = await get_all_flows(client)
    LOG.info(f'Retrieved {len(flows)} flows.')
    links = [
        links_manager.get_flows_dashboard_link(ORCHESTRATOR_COMPONENT_ID),
        links_manager.get_flows_dashboard_link(CONDITIONAL_FLOW_COMPONENT_ID),
    ]
    return GetFlowsListOutput(flows=flows, links=links)


@tool_errors()
async def get_flow_examples(
    ctx: Context,
    flow_type: Annotated[FlowType, Field(description='The type of the flow to retrieve examples for.')],
) -> Annotated[str, Field(description='Examples of the flow configurations.')]:
    """
    Retrieves examples of valid flow configurations.

    PRE-REQUISITES:
    - Unknown examples for the target flow type: `keboola.flow` (conditional) or `keboola.orchestrator` (legacy) to help
    build the specific flow configuration by mirroring the structure/fields.

    RULES:
    - Conditional-flow examples require conditional flows to be enabled; otherwise use legacy orchestrator examples
    - Present the examples or cite unavailability to the user
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
