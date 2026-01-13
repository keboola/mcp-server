import copy
import logging
from typing import Any

import jsonschema
import pydantic
import yaml
from pydantic import AliasChoices, BaseModel, Field, TypeAdapter, field_validator
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from keboola_mcp_server.clients import KeboolaClient
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID
from keboola_mcp_server.mcp import ServerState, SessionStateMiddleware
from keboola_mcp_server.tools import data_apps as data_app_tools
from keboola_mcp_server.tools.components import tools as components_tools
from keboola_mcp_server.tools.components.model import ConfigParamUpdate, TfParamUpdate
from keboola_mcp_server.tools.components.utils import get_sql_transformation_id_from_sql_dialect
from keboola_mcp_server.tools.flow import tools as flow_tools
from keboola_mcp_server.tools.flow.scheduler import list_schedules_for_config, validate_cron_tab
from keboola_mcp_server.tools.flow.scheduler_model import ScheduleDetail, ScheduleRequest
from keboola_mcp_server.tools.flow.tools import MODIFY_FLOW_TOOL_NAME, UPDATE_FLOW_TOOL_NAME
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


class PreviewConfigDiffRq(BaseModel):
    tool_name: str = Field(
        validation_alias=AliasChoices('toolName', 'tool_name', 'tool-name', 'ToolName'),
        serialization_alias='toolName',
    )
    tool_params: dict[str, Any] = Field(
        validation_alias=AliasChoices('toolParams', 'tool_params', 'tool-params', 'ToolParams'),
        serialization_alias='toolParams',
    )


class ConfigCoordinates(BaseModel):
    component_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices('componentId', 'component_id', 'component-id', 'ComponentId'),
        serialization_alias='componentId',
    )
    configuration_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices('configurationId', 'configuration_id', 'configuration-id', 'ConfigurationId'),
        serialization_alias='configurationId',
    )
    configuration_row_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            'configurationRowId', 'configuration_row_id', 'configuration-row-id', 'ConfigurationRowId'
        ),
        serialization_alias='configurationRowId',
    )

    @field_validator('component_id', 'configuration_id', 'configuration_row_id', mode='before')
    @classmethod
    def convert_to_string(cls, v):
        """Convert any value to string, preserving None."""
        return None if v is None else str(v)


class PreviewConfigDiffResp(BaseModel):
    coordinates: ConfigCoordinates = Field(
        validation_alias=AliasChoices('coordinates', 'Coordinates'),
        serialization_alias='coordinates',
    )
    original_config: dict[str, Any] | None = Field(
        validation_alias=AliasChoices('originalConfig', 'original_config', 'original-config', 'OriginalConfig'),
        serialization_alias='originalConfig',
    )
    updated_config: dict[str, Any] | None = Field(
        validation_alias=AliasChoices('updatedConfig', 'updated_config', 'updated-config', 'UpdatedConfig'),
        serialization_alias='updatedConfig',
    )
    is_valid: bool = Field(
        validation_alias=AliasChoices('isValid', 'is_valid', 'is-valid', 'IsValid'),
        serialization_alias='isValid',
    )
    validation_errors: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices('validationErrors', 'validation_errors', 'validation-errors', 'ValidationErrors'),
        serialization_alias='validationErrors',
    )


async def _extract_coordinates(
    tool_name: str, tool_params: dict[str, Any], workspace_manager: WorkspaceManager
) -> ConfigCoordinates:
    """Extract configuration coordinates from tool parameters."""
    if tool_name == 'update_config':
        return ConfigCoordinates(
            component_id=tool_params.get('component_id'),
            configuration_id=tool_params.get('configuration_id'),
        )
    elif tool_name == 'update_config_row':
        return ConfigCoordinates(
            component_id=tool_params.get('component_id'),
            configuration_id=tool_params.get('configuration_id'),
            configuration_row_id=tool_params.get('configuration_row_id'),
        )
    elif tool_name == 'update_sql_transformation':
        sql_dialect = await workspace_manager.get_sql_dialect()
        return ConfigCoordinates(
            component_id=get_sql_transformation_id_from_sql_dialect(sql_dialect),
            configuration_id=tool_params.get('configuration_id'),
        )
    elif tool_name in {UPDATE_FLOW_TOOL_NAME, MODIFY_FLOW_TOOL_NAME}:
        return ConfigCoordinates(
            component_id=tool_params.get('flow_type'),
            configuration_id=tool_params.get('configuration_id'),
        )
    elif tool_name == 'modify_data_app':
        return ConfigCoordinates(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=tool_params.get('configuration_id'),
        )
    else:
        raise ValueError(f'Invalid tool name: "{tool_name}"')


async def _validate_tool_params(
    tool_name: str,
    tool_params: dict[str, Any],
    tool_input_schema: dict[str, Any],
) -> tuple[bool, str | None]:
    """
    Validate tool parameters against the tool's input schema using JSON schema validation.

    This validates the parameters without executing the tool function.

    :param tool_name: Name of the MCP tool to validate against
    :param tool_params: Parameters to validate (raw user-provided params)
    :param tool_input_schema:
    :return: Tuple of (is_valid, validation_errors)
        - is_valid: True if validation passed, False otherwise
        - validation_errors: List of error messages if validation failed, None if successful
    """
    try:
        jsonschema.validate(instance=tool_params, schema=tool_input_schema)
        return True, None

    except jsonschema.ValidationError as e:
        # Format a validation error message similarly to errors.prettify_validation_error() function
        header = f'Found 1 validation error for {tool_name}:'
        formatted = {
            'errors': [
                {
                    'field': '.'.join(str(p) for p in e.path or []),
                    'message': e.message,
                    'extra': {
                        'schema': e.schema,
                    },
                }
            ]
        }
        try:
            yaml_str = yaml.dump(formatted, default_flow_style=False, sort_keys=False, allow_unicode=True)
        except Exception:
            yaml_str = str(formatted)

        return False, f'{header}\n{yaml_str}'

    except jsonschema.SchemaError as e:
        # Schema itself is invalid
        LOG.exception(f"[validate_tool_params] Invalid schema for tool '{tool_name}': {e}")
        return False, 'Internal error: Invalid tool schema'

    except Exception as e:
        # Handle other unexpected errors
        LOG.exception(f'[validate_tool_params] Unexpected error: {e}')
        return False, f'Validation error: {str(e)}'


def _prepare_mutator(
    preview_rq: PreviewConfigDiffRq, client: KeboolaClient, workspace_manager: WorkspaceManager
) -> tuple[Any, dict[str, Any]]:
    """
    Prepare mutator function and parameters for config diff preview.

    :param preview_rq: PreviewConfigDiffRq object containing tool parameters and name.
    :param client: KeboolaClient instance for API operations.
    :param workspace_manager: WorkspaceManager instance for workspace operations.
    :return: Tuple containing mutator function and parameters.
    """
    mutator_params: dict[str, Any] = {
        **preview_rq.tool_params,
        'client': client,
    }

    if preview_rq.tool_name == 'update_config':
        mutator_fn = components_tools.update_config_internal
        if parameter_updates := mutator_params.get('parameter_updates'):
            type_adapter = TypeAdapter(list[ConfigParamUpdate])
            mutator_params['parameter_updates'] = type_adapter.validate_python(parameter_updates)

    elif preview_rq.tool_name == 'update_config_row':
        mutator_fn = components_tools.update_config_row_internal
        if parameter_updates := mutator_params.get('parameter_updates'):
            type_adapter = TypeAdapter(list[ConfigParamUpdate])
            mutator_params['parameter_updates'] = type_adapter.validate_python(parameter_updates)

    elif preview_rq.tool_name == 'update_sql_transformation':
        mutator_fn = components_tools.update_sql_transformation_internal
        mutator_params['workspace_manager'] = workspace_manager
        if parameter_updates := mutator_params.get('parameter_updates'):
            type_adapter = TypeAdapter(list[TfParamUpdate])
            mutator_params['parameter_updates'] = type_adapter.validate_python(parameter_updates)

    elif preview_rq.tool_name in {UPDATE_FLOW_TOOL_NAME, MODIFY_FLOW_TOOL_NAME}:
        mutator_fn = flow_tools.update_flow_internal
        mutator_params.pop('schedules', None)

    elif preview_rq.tool_name == 'modify_data_app':
        mutator_fn = data_app_tools.modify_data_app_internal
        mutator_params['workspace_manager'] = workspace_manager

    else:
        raise ValueError(f'Invalid tool name: "{preview_rq.tool_name}"')

    return mutator_fn, mutator_params


def _apply_schedule_preview(
    schedules: list[ScheduleDetail],
    requests: list[ScheduleRequest],
) -> list[ScheduleDetail]:
    updated_schedules = [schedule.model_copy(deep=True) for schedule in schedules]
    schedule_index = {schedule.schedule_id: idx for idx, schedule in enumerate(updated_schedules)}
    new_index = 1

    for request in requests:
        validate_cron_tab(request.cron_tab)
        if request.action == 'add':
            if request.cron_tab is None:
                raise ValueError('Cron tab is required when creating a new schedule')
            schedule_id = f'preview-new-{new_index}'
            new_index += 1
            updated_schedules.append(
                ScheduleDetail(
                    schedule_id=schedule_id,
                    timezone=request.timezone or 'UTC',
                    state=request.state or 'enabled',
                    cron_tab=request.cron_tab,
                    target_executions=[],
                )
            )
            schedule_index[schedule_id] = len(updated_schedules) - 1
        elif request.action == 'update':
            if not request.schedule_id:
                raise ValueError('Schedule ID is required to update a schedule')
            schedule_idx = schedule_index.get(request.schedule_id)
            if schedule_idx is None:
                raise ValueError(f'Schedule ID "{request.schedule_id}" not found')
            schedule = updated_schedules[schedule_idx]
            updates: dict[str, Any] = {}
            if request.cron_tab is not None:
                updates['cron_tab'] = request.cron_tab
            if request.timezone is not None:
                updates['timezone'] = request.timezone
            if request.state is not None:
                updates['state'] = request.state
            if updates:
                updated_schedules[schedule_idx] = schedule.model_copy(update=updates)
        elif request.action == 'remove':
            if not request.schedule_id:
                raise ValueError('Schedule ID is required to remove a schedule')
            if request.schedule_id not in schedule_index:
                raise ValueError(f'Schedule ID "{request.schedule_id}" not found')
            updated_schedules = [schedule for schedule in updated_schedules if schedule.schedule_id != request.schedule_id]
            schedule_index = {schedule.schedule_id: idx for idx, schedule in enumerate(updated_schedules)}
        else:
            raise ValueError(f'Invalid action for schedulers: {request.action}.')

    return updated_schedules


async def preview_config_diff(rq: Request) -> Response:
    preview_rq = PreviewConfigDiffRq.model_validate(await rq.json())

    LOG.info(f'[preview_config_diff] {preview_rq}')
    LOG.info(f'[preview_config_diff] rq.app={rq.app}')
    LOG.info(f'[preview_config_diff] rq.app.state={rq.app.state} vars={vars(rq.app.state)}')
    LOG.info(f'[preview_config_diff] rq.state={rq.state} vars={vars(rq.state)}')

    server_state = ServerState.from_starlette(rq.app)
    config = SessionStateMiddleware.apply_request_config(rq, server_state.config)
    state = SessionStateMiddleware.create_session_state(config, server_state.runtime_info, readonly=True)
    client = KeboolaClient.from_state(state)
    workspace_manager = WorkspaceManager.from_state(state)

    coordinates = await _extract_coordinates(preview_rq.tool_name, preview_rq.tool_params, workspace_manager)

    if tool_input_schema := rq.app.state.mcp_tools_input_schema.get(preview_rq.tool_name):
        is_valid, validation_errors = await _validate_tool_params(
            tool_name=preview_rq.tool_name,
            tool_params=preview_rq.tool_params,
            tool_input_schema=tool_input_schema,
        )

        if not is_valid:
            preview_resp = PreviewConfigDiffResp(
                coordinates=coordinates,
                original_config=None,
                updated_config=None,
                is_valid=False,
                validation_errors=[validation_errors],
            )
            return JSONResponse(preview_resp.model_dump(by_alias=True, exclude_none=True))
    else:
        LOG.warning(f'[preview_config_diff] No input schema found for tool "{preview_rq.tool_name}"')

    mutator_fn, mutator_params = _prepare_mutator(preview_rq, client, workspace_manager)

    try:
        original_config, new_config, *_ = await mutator_fn(**mutator_params)
        if isinstance(original_config, BaseModel):
            original_config = original_config.model_dump()

        updated_config = copy.deepcopy(original_config)
        updated_config['configuration'] = new_config
        if name := preview_rq.tool_params.get('name'):
            updated_config['name'] = name
        description = preview_rq.tool_params.get('description') or preview_rq.tool_params.get('updated_description')
        if description:
            updated_config['description'] = description
        if change_description := preview_rq.tool_params.get('change_description'):
            updated_config['changeDescription'] = change_description
        if preview_rq.tool_name in {UPDATE_FLOW_TOOL_NAME, MODIFY_FLOW_TOOL_NAME}:
            schedule_payload = preview_rq.tool_params.get('schedules') or []
            if schedule_payload:
                type_adapter = TypeAdapter(list[ScheduleRequest])
                schedule_requests = type_adapter.validate_python(schedule_payload)
                flow_type = preview_rq.tool_params.get('flow_type')
                configuration_id = preview_rq.tool_params.get('configuration_id')
                if not flow_type or not configuration_id:
                    raise ValueError('Flow type and configuration ID are required to preview schedules')
                original_schedules = await list_schedules_for_config(
                    client=client,
                    component_id=flow_type,
                    configuration_id=configuration_id,
                )
                updated_schedules = _apply_schedule_preview(original_schedules, schedule_requests)
                original_config['schedules'] = [s.model_dump(by_alias=True) for s in original_schedules]
                updated_config['schedules'] = [s.model_dump(by_alias=True) for s in updated_schedules]

        preview_resp = PreviewConfigDiffResp(
            coordinates=coordinates,
            original_config=original_config,
            updated_config=updated_config,
            is_valid=True,
            validation_errors=None,
        )

    except (pydantic.ValidationError, jsonschema.ValidationError, ValueError) as ex:
        LOG.exception(f'[preview_config_diff] {ex}')
        preview_resp = PreviewConfigDiffResp(
            coordinates=coordinates,
            original_config=None,
            updated_config=None,
            is_valid=False,
            validation_errors=[str(ex)],
        )

    return JSONResponse(preview_resp.model_dump(by_alias=True, exclude_none=True))
