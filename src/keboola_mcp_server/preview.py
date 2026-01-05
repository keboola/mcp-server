import copy
import logging
from typing import Any

import jsonschema
import pydantic
from pydantic import AliasChoices, BaseModel, Field
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from keboola_mcp_server.clients import KeboolaClient
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID
from keboola_mcp_server.mcp import ServerState, SessionStateMiddleware
from keboola_mcp_server.tools import data_apps as data_app_tools
from keboola_mcp_server.tools.components import tools as components_tools
from keboola_mcp_server.tools.components.utils import get_sql_transformation_id_from_sql_dialect
from keboola_mcp_server.tools.flow import tools as flow_tools
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
    component_id: str = Field(
        validation_alias=AliasChoices('componentId', 'component_id', 'component-id', 'ComponentId'),
        serialization_alias='componentId',
    )
    configuration_id: str = Field(
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

    mutator_params: dict[str, Any] = {
        'client': client,
    }

    if preview_rq.tool_name == 'update_config':
        coordinates = ConfigCoordinates(
            component_id=preview_rq.tool_params.get('component_id'),
            configuration_id=preview_rq.tool_params.get('configuration_id'),
        )
        mutator_fn = components_tools.update_config_internal

    elif preview_rq.tool_name == 'update_config_row':
        coordinates = ConfigCoordinates(
            component_id=preview_rq.tool_params.get('component_id'),
            configuration_id=preview_rq.tool_params.get('configuration_id'),
            configuration_row_id=preview_rq.tool_params.get('configuration_row_id'),
        )
        mutator_fn = components_tools.update_config_row_internal

    elif preview_rq.tool_name == 'update_sql_transformation':
        workspace_manager = WorkspaceManager.from_state(state)
        coordinates = ConfigCoordinates(
            component_id=get_sql_transformation_id_from_sql_dialect(await workspace_manager.get_sql_dialect()),
            configuration_id=preview_rq.tool_params.get('configuration_id'),
        )
        mutator_fn = components_tools.update_sql_transformation_internal
        mutator_params['workspace_manager'] = workspace_manager

    elif preview_rq.tool_name == 'update_flow':
        coordinates = ConfigCoordinates(
            component_id=preview_rq.tool_params.get('flow_type'),
            configuration_id=preview_rq.tool_params.get('configuration_id'),
        )
        mutator_fn = flow_tools.update_flow_internal

    elif preview_rq.tool_name == 'modify_data_app':
        coordinates = ConfigCoordinates(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=preview_rq.tool_params.get('configuration_id'),
        )
        mutator_fn = data_app_tools.modify_data_app_internal
        mutator_params['workspace_manager'] = WorkspaceManager.from_state(state)

    else:
        raise ValueError(f'Invalid tool name: "{preview_rq.tool_name}"')

    try:
        original_config, new_config, *_ = await mutator_fn(**mutator_params, **preview_rq.tool_params)
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
