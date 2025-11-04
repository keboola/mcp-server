import asyncio
import importlib.resources as resources
import logging
import re
from typing import Annotated, Any, Literal, Optional, Sequence, Union, cast

import httpx
from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import DataAppConfig, DataAppResponse
from keboola_mcp_server.clients.storage import ConfigurationAPIResponse
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.tools.components.utils import set_cfg_creation_metadata, set_cfg_update_metadata
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

DATA_APP_TOOLS_TAG = 'data-apps'


def add_data_app_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(
        FunctionTool.from_function(
            modify_data_app,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_data_apps,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            deploy_data_app,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    LOG.info('Data app tools initialized.')


# State of the data app
State = Literal['created', 'running', 'stopped', 'starting', 'stopping', 'restarting']
# Accepts known states or any string preventing from validation errors when receiving unknown states from the API
# LLM agent can still understand the state of the data app even if it is different from the known states
SafeState = Union[State, str]
# Type of the data app
Type = Literal['streamlit']
# Accepts known types or any string preventing from validation errors when receiving unknown types from the API
# LLM agent can still understand the type of the data app even if it is different from the known types
SafeType = Union[Type, str]

_DATA_APP_RESOURCES = resources.files('keboola_mcp_server.resources.data_app')
_QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE = _DATA_APP_RESOURCES.joinpath('qsapi_query_data_code.py').read_text(
    encoding='utf-8'
)
_STORAGE_QUERY_DATA_FUNCTION_CODE = _DATA_APP_RESOURCES.joinpath('sapi_query_data_code.py').read_text(encoding='utf-8')

_DEFAULT_STREAMLIT_THEME = (
    '[theme]\nfont = "sans serif"\ntextColor = "#222529"\nbackgroundColor = "#FFFFFF"\nsecondaryBackgroundColor = '
    '"#E6F2FF"\nprimaryColor = "#1F8FFF"'
)
_DEFAULT_PACKAGES = ['pandas', 'httpx']

INJECTED_BLOCK_RE = re.compile(
    r'(?P<before>.*?)#\s###\sINJECTED_CODE\s####.*?#\s###\sEND_OF_INJECTED_CODE\s####(?P<after>.*)',
    re.DOTALL,
)

# Type of the authentication used in the data app
AuthenticationType = Literal['no-auth', 'basic-auth', 'default']


class DataAppSummary(BaseModel):
    """A summary of a data app used for sync operations."""

    component_id: str = Field(description='The ID of the data app component.')
    configuration_id: str = Field(description='The ID of the data app config.')
    data_app_id: str = Field(description='The ID of the data app.')
    project_id: str = Field(description='The ID of the project.')
    branch_id: str = Field(description='The ID of the branch.')
    config_version: str = Field(description='The version of the data app config.')
    state: SafeState = Field(description='The state of the data app.')
    type: SafeType = Field(
        description=(
            'The type of the data app. Currently, only "streamlit" is supported in the MCP. However, Keboola DSAPI '
            'supports additional types, which can be retrieved from the API.'
        )
    )
    deployment_url: Optional[str] = Field(description='The URL of the running data app.', default=None)
    auto_suspend_after_seconds: int = Field(
        description='The number of seconds after which the running data app is automatically suspended.'
    )

    @classmethod
    def from_api_response(cls, api_response: DataAppResponse) -> 'DataAppSummary':
        return cls(
            component_id=api_response.component_id,
            configuration_id=api_response.config_id,
            data_app_id=api_response.id,
            project_id=api_response.project_id,
            branch_id=api_response.branch_id or '',
            config_version=api_response.config_version,
            state=api_response.state,
            type=api_response.type,
            deployment_url=api_response.url,
            auto_suspend_after_seconds=api_response.auto_suspend_after_seconds,
        )


class DeploymentInfo(BaseModel):
    """Deployment information of a data app."""

    version: str = Field(description='The version of the data app deployment.')
    state: str = Field(description='The state of the data app deployment.')
    url: Optional[str] = Field(description='The URL of the running data app deployment.', default=None)
    last_request_timestamp: Optional[str] = Field(
        description='The last request timestamp of the data app deployment.', default=None
    )
    last_start_timestamp: Optional[str] = Field(
        description='The last start timestamp of the data app deployment.', default=None
    )
    logs: list[str] = Field(
        description='The latest 20 log lines reported in the data app deployment.', default_factory=list
    )


class DataApp(BaseModel):
    """A data app used for detail views."""

    name: str = Field(description='The name of the data app.')
    description: Optional[str] = Field(description='The description of the data app.', default=None)
    component_id: str = Field(description='The ID of the data app component.')
    configuration_id: str = Field(description='The ID of the data app configuration.')
    data_app_id: str = Field(description='The ID of the data app.')
    project_id: str = Field(description='The ID of the project.')
    branch_id: str = Field(description='The ID of the branch.')
    config_version: str = Field(description='The version of the data app config.')
    state: SafeState = Field(description='The state of the data app.')
    type: SafeType = Field(
        description=(
            'The type of the data app. Currently, only "streamlit" is supported in the MCP. However, Keboola DSAPI '
            'supports additional types, which can be retrieved from the API.'
        )
    )
    deployment_url: Optional[str] = Field(description='The URL of the running data app.', default=None)
    auto_suspend_after_seconds: int = Field(
        description='The number of seconds after which the running data app is automatically suspended.'
    )
    parameters: dict[str, Any] = Field(description='The parameters settings of the data app.')
    authorization: dict[str, Any] = Field(description='The authorization settings of the data app.')
    storage: dict[str, Any] = Field(
        description='The storage input/output mapping of the data app.', default_factory=dict
    )
    deployment_info: Optional[DeploymentInfo] = Field(
        description='Deployment info of the data app including a url of the app and logs to diagnose in-app errors.',
        default=None,
    )
    links: list[Link] = Field(description='Navigation links for the web interface.', default_factory=list)

    @classmethod
    def from_api_responses(
        cls,
        api_response: DataAppResponse,
        api_configuration: ConfigurationAPIResponse,
    ) -> 'DataApp':
        parameters = api_configuration.configuration.get('parameters', {}) or {}
        authorization = api_configuration.configuration.get('authorization', {}) or {}
        storage = api_configuration.configuration.get('storage', {}) or {}
        return cls(
            component_id=api_configuration.component_id,
            configuration_id=api_configuration.configuration_id,
            data_app_id=api_response.id,
            project_id=api_response.project_id,
            branch_id=api_response.branch_id or '',
            config_version=str(api_configuration.version),
            state=api_response.state,
            type=api_response.type,
            deployment_url=api_response.url,
            auto_suspend_after_seconds=api_response.auto_suspend_after_seconds,
            name=api_configuration.name,
            description=api_configuration.description,
            parameters=parameters,
            authorization=authorization,
            storage=storage,
            deployment_info=None,
            links=[],
        )

    def with_links(self, links: list[Link]) -> 'DataApp':
        self.links = links
        return self

    def with_deployment_info(self, logs: list[str]) -> 'DataApp':
        """Adds deployment info to the data app.

        :param logs: The logs of the data app deployment.
        :return: The data app with the deployment info.
        """
        self.deployment_info = DeploymentInfo(
            version=self.config_version,
            state=self.state,
            url=self.deployment_url or 'deployment link not available yet',
            logs=logs,
        )
        return self


class ModifiedDataAppOutput(BaseModel):
    """Modified data app output containing the response of the action performed and the data app and links to the web
    interface."""

    response: str = Field(description='The response of the action performed with potential additional information.')
    data_app: DataAppSummary = Field(description='The data app.')
    links: list[Link] = Field(description='Navigation links for the web interface.')


class DeploymentDataAppOutput(BaseModel):
    """Deployment data app output containing the action performed, links and deployment info."""

    state: SafeState = Field(description='The state of the data app deployment.')
    deployment_info: DeploymentInfo | None = Field(
        description='Deployment info with a link to the app and logs to diagnose in-app errors.', default=None
    )
    links: list[Link] = Field(description='Navigation links for the web interface.')


class GetDataAppsOutput(BaseModel):
    """Output of the get_data_apps tool. Serves for both DataAppSummary and DataApp outputs."""

    data_apps: Sequence[DataAppSummary | DataApp] = Field(description='The data apps in the project.')
    links: list[Link] = Field(description='Navigation links for the web interface.', default_factory=list)


@tool_errors()
async def modify_data_app(
    ctx: Context,
    name: Annotated[str, Field(description='Name of the data app.')],
    description: Annotated[str, Field(description='Description of the data app.')],
    source_code: Annotated[str, Field(description='Complete Python/Streamlit source code for the data app.')],
    packages: Annotated[
        list[str],
        Field(
            description='Python packages used in the source code that will be installed by `pip install` '
            'into the environment before the code runs. For example: ["pandas", "requests~=2.32"].'
        ),
    ],
    authentication_type: Annotated[
        AuthenticationType,
        Field(
            description=(
                'Authentication type, "no-auth" removes authentication completely, "basic-auth" sets the data '
                'app to be secured using the HTTP basic authentication, and "default" keeps the existing '
                'authentication type when updating.'
            )
        ),
    ],
    configuration_id: Annotated[
        str, Field(description='The ID of existing data app configuration when updating, otherwise empty string.')
    ] = '',
    change_description: Annotated[
        str,
        Field(description='The description of the change when updating (e.g. "Update Code"), otherwise empty string.'),
    ] = '',
) -> ModifiedDataAppOutput:
    """Creates or updates a Streamlit data app.

    Considerations:
    - The `source_code` parameter must be a complete and runnable Streamlit app. It must include a placeholder
    `{QUERY_DATA_FUNCTION}` where a `query_data` function will be injected. This function queries the workspace to get
    data, it accepts a string of SQL query following current sql dialect and returns a pandas DataFrame with the results
    from the workspace.
    - Write SQL queries so they are compatible with the current workspace backend, you can ensure this by using the
    `query_data` tool to inspect the data in the workspace before using it in the data app.
    - If you're updating an existing data app, provide the `configuration_id` parameter and the `change_description`
    parameter. To keep existing data app values during an update, leave them as empty strings, lists, or None
    appropriately based on the parameter type.
    - If the data app is updated while running, it must be redeployed for the changes to take effect.
    - New apps use the HTTP basic authentication by default for security unless explicitly specified otherwise; when
    updating, set `authentication_type` to `default` to keep the existing authentication type configuration
    (including OIDC setups) unless explicitly specified otherwise.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    project_id = await client.storage_client.project_id()
    workspace_id = await workspace_manager.get_workspace_id()
    sql_dialect = await workspace_manager.get_sql_dialect()
    branch_id = await workspace_manager.get_branch_id()

    secrets = _get_secrets(
        workspace_id=str(workspace_id),
        branch_id=str(branch_id),
    )

    if configuration_id:
        # Update existing data app
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        existing_config = {
            'parameters': data_app.parameters,
            'authorization': data_app.authorization,
            'storage': data_app.storage,
        }
        updated_config = _update_existing_data_app_config(
            existing_config, name, source_code, packages, authentication_type, secrets, sql_dialect
        )
        updated_config = cast(
            dict[str, Any],
            await client.encryption_client.encrypt(
                updated_config, component_id=DATA_APP_COMPONENT_ID, project_id=project_id
            ),
        )
        await client.storage_client.configuration_update(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration=updated_config,
            change_description=change_description or 'Change Data App',
            updated_name=name or data_app.name,
            updated_description=description or data_app.description,
        )
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        await set_cfg_update_metadata(
            client=client,
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration_version=int(data_app.config_version),
        )
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.authorization),
        )
        response = (
            'updated (redeploy required to apply changes in the running app)'
            if data_app.state in ('running', 'starting')
            else 'updated'
        )
        return ModifiedDataAppOutput(
            response=response, data_app=DataAppSummary.model_validate(data_app.model_dump()), links=links
        )
    else:
        # Create new data app
        config = _build_data_app_config(name, source_code, packages, authentication_type, secrets, sql_dialect)
        config = await client.encryption_client.encrypt(
            config, component_id=DATA_APP_COMPONENT_ID, project_id=project_id
        )
        validated_config = DataAppConfig.model_validate(config)
        data_app_resp = await client.data_science_client.create_data_app(
            name, description, configuration=validated_config
        )
        await set_cfg_creation_metadata(
            client=client,
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=data_app_resp.config_id,
        )
        links = links_manager.get_data_app_links(
            configuration_id=data_app_resp.config_id,
            configuration_name=name,
            deployment_link=data_app_resp.url,
            uses_basic_authentication=_uses_basic_authentication(validated_config.authorization),
        )
        return ModifiedDataAppOutput(
            response='created', data_app=DataAppSummary.from_api_response(data_app_resp), links=links
        )


@tool_errors()
async def get_data_apps(
    ctx: Context,
    configuration_ids: Annotated[Sequence[str], Field(description='The IDs of the data app configurations.')] = tuple(),
    limit: Annotated[int, Field(description='The limit of the data apps to fetch.')] = 100,
    offset: Annotated[int, Field(description='The offset of the data apps to fetch.')] = 0,
) -> GetDataAppsOutput:
    """Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by
    providing their configuration IDs.

    Considerations:
    - If configuration_ids are provided, the tool will return details of the data apps by their configuration IDs.
    - If no configuration_ids are provided, the tool will list all data apps in the project given the limit and offset.
    - Data App detail contains configuration, metadata, source code, links, and deployment info along with the latest
    data app logs to investigate in-app errors. The logs may be updated after opening the data app URL.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if configuration_ids:
        # Get details of the data apps by their configuration IDs using 10 parallel requests at a time to not overload
        # the API
        data_app_details: list[DataApp | str] = []
        batch_size = 10  # fetching 10 data apps details at a time to not overload the API
        for current_batch in range(0, len(configuration_ids), batch_size):
            batch_ids = configuration_ids[current_batch : current_batch + batch_size]
            data_app_details.extend(
                await asyncio.gather(
                    *(
                        _fetch_data_app_details_task(client, links_manager, configuration_id)
                        for configuration_id in batch_ids
                    )
                )
            )
        found_data_apps: list[DataApp] = [dap for dap in data_app_details if isinstance(dap, DataApp)]
        not_found_ids: list[str] = [dap for dap in data_app_details if isinstance(dap, str)]
        if not_found_ids:
            await ctx.log(f'Could not find Data Apps Configurations for IDs: {not_found_ids}', 'error')
            logging.error(f'Could not find Data Apps Configurations for IDs: {not_found_ids}')
        return GetDataAppsOutput(data_apps=found_data_apps)
    else:
        # List all data apps in the project
        data_apps: list[DataAppResponse] = await client.data_science_client.list_data_apps(limit=limit, offset=offset)
        links = [links_manager.get_data_app_dashboard_link()]
        return GetDataAppsOutput(
            data_apps=[DataAppSummary.from_api_response(data_app) for data_app in data_apps],
            links=links,
        )


@tool_errors()
async def deploy_data_app(
    ctx: Context,
    action: Annotated[Literal['deploy', 'stop'], Field(description='The action to perform.')],
    configuration_id: Annotated[str, Field(description='The ID of the data app configuration.')],
) -> DeploymentDataAppOutput:
    """Deploys/redeploys a data app or stops running data app in the Keboola environment asynchronously given the action
    and the configuration ID.

    Considerations:
    - Redeploying a data app takes some time, and the app temporarily may have status "stopped" during this process
    because it needs to restart.
    - After deployment, the deployment info includes the app URL and the latest logs to diagnose in-app errors.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    if action == 'deploy':
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        if data_app.state == 'stopping':
            raise ValueError('Data app is currently "stopping", could not be started at the moment.')
        config_version = await client.storage_client.configuration_version_latest(
            DATA_APP_COMPONENT_ID, data_app.configuration_id
        )
        _ = await client.data_science_client.deploy_data_app(data_app.data_app_id, str(config_version))
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        data_app = data_app.with_deployment_info(await _fetch_logs(client, data_app.data_app_id))
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.authorization),
        )
        return DeploymentDataAppOutput(state=data_app.state, links=links, deployment_info=data_app.deployment_info)
    elif action == 'stop':
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        if data_app.state in ('starting', 'restarting'):
            raise ValueError('Data app is currently "starting", could not be stopped at the moment.')
        _ = await client.data_science_client.suspend_data_app(data_app.data_app_id)
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=None,
            uses_basic_authentication=_uses_basic_authentication(data_app.authorization),
        )
        return DeploymentDataAppOutput(state=data_app.state, links=links, deployment_info=None)
    else:
        raise ValueError(f'Invalid action: {action}')


def _build_data_app_config(
    name: str,
    source_code: str,
    packages: list[str],
    authentication_type: AuthenticationType,
    secrets: dict[str, Any],
    sql_dialect: str,
) -> dict[str, Any]:
    packages = sorted(list(set(packages + _DEFAULT_PACKAGES)))
    slug = _get_data_app_slug(name) or 'Data-App'
    parameters = {
        'size': 'tiny',
        'autoSuspendAfterSeconds': 900,
        'dataApp': {
            'slug': slug,
            'streamlit': {
                'config.toml': _DEFAULT_STREAMLIT_THEME,
            },
            'secrets': secrets,
        },
        'script': [_inject_query_to_source_code(source_code, sql_dialect)],
        'packages': packages,
    }
    # By default secure with basic authorization
    authorization = _get_authorization(authentication_type in ['basic-auth', 'default'])
    return {'parameters': parameters, 'authorization': authorization}


def _update_existing_data_app_config(
    existing_config: dict[str, Any],
    name: str,
    source_code: str,
    packages: list[str],
    authentication_type: AuthenticationType,
    secrets: dict[str, Any],
    sql_dialect: str,
) -> dict[str, Any]:
    new_config = existing_config.copy()
    new_config['parameters']['dataApp']['slug'] = (
        _get_data_app_slug(name) or existing_config['parameters']['dataApp']['slug']
    )
    new_config['parameters']['script'] = (
        [_inject_query_to_source_code(source_code, sql_dialect)]
        if source_code
        else existing_config['parameters']['script']
    )
    new_config['parameters']['packages'] = (
        sorted(list[str](set[str](packages + _DEFAULT_PACKAGES)))
        if packages
        else sorted(list[str](set[str](existing_config['parameters']['packages'] + _DEFAULT_PACKAGES)))
    )
    new_config['parameters']['dataApp']['secrets'] = (
        existing_config['parameters']['dataApp'].get('secrets', {}) | secrets
    )
    new_config['authorization'] = (
        existing_config['authorization']
        if authentication_type == 'default'
        else _get_authorization(authentication_type == 'basic-auth')
    )
    return new_config


async def _fetch_data_app(
    client: KeboolaClient,
    *,
    data_app_id: Optional[str],
    configuration_id: Optional[str],
) -> DataApp:
    """
    Fetches data app from both data-science API and storage API based on the provided data_app_id or
    configuration_id.

    :param client: The Keboola client
    :param data_app_id: The ID of the data app
    :param configuration_id: The ID of the configuration
    :return: The data app
    """

    if data_app_id:
        # Fetch data app from science API to get the configuration ID
        data_app_science = await client.data_science_client.get_data_app(data_app_id)
        raw_data_app_config = await client.storage_client.configuration_detail(
            component_id=DATA_APP_COMPONENT_ID, configuration_id=data_app_science.config_id
        )
        api_config = ConfigurationAPIResponse.model_validate(
            raw_data_app_config | {'component_id': DATA_APP_COMPONENT_ID}
        )
        return DataApp.from_api_responses(data_app_science, api_config)
    elif configuration_id:
        raw_configuration = await client.storage_client.configuration_detail(
            component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
        )
        api_config = ConfigurationAPIResponse.model_validate(
            raw_configuration | {'component_id': DATA_APP_COMPONENT_ID}
        )
        data_app_id = cast(str, api_config.configuration['parameters']['id'])
        data_app_science = await client.data_science_client.get_data_app(data_app_id)
        return DataApp.from_api_responses(data_app_science, api_config)
    else:
        raise ValueError('Either data_app_id or configuration_id must be provided.')


async def _fetch_data_app_details_task(
    client: KeboolaClient, links_manager: ProjectLinksManager, configuration_id: str
) -> DataApp | str:
    """Task fetching data app details with logs and links by configuration ID.
    :param client: The Keboola client
    :param configuration_id: The ID of the data app configuration
    :return: The data app details or the configuration ID if the data app is not found
    """
    try:
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.authorization),
        )
        logs = await _fetch_logs(client, data_app.data_app_id)
        return data_app.with_links(links).with_deployment_info(logs)
    except Exception:
        return configuration_id


async def _fetch_logs(client: KeboolaClient, data_app_id: str) -> list[str]:
    """Fetches the logs of a data app if it is running otherwise returns empty list."""
    try:
        str_logs = await client.data_science_client.tail_app_logs(data_app_id, since=None, lines=20)
        logs = str_logs.split('\n')
        return logs
    except httpx.HTTPStatusError:
        # The data app is not running, return empty list
        return []


def _get_authorization(auth_with_password: bool) -> dict[str, Any]:
    if auth_with_password:
        return {
            'app_proxy': {
                'auth_providers': [{'id': 'simpleAuth', 'type': 'password'}],
                'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['simpleAuth']}],
            },
        }
    else:
        return {
            'app_proxy': {
                'auth_providers': [],
                'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': False}],
            }
        }


def _get_data_app_slug(name: str) -> str:
    return re.sub(r'[^a-z0-9\-]', '', name.lower().replace(' ', '-'))


def _uses_basic_authentication(authorization: dict[str, Any]) -> bool:
    try:
        return any(
            auth_rule['auth_required'] and 'simpleAuth' in auth_rule.get('auth', [])
            for auth_rule in authorization['app_proxy']['auth_rules']
        )
    except Exception:
        return False


def _get_query_function_code(sql_dialect: str) -> str:
    """
    Selects the appropriate query function code for the given SQL dialect.
    - Snowflake: uses Query Service API
    - BigQuery: uses Storage API (Query Service API is not supported for BigQuery yet)
    """
    sql_dialect = sql_dialect.lower()
    if sql_dialect == 'snowflake':
        return _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE
    elif sql_dialect == 'bigquery':
        return _STORAGE_QUERY_DATA_FUNCTION_CODE
    else:
        raise ValueError(f'Unsupported SQL dialect: {sql_dialect}')


def _strip_injected_query_code(source_code: str) -> str:
    """
    Removes injected query_data function code to keep the generated source consistent when reinjecting the code.

    :param source_code: The source code of the data app
    :return: The source code with the injected query_data function code removed
    """
    for snippet in (_QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE, _STORAGE_QUERY_DATA_FUNCTION_CODE):
        source_code = source_code.replace(snippet, '')
    return source_code


def _inject_query_to_source_code(source_code: str, sql_dialect: str) -> str:
    """
    Injects the query_data function into the source code based on the SQL dialect, while removing the
    existing injected code for consistency.

    :param source_code: The source code of the data app
    :param sql_dialect: The SQL dialect of the workspace
    :return: The source code with the query_data function injected
    """
    if not source_code:
        return ''

    query_function_code = _get_query_function_code(sql_dialect)
    if query_function_code in source_code:
        return source_code

    # remove existing injected code to keep the code in sync with the current SQL dialect
    source_code = _strip_injected_query_code(source_code)

    if '{QUERY_DATA_FUNCTION}' in source_code:
        return source_code.replace('{QUERY_DATA_FUNCTION}', query_function_code)

    match = INJECTED_BLOCK_RE.match(source_code)
    if match:
        before = match.group('before').rstrip()
        after = match.group('after').lstrip()
        return f'{before}\n\n{query_function_code}\n\n{after}'
    else:
        return f'{query_function_code}\n\n{source_code.lstrip()}'


def _get_secrets(workspace_id: str, branch_id: str) -> dict[str, Any]:
    """
    Generates secrets for the data app for querying the tables in the given workspace QS or SAPI.
    """
    secrets: dict[str, Any] = {
        'WORKSPACE_ID': workspace_id,
        'BRANCH_ID': branch_id,
    }
    return secrets
