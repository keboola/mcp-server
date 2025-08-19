import base64
import logging
import os
import re
import string
from typing import Annotated, Any, Literal, Optional, Sequence, cast

import httpx
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import DataAppResponse
from keboola_mcp_server.clients.storage import ConfigurationAPIResponse
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.tools.components.utils import set_cfg_creation_metadata, set_cfg_update_metadata
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

DATA_APP_TOOLS_TAG = 'data-app'


def add_data_app_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(
        FunctionTool.from_function(
            sync_data_app,
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
            manage_data_app,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    LOG.info('Data app tools initialized.')


class DataAppSummary(BaseModel):
    """A summary of a data app used for sync operations."""

    component_id: str = Field(description='The ID of the data app component.')
    configuration_id: str = Field(description='The ID of the data app config.')
    data_app_id: str = Field(description='The ID of the data app.')
    project_id: str = Field(description='The ID of the project.')
    branch_id: str = Field(description='The ID of the branch.')
    config_version: str = Field(description='The version of the data app config.')
    state: str = Field(description='The state of the data app.')
    type: str = Field(description='The type of the data app.')
    deployment_url: Optional[str] = Field(description='The URL of the running data app.', default=None)
    auto_suspend_after_seconds: int = Field(description='The auto suspend after seconds.')

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
    """A deployment info of a data app."""

    version: str = Field(description='The version of the data app deployment.')
    state: str = Field(description='The state of the data app deployment.')
    url: Optional[str] = Field(description='The URL of the running data app deployment.', default=None)
    last_request_timestamp: Optional[str] = Field(
        description='The last request timestamp of the data app deployment.', default=None
    )
    last_start_timestamp: Optional[str] = Field(
        description='The last start timestamp of the data app deployment.', default=None
    )
    logs: list[str] = Field(description='The latest 100 logs of the data app deployment.', default_factory=list)


class DataApp(DataAppSummary):
    """A data app used for detail views."""

    name: str = Field(description='The name of the data app.')
    description: Optional[str] = Field(description='The description of the data app.', default=None)
    is_authorized: bool = Field(description='Whether the data app is authorized using simple password or not.')
    parameters: dict[str, Any] = Field(description='The parameters of the data app.')
    authorization: dict[str, Any] = Field(description='The authorization of the data app.')
    storage: dict[str, Any] = Field(
        description='The storage input/output mapping of the data app.', default_factory=dict
    )
    deployment_info: Optional[DeploymentInfo] = Field(description='The deployment info of the data app.', default=None)
    links: list[Link] = Field(description='Navigation links for the web interface.', default_factory=list)

    @classmethod
    def from_api_responses(
        cls,
        api_response: DataAppResponse,
        api_configuration: ConfigurationAPIResponse,
        logs: list[str],
    ) -> 'DataApp':
        parameters = api_configuration.configuration.get('parameters', {})
        authorization = api_configuration.configuration.get('authorization', {})
        storage = api_configuration.configuration.get('storage', {})
        deployment_info = (
            None
            if not logs
            else DeploymentInfo(
                version=api_response.config_version,
                state=api_response.state,
                url=api_response.url or 'not yet available',
                last_request_timestamp=api_response.last_request_timestamp,
                last_start_timestamp=api_response.last_start_timestamp,
                logs=logs,
            )
        )
        return cls(
            component_id=api_response.component_id,
            configuration_id=api_configuration.configuration_id,
            data_app_id=api_response.id,
            project_id=api_response.project_id,
            branch_id=api_response.branch_id or '',
            config_version=api_response.config_version,
            state=api_response.state,
            type=api_response.type,
            deployment_url=api_response.url,
            auto_suspend_after_seconds=api_response.auto_suspend_after_seconds,
            name=api_configuration.name,
            description=api_configuration.description,
            parameters=parameters,
            authorization=authorization,
            storage=storage,
            is_authorized=_is_authorized(authorization),
            deployment_info=deployment_info,
        )

    def add_links(self, links: list[Link]) -> None:
        self.links.extend(links)

    def to_summary(self) -> DataAppSummary:
        return DataAppSummary(
            component_id=self.component_id,
            configuration_id=self.configuration_id,
            data_app_id=self.data_app_id,
            project_id=self.project_id,
            branch_id=self.branch_id,
            config_version=self.config_version,
            state=self.state,
            type=self.type,
            deployment_url=self.deployment_url,
            auto_suspend_after_seconds=self.auto_suspend_after_seconds,
        )


class SyncDataAppOutput(BaseModel):
    """Output of the sync_data_app tool."""

    action: Literal['created', 'updated'] = Field(description='The action performed.')
    data_app: DataAppSummary = Field(description='The data app.')
    links: list[Link] = Field(description='Navigation links for the web interface.')


class ManageDataAppOutput(BaseModel):
    """Output of the deploy_data_app tool."""

    action: Literal['deployed', 'stopped'] = Field(description='The action performed.')
    links: list[Link] = Field(description='Deployment links for the data app.')


class GetDataAppsOutput(BaseModel):
    """Output of the get_data_apps tool. Serves for both DataAppSummary and DataApp outputs."""

    data_apps: Sequence[DataAppSummary | DataApp] = Field(description='The data apps in the project.')


@tool_errors()
async def sync_data_app(
    ctx: Context,
    name: Annotated[str, Field(description='Name of the data app.')],
    description: Annotated[str, Field(description='Description of the data app.')],
    source_code: Annotated[str, Field(description='Complete Python/Streamlit source code for the data app.')],
    packages: Annotated[
        list[str], Field(description='Python packages used in the source code necessary to be installed.')
    ],
    authorization_required: Annotated[
        bool, Field(description='Whether the data app is authorized using simple password or not.')
    ] = False,
    configuration_id: Annotated[
        str, Field(description='The ID of existing data app configuration when updating, otherwise None.')
    ] = None,  # type: ignore
) -> Annotated[SyncDataAppOutput, Field(description='The created or updated data app.')]:
    """Creates or updates a Streamlit data app in Keboola workspace integration.

    Considerations:
    - The `source_code` parameter must be a complete and runnable Streamlit app.
    It must include a placeholder `{QUERY_DATA_FUNCTION}` where the `query_data` function will be injected.
    This function accepts a SQL query string and returns a pandas DataFrame with the results from the workspace.
    - Always use `query_data(sql_query)` to retrieve data from the workspace.
    - Write SQL queries so they are compatible with the current workspace backend, you can ensure this by using the
    `query_data` tool to inspect the data in the workspace before creating the data app.
    - If you're updating an existing data app, provide the `config_id` parameter. In this case, all existing parameters
    must either be preserved or explicitly updated.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    project_id = await client.storage_client.project_id()
    source_code = _inject_query_to_source_code(source_code)
    secrets = _get_secrets(client, str(await workspace_manager.get_workspace_id()))
    if configuration_id:
        # Update existing data app
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        existing_config = {
            'parameters': data_app.parameters,
            'authorization': data_app.authorization,
            'storage': data_app.storage,
        }
        updated_config = _update_existing_data_app_config(
            existing_config, name, source_code, packages, authorization_required, secrets
        )
        updated_config = await client.encryption_client.encrypt(
            updated_config, component_id=DATA_APP_COMPONENT_ID, project_id=project_id
        )
        _ = await client.storage_client.configuration_update(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration=updated_config,
            change_description='Updated data app',
            updated_name=name,
            updated_description=description,
        )
        await set_cfg_update_metadata(
            client=client,
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration_version=int(data_app.config_version),
        )
        data_app_science = await client.data_science_client.get_data_app(data_app.data_app_id)
        links = links_manager.get_data_app_links(
            configuration_id=data_app_science.config_id,
            configuration_name=name,
            deployment_link=data_app_science.url,
        )
        return SyncDataAppOutput(
            action='updated', data_app=DataAppSummary.from_api_response(data_app_science), links=links
        )
    else:
        # Create new data app
        config = _build_data_app_config(name, source_code, packages, authorization_required, secrets)
        config = await client.encryption_client.encrypt(
            config, component_id=DATA_APP_COMPONENT_ID, project_id=project_id
        )
        data_app_science = await client.data_science_client.create_data_app(
            name, description, config['parameters'], config['authorization']
        )
        await set_cfg_creation_metadata(
            client=client,
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=data_app_science.config_id,
        )
        links = links_manager.get_data_app_links(
            configuration_id=data_app_science.config_id,
            configuration_name=name,
            deployment_link=data_app_science.url,
        )
        return SyncDataAppOutput(
            action='created', data_app=DataAppSummary.from_api_response(data_app_science), links=links
        )


@tool_errors()
async def get_data_apps(
    ctx: Context,
    configuration_ids: Annotated[Sequence[str], Field(description='The IDs of the data app configurations.')] = tuple(),
    limit: Annotated[int, Field(description='The limit of the data apps to fetch.')] = 100,
    offset: Annotated[int, Field(description='The offset of the data apps to fetch.')] = 0,
) -> Annotated[GetDataAppsOutput, Field(description='The data apps.')]:
    """Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by
    providing its configuration IDs.

    Considerations:
    - If configuration_ids are provided, the tool will return details of the data apps by their configuration IDs.
    - If no configuration_ids are provided, the tool will list all data apps in the project given the limit and offset.
    - Data App details contain configurations, deployment info along with logs and links to the data app dashboard.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if configuration_ids:
        # Get details of the data apps by their configuration IDs
        data_app_details: list[DataApp] = []
        for configuration_id in configuration_ids:
            data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
            links = links_manager.get_data_app_links(
                configuration_id=data_app.configuration_id,
                configuration_name=data_app.name,
                deployment_link=data_app.deployment_url,
                include_password_link=data_app.is_authorized,
            )
            data_app.add_links(links)
            data_app_details.append(data_app)
        return GetDataAppsOutput(data_apps=data_app_details)
    else:
        # List all data apps in the project
        data_apps: list[DataAppResponse] = await client.data_science_client.list_data_apps(limit=limit, offset=offset)
        links = [links_manager.get_data_app_dashboard_link()]
        return GetDataAppsOutput(
            data_apps=[DataAppSummary.from_api_response(data_app) for data_app in data_apps],
        )


@tool_errors()
async def manage_data_app(
    ctx: Context,
    action: Annotated[Literal['deploy', 'stop'], Field(description='The action to perform.')],
    configuration_id: Annotated[str, Field(description='The ID of the data app configuration.')],
) -> Annotated[ManageDataAppOutput, Field(description='The created or updated data app.')]:
    """Deploys a data app or stops running data app in the Keboola workspace integration given the action and config
    id.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    if action == 'deploy':
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        _ = await client.data_science_client.deploy_data_app(data_app.data_app_id, str(data_app.config_version))
        data_app = await _fetch_data_app(client, configuration_id=None, data_app_id=data_app.data_app_id)
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=data_app.deployment_url,
            include_password_link=data_app.is_authorized,
        )
        return ManageDataAppOutput(action='deployed', links=links)
    elif action == 'stop':
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        _ = await client.data_science_client.suspend_data_app(data_app.data_app_id)
        data_app = await _fetch_data_app(client, configuration_id=None, data_app_id=data_app.data_app_id)
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=None,
            include_password_link=data_app.is_authorized,
        )
        return ManageDataAppOutput(action='stopped', links=links)
    else:
        raise ValueError(f'Invalid action: {action}')


_DEFAULT_STREAMLIT_THEME = (
    '[theme]\nfont = "sans serif"\ntextColor = "#222529"\nbackgroundColor = "#FFFFFF"\nsecondaryBackgroundColor = '
    '"#E6F2FF"\nprimaryColor = "#1F8FFF"'
)

_DEFAULT_PACKAGES = ['pandas', 'httpx', 'cryptography']


def _build_data_app_config(
    name: str,
    source_code: str,
    packages: list[str],
    authorize_with_password: bool,
    secrets: dict[str, Any],
) -> dict[str, Any]:
    packages = list(set(packages + _DEFAULT_PACKAGES))
    slug = _get_data_app_slug(name)
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
        'script': [source_code],
        'packages': packages,
    }

    authorization = _get_authorization(authorize_with_password)
    return {'parameters': parameters, 'authorization': authorization}


def _update_existing_data_app_config(
    existing_config: dict[str, Any],
    name: str,
    source_code: str,
    packages: list[str],
    authorize_with_password: bool,
    secrets: dict[str, Any],
) -> dict[str, Any]:
    new_config = existing_config.copy()
    new_config['parameters']['dataApp']['slug'] = (
        _get_data_app_slug(name) or existing_config['parameters']['dataApp']['slug']
    )
    new_config['parameters']['script'] = [source_code] or existing_config['parameters']['script']
    new_packages = packages or existing_config['parameters'].get('packages', [])
    new_config['parameters']['packages'] = list(set(new_packages + _DEFAULT_PACKAGES))
    new_config['parameters']['dataApp']['secrets'] = {**secrets, **existing_config['parameters']['dataApp']['secrets']}
    new_config['authorization'] = _get_authorization(authorize_with_password)
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
        logs = await _fetch_logs(client, data_app_id)
        return DataApp.from_api_responses(data_app_science, api_config, logs)
    elif configuration_id:
        raw_configuration = await client.storage_client.configuration_detail(
            component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
        )
        api_config = ConfigurationAPIResponse.model_validate(
            raw_configuration | {'component_id': DATA_APP_COMPONENT_ID}
        )
        data_app_id = cast(str, api_config.configuration['parameters']['id'])
        data_app_science = await client.data_science_client.get_data_app(data_app_id)
        logs = await _fetch_logs(client, data_app_id)
        return DataApp.from_api_responses(data_app_science, api_config, logs)
    else:
        raise ValueError('Either data_app_id or configuration_id must be provided.')


async def _fetch_logs(client: KeboolaClient, data_app_id: str) -> list[str]:
    """Fetches the logs of a data app if it is running otherwise returns empty list."""
    try:
        str_logs = await client.data_science_client.tail_app_logs(data_app_id)
        logs = str_logs.split('\n')
        return logs
    except httpx.HTTPStatusError as e:
        LOG.warning(f'Failed to fetch logs for data app ({data_app_id}), returning empty list: {e}')
        return []


def _get_authorization(auth_required: bool) -> dict[str, Any]:
    if auth_required:
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

def _is_authorized(authorization: dict[str, Any]) -> bool:
    try:
        return any(auth_rule['auth_required'] for auth_rule in authorization['app_proxy']['auth_rules'])
    except Exception:
        return False


_QUERY_DATA_FUNCTION_CODE = """
#### INJECTED_CODE ####
#### QUERY DATA FUNCTION ####
import base64
import os
import httpx
import pandas as pd
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def query_data(query: str) -> pd.DataFrame:
    bid = os.environ.get('BRANCH_ID')
    wid = os.environ.get('WORKSPACE_ID')
    random_seed = base64.urlsafe_b64decode(os.environ.get('SAPI_RANDOM_SEED').encode())
    encrypted_token = base64.urlsafe_b64decode(os.environ.get('STORAGE_API_TOKEN').encode())
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,  # Fernet requires 32-byte keys
        salt=wid.encode("utf-8"),
        iterations=390000,  # recommended by cryptography.io
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(random_seed))
    decoded_token = Fernet(key).decrypt(encrypted_token).decode()
    base_url = os.environ.get('STORAGE_API_URL')
    with httpx.Client() as client:
        response = client.post(
            f'{base_url}/v2/storage/branch/{bid}/workspaces/{wid}/query',
            json={'query': query},
            headers={'X-StorageAPI-Token': decoded_token},
        )
        response.raise_for_status()
        return pd.DataFrame(response.json()['data']['rows'])

#### END_OF_GENERATED_CODE ####
"""


def _inject_query_to_source_code(source_code: str) -> str:
    """
    Injects the query_data function into the source code if it is not already present.
    """
    if _QUERY_DATA_FUNCTION_CODE in source_code:
        return source_code
    if '### INJECTED_CODE ###' in source_code and '### END_OF_INJECTED_CODE ###' in source_code:
        # get the first and the last part before and after generated code and inject the query_data function
        imports = source_code.split('### INJECTED_CODE ###')[0]
        source_code = source_code.split('### INJECTED_CODE ###')[1].split('### END_OF_INJECTED_CODE ###')[1]
        return imports + '\n\n' + _QUERY_DATA_FUNCTION_CODE + '\n\n' + source_code
    elif '{QUERY_DATA_FUNCTION}' in source_code:
        return source_code.replace('{QUERY_DATA_FUNCTION}', _QUERY_DATA_FUNCTION_CODE)
    else:
        return _QUERY_DATA_FUNCTION_CODE + '\n\n' + source_code


def _get_secrets(client: KeboolaClient, workspace_id: str) -> dict[str, Any]:
    """
    Generates secrets for the data app for querying the tables in the given wokrspace using the query_data endpoint.

    - First, the storage token is encrypted using `cryptography.fernet`, combining a random seed and the workspace ID.
    This encrypted token is sent to the data app as part of the secrets.
    - Next, all values with keys starting with a hashtag (`#`) are further encrypted by sending them to an encryption
    service endpoint. These values are automatically decrypted by the service when the data app starts.
    - Finally, the storage token is decrypted inside the data app using Fernet and the associated metadata.

    :param client: The Keboola client
    :param workspace_id: The ID of the workspace
    :return: The secrets
    """
    random_seed = os.urandom(32)
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,  # Fernet requires 32-byte keys
        salt=workspace_id.encode('utf-8'),
        iterations=390000,
        backend=default_backend(),
    )
    key = base64.urlsafe_b64encode(kdf.derive(random_seed))
    encrypted_token = Fernet(key).encrypt(client.token.encode())
    return {
        'WORKSPACE_ID': workspace_id,
        'STORAGE_API_URL': client.storage_client.base_api_url,
        'BRANCH_ID': client.storage_client.branch_id,
        '#STORAGE_API_TOKEN': base64.urlsafe_b64encode(encrypted_token).decode('utf-8'),
        '#SAPI_RANDOM_SEED': base64.urlsafe_b64encode(random_seed).decode('utf-8'),
    }
