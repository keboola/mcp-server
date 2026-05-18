import copy
import importlib.resources as resources
import logging
import re
from typing import Annotated, Any, Literal, Mapping, Optional, Sequence, Union, cast

import httpx
from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient, get_metadata_property
from keboola_mcp_server.clients.data_science import CodeDataAppConfig, DataAppConfig, DataAppResponse
from keboola_mcp_server.clients.storage import ConfigurationAPIResponse
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import process_concurrently, toon_serializer_compact
from keboola_mcp_server.tools.components.utils import (
    apply_folder_metadata,
    folder_field_description,
    set_cfg_creation_metadata,
    set_cfg_update_metadata,
)
from keboola_mcp_server.tools.constants import CONFIG_DIFF_PREVIEW_TAG
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

DATA_APP_TOOLS_TAG = 'data-apps'


def add_data_app_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(
        FunctionTool.from_function(
            modify_streamlit_data_app,
            tags={DATA_APP_TOOLS_TAG, CONFIG_DIFF_PREVIEW_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            modify_python_js_data_app,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            register_python_js_data_app_ssh_key,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_data_apps,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
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
Type = Literal['streamlit', 'python-js']
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

# TODO: Remove this hardcoded image version once the platform sets it as the default for python-js apps.
_HARDCODED_PYTHON_JS_IMAGE_VERSION = 'dev-PAT-1772.4'

INJECTED_BLOCK_RE = re.compile(
    r'(?P<before>.*?)#\s###\sINJECTED_CODE\s####.*?#\s###\sEND_OF_INJECTED_CODE\s####(?P<after>.*)',
    re.DOTALL,
)

# Type of the authentication used in the data app
AuthenticationType = Literal['no-auth', 'basic-auth', 'default']

SECRET_WORKSPACE_ID = 'WORKSPACE_ID'
SECRET_BRANCH_ID = 'BRANCH_ID'
SECRET_KBC_TOKEN = 'KBC_TOKEN'
SECRET_KBC_URL = 'KBC_URL'


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
    auto_suspend_after_seconds: Optional[int] = Field(
        description='The number of seconds after which the running data app is automatically suspended.',
        default=None,
    )
    repo_url: Optional[str] = Field(
        default=None,
        description='SSH clone URL of the managed git repo. Only set for python-js data apps.',
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
    auto_suspend_after_seconds: Optional[int] = Field(
        description='The number of seconds after which the running data app is automatically suspended.',
        default=None,
    )
    repo_url: Optional[str] = Field(
        default=None,
        description='SSH clone URL of the managed git repo. Only set for python-js data apps.',
    )
    configuration: dict[str, Any] = Field(
        description='The nested configuration object containing parameters, storage and authorization'
    )
    folder: str = Field(default='', description='The UI folder this data app is organized into')
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
            folder=get_metadata_property(api_configuration.metadata, MetadataField.CONFIGURATION_FOLDER_NAME) or '',
            configuration=api_configuration.configuration,
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
    change_summary: Optional[str] = Field(default=None, description='Additional notes or hints about the operation.')
    data_app: DataAppSummary = Field(description='The data app.')
    links: list[Link] = Field(description='Navigation links for the web interface.')


class ModifiedPythonJsDataAppOutput(BaseModel):
    """Output for `modify_python_js_data_app`. Includes git repo URL on create."""

    response: str = Field(description='The response of the action performed with potential additional information.')
    change_summary: Optional[str] = Field(default=None, description='Additional notes or hints about the operation.')
    data_app: DataAppSummary = Field(description='The data app.')
    repo_url: Optional[str] = Field(
        default=None,
        description=(
            'SSH clone URL of the managed git repo. Returned on create so the caller can clone the repo and push '
            'initial source code. On update, populated when the repo info can be fetched.'
        ),
    )
    links: list[Link] = Field(description='Navigation links for the web interface.')


class RegisteredSshKeyOutput(BaseModel):
    """Output for `register_python_js_data_app_ssh_key`."""

    response: str = Field(description='The response of the action performed.')
    configuration_id: str = Field(description='The Storage configuration ID of the python-js data app.')
    data_app_id: str = Field(description='The ID of the data app the SSH key was registered on.')
    ssh_key_id: str = Field(description='The ID of the registered SSH key.')
    public_key: str = Field(description='The registered public key (echoed back).')
    permissions: str = Field(description='The permissions of the key, e.g. "readWrite".')
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
async def modify_streamlit_data_app(
    ctx: Context,
    name: Annotated[str, Field(description='Name of the data app (max ~50 chars to fit DNS label limit).')],
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
    folder: Annotated[
        Optional[str],
        Field(description=folder_field_description('data app', 'data apps')),
    ] = None,
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
    - After creating or updating a data app with this tool, ALWAYS call
    `deploy_data_app(action="deploy", configuration_id=...)` to start a new app or restart an existing app so
    changes take effect. Without this step, a newly created app will not start, and an existing app will keep
    running the previous deployment without the latest changes.
    - New apps use the HTTP basic authentication by default for security unless explicitly specified otherwise; when
    updating, set `authentication_type` to `default` to keep the existing authentication type configuration
    (including OIDC setups) unless explicitly specified otherwise.

    SQL & DATA TYPE RULES:
    - Use delimited identifiers for the current SQL dialect for all column names and aliases in SQL.
      Match the exact identifier case used in SQL when referencing columns in Python code.
    - `query_data` RETURNS ALL COLUMNS AS STRINGS regardless of SQL CAST. Always convert types in Python after loading:
    `df["col"] = pd.to_numeric(df["col"], errors="coerce").fillna(0)` and
    `df["date"] = pd.to_datetime(df["date"], errors="coerce")`.
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
        storage_token=client.token,
        storage_api_url=client.storage_api_url,
    )

    if configuration_id:
        # Update existing data app
        data_app, updated_config, _ = await modify_streamlit_data_app_internal(
            client=client,
            workspace_manager=workspace_manager,
            name=name,
            description=description,
            source_code=source_code,
            packages=packages,
            authentication_type=authentication_type,
            configuration_id=configuration_id,
            change_description=change_description,
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
        folder_hint = await apply_folder_metadata(
            client, DATA_APP_COMPONENT_ID, configuration_id, folder, 'data apps', 'modify_streamlit_data_app'
        )
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
        )
        response = (
            'updated (redeploy required to apply changes in the running app)'
            if data_app.state in ('running', 'starting')
            else 'updated'
        )
        return ModifiedDataAppOutput(
            response=response,
            change_summary=folder_hint,
            data_app=DataAppSummary.model_validate(data_app.model_dump()),
            links=links,
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
        folder_hint = await apply_folder_metadata(
            client,
            DATA_APP_COMPONENT_ID,
            data_app_resp.config_id,
            folder,
            'data apps',
            'modify_streamlit_data_app',
            is_new=True,
        )
        links = links_manager.get_data_app_links(
            configuration_id=data_app_resp.config_id,
            configuration_name=name,
            deployment_link=data_app_resp.url,
            uses_basic_authentication=_uses_basic_authentication(validated_config.authorization),
        )
        return ModifiedDataAppOutput(
            response='created',
            change_summary=folder_hint,
            data_app=DataAppSummary.from_api_response(data_app_resp),
            links=links,
        )


async def modify_streamlit_data_app_internal(
    *,
    client: KeboolaClient,
    workspace_manager: WorkspaceManager,
    name: str,
    description: str = '',
    source_code: str,
    packages: list[str],
    authentication_type: AuthenticationType,
    configuration_id: str,
    change_description: str = '',
    folder: Optional[str] = None,
) -> tuple[DataApp, JsonDict, dict | None]:
    secrets = _get_secrets(
        workspace_id=str(await workspace_manager.get_workspace_id()),
        branch_id=str(await workspace_manager.get_branch_id()),
        storage_token=client.token,
        storage_api_url=client.storage_api_url,
    )
    data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
    existing_config = data_app.configuration
    updated_config = _update_existing_data_app_config(
        existing_config,
        name,
        source_code,
        packages,
        authentication_type,
        secrets,
        await workspace_manager.get_sql_dialect(),
    )
    updated_config = cast(
        JsonDict,
        await client.encryption_client.encrypt(
            updated_config, component_id=DATA_APP_COMPONENT_ID, project_id=await client.storage_client.project_id()
        ),
    )

    folder_preview: dict | None = None
    if folder is not None:
        normalized_folder = folder.strip()
        try:
            current_metadata = await client.storage_client.configuration_metadata_get(
                component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
            )
            current_folder = next(
                (
                    m.get('value', '')
                    for m in current_metadata
                    if m.get('key') == MetadataField.CONFIGURATION_FOLDER_NAME
                ),
                '',
            )
            if normalized_folder != current_folder:
                folder_preview = {'original_folder': current_folder, 'updated_folder': normalized_folder}
        except Exception as e:
            LOG.warning(
                'Failed to fetch configuration metadata for folder preview '
                '(component_id=%s, configuration_id=%s): %s. Proceeding without folder preview.',
                DATA_APP_COMPONENT_ID,
                configuration_id,
                e,
            )

    return data_app, updated_config, folder_preview


@tool_errors()
async def modify_python_js_data_app(
    ctx: Context,
    name: Annotated[str, Field(description='Name of the data app (max ~50 chars to fit DNS label limit).')],
    description: Annotated[str, Field(description='Description of the data app.')],
    configuration_id: Annotated[
        str, Field(description='The ID of existing data app configuration when updating, otherwise empty string.')
    ] = '',
    change_description: Annotated[
        str,
        Field(description='The description of the change when updating (e.g. "Bump image"), otherwise empty string.'),
    ] = '',
    slug: Annotated[
        Optional[str],
        Field(
            description=(
                'URL-safe slug for the data app (used as a subdomain). Required when creating; immutable after.'
            ),
        ),
    ] = None,
    existing_repo_url: Annotated[
        Optional[str],
        Field(
            description=(
                'When set on create, the new data app is bound to this existing managed git repo (the URL '
                'returned by `get_data_apps` on a sibling app) instead of provisioning a fresh repo. Use this '
                "to create a prod app that shares its sibling dev app's repo (promote-to-prod), or to create "
                "a dev twin that shares an existing prod app's repo (edit flow). Ignored when updating."
            ),
        ),
    ] = None,
    authentication_type: Annotated[
        AuthenticationType,
        Field(
            description=(
                'Authentication type. "no-auth" removes authentication completely, "basic-auth" secures the '
                'data app via HTTP basic authentication, and "default" means: on create, apply basic auth '
                '(safe default for new apps); on update, keep the existing authentication configuration '
                '(including OIDC setups configured outside the MCP).'
            ),
        ),
    ] = 'default',
    auto_suspend_after_seconds: Annotated[
        int,
        Field(
            description='Number of seconds after which the running data app is automatically suspended.',
        ),
    ] = 900,
    folder: Annotated[
        Optional[str],
        Field(description=folder_field_description('data app', 'data apps')),
    ] = None,
) -> ModifiedPythonJsDataAppOutput:
    """Creates or updates a python-js data app backed by a managed git repository.

    Two-app project model: every python-js project has a persistent **prod app** that users actually run,
    and one or more **dev twins** that share the same managed git repo for LLM iteration. Dev twins appear
    in the Keboola UI under their parent prod app in a "Drafts" section; the user discards them manually
    via a "Discard" button when no longer needed. The MCP server does not delete dev twins.

    This tool only creates/updates the app configuration. SSH-key registration is a separate step
    handled by `register_python_js_data_app_ssh_key` — call it after every successful create before
    attempting any `git` operation against the returned `repo_url`.

    ## Create flow (new project bootstrap)
    No `configuration_id`, no `existing_repo_url`. `slug` is required.
    Steps:
    1. LLM generates an SSH keypair locally:
       `ssh-keygen -t ed25519 -N '' -f ~/.ssh/keboola-app-<slug>`.
    2a. Call this tool with `slug`. Returns `(configuration_id=C1, repo_url=R)` for a temporary
        dev iteration app and its fresh managed git repo.
    2b. Call `register_python_js_data_app_ssh_key(configuration_id=C1, public_key=K)` to enable
        git access on the new app.
    3. Clone the repo with the matching private key
       (`GIT_SSH_COMMAND="ssh -i ~/.ssh/keboola-app-<slug>" git clone <repo_url>`),
       write the initial source code, commit, push to `main`.
    4. `deploy_data_app(action='deploy', configuration_id=C1, mode='dev')` → preview URL. Iterate with
       the user against this dev app.
    5a. After the user approves, **call this tool again with `existing_repo_url=R`** plus the
        user-facing `slug` (typically without the iteration suffix) — this creates the **prod app**
        bound to the same repo. Returns `(configuration_id=C2, repo_url=R)`.
    5b. Call `register_python_js_data_app_ssh_key(configuration_id=C2, public_key=K)` — SSH keys are
        per-app, so the new prod app needs its own registration; the same public key works for both.
    6. `deploy_data_app(action='deploy', configuration_id=C2)` (no `mode='dev'`) → prod URL. The
       temporary dev iteration app from step 2a stays listed under the new prod app in the UI's
       "Drafts" section until the user discards it.

    ## Edit flow (modifying an existing prod app)
    Steps:
    1. `get_data_apps(configuration_ids=[<prod_id>])` to retrieve the prod app's `repo_url`.
    2. Generate a fresh SSH keypair locally (per dev twin — keys are per-app).
    3a. Call this tool with a temporary `slug` (e.g. `<prod-slug>-dev-<rand>` to stay unique) and
        **`existing_repo_url=<prod repo_url>`** — creates the dev twin sharing the prod app's repo.
        Returns `(configuration_id=C3, repo_url=R)`.
    3b. Call `register_python_js_data_app_ssh_key(configuration_id=C3, public_key=K2)` before cloning.
    4. Clone the repo, `git checkout -b feature-x`, write changes, commit, push the branch.
    5. `deploy_data_app(action='deploy', configuration_id=C3, mode='dev', branch='feature-x')` → preview
       URL serving that branch. Iterate with the user.
    6. After approval, locally `git checkout main && git merge feature-x && git push`.
    7. `deploy_data_app(action='deploy', configuration_id=<prod_id>)` — no `mode`, no `branch`. The prod
       app picks up the merged `main`. The dev twin stays listed under the prod app in the UI's "Drafts"
       section until the user discards it.

    ## Update flow (modifying an existing app's deployment metadata)
    When `configuration_id` is set: updates the Storage configuration (auto-suspend, name, description,
    `authentication_type`). `slug` and `existing_repo_url` are rejected here — slug is immutable and the
    repo binding is fixed at creation. Use `authentication_type='default'` to keep the existing auth
    setup (including OIDC configured outside the MCP); pass `'no-auth'` or `'basic-auth'` to overwrite.
    After updating, ALWAYS call `deploy_data_app(action='deploy', ...)` to restart the app so the changes
    take effect.

    ## Authentication
    New apps default to HTTP basic authentication for safety. Pass `authentication_type='no-auth'`
    explicitly to expose the app publicly. OIDC and other advanced auth setups are managed outside the
    MCP — when updating such an app, leave `authentication_type='default'` to preserve them.

    ## Slug constraint
    Must be DNS-label-safe (lowercase letters, digits, hyphens, ≤63 chars). For dev twins in the edit flow,
    append a short suffix (e.g. `-dev-abc123`) to keep slugs unique across the prod app and its twins.

    ## Source code
    Source code lives in the managed git repo, NOT in this tool's input. This tool only manages deployment
    metadata. Source code changes are pushed via `git push` to the repo URL.
    """
    if configuration_id:
        if slug:
            raise ValueError('slug cannot be changed after the data app is created.')
        if existing_repo_url:
            raise ValueError('existing_repo_url is only valid on create.')
    else:
        if not slug:
            raise ValueError('slug is required when creating a python-js data app.')

    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    secrets = _get_python_js_secrets(
        branch_id=str(await workspace_manager.get_branch_id()),
        storage_token=client.token,
        storage_api_url=client.storage_api_url,
    )

    if configuration_id:
        # Update existing python-js data app
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        updated_config = _update_existing_code_data_app_config(
            existing_config=data_app.configuration,
            image_version=_HARDCODED_PYTHON_JS_IMAGE_VERSION,
            auto_suspend_after_seconds=auto_suspend_after_seconds,
            authentication_type=authentication_type,
            secrets=secrets,
        )
        await client.storage_client.configuration_update(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration=updated_config,
            change_description=change_description or 'Update python-js data app',
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
        folder_hint = await apply_folder_metadata(
            client, DATA_APP_COMPONENT_ID, configuration_id, folder, 'data apps', 'modify_python_js_data_app'
        )
        repo_url: Optional[str] = None
        try:
            repo_resp = await client.data_science_client.get_app_git_repo(data_app.data_app_id)
            repo_url = repo_resp.url
        except Exception as exc:
            LOG.warning(f'Could not fetch git repo URL for app {data_app.data_app_id}: {exc}')
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=name or data_app.name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
        )
        response = (
            'updated (redeploy required to apply changes in the running app)'
            if data_app.state in ('running', 'starting')
            else 'updated'
        )
        data_app_summary = DataAppSummary.model_validate(data_app.model_dump())
        data_app_summary.repo_url = repo_url
        return ModifiedPythonJsDataAppOutput(
            response=response,
            change_summary=folder_hint,
            data_app=data_app_summary,
            repo_url=repo_url,
            links=links,
        )
    else:
        # Create new python-js data app
        # Narrowed by the validation block at the top of this function.
        assert slug is not None
        # On create, treat 'default' as 'basic-auth' (safe-by-default) to match modify_streamlit_data_app.
        uses_basic_auth = authentication_type in ('basic-auth', 'default')
        authorization_model = DataAppConfig.Authorization.model_validate(_get_authorization(uses_basic_auth))
        config = CodeDataAppConfig(
            parameters=CodeDataAppConfig.Parameters(
                auto_suspend_after_seconds=auto_suspend_after_seconds,
                data_app=CodeDataAppConfig.Parameters.DataApp(slug=slug, secrets=secrets),
            ),
            runtime=CodeDataAppConfig.Runtime(
                image=CodeDataAppConfig.Runtime.Image(version=_HARDCODED_PYTHON_JS_IMAGE_VERSION),
                workspace=CodeDataAppConfig.Runtime.Workspace(enabled=True),
            ),
            authorization=authorization_model,
        )
        data_app_resp = await client.data_science_client.create_data_app(
            name=name,
            description=description,
            configuration=config,
            app_type='python-js',
            use_managed_git_repo=True,
            existing_repo_url=existing_repo_url,
        )
        if existing_repo_url is not None:
            repo_url = existing_repo_url
        else:
            repo_resp = await client.data_science_client.get_app_git_repo(data_app_resp.id)
            repo_url = repo_resp.url
        await set_cfg_creation_metadata(
            client=client,
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=data_app_resp.config_id,
        )
        folder_hint = await apply_folder_metadata(
            client,
            DATA_APP_COMPONENT_ID,
            data_app_resp.config_id,
            folder,
            'data apps',
            'modify_python_js_data_app',
            is_new=True,
        )
        links = links_manager.get_data_app_links(
            configuration_id=data_app_resp.config_id,
            configuration_name=name,
            deployment_link=data_app_resp.url,
            uses_basic_authentication=uses_basic_auth,
        )
        data_app_summary = DataAppSummary.from_api_response(data_app_resp)
        data_app_summary.repo_url = repo_url
        return ModifiedPythonJsDataAppOutput(
            response='created',
            change_summary=folder_hint,
            data_app=data_app_summary,
            repo_url=repo_url,
            links=links,
        )


@tool_errors()
async def register_python_js_data_app_ssh_key(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='Storage configuration ID of the python-js data app.')],
    public_key: Annotated[
        str,
        Field(description='SSH public key contents (e.g. the contents of an `id_ed25519.pub` file).'),
    ],
) -> RegisteredSshKeyOutput:
    """Registers an SSH public key on a python-js data app so the holder of the matching private key
    can clone, pull, and push to the app's managed git repo.

    The data-science API accepts multiple keys per app, so calling this again with a fresh key does
    **not** invalidate any keys already held by other clients.

    ## When to call

    1. **Right after `modify_python_js_data_app` create** — the new app has a managed repo but no
       authorized keys yet. The LLM generates a keypair locally
       (`ssh-keygen -t ed25519 -N '' -f ~/.ssh/keboola-app-<slug>`) and calls this tool with the
       new app's `configuration_id` to enable git access.

    2. **Recovery when the private key is gone** (e.g., a fresh Kai sandbox continuing an old draft
       — the previous sandbox's filesystem was wiped, taking the private key with it). If a `git
       clone`/`pull`/`push` against a python-js app the LLM did not create in the current session
       fails with `Permission denied (publickey)`, generate a fresh keypair locally and call this
       tool with the same `configuration_id` and the new public key. Existing keys remain valid,
       so other clients are not disrupted.

    ## Constraints
    - Only python-js data apps have a managed git repo. Streamlit apps reject the call with a clear
      error.
    - Permissions are always `readWrite` — the LLM virtually always needs push access. The
      data-science API supports read-only keys, but the tool does not expose that knob; revisit
      once a real use case appears.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
    if data_app.type != 'python-js':
        raise ValueError(
            f'register_python_js_data_app_ssh_key only supports python-js data apps, but configuration '
            f'"{configuration_id}" is type "{data_app.type}".'
        )

    ssh_key_resp = await client.data_science_client.register_app_ssh_key(
        data_app_id=data_app.data_app_id,
        public_key=public_key,
    )
    links = links_manager.get_data_app_links(
        configuration_id=data_app.configuration_id,
        configuration_name=data_app.name,
        deployment_link=data_app.deployment_url,
        uses_basic_authentication=False,
    )
    return RegisteredSshKeyOutput(
        response='registered',
        configuration_id=data_app.configuration_id,
        data_app_id=data_app.data_app_id,
        ssh_key_id=ssh_key_resp.id,
        # Echo the caller-supplied key — the DSAPI registration endpoint does not return it.
        public_key=public_key,
        permissions=ssh_key_resp.permissions,
        links=links,
    )


def _update_existing_code_data_app_config(
    existing_config: Mapping[str, Any],
    image_version: Optional[str],
    auto_suspend_after_seconds: int,
    authentication_type: AuthenticationType = 'default',
    secrets: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Apply requested updates to the existing python-js data app storage configuration.

    Slug is intentionally not updated here (immutable post-create).
    `authentication_type='default'` preserves the existing `authorization` block (including OIDC
    setups configured outside the MCP); 'no-auth' / 'basic-auth' overwrite it.
    `secrets` are merged into the existing `parameters.dataApp.secrets` map without overwriting
    keys that are already present (mirrors the Streamlit update path).
    """
    new_config = cast(dict[str, Any], copy.deepcopy(existing_config))
    new_config.setdefault('parameters', {})
    new_config['parameters']['autoSuspendAfterSeconds'] = auto_suspend_after_seconds
    if image_version:
        runtime = new_config.setdefault('runtime', {})
        image = runtime.setdefault('image', {})
        image['version'] = image_version
    if authentication_type != 'default':
        new_config['authorization'] = _get_authorization(authentication_type == 'basic-auth')
    if secrets:
        data_app = new_config['parameters'].setdefault('dataApp', {})
        updated_secrets = dict(data_app.get('secrets') or {})
        for key, value in secrets.items():
            if key not in updated_secrets:
                updated_secrets[key] = value
        data_app['secrets'] = updated_secrets
    return new_config


@tool_errors()
async def get_data_apps(
    ctx: Context,
    configuration_ids: Annotated[Sequence[str], Field(description='The IDs of the data app configurations.')] = tuple(),
    limit: Annotated[int, Field(description='The limit of the data apps to fetch.')] = 100,
    offset: Annotated[int, Field(description='The offset of the data apps to fetch.')] = 0,
) -> GetDataAppsOutput:
    """Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by
    providing their configuration IDs.

    WHEN NOT TO USE:
    - Do NOT list all data apps just to find one by name. Use `search` with
      item_types=["data-app"] instead.
    - Only list all data apps when you need a complete inventory.

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
        async def fetch_data_app_detail(configuration_id: str) -> DataApp | str:
            return await _fetch_data_app_details_task(client, links_manager, configuration_id)

        data_app_details = await process_concurrently(configuration_ids, fetch_data_app_detail, max_concurrency=10)
        found_data_apps: list[DataApp] = [dap for dap in data_app_details if isinstance(dap, DataApp)]
        not_found_ids: list[str] = [dap for dap in data_app_details if isinstance(dap, str)]
        if not_found_ids:
            LOG.error(f'Could not find Data Apps Configurations for IDs: {not_found_ids}')
        return GetDataAppsOutput(data_apps=found_data_apps)
    else:
        # List all data apps in the project
        data_apps: list[DataAppResponse] = await client.data_science_client.list_data_apps(limit=limit, offset=offset)
        # Filter to only include keboola.data-apps component
        data_apps = [app for app in data_apps if app.component_id == DATA_APP_COMPONENT_ID]
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
    mode: Annotated[
        Optional[Literal['dev', 'production']],
        Field(
            description=(
                'Deployment mode. Set to "dev" to enable the in-platform preview for python-js data apps. '
                'Leave None (default) for Streamlit apps and for production deploys.'
            ),
        ),
    ] = None,
    branch: Annotated[
        Optional[str],
        Field(
            description=(
                'Git branch to deploy from. Only meaningful when `mode="dev"` for python-js apps backed by a '
                'managed repo — the dev twin will deploy from this branch instead of `main`, enabling '
                'branch-based preview during the edit flow. Leave None for prod deploys and for Streamlit apps.'
            ),
        ),
    ] = None,
) -> DeploymentDataAppOutput:
    """Deploys/redeploys a data app or stops a running data app in the Keboola environment asynchronously, given the
    action and the configuration ID.

    ## Mode and branch (python-js apps)
    - `mode='dev'` enables the in-platform preview for a python-js dev twin. Pair with `branch` to deploy a
      specific git branch (edit flow); without `branch`, the dev twin deploys `main`.
    - For prod redeploys (including after merging a feature branch into `main` during the edit flow), use
      no `mode` and no `branch` — the prod app picks up the current `main`.
    - python-js apps do NOT fetch a Storage `configVersion` for deployment (their source lives in git, not in
      the Storage configuration); this is handled automatically.

    ## Streamlit apps
    `mode` and `branch` are silently ignored for Streamlit apps (which have no managed git repo).

    ## Validation
    `branch` is only meaningful with `mode='dev'`; setting `branch` without `mode='dev'` raises an error.

    ## General considerations
    - Redeploying a data app takes some time, and the app may temporarily report status "stopped" during the
      restart.
    - After deployment, the deployment info includes the app URL and the latest logs to help diagnose in-app
      errors.
    """
    if branch is not None and mode != 'dev':
        raise ValueError('branch is only meaningful with mode="dev"')
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    if action == 'deploy':
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        if data_app.state == 'stopping':
            raise ValueError('Data app is currently "stopping", could not be started at the moment.')
        # python-js apps don't carry a Storage configVersion in the deploy payload; only Streamlit apps do.
        if data_app.type == 'python-js':
            config_version_arg: str | None = None
            branch_arg: str | None = branch
        else:
            config_version = await client.storage_client.configuration_version_latest(
                DATA_APP_COMPONENT_ID, data_app.configuration_id
            )
            config_version_arg = str(config_version)
            branch_arg = None
        _ = await client.data_science_client.deploy_data_app(
            data_app.data_app_id,
            config_version_arg,
            mode=mode,
            branch=branch_arg,
        )
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        data_app = data_app.with_deployment_info(await _fetch_logs(client, data_app.data_app_id))
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
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
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
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
    existing_config: Mapping[str, Any],
    name: str,
    source_code: str,
    packages: list[str],
    authentication_type: AuthenticationType,
    secrets: dict[str, Any],
    sql_dialect: str,
) -> dict[str, Any]:
    new_config = cast(dict[str, Any], copy.deepcopy(existing_config))
    new_config['parameters']['dataApp']['slug'] = (
        _get_data_app_slug(name) or existing_config['parameters']['dataApp']['slug']
    )
    if source_code:
        new_config['parameters']['script'] = [_inject_query_to_source_code(source_code, sql_dialect)]
    new_config['parameters']['packages'] = (
        sorted(list[str](set[str](packages + _DEFAULT_PACKAGES)))
        if packages
        else sorted(list[str](set[str](existing_config['parameters'].get('packages', []) + _DEFAULT_PACKAGES)))
    )

    updated_secrets = existing_config['parameters']['dataApp'].get('secrets', {}).copy()
    # Add new secrets, do not overwrite existing secrets
    for key in secrets:
        if key not in updated_secrets:
            updated_secrets[key] = secrets[key]

    new_config['parameters']['dataApp']['secrets'] = updated_secrets

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
        if data_app_science.component_id != DATA_APP_COMPONENT_ID:
            raise ValueError(
                f'Data app tools only support {DATA_APP_COMPONENT_ID} component, but the data app '
                f'"{data_app_id}" has component_id "{data_app_science.component_id}".'
            )
        raw_data_app_config = await client.storage_client.configuration_detail(
            component_id=DATA_APP_COMPONENT_ID, configuration_id=data_app_science.config_id
        )
        api_config = ConfigurationAPIResponse.model_validate(
            raw_data_app_config | {'component_id': DATA_APP_COMPONENT_ID}
        )
        return await _build_data_app_with_repo(client, data_app_science, api_config)
    elif configuration_id:
        raw_configuration = await client.storage_client.configuration_detail(
            component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
        )
        api_config = ConfigurationAPIResponse.model_validate(
            raw_configuration | {'component_id': DATA_APP_COMPONENT_ID}
        )
        data_app_id = cast(str, api_config.configuration['parameters']['id'])
        data_app_science = await client.data_science_client.get_data_app(data_app_id)
        if data_app_science.component_id != DATA_APP_COMPONENT_ID:
            raise ValueError(
                f'Data app tools only support {DATA_APP_COMPONENT_ID} component, but the data app '
                f'"{data_app_id}" has component_id "{data_app_science.component_id}".'
            )
        return await _build_data_app_with_repo(client, data_app_science, api_config)
    else:
        raise ValueError('Either data_app_id or configuration_id must be provided.')


async def _build_data_app_with_repo(
    client: KeboolaClient,
    data_app_science: DataAppResponse,
    api_config: ConfigurationAPIResponse,
) -> DataApp:
    """Build a `DataApp` and, for python-js apps, attach the managed git repo URL."""
    data_app = DataApp.from_api_responses(data_app_science, api_config)
    if data_app_science.type == 'python-js':
        try:
            repo_resp = await client.data_science_client.get_app_git_repo(data_app_science.id)
            data_app.repo_url = repo_resp.url
        except Exception as exc:
            LOG.warning(f'Could not fetch git repo URL for python-js app {data_app_science.id}: {exc}')
    return data_app


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
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
        )
        logs = await _fetch_logs(client, data_app.data_app_id)
        return data_app.with_links(links).with_deployment_info(logs)
    except Exception:
        LOG.exception(f'Failed to fetch data app by configuration ID: {configuration_id}')
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


# Maximum length for DNS labels per RFC 1035
MAX_DNS_LABEL_LENGTH = 63


class DataAppSlugTooLongError(ValueError):
    """Raised when the generated data app slug exceeds the DNS label length limit."""

    pass


def _get_data_app_slug(name: str) -> str:
    """Generate a URL-safe slug from the data app name.

    The slug is used as part of the data app URL prefix, which is a DNS label.
    DNS labels have a maximum length of 63 characters per RFC 1035.

    :param name: The name of the data app
    :return: A URL-safe slug
    :raises DataAppSlugTooLongError: If the generated slug exceeds 63 characters
    """
    slug = re.sub(r'[^a-z0-9\-]', '', name.strip().lower().replace(' ', '-'))
    if len(slug) > MAX_DNS_LABEL_LENGTH:
        raise DataAppSlugTooLongError(
            f'Data app name "{name}" generates a URL slug that is {len(slug)} characters long, '
            f'which exceeds the maximum DNS label length of {MAX_DNS_LABEL_LENGTH} characters. '
            f'Please use a shorter name (the slug "{slug[:20]}..." is too long). '
            f'The name should generate a slug of at most {MAX_DNS_LABEL_LENGTH} characters after '
            f'converting to lowercase, replacing spaces with hyphens, and removing special characters.'
        )
    return slug


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


def _get_secrets(workspace_id: str, branch_id: str, storage_token: str, storage_api_url: str) -> dict[str, Any]:
    """
    Generates secrets exposed to the data app as runtime environment variables. The injected
    `query_data` helper (and python-js apps that call Storage/Query Service directly) reads
    `BRANCH_ID`, `WORKSPACE_ID`, `KBC_TOKEN` and `KBC_URL` from the environment.
    """
    secrets: dict[str, Any] = {
        SECRET_WORKSPACE_ID: workspace_id,
        SECRET_BRANCH_ID: branch_id,
        SECRET_KBC_TOKEN: storage_token,
        SECRET_KBC_URL: storage_api_url,
    }
    return secrets


def _get_python_js_secrets(branch_id: str, storage_token: str, storage_api_url: str) -> dict[str, Any]:
    """
    Generates runtime secrets for python-js data apps. WORKSPACE_ID is intentionally omitted —
    python-js apps are created with `runtime.workspace.enabled = true`, so the platform
    auto-provisions a per-app workspace and sets WORKSPACE_ID in the runtime env itself.
    """
    return {
        SECRET_BRANCH_ID: branch_id,
        SECRET_KBC_TOKEN: storage_token,
        SECRET_KBC_URL: storage_api_url,
    }
