import copy
import importlib.resources as resources
import logging
import re
from typing import Annotated, Any, Literal, Mapping, Optional, Sequence, Union, cast
from urllib.parse import quote, urlsplit, urlunsplit

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
from keboola_mcp_server.tools.validation import ValidationContext, validate_storage_configuration_against_schema
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
            create_python_js_data_app_git_credential,
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
    mcp.add_tool(
        FunctionTool.from_function(
            delete_python_js_data_app_draft,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
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

# Username embedded in the HTTPS clone URL alongside the one-time token returned by the
# managed git-repo credentials endpoint. The git-service ignores the username portion of
# basic auth — only the password (token) is checked — but a non-empty username is required
# for `git clone` to accept the URL without prompting.
_MANAGED_GIT_REPO_USERNAME = 'kai'

# Default branch name used for the very first draft of a brand-new prod app, when the agent
# doesn't supply a descriptive branch via `branch=`. Uniqueness across drafts is the agent's
# responsibility — if `init` collides with an existing branch on the prod's repo, the agent
# will see the error from its own `git push` or from `deploy_data_app`.
_DEFAULT_DRAFT_BRANCH = 'init'


INJECTED_BLOCK_RE = re.compile(
    r'(?P<before>.*?)#\s###\sINJECTED_CODE\s####.*?#\s###\sEND_OF_INJECTED_CODE\s####(?P<after>.*)',
    re.DOTALL,
)

# Type of the authentication used in the data app
AuthenticationType = Literal['no-auth', 'basic-auth', 'default']

SECRET_WORKSPACE_ID = 'WORKSPACE_ID'
SECRET_BRANCH_ID = 'BRANCH_ID'

# Project feature that opts python-js data apps into platform-managed per-app workspaces.
# When enabled, the platform auto-provisions a workspace and injects WORKSPACE_ID at runtime.
# When disabled, the MCP falls back to passing WORKSPACE_ID via parameters.dataApp.secrets.
DATA_APPS_STORAGE_WORKSPACE_FEATURE = 'data-apps-storage-workspace'


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
        description=(
            'HTTPS clone URL of the managed git repo (without embedded credentials). '
            'Only set for python-js data apps, and only populated by detail-style fetches '
            '(`get_data_apps(configuration_ids=[...])`) and `modify_python_js_data_app` '
            'responses. The inventory list path (`get_data_apps` without `configuration_ids`) '
            'always leaves this `None` to keep the listing cheap — call the detail path to '
            'retrieve the URL. Mint a token via `create_python_js_data_app_git_credential` '
            'to authenticate.'
        ),
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
        description=(
            'HTTPS clone URL of the managed git repo (without embedded credentials). '
            'Only set for python-js data apps. Mint a token via '
            '`create_python_js_data_app_git_credential` to authenticate.'
        ),
    )
    configuration: dict[str, Any] = Field(
        description='The nested configuration object containing parameters, storage and authorization'
    )
    folder: str = Field(default='', description='The UI folder this data app is organized into')
    deployment_info: Optional[DeploymentInfo] = Field(
        description='Deployment info of the data app including a url of the app and logs to diagnose in-app errors.',
        default=None,
    )
    drafts: list[DataAppSummary] = Field(
        default_factory=list,
        description=(
            'Draft python-js data apps that iterate against this prod app (each carries '
            '`parameters.dataApp.parentConfigurationId == this.configuration_id`). Populated only '
            'when the detail path is used for a python-js **prod** app — empty for drafts '
            'themselves and for non-python-js apps.'
        ),
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
            'HTTPS clone URL of the managed git repo (without embedded credentials). Returned on create so the '
            'caller can clone the repo and push initial source code. On update, populated when the repo info can '
            "be fetched. On the draft create path this is the **parent prod app's** managed repo URL — that "
            'is the repo the agent should clone, branch, and push to. Mint a token via '
            '`create_python_js_data_app_git_credential` to authenticate (or use `git_clone_url` returned by this '
            'tool on the draft create path).'
        ),
    )
    git_clone_url: Optional[str] = Field(
        default=None,
        description=(
            'Ready-to-use authenticated HTTPS clone URL embedding the freshly-minted prod-app token (format: '
            '`https://kai:<secret>@<host>/<path>.git`). Only populated on the **draft create path** — the '
            'token was minted on the parent prod app and is one-time, so it is surfaced here so the agent can '
            'clone immediately without a separate `create_python_js_data_app_git_credential` call. None on prod '
            'create and on update.'
        ),
    )
    branch: Optional[str] = Field(
        default=None,
        description=(
            'Draft branch the new draft is pinned to (set in `parameters.dataApp.git.branch`). Only populated '
            'on the **draft create path** — defaults to `init` when the caller does not pass `branch`. The '
            'agent should `git checkout <branch>` (creating it if needed) and push code on this branch before '
            'calling `deploy_data_app(mode="dev")`. None on prod create and on update.'
        ),
    )
    links: list[Link] = Field(description='Navigation links for the web interface.')


class CreatedGitCredentialOutput(BaseModel):
    """Output for `create_python_js_data_app_git_credential`."""

    response: str = Field(description='The response of the action performed.')
    configuration_id: str = Field(description='The Storage configuration ID of the python-js data app.')
    data_app_id: str = Field(description='The ID of the data app the credential was created on.')
    credential_id: str = Field(description='The ID of the created credential.')
    git_clone_url: str = Field(
        description=(
            'Ready-to-use HTTPS clone URL with the one-time token embedded (format: '
            '`https://kai:<secret>@<host>/<path>.git`). Pass directly to `git clone`.'
        ),
    )
    secret: str = Field(
        description=(
            'One-time HTTPS token. Also embedded in `git_clone_url`. Surfaced separately so it can be plugged into '
            'a `git credential` helper. **The platform does not return this value again** — store it if you need '
            'to reuse it outside of `git_clone_url`.'
        ),
    )
    permissions: str = Field(description='The permissions of the credential, e.g. "readWrite".')
    links: list[Link] = Field(description='Navigation links for the web interface.')


class DeploymentDataAppOutput(BaseModel):
    """Deployment data app output containing the action performed, links and deployment info."""

    state: SafeState = Field(description='The state of the data app deployment.')
    deployment_info: DeploymentInfo | None = Field(
        description='Deployment info with a link to the app and logs to diagnose in-app errors.', default=None
    )
    links: list[Link] = Field(description='Navigation links for the web interface.')


class DeletedDraftOutput(BaseModel):
    """Output for `delete_python_js_data_app_draft`."""

    response: str = Field(description='Status of the delete operation, e.g. "deleted".')
    configuration_id: str = Field(description='Storage configuration ID of the deleted draft.')
    data_app_id: str = Field(description='Data-science API ID of the deleted draft data app.')
    parent_configuration_id: Optional[str] = Field(
        default=None,
        description=(
            'Storage configuration ID of the parent prod app the draft was iterating against. '
            'Surfaced so the agent can pivot back to the prod app after cleanup. May be None if '
            'the draft was orphaned (parent already deleted).'
        ),
    )
    links: list[Link] = Field(description='Navigation links for the web interface.', default_factory=list)


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
    parent_configuration_id: Annotated[
        Optional[str],
        Field(
            description=(
                'Storage configuration ID of the prod python-js data app this draft will iterate against. '
                'When set on create, the new app is created as a **draft**: no managed repo is provisioned '
                "for it; instead its `parameters.dataApp.git` block is populated to point at the prod app's "
                'managed repo, with a freshly-minted prod-app HTTPS token and the chosen draft branch. '
                'Leave None on create to make a **prod app** (which gets its own managed repo). Rejected on '
                'update.'
            ),
        ),
    ] = None,
    branch: Annotated[
        Optional[str],
        Field(
            description=(
                'Draft branch to pin the new draft to. Only valid on the draft create path '
                '(when `parent_configuration_id` is set). Defaults to `init` when unset (a sensible '
                'name for the first draft of a brand-new prod app). For subsequent edit-existing '
                "drafts, pass a descriptive branch name like 'add-revenue-filter'. Must not be `main` "
                '(reserved for the prod app). Rejected on prod create and on update.'
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
    storage: Annotated[
        Optional[dict[str, Any]],
        Field(
            description=(
                'Complete storage configuration for the data app (input/output table mappings). '
                'Validated against the storage JSON schema. Replaces the ENTIRE storage block when '
                'updating an existing app. For data apps with Storage Access, declare output tables '
                'with `unload_strategy: "direct-grant"` (in that case `source` is not required and '
                'the workspace is granted direct SELECT/INSERT/UPDATE/DELETE/TRUNCATE on the destination '
                'Storage table). Leave unset (None) to preserve the existing storage configuration; '
                'pass an empty dict to explicitly clear it.'
            ),
        ),
    ] = None,
    folder: Annotated[
        Optional[str],
        Field(description=folder_field_description('data app', 'data apps')),
    ] = None,
) -> ModifiedPythonJsDataAppOutput:
    """Creates or updates a python-js data app.

    Two-app project model. Every python-js project has a persistent **prod app** that owns the
    only managed git repository for the project, and zero or more **drafts** parented to that
    prod app. A draft is a Storage configuration with `parameters.dataApp.isDraft=true` and
    `parameters.dataApp.parentConfigurationId=<prod cfg id>`; it's an *external-git* app that
    clones the parent prod's repo at a pinned branch on every deploy. Drafts are surfaced in the
    Keboola UI under their parent prod app. Use `deploy_data_app(mode='dev')` to deploy a draft
    as a dev version of the data app (hot reload + auto-auth for iframe preview); use
    `delete_python_js_data_app_draft` to tear a draft down after its branch has been promoted.

    **MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge,
    branch-delete — is yours. MCP gives you authenticated clone URLs and manages configs/deploys;
    it never invokes git.

    **The draft flow below is mandatory — never edit prod source directly.** Every source-code
    change goes through a draft branch that the user previews and explicitly approves first. NEVER
    push directly to `main`: `main` only ever advances by merging an approved draft branch, and
    only after the user has approved that draft's preview.

    Three scenarios the agent has to distinguish:

    ## Scenario A — Create a brand-new data app

    1. `modify_python_js_data_app(slug='demo')` → `(configuration_id=PROD, repo_url=R)`.
       PROD owns the only managed repo for this app.
    2. `modify_python_js_data_app(slug='demo-draft', parent_configuration_id=PROD)`
       → `(configuration_id=DRAFT, repo_url=R, git_clone_url=U, branch='init')`.
       Default draft branch is `'init'`. Override with `branch=<name>` for a descriptive name.
    3. YOU: `git clone U`; `git checkout init` (creating it if the repo is empty); write source;
       `git push origin init`.
    4. `deploy_data_app(action='deploy', configuration_id=DRAFT, mode='dev')`
       → preview URL serving the `init` branch as a dev version. Iterate with the user.
    5. Once approved — YOU: `git checkout main`; `git merge init`; `git push origin main`;
       `git push origin --delete init`.
    6. `deploy_data_app(action='deploy', configuration_id=PROD)`
       → prod URL now serves the merged `main`.
    7. `delete_python_js_data_app_draft(configuration_id=DRAFT)`
       → tears down the draft's config + data-app instance. Always run this once promoted.

    ## Scenario B — Edit an existing data app

    You already have PROD's `configuration_id` (from `get_data_apps` or earlier conversation).

    1. `create_python_js_data_app_git_credential(configuration_id=PROD)`
       → fresh `git_clone_url U` with an embedded one-time token.
    2. `modify_python_js_data_app(
            slug='demo-draft-<short suffix>',
            parent_configuration_id=PROD,
            branch='<describes-the-change>',   # e.g. 'add-revenue-filter'
       )` → `(DRAFT, R, U2, branch)`. Use U2 (it has its own fresh token).
    3. YOU: `git clone U2`; `git checkout <branch>` (creating it from `main`); edit source;
       `git push origin <branch>`.
    4–7. Same as Scenario A steps 4–7.

    ## Scenario C — Continue an unfinished draft

    The previous sandbox is gone. You have PROD's `configuration_id` but no working clone and no
    draft handle.

    1. `get_data_apps(configuration_ids=[PROD])` → returns PROD's detail including `drafts: [...]`.
       Pick the draft the user means (ask if multiple and unclear). Each entry exposes its
       `configuration_id`, slug, and pinned branch.
    2. `create_python_js_data_app_git_credential(configuration_id=PROD)`
       → fresh `git_clone_url U` (the previous one was minted in a wiped sandbox and is lost).
       Drafts have no managed repo of their own — always mint against PROD.
    3. YOU: `git clone U`; `git checkout <draft's pinned branch>`; resume work; `git push`.
    4. `deploy_data_app(action='deploy', configuration_id=<DRAFT>, mode='dev')` → preview URL.
       The draft's branch is already pinned in its config, no override needed.
    5–7. Same promote/cleanup sequence as Scenario A steps 5–7.

    ## Argument rules

    - `parent_configuration_id` is **create-only**. Rejected on update.
    - `branch` is **create-only** and only valid when `parent_configuration_id` is set.
      Defaults to `'init'`. Must not be `'main'`. Rejected on prod create and on update.
    - `slug` is required on create and immutable after.
    - The **update path** (passing `configuration_id`) is for changing `name`, `description`,
      `authentication_type`, `auto_suspend_after_seconds`, `storage` on either a prod app or
      a draft. Source code changes go through the git flow above, not this tool.

    ## Authentication

    New apps default to HTTP basic authentication for safety. Pass `authentication_type='no-auth'`
    to expose publicly. On update, `authentication_type='default'` preserves the existing
    `authorization` block (including OIDC setups configured outside the MCP); `'basic-auth'` /
    `'no-auth'` overwrite it.

    ## Slug constraint

    Must be DNS-label-safe (lowercase letters, digits, hyphens, ≤63 chars). For drafts, append a
    short suffix (e.g. `-draft-abc123`) to keep slugs unique across the prod and its drafts.
    """
    if configuration_id:
        if slug:
            raise ValueError('slug cannot be changed after the data app is created.')
        if parent_configuration_id:
            raise ValueError('parent_configuration_id is only valid when creating a draft (no configuration_id).')
        if branch:
            raise ValueError('branch is only valid when creating a draft (no configuration_id).')
    else:
        if not slug:
            raise ValueError('slug is required when creating a python-js data app.')
        if branch is not None and not parent_configuration_id:
            raise ValueError('branch is only valid on the draft create path (pair it with parent_configuration_id).')

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    validated_storage = _validate_data_app_storage(storage, configuration_id=configuration_id or None)

    # When the platform-managed workspace feature is off, the data app cannot rely on the
    # platform to inject WORKSPACE_ID; fall back to passing it via parameters.dataApp.secrets.
    has_storage_workspace = await client.has_feature(DATA_APPS_STORAGE_WORKSPACE_FEATURE)
    legacy_secrets: Optional[dict[str, Any]] = None
    if not has_storage_workspace:
        workspace_manager = WorkspaceManager.from_state(ctx.session.state)
        legacy_secrets = {SECRET_WORKSPACE_ID: str(await workspace_manager.get_workspace_id())}

    if configuration_id:
        # Update existing python-js data app
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
        updated_config = _update_existing_code_data_app_config(
            existing_config=data_app.configuration,
            auto_suspend_after_seconds=auto_suspend_after_seconds,
            authentication_type=authentication_type,
            secrets=legacy_secrets,
            storage=validated_storage,
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
        repo_url = data_app.repo_url
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
        # Create new python-js data app — either a prod app (own managed repo) or a draft
        # (external-git binding pointing at the parent prod app's managed repo).
        # Narrowed by the validation block at the top of this function.
        assert slug is not None
        # On create, treat 'default' as 'basic-auth' (safe-by-default) to match modify_streamlit_data_app.
        uses_basic_auth = authentication_type in ('basic-auth', 'default')
        authorization_model = DataAppConfig.Authorization.model_validate(_get_authorization(uses_basic_auth))

        git_clone_url: Optional[str] = None
        draft_branch: Optional[str] = None
        git_block: Optional[CodeDataAppConfig.Parameters.DataApp.Git] = None
        if parent_configuration_id:
            # Draft create path: resolve the parent's repo + mint a parent-side credential, then
            # serialize an external-git block into the draft's config.
            parent = await _fetch_data_app(client, configuration_id=parent_configuration_id, data_app_id=None)
            if parent.type != 'python-js':
                raise ValueError(
                    f'parent_configuration_id "{parent_configuration_id}" is type "{parent.type}", but only '
                    f'python-js prod apps can parent a draft.'
                )
            if _is_draft_config(parent.configuration):
                # A draft has no managed repo of its own and cannot parent another draft. Reject it
                # explicitly instead of falling through to the misleading "no repo URL" error below.
                raise ValueError(
                    f'parent_configuration_id "{parent_configuration_id}" is itself a python-js **draft**, '
                    "not a prod app. Drafts iterate against the prod app's repo and cannot parent another "
                    "draft — pass the prod app's configuration_id (a draft's parentConfigurationId points to it)."
                )
            if not parent.repo_url:
                raise ValueError(
                    f'Parent python-js data app "{parent_configuration_id}" has no managed git repo URL. '
                    'This indicates a platform-side bug — retry or contact support.'
                )
            draft_branch = (branch or _DEFAULT_DRAFT_BRANCH).strip()
            if not draft_branch or any(c.isspace() for c in draft_branch):
                raise ValueError(f'branch "{branch}" is not a valid git branch name.')
            if draft_branch == 'main':
                raise ValueError('branch "main" is reserved for the prod app — pick a different draft branch.')
            cred = await client.data_science_client.create_app_git_credential(parent.data_app_id)
            if not cred.secret:
                raise ValueError(
                    f'Parent data app {parent.data_app_id} credentials endpoint returned no `secret` for an '
                    f'http_token credential. This indicates a platform-side bug — retry or contact support.'
                )
            git_block = CodeDataAppConfig.Parameters.DataApp.Git(
                repository=parent.repo_url,
                username=_MANAGED_GIT_REPO_USERNAME,
                password=cred.secret,
                branch=draft_branch,
            )
            git_clone_url = _build_authenticated_clone_url(parent.repo_url, cred.secret)

        config = CodeDataAppConfig(
            parameters=CodeDataAppConfig.Parameters(
                auto_suspend_after_seconds=auto_suspend_after_seconds,
                data_app=CodeDataAppConfig.Parameters.DataApp(
                    slug=slug,
                    secrets=legacy_secrets,
                    git=git_block,
                    is_draft=True if parent_configuration_id is not None else None,
                    parent_configuration_id=parent_configuration_id,
                ),
            ),
            runtime=(
                CodeDataAppConfig.Runtime(workspace=CodeDataAppConfig.Runtime.Workspace(enabled=True))
                if has_storage_workspace
                else None
            ),
            authorization=authorization_model,
            storage=validated_storage,
        )
        if git_block is not None:
            # The git block's `#password` is plaintext at this point; the encryption service walks
            # the dict and only encrypts keys starting with `#`, so everything else is untouched.
            project_id = await client.storage_client.project_id()
            config_payload = cast(dict[str, Any], config.model_dump(by_alias=True, exclude_none=True))
            encrypted_payload = await client.encryption_client.encrypt(
                config_payload,
                component_id=DATA_APP_COMPONENT_ID,
                project_id=project_id,
            )
            config = CodeDataAppConfig.model_validate(encrypted_payload)
        data_app_resp = await client.data_science_client.create_data_app(
            name=name,
            description=description,
            configuration=config,
            app_type='python-js',
            # Dev twins bring their own external-git binding; only prod apps get a managed repo.
            use_managed_git_repo=parent_configuration_id is None,
        )
        if parent_configuration_id:
            # Dev twin: the repo the agent must clone is the parent prod's managed repo.
            assert git_block is not None
            repo_url = git_block.repository
        else:
            repo_resp = await client.data_science_client.get_app_git_repo(data_app_resp.id)
            if repo_resp.https_url is None:
                raise ValueError(
                    f'Data app {data_app_resp.id} reports no HTTPS clone URL despite having a managed git repo. '
                    'This indicates a platform-side bug — retry or contact support.'
                )
            repo_url = repo_resp.https_url
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
            git_clone_url=git_clone_url,
            branch=draft_branch,
            links=links,
        )


@tool_errors()
async def create_python_js_data_app_git_credential(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='Storage configuration ID of the python-js data app.')],
) -> CreatedGitCredentialOutput:
    """Mints a one-time HTTPS token on a python-js **prod** data app so the caller can clone, pull,
    and push to the app's managed git repo over HTTPS.

    **Always call against the prod app's configuration_id** — drafts have no managed repo of their
    own, so calling this on a draft fails. The prod app is the canonical repo owner; drafts
    iterate against branches of that same repo.

    **MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge,
    branch-delete — is yours. This tool only mints credentials.

    Returns a ready-to-use `git_clone_url` of the form `https://kai:<secret>@<host>/<path>.git`
    plus the raw `secret`. The token is returned **only** at creation — the platform cannot return
    it again on any subsequent read. Stash the URL (or the secret) somewhere the LLM can reuse for
    the rest of the session.

    The data-science API accepts multiple credentials per app, so calling this again mints an
    additional token without invalidating any tokens already held by other clients.

    ## When to call

    1. **Right after `modify_python_js_data_app` create of a prod app** — the new prod has a
       managed repo but no credentials yet. Call this tool with the new app's `configuration_id`
       to enable git access. (Note: when creating a **draft**, the prod-side token is minted and
       embedded into the returned `git_clone_url` automatically — no separate call needed.)

    2. **Recovery when the cached token is gone / continuing an unfinished draft** — e.g., a fresh
       sandbox continuing yesterday's work, with the previous sandbox's filesystem wiped. The
       cached `git_clone_url` is lost; the configuration ID for the prod app is all you have.
       Call this tool with the **prod app's** `configuration_id` to mint a fresh token (drafts
       have no managed repo, so always mint against prod). Existing credentials remain valid, so
       other clients are not disrupted.

    ## Constraints
    - Only python-js prod data apps have a managed git repo. Streamlit apps reject the call with
      a clear error.
    - Permissions are always `readWrite` — the LLM virtually always needs push access. The
      data-science API supports read-only credentials, but the tool does not expose that knob;
      revisit once a real use case appears.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
    if data_app.type != 'python-js':
        raise ValueError(
            f'create_python_js_data_app_git_credential only supports python-js data apps, but configuration '
            f'"{configuration_id}" is type "{data_app.type}".'
        )
    if _is_draft_config(data_app.configuration):
        # Drafts have no managed repo of their own — they iterate against branches of the parent
        # prod's repo. Reject early with an actionable message instead of letting get_app_git_repo
        # return https_url=None below and raising a misleading "platform-side bug" error.
        data_app_block = cast(Mapping[str, Any], data_app.configuration.get('parameters') or {}).get('dataApp') or {}
        parent_cfg_id = data_app_block.get('parentConfigurationId')
        hint = f' (parentConfigurationId="{parent_cfg_id}")' if isinstance(parent_cfg_id, str) else ''
        raise ValueError(
            f'Configuration "{configuration_id}" is a python-js **draft**, which has no managed git repo '
            f'of its own. Mint credentials against the parent prod app instead{hint}.'
        )

    repo_resp = await client.data_science_client.get_app_git_repo(data_app.data_app_id)
    if repo_resp.https_url is None:
        raise ValueError(
            f'Data app {data_app.data_app_id} reports no HTTPS clone URL despite being a python-js managed-repo '
            f'app. This indicates a platform-side bug — retry or contact support.'
        )

    credential_resp = await client.data_science_client.create_app_git_credential(
        data_app_id=data_app.data_app_id,
    )
    if not credential_resp.secret:
        raise ValueError(
            f'Data app {data_app.data_app_id} credentials endpoint returned no `secret` for an http_token '
            f'credential. This indicates a platform-side bug — retry or contact support.'
        )

    git_clone_url = _build_authenticated_clone_url(repo_resp.https_url, credential_resp.secret)
    links = links_manager.get_data_app_links(
        configuration_id=data_app.configuration_id,
        configuration_name=data_app.name,
        deployment_link=data_app.deployment_url,
        uses_basic_authentication=False,
    )
    return CreatedGitCredentialOutput(
        response='created',
        configuration_id=data_app.configuration_id,
        data_app_id=data_app.data_app_id,
        credential_id=credential_resp.id,
        git_clone_url=git_clone_url,
        secret=credential_resp.secret,
        permissions=credential_resp.permissions,
        links=links,
    )


def _build_authenticated_clone_url(https_url: str, secret: str) -> str:
    """Embed the hardcoded git-service username and the one-time `secret` into the bare HTTPS URL
    so the LLM can pass it straight to `git clone`.
    """
    parts = urlsplit(https_url)
    if not parts.scheme or not parts.netloc:
        raise ValueError(f'Could not parse HTTPS clone URL: {https_url!r}')
    # Strip any pre-existing userinfo (the GET /git-repo endpoint already strips credentials,
    # but be defensive).
    host = parts.hostname or ''
    if parts.port is not None:
        host = f'{host}:{parts.port}'
    netloc = f'{_MANAGED_GIT_REPO_USERNAME}:{quote(secret, safe="")}@{host}'
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _validate_data_app_storage(
    storage: Optional[dict[str, Any]],
    *,
    configuration_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Validate a caller-provided storage block for a data app.

    Returns the validated `storage` dict, or None when no storage was provided (caller
    should preserve the existing storage configuration).

    The storage component-type rules in `validate_root_storage_configuration` (writer / SQL
    transformation special cases) don't apply to data apps — we just run the JSON-schema check.
    """
    if storage is None:
        return None
    # Accept both raw `storage` dict and pre-wrapped {'storage': storage}, mirroring the
    # behavior of validate_root_storage_configuration.
    storage_cfg = cast(dict[str, Any], storage.get('storage', storage)) if storage else {}
    normalized = cast(dict[str, Any], {'storage': storage_cfg})
    validation_context = ValidationContext(
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id=configuration_id,
        scope='storage',
    )
    validated = validate_storage_configuration_against_schema(
        normalized,
        initial_message='The "storage" field is not valid.',
        validation_context=validation_context,
    )
    return cast(dict[str, Any], validated['storage'])


def _update_existing_code_data_app_config(
    existing_config: Mapping[str, Any],
    auto_suspend_after_seconds: int,
    authentication_type: AuthenticationType = 'default',
    secrets: Optional[dict[str, Any]] = None,
    storage: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Apply requested updates to the existing python-js data app storage configuration.

    Slug is intentionally not updated here (immutable post-create). `runtime.image.version` is
    not touched either — the platform now picks a default for python-js apps, and any legacy
    `image.version` pin already in the stored config is preserved verbatim via deepcopy.
    `authentication_type='default'` preserves the existing `authorization` block (including OIDC
    setups configured outside the MCP); 'no-auth' / 'basic-auth' overwrite it.
    `secrets` are merged into the existing `parameters.dataApp.secrets` map without overwriting
    keys already present. Used on projects without the `data-apps-storage-workspace` feature to
    inject WORKSPACE_ID; on projects with the feature, pass None.
    `storage` replaces the entire `storage` block when provided (None preserves the existing one;
    an empty dict is an explicit wipe).
    """
    new_config = cast(dict[str, Any], copy.deepcopy(existing_config))
    new_config.setdefault('parameters', {})
    new_config['parameters']['autoSuspendAfterSeconds'] = auto_suspend_after_seconds
    if authentication_type != 'default':
        new_config['authorization'] = _get_authorization(authentication_type == 'basic-auth')
    if secrets:
        data_app = new_config['parameters'].setdefault('dataApp', {})
        updated_secrets = dict(data_app.get('secrets') or {})
        for key, value in secrets.items():
            if key not in updated_secrets:
                updated_secrets[key] = value
        data_app['secrets'] = updated_secrets
    if storage is not None:
        new_config['storage'] = storage
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
    - `repo_url` (managed git repo URL for python-js apps) is ONLY populated on the detail path
      (when `configuration_ids` is provided). The inventory list always returns `repo_url=None`,
      even for python-js apps with a managed repo — to retrieve the URL, call this tool again
      with the target `configuration_ids`.
    - When called with `configuration_ids=[<prod-cfg>]` for a python-js **prod** app, the response
      includes a `drafts: [...]` array of every draft (configs with `isDraft=true` and
      `parentConfigurationId == <prod-cfg>`) currently in the project. Drafts in trash are not
      included. Use this to discover existing drafts when continuing a previously abandoned
      iteration (Scenario C in `modify_python_js_data_app`). The array is empty for drafts
      themselves and for Streamlit apps.
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
                'Deployment mode. Set to "dev" to deploy a python-js draft as a **dev version of the data '
                'app** — the runtime uses a development `setup.sh` (hot reload), and the data-app proxy '
                'enables an auto-auth path so an iframe preview can render without a manual login. '
                'Only meaningful on **draft** configs (python-js apps with `isDraft=true`). Leave None '
                '(default) for prod redeploys and for Streamlit apps.'
            ),
        ),
    ] = None,
    branch: Annotated[
        Optional[str],
        Field(
            description=(
                'Git branch to deploy from. Only meaningful when `mode="dev"` for python-js drafts. '
                'Normally unnecessary — drafts have their branch pinned in `parameters.dataApp.git.branch` '
                'at create time; this argument overrides that pin for this single deploy (escape hatch). '
                'Leave None for prod deploys and for Streamlit apps.'
            ),
        ),
    ] = None,
) -> DeploymentDataAppOutput:
    """Deploys/redeploys a data app or stops a running data app in the Keboola environment asynchronously, given the
    action and the configuration ID.

    **MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge,
    branch-delete — is yours. This tool only triggers deploys against existing git state.

    ## Mode and branch (python-js apps)
    - `mode='dev'` deploys the target as a **dev version of the data app** — the runtime uses a
      development `setup.sh` (hot reload) and the data-app proxy enables an auto-auth path so an
      iframe preview can render without a manual login. Only meaningful on **draft** configs
      (python-js apps with `isDraft=true`).
    - For prod redeploys (including after merging a draft's branch into `main`), use no `mode` and
      no `branch` — the prod app picks up the current `main`.
    - The optional `branch=` argument overrides the branch the draft deploys from for this single
      deploy. Normally unnecessary — drafts have their draft branch pinned in
      `parameters.dataApp.git.branch` at create time.
    - python-js apps do NOT fetch a Storage `configVersion` for deployment (their source lives in
      git, not in the Storage configuration); this is handled automatically.

    ## Streamlit apps
    Streamlit apps have no managed git repo, so `mode` and `branch` have no effect on the
    deployed app. `mode=None` is the expected call shape; don't pass `branch`.

    ## Validation
    `branch` is only meaningful with `mode='dev'`; setting `branch` without `mode='dev'` raises an
    error for any app type (Streamlit or python-js).

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


@tool_errors()
async def delete_python_js_data_app_draft(
    ctx: Context,
    configuration_id: Annotated[
        str, Field(description='Storage configuration ID of the python-js draft data app to delete.')
    ],
) -> DeletedDraftOutput:
    """Deletes a python-js DRAFT data app — both the data-app instance (DSAPI) and its Storage
    configuration.

    **MCP never runs git on your behalf.** Deleting the feature branch on the remote is your job;
    this tool only tears down the draft config and its data-app instance.

    WHEN TO CALL: at the end of a promote-to-prod sequence, after you have merged the draft's
    branch into `main`, pushed, deleted the feature branch from the remote, and redeployed the
    prod app. The Keboola UI lists drafts under their parent prod app; once you call this tool,
    the draft disappears from that list.

    WHAT THIS TOOL REFUSES:
      - prod apps (no `isDraft` flag) — protects against accidental prod deletion;
      - Streamlit apps — they have no draft concept.

    WHAT THIS TOOL DOES NOT DO:
      - Run git. Deleting the feature branch on the remote is your job.
      - Revoke the prod-side git credential minted when the draft was created. Credential
        rotation is the user's job via the Keboola UI.

    After a successful call, pivot back to the parent prod app (its configuration_id is returned
    in the response) or to `get_data_apps` for further work.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    data_app = await _fetch_data_app(client, configuration_id=configuration_id, data_app_id=None)
    if data_app.type != 'python-js':
        raise ValueError(
            f'delete_python_js_data_app_draft only supports python-js data apps, but configuration '
            f'"{configuration_id}" is type "{data_app.type}".'
        )
    if not _is_draft_config(data_app.configuration):
        raise ValueError(
            f'Configuration "{configuration_id}" is a python-js **prod** app, not a draft '
            '(parameters.dataApp.isDraft is not true). This tool only deletes drafts — '
            'prod apps must be deleted from the Keboola UI.'
        )

    data_app_block = cast(Mapping[str, Any], data_app.configuration.get('parameters') or {}).get('dataApp') or {}
    parent_cfg_id = data_app_block.get('parentConfigurationId')
    parent_configuration_id: Optional[str] = parent_cfg_id if isinstance(parent_cfg_id, str) else None

    await client.data_science_client.delete_data_app(data_app.data_app_id)
    # DSAPI delete may also remove the Storage config (its docstring says so, and removal can be
    # eventually-consistent). Tolerate a 404 so the tool stays idempotent if the config is already
    # gone; any other status is a real error and must propagate.
    try:
        await client.storage_client.configuration_delete(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            skip_trash=False,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise
        LOG.info(
            f'Storage config "{configuration_id}" was already deleted (404) while tearing down the '
            'draft — treating as already removed.'
        )

    # When a parent prod app is known, the links pivot to it. We don't have the parent's name here,
    # so label it explicitly as the parent rather than reusing the (now-deleted) draft's name, which
    # would mislabel a link that points at a different configuration.
    links = links_manager.get_data_app_links(
        configuration_id=parent_configuration_id or configuration_id,
        configuration_name='parent prod app' if parent_configuration_id else data_app.name,
        deployment_link=None,
        uses_basic_authentication=False,
    )
    return DeletedDraftOutput(
        response='deleted',
        configuration_id=configuration_id,
        data_app_id=data_app.data_app_id,
        parent_configuration_id=parent_configuration_id,
        links=links,
    )


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

    if authentication_type != 'default':
        new_config['authorization'] = _get_authorization(authentication_type == 'basic-auth')
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
            data_app.repo_url = repo_resp.https_url
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
        data_app = data_app.with_links(links).with_deployment_info(logs)
        # Drafts of a python-js prod are surfaced inline so the agent can find them in one round-trip
        # — see Scenario C in `modify_python_js_data_app`. Skip for drafts themselves and for Streamlit
        # (neither has children).
        if data_app.type == 'python-js' and not _is_draft_config(data_app.configuration):
            data_app.drafts = await _fetch_prod_drafts(client, prod_configuration_id=data_app.configuration_id)
        return data_app
    except Exception:
        LOG.exception(f'Failed to fetch data app by configuration ID: {configuration_id}')
        return configuration_id


def _is_draft_config(configuration: Mapping[str, Any]) -> bool:
    """True iff the data app's stored configuration carries `parameters.dataApp.isDraft = true`.

    Shape-safe: a malformed/corrupted config whose `parameters` or `dataApp` is not a mapping is
    simply "not a draft" rather than an `AttributeError` (this helper runs in the detail-fetch path).
    """
    parameters = configuration.get('parameters')
    if not isinstance(parameters, Mapping):
        return False
    data_app = parameters.get('dataApp')
    if not isinstance(data_app, Mapping):
        return False
    return data_app.get('isDraft') is True


async def _fetch_prod_drafts(client: KeboolaClient, *, prod_configuration_id: str) -> list[DataAppSummary]:
    """List the drafts (configs with `parentConfigurationId == prod_configuration_id`) of a python-js
    prod app. Returns full `DataAppSummary` entries (one extra DSAPI fetch per draft, capped at
    10 parallel). Drafts in trash are not returned by `configuration_list` and so do not appear here.
    """
    configs = await client.storage_client.configuration_list(DATA_APP_COMPONENT_ID)
    draft_cfg_ids: list[str] = []
    for cfg in configs:
        cfg_body = cast(Mapping[str, Any], cfg.get('configuration') or {})
        # A draft must satisfy BOTH halves of the contract: `isDraft=true` AND `parentConfigurationId`
        # pointing at this prod. Checking only the parent pointer would surface a misconfigured
        # non-draft (e.g. a clone that kept the pointer but lost the flag) as a draft.
        if not _is_draft_config(cfg_body):
            continue
        data_app_block = cast(Mapping[str, Any], cfg_body.get('parameters') or {}).get('dataApp') or {}
        if data_app_block.get('parentConfigurationId') == prod_configuration_id:
            cfg_id = cfg.get('id')
            if isinstance(cfg_id, str):
                draft_cfg_ids.append(cfg_id)

    if not draft_cfg_ids:
        return []

    async def fetch_summary(cfg_id: str) -> DataAppSummary | None:
        try:
            draft = await _fetch_data_app(client, configuration_id=cfg_id, data_app_id=None)
        except Exception:
            LOG.exception(f'Failed to fetch draft data app by configuration ID: {cfg_id}')
            return None
        summary = DataAppSummary.model_validate(draft.model_dump())
        summary.repo_url = draft.repo_url
        return summary

    results = await process_concurrently(draft_cfg_ids, fetch_summary, max_concurrency=10)
    return [s for s in results if isinstance(s, DataAppSummary)]


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


def _get_secrets(workspace_id: str, branch_id: str) -> dict[str, Any]:
    """
    Generates secrets for the data app for querying the tables in the given workspace QS or SAPI.
    """
    secrets: dict[str, Any] = {
        SECRET_WORKSPACE_ID: workspace_id,
        SECRET_BRANCH_ID: branch_id,
    }
    return secrets
