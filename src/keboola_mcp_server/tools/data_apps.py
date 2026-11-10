import asyncio
import copy
import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from importlib import resources
from typing import Annotated, Any, Literal, cast
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
from fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field, PrivateAttr

from keboola_mcp_server.clients.base import NEVER_SENT_ERRORS, InvalidArgumentError, JsonDict
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import (
    ORGANIZATION_GIT_AUTH,
    AppRunResponse,
    CodeDataAppConfig,
    DataAppConfig,
    DataAppResponse,
    DraftResponse,
    GitAuthType,
    RuntimeResponse,
    validate_organization_repository_url,
)
from keboola_mcp_server.clients.storage import ConfigurationAPIResponse
from keboola_mcp_server.elicitation import offer_url
from keboola_mcp_server.errors import is_caller_error, tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import PlainFunctionTool as FunctionTool
from keboola_mcp_server.mcp import ToonCompactFunctionTool, process_concurrently
from keboola_mcp_server.scope import ProjectIdArg
from keboola_mcp_server.tools.components.utils import (
    apply_configuration_folder,
    build_folder_preview,
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
            annotations=ToolAnnotations(
                readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=False
            ),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            modify_python_js_data_app,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(
                readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=False
            ),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            create_python_js_data_app_git_credential,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(
                readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False
            ),
        )
    )
    mcp.add_tool(
        ToonCompactFunctionTool.from_function(
            get_data_apps,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(
                readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
            ),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            deploy_data_app,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(
                readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=True
            ),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            create_python_js_data_app_draft,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(
                readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False
            ),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            delete_python_js_data_app_draft,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(
                readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=False
            ),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_data_app_preview_link,
            tags={DATA_APP_TOOLS_TAG},
            annotations=ToolAnnotations(
                readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
            ),
        )
    )
    LOG.info('Data app tools initialized.')


# State of the data app's production: `starting` also covers a deployment on its way to production
State = Literal['notDeployed', 'starting', 'running', 'suspended', 'disabled']
# Accepts known states or any string preventing from validation errors when receiving unknown states from the API
# LLM agent can still understand the state of the data app even if it is different from the known states
SafeState = State | str
# Type of the data app
Type = Literal['streamlit', 'python-js']
# Accepts known types or any string preventing from validation errors when receiving unknown types from the API
# LLM agent can still understand the type of the data app even if it is different from the known types
SafeType = Type | str

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

# How much of an AppRun's diagnostics is surfaced in tool output. `failure_message` can embed the
# entire startup log (StartupProbeFailed duplicates it verbatim), so both are trimmed to keep the
# tool output bounded while preserving the error tail, which is where the actionable line lives.
_APP_RUN_LOG_LINES = 30
_APP_RUN_MESSAGE_LIMIT = 3000


INJECTED_BLOCK_RE = re.compile(
    r'(?P<before>.*?)#\s###\sINJECTED_CODE\s####.*?#\s###\sEND_OF_INJECTED_CODE\s####(?P<after>.*)',
    re.DOTALL,
)

# Type of the authentication used in the data app
AuthenticationType = Literal['no-auth', 'basic-auth', 'oidc', 'default']

OIDC_PROVIDER_ID = 'oidc-1'


class OidcAuthConfig(BaseModel):
    """OpenID Connect settings of a data app. The client secret is never part of it."""

    client_id: str = Field(description='The OIDC client ID registered at the identity provider.')
    issuer_url: str = Field(description='The URL of the OIDC issuer, for example https://login.example.com.')
    logout_url: str | None = Field(
        default=None,
        description=(
            'The URL where the user is sent after logout. '
            'Omit it or pass null to keep the stored value. Pass an empty string to remove it.'
        ),
    )
    allowed_roles: list[str] | None = Field(
        default=None,
        description=(
            'The roles that may open the app. '
            'Omit it or pass null to keep the stored value. Pass an empty list to remove it.'
        ),
    )


def _type_object_on_ref(schema: dict[str, Any]) -> None:
    """Add `type: object` to the `$ref` member of the schema, as some MCP clients need every member typed."""
    for member in schema.get('anyOf', []):
        if '$ref' in member:
            member['type'] = 'object'


_NO_AUTH_ON_DRAFT_MESSAGE = (
    "authentication_type='no-auth' is not allowed on draft data apps. A draft inherits the prod "
    'app\'s data access, so disabling its authentication exposes Storage-reading and Storage-writing '
    'endpoints publicly. Never disable authentication to make the preview work -- the in-platform '
    "preview link authenticates on top of the configured auth. Keep "
    "'basic-auth' or 'default'; if the user explicitly wants the published app to be public, set "
    "'no-auth' on the prod app."
)


def _draft_may_exist(configuration_id: str, branch: str) -> ValueError:
    """The error for a draft request that was sent but has no usable answer."""
    return ValueError(
        f'The draft request got no usable answer, so the draft may exist already. Check '
        f'get_data_apps(configuration_ids=["{configuration_id}"]) for a draft on "{branch}" before calling '
        'this tool again.'
    )


def _reject_no_auth_on_draft(authentication_type: str) -> None:
    if authentication_type == 'no-auth':
        raise ValueError(_NO_AUTH_ON_DRAFT_MESSAGE)


_OIDC_ON_DRAFT_MESSAGE = (
    "authentication_type='oidc' is not allowed on draft data apps. A draft has its own URL and so its "
    'own callback URL, which the identity provider does not know. Set up OIDC on the prod app instead.'
)


def _reject_oidc_on_draft(authentication_type: AuthenticationType) -> None:
    if authentication_type == 'oidc':
        raise ValueError(_OIDC_ON_DRAFT_MESSAGE)


def _validate_oidc_args(
    authentication_type: AuthenticationType, oidc: OidcAuthConfig | None, *, is_create: bool
) -> None:
    if authentication_type == 'oidc':
        if is_create:
            raise ValueError(
                "authentication_type='oidc' is not allowed when creating a data app, because the callback URL "
                "needs the app ID. Create the app with authentication_type='basic-auth' first, "
                "then update it with 'oidc'."
            )
        if oidc is None:
            raise ValueError("authentication_type='oidc' needs the `oidc` settings (client_id and issuer_url).")
    elif oidc is not None:
        raise ValueError(
            "The `oidc` settings are only used with authentication_type='oidc'. "
            'Set that authentication type or drop `oidc`.'
        )


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
            'The type of the data app: "python-js" (the default for new apps) or "streamlit". Keboola DSAPI '
            'may report other types as well.'
        )
    )
    deployment_url: str | None = Field(description='The URL of the running data app.', default=None)
    auto_suspend_after_seconds: int | None = Field(
        description='The number of seconds after which the running data app is automatically suspended.',
        default=None,
    )
    storage_access_enabled: bool | None = Field(
        default=None,
        description=(
            'Whether the data app has read-only Storage access -- i.e. whether a workspace ID '
            'reaches the running app as the WORKSPACE_ID environment variable. True when the stored '
            'configuration carries `runtime.workspace.enabled = true` (projects with the '
            '`data-apps-storage-workspace` feature) or a `parameters.dataApp.secrets.WORKSPACE_ID` '
            'entry (projects without it).\n'
            '<!-- directory:omit -->\n'
            'Only populated by `modify_python_js_data_app` responses; '
            'left `None` elsewhere, which means "not determined", not "disabled".\n'
            '<!-- /directory:omit -->\n'
            '<!-- directory:only -->\n'
            'Left `None` here, which means "not determined", not "disabled".\n'
            '<!-- /directory:only -->\n'
        ),
    )
    repo_url: str | None = Field(
        default=None,
        description=(
            'HTTPS clone URL of the managed git repo (without embedded credentials). '
            'Only set for python-js data apps.\n'
            '<!-- directory:omit -->\n'
            'Only populated by detail-style fetches (`get_data_apps(configuration_ids=[...])`) and '
            '`modify_python_js_data_app` responses.\n'
            '<!-- /directory:omit -->\n'
            '<!-- directory:only -->\n'
            'Only populated by detail-style fetches (`get_data_apps(configuration_ids=[...])`).\n'
            '<!-- /directory:only -->\n'
            'The inventory list path (`get_data_apps` without `configuration_ids`) '
            'always leaves this `None` to keep the listing cheap — call the detail path to '
            'retrieve the URL.\n'
            '<!-- directory:omit -->\n'
            'Mint a token via `create_python_js_data_app_git_credential` to authenticate.\n'
            '<!-- /directory:omit -->\n'
        ),
    )
    git_branch: str | None = Field(
        default=None,
        description='The git branch the app deploys. Not set by the inventory listing (`get_data_apps` without IDs).',
    )
    git_auth_type: str | None = Field(
        default=None,
        description=(
            '"organization" when the app clones a customer GitHub repository through the Keboola organization\'s '
            'GitHub connection: no managed repo, no git credential, production deploys only. None otherwise. '
            'Not set by the inventory listing (`get_data_apps` without IDs).'
        ),
    )

    @classmethod
    def from_api_response(cls, api_response: DataAppResponse, config_version: str) -> 'DataAppSummary':
        return cls(
            component_id=api_response.component_id,
            configuration_id=api_response.config_id,
            data_app_id=api_response.id,
            project_id=api_response.project_id,
            branch_id=api_response.branch_id or '',
            config_version=config_version,
            state=api_response.reported_state,
            type=api_response.type or 'unknown',
            deployment_url=api_response.url,
        )


class AppRunInfo(BaseModel):
    """Outcome of a single deployment attempt (AppRun) of a data app."""

    state: str = Field(description='The state of the run: "starting", "running", "finished" or "failed".')
    created_at: str | None = Field(description='The timestamp when the run was created.', default=None)
    stopped_at: str | None = Field(
        description='The timestamp when the run stopped, or `null` while it is still active.', default=None
    )
    failure_reason: str | None = Field(
        description=(
            'Machine-readable code identifying why the run failed, e.g. "ConfigDecryptionFailed" '
            'or "StartupProbeFailed". `null` for successful runs.'
        ),
        default=None,
    )
    failure_message: str | None = Field(
        description='Detailed explanation of the failure, when the platform provides one.', default=None
    )
    startup_logs: list[str] = Field(
        description="The last lines of the run's startup (entrypoint) log, when available.",
        default_factory=list,
    )

    @classmethod
    def from_api_response(cls, run: AppRunResponse) -> 'AppRunInfo':
        startup_logs = (run.startup_logs or '').strip().rsplit('\n', _APP_RUN_LOG_LINES)[-_APP_RUN_LOG_LINES:]
        failure_message = run.failure_reason.message if run.failure_reason else None
        if failure_message and len(failure_message) > _APP_RUN_MESSAGE_LIMIT:
            failure_message = '…' + failure_message[-(_APP_RUN_MESSAGE_LIMIT - 1) :]
        return cls(
            state=run.state,
            created_at=run.created_at,
            stopped_at=run.stopped_at,
            failure_reason=run.failure_reason.reason if run.failure_reason else None,
            failure_message=failure_message,
            startup_logs=[line for line in startup_logs if line],
        )


class RuntimeImage(BaseModel):
    """A backend image a python-js data app can run on, from the platform's runtimes catalog."""

    version: str = Field(
        description=(
            'The image tag.\n'
            '<!-- directory:omit -->\n'
            'The value `modify_python_js_data_app(image_version=...)` takes.\n'
            '<!-- /directory:omit -->\n'
        )
    )
    description: str | None = Field(
        default=None, description='Human-readable label naming its Python and Node.js versions.'
    )
    is_default: bool = Field(
        default=False, description='Whether the platform runs this image for an app that pins none.'
    )
    end_of_life_date: str | None = Field(
        default=None, description='When the image reaches its end of life, or `null` when it has no end date.'
    )


class DeploymentInfo(BaseModel):
    """Deployment information of a data app."""

    version: str = Field(
        description=(
            'The configuration version deployed to production (the published version pinned by the data-app '
            'service); while `state` is "starting" it is still rolling out. This is NOT necessarily the latest '
            'saved configuration version -- compare with `latest_config_version`.'
        )
    )
    latest_config_version: str | None = Field(
        default=None, description='The latest saved version of the data app configuration in Storage.'
    )
    has_unpublished_changes: bool = Field(
        default=False,
        description=(
            'True when the latest saved configuration version is newer than the running (published) '
            'version, i.e. configuration changes (secrets, Storage access, git branch, size, ...) are '
            'saved but NOT live. Call `deploy_data_app(action="deploy")` to publish them.'
        ),
    )
    state: str = Field(description='The state of the data app deployment.')
    url: str | None = Field(description='The URL of the running data app deployment.', default=None)
    last_request_timestamp: str | None = Field(
        description='The last request timestamp of the data app deployment.', default=None
    )
    last_start_timestamp: str | None = Field(
        description='The last start timestamp of the data app deployment.', default=None
    )
    logs: list[str] = Field(
        description='The latest 20 log lines reported in the data app deployment.', default_factory=list
    )
    image: RuntimeImage | None = Field(
        default=None,
        description=(
            'For python-js apps: the backend image the latest saved configuration selects — the one the '
            'app runs unless `has_unpublished_changes`. `null` when the runtimes catalog is unavailable.'
        ),
    )
    image_pinned: bool | None = Field(
        default=None,
        description=(
            'For python-js apps: true when the configuration pins `image`, false when the app follows the '
            'platform default (and moves with it on its next deploy).'
        ),
    )
    last_run: AppRunInfo | None = Field(
        description=(
            'The most recent deployment attempt (AppRun). When the app failed to start, its '
            '`failure_reason`/`failure_message` explain why — including setup-phase failures '
            '(e.g. invalid secrets) that happen before the container starts and so produce no '
            'container logs at all. Check this FIRST when diagnosing an app that does not start.'
        ),
        default=None,
    )


class DraftInfo(BaseModel):
    """A draft of a python-js data app: its own workload on its own host, syncing one git branch."""

    draft_id: str = Field(description='The draft ID.')
    git_branch: str = Field(description='The git branch the draft syncs; a push to it reaches the draft in seconds.')
    url: str | None = Field(default=None, description='The draft host URL; `null` until the platform assigns it.')
    run_state: str = Field(description='neverRun, pending, starting, running, stopping, stopped or failed.')
    created_at: str = Field(description='When the draft was created.')

    @classmethod
    def from_response(cls, draft: DraftResponse) -> 'DraftInfo':
        return cls(
            draft_id=draft.id,
            git_branch=draft.git_branch,
            url=draft.url,
            run_state=draft.run_state,
            created_at=draft.created_at,
        )


class DataApp(BaseModel):
    """A data app used for detail views."""

    name: str = Field(description='The name of the data app.')
    description: str | None = Field(description='The description of the data app.', default=None)
    component_id: str = Field(description='The ID of the data app component.')
    configuration_id: str = Field(description='The ID of the data app configuration.')
    data_app_id: str = Field(description='The ID of the data app.')
    project_id: str = Field(description='The ID of the project.')
    branch_id: str = Field(description='The ID of the branch.')
    config_version: str = Field(description='The latest saved version of the data app config.')
    published_config_version: str | None = Field(
        default=None,
        description=(
            'The config version deployed to production (published). When it is lower than '
            '`config_version`, the latest configuration changes are not live yet.'
        ),
    )
    state: SafeState = Field(description='The state of the data app.')
    type: SafeType = Field(
        description=(
            'The type of the data app: "python-js" (the default for new apps) or "streamlit". Keboola DSAPI '
            'may report other types as well.'
        )
    )
    desired_deployment_id: str | None = Field(
        description='The deployment most recently asked to be production.', default=None, exclude=True
    )
    deployment_url: str | None = Field(
        description='The URL the data app serves on; a draft reports it before its first run.', default=None
    )
    auto_suspend_after_seconds: int | None = Field(
        description='The number of seconds after which the running data app is automatically suspended.',
        default=None,
    )
    repo_url: str | None = Field(
        default=None,
        description=(
            'HTTPS clone URL of the managed git repo (without embedded credentials). '
            'Only set for python-js data apps.\n'
            '<!-- directory:omit -->\n'
            'Mint a token via `create_python_js_data_app_git_credential` to authenticate.\n'
            '<!-- /directory:omit -->\n'
        ),
    )
    is_managed_git_repo: bool | None = Field(
        default=None,
        description=(
            'For python-js data apps: True when the app runs on a Keboola-managed git repo, False when it '
            'is bound to an external repository (a draft, or an app pointing at its own external repo). '
            'None when it is not a python-js app or the git-repo lookup failed. Sourced from the '
            'data-science `.../git-repo` endpoint.'
        ),
    )
    git_branch: str | None = Field(default=None, description='The git branch the app deploys.')
    git_auth_type: str | None = Field(
        default=None,
        description=(
            '"organization" when the app clones a customer GitHub repository through the Keboola organization\'s '
            'GitHub connection: no managed repo, no git credential, production deploys only. None otherwise.'
        ),
    )
    configuration: dict[str, Any] = Field(
        description='The nested configuration object containing parameters, storage and authorization'
    )
    folder: str = Field(default='', description='The UI folder this data app is organized into')
    deployment_info: DeploymentInfo | None = Field(
        description='Deployment info of the data app including a url of the app and logs to diagnose in-app errors.',
        default=None,
    )
    drafts: list[DraftInfo] = Field(
        default_factory=list,
        description='Live drafts of this python-js prod app, newest first. Empty for legacy drafts and Streamlit apps.',
    )
    drafts_unavailable: bool = Field(
        default=False,
        description=(
            'True when the drafts could not be listed on this call: `drafts` is incomplete; retry before concluding '
            'there are none.'
        ),
    )
    legacy_drafts: list[DataAppSummary] = Field(
        default_factory=list,
        description=(
            'Legacy drafts of this python-js prod app: separate configurations with '
            '`parameters.dataApp.parentConfigurationId == this.configuration_id`, each on its own `git_branch`.'
        ),
    )
    legacy_drafts_unavailable: int = Field(
        default=0,
        description=(
            'Count of legacy drafts whose details could not be fetched on this call. Non-zero means '
            '`legacy_drafts` is incomplete; retry before concluding there are none.'
        ),
    )
    links: list[Link] = Field(description='Navigation links for the web interface.', default_factory=list)

    # Whether the Storage API returned the configuration `folder` field (False on stacks without folder support).
    _has_folder_field: bool = PrivateAttr(default=False)

    @property
    def has_folder_field(self) -> bool:
        return self._has_folder_field

    @classmethod
    def from_api_responses(
        cls,
        api_response: DataAppResponse,
        api_configuration: ConfigurationAPIResponse,
    ) -> 'DataApp':
        data_app = cls(
            component_id=api_configuration.component_id,
            configuration_id=api_configuration.configuration_id,
            data_app_id=api_response.id,
            project_id=api_response.project_id,
            branch_id=api_response.branch_id or '',
            config_version=str(api_configuration.version),
            state=api_response.reported_state,
            desired_deployment_id=api_response.production.desired_deployment_id,
            published_config_version=api_response.production.desired_config_version,
            type=api_response.type or 'unknown',
            deployment_url=api_response.url,
            auto_suspend_after_seconds=_config_auto_suspend(api_configuration.configuration),
            name=api_configuration.name,
            description=api_configuration.description,
            folder=api_configuration.folder or '',
            configuration=api_configuration.configuration,
            git_branch=_config_git_branch(api_configuration.configuration),
            git_auth_type=_config_git_auth_type(api_configuration.configuration),
            deployment_info=None,
            links=[],
        )
        data_app._has_folder_field = 'folder' in api_configuration.model_fields_set
        return data_app

    def with_links(self, links: list[Link]) -> 'DataApp':
        self.links = links
        return self

    def with_deployment_info(self, logs: list[str], last_run: AppRunInfo | None = None) -> 'DataApp':
        """Adds deployment info to the data app.

        :param logs: The logs of the data app deployment.
        :param last_run: The most recent deployment attempt (AppRun), when available.
        :return: The data app with the deployment info.
        """
        published_version = self.published_config_version or self.config_version
        if last_run is not None and _uses_organization_git_auth(self.configuration):
            hint = _organization_git_error_hint(last_run.failure_message)
            if hint:
                last_run.failure_message = f'{last_run.failure_message}\n{hint}'
        self.deployment_info = DeploymentInfo(
            version=published_version,
            latest_config_version=self.config_version,
            has_unpublished_changes=_is_version_newer(self.config_version, published_version),
            state=self.state,
            url=self.deployment_url or 'deployment link not available yet',
            logs=logs,
            last_run=last_run,
        )
        return self

    def with_image(self, images: list[RuntimeImage] | None) -> 'DataApp':
        """Reports the image a python-js app runs in its deployment info.

        - Call it after `with_deployment_info`.
        - A no-op for other apps, or when the catalog is unavailable (`None`).
        """
        assert self.deployment_info is not None, 'with_image needs with_deployment_info first'
        if self.type == 'python-js' and images is not None:
            self.deployment_info.image, self.deployment_info.image_pinned = _resolve_app_image(
                self.configuration, images
            )
        return self


class ModifiedDataAppOutput(BaseModel):
    """Modified data app output containing the response of the action performed and the data app and links to the web
    interface."""

    response: str = Field(description='The response of the action performed with potential additional information.')
    change_summary: str | None = Field(default=None, description='Additional notes or hints about the operation.')
    data_app: DataAppSummary = Field(description='The data app.')
    links: list[Link] = Field(description='Navigation links for the web interface.')
    oidc_callback_url: str | None = Field(
        default=None,
        description=(
            'The OIDC redirect URI of the app (`https://<slug>-<app id>.hub.<stack>/_proxy/callback`). Register it '
            'at the identity provider before you switch the app to OIDC. None for drafts.'
        ),
    )
    oidc_setup_required: bool = Field(
        default=False,
        description=(
            'True when the app uses OIDC but has no client secret yet: it cannot log anyone in. '
            'Tell the user to follow `change_summary` before deploying.'
        ),
    )


class ModifiedPythonJsDataAppOutput(BaseModel):
    """Output for `modify_python_js_data_app`. Includes git repo URL on create."""

    response: str = Field(description='The response of the action performed with potential additional information.')
    change_summary: str | None = Field(default=None, description='Additional notes or hints about the operation.')
    data_app: DataAppSummary = Field(description='The data app.')
    oidc_callback_url: str | None = Field(
        default=None,
        description=(
            'The OIDC redirect URI of the app (`https://<slug>-<app id>.hub.<stack>/_proxy/callback`). Register it '
            'at the identity provider before you switch the app to OIDC. None for drafts.'
        ),
    )
    oidc_setup_required: bool = Field(
        default=False,
        description=(
            'True when the app uses OIDC but has no client secret yet: it cannot log anyone in. '
            'Tell the user to follow `change_summary` before deploying.'
        ),
    )
    repo_url: str | None = Field(
        default=None,
        description=(
            'HTTPS clone URL of the managed git repo (without embedded credentials). Returned on create so the '
            'caller can clone the repo and push initial source code. On update, populated when the repo info can '
            'be fetched. Mint a token via `create_python_js_data_app_git_credential` to authenticate. With '
            "`git_repository` this is the customer's GitHub repository, which you reach with the user's own GitHub "
            'access.'
        ),
    )
    branch: str | None = Field(
        default=None,
        description='With `git_repository` on create: the branch of that repository the app deploys. None otherwise.',
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
    change_summary: str | None = Field(default=None, description='Notes about the result to act on.')
    links: list[Link] = Field(description='Navigation links for the web interface.')


class CreatedDraftOutput(BaseModel):
    """Output of `create_python_js_data_app_draft`."""

    configuration_id: str = Field(description='Storage configuration ID of the prod app the draft belongs to.')
    draft: DraftInfo = Field(description='The new draft.')
    change_summary: str | None = Field(default=None, description='Notes about the result to act on.')
    links: list[Link] = Field(description='Navigation links for the web interface.', default_factory=list)


class DeletedDraftOutput(BaseModel):
    """Output for `delete_python_js_data_app_draft`."""

    response: str = Field(description='Status of the delete operation, e.g. "deleted".')
    configuration_id: str = Field(
        description='Storage configuration ID of the deleted legacy draft, or of the prod app whose draft was deleted.'
    )
    draft_id: str | None = Field(default=None, description='ID of the deleted draft; `null` for a legacy draft.')
    data_app_id: str = Field(description='Data-science API ID of the deleted legacy draft, or of the prod app.')
    parent_configuration_id: str | None = Field(
        default=None,
        description=(
            'Storage configuration ID of the parent prod app the draft was iterating against. '
            'Surfaced so the agent can pivot back to the prod app after cleanup. May be None if '
            'the draft was orphaned (parent already deleted).'
        ),
    )
    links: list[Link] = Field(description='Navigation links for the web interface.', default_factory=list)


class DataAppPreviewLinkOutput(BaseModel):
    """Output of `get_data_app_preview_link`."""

    url: str = Field(
        repr=False,
        description=(
            "Link that opens the data app in a browser without the app's login. It contains a secret: open it "
            'only in a browser, before `link_expires_at`.'
        ),
    )
    link_expires_at: str = Field(
        description=(
            'Open `url` before this time (ISO 8601). A browser session opened with it keeps working after this '
            'time; call `get_data_app_preview_link` again only when the app shows its login page ("This app is '
            'password protected") or looks broken until reloaded, or when `url` was not opened before this time.'
        )
    )


class GetDataAppsOutput(BaseModel):
    """Output of the get_data_apps tool. Serves for both DataAppSummary and DataApp outputs."""

    data_apps: Sequence[DataAppSummary | DataApp] = Field(description='The data apps in the project.')
    available_images: list[RuntimeImage] = Field(
        default_factory=list,
        description=(
            'When the details include a python-js app: every backend image the platform offers.\n'
            '<!-- directory:omit -->\n'
            'The valid `image_version` values for `modify_python_js_data_app`.\n'
            '<!-- /directory:omit -->\n'
        ),
    )
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
                'app to be secured using the HTTP basic authentication, "oidc" sets up OIDC login (update '
                'only, needs the `oidc` argument), and "default" keeps the existing authentication type '
                'when updating.'
            )
        ),
    ],
    configuration_id: Annotated[
        str, Field(description='The ID of existing data app configuration when updating, otherwise empty string.')
    ] = '',
    oidc: Annotated[
        OidcAuthConfig | None,
        Field(
            description=(
                'OIDC settings. Required with authentication_type="oidc" and not allowed with any other '
                'type. Only for updating an existing app, not for create. It has no secret '
                'field: the user enters the client secret in the Keboola UI.'
            ),
            json_schema_extra=_type_object_on_ref,
        ),
    ] = None,
    change_description: Annotated[
        str,
        Field(description='The description of the change when updating (e.g. "Update Code"), otherwise empty string.'),
    ] = '',
    folder: Annotated[
        str | None,
        Field(description=folder_field_description('data app', 'data apps')),
    ] = None,
    project_id: ProjectIdArg = None,
) -> ModifiedDataAppOutput:
    """Creates or updates a Streamlit data app.

    <!-- directory:omit -->
    Python-js is the default type for new data apps when you can run git: create them with
    `modify_python_js_data_app` (python-js source code is delivered only by `git push`). Use this tool to create
    a new app when you cannot run git or when the user asks for Streamlit explicitly, and to update existing
    Streamlit apps.
    <!-- /directory:omit -->
    <!-- directory:only -->
    Use this tool to create new data apps and to update existing Streamlit apps. Other data apps cannot be changed here;
    tell the user so.
    <!-- /directory:only -->

    Considerations:
    - The `source_code` parameter must be a complete and runnable Streamlit app. It must include a placeholder
    `{QUERY_DATA_FUNCTION}` where a `query_data` function will be injected. This function queries the workspace to get
    data, it accepts a string of SQL query following current sql dialect and returns a pandas DataFrame with the results
    from the workspace.
    - The placeholder is substituted only by this tool when it saves the configuration; it is not a platform
    feature. A configuration written any other way (e.g. Storage API, CLI) must define `query_data` itself, otherwise
    the literal placeholder breaks the app.
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
    - `authentication_type='oidc'` (with the `oidc` argument) switches an existing app to OIDC. It is rejected on
    create. The tool never takes a client secret: the user enters it in the Keboola UI.
    Set up OIDC in this order:
    1. Create the app with `'basic-auth'`. The output has `oidc_callback_url`.
    2. The user registers `oidc_callback_url` at the identity provider and gives you the `client_id`.
    3. Update the app with `authentication_type='oidc'` and `oidc` (`client_id`, `issuer_url`, and optionally
       `logout_url`, `allowed_roles`).
    4. The user enters the client secret in the app's Authentication section in the Keboola UI. If the client
       can open URLs, it is asked to open that page.
    5. Only if you use `allowed_roles`: update with `'oidc'` once more after the secret is saved.
    6. Call `deploy_data_app` last.
    If the output has `oidc_setup_required=true`, tell the user to follow `change_summary`. In `oidc`, null keeps
    the stored `logout_url` / `allowed_roles`; `''` or `[]` removes them. The slug stays fixed on OIDC apps.

    SQL & DATA TYPE RULES:
    - Use delimited identifiers for the current SQL dialect for all column names and aliases in SQL.
      Match the exact identifier case used in SQL when referencing columns in Python code.
    - `query_data` RETURNS ALL COLUMNS AS STRINGS regardless of SQL CAST. Always convert types in Python after loading:
    `df["col"] = pd.to_numeric(df["col"], errors="coerce").fillna(0)` and
    `df["date"] = pd.to_datetime(df["date"], errors="coerce")`.
    """
    _validate_oidc_args(authentication_type, oidc, is_create=not configuration_id)
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    project_id = await client.storage_client.project_id()
    workspace_id = await workspace_manager.get_data_app_workspace_id()
    sql_dialect = await workspace_manager.get_data_app_sql_dialect()
    branch_id = await workspace_manager.get_data_app_branch_id()

    secrets = _get_secrets(
        workspace_id=str(workspace_id),
        branch_id=str(branch_id),
    )

    if configuration_id:
        # Update existing data app.
        #
        # `configuration_update` below is the ONLY step allowed to fail the whole tool: if it raises, nothing
        # was committed and a ToolError truthfully reports the failure. Everything after it merely enriches the
        # response (re-fetch, metadata, folder, links) and must NEVER turn a committed write into a reported
        # failure -- otherwise the agent, seeing an error, retries and re-applies the change, producing the
        # v27->v29 double-write that deployed broken code (AJDA-2852). Hence the best-effort block below.
        data_app_pre, updated_config, _ = await modify_streamlit_data_app_internal(
            client=client,
            workspace_manager=workspace_manager,
            name=name,
            description=description,
            source_code=source_code,
            packages=packages,
            authentication_type=authentication_type,
            oidc=oidc,
            configuration_id=configuration_id,
            change_description=change_description,
        )
        update_resp = await client.storage_client.configuration_update(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration=updated_config,
            change_description=change_description or 'Change Data App',
            updated_name=name or data_app_pre.name,
            updated_description=description or data_app_pre.description,
        )
        # --- write committed past this point; response building is strictly best-effort ---
        # The new version comes straight from the PUT response, so it is known even if the re-fetch below fails.
        # Read it defensively: this runs after the committing write and must not raise (a non-dict response would
        # otherwise turn the committed write into a ToolError -- the very failure mode this block guards against).
        new_version = str(update_resp.get('version') or '') if isinstance(update_resp, dict) else ''
        try:
            # Only stamp the UPDATED_BY_MCP version when the new version is actually known. Falling back to the
            # pre-update version would record a misleading version (the write did increment it), so skip the
            # metadata write when the response carried no numeric version.
            if new_version.isdigit():
                await set_cfg_update_metadata(
                    client=client,
                    component_id=DATA_APP_COMPONENT_ID,
                    configuration_id=configuration_id,
                    configuration_version=int(new_version),
                )
            # Without `folder`, an update keeps the folder: the hint lists every data app, so only a create gives it.
            folder_hint = (
                await apply_configuration_folder(
                    client, DATA_APP_COMPONENT_ID, configuration_id, folder, 'data apps', 'modify_streamlit_data_app'
                )
                if folder is not None
                else None
            )
            data_app = await _fetch_data_app(client, configuration_id=configuration_id)
            links = links_manager.get_data_app_links(
                configuration_id=data_app.configuration_id,
                configuration_name=name,
                deployment_link=data_app.deployment_url,
                uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
            )
            response = _update_response(data_app)
            written_authorization = updated_config.get('authorization') or {}
            config_link = links_manager.get_data_app_config_link(configuration_id, name, False).url
            oidc_hint = _oidc_change_summary(
                written_authorization,
                config_link=config_link,
                roles_requested=bool(oidc and oidc.allowed_roles),
            )
            if _oidc_setup_required(written_authorization):
                offer_url(_OIDC_SECRET_PAGE_MESSAGE, config_link)
            return ModifiedDataAppOutput(
                response=response,
                change_summary='\n'.join(note for note in (folder_hint, oidc_hint) if note) or None,
                data_app=DataAppSummary.model_validate(data_app.model_dump()),
                links=links,
                oidc_callback_url=_oidc_callback_url(client, data_app_pre.data_app_id, updated_config),
                oidc_setup_required=_oidc_setup_required(written_authorization),
            )
        except Exception:
            LOG.exception(
                'Data app configuration %s was updated (version %s) but building the response failed; '
                'returning a partial success so the change is not retried.',
                configuration_id,
                new_version or '?',
            )
            return _partial_update_output(
                data_app_pre=data_app_pre,
                links_manager=links_manager,
                configuration_id=configuration_id,
                name=name,
                new_version=new_version,
            )
    else:
        # Create new data app. As with the update path, `create_data_app` is the committing write; the
        # metadata/folder/links steps after it are best-effort and must not fail the tool once the app exists,
        # so a failed retry cannot create a duplicate app (AJDA-2852).
        config = _build_data_app_config(name, source_code, packages, authentication_type, secrets, sql_dialect)
        config = await client.encryption_client.encrypt(
            config, component_id=DATA_APP_COMPONENT_ID, project_id=project_id
        )
        validated_config = DataAppConfig.model_validate(config)
        data_app_resp = await client.data_science_client.create_data_app(
            name, description, configuration=validated_config
        )
        # --- app created past this point; response building is strictly best-effort ---
        try:
            await set_cfg_creation_metadata(
                client=client,
                component_id=DATA_APP_COMPONENT_ID,
                configuration_id=data_app_resp.config_id,
            )
            folder_hint = await apply_configuration_folder(
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
                data_app=DataAppSummary.from_api_response(
                    data_app_resp, await _config_version_or_blank(client, data_app_resp.config_id)
                ),
                links=links,
                oidc_callback_url=_oidc_callback_url(client, data_app_resp.id, config),
            )
        except Exception:
            LOG.exception(
                'Data app %s was created (configuration %s) but building the response failed; '
                'returning a partial success so creation is not retried.',
                data_app_resp.id,
                data_app_resp.config_id,
            )
            return _partial_create_output(
                data_app_resp=data_app_resp,
                links_manager=links_manager,
                validated_config=validated_config,
                name=name,
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
    folder: str | None = None,
    oidc: OidcAuthConfig | None = None,
) -> tuple[DataApp, JsonDict, dict | None]:
    secrets = _get_secrets(
        workspace_id=str(await workspace_manager.get_data_app_workspace_id()),
        branch_id=str(await workspace_manager.get_data_app_branch_id()),
    )
    data_app = await _fetch_data_app(client, configuration_id=configuration_id)
    existing_config = data_app.configuration
    updated_config = _update_existing_data_app_config(
        existing_config,
        name,
        source_code,
        packages,
        authentication_type,
        secrets,
        await workspace_manager.get_data_app_sql_dialect(),
        oidc,
    )
    updated_config = cast(
        JsonDict,
        await client.encryption_client.encrypt(
            updated_config, component_id=DATA_APP_COMPONENT_ID, project_id=await client.storage_client.project_id()
        ),
    )

    folder_preview = build_folder_preview({'folder': data_app.folder} if data_app.has_folder_field else {}, folder)

    return data_app, updated_config, folder_preview


def _partial_update_output(
    *,
    data_app_pre: DataApp,
    links_manager: ProjectLinksManager,
    configuration_id: str,
    name: str,
    new_version: str,
) -> ModifiedDataAppOutput:
    """Build a truthful partial-success response after a committed Streamlit update whose response building failed.

    The configuration write already landed, so this MUST NOT raise -- it is built entirely from the pre-update
    data app (fetched before the write) plus the new version from the update response. The ``change_summary``
    tells the agent the change IS applied and must not be retried, preventing the duplicate-write loop. The
    underlying exception is intentionally not surfaced to the caller (it is logged at the call site) -- the
    agent-facing message stays stable and free of internal detail.
    """
    try:
        summary = DataAppSummary.model_validate(data_app_pre.model_dump())
        summary.config_version = new_version or summary.config_version
    except Exception:
        # Defensive: this helper must never raise (the write already committed). SafeState/SafeType accept any
        # string, so building from the pre-update primitives cannot fail even if the schema is later tightened.
        summary = DataAppSummary(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            data_app_id=getattr(data_app_pre, 'data_app_id', '') or '',
            project_id=getattr(data_app_pre, 'project_id', '') or '',
            branch_id=getattr(data_app_pre, 'branch_id', '') or '',
            config_version=new_version or getattr(data_app_pre, 'config_version', '') or '',
            state=getattr(data_app_pre, 'state', 'unknown') or 'unknown',
            type=getattr(data_app_pre, 'type', 'streamlit') or 'streamlit',
        )
    try:
        links = links_manager.get_data_app_links(
            configuration_id=configuration_id,
            configuration_name=name or data_app_pre.name,
            deployment_link=data_app_pre.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app_pre.configuration.get('authorization') or {}),
        )
    except Exception:
        links = []
    # Mirror the success-path wording: the redeploy hint only applies to a deployed app. The pre-update
    # state is a faithful proxy here -- a config write does not change deployment state on its own.
    response = _update_response(data_app_pre)
    return ModifiedDataAppOutput(
        response=response,
        change_summary=(
            f'The configuration WAS updated (version {new_version or "unknown"}), but loading the full app '
            f'details failed, so this response is partial. Do NOT retry the update -- the change is already '
            f'applied. Call deploy_data_app to apply it to the running app.'
        ),
        data_app=summary,
        links=links,
    )


def _partial_create_output(
    *,
    data_app_resp: DataAppResponse,
    links_manager: ProjectLinksManager,
    validated_config: DataAppConfig,
    name: str,
) -> ModifiedDataAppOutput:
    """Build a truthful partial-success response after a committed Streamlit create whose response building failed.

    The app already exists, so this MUST NOT raise -- it is built from the create response the API already
    returned. The ``change_summary`` tells the agent the app IS created and must not be retried, preventing a
    duplicate app. The underlying exception is intentionally not surfaced to the caller (it is logged at the
    call site) -- the agent-facing message stays stable and free of internal detail.
    """
    try:
        summary = DataAppSummary.from_api_response(data_app_resp, '')
    except Exception:
        # Defensive: this helper must never raise (the app already exists). SafeState/SafeType accept any
        # string, so building from the create-response primitives cannot fail even if the schema is tightened.
        summary = DataAppSummary(
            component_id=getattr(data_app_resp, 'component_id', DATA_APP_COMPONENT_ID) or DATA_APP_COMPONENT_ID,
            configuration_id=getattr(data_app_resp, 'config_id', '') or '',
            data_app_id=getattr(data_app_resp, 'id', '') or '',
            project_id=getattr(data_app_resp, 'project_id', '') or '',
            branch_id=getattr(data_app_resp, 'branch_id', '') or '',
            config_version=getattr(data_app_resp, 'config_version', '') or '',
            state=getattr(data_app_resp, 'state', 'unknown') or 'unknown',
            type=getattr(data_app_resp, 'type', 'streamlit') or 'streamlit',
        )
    try:
        links = links_manager.get_data_app_links(
            configuration_id=data_app_resp.config_id,
            configuration_name=name,
            deployment_link=data_app_resp.url,
            uses_basic_authentication=_uses_basic_authentication(validated_config.authorization),
        )
    except Exception:
        links = []
    return ModifiedDataAppOutput(
        response='created',
        change_summary=(
            f'The data app WAS created (configuration {data_app_resp.config_id}), but building the full response '
            f'failed, so this response is partial. Do NOT retry creation -- it would create a duplicate. '
            f'Call deploy_data_app to start the app.'
        ),
        data_app=summary,
        links=links,
    )


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
        str | None,
        Field(
            description=(
                'URL-safe slug for the data app (used as a subdomain). Optional on create — when omitted '
                'it is auto-derived from `name`. An explicit slug must be at '
                'most 63 characters (DNS-label max; the UI URL-prefix limit is 50). On update it moves a '
                'prod app to a new URL — pass it only after the user approved that URL (see "Slug on update").'
            ),
        ),
    ] = None,
    branch: Annotated[
        str | None,
        Field(
            description=(
                'Git branch of the data app, written to `parameters.dataApp.git.branch`. Two uses:\n'
                '- **On update** (with `configuration_id`): repoints an existing **external-git** app to '
                'a different branch (e.g. flip a repo-backed app from `main` to a feature branch for '
                'testing, then back). Only valid for external-git apps — a legacy draft, or an app bound to an '
                'external repository. Rejected for apps on a Keboola-managed git repo, whose branch is '
                'owned by the platform. On update `main` is allowed. Redeploy the app afterwards to serve '
                'the new branch.\n'
                '- **With `git_repository`** (create or update of a prod app): the branch of that repository '
                'the app deploys. Defaults to `main`.'
            ),
        ),
    ] = None,
    git_repository: Annotated[
        str | None,
        Field(
            description=(
                "HTTPS URL of a customer's GitHub repository (`https://github.com/<owner>/<repo>`, or a "
                '`*.ghe.com` host) to deploy a **prod** app from instead of a Keboola-managed repo. Requires '
                '`git_auth`. On create, the app gets no managed repo; on update, an app already bound to an '
                'external repository is rebound to this one. Rejected for apps on a Keboola-managed repo. '
                'No credential is stored.'
            ),
        ),
    ] = None,
    git_auth: Annotated[
        GitAuthType | None,
        Field(
            description=(
                'How the platform authenticates to `git_repository`; required together with it. Only '
                '"organization" is supported: a short-lived read-only token is minted through the Keboola '
                "organization's GitHub App connection, so no credential is stored in the configuration."
            ),
        ),
    ] = None,
    authentication_type: Annotated[
        AuthenticationType,
        Field(
            description=(
                'Authentication type. "no-auth" removes authentication completely (the app becomes '
                'public -- only use it when the user explicitly asks for a public app; it is rejected '
                'on drafts, which inherit the prod app\'s data access), "basic-auth" secures the data '
                'app via HTTP basic authentication, "oidc" sets up OIDC login (update of a prod app only, '
                'needs the `oidc` argument), and "default" means: on create, apply basic auth '
                '(safe default for new apps); on update, keep the existing authentication configuration '
                '(including OIDC setups configured outside the MCP). Never disable authentication to '
                'make a preview work -- the in-platform preview authenticates on top of the configured '
                'auth.'
            ),
        ),
    ] = 'default',
    oidc: Annotated[
        OidcAuthConfig | None,
        Field(
            description=(
                'OIDC settings. Required with authentication_type="oidc" and not allowed with any other '
                'type. Only for updating an existing prod app, not for create or drafts. It has no secret '
                'field: the user enters the client secret in the Keboola UI.'
            ),
            json_schema_extra=_type_object_on_ref,
        ),
    ] = None,
    auto_suspend_after_seconds: Annotated[
        int | None,
        Field(
            description=(
                'Number of seconds after which the running data app is automatically suspended. '
                'Leave unset (None) to keep the app\'s current value on update; on create it defaults '
                'to 900.'
            ),
        ),
    ] = None,
    storage: Annotated[
        dict[str, Any] | None,
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
    storage_access: Annotated[
        bool | None,
        Field(
            description=(
                'Whether the app gets read-only Storage access -- a workspace whose ID the platform '
                'injects as the WORKSPACE_ID environment variable, which the generated `query_data` '
                'helper needs. Distinct from `storage`, which declares table input/output mappings: '
                'this switch decides whether the app can reach Storage at all.\n'
                '- **On create**: leave unset (None) for the default, which is Storage access ON. '
                'Pass `False` only for an app that must not read Storage.\n'
                '- **On update**: leave unset (None) to keep whatever the app has -- an unrelated '
                'edit never grants or revokes Storage access. Pass `True` to turn it on (this is how '
                'you fix an existing app failing with `missing required env vars: WORKSPACE_ID`, with '
                'no UI step), or `False` to turn it off. Redeploy the app afterwards to apply it.\n'
                'Check `data_app.storage_access_enabled` in the response to confirm the result.\n'
                'The workspace is scoped to the **whole project**, not to the tables the app '
                'queries -- the app can read every table in the project Storage. Tell the user that '
                'when you turn it on, and especially when the app is `no-auth`, where anyone with '
                'the URL reaches project data through it. If the app must be limited to particular '
                'tables, that is an input/output mapping via `storage`, not this switch.\n'
                'On projects without the `data-apps-storage-workspace` feature this falls back to '
                'writing `parameters.dataApp.secrets.WORKSPACE_ID`, which is **deprecated** -- it pins '
                'the app to the shared MCP-managed workspace instead of a per-app one. Never write that '
                'secret by hand; use this argument, and pass the hint in `change_summary` on to the user.'
            ),
        ),
    ] = None,
    image_version: Annotated[
        str | None,
        Field(
            description=(
                'Backend image the app runs on (the UI\'s "Backend version"), written to '
                '`runtime.image.version`. An image tag from the platform catalog, which names its Python '
                'and Node.js versions (e.g. `<release>_python-3.13_node-24`). '
                '`get_data_apps(configuration_ids=[...])` lists the valid tags in `available_images` and '
                "the app's current one in `deployment_info.image`; an unknown tag is rejected with the "
                'list of valid ones.\n'
                '- Leave unset (None) to keep what the app has on update; on create the app follows the '
                'platform default.\n'
                '- Pass "" to drop a pin and follow the platform default again.\n'
                'Redeploy the app afterwards to apply it.'
            ),
        ),
    ] = None,
    folder: Annotated[
        str | None,
        Field(description=folder_field_description('data app', 'data apps')),
    ] = None,
    project_id: ProjectIdArg = None,
) -> ModifiedPythonJsDataAppOutput:
    """Creates or updates a python-js data app.

    Python-js is the default type for new data apps when you can run git: this tool only creates the
    configuration and the repository, and you deliver the source code by `git push`. Use
    `modify_streamlit_data_app` instead when you cannot run git or when the user asks for Streamlit explicitly.

    - A python-js app owns a managed git repository; `git_clone_url` comes from
      `create_python_js_data_app_git_credential`. MCP never runs git: clone, branch, push and merge are yours.
    - Source changes are previewed in a draft (`create_python_js_data_app_draft`) on a branch of that repository.
      `main` advances only by merging an approved branch; then `deploy_data_app` serves it from the prod app.
    - A legacy draft (`parameters.dataApp.isDraft=true`) is a separate configuration; this tool can still update it,
      but no longer creates one.

    ## Scenario D — Deploy a prod app from a customer's GitHub repository

    The code lives in the customer's own GitHub repository, not in a Keboola-managed one, and the
    platform clones it through the Keboola organization's GitHub App connection — no token is ever
    passed to or stored by the MCP.

    1. `modify_python_js_data_app(name=..., git_repository='https://github.com/<owner>/<repo>',
       git_auth='organization', branch='main')` → prod app with `parameters.dataApp.git`
       `{repository, branch, authType: 'organization', private: true}` and no managed repo.
       To move an app that is already bound to an external repository onto another repository,
       pass the same arguments with its `configuration_id` (rebind). An app on a Keboola-managed repo
       cannot be rebound — create a new app instead.
    2. Code changes go to that GitHub repository through the user's own GitHub access; the MCP mints
       no git credential for it (`create_python_js_data_app_git_credential` is rejected).
    3. `deploy_data_app(action='deploy', configuration_id=PROD)` — production mode only. Drafts,
       dev mode and the preview link are not supported for such apps yet.
    If the deploy fails because the organization's GitHub connection is missing, someone who is both a
    Keboola organization admin and an owner of the GitHub organization installs the Keboola GitHub App
    first, then connects the GitHub organization:
    https://help.keboola.com/management/organization/#connect-github (not a menu path).

    ## Argument rules

    - `branch` on **create** is only valid with `git_repository` (defaults to `'main'`). On **update** it
      repoints an existing **external-git** app's pinned branch.
    - `git_repository` and `git_auth` go together (Scenario D).
    - `slug` is optional on create (auto-derived from `name` when omitted). See "Slug on update" below.
    - The **update path** (passing `configuration_id`) is for changing `name`, `description`,
      `authentication_type`, `auto_suspend_after_seconds`, `storage`, `image_version`, a prod app's `slug`
      (see "Slug on update"), and an **external-git** app's `branch`. Source code changes go through git, not
      this tool. After a `branch` repoint, call `deploy_data_app` to serve the new branch.

    ## Authentication

    New apps default to HTTP basic authentication for safety. Pass `authentication_type='no-auth'`
    to expose publicly -- only when the user explicitly asks for a public app. `'no-auth'` is
    rejected on legacy drafts: a draft inherits the prod app's data access, so disabling its
    authentication would expose Storage-reading and Storage-writing endpoints publicly. Never
    disable authentication to make the preview work -- the preview link authenticates on top of the
    configured auth. On update,
    `authentication_type='default'` preserves the existing `authorization` block (including OIDC
    setups configured outside the MCP); `'basic-auth'` / `'no-auth'` overwrite it.

    `authentication_type='oidc'` (with the `oidc` argument) switches an existing prod app to OIDC. It is
    rejected on create and on drafts. The tool never takes a client secret: the user enters it in the Keboola UI.

    Set up OIDC in this order:
    1. Create the app with `'basic-auth'`. The output has `oidc_callback_url`.
    2. The user registers `oidc_callback_url` at the identity provider and gives you the `client_id`.
    3. Update the app with `authentication_type='oidc'` and `oidc` (`client_id`, `issuer_url`, and optionally
       `logout_url`, `allowed_roles`).
    4. The user enters the client secret in the app's Authentication section in the Keboola UI. If the client
       can open URLs, it is asked to open that page.
    5. Only if you use `allowed_roles`: update with `'oidc'` once more after the secret is saved.
    6. Call `deploy_data_app` last.
    If the output has `oidc_setup_required=true`, tell the user to follow `change_summary`. In `oidc`, null keeps
    the stored `logout_url` / `allowed_roles`; `''` or `[]` removes them. The slug stays fixed on OIDC apps.

    ## Slug constraint

    Must be DNS-label-safe (lowercase letters, digits, hyphens). Optional on create: when omitted it
    is auto-derived from `name` and capped at 50 characters (the data-app URL-prefix limit enforced
    by the UI). Pass an explicit slug to override; an explicit
    slug must be at most 63 characters (the DNS-label max), and note the UI's own URL-prefix limit is
    50, so an explicit slug of 51-63 characters may still be rejected at deploy time.

    ## Slug on update

    The slug is the app URL (`https://<slug>-<app id>.hub.<stack>`), so changing it moves the app.
    1. Until the prod app is first deployed, a rename moves a slug that follows the name (e.g.
       `new-app` for "New App") to the new name automatically — nobody has the URL yet.
    2. Once the app has been deployed, a rename keeps the slug. Change it only with an explicit
       `slug`, and only after the user approved the new URL.
    3. Legacy drafts never change their slug.
    After an explicit `slug` the new URL applies on the next `deploy_data_app` and the old URL stops
    working; tell the user both.
    """
    _validate_oidc_args(authentication_type, oidc, is_create=not configuration_id)
    _validate_git_repository_args(git_repository, git_auth)
    if configuration_id:
        if slug:
            _validate_explicit_slug(slug)
        # `branch` IS allowed on update — it repoints an external-git app's pinned branch
        # (validated against the app's git block below).
    else:
        if not slug:
            # `slug` is schema-optional, so the calling LLM often omits it on create.
            slug = _derive_slug_from_name(name)
        else:
            _validate_explicit_slug(slug)
        if branch is not None and not git_repository:
            raise ValueError('branch is only valid on create together with git_repository.')

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    validated_storage = _validate_data_app_storage(storage, configuration_id=configuration_id or None)
    if image_version:
        await _validate_image_version(client, image_version)

    # When the platform-managed workspace feature is off, the data app cannot rely on the
    # platform to inject WORKSPACE_ID; fall back to passing it via parameters.dataApp.secrets.
    has_storage_workspace = await client.has_feature(DATA_APPS_STORAGE_WORKSPACE_FEATURE)
    # Create keeps its historical default of Storage access ON; update only ever moves the flag
    # when explicitly asked to, so an unrelated edit (a rename, an auto-suspend change) can neither
    # grant nor revoke Storage access -- on either mechanism.
    wants_storage_access = storage_access is True if configuration_id else storage_access is not False
    legacy_workspace_id: str | None = None
    legacy_fallback_hint: str | None = None

    if configuration_id:
        # Update existing python-js data app; the repo is read only for the rebind and branch checks.
        data_app = await _fetch_data_app(
            client, configuration_id=configuration_id, with_repo=bool(git_repository or branch)
        )
        if _is_draft_config(data_app.configuration):
            _reject_no_auth_on_draft(authentication_type)
            _reject_oidc_on_draft(authentication_type)
        normalized_branch: str | None = None
        git_rebind: CodeDataAppConfig.Parameters.DataApp.Git | None = None
        if git_repository:
            _validate_git_rebind(data_app, configuration_id)
            git_rebind = _organization_git_block(git_repository, branch)
        elif branch:
            normalized_branch = _validate_branch_update(branch, data_app, configuration_id)
        oidc_app = authentication_type == 'oidc' or (
            authentication_type == 'default'
            and _oidc_provider(data_app.configuration.get('authorization') or {}) is not None
        )
        new_slug = _resolve_slug_update(data_app, name=name, explicit_slug=slug, oidc_app=oidc_app)
        if (
            not has_storage_workspace
            and wants_storage_access
            and not _config_legacy_workspace_id(data_app.configuration)
        ):
            # Only resolve once we know the app has no id of its own to keep. Resolving provisions
            # the shared MCP-managed workspace when the project has none, and raises outright in a
            # workspace-pinned session -- neither of which should happen when the existing id is
            # about to be preserved verbatim.
            legacy_workspace_id, legacy_fallback_hint = await _resolve_legacy_workspace_fallback(
                ctx, client, configuration_id=configuration_id
            )
        updated_config = _update_existing_code_data_app_config(
            existing_config=data_app.configuration,
            auto_suspend_after_seconds=auto_suspend_after_seconds,
            authentication_type=authentication_type,
            oidc=oidc,
            storage_access=storage_access,
            has_storage_workspace=has_storage_workspace,
            legacy_workspace_id=legacy_workspace_id,
            storage=validated_storage,
            branch=normalized_branch,
            git=git_rebind,
            slug=new_slug,
            image_version=image_version,
        )
        await client.storage_client.configuration_update(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration=updated_config,
            change_description=change_description or 'Update python-js data app',
            updated_name=name or data_app.name,
            updated_description=description or data_app.description,
        )
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, with_repo=True)
        await set_cfg_update_metadata(
            client=client,
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            configuration_version=int(data_app.config_version),
        )
        # Without `folder`, an update keeps the folder: the hint lists every data app, so only a create gives it.
        folder_hint = (
            await apply_configuration_folder(
                client, DATA_APP_COMPONENT_ID, configuration_id, folder, 'data apps', 'modify_python_js_data_app'
            )
            if folder is not None
            else None
        )
        written_authorization = updated_config.get('authorization') or {}
        oidc_callback_url = _oidc_callback_url(client, data_app.data_app_id, updated_config)
        # Without an explicit `slug`, a new slug is a never-deployed app's slug following its name.
        if new_slug is None:
            slug_hint = None
            # On a retry after URL elicitation the first round already wrote the explicit slug, so
            # nothing moves now, but the agent has not yet seen the IdP warning.
            if slug and oidc_app:
                slug_hint = (
                    f"The URL slug is '{slug}'. The app uses OIDC login: register the callback URL "
                    f'{oidc_callback_url} at the identity provider before you redeploy, or every login fails.'
                )
        elif slug:
            slug_hint = (
                f"Changed the URL slug to '{new_slug}'. The app keeps its current URL until it is redeployed "
                '(deploy_data_app); from then on it is served under the new slug and the old URL stops working. '
                'Tell the user both.'
            )
            if oidc_app:
                slug_hint += (
                    f' The app uses OIDC login: register the new callback URL {oidc_callback_url} at the '
                    'identity provider before you redeploy, or every login fails.'
                )
        else:
            slug_hint = (
                f"The URL slug followed the new name to '{new_slug}': the app has never been deployed, so its "
                'URL still tracks its name. Nothing to do or undo.'
            )
        branch_hint = (
            f"Repointed the external-git branch to '{normalized_branch}'. Redeploy the app "
            '(deploy_data_app) to serve the new branch.'
            if normalized_branch is not None
            else None
        )
        if git_rebind is not None:
            branch_hint = _organization_git_hint(git_rebind, rebound=True)
        storage_access_hint = (
            f'Storage access {"enabled" if storage_access else "disabled"}. Redeploy the app '
            '(deploy_data_app) to apply it to the running app.'
            if storage_access is not None
            else None
        )
        config_link = links_manager.get_data_app_config_link(configuration_id, name or data_app.name, False).url
        oidc_hint = _oidc_change_summary(
            written_authorization,
            config_link=config_link,
            roles_requested=bool(oidc and oidc.allowed_roles),
        )
        if _oidc_setup_required(written_authorization):
            offer_url(_OIDC_SECRET_PAGE_MESSAGE, config_link)
        change_summary = (
            '\n'.join(
                note
                for note in (
                    folder_hint,
                    slug_hint,
                    branch_hint,
                    storage_access_hint,
                    _image_version_hint(image_version),
                    legacy_fallback_hint,
                    oidc_hint,
                )
                if note
            )
            or None
        )
        repo_url = data_app.repo_url
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=name or data_app.name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
        )
        response = _update_response(data_app)
        data_app_summary = DataAppSummary.model_validate(data_app.model_dump())
        data_app_summary.repo_url = repo_url
        # Read it back off the config we just wrote rather than the re-fetched app, so the answer
        # holds even when the fetch lags behind the write.
        data_app_summary.storage_access_enabled = _code_config_has_storage_access(updated_config)
        return ModifiedPythonJsDataAppOutput(
            response=response,
            change_summary=change_summary,
            data_app=data_app_summary,
            repo_url=repo_url,
            links=links,
            oidc_callback_url=oidc_callback_url,
            oidc_setup_required=_oidc_setup_required(written_authorization),
        )
    else:
        # Create a new python-js prod app: a managed repo, or a customer's repository with `git_repository`.
        # Narrowed by the validation block at the top of this function.
        assert slug is not None
        if not has_storage_workspace and wants_storage_access:
            legacy_workspace_id, legacy_fallback_hint = await _resolve_legacy_workspace_fallback(
                ctx, client, configuration_id=None
            )
        # On create, anything but an explicit 'no-auth' becomes 'basic-auth' (fail closed), to match
        # modify_streamlit_data_app.
        uses_basic_auth = authentication_type != 'no-auth'
        authorization_model = DataAppConfig.Authorization.model_validate(
            _build_authorization('basic-auth' if uses_basic_auth else 'no-auth')
        )

        git_block: CodeDataAppConfig.Parameters.DataApp.Git | None = None
        if git_repository:
            git_block = _organization_git_block(git_repository, branch)

        config = CodeDataAppConfig(
            parameters=CodeDataAppConfig.Parameters(
                auto_suspend_after_seconds=(
                    auto_suspend_after_seconds if auto_suspend_after_seconds is not None else 900
                ),
                data_app=CodeDataAppConfig.Parameters.DataApp(
                    slug=slug,
                    secrets={SECRET_WORKSPACE_ID: legacy_workspace_id} if legacy_workspace_id else None,
                    git=git_block,
                ),
            ),
            runtime=_new_code_data_app_runtime(
                workspace_enabled=has_storage_workspace and wants_storage_access, image_version=image_version
            ),
            authorization=authorization_model,
            # An empty (or all-empty) storage block prunes to `{}`; omit it entirely rather than
            # persisting an empty object that the backend would store as `[]` (AI-3135).
            storage=validated_storage or None,
        )
        data_app_resp = await client.data_science_client.create_data_app(
            name=name,
            description=description,
            configuration=config,
            app_type='python-js',
            # An app on a customer's repository brings its own git binding; any other gets a managed repo.
            use_managed_git_repo=git_block is None,
        )
        if git_block is not None:
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
        folder_hint = await apply_configuration_folder(
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
        data_app_summary = DataAppSummary.from_api_response(
            data_app_resp, await _config_version_or_blank(client, data_app_resp.config_id)
        )
        data_app_summary.repo_url = repo_url
        data_app_summary.storage_access_enabled = wants_storage_access
        app_branch = git_block.branch if git_block is not None else None
        data_app_summary.git_branch = app_branch
        data_app_summary.git_auth_type = git_block.auth_type if git_block is not None else None
        organization_hint = (
            _organization_git_hint(git_block, rebound=False) if git_block is not None and git_block.auth_type else None
        )
        change_summary = (
            '\n'.join(note for note in (folder_hint, organization_hint, legacy_fallback_hint) if note) or None
        )
        return ModifiedPythonJsDataAppOutput(
            response='created',
            change_summary=change_summary,
            data_app=data_app_summary,
            repo_url=repo_url,
            branch=app_branch,
            links=links,
            oidc_callback_url=_oidc_callback_url(
                client, data_app_resp.id, config.model_dump(by_alias=True, exclude_none=True)
            ),
        )


@tool_errors()
async def create_python_js_data_app_git_credential(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='Storage configuration ID of the python-js data app.')],
    project_id: ProjectIdArg = None,
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
       to enable git access.

    2. **Recovery when the cached token is gone / continuing an unfinished draft** — e.g., a fresh
       sandbox continuing yesterday's work, with the previous sandbox's filesystem wiped. The
       cached `git_clone_url` is lost; the configuration ID for the prod app is all you have.
       Call this tool with the **prod app's** `configuration_id` to mint a fresh token (drafts
       have no managed repo, so always mint against prod). Existing credentials remain valid, so
       other clients are not disrupted.

    ## Constraints
    - Only python-js prod data apps have a managed git repo. Streamlit apps reject the call with
      a clear error, and so do apps on a customer GitHub repository (`git_auth_type='organization'`).
    - Permissions are always `readWrite` — the LLM virtually always needs push access. The
      data-science API supports read-only credentials, but the tool does not expose that knob;
      revisit once a real use case appears.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    data_app = await _fetch_data_app(client, configuration_id=configuration_id)
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
    if _uses_organization_git_auth(data_app.configuration):
        raise ValueError(
            f'Configuration "{configuration_id}" clones a customer GitHub repository through the Keboola '
            "organization's GitHub connection. It has no managed git repo, and the MCP cannot mint a git "
            "credential for it: push to that repository with the user's own GitHub access."
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


def _prune_empty_storage_objects(value: Any) -> Any:
    """Recursively drop empty-object (``{}``) values from a data-app ``storage`` block.

    The Keboola backend (PHP/SAPI) cannot tell an empty JSON object from an empty array and
    serializes ``{}`` as ``[]``. The UI's Input/Output Mapping editor (Writable Tables) then
    silently refuses to add or save entries when ``storage`` — or one of its ``input``/``output``
    containers — is an array instead of an object (AI-3135). We therefore never persist an empty
    object inside the storage block; the platform recreates a correctly shaped one when the user
    adds the first mapping. Empty *arrays* (e.g. the canonical ``{"output": {"tables": []}}``) are
    intentionally preserved.
    """
    if isinstance(value, dict):
        pruned: dict[str, Any] = {}
        for key, sub_value in value.items():
            pruned_sub = _prune_empty_storage_objects(sub_value)
            if isinstance(pruned_sub, dict) and not pruned_sub:
                continue
            pruned[key] = pruned_sub
        return pruned
    if isinstance(value, list):
        return [_prune_empty_storage_objects(item) for item in value]
    return value


def _normalize_config_storage(config: dict[str, Any]) -> None:
    """Normalize the ``storage`` block of a data-app config dict in place before persisting.

    Drops empty mapping containers and removes the ``storage`` key entirely when it carries no
    mappings (or is a non-object leftover such as ``[]``), so the broken array shape that breaks
    the Writable Tables editor is never written or left behind (AI-3135).
    """
    if 'storage' not in config:
        return
    storage = config['storage']
    pruned = _prune_empty_storage_objects(storage) if isinstance(storage, dict) else None
    if pruned:
        config['storage'] = pruned
    else:
        config.pop('storage', None)


def _validate_data_app_storage(
    storage: dict[str, Any] | None,
    *,
    configuration_id: str | None = None,
) -> dict[str, Any] | None:
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
    # Strip empty mapping containers so we never persist an empty object that the backend would
    # serialize as `[]`, which silently breaks the Writable Tables editor (AI-3135). An all-empty
    # block prunes down to `{}` (treated as a wipe by the callers).
    return cast(dict[str, Any], _prune_empty_storage_objects(validated['storage']))


def _config_legacy_workspace_id(config: Mapping[str, Any]) -> str | None:
    """The `parameters.dataApp.secrets.WORKSPACE_ID` entry of a data app config, if it has one."""
    secrets = ((config.get('parameters') or {}).get('dataApp') or {}).get('secrets') or {}
    workspace_id = secrets.get(SECRET_WORKSPACE_ID)
    return str(workspace_id) if workspace_id else None


async def _resolve_legacy_workspace_fallback(
    ctx: Context, client: KeboolaClient, *, configuration_id: str | None
) -> tuple[str, str]:
    """Resolve the shared workspace id for the deprecated secrets fallback, and the hint that goes
    with it.

    Call this only when the id is actually going to be written. Resolving it provisions the shared
    MCP-managed workspace when the project has none yet, and raises outright in a workspace-pinned
    session (see `WorkspaceManager._get_managed_workspace`) -- so a speculative call can both create
    a workspace nothing will use and fail an update that needed nothing from it.
    """
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    workspace_id = str(await workspace_manager.get_data_app_workspace_id())
    hint = (
        'Storage access is wired through the deprecated '
        '`parameters.dataApp.secrets.WORKSPACE_ID` fallback, because this project does not have '
        f'the `{DATA_APPS_STORAGE_WORKSPACE_FEATURE}` feature. The fallback pins the app to the '
        'shared MCP-managed workspace rather than one provisioned per app. Ask Keboola support '
        f'to enable `{DATA_APPS_STORAGE_WORKSPACE_FEATURE}` on this project to move to '
        'platform-managed per-app workspaces.'
    )
    # Logged so the remaining exposure is measurable per project -- the fallback can only be
    # retired once we know who still depends on it. Worth one extra `tokens/verify` on a
    # deprecated path that is already doing a workspace lookup.
    # Deliberately logs no workspace id: `project_id` + `configuration_id` are what the rollout
    # needs to count, the id is recoverable from the config, and keeping values that reach a
    # `secrets` map out of the logs avoids clear-text-logging findings entirely.
    project_id = await client.storage_client.project_id()
    LOG.warning(
        f'Data app Storage access is using the deprecated WORKSPACE_ID secret fallback: '
        f'project_id={project_id}, configuration_id={configuration_id or "<new>"}. '
        f'The {DATA_APPS_STORAGE_WORKSPACE_FEATURE} feature is not enabled on this project.'
    )
    return workspace_id, hint


def _code_config_has_storage_access(config: Mapping[str, Any]) -> bool:
    """Whether a python-js data app config grants read-only Storage access.

    Either marker counts, regardless of the project's feature state. `parameters.dataApp.secrets`
    reach the app as environment variables whatever the feature does, so an app carrying a legacy
    `WORKSPACE_ID` secret has Storage access even on a feature-enabled project whose
    `runtime.workspace.enabled` is false. Reporting the union is the safe direction to err: telling
    a caller Storage access is off while the app can still read Storage is the dangerous answer.
    """
    workspace = (config.get('runtime') or {}).get('workspace') or {}
    return bool(workspace.get('enabled')) or _config_legacy_workspace_id(config) is not None


def _update_existing_code_data_app_config(
    existing_config: Mapping[str, Any],
    auto_suspend_after_seconds: int | None = None,
    authentication_type: AuthenticationType = 'default',
    storage_access: bool | None = None,
    has_storage_workspace: bool = True,
    legacy_workspace_id: str | None = None,
    storage: dict[str, Any] | None = None,
    branch: str | None = None,
    git: CodeDataAppConfig.Parameters.DataApp.Git | None = None,
    slug: str | None = None,
    image_version: str | None = None,
    oidc: OidcAuthConfig | None = None,
) -> dict[str, Any]:
    """Apply requested updates to the existing python-js data app storage configuration.

    `auto_suspend_after_seconds` rewrites `parameters.autoSuspendAfterSeconds` (None leaves it
    untouched, so a rename keeps the app's suspend timeout).
    `slug` rewrites `parameters.dataApp.slug` (None leaves it untouched); `_resolve_slug_update`
    decides whether it may change.
    `image_version` pins `runtime.image.version`; '' drops the pin (both it and the legacy
    `parameters.imageVersion`) so the app follows the platform default; None leaves the stored pin as is.
    `authentication_type='default'` preserves the existing `authorization` block (including OIDC
    setups configured outside the MCP); 'no-auth' / 'basic-auth' overwrite it; 'oidc' rewrites the
    OIDC provider from `oidc` and keeps the stored client secret.
    `storage_access` moves the app's read-only Storage access, through whichever mechanism the
    project supports: `runtime.workspace.enabled` when `has_storage_workspace` is true, and the
    legacy `parameters.dataApp.secrets.WORKSPACE_ID` entry (taken from `legacy_workspace_id`) when
    it is false. `None` is the no-op: the stored config keeps whatever it has, so an update that
    only renames an app never grants or revokes Storage access. Any other pre-existing
    secrets are preserved verbatim by the deepcopy either way.
    `storage` replaces the entire `storage` block when provided (None preserves the existing one;
    an empty dict — or one that prunes down to nothing — is an explicit wipe that removes the
    `storage` key entirely). Whatever storage ends up in the config is normalized so empty mapping
    containers never persist as `[]` and break the Writable Tables editor (AI-3135).
    `branch` repoints the external-git binding's `parameters.dataApp.git.branch` (None leaves it
    untouched). Only the `branch` field is rewritten — the encrypted `#password`, `repository`, and
    `username` are preserved verbatim via the deepcopy, so no re-encryption is needed. The caller
    must have verified the app is external-git; if the git block is missing this raises, so the
    invariant holds even when the helper is called directly.
    `git` replaces the whole `parameters.dataApp.git` block (a rebind onto another external repository),
    dropping any stored credentials with it; pass `branch=None` alongside it. The caller must have
    verified the app is not on a Keboola-managed repo.
    """
    new_config = cast(dict[str, Any], copy.deepcopy(existing_config))
    new_config.setdefault('parameters', {})
    if auto_suspend_after_seconds is not None:
        new_config['parameters']['autoSuspendAfterSeconds'] = auto_suspend_after_seconds
    if authentication_type != 'default':
        new_config['authorization'] = _build_authorization(
            authentication_type, oidc, existing_config.get('authorization')
        )
    if branch is not None:
        data_app_block = new_config['parameters'].get('dataApp')
        git_block = data_app_block.get('git') if isinstance(data_app_block, dict) else None
        if not isinstance(git_block, dict):
            raise ValueError(
                'Cannot set branch: the configuration has no parameters.dataApp.git block '
                '(not an external-git data app).'
            )
        git_block['branch'] = branch
    if git is not None:
        new_config['parameters'].setdefault('dataApp', {})['git'] = git.model_dump(by_alias=True, exclude_none=True)
    if slug is not None:
        new_config['parameters'].setdefault('dataApp', {})['slug'] = slug
    if storage_access is not None:
        if has_storage_workspace:
            # Write the flag either way rather than dropping the block on disable: an explicit
            # `enabled: false` records the decision, where an absent block is indistinguishable
            # from an app that predates the feature.
            new_config.setdefault('runtime', {})['workspace'] = {'enabled': storage_access}
        data_app = new_config['parameters'].setdefault('dataApp', {})
        updated_secrets = dict(data_app.get('secrets') or {})
        if not storage_access:
            # Strip the legacy secret whatever the project's feature state. `secrets` reach the app
            # as environment variables regardless of `runtime.workspace`, so leaving a stale entry
            # behind would keep WORKSPACE_ID flowing into an app we just reported as having no
            # Storage access -- an app migrated onto the feature still carries one.
            updated_secrets.pop(SECRET_WORKSPACE_ID, None)
        elif not has_storage_workspace and legacy_workspace_id is not None:
            # Don't overwrite an id the app is already bound to -- it may be a workspace the user
            # wired up outside the MCP.
            updated_secrets.setdefault(SECRET_WORKSPACE_ID, legacy_workspace_id)
        if updated_secrets:
            data_app['secrets'] = updated_secrets
        else:
            # Don't leave an empty `secrets` object behind when the disable emptied it.
            data_app.pop('secrets', None)
    if image_version is not None:
        new_config['parameters'].pop('imageVersion', None)
        if image_version:
            new_config.setdefault('runtime', {})['image'] = {'version': image_version}
        elif runtime := new_config.get('runtime'):
            runtime.pop('image', None)
            if not runtime:
                new_config.pop('runtime')
    if storage is not None:
        new_config['storage'] = storage
    _normalize_config_storage(new_config)
    return new_config


@tool_errors()
async def get_data_apps(
    ctx: Context,
    configuration_ids: Annotated[Sequence[str], Field(description='The IDs of the data app configurations.')] = (),
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
    - `deployment_info.last_run` carries the outcome of the most recent deployment attempt. For an app
      that fails to start, check its `failure_reason`/`failure_message` FIRST — they cover setup-phase
      failures (e.g. invalid secrets, git clone errors, failing setup scripts) that happen before the
      container starts and therefore never appear in the regular logs.
    - `repo_url` (managed git repo URL for python-js apps) is ONLY populated on the detail path
      (when `configuration_ids` is provided). The inventory list always returns `repo_url=None`,
      even for python-js apps with a managed repo — to retrieve the URL, call this tool again
      with the target `configuration_ids`.
    - For a python-js **prod** app the detail also lists its `drafts` (with `draft_id`, `git_branch`, `url`,
      `run_state`) and its `legacy_drafts` (separate draft configurations).
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if configuration_ids:
        # Get details of the data apps by their configuration IDs using 10 parallel requests at a time to not overload
        # the API
        async def fetch_data_app_detail(configuration_id: str) -> DataApp | str:
            return await _fetch_data_app_details_task(client, links_manager, configuration_id)

        data_app_details, images = await asyncio.gather(
            process_concurrently(configuration_ids, fetch_data_app_detail, max_concurrency=10),
            _fetch_python_js_images(client),
        )
        found_data_apps: list[DataApp] = [
            dap.with_image(images) for dap in data_app_details if isinstance(dap, DataApp)
        ]
        not_found_ids: list[str] = [dap for dap in data_app_details if isinstance(dap, str)]
        if not_found_ids:
            LOG.error(f'Could not find Data Apps Configurations for IDs: {not_found_ids}')
        has_python_js = any(dap.type == 'python-js' for dap in found_data_apps)
        return GetDataAppsOutput(data_apps=found_data_apps, available_images=images if has_python_js and images else [])
    else:
        # List all data apps in the project
        data_apps, configs = await asyncio.gather(
            client.data_science_client.list_data_apps(limit=limit, offset=offset),
            client.storage_client.configuration_list(DATA_APP_COMPONENT_ID),
        )
        # The /v2 record carries no config version; one Storage listing has them all.
        versions = {str(cfg.get('id')): str(cfg.get('version', '')) for cfg in configs}
        links = [links_manager.get_data_app_dashboard_link()]
        return GetDataAppsOutput(
            data_apps=[
                DataAppSummary.from_api_response(data_app, versions.get(data_app.config_id, ''))
                for data_app in data_apps
                if data_app.component_id == DATA_APP_COMPONENT_ID
            ],
            links=links,
        )


@tool_errors()
async def deploy_data_app(
    ctx: Context,
    action: Annotated[Literal['deploy', 'stop'], Field(description='The action to perform.')],
    configuration_id: Annotated[str, Field(description='The ID of the data app configuration.')],
    mode: Annotated[
        Literal['dev', 'production'] | None,
        Field(
            description=(
                'Leave None (default). "dev" is rejected: an app never runs in dev mode by a deploy.\n'
                '<!-- directory:omit -->\n'
                'A draft from `create_python_js_data_app_draft` runs in dev mode by itself and needs no deploy.\n'
                '<!-- /directory:omit -->\n'
            ),
        ),
    ] = None,
    wait: Annotated[
        bool,
        Field(
            description=(
                'With action="deploy": wait (up to about 4 minutes) until the new deployment runs or fails. '
                'False returns at once with state "starting".'
            ),
        ),
    ] = True,
    project_id: ProjectIdArg = None,
) -> DeploymentDataAppOutput:
    """Deploys/redeploys a data app or stops a running data app in the Keboola environment asynchronously, given the
    action and the configuration ID.

    **MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge,
    branch-delete — is yours. This tool only triggers deploys against existing git state.

    ## Mode (python-js apps)
    - For prod redeploys (including after merging a draft's branch into `main`), use no `mode` —
      the prod app picks up the current `main`.
    <!-- directory:omit -->
    - A draft from `create_python_js_data_app_draft` needs no deploy: it serves its branch live.
    <!-- /directory:omit -->
    - `mode='dev'` is rejected, and so is a deploy of a legacy draft (`isDraft=true`): convert the legacy
      draft to a draft on its branch instead.
    - An app on a customer GitHub repository (`git_auth_type='organization'`) deploys in production
      mode only: a deploy without `mode` switches an app left in dev mode back to production.

    ## Configuration publishing
    - Every deploy **publishes the latest saved configuration version** (for both python-js and
      Streamlit apps), so configuration-only changes -- secrets, Storage access, git branch, size --
      go live together with the code. For python-js apps the code itself is always re-pulled from git.
    - `deployment_info.version` is the configuration version the app is actually running;
      `deployment_info.latest_config_version` is the latest saved one. If
      `deployment_info.has_unpublished_changes` is true, the configuration was changed after the
      deploy was triggered -- deploy again to publish it.

    ## Streamlit apps
    Streamlit apps have no managed git repo; deploy them without `mode`.

    ## Waiting
    - A deploy returns when the new deployment serves production (`state="running"`), when it failed to
      start (`change_summary` says so; `deployment_info.last_run` and `logs` say why), or after about 4
      minutes with `state="starting"`. Act on the result; do not sleep or poll before reading it.
    - `wait=False` returns at once with `state="starting"`.

    ## General considerations
    - `state` is `notDeployed` (never deployed), `starting`, `running`, `suspended` (asleep, wakes on the next
      visit) or `disabled` (failed to start; only a deploy brings it back).
    - To start or restart an app on a schedule or after other jobs, add it to a flow as a `keboola.data-app-control`
      task (see the flow tools) instead of calling this tool.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    if action == 'deploy':
        data_app = await _fetch_data_app(client, configuration_id=configuration_id)
        if _is_draft_config(data_app.configuration):
            raise ValueError(_legacy_draft_convert_message(data_app))
        if mode == 'dev':
            raise ValueError(
                f'Data app "{configuration_id}" cannot be deployed in dev mode. Deploy it without `mode`; to preview '
                'a change, create a draft with `create_python_js_data_app_draft`.'
            )
        if _uses_organization_git_auth(data_app.configuration):
            # An app keeps the mode of its last deploy, and the platform refuses organization auth in dev
            # mode: an external-repo app once deployed in dev mode would stay there on a deploy without
            # `mode`, so switch it to production explicitly.
            mode = 'production'
        # Always pin the latest config version (python-js included): without `configVersion` the data-app
        # service keeps the previously published version, so config-only changes (secrets, Storage access,
        # git branch, ...) would never go live.
        # The detail read gives the current (latest) version; pin it (python-js included): without
        # `configVersion` the data-app service keeps the previously published version, so config-only changes
        # (secrets, Storage access, git branch, ...) would never go live.
        deployment_id = await _trigger_deploy(client, data_app, data_app.config_version, mode)
        # --- deploy committed past this point: waiting and reporting must not turn it into an error ---
        waited = await _wait_for_deployment(client, data_app, deployment_id, _DEPLOY_WAIT_SECONDS) if wait else None
        data_app, logs, last_run, change_summary = await _deploy_outcome(client, data_app, waited)
        data_app = data_app.with_deployment_info(logs, last_run=last_run)
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
        )
        return DeploymentDataAppOutput(
            state=data_app.state,
            links=links,
            deployment_info=data_app.deployment_info,
            change_summary=change_summary,
        )
    elif action == 'stop':
        data_app = await _fetch_data_app(client, configuration_id=configuration_id)
        if data_app.state == 'starting':
            raise ValueError('Data app is currently "starting", could not be stopped at the moment.')
        if data_app.state != 'suspended':
            try:
                await client.data_science_client.suspend_data_app(data_app.data_app_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == HTTPStatus.CONFLICT:
                    raise ValueError(
                        f'Data app "{data_app.configuration_id}" has no running deployment to stop '
                        f'(state "{data_app.state}"). API error: {_api_error_text(e.response)}'
                    ) from e
                raise
        data_app = await _fetch_data_app(client, configuration_id=configuration_id)
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=None,
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
        )
        return DeploymentDataAppOutput(state=data_app.state, links=links, deployment_info=None)
    else:
        raise ValueError(f'Invalid action: {action}')


async def _trigger_deploy(
    client: KeboolaClient, data_app: DataApp, config_version: str, mode: Literal['production'] | None
) -> str | None:
    """Ask the data-science API to deploy `config_version`, mapping known refusals to actionable errors.

    :return: The new deployment's ID; None for a `mode` deploy, whose `/v1` endpoint returns none.
    """
    try:
        if mode is not None:
            await client.data_science_client.deploy_data_app_in_mode(data_app.data_app_id, config_version, 'prod')
            return None
        deployment = await client.data_science_client.deploy_data_app(data_app.data_app_id, config_version)
        return deployment.id
    except httpx.HTTPStatusError as e:
        api_error = _api_error_text(e.response)
        if mode is None and e.response.status_code == HTTPStatus.CONFLICT and _LEGACY_WORKLOAD in api_error:
            raise ValueError(_legacy_workload_message(data_app)) from e
        hint = _organization_git_error_hint(api_error) if _uses_organization_git_auth(data_app.configuration) else None
        if hint:
            # Keep the original classification: a 4xx stays a caller error, a 5xx an internal one.
            error_class = InvalidArgumentError if is_caller_error(e) else ValueError
            raise error_class(
                f'Deploying data app "{data_app.configuration_id}" failed with HTTP {e.response.status_code}: '
                f'{api_error} {hint}'
            ) from e
        raise


# A prod deploy is a cold image build (2-4 min). The budget stays under every hard limit on the hosted path
# (Kai's tool-call limit is 12 min, the platform blocks a promotion after 10 min); the 60 s idle timeout of
# the ingress never fires because a streamable-HTTP response pings every 15 s while the tool runs.
_DEPLOY_WAIT_SECONDS = 240
# (elapsed seconds, poll interval): poll every 2 s for the first 10 s, every 5 s up to a minute, then every 10 s.
_DEPLOY_POLL_STEPS = ((10, 2), (60, 5))
_DEPLOY_POLL_MAX_SECONDS = 10


def _deploy_poll_interval(elapsed: float) -> float:
    return next((interval for until, interval in _DEPLOY_POLL_STEPS if elapsed < until), _DEPLOY_POLL_MAX_SECONDS)


@dataclass
class _DeployWait:
    """What the deploy wait saw last; `record` is None when it never read the app."""

    record: DataAppResponse | None = None
    failed_run: AppRunResponse | None = None
    running: bool = False
    timed_out: bool = False


async def _wait_for_deployment(
    client: KeboolaClient, data_app: DataApp, deployment_id: str | None, budget: float
) -> _DeployWait:
    """Poll the app until the new deployment serves production, its run fails, or `budget` runs out.

    - Done when `production.actualDeploymentId` is the new deployment and the app is `running`: during a
      rollout of a running app, `state` stays `running` while the previous deployment still serves.
    - Failed when the newest run of the new deployment failed.
    - A `mode` deploy (`/v1`) returns no ID: the new one is the desired deployment once it differs from the
      one before the deploy. Without it (an app still on a /v1 workload) there is nothing to wait on.
    - Never raises: the deploy is committed, so a failed read only ends the wait.
    """
    app_id = data_app.data_app_id
    seen = _DeployWait()
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        async with asyncio.timeout(budget):
            if deployment_id is None:
                seen.record = await client.data_science_client.get_data_app(app_id)
                desired = seen.record.production.desired_deployment_id
                if not desired or desired == data_app.desired_deployment_id:
                    return seen
                deployment_id = desired
            while True:
                await asyncio.sleep(_deploy_poll_interval(loop.time() - started))
                seen.record, runs = await asyncio.gather(
                    client.data_science_client.get_data_app(app_id),
                    client.data_science_client.list_app_runs(app_id, limit=1, deployment_id=deployment_id),
                )
                if seen.record.production.actual_deployment_id == deployment_id and seen.record.state == 'running':
                    seen.running = True
                    return seen
                if runs and runs[0].state == 'failed':
                    seen.failed_run = runs[0]
                    return seen
    except TimeoutError:
        LOG.info(f'Deployment {deployment_id} of data app {app_id} is not running after {budget} s')
        seen.timed_out = True
    except Exception:
        LOG.exception(f'Failed to read data app {app_id} while waiting for deployment {deployment_id}')
    return seen


async def _deploy_outcome(
    client: KeboolaClient, data_app: DataApp, waited: _DeployWait | None
) -> tuple[DataApp, list[str], AppRunInfo | None, str | None]:
    """The app as the deploy leaves it, with logs and the failed run only when the deployment failed."""
    configuration_id = data_app.configuration_id
    record = waited.record if waited else None
    # The deploy published the latest version; the record says so once it was read.
    data_app.published_config_version = data_app.config_version
    if record is not None:
        data_app.deployment_url = record.url or data_app.deployment_url
        data_app.published_config_version = record.production.desired_config_version or data_app.config_version
    check = f'Check it with get_data_apps(configuration_ids=["{configuration_id}"]); do not deploy again.'
    if waited is not None and waited.running:
        data_app.state = 'running'
        return data_app, [], None, None
    if waited is not None and waited.failed_run is not None:
        # A failed rollout of a running app reads `starting` (the previous deployment still serves), never
        # `running`: the new code is not live.
        data_app.state = record.reported_state if record is not None else 'disabled'
        serving = (
            ' Production still serves the previous deployment.'
            if record is not None and record.production.actual_deployment_id
            else ''
        )
        return (
            data_app,
            await _fetch_logs(client, data_app.data_app_id),
            AppRunInfo.from_api_response(waited.failed_run),
            (
                'The new deployment failed to start; `deployment_info.last_run` and `logs` say why.'
                f'{serving} Fix the cause, then deploy again.'
            ),
        )
    data_app.state = 'starting'
    if waited is not None and waited.timed_out:
        return data_app, [], None, f'The app is still starting after {_DEPLOY_WAIT_SECONDS} s. {check}'
    return data_app, [], None, f'The deploy started. {check}'


def _organization_git_dev_mode_message(configuration_id: str) -> str:
    return (
        f'Data app "{configuration_id}" clones a customer GitHub repository through the Keboola organization\'s '
        'GitHub connection, and dev mode and the preview link are not supported for such apps yet. Deploy it in '
        f'production mode instead: `deploy_data_app` (action="deploy", configuration_id="{configuration_id}") '
        'without `mode`.'
    )


_DRAFT_URL_WAIT_SECONDS = 30
_DRAFT_URL_POLL_SECONDS = 2
_DRAFT_BRANCH_MISSING = 'does not exist'


@tool_errors()
async def create_python_js_data_app_draft(
    ctx: Context,
    configuration_id: Annotated[
        str, Field(description='Storage configuration ID of the python-js prod app the draft belongs to.')
    ],
    git_branch: Annotated[
        str,
        Field(description="Branch of the app's git repository the draft serves. It must already exist on the remote."),
    ],
    project_id: ProjectIdArg = None,
) -> CreatedDraftOutput:
    """Creates a draft of a python-js data app: its own workload on its own host, serving `git_branch`.

    - A push to `git_branch` reaches the running draft within seconds; it needs no deploy.
    - The draft runs in dev mode with a copy of the prod app's configuration from this moment. It never takes
      production traffic.
    - It suspends itself after 15 minutes without requests and wakes on the next one.
    - It outlives a prod deployment. Delete it with `delete_python_js_data_app_draft` when the work is done.
    - The tool waits until the draft has a `url`. `run_state` reaches `running` 1–2 minutes later; check it with
      `get_data_apps`.
    - Not idempotent: every call starts another draft. When a call fails or times out, look for the draft in
      `get_data_apps` before calling again.

    Errors:
    - branch "does not exist": push the branch first, then call again.
    - "not available on this Keboola stack": tell the user.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    data_app = await _fetch_data_app(client, configuration_id=configuration_id)
    if data_app.type != 'python-js':
        raise ValueError(f'Data app "{configuration_id}" is type "{data_app.type}"; only python-js apps have drafts.')
    if _is_draft_config(data_app.configuration):
        parent = _data_app_block(data_app.configuration).get('parentConfigurationId')
        if not isinstance(parent, str):
            raise ValueError(_legacy_draft_convert_message(data_app))
        raise ValueError(
            f'Configuration "{configuration_id}" is a legacy draft, not a prod app. Pass the prod app\'s '
            f'configuration_id ("{parent}").'
        )
    if _uses_organization_git_auth(data_app.configuration):
        raise ValueError(
            f'Data app "{configuration_id}" deploys from a customer GitHub repository, and drafts of such an app are '
            'not supported yet. Change the prod app itself and deploy it in production mode.'
        )
    branch = _normalize_branch_name(git_branch)
    try:
        draft = await client.data_science_client.create_draft(data_app.data_app_id, branch)
    except NEVER_SENT_ERRORS:
        raise  # never sent: no draft exists
    except httpx.HTTPStatusError as e:
        api_error = _api_error_text(e.response)
        if e.response.status_code == HTTPStatus.BAD_REQUEST and _DRAFT_BRANCH_MISSING in api_error:
            raise ValueError(f'{api_error} Push the branch first, then call this tool again.') from e
        if e.response.status_code == HTTPStatus.NOT_FOUND and _PREVIEW_LINK_NO_ROUTE in api_error:
            raise ValueError(_DRAFTS_NOT_AVAILABLE_MESSAGE) from e
        if e.response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            raise
        raise _draft_may_exist(configuration_id, branch) from e
    except (httpx.TransportError, ValueError) as e:
        # Unanswered, or answered with a body that does not read as a draft.
        raise _draft_may_exist(configuration_id, branch) from e

    try:
        # One budget for the whole wait, so a stalled read (timeouts, retries) cannot outlast it.
        async with asyncio.timeout(_DRAFT_URL_WAIT_SECONDS):
            while draft.url is None:
                await asyncio.sleep(_DRAFT_URL_POLL_SECONDS)
                draft = await client.data_science_client.get_draft(data_app.data_app_id, draft.id)
    except TimeoutError:
        LOG.info(f'Draft {draft.id} of data app {data_app.data_app_id} has no URL after {_DRAFT_URL_WAIT_SECONDS} s')
    except Exception:
        # The draft exists already: a failed read must not make the caller create another one.
        LOG.exception(f'Failed to read draft {draft.id} of data app {data_app.data_app_id} while waiting for its URL')

    change_summary = (
        None
        if draft.url
        else f'The draft has no URL yet. Read it from get_data_apps(configuration_ids=["{configuration_id}"]) '
        'in a moment; do not create another draft.'
    )
    links = links_manager.get_data_app_links(
        configuration_id=configuration_id,
        configuration_name=data_app.name,
        deployment_link=None,
        uses_basic_authentication=False,
    )
    return CreatedDraftOutput(
        configuration_id=configuration_id,
        draft=DraftInfo.from_response(draft),
        change_summary=change_summary,
        links=links,
    )


@tool_errors()
async def delete_python_js_data_app_draft(
    ctx: Context,
    configuration_id: Annotated[
        str,
        Field(
            description=(
                'With `draft_id`: Storage configuration ID of the prod app the draft belongs to. Without it: '
                'Storage configuration ID of the legacy draft to delete.'
            )
        ),
    ],
    draft_id: Annotated[
        str | None,
        Field(
            description='ID of the draft to delete (`drafts[].draft_id` in `get_data_apps`). None for a legacy draft.'
        ),
    ] = None,
    project_id: ProjectIdArg = None,
) -> DeletedDraftOutput:
    """Deletes a draft of a python-js data app. It never touches the prod app or the git repository.

    - With `draft_id`: stops and removes that draft. Its branch stays.
    - Without `draft_id`: deletes the legacy draft `configuration_id` (its data app and its configuration).
      Refuses prod apps and Streamlit apps.
    - It runs no git: deleting the branch on the remote is up to you.
    """
    if draft_id is not None:
        _validate_draft_id(draft_id)
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    data_app = await _fetch_data_app(client, configuration_id=configuration_id)
    if draft_id is not None:
        try:
            await client.data_science_client.delete_draft(data_app.data_app_id, draft_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != HTTPStatus.NOT_FOUND:
                raise
            raise ValueError(
                f'Data app "{configuration_id}" has no draft "{draft_id}"; it may be deleted already. '
                f'Check get_data_apps(configuration_ids=["{configuration_id}"]).'
            ) from e
        return DeletedDraftOutput(
            response='deleted',
            configuration_id=configuration_id,
            draft_id=draft_id,
            data_app_id=data_app.data_app_id,
            links=links_manager.get_data_app_links(
                configuration_id=configuration_id,
                configuration_name=data_app.name,
                deployment_link=None,
                uses_basic_authentication=False,
            ),
        )
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
    parent_configuration_id: str | None = parent_cfg_id if isinstance(parent_cfg_id, str) else None

    # DSAPI deletes the data app and moves its Storage config to the trash. Don't delete the config
    # via Storage API on top of that — deleting an already-trashed config purges it from the trash
    # (even with skip_trash=False), making the draft unrestorable.
    await client.data_science_client.delete_data_app(data_app.data_app_id)

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


@tool_errors()
async def get_data_app_preview_link(
    ctx: Context,
    configuration_id: Annotated[
        str,
        Field(
            description=(
                'With `draft_id`: Storage configuration ID of the prod app the draft belongs to. Without it: '
                'Storage configuration ID of a legacy draft running in dev mode.'
            )
        ),
    ],
    draft_id: Annotated[
        str | None,
        Field(description='ID of the draft to open (`drafts[].draft_id` in `get_data_apps`). None for a legacy draft.'),
    ] = None,
    project_id: ProjectIdArg = None,
) -> DataAppPreviewLinkOutput:
    """Creates a short-lived link that opens a python-js draft in your browser without the app's login.

    - With `draft_id`: the link opens that draft on its own host.
    - Without it: the link opens a legacy draft still running in dev mode.
      An app that is not in dev mode has no preview link.

    ## How to use the link
    - Open `url` in a real browser before `link_expires_at` (about 60 seconds after this call): a browser tool,
      or a headless browser CLI run from your shell. Do not fetch it with an HTTP client or curl: only a browser
      can turn it into a session.
    - After one successful open, the browser session keeps working and slides while you use the app, so reloads,
      navigation and later checks of the same app need no new link. Do not call this tool before every check.
    - The session belongs to this one app. It ends without notice: after about 4 hours without requests,
      12 hours after the link was opened at the latest, or at once when the app leaves dev mode.
    - Call this tool again only when the app shows its login page ("This app is password protected") or looks
      broken until reloaded, or when you did not open the previous `url` before its `link_expires_at` (the
      browser then shows "Preview link is invalid or expired").
    - Never type a password into the app's login page, and never ask the user for one.
    - Do not share `url`: do not show it to the user and do not put it into files, commits, messages, or any
      tool or any command other than the one that opens the browser. Anyone who opens it before it expires gets
      into the app.

    ## Errors
    - "has no URL yet" on a draft: it is still starting; call again in a few seconds.
    - "not running in dev mode" on a legacy draft: convert it to a draft on its branch, as the error says.
    - "is a production app": preview a draft of it instead; never switch a production app to dev mode.
    - "is not in dev mode" on another app type (e.g. Streamlit): it has no dev mode; tell the user and do not
      change its deploy mode.
    - "not configured on this Keboola stack": preview links are not available here; tell the user.
    """
    if draft_id is not None:
        _validate_draft_id(draft_id)
    client = KeboolaClient.from_state(ctx.session.state)
    try:
        data_app = await _fetch_data_app(client, configuration_id=configuration_id)
    except httpx.HTTPStatusError as e:
        api_error = _api_error_text(e.response)
        if e.response.status_code in (HTTPStatus.BAD_REQUEST, HTTPStatus.FORBIDDEN) and (
            _PREVIEW_LINK_NOT_AUTHORIZED in api_error
        ):
            raise _not_authorized_error(configuration_id, api_error) from e
        raise
    if draft_id is not None:
        try:
            link = await client.data_science_client.create_draft_preview_link(data_app.data_app_id, draft_id)
        except httpx.HTTPStatusError as e:
            mapped = _draft_preview_link_error(e, configuration_id, draft_id)
            if mapped is None:
                raise
            raise mapped from e
        return DataAppPreviewLinkOutput(url=link.url, link_expires_at=link.link_expires_at)
    try:
        link = await client.data_science_client.create_app_preview_link(data_app.data_app_id)
    except httpx.HTTPStatusError as e:
        mapped = _preview_link_error(e, data_app)
        if mapped is None:
            raise
        raise mapped from e
    return DataAppPreviewLinkOutput(url=link.url, link_expires_at=link.link_expires_at)


_LEGACY_WORKLOAD = 'still running as a /v1 workload'
_PREVIEW_LINK_NOT_DEV = 'is not in dev mode'
_PREVIEW_LINK_NO_URL = 'has no URL yet'
_PREVIEW_LINK_NOT_AUTHORIZED = 'is not authorized to manage app'
_PREVIEW_LINK_NOT_CONFIGURED = 'not configured'
_PREVIEW_LINK_NO_ROUTE = 'No route found'


def _api_error_text(response: httpx.Response) -> str:
    """The `error` field of a Keboola API error body, or the raw body text when it is not that JSON shape."""
    try:
        body = response.json()
    except ValueError:
        return response.text
    if isinstance(body, Mapping) and isinstance(body.get('error'), str):
        return body['error']
    return response.text


def _preview_link_error(exc: httpx.HTTPStatusError, data_app: DataApp) -> ValueError | None:
    """An agent-actionable error for a known preview-link refusal, or None to re-raise `exc` unchanged."""
    status = exc.response.status_code
    api_error = _api_error_text(exc.response)
    cfg = data_app.configuration_id
    if status == HTTPStatus.BAD_REQUEST and _PREVIEW_LINK_NOT_DEV in api_error:
        return ValueError(_not_in_dev_mode_message(data_app))
    if status == HTTPStatus.BAD_REQUEST and _PREVIEW_LINK_NO_URL in api_error:
        if _is_draft_config(data_app.configuration):
            return ValueError(_legacy_draft_convert_message(data_app))
        return ValueError(
            f'Data app "{cfg}" has no URL yet, so it has no preview link. Wait until `get_data_apps` reports it '
            f'running, then call this tool again.'
        )
    if status == HTTPStatus.FORBIDDEN or (
        status == HTTPStatus.BAD_REQUEST and _PREVIEW_LINK_NOT_AUTHORIZED in api_error
    ):
        return _not_authorized_error(cfg, api_error)
    if status == HTTPStatus.NOT_FOUND and _PREVIEW_LINK_NO_ROUTE in api_error:
        return ValueError(
            'Preview links are not available on this Keboola stack yet (the data-science service does not know '
            "the endpoint). Tell the user. Do not try the app's password login instead."
        )
    if status == HTTPStatus.NOT_FOUND:
        return ValueError(
            f'Data app "{cfg}" (data app ID "{data_app.data_app_id}") was not found by the data-science '
            f'service when minting the link. It may have just been deleted; check with `get_data_apps`. '
            f'API error: {api_error}'
        )
    if status == HTTPStatus.SERVICE_UNAVAILABLE and _PREVIEW_LINK_NOT_CONFIGURED in api_error:
        return ValueError(
            'Preview links are not configured on this Keboola stack, so the app cannot be opened through a '
            "preview link. Tell the user. Do not try the app's password login instead."
        )
    return None


def _validate_draft_id(draft_id: str) -> None:
    """A draft id is a UUID: anything else would turn the draft path into another endpoint."""
    try:
        valid = str(uuid.UUID(draft_id)) == draft_id.lower()
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(
            f'"{draft_id}" is not a draft id. Pass the `draft_id` of an entry in `drafts` from `get_data_apps`.'
        )


_DRAFTS_NOT_AVAILABLE_MESSAGE = (
    'Drafts are not available on this Keboola stack yet (the data-science service does not know the endpoint). '
    'Tell the user.'
)


def _draft_preview_link_error(exc: httpx.HTTPStatusError, configuration_id: str, draft_id: str) -> ValueError | None:
    """An agent-actionable error for a known draft preview-link refusal, or None to re-raise `exc` unchanged."""
    status = exc.response.status_code
    api_error = _api_error_text(exc.response)
    if status == HTTPStatus.BAD_REQUEST and _PREVIEW_LINK_NO_URL in api_error:
        return ValueError(
            f'Draft "{draft_id}" has no URL yet: it is still starting. Call this tool again in a few seconds.'
        )
    if status == HTTPStatus.NOT_FOUND and _PREVIEW_LINK_NO_ROUTE in api_error:
        return ValueError(_DRAFTS_NOT_AVAILABLE_MESSAGE)
    if status == HTTPStatus.NOT_FOUND:
        return ValueError(
            f'Data app "{configuration_id}" has no draft "{draft_id}". '
            f'Check get_data_apps(configuration_ids=["{configuration_id}"]).'
        )
    if status == HTTPStatus.FORBIDDEN or (
        status == HTTPStatus.BAD_REQUEST and _PREVIEW_LINK_NOT_AUTHORIZED in api_error
    ):
        return _not_authorized_error(configuration_id, api_error)
    return None


def _legacy_draft_convert_message(data_app: DataApp) -> str:
    """How to move on from a legacy draft, which is never deployed again: convert it to a draft on its branch."""
    block = _data_app_block(data_app.configuration)
    parent = block.get('parentConfigurationId')
    branch = _config_git_branch(data_app.configuration)
    if not isinstance(parent, str) or branch is None:
        return (
            f'Configuration "{data_app.configuration_id}" is a legacy draft with no prod app or branch recorded, so '
            'it cannot be converted. Tell the user; delete it (`delete_python_js_data_app_draft(configuration_id='
            f'"{data_app.configuration_id}")`) only once they agree: the delete cannot be undone.'
        )
    return (
        f'Configuration "{data_app.configuration_id}" is a legacy draft, which is no longer deployed. Convert it: '
        f'`create_python_js_data_app_draft(configuration_id="{parent}", git_branch="{branch}")`; the branch keeps '
        f'the work. Delete the legacy draft (`delete_python_js_data_app_draft(configuration_id='
        f'"{data_app.configuration_id}")`) only once its configuration holds nothing the prod app lacks, or the user '
        'agreed to lose it: the delete cannot be undone.'
    )


def _legacy_workload_message(data_app: DataApp) -> str:
    timeout = data_app.auto_suspend_after_seconds
    after = f'after {timeout} seconds without visitors' if timeout else 'once it has no visitors'
    return (
        f'Data app "{data_app.configuration_id}" still runs on the old platform and cannot be redeployed while it '
        f'runs. It falls asleep on its own {after}; deploy it again then. Tell the user; do not retry in a loop.'
    )


def _not_authorized_error(configuration_id: str, api_error: str) -> ValueError:
    return ValueError(
        f'The token cannot manage data app "{configuration_id}", so it cannot get a preview link for it. '
        f'Tell the user. API error: {api_error}'
    )


def _not_in_dev_mode_message(data_app: DataApp) -> str:
    cfg = data_app.configuration_id
    if data_app.type != 'python-js':
        return (
            f'Data app "{cfg}" ({data_app.type}) is not in dev mode, so it has no preview link. '
            f'Tell the user; do not change its deploy mode.'
        )
    if _uses_organization_git_auth(data_app.configuration):
        return _organization_git_dev_mode_message(cfg)
    if _is_draft_config(data_app.configuration):
        return _legacy_draft_convert_message(data_app)
    return (
        f'Data app "{cfg}" is a production app and is not in dev mode, so it has no preview link. Do not switch '
        f'it to dev mode. Preview a draft instead: find one with `get_data_apps` (configuration_ids=["{cfg}"]) '
        f'or create one with `create_python_js_data_app_draft`, then call this tool with its `draft_id`.'
    )


def _build_data_app_config(
    name: str,
    source_code: str,
    packages: list[str],
    authentication_type: AuthenticationType,
    secrets: dict[str, Any],
    sql_dialect: str,
) -> dict[str, Any]:
    packages = sorted(set(packages + _DEFAULT_PACKAGES))
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
    authorization = _build_authorization('no-auth' if authentication_type == 'no-auth' else 'basic-auth')
    return {'parameters': parameters, 'authorization': authorization}


def _update_existing_data_app_config(
    existing_config: Mapping[str, Any],
    name: str,
    source_code: str,
    packages: list[str],
    authentication_type: AuthenticationType,
    secrets: dict[str, Any],
    sql_dialect: str,
    oidc: OidcAuthConfig | None = None,
) -> dict[str, Any]:
    new_config = cast(dict[str, Any], copy.deepcopy(existing_config))
    if authentication_type != 'default':
        new_config['authorization'] = _build_authorization(
            authentication_type, oidc, existing_config.get('authorization')
        )
    # The OIDC callback URL contains the slug, so a rename must not move it.
    if _oidc_provider(new_config.get('authorization') or {}) is None:
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
    for key, value in secrets.items():
        if key not in updated_secrets:
            updated_secrets[key] = value

    new_config['parameters']['dataApp']['secrets'] = updated_secrets

    # Clean up any empty/array-shaped storage left behind by earlier MCP/CLI/KAI writes so the
    # Writable Tables editor keeps working after a re-save (AI-3135).
    _normalize_config_storage(new_config)
    return new_config


def _python_js_images(runtimes: Sequence[RuntimeResponse]) -> list[RuntimeImage]:
    """The python-js entries of the runtimes catalog, one per image tag.

    - The catalog can list one tag twice (the default and a "pinned" twin); the default entry wins.
    """
    images: dict[str, RuntimeImage] = {}
    for runtime in runtimes:
        if runtime.type != 'python-js' or not runtime.image_tag:
            continue
        if runtime.image_tag in images and not runtime.is_type_default:
            continue
        images[runtime.image_tag] = RuntimeImage(
            version=runtime.image_tag,
            description=runtime.description,
            is_default=runtime.is_type_default,
            end_of_life_date=runtime.end_of_life_date,
        )
    return list(images.values())


async def _fetch_python_js_images(client: KeboolaClient) -> list[RuntimeImage] | None:
    """The python-js image catalog for read paths; `None` when it cannot be fetched."""
    try:
        return _python_js_images(await client.data_science_client.list_runtimes())
    except Exception as exc:
        LOG.warning(f'Could not fetch the runtimes catalog: {exc}')
        return None


async def _fetch_app_images(client: KeboolaClient, data_app: DataApp) -> list[RuntimeImage] | None:
    """The python-js image catalog for a python-js app; `None` for other apps, without fetching it."""
    return await _fetch_python_js_images(client) if data_app.type == 'python-js' else None


async def _validate_image_version(client: KeboolaClient, image_version: str) -> None:
    """Raise unless `image_version` is a python-js image the platform catalog offers.

    - Fails closed when the catalog is unavailable: an unverified tag would only fail at deploy.
    """
    images = await _fetch_python_js_images(client)
    if images is None:
        raise ValueError(
            f'Could not verify image_version "{image_version}": the platform runtimes catalog is unavailable. '
            "Retry, or leave image_version unset to keep the app's current image."
        )
    if any(image.version == image_version for image in images):
        return
    offered = '; '.join(
        f'`{image.version}` ({image.description}{", default" if image.is_default else ""})' for image in images
    )
    raise ValueError(
        f'Unknown image_version "{image_version}". The platform offers these python-js images: '
        f'{offered or "none"}. Pass one of them, or "" to follow the platform default.'
    )


def _resolve_app_image(
    configuration: Mapping[str, Any], images: Sequence[RuntimeImage]
) -> tuple[RuntimeImage | None, bool]:
    """The image a python-js app's configuration selects, and whether it pins it.

    1. A pin the catalog offers → that catalog entry.
    2. A pin the catalog no longer offers → the bare tag, flagged in its description.
    3. No pin → the catalog default (`None` when the catalog has none).
    """
    runtime = configuration.get('runtime') or {}
    pinned = (runtime.get('image') or {}).get('version') or (configuration.get('parameters') or {}).get('imageVersion')
    if not pinned:
        return next((image for image in images if image.is_default), None), False
    match = next((image for image in images if image.version == pinned), None)
    return match or RuntimeImage(version=pinned, description='no longer offered by the platform'), True


def _new_code_data_app_runtime(
    *, workspace_enabled: bool, image_version: str | None
) -> CodeDataAppConfig.Runtime | None:
    """The `runtime` block of a new python-js app; `None` when it overrides nothing."""
    if not workspace_enabled and not image_version:
        return None
    return CodeDataAppConfig.Runtime(
        image=CodeDataAppConfig.Runtime.Image(version=image_version) if image_version else None,
        workspace=CodeDataAppConfig.Runtime.Workspace(enabled=True) if workspace_enabled else None,
    )


def _image_version_hint(image_version: str | None) -> str | None:
    """The `change_summary` note for an `image_version` update."""
    if image_version is None:
        return None
    target = f"image '{image_version}'" if image_version else 'the platform default image'
    return f'Backend set to {target}. Redeploy the app (deploy_data_app) to run on it.'


async def _fetch_data_app(client: KeboolaClient, *, configuration_id: str, with_repo: bool = False) -> DataApp:
    """
    Fetches a data app from the Storage API and the data-science API by its configuration ID.

    - `with_repo` also reads the git repo of a python-js prod app (`repo_url`, `is_managed_git_repo`);
      only the paths that return or check those fields pass it. A legacy draft has no repo of its own.
    """
    raw_configuration = await client.storage_client.configuration_detail(
        component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
    )
    api_config = ConfigurationAPIResponse.model_validate(raw_configuration | {'component_id': DATA_APP_COMPONENT_ID})
    data_app_id = cast(str, api_config.configuration['parameters']['id'])
    data_app_science = await client.data_science_client.get_data_app(data_app_id)
    if data_app_science.component_id != DATA_APP_COMPONENT_ID:
        raise ValueError(
            f'Data app tools only support {DATA_APP_COMPONENT_ID} component, but the data app '
            f'"{data_app_id}" has component_id "{data_app_science.component_id}".'
        )
    data_app = DataApp.from_api_responses(data_app_science, api_config)
    data_app.deployment_url = data_app.deployment_url or _app_url(client, data_app_science.id, api_config.configuration)
    if with_repo and data_app_science.type == 'python-js' and not _is_draft_config(api_config.configuration):
        try:
            repo_resp = await client.data_science_client.get_app_git_repo(data_app_science.id)
            data_app.repo_url = repo_resp.https_url
            data_app.is_managed_git_repo = repo_resp.is_managed_git_repo
        except Exception as exc:
            LOG.warning(f'Could not fetch git repo info for python-js app {data_app_science.id}: {exc}')
    return data_app


async def _config_version_or_blank(client: KeboolaClient, configuration_id: str) -> str:
    """The latest version of a just-created app's configuration; blank when Storage does not answer.

    - A blank version must not fail a create that already committed.
    """
    try:
        return str(await client.storage_client.configuration_version_latest(DATA_APP_COMPONENT_ID, configuration_id))
    except Exception:
        LOG.exception(f'Could not read the config version of the created data app {configuration_id}')
        return ''


async def _fetch_data_app_details_task(
    client: KeboolaClient, links_manager: ProjectLinksManager, configuration_id: str
) -> DataApp | str:
    """Task fetching data app details with logs and links by configuration ID.
    :param client: The Keboola client
    :param configuration_id: The ID of the data app configuration
    :return: The data app details or the configuration ID if the data app is not found
    """
    try:
        data_app = await _fetch_data_app(client, configuration_id=configuration_id, with_repo=True)
        links = links_manager.get_data_app_links(
            configuration_id=data_app.configuration_id,
            configuration_name=data_app.name,
            deployment_link=data_app.deployment_url,
            uses_basic_authentication=_uses_basic_authentication(data_app.configuration.get('authorization') or {}),
        )
        logs, last_run = await asyncio.gather(
            _fetch_logs(client, data_app.data_app_id),
            _fetch_latest_run(client, data_app),
        )
        data_app = data_app.with_links(links).with_deployment_info(logs, last_run=last_run)
        # Legacy drafts and Streamlit apps have no drafts of their own.
        if data_app.type == 'python-js' and not _is_draft_config(data_app.configuration):
            (data_app.legacy_drafts, data_app.legacy_drafts_unavailable), drafts = await asyncio.gather(
                _fetch_prod_drafts(client, prod_configuration_id=data_app.configuration_id),
                _list_drafts_or_none(client, data_app.data_app_id),
            )
            data_app.drafts = drafts or []
            data_app.drafts_unavailable = drafts is None
        return data_app
    except Exception:
        LOG.exception(f'Failed to fetch data app by configuration ID: {configuration_id}')
        return configuration_id


def _is_draft_config(configuration: Mapping[str, Any]) -> bool:
    """True iff the data app's stored configuration carries `parameters.dataApp.isDraft = true`.

    Shape-safe: a malformed/corrupted config whose `parameters` or `dataApp` is not a mapping is
    simply "not a draft" rather than an `AttributeError` (this helper runs in the detail-fetch path).
    """
    return _data_app_block(configuration).get('isDraft') is True


def _parameters_block(configuration: Mapping[str, Any]) -> Mapping[str, Any]:
    """The `parameters` mapping of a stored configuration; empty when it is missing or malformed."""
    parameters = configuration.get('parameters')
    return parameters if isinstance(parameters, Mapping) else {}


def _data_app_block(configuration: Mapping[str, Any]) -> Mapping[str, Any]:
    """The `parameters.dataApp` mapping of a stored configuration; empty when it is missing or malformed."""
    data_app = _parameters_block(configuration).get('dataApp')
    return data_app if isinstance(data_app, Mapping) else {}


def _validate_branch_update(branch: str, data_app: 'DataApp', configuration_id: str) -> str:
    """Validate a `branch` repoint requested on the update path and return the normalized name.

    The repoint is only for **external-git** python-js apps — a draft, or an app bound to an
    external repository. An app on a **Keboola-managed** git repo is rejected: its branch is
    owned by the platform (prod is expected to track the platform default), and this tool must
    not change that behavior. The authoritative signal is `is_managed_git_repo` from the
    data-science `.../git-repo` endpoint, NOT the presence of a `parameters.dataApp.git` block
    (managed apps can carry one too). When the flag could not be determined (`None` — e.g. the
    lookup failed, or the app is not python-js), the repoint is refused rather than risking a
    change to a managed app.

    Unlike the draft create path, `main` is allowed here — repointing a repo-backed app back to
    `main` is a normal operation.
    """
    if data_app.is_managed_git_repo is None:
        raise ValueError(
            f'Cannot change branch on configuration "{configuration_id}": could not determine whether it '
            'is an external-git app (the git-repo lookup failed or it is not a python-js app). Not changing '
            'the branch — retry, or verify the configuration.'
        )
    if data_app.is_managed_git_repo:
        raise ValueError(
            f'Cannot change branch on configuration "{configuration_id}": it runs on a Keboola-managed git '
            'repo, whose branch is owned by the platform and cannot be repointed with this tool. Only '
            'external-git apps — a draft, or an app bound to an external repository — can be repointed.'
        )
    return _normalize_branch_name(branch)


def _validate_git_repository_args(git_repository: str | None, git_auth: GitAuthType | None) -> None:
    """Check the `git_repository` / `git_auth` pair of `modify_python_js_data_app` before any API call."""
    if git_repository is None and git_auth is None:
        return
    if not git_repository or git_auth is None:
        raise ValueError('git_repository and git_auth must be passed together.')
    validate_organization_repository_url(git_repository)


def _validate_git_rebind(data_app: 'DataApp', configuration_id: str) -> None:
    """Allow a `git_repository` rebind only for a prod app that is already bound to an external repository.

    A Keboola-managed repo is fixed when the app is created; the platform refuses organization git auth
    on such an app at deploy time, so the rebind is refused here instead.
    """
    if _is_draft_config(data_app.configuration):
        raise ValueError(
            f'Cannot set git_repository on configuration "{configuration_id}": it is a draft. Drafts of an app '
            'on a customer GitHub repository are not supported yet.'
        )
    if data_app.is_managed_git_repo is None:
        raise ValueError(
            f'Cannot set git_repository on configuration "{configuration_id}": could not determine whether it '
            'runs on a Keboola-managed git repo (the git-repo lookup failed or it is not a python-js app). '
            'Retry, or verify the configuration.'
        )
    if data_app.is_managed_git_repo:
        raise ValueError(
            f'Cannot set git_repository on configuration "{configuration_id}": it runs on a Keboola-managed git '
            'repo, which cannot be swapped for an external one. Create a new app with git_repository instead.'
        )


def _organization_git_block(repository: str, branch: str | None) -> CodeDataAppConfig.Parameters.DataApp.Git:
    """The `parameters.dataApp.git` block of an app cloned through the organization's GitHub App connection.

    Same shape as the Keboola UI writes: no credential, `private: true`, the branch defaulting to `main`.
    """
    return CodeDataAppConfig.Parameters.DataApp.Git(
        repository=repository,
        branch=_normalize_branch_name(branch) if branch is not None else 'main',
        auth_type=ORGANIZATION_GIT_AUTH,
        private=True,
    )


def _normalize_branch_name(branch: str) -> str:
    normalized = branch.strip()
    if not normalized or any(c.isspace() for c in normalized):
        raise ValueError(f'branch "{branch}" is not a valid git branch name.')
    return normalized


def _organization_git_hint(git: CodeDataAppConfig.Parameters.DataApp.Git, *, rebound: bool) -> str:
    action = 'Rebound the app to' if rebound else 'The app deploys from'
    return (
        f"{action} {git.repository} (branch '{git.branch}') through the Keboola organization's GitHub "
        "connection; no credential is stored. Push code there with the user's own GitHub access — the MCP "
        'mints no git credential for it. Deploy with deploy_data_app in production mode; drafts, dev mode '
        'and the preview link are not supported for this app yet. ' + _ORGANIZATION_CONNECTION_HINT
    )


_ORGANIZATION_CONNECTION_HINT = (
    'If the deploy fails because the GitHub organization is not connected or the GitHub App is not installed '
    'on the repository owner, someone who is both a Keboola organization admin and an owner of the GitHub '
    'organization installs the Keboola GitHub App on the GitHub organization first, then connects that '
    'organization to the Keboola organization. Give the user '
    'https://help.keboola.com/management/organization/#connect-github, not a menu path: most customer admins '
    'do not see the menu link to that page.'
)
_NO_GITHUB_APP_HINT = (
    "This Keboola stack has no GitHub App registered for the repository's host, so organization git auth is "
    'not available for it on this stack yet. Neither the user nor an organization admin can fix that: tell the '
    'user to contact Keboola support. Until then the app can only run from a Keboola-managed repository.'
)
# Fragments of the platform's refusals to mint an organization git token (see `_organization_git_error_hint`).
_NO_GITHUB_APP_ERRORS = ('no github app is registered', 'cannot issue git tokens')
_ORGANIZATION_CONNECTION_ERRORS = (
    'github connection',
    'not covered by this github installation',
    'github app is not installed',
)


def _organization_git_error_hint(message: str | None) -> str | None:
    """An actionable hint for a platform error about the organization's GitHub connection, or None."""
    lowered = (message or '').lower()
    if any(fragment in lowered for fragment in _NO_GITHUB_APP_ERRORS):
        return _NO_GITHUB_APP_HINT
    if any(fragment in lowered for fragment in _ORGANIZATION_CONNECTION_ERRORS):
        return _ORGANIZATION_CONNECTION_HINT
    return None


def _config_git_auth_type(configuration: Mapping[str, Any]) -> str | None:
    """The `parameters.dataApp.git.authType` of a stored data-app configuration, or None when it has none."""
    git = _data_app_block(configuration).get('git')
    auth_type = git.get('authType') if isinstance(git, Mapping) else None
    return auth_type if isinstance(auth_type, str) and auth_type else None


def _uses_organization_git_auth(configuration: Mapping[str, Any]) -> bool:
    return _config_git_auth_type(configuration) == ORGANIZATION_GIT_AUTH


async def _fetch_prod_drafts(client: KeboolaClient, *, prod_configuration_id: str) -> tuple[list[DataAppSummary], int]:
    """List the drafts (configs with `parentConfigurationId == prod_configuration_id`) of a python-js
    prod app. Returns full `DataAppSummary` entries (one extra DSAPI fetch per draft, capped at
    10 parallel) plus the count of drafts whose detail fetch transiently failed and were omitted —
    so the caller can tell "temporarily unreachable" from "deleted". Drafts in trash are not returned
    by `configuration_list` and so do not appear here.
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
        return [], 0

    async def fetch_summary(cfg_id: str) -> DataAppSummary | None:
        try:
            draft = await _fetch_data_app(client, configuration_id=cfg_id, with_repo=True)
        except Exception:
            LOG.exception(f'Failed to fetch draft data app by configuration ID: {cfg_id}')
            return None
        summary = DataAppSummary.model_validate(draft.model_dump())
        summary.repo_url = draft.repo_url
        return summary

    results = await process_concurrently(draft_cfg_ids, fetch_summary, max_concurrency=10)
    drafts = [s for s in results if isinstance(s, DataAppSummary)]
    return drafts, len(draft_cfg_ids) - len(drafts)


async def _list_drafts_or_none(client: KeboolaClient, data_app_id: str) -> list[DraftInfo] | None:
    """The app's drafts, or None when the listing fails (e.g. a stack without the drafts API)."""
    try:
        drafts = await client.data_science_client.list_drafts(data_app_id)
    except Exception:
        LOG.exception(f'Failed to list drafts of data app {data_app_id}')
        return None
    return [DraftInfo.from_response(draft) for draft in drafts]


async def _fetch_logs(client: KeboolaClient, data_app_id: str) -> list[str]:
    """Fetches the logs of a data app if it is running otherwise returns empty list."""
    try:
        str_logs = await client.data_science_client.tail_app_logs(data_app_id, since=None, lines=20)
        logs = str_logs.split('\n')
        return logs
    except httpx.HTTPStatusError:
        # The data app is not running, return empty list
        return []


async def _fetch_latest_run(client: KeboolaClient, data_app: DataApp) -> AppRunInfo | None:
    """The most recent run of the app's latest deployment, or None when there is none.

    - Runs of the app's drafts are listed under the app too; filtering on the deployment leaves them out.
    - A prod app with no `/v2` deployment has no run of its own: None.
    - A legacy draft reads its newest run, which is always its own.
    - Diagnostics must not break the detail fetch: any error is logged and reported as "no run info".
    """
    is_legacy_draft = _is_draft_config(data_app.configuration)
    if not is_legacy_draft and not data_app.desired_deployment_id:
        return None
    deployment_id = None if is_legacy_draft else data_app.desired_deployment_id
    try:
        runs = await client.data_science_client.list_app_runs(
            data_app.data_app_id, limit=1, deployment_id=deployment_id
        )
    except Exception:
        LOG.exception(f'Failed to fetch app runs for data app: {data_app.data_app_id}')
        return None
    return AppRunInfo.from_api_response(runs[0]) if runs else None


def _oidc_provider(authorization: Mapping[str, Any]) -> Mapping[str, Any] | None:
    providers = (authorization.get('app_proxy') or {}).get('auth_providers') or []
    return next((p for p in providers if p.get('type') == 'oidc'), None)


def _oidc_setup_required(authorization: Mapping[str, Any]) -> bool:
    provider = _oidc_provider(authorization)
    return provider is not None and not str(provider.get('#client_secret') or '').strip()


def _build_authorization(
    authentication_type: Literal['no-auth', 'basic-auth', 'oidc'],
    oidc: OidcAuthConfig | None = None,
    existing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if authentication_type == 'basic-auth':
        provider_id, providers = 'simpleAuth', [{'id': 'simpleAuth', 'type': 'password'}]
    elif authentication_type == 'oidc':
        if oidc is None:
            raise ValueError('The OIDC settings are required for the OIDC authentication.')
        # keep the stored secret and unknown keys, so an update does not wipe them
        provider = dict(_oidc_provider(existing or {}) or {})
        provider.update(id=OIDC_PROVIDER_ID, type='oidc', client_id=oidc.client_id, issuer_url=oidc.issuer_url)
        for key, value in (('logout_url', oidc.logout_url), ('allowed_roles', oidc.allowed_roles)):
            if value is None:
                continue
            if value in ('', []):
                provider.pop(key, None)
            else:
                provider[key] = value
        provider_id, providers = OIDC_PROVIDER_ID, [provider]
    else:
        return {
            'app_proxy': {
                'auth_providers': [],
                'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': False}],
            }
        }
    return {
        'app_proxy': {
            'auth_providers': providers,
            'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': [provider_id]}],
        },
    }


# Maximum length for DNS labels per RFC 1035
MAX_DNS_LABEL_LENGTH = 63

# Maximum length for a data-app URL prefix (`parameters.dataApp.slug`). The data-app UI enforces
# this 50-char limit on the same field, so it is tighter than the 63-char DNS-label max — a slug
# capped at 50 is valid under both limits and won't be rejected at deploy.
MAX_DATA_APP_SLUG_LENGTH = 50


class DataAppSlugTooLongError(ValueError):
    """Raised when the generated data app slug exceeds the DNS label length limit."""


_DNS_LABEL_PATTERN = re.compile(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?$')


def _validate_explicit_slug(slug: str) -> None:
    """Reject a caller-supplied slug that is not a DNS label.

    - The hard length limit is the DNS-label max (63), not the 50-char UI limit, so a slug of 51-63
      characters is accepted but may still be rejected at deploy time.
    - Allowed characters are lowercase letters, digits and inner hyphens.
    """
    if len(slug) > MAX_DNS_LABEL_LENGTH:
        raise ValueError(
            f'slug must be at most {MAX_DNS_LABEL_LENGTH} characters (got {len(slug)}); '
            f'note the data-app URL-prefix UI limit is {MAX_DATA_APP_SLUG_LENGTH} characters.'
        )
    if not _DNS_LABEL_PATTERN.match(slug):
        raise ValueError(
            f'slug "{slug}" must contain only lowercase letters, digits and hyphens, '
            'and must not start or end with a hyphen.'
        )


def _is_never_deployed(data_app: 'DataApp') -> bool:
    """Whether the data app has never been deployed.

    - No deployment was ever asked to be production.
    - A first deploy names one at once, while the app is still coming up; that app counts as deployed.
    """
    return data_app.desired_deployment_id is None


def _config_auto_suspend(configuration: Mapping[str, Any]) -> int | None:
    """The `parameters.autoSuspendAfterSeconds` of a stored data-app configuration, or None without one."""
    value = _parameters_block(configuration).get('autoSuspendAfterSeconds')
    return value if isinstance(value, int) else None


def _config_slug(configuration: Mapping[str, Any]) -> str | None:
    """The `parameters.dataApp.slug` of a stored data-app configuration, or None when it has none."""
    slug = _data_app_block(configuration).get('slug')
    return slug if isinstance(slug, str) and slug else None


def _update_response(data_app: 'DataApp') -> str:
    """The response of a config update: a deployed app runs its old config until it is redeployed."""
    if _is_never_deployed(data_app):
        return 'updated'
    return 'updated (redeploy required to apply changes in the running app)'


def _hub_url(client: KeboolaClient, data_app_id: str, slug: str) -> str:
    """The app address `https://<slug>-<app id>.hub.<stack>`. The formula is the operator's."""
    return f'https://{slug}-{data_app_id}.hub.{client.hostname_suffix}'


def _app_url(client: KeboolaClient, data_app_id: str, configuration: Mapping[str, Any]) -> str | None:
    """A draft's address, `https://<slug>-<app id>.hub.<stack>`; None for a prod app or without a slug.

    - The data-science API reports no URL until the app's first run; the formula is the operator's.
    - Drafts only: their slug never changes, while a prod app's explicit slug is editable at any time.
    """
    slug = _config_slug(configuration)
    if not slug or not _is_draft_config(configuration):
        return None
    return _hub_url(client, data_app_id, slug)


def _oidc_callback_url(client: KeboolaClient, data_app_id: str, configuration: Mapping[str, Any]) -> str | None:
    """The OIDC redirect URI of a prod app; None for a draft (drafts cannot use OIDC) or without a slug."""
    slug = _config_slug(configuration)
    if not slug or _is_draft_config(configuration):
        return None
    return f'{_hub_url(client, data_app_id, slug)}/_proxy/callback'


_OIDC_SECRET_PAGE_MESSAGE = (
    "Open the data app's configuration page, go to the Authentication section, enter the OIDC client secret and save."
)


def _oidc_change_summary(authorization: Mapping[str, Any], config_link: str, roles_requested: bool) -> str | None:
    """The next steps for an OIDC app, or None when there is nothing to tell."""
    if _oidc_setup_required(authorization):
        roles_step = (
            "After that, run this update again with authentication_type='oidc' and the same `allowed_roles`: "
            'saving the form in the UI removes them. '
            if roles_requested
            else ''
        )
        return (
            'OIDC login is set, but the app cannot log anyone in yet: the client secret is missing. '
            f'Ask the user to open {config_link}, go to the Authentication section, enter the client secret '
            f'and save. {roles_step}Deploy the app (deploy_data_app) only as the last step.'
        )
    if roles_requested:
        return (
            'The UI form does not show `allowed_roles`, so any later save of the Authentication section in the UI '
            "removes them. After such a save, run this update again with authentication_type='oidc' and "
            '`allowed_roles`, then redeploy.'
        )
    return None


def _config_git_branch(configuration: Mapping[str, Any]) -> str | None:
    """The `parameters.dataApp.git.branch` of a stored data-app configuration, or None when it has none."""
    git = _data_app_block(configuration).get('git')
    branch = git.get('branch') if isinstance(git, Mapping) else None
    return branch if isinstance(branch, str) and branch else None


def _resolve_slug_update(data_app: 'DataApp', *, name: str, explicit_slug: str | None, oidc_app: bool) -> str | None:
    """The slug a python-js update writes, or None to keep the stored one.

    1. An explicit slug on a draft raises — draft slugs never change.
    2. An explicit slug on a prod app is written as given.
    3. An app whose written authorization has an OIDC provider (`oidc_app`) keeps its slug, because
       the callback URL registered at the IdP contains it.
    4. A rename of a prod app that was never deployed moves the slug to `_derive_slug_from_name(name)`
       when the stored slug still follows the current name.
    5. Anything else keeps the stored slug.

    - "Follows the name" compares both through `_derive_slug_from_name`, so a slug the UI derived
      (`prepareSlug`, which keeps repeated hyphens) counts as well.
    - Returns None when the resulting slug equals the stored one.
    """
    if _is_draft_config(data_app.configuration):
        if explicit_slug:
            raise ValueError('slug cannot be changed on a draft data app; change the prod app instead.')
        return None
    current_slug = _config_slug(data_app.configuration)
    if explicit_slug:
        return None if explicit_slug == current_slug else explicit_slug
    if oidc_app or not name or name == data_app.name or current_slug is None or not _is_never_deployed(data_app):
        return None
    if _derive_slug_from_name(current_slug) != _derive_slug_from_name(data_app.name):
        return None
    new_slug = _derive_slug_from_name(name)
    return None if new_slug == current_slug else new_slug


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


def _derive_slug_from_name(name: str) -> str:
    """Derive a DNS-label-safe slug from a data app name when the caller omits one.

    Used on the create path to fill in `slug` for a python-js data app: the tool schema marks
    `slug` optional, so the calling LLM frequently omits it — deriving from `name` lets the create
    succeed instead of raising.

    Lowercases the name, collapses every run of non `[a-z0-9]` characters into a single hyphen,
    strips leading/trailing hyphens, and truncates to fit the data-app URL-prefix length limit
    (``MAX_DATA_APP_SLUG_LENGTH``). Falls back to ``'data-app'`` when the name slugifies to empty.

    :param name: The name of the data app
    :return: A URL-safe slug (lowercase letters, digits, hyphens, <=50 chars)
    """
    base = re.sub(r'[^a-z0-9]+', '-', name.strip().lower()).strip('-')
    return base[:MAX_DATA_APP_SLUG_LENGTH].strip('-') or 'data-app'


def _is_version_newer(latest: str, published: str) -> bool:
    """True iff the `latest` config version is newer than the `published` one. Config versions are numeric
    strings; anything non-numeric is compared for inequality only."""
    try:
        return int(latest) > int(published)
    except ValueError:
        return latest != published


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
    if query_function_code in source_code and '{QUERY_DATA_FUNCTION}' not in source_code:
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
