import asyncio
import logging
from datetime import datetime
from http import HTTPStatus
from typing import Any, Union, cast

import httpx
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError, model_validator

from keboola_mcp_server.clients.base import KeboolaServiceClient, RawKeboolaClient

LOG = logging.getLogger(__name__)

# DSAPI's app delete can deadlock against its own state watcher saving the app (typically right after
# a redeploy) and answers 500 with the MySQL error number in the body's `code` (1213 deadlock, 1205
# lock wait timeout). Every unexpected DSAPI failure is a 500 with the same generic message, so the
# delete retries any 500 with a short backoff (502/503/504 are already retried by the transport,
# base.py). Scoped to this one endpoint, which is safe to repeat: a 404 on a retry means an earlier
# attempt went through.
_DELETE_RETRY_DELAYS_SECONDS = (2.0, 5.0)


class AppProductionResponse(BaseModel):
    """Which deployment of an app is production."""

    model_config = ConfigDict(populate_by_name=True)

    desired_deployment_id: str | None = Field(
        validation_alias=AliasChoices('desiredDeploymentId', 'desired_deployment_id'),
        default=None,
        description='The deployment most recently asked to be production; `null` when nothing ever was.',
    )
    actual_deployment_id: str | None = Field(
        validation_alias=AliasChoices('actualDeploymentId', 'actual_deployment_id'),
        default=None,
        description='The deployment serving production traffic right now.',
    )
    desired_config_version: str | None = Field(
        validation_alias=AliasChoices('desiredConfigVersion', 'desired_config_version'),
        default=None,
        description='The config version of `desired_deployment_id`.',
    )
    actual_config_version: str | None = Field(
        validation_alias=AliasChoices('actualConfigVersion', 'actual_config_version'),
        default=None,
        description='The config version of `actual_deployment_id`.',
    )
    desired_created_at: datetime | None = Field(
        validation_alias=AliasChoices('desiredCreatedAt', 'desired_created_at'),
        default=None,
        description='When `desired_deployment_id` was created.',
    )


class DataAppResponse(BaseModel):
    """A data app as `GET /v2/apps/{appId}` reports it."""

    id: str = Field(validation_alias=AliasChoices('id', 'data_app_id'), description='The data app ID')
    project_id: str = Field(validation_alias=AliasChoices('projectId', 'project_id'), description='The project ID')
    component_id: str = Field(
        validation_alias=AliasChoices('componentId', 'component_id'), description='The component ID'
    )
    branch_id: str | None = Field(validation_alias=AliasChoices('branchId', 'branch_id'), description='The branch ID')
    config_id: str = Field(
        validation_alias=AliasChoices('configId', 'config_id'), description='The component config ID'
    )
    type: str | None = Field(default=None, description='The type of the data app')
    state: str = Field(
        description='Whether production serves traffic: notDeployed, starting, running, suspended or disabled'
    )
    url: str | None = Field(default=None, description='The production URL of the data app')
    production: AppProductionResponse = Field(
        default_factory=AppProductionResponse, description='Which deployment is production'
    )

    @property
    def reported_state(self) -> str:
        """The state of the app's production, with a rollout reported as `starting`.

        - `/v2` keeps `running` while a new deployment is on its way to production.
        - A promotion wakes a suspended app, so its rollout is a start too.
        - A disabled app stays so: its start failed.
        """
        rolling_out = self.production.desired_deployment_id != self.production.actual_deployment_id
        return 'starting' if rolling_out and self.state != 'disabled' else self.state


class DeploymentResponse(BaseModel):
    """One immutable deployment of a data app (`/v2/apps/{appId}/deployments`)."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description='The deployment ID')
    config_version: str = Field(
        validation_alias=AliasChoices('configVersion', 'config_version'),
        description='The configuration version the deployment runs',
    )


class DataAppConfig(BaseModel):
    """
    The simplified data app config model, which is used for creating a data app within the mcp server.
    """

    class Parameters(BaseModel):
        class DataApp(BaseModel):
            slug: str = Field(description='The slug of the data app')
            streamlit: dict[str, str] = Field(
                description=(
                    'The streamlit configuration, expected to have a key with TOML file name and the value with the '
                    'file content'
                )
            )
            secrets: dict[str, str] | None = Field(description='The secrets of the data app', default=None)

        size: str = Field(description='The size of the data app')
        auto_suspend_after_seconds: int = Field(
            validation_alias=AliasChoices('autoSuspendAfterSeconds', 'auto_suspend_after_seconds'),
            serialization_alias='autoSuspendAfterSeconds',
            description='The auto suspend after seconds',
        )
        data_app: DataApp = Field(
            description='The data app sub config',
            serialization_alias='dataApp',
            validation_alias=AliasChoices('dataApp', 'data_app'),
        )
        id: str | None = Field(description='The id of the data app', default=None)
        script: list[str] | None = Field(description='The script of the data app', default=None)
        packages: list[str] | None = Field(
            description='The python packages needed to be installed in the data app', default=None
        )

    class Authorization(BaseModel):
        class AppProxy(BaseModel):
            auth_providers: list[dict[str, Any]] = Field(description='The auth providers')
            auth_rules: list[dict[str, Any]] = Field(description='The auth rules')

        app_proxy: AppProxy = Field(description='The app proxy')

    parameters: Parameters = Field(description='The parameters of the data app')
    authorization: Authorization = Field(description='The authorization of the data app')
    # Optional with a None default so an app without storage mappings omits the key entirely. An
    # empty object would be serialized as `[]` by the backend and break the Writable Tables editor
    # (AI-3135); see `_prune_empty_storage_objects` in tools/data_apps.py.
    storage: dict[str, Any] | None = Field(description='The storage of the data app', default=None)


class CodeDataAppConfig(BaseModel):
    """
    Config model for python-js (code) data apps backed by a managed git repository.

    Unlike `DataAppConfig` (Streamlit), python-js apps don't embed source code in the config.
    Code lives in the managed git repo; the config only carries deployment metadata
    (slug, auto-suspend, optional runtime overrides).
    """

    model_config = ConfigDict(populate_by_name=True)

    class Parameters(BaseModel):
        class DataApp(BaseModel):
            class Git(BaseModel):
                """External-git binding for a python-js draft data app.

                When set, the data-science API treats the app as externally configured: it does NOT
                provision a managed repo for it, and the data-app runtime clones the configured
                `repository` (at `branch`) using `username`/`#password` as HTTPS basic auth on every
                deploy.

                Use case: a draft points at its parent prod app's managed repo, with credentials
                minted on the prod app via `create_app_git_credential`.
                """

                model_config = ConfigDict(populate_by_name=True)

                repository: str = Field(description='HTTPS clone URL of the upstream managed git repo.')
                username: str = Field(
                    description=(
                        'Username for HTTPS basic auth. The git-service ignores this and only validates the token.'
                    ),
                )
                password: str = Field(
                    validation_alias=AliasChoices('#password', 'password'),
                    serialization_alias='#password',
                    description=(
                        'Encrypted HTTPS token (KBC::ConfigSecureGKMS::...). Must be passed through '
                        'EncryptionClient.encrypt before writing to Storage so the platform can '
                        'decrypt it at runtime.'
                    ),
                )
                branch: str | None = Field(
                    default=None,
                    description='Branch to deploy from. None defaults to the platform default ("main").',
                )

            slug: str = Field(description='The slug of the data app (used as URL subdomain).')
            secrets: dict[str, str] | None = Field(
                description=(
                    'Runtime secrets exposed to the data app as environment variables. '
                    'KBC_TOKEN/KBC_URL/BRANCH_ID are always injected by the platform and must not be '
                    'set here. WORKSPACE_ID is set by the platform only when '
                    '`runtime.workspace.enabled = true`; on projects without the '
                    '`data-apps-storage-workspace` feature, WORKSPACE_ID must be passed here instead.'
                ),
                default=None,
            )
            git: 'CodeDataAppConfig.Parameters.DataApp.Git | None' = Field(
                default=None,
                description=(
                    "External-git binding. Set on drafts to point at the parent prod app's "
                    'managed repo + a fresh prod-issued credential + the draft branch. Leave '
                    'unset on prod apps (which own their own managed repo via `useManagedGitRepo`).'
                ),
            )
            is_draft: bool | None = Field(
                validation_alias=AliasChoices('isDraft', 'is_draft'),
                serialization_alias='isDraft',
                default=None,
                description=(
                    'When true, the UI hides this app from the main data-apps list and lists it '
                    'under its parent prod app instead. Set automatically on draft creation.'
                ),
            )
            parent_configuration_id: str | None = Field(
                validation_alias=AliasChoices('parentConfigurationId', 'parent_configuration_id'),
                serialization_alias='parentConfigurationId',
                default=None,
                description=(
                    'Storage configuration ID of the prod python-js data app this draft iterates against. '
                    "Set automatically on draft creation. Used by get_data_apps to list a prod app's drafts."
                ),
            )

        auto_suspend_after_seconds: int = Field(
            validation_alias=AliasChoices('autoSuspendAfterSeconds', 'auto_suspend_after_seconds'),
            serialization_alias='autoSuspendAfterSeconds',
            description='The number of seconds after which the running data app is automatically suspended.',
        )
        data_app: 'CodeDataAppConfig.Parameters.DataApp' = Field(
            validation_alias=AliasChoices('dataApp', 'data_app'),
            serialization_alias='dataApp',
            description='The data app sub config.',
        )

    class Runtime(BaseModel):
        class Image(BaseModel):
            version: str = Field(description='The runtime image version tag.')

        class Workspace(BaseModel):
            enabled: bool = Field(
                description=(
                    'When true, the platform auto-provisions a workspace per data app and injects '
                    'its WORKSPACE_ID into the runtime env.'
                ),
            )

        image: 'CodeDataAppConfig.Runtime.Image | None' = Field(
            default=None,
            description=(
                'Optional pin of the runtime image version. Omit to let the data-science platform '
                'apply its default for python-js apps.'
            ),
        )
        workspace: 'CodeDataAppConfig.Runtime.Workspace | None' = Field(
            default=None,
            description=(
                'Optional workspace runtime config. Provide `{enabled: true}` to opt into '
                'platform-managed per-app workspaces.'
            ),
        )

    parameters: 'CodeDataAppConfig.Parameters' = Field(description='The parameters of the data app.')
    runtime: 'CodeDataAppConfig.Runtime | None' = Field(
        default=None,
        description=(
            'Optional runtime configuration block (image pin, per-app workspace, etc.). Omit '
            'entirely when no runtime overrides are needed — the platform picks defaults.'
        ),
    )
    authorization: DataAppConfig.Authorization | None = Field(
        default=None,
        description=(
            'Optional authorization block. Same shape as for Streamlit data apps. Omit (None) to let the '
            'DSAPI apply its default behavior for python-js apps.'
        ),
    )
    storage: dict[str, Any] | None = Field(
        default=None,
        description=(
            'Optional Storage input/output mappings (validated against the storage JSON schema). '
            'Omit when the app does not need Storage I/O.'
        ),
    )


class CreatedGitCredentialResponse(BaseModel):
    """Response model for credential creation on a managed-git-repo data app.

    Matches the `CreatedCredential` schema from sandboxes-service. For `http_token`
    credentials the response includes a one-time `secret` that cannot be retrieved later.
    """

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description='The ID of the created credential.')
    type: str = Field(description='The credential type, e.g. "http_token" or "ssh_key".')
    name: str = Field(default='', description='Caller-supplied display label (may be empty).')
    permissions: str = Field(description='The permissions of the credential, e.g. "readWrite" or "readOnly".')
    owner_admin_id: str | None = Field(
        validation_alias=AliasChoices('ownerAdminId', 'owner_admin_id'),
        default=None,
        description='The admin ID that owns the credential.',
    )
    created_at: str | None = Field(
        validation_alias=AliasChoices('createdAt', 'created_at'),
        default=None,
        description='The timestamp when the credential was created.',
    )
    secret: str | None = Field(
        default=None,
        description=(
            'One-time secret returned only at creation for `http_token` credentials. '
            'It cannot be retrieved by subsequent reads.'
        ),
    )


class AppGitRepoResponse(BaseModel):
    """Response model for the managed git repo info of a data app."""

    model_config = ConfigDict(populate_by_name=True)

    ssh_url: str | None = Field(
        validation_alias=AliasChoices('sshUrl', 'ssh_url'),
        default=None,
        description='SSH clone URL. `null` for externally configured HTTP(S) repositories.',
    )
    https_url: str | None = Field(
        validation_alias=AliasChoices('httpsUrl', 'https_url'),
        default=None,
        description=(
            'HTTPS clone URL (without embedded credentials). `null` for externally configured SSH repositories.'
        ),
    )
    is_managed_git_repo: bool | None = Field(
        validation_alias=AliasChoices('isManagedGitRepo', 'is_managed_git_repo'),
        default=None,
        description=(
            'Whether the repository is a managed git repository provisioned by the service. '
            'None when the field is absent from the response (spec drift / older service); '
            'callers must treat None as "unknown" and fail closed.'
        ),
    )


class AppRunFailureReason(BaseModel):
    """Machine-readable failure info attached by the platform to an unsuccessful AppRun."""

    model_config = ConfigDict(populate_by_name=True)

    reason: str | None = Field(
        default=None,
        description=(
            'Machine-readable code identifying the kind of failure, '
            'e.g. "ConfigDecryptionFailed" or "StartupProbeFailed".'
        ),
    )
    message: str | None = Field(default=None, description='Human-readable explanation of the failure.')


class AppRunResponse(BaseModel):
    """Response model for a single AppRun — one deployment attempt of a data app."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description='The ID of the app run.')
    app_id: str | None = Field(
        validation_alias=AliasChoices('appId', 'app_id'),
        default=None,
        description='The ID of the data app this run belongs to.',
    )
    state: str = Field(description='The state of the run: "starting", "running", "finished" or "failed".')
    created_at: str | None = Field(
        validation_alias=AliasChoices('createdAt', 'created_at'),
        default=None,
        description='The timestamp when the run was created.',
    )
    started_at: str | None = Field(
        validation_alias=AliasChoices('startedAt', 'started_at'),
        default=None,
        description='The timestamp when the app became ready, or `null` if it never started.',
    )
    stopped_at: str | None = Field(
        validation_alias=AliasChoices('stoppedAt', 'stopped_at'),
        default=None,
        description='The timestamp when the run stopped, or `null` while it is still active.',
    )
    startup_logs: str | None = Field(
        validation_alias=AliasChoices('startupLogs', 'startup_logs'),
        default=None,
        description='Output of the startup phase (entrypoint log), when available.',
    )
    failure_reason: AppRunFailureReason | None = Field(
        validation_alias=AliasChoices('failureReason', 'failure_reason'),
        default=None,
        description=(
            'Why the run was not successful. Populated by the platform for failed runs, including '
            'setup-phase failures (e.g. invalid secrets) that produce no container logs.'
        ),
    )
    mode: str | None = Field(default=None, description='The mode of the run, e.g. "prod" or "dev".')

    @model_validator(mode='before')
    @classmethod
    def _state_of_a_historic_run(cls, data: Any) -> Any:
        """A historic `/v2` run carries no `state`; it finished when it has `stoppedAt`."""
        if isinstance(data, dict) and data.get('state') is None:
            stopped = data.get('stoppedAt') or data.get('stopped_at')
            data = {**data, 'state': 'finished' if stopped else 'running'}
        return data


class AppPreviewLinkResponse(BaseModel):
    """Response of the preview-link mint endpoint of a dev-mode data app."""

    model_config = ConfigDict(populate_by_name=True, hide_input_in_errors=True)

    url: str = Field(
        repr=False,
        description='Link that opens the app in a browser; the token sits in the `#t=` fragment.',
    )
    link_expires_at: str = Field(
        validation_alias=AliasChoices('linkExpiresAt', 'link_expires_at'),
        description='When the link stops working (ISO 8601 with offset).',
    )


class RuntimeResponse(BaseModel):
    """One entry of the data-science runtimes catalog (`GET /runtimes`)."""

    type: str = Field(description='The app type the runtime serves, e.g. "python-js" or "streamlit".')
    description: str = Field(description='Human-readable label, e.g. "Python 3.13 + Node.js 24".')
    is_type_default: bool = Field(
        validation_alias=AliasChoices('isTypeDefault', 'is_type_default'),
        description='Whether the platform runs this runtime when a configuration pins none.',
    )
    image_tag: str | None = Field(
        validation_alias=AliasChoices('imageTag', 'image_tag'),
        default=None,
        description='The value a data app pins in `runtime.image.version`.',
    )
    end_of_life_date: str | None = Field(
        validation_alias=AliasChoices('endOfLifeDate', 'end_of_life_date'),
        default=None,
        description='When the runtime reaches its end of life, or `null` when it has no end date.',
    )


class DataScienceClient(KeboolaServiceClient):
    def __init__(self, raw_client: RawKeboolaClient, branch_id: str | None = None) -> None:
        """
        Creates a DataScienceClient from a RawKeboolaClient and a branch id.

        :param raw_client: The raw client to use
        :param branch_id: The id of the branch
        """
        super().__init__(raw_client=raw_client)
        self._branch_id = branch_id

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str | None,
        branch_id: str | None = None,
        headers: dict[str, Any] | None = None,
        readonly: bool | None = None,
    ) -> 'DataScienceClient':
        """
        Creates a DataScienceClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token. If None, the client will not send any authorization header.
        :param branch_id: The id of the Keboola project branch to work on
        :param headers: Additional headers for the requests
        :param readonly: If True, the client will only use HTTP GET, HEAD operations.
        :return: A new instance of DataScienceClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
                readonly=readonly,
            ),
            branch_id=branch_id,
        )

    async def get_data_app(self, data_app_id: str) -> DataAppResponse:
        """
        Get a data app by its ID.

        :param data_app_id: The ID of the data app
        :return: The data app
        """
        response = await self.get(endpoint=f'v2/apps/{data_app_id}')
        return DataAppResponse.model_validate(response)

    async def deploy_data_app(self, data_app_id: str, config_version: str) -> DeploymentResponse:
        """
        Deploy a data app: create a deployment of the given config version and promote it to production.

        :param data_app_id: The ID of the data app
        :param config_version: The config version to deploy (publish)
        :return: The new deployment
        """
        response = await self.post(
            endpoint=f'v2/apps/{data_app_id}/deployments', data={'configVersion': config_version}
        )
        return DeploymentResponse.model_validate(response)

    async def deploy_data_app_in_mode(self, data_app_id: str, config_version: str, mode: str) -> None:
        """
        Deploy a data app in the given mode through `/v1`: `/v2` has no mode.

        - The app keeps the mode for its later deployments.

        :param data_app_id: The ID of the data app
        :param config_version: The config version to deploy (publish)
        :param mode: 'dev' (hot reload + preview links) or 'prod'
        """
        data = {'desiredState': 'running', 'restartIfRunning': True, 'configVersion': config_version, 'mode': mode}
        await self.patch(endpoint=f'apps/{data_app_id}', data=data)

    async def suspend_data_app(self, data_app_id: str) -> None:
        """
        Suspend a data app: it sleeps now and wakes on the next request.

        :param data_app_id: Data app ID to suspend
        """
        await self.post(endpoint=f'v2/apps/{data_app_id}/suspend', data={})

    async def get_data_app_password(self, data_app_id: str) -> str:
        """
        Get the password for a data app by its ID.
        """
        response = await self.get(endpoint=f'v2/apps/{data_app_id}/password')
        assert isinstance(response, dict)
        return cast(str, response['password'])

    async def create_app_preview_link(self, data_app_id: str) -> AppPreviewLinkResponse:
        """
        Mint a short-lived preview link for a data app in dev mode.

        :param data_app_id: The ID of the data app
        :return: The link and its expiry
        """
        response = await self.post(endpoint=f'apps/{data_app_id}/preview-link')
        try:
            return AppPreviewLinkResponse.model_validate(response)
        except ValidationError as e:
            problems = ', '.join(
                f'{".".join(str(part) for part in err["loc"])}: {err["type"]}'
                for err in e.errors(include_input=False, include_url=False)
            )
        raise ValueError(f'Unexpected response from the data-science API preview-link endpoint: {problems}')

    async def create_data_app(
        self,
        name: str,
        description: str,
        configuration: Union['DataAppConfig', 'CodeDataAppConfig'],
        *,
        app_type: str = 'streamlit',
        use_managed_git_repo: bool = False,
    ) -> DataAppResponse:
        """
        Create a data app from a simplified config used in the MCP server.

        :param name: The name of the data app
        :param description: The description of the data app
        :param configuration: The simplified configuration of the data app
        :param app_type: The data app type, e.g. 'streamlit' or 'python-js'. Defaults to 'streamlit'.
        :param use_managed_git_repo: When True, the data-science API provisions a managed git repo for the app.
                    Only meaningful for python-js apps. Pass False on drafts that bring their own
                    external-git binding via `configuration.parameters.dataApp.git`.
        :return: The data app
        """
        data: dict[str, Any] = {
            'branchId': self._branch_id,
            'name': name,
            'type': app_type,
            'description': description,
            'config': configuration.model_dump(exclude_none=True, by_alias=True),
        }
        if use_managed_git_repo:
            data['useManagedGitRepo'] = True
        response = await self.post(endpoint='v2/apps', data=data)
        return DataAppResponse.model_validate(response)

    async def create_app_git_credential(
        self,
        data_app_id: str,
        *,
        permissions: str = 'readWrite',
    ) -> CreatedGitCredentialResponse:
        """
        Create an HTTP-token credential on a managed-git-repo data app so the caller can clone,
        pull, and push to the app's repo over HTTPS. The response includes a one-time `secret`
        that is not returned by any subsequent read.

        :param data_app_id: The ID of the data app
        :param permissions: 'readWrite' (default) or 'readOnly'.
        :return: The created credential, including the one-time `secret`.
        """
        data = {'type': 'http_token', 'permissions': permissions}
        response = await self.post(endpoint=f'v2/apps/{data_app_id}/git-repo/credentials', data=data)
        return CreatedGitCredentialResponse.model_validate(response)

    async def get_app_git_repo(self, data_app_id: str) -> AppGitRepoResponse:
        """
        Get the managed git repo info (clone URL) for a data app.

        Only meaningful for python-js apps created with `use_managed_git_repo=True`.

        :param data_app_id: The ID of the data app
        :return: The git repo info, including the clone URL.
        """
        response = await self.get(endpoint=f'v2/apps/{data_app_id}/git-repo')
        return AppGitRepoResponse.model_validate(response)

    async def list_app_runs(self, data_app_id: str, *, limit: int = 5, offset: int = 0) -> list[AppRunResponse]:
        """
        List runs (deployment attempts) of a data app, newest first.

        :param data_app_id: The ID of the data app
        :param limit: Maximum number of runs to return
        :param offset: Number of runs to skip
        :return: The app runs, including `failure_reason` for unsuccessful ones.
        """
        response = await self.get(endpoint=f'v2/apps/{data_app_id}/runs', params={'limit': limit, 'offset': offset})
        assert isinstance(response, list)
        return [AppRunResponse.model_validate(run) for run in response]

    async def delete_data_app(self, data_app_id: str) -> None:
        """
        Delete a data app by its ID.
        - Stops every deployment, moves the app's Storage configuration to the trash and removes its credentials.
        - Deprovisions the app's managed git repository, when it has one.
        - Retries a DSAPI 500 (typically a deadlock, code 1213) a couple of times; any other error is raised as is.
        :param data_app_id: ID of the data app to delete
        """
        endpoint = f'v2/apps/{data_app_id}'
        for attempt, retry_delay in enumerate((*_DELETE_RETRY_DELAYS_SECONDS, None), start=1):
            try:
                await self.delete(endpoint=endpoint)
                return
            except httpx.HTTPStatusError as e:
                if attempt > 1 and e.response.status_code == HTTPStatus.NOT_FOUND:
                    LOG.info(
                        'Data app %s is gone on delete attempt %d, an earlier attempt deleted it.', data_app_id, attempt
                    )
                    return
                if retry_delay is None or e.response.status_code != HTTPStatus.INTERNAL_SERVER_ERROR:
                    raise
                LOG.warning(
                    'Data app delete failed with a 500 in DSAPI (attempt %d), retrying in %.0fs: data_app_id=%s',
                    attempt,
                    retry_delay,
                    data_app_id,
                    exc_info=True,
                )
                await asyncio.sleep(retry_delay)
        raise AssertionError('unreachable: loop always returns or raises')

    async def list_runtimes(self) -> list[RuntimeResponse]:
        """
        List the runtimes catalog — the image variants apps, workspaces and transformations can run on.
        """
        response = await self.get(endpoint='runtimes')
        assert isinstance(response, list)
        return [RuntimeResponse.model_validate(runtime) for runtime in response]

    async def list_data_apps(self, limit: int = 100, offset: int = 0) -> list[DataAppResponse]:
        """
        List the data apps of the project, across all its branches.
        """
        response = await self.get(endpoint='v2/apps', params={'limit': limit, 'offset': offset})
        return [DataAppResponse.model_validate(app) for app in response]

    async def tail_app_logs(
        self,
        app_id: str,
        *,
        since: datetime | None,
        lines: int | None,
    ) -> str:
        """
        Tail application logs. Either `since` or `lines` must be provided but not both at the same time.
        In case when none of the parameters are provided, it uses the `lines` parameter with
        the last 100 lines.
        :param app_id: ID of the app.
        :param since: ISO-8601 timestamp with nanoseconds as a datetime object
                      Providing microseconds is enough, nanoseconds are not supported via datetime
                      E.g: since = datetime.now(timezone.utc) - timedelta(days=1)
        :param lines: Number of log lines from the end. Defaults to 100.
        :return: Logs as plain text.
        :raise ValueError: If both "since" and "lines" are provided.
        :raise ValueError: If neither "since" nor "lines" are provided.
        :raise httpx.HTTPStatusError: For non-200 status codes.
        """
        if since and lines:
            raise ValueError('You cannot use both "since" and "lines" query parameters together.')
        elif since is None and lines is None:
            raise ValueError('Either "since" or "lines" must be provided.')

        if lines is not None:
            lines = max(lines, 1)  # Ensure lines is at least 1
            params = {'lines': lines}
        elif since is not None:
            iso_since = since.isoformat(timespec='microseconds')
            params = {'since': iso_since}
        else:
            raise ValueError('Either "since" or "lines" must be provided.')

        response = await self.get_text(endpoint=f'v2/apps/{app_id}/logs/tail', params=params)
        return cast(str, response)
