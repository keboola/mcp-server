import logging
from datetime import datetime
from typing import Any, Union, cast

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from keboola_mcp_server.clients.base import KeboolaServiceClient, RawKeboolaClient

LOG = logging.getLogger(__name__)


class DataAppResponse(BaseModel):
    id: str = Field(validation_alias=AliasChoices('id', 'data_app_id'), description='The data app ID')
    project_id: str = Field(validation_alias=AliasChoices('projectId', 'project_id'), description='The project ID')
    component_id: str = Field(
        validation_alias=AliasChoices('componentId', 'component_id'), description='The component ID'
    )
    branch_id: str | None = Field(validation_alias=AliasChoices('branchId', 'branch_id'), description='The branch ID')
    config_id: str = Field(
        validation_alias=AliasChoices('configId', 'config_id'), description='The component config ID'
    )
    config_version: str = Field(
        validation_alias=AliasChoices('configVersion', 'config_version'), description='The config version'
    )
    type: str = Field(description='The type of the data app')
    state: str = Field(description='The state of the data app')
    desired_state: str = Field(
        validation_alias=AliasChoices('desiredState', 'desired_state'), description='The desired state'
    )
    last_request_timestamp: str | None = Field(
        validation_alias=AliasChoices('lastRequestTimestamp', 'last_request_timestamp'),
        default=None,
        description='The last request timestamp',
    )
    last_start_timestamp: str | None = Field(
        validation_alias=AliasChoices('lastStartTimestamp', 'last_start_timestamp'),
        default=None,
        description='The last start timestamp',
    )
    url: str | None = Field(
        validation_alias=AliasChoices('url', 'url'), description='The URL of the running data app', default=None
    )
    auto_suspend_after_seconds: int | None = Field(
        validation_alias=AliasChoices('autoSuspendAfterSeconds', 'auto_suspend_after_seconds'),
        description='The auto suspend after seconds',
        default=None,
    )
    size: str | None = Field(
        validation_alias=AliasChoices('size', 'size'), description='The size of the data app', default=None
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
    storage: dict[str, Any] = Field(description='The storage of the data app', default_factory=dict)


class CodeDataAppConfig(BaseModel):
    """
    Config model for python-js (code) data apps backed by a managed git repository.

    Unlike `DataAppConfig` (Streamlit), python-js apps don't embed source code in the config.
    Code lives in the managed git repo; the config only carries deployment metadata
    (slug, auto-suspend, runtime image version).
    """

    model_config = ConfigDict(populate_by_name=True)

    class Parameters(BaseModel):
        class DataApp(BaseModel):
            slug: str = Field(description='The slug of the data app (used as URL subdomain).')
            secrets: dict[str, str] | None = Field(
                description=(
                    'Runtime secrets exposed to the data app as environment variables (e.g. KBC_TOKEN, '
                    'KBC_URL, BRANCH_ID). Mirrors the Streamlit data-app secrets shape. WORKSPACE_ID is '
                    'set by the platform itself when `runtime.workspace.enabled = true`.'
                ),
                default=None,
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

        image: 'CodeDataAppConfig.Runtime.Image' = Field(description='The runtime image.')
        workspace: 'CodeDataAppConfig.Runtime.Workspace | None' = Field(
            default=None,
            description=(
                'Optional workspace runtime config. Provide `{enabled: true}` to opt into '
                'platform-managed per-app workspaces.'
            ),
        )

    parameters: 'CodeDataAppConfig.Parameters' = Field(description='The parameters of the data app.')
    runtime: 'CodeDataAppConfig.Runtime' = Field(description='The runtime configuration (image version, etc.).')
    authorization: DataAppConfig.Authorization | None = Field(
        default=None,
        description=(
            'Optional authorization block. Same shape as for Streamlit data apps. Omit (None) to let the '
            'DSAPI apply its default behavior for python-js apps.'
        ),
    )


class AppSshKeyResponse(BaseModel):
    """Response model for SSH key registration on a managed-git-repo data app."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description='The ID of the registered SSH key.')
    public_key: str | None = Field(
        validation_alias=AliasChoices('publicKey', 'public_key'),
        default=None,
        description=(
            'The registered public key. The DSAPI registration endpoint does not echo it back, so this is '
            'typically only populated when the response comes from a list/get endpoint that includes it.'
        ),
    )
    permissions: str = Field(description='The permissions of the key, e.g. "readWrite" or "readOnly".')
    created_at: str | None = Field(
        validation_alias=AliasChoices('createdAt', 'created_at'),
        default=None,
        description='The timestamp when the key was registered.',
    )


class AppGitRepoResponse(BaseModel):
    """Response model for the managed git repo info of a data app."""

    model_config = ConfigDict(populate_by_name=True)

    url: str = Field(description='The clone URL of the managed git repo (typically an SSH URL).')


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
        response = await self.get(endpoint=f'apps/{data_app_id}')
        return DataAppResponse.model_validate(response)

    async def deploy_data_app(
        self,
        data_app_id: str,
        config_version: str | None = None,
        *,
        mode: str | None = None,
        branch: str | None = None,
        restart_if_running: bool = True,
        update_dependencies: bool = False,
    ) -> DataAppResponse:
        """
        Deploy a data app by its ID.

        :param data_app_id: The ID of the data app
        :param config_version: The version of the config to deploy. Required for Streamlit apps; omit for python-js
                    apps backed by a managed git repo (they have no Storage configVersion).
        :param mode: Deployment mode. Set to 'dev' to enable the in-platform preview for python-js apps.
                    Leave None for Streamlit apps.
        :param branch: Git branch to deploy from. Only meaningful for python-js apps in `mode='dev'`; when set,
                    the dev twin deploys from this branch instead of `main`. Leave None for `main` / prod deploys.
        :param restart_if_running: Whether to restart the data app if it is already running
        :param update_dependencies: If set to `true`, latest package versions are installed during app startup,
                    instead of using frozen versions.
        :return: The data app
        """
        data: dict[str, Any] = {
            'desiredState': 'running',
            'restartIfRunning': restart_if_running,
            'updateDependencies': update_dependencies,
        }
        if config_version is not None:
            data['configVersion'] = config_version
        if mode is not None:
            data['mode'] = mode
        if branch is not None:
            data['branch'] = branch
        response = await self.patch(endpoint=f'apps/{data_app_id}', data=data)
        return DataAppResponse.model_validate(response)

    async def suspend_data_app(self, data_app_id: str) -> DataAppResponse:
        """
        Suspend a data app by setting its desired state to 'stopped'.
        :param data_app_id: Data app ID to suspend
        :return: Updated data app response with the new state
        """
        data = {'desiredState': 'stopped'}
        response = await self.patch(endpoint=f'apps/{data_app_id}', data=data)
        return DataAppResponse.model_validate(response)

    async def get_data_app_password(self, data_app_id: str) -> str:
        """
        Get the password for a data app by its ID.
        """
        response = await self.get(endpoint=f'apps/{data_app_id}/password')
        assert isinstance(response, dict)
        return cast(str, response['password'])

    async def create_data_app(
        self,
        name: str,
        description: str,
        configuration: Union['DataAppConfig', 'CodeDataAppConfig'],
        *,
        app_type: str = 'streamlit',
        use_managed_git_repo: bool = False,
        existing_repo_url: str | None = None,
    ) -> DataAppResponse:
        """
        Create a data app from a simplified config used in the MCP server.

        :param name: The name of the data app
        :param description: The description of the data app
        :param configuration: The simplified configuration of the data app
        :param app_type: The data app type, e.g. 'streamlit' or 'python-js'. Defaults to 'streamlit'.
        :param use_managed_git_repo: When True, the data-science API provisions a managed git repo for the app.
                    Only meaningful for python-js apps.
        :param existing_repo_url: When set, bind the new app to this existing managed git repo (no fresh
                    provisioning). Use this to create a prod app sharing a dev app's repo, or a dev twin
                    sharing an existing prod app's repo. Pair with `use_managed_git_repo=True`.
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
        if existing_repo_url is not None:
            data['existingRepoUrl'] = existing_repo_url
        response = await self.post(endpoint='apps', data=data)
        return DataAppResponse.model_validate(response)

    async def register_app_ssh_key(
        self,
        data_app_id: str,
        public_key: str,
        *,
        permissions: str = 'readWrite',
    ) -> AppSshKeyResponse:
        """
        Register an SSH public key on a managed-git-repo data app so the holder of the matching
        private key can clone, pull, and push to the app's repo.

        :param data_app_id: The ID of the data app
        :param public_key: The full public key contents (e.g. the contents of an `id_ed25519.pub` file).
        :param permissions: 'readWrite' (default) or 'readOnly'.
        :return: The registered SSH key metadata.
        """
        data = {'publicKey': public_key, 'permissions': permissions}
        response = await self.post(endpoint=f'apps/{data_app_id}/git-repo/ssh-keys', data=data)
        return AppSshKeyResponse.model_validate(response)

    async def get_app_git_repo(self, data_app_id: str) -> AppGitRepoResponse:
        """
        Get the managed git repo info (clone URL) for a data app.

        Only meaningful for python-js apps created with `use_managed_git_repo=True`.

        :param data_app_id: The ID of the data app
        :return: The git repo info, including the clone URL.
        """
        response = await self.get(endpoint=f'apps/{data_app_id}/git-repo')
        return AppGitRepoResponse.model_validate(response)

    async def delete_data_app(self, data_app_id: str) -> None:
        """
        Delete a data app by its ID.
        - The DSAPI delete endpoint removes the data app only if its desired and current states match.
        - If they do not match, it returns a 400 Bad Request.
        - Desired state is the state where the app is supposed to be after the action is completed. While current
        state reflects the actual state of the app. E.g. If we deploy the app, the desired state is 'running' and the
        current state is 'started' until the app is deployed.
        - When successful, DSAPI deletes both the app configuration from storage and the data app itself.
        If the configuration was already deleted, DSAPI does not delete the data app and returns 500 error.
        :param data_app_id: ID of the data app to delete
        """
        await self.delete(endpoint=f'apps/{data_app_id}')

    async def list_data_apps(self, limit: int = 100, offset: int = 0) -> list[DataAppResponse]:
        """
        List all data apps.
        """
        response = await self.get(endpoint='apps', params={'limit': limit, 'offset': offset})
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

        response = await self.get_text(endpoint=f'apps/{app_id}/logs/tail', params=params)
        return cast(str, response)
