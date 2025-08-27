import logging
from datetime import datetime
from typing import Any, cast

from pydantic import AliasChoices, BaseModel, Field

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
    auto_suspend_after_seconds: int = Field(
        validation_alias=AliasChoices('autoSuspendAfterSeconds', 'auto_suspend_after_seconds'),
        description='The auto suspend after seconds',
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
    ) -> 'DataScienceClient':
        """
        Creates a DataScienceClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token. If None, the client will not send any authorization header.
        :param branch_id: The id of the Keboola project branch to work on
        :param headers: Additional headers for the requests
        :return: A new instance of DataScienceClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
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
        config_version: str,
        *,
        restart_if_running: bool = True,
        update_dependencies: bool = False,
    ) -> DataAppResponse:
        """
        Deploy a data app by its ID.

        :param data_app_id: The ID of the data app
        :param config_version: The version of the config to deploy
        :param restart_if_running: Whether to restart the data app if it is already running
        :param update_dependencies: If set to `true`, latest package versions are installed during app startup,
                    instead of using frozen versions.
        :return: The data app
        """
        data = {
            'desiredState': 'running',
            'configVersion': config_version,
            'restartIfRunning': restart_if_running,
            'updateDependencies': update_dependencies,
        }
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
        configuration: DataAppConfig,
    ) -> DataAppResponse:
        """
        Create a data app from a simplified config used in the MCP server.
        :param name: The name of the data app
        :param description: The description of the data app
        :param configuration: The simplified configuration of the data app
        :return: The data app
        """
        data = {
            'branchId': self._branch_id,
            'name': name,
            'type': 'streamlit',
            'description': description,
            'config': configuration.model_dump(exclude_none=True, by_alias=True),
        }
        response = await self.post(endpoint='apps', data=data)
        return DataAppResponse.model_validate(response)

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
