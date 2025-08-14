from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.clients.base import KeboolaServiceClient, RawKeboolaClient


class DataAppResponse(BaseModel):
    id: str = Field(validation_alias=AliasChoices('id', 'data_app_id'), description='The data app ID')
    project_id: str = Field(validation_alias=AliasChoices('projectId', 'project_id'), description='The project ID')
    component_id: str = Field(
        validation_alias=AliasChoices('componentId', 'component_id'), description='The component ID'
    )
    branch_id: Optional[str] = Field(
        validation_alias=AliasChoices('branchId', 'branch_id'), description='The branch ID'
    )
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
    last_request_timestamp: str = Field(
        validation_alias=AliasChoices('lastRequestTimestamp', 'last_request_timestamp'),
        description='The last request timestamp',
    )
    last_start_timestamp: str = Field(
        validation_alias=AliasChoices('lastStartTimestamp', 'last_start_timestamp'),
        description='The last start timestamp',
    )
    url: str = Field(validation_alias=AliasChoices('url', 'url'), description='The URL of the running data app')
    auto_suspend_after_seconds: int = Field(
        validation_alias=AliasChoices('autoSuspendAfterSeconds', 'auto_suspend_after_seconds'),
        description='The auto suspend after seconds',
    )
    size: Optional[str] = Field(validation_alias=AliasChoices('size', 'size'), description='The size of the data app')


class DataAppConfig(BaseModel):
    class Parameters(BaseModel):
        class DataApp(BaseModel):
            slug: str = Field(description='The slug of the data app')
            streamlit: dict[str, str] = Field(description='The streamlit config.toml file')
            secrets: Optional[dict[str, str]] = Field(description='The secrets of the data app', default=None)

        size: str = Field(description='The size of the data app')
        auto_suspend_after_seconds: int = Field(
            validation_alias=AliasChoices('autoSuspendAfterSeconds', 'auto_suspend_after_seconds'),
            serialization_alias='autoSuspendAfterSeconds',
            description='The auto suspend after seconds',
        )
        data_app: DataApp = Field(description='The data app sub config', serialization_alias='dataApp')
        id: Optional[str] = Field(description='The id of the data app', default=None)
        script: Optional[list[str]] = Field(description='The script of the data app', default=None)
        packages: Optional[list[str]] = Field(
            description='The python packages needed to be installed in the data app', default=None
        )

    class Authorization(BaseModel):
        class AppProxy(BaseModel):
            auth_providers: list[dict[str, Any]] = Field(description='The auth providers')
            auth_rules: list[dict[str, Any]] = Field(description='The auth rules')

        app_proxy: AppProxy = Field(description='The app proxy')

    parameters: Parameters = Field(description='The parameters of the data app')
    authorization: Authorization = Field(description='The authorization of the data app')


class AsyncDataScienceClient(KeboolaServiceClient):

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        """
        Creates an AsyncDataScienceClient from a RawKeboolaClient and a branch id.

        :param raw_client: The raw client to use
        """
        super().__init__(raw_client=raw_client)

    @property
    def base_api_url(self) -> str:
        return self.raw_client.base_api_url.split('/apps')[0]

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str,
        headers: dict[str, Any] | None = None,
    ) -> 'AsyncDataScienceClient':
        """
        Creates an AsyncDataScienceClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token
        :param headers: Additional headers for the requests
        :return: A new instance of AsyncDataScienceClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=f'{root_url}/apps',
                api_token=token,
                headers=headers,
            )
        )

    async def get_data_app(self, data_app_id: str) -> DataAppResponse:
        """
        Get a data app by its ID.

        :param data_app_id: The ID of the data app
        :return: The data app
        """
        response = await self.raw_client.get(endpoint=data_app_id)
        return DataAppResponse.model_validate(response)

    async def create_data_app(
        self,
        name: str,
        description: str,
        parameters: dict[str, Any],
        authorization: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create a data app.

        :param data_app_id: The ID of the data app
        :return: The data app
        """
        _params = DataAppConfig.Parameters.model_validate(parameters).model_dump(exclude_none=True, by_alias=True)
        _authorization = DataAppConfig.Authorization.model_validate(authorization).model_dump(
            exclude_none=True, by_alias=True
        )
        params = {
            'name': name,
            'type': 'streamlit',
            'description': description,
            'config': {
                'parameters': _params,
                'authorization': _authorization,
            },
        }
        response = await self.raw_client.post('', params=params)
        return response
