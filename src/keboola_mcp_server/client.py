"""Keboola Storage API client wrapper."""

import importlib.metadata
import logging
import os
from typing import Any, Mapping, Optional, Union, cast

import httpx
from pydantic import BaseModel, Field

LOG = logging.getLogger(__name__)

JsonPrimitive = Union[int, float, str, bool, None]
JsonDict = dict[str, Union[JsonPrimitive, 'JsonStruct']]
JsonList = list[Union[JsonPrimitive, 'JsonStruct']]
JsonStruct = Union[JsonDict, JsonList]

ORCHESTRATOR_COMPONENT_ID = 'keboola.orchestrator'


class KeboolaClient:
    """Class holding clients for Keboola APIs: Storage API, Job Queue API, and AI Service."""

    STATE_KEY = 'sapi_client'
    # Prefixes for the storage and queue API URLs, we do not use http:// or https:// here since we split the storage
    # api url by `connection` word
    _PREFIX_STORAGE_API_URL = 'connection.'
    _PREFIX_QUEUE_API_URL = 'https://queue.'
    _PREFIX_AISERVICE_API_URL = 'https://ai.'

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'KeboolaClient':
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, KeboolaClient), f'Expected KeboolaClient, got: {instance}'
        return instance

    def __init__(
        self,
        storage_api_token: str,
        storage_api_url: str,
    ) -> None:
        """
        Initialize the client.

        :param storage_api_token: Keboola Storage API token
        :param storage_api_url: Keboola Storage API URL
        """
        self.token = storage_api_token
        # Ensure the base URL has a scheme
        if not storage_api_url.startswith(('http://', 'https://')):
            storage_api_url = f'https://{storage_api_url}'

        # Construct the queue API URL from the storage API URL expecting the following format:
        # https://connection.REGION.keboola.com
        # Remove the prefix from the storage API URL https://connection.REGION.keboola.com -> REGION.keboola.com
        # and add the prefix for the queue API https://queue.REGION.keboola.com
        queue_api_url = f'{self._PREFIX_QUEUE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}'
        ai_service_api_url = f'{self._PREFIX_AISERVICE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}'

        # Initialize clients for individual services
        self.storage_client = AsyncStorageClient.create(
            root_url=storage_api_url, token=self.token, headers=self._get_headers()
        )
        self.jobs_queue_client = JobsQueueClient.create(
            root_url=queue_api_url, token=self.token, headers=self._get_headers()
        )
        self.ai_service_client = AIServiceClient.create(
            root_url=ai_service_api_url, token=self.token, headers=self._get_headers()
        )

    @classmethod
    def _get_user_agent(cls) -> str:
        """
        :return: User agent string.
        """
        try:
            version = importlib.metadata.version('keboola-mcp-server')
        except importlib.metadata.PackageNotFoundError:
            version = 'NA'

        app_env = os.getenv('APP_ENV', 'local')
        return f'Keboola MCP Server/{version} app_env={app_env}'

    @classmethod
    def _get_headers(cls) -> dict[str, Any]:
        """
        :return: Additional headers for the requests, namely the user agent.
        """
        return {'User-Agent': cls._get_user_agent()}


class RawKeboolaClient:
    """
    Raw async client for Keboola services.

    Implements the basic HTTP methods (GET, POST, PUT, DELETE)
    and can be used to implement high-level functions in clients for individual services.
    """

    def __init__(
        self,
        base_api_url: str,
        api_token: str,
        headers: dict[str, Any] | None = None,
        timeout: httpx.Timeout | None = None,
    ) -> None:
        self.base_api_url = base_api_url
        self.headers = {
            'X-StorageApi-Token': api_token,
            'Content-Type': 'application/json',
            'Accept-encoding': 'gzip',
        }
        self.timeout = timeout or httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
        if headers:
            self.headers.update(headers)

    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct:
        """
        Makes a GET request to the service API.

        :param endpoint: API endpoint to call
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
            )
            response.raise_for_status()
            return cast(JsonStruct, response.json())

    async def post(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct:
        """
        Makes a POST request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
                json=data or {},
            )
            response.raise_for_status()
            return cast(JsonStruct, response.json())

    async def put(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct:
        """
        Makes a PUT request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.put(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
                json=data or {},
            )
            response.raise_for_status()
            return cast(JsonStruct, response.json())

    async def delete(
        self,
        endpoint: str,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct | None:
        """
        Makes a DELETE request to the service API.

        :param endpoint: API endpoint to call
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.delete(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
            )
            response.raise_for_status()

            if response.content:
                return cast(JsonStruct, response.json())

            return None


class KeboolaServiceClient:
    """
    Base class for Keboola service clients.

    Implements the basic HTTP methods (GET, POST, PUT, DELETE)
    and is used as a base class for clients for individual services.
    """

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        """
        Creates a client instance.

        The inherited classes should implement the `create` method
        rather than overriding this constructor.

        :param raw_client: The raw client to use
        """
        self.raw_client = raw_client

    @classmethod
    def create(cls, root_url: str, token: str) -> 'KeboolaServiceClient':
        """
        Creates a KeboolaServiceClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token
        :return: A new instance of KeboolaServiceClient
        """
        return cls(raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token))

    async def get(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a GET request to the service API.

        :param endpoint: API endpoint to call
        :param params: Query parameters for the request
        :return: API response as dictionary
        """
        return await self.raw_client.get(endpoint=endpoint, params=params)

    async def post(
        self,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a POST request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :return: API response as dictionary
        """
        return await self.raw_client.post(endpoint=endpoint, data=data, params=params)

    async def put(
        self,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a PUT request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :return: API response as dictionary
        """
        return await self.raw_client.put(endpoint=endpoint, data=data, params=params)

    async def delete(
        self,
        endpoint: str,
    ) -> JsonStruct | None:
        """
        Makes a DELETE request to the service API.

        :param endpoint: API endpoint to call
        :return: API response as dictionary
        """
        return await self.raw_client.delete(endpoint=endpoint)


class AsyncStorageClient(KeboolaServiceClient):

    def __init__(self, raw_client: RawKeboolaClient, branch_id: str = 'default') -> None:
        """
        Creates an AsyncStorageClient from a RawKeboolaClient and a branch id.

        :param raw_client: The raw client to use
        :param branch_id: The id of the branch
        """
        super().__init__(raw_client=raw_client)
        self._branch_id: str = branch_id

    @property
    def branch_id(self) -> str:
        return self._branch_id

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str,
        version: str = 'v2',
        branch_id: str = 'default',
        headers: dict[str, Any] | None = None,
    ) -> 'AsyncStorageClient':
        """
        Creates an AsyncStorageClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token
        :param version: The version of the API to use (default: 'v2')
        :param branch_id: The id of the branch
        :param headers: Additional headers for the requests
        :return: A new instance of AsyncStorageClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=f'{root_url}/{version}/storage',
                api_token=token,
                headers=headers,
            ),
            branch_id=branch_id,
        )

    async def bucket_detail(self, bucket_id: str) -> JsonStruct:
        """
        Retrieves information about a given bucket.

        :param bucket_id: The id of the bucket
        :return: Bucket details as dictionary
        """
        return await self.get(endpoint=f'buckets/{bucket_id}')

    async def bucket_list(self) -> JsonList:
        """
        Lists all buckets.

        :return: List of buckets as dictionary
        """
        return cast(JsonList, await self.get(endpoint='buckets'))

    async def bucket_table_list(self, bucket_id: str, include: list[str] | None = None) -> list[JsonDict]:
        """
        Lists all tables in a given bucket.

        :param bucket_id: The id of the bucket
        :param include: List of fields to include in the response
        :return: List of tables as dictionary
        """
        params = {}
        if include is not None and isinstance(include, list):
            params['include'] = ','.join(include)
        return cast(list[JsonDict], await self.get(endpoint=f'buckets/{bucket_id}/tables', params=params))

    async def configuration_create(
        self,
        component_id: str,
        name: str,
        description: str,
        configuration: dict[str, Any],
    ) -> JsonDict:
        """
        Creates a new configuration for a component.

        :param component_id: The id of the component for which to create the configuration.
        :param name: The name of the configuration.
        :param description: The description of the configuration.
        :param configuration: The configuration definition as a dictionary.

        :return: The SAPI call response - created configuration or raise an error.
        """
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs'

        payload = {
            'name': name,
            'description': description,
            'configuration': configuration,
        }
        return cast(JsonDict, await self.post(endpoint=endpoint, data=payload))

    async def configuration_delete(self, component_id: str, configuration_id: str, skip_trash: bool = False) -> None:
        """
        Deletes a configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :param skip_trash: If True, the configuration is deleted without moving to the trash.
            (Technically it means the API endpoint is called twice.)
        :raises ValueError: If the component_id or configuration_id is invalid.
        """
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs/{configuration_id}'
        await self.delete(endpoint=endpoint)
        if skip_trash:
            await self.delete(endpoint=endpoint)

    async def configuration_detail(self, component_id: str, configuration_id: str) -> JsonDict:
        """
        Retrieves information about a given configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :return: The parsed json from the HTTP response.
        :raises ValueError: If the component_id or configuration_id is invalid.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError(f"Invalid component_id '{component_id}'.")
        if not isinstance(configuration_id, str) or configuration_id == '':
            raise ValueError(f"Invalid configuration_id '{configuration_id}'.")
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs/{configuration_id}'

        return cast(JsonDict, await self.get(endpoint=endpoint))

    async def configuration_list(self, component_id: str) -> list[JsonDict]:
        """
        Lists configurations of the given component.

        :param component_id: The id of the component.
        :return: List of configurations.
        :raises ValueError: If the component_id is invalid.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError(f"Invalid component_id '{component_id}'.")
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs'

        return cast(list[JsonDict], await self.get(endpoint=endpoint))

    async def configuration_metadata_get(self, component_id: str, configuration_id: str) -> JsonList:
        """
        Retrieves metadata for a given configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :return: Configuration metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs/{configuration_id}/metadata'
        return cast(JsonList, await self.get(endpoint=endpoint))

    async def configuration_metadata_update(
        self,
        component_id: str,
        configuration_id: str,
        metadata: dict[str, Any],
    ) -> JsonList:
        """
        Updates metadata for the given configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :param metadata: The metadata to update.
        :return: Configuration metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs/{configuration_id}/metadata'
        payload = {
            'metadata': [{'key': key, 'value': value} for key, value in metadata.items()],
        }
        return cast(JsonList, await self.post(endpoint=endpoint, data=payload))

    async def configuration_update(
        self,
        component_id: str,
        configuration_id: str,
        configuration: dict[str, Any],
        change_description: str,
        updated_name: Optional[str] = None,
        updated_description: Optional[str] = None,
        is_disabled: bool = False,
    ) -> JsonDict:
        """
        Updates a component configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :param configuration: The updated configuration dictionary.
        :param change_description: The description of the modification to the configuration.
        :param updated_name: The updated name of the configuration, if None, the original
            name is preserved.
        :param updated_description: The entire description of the updated configuration, if None, the original
            description is preserved.
        :param is_disabled: Whether the configuration should be disabled.
        :return: The SAPI call response - updated configuration or raise an error.
        """
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs/{configuration_id}'

        payload = {
            'configuration': configuration,
            'changeDescription': change_description,
        }
        if updated_name:
            payload['name'] = updated_name

        if updated_description:
            payload['description'] = updated_description

        if is_disabled:
            payload['isDisabled'] = is_disabled

        return cast(JsonDict, await self.put(endpoint=endpoint, data=payload))

    async def configuration_row_create(
        self,
        component_id: str,
        config_id: str,
        name: str,
        description: str,
        configuration: dict[str, Any],
    ) -> JsonDict:
        """
        Creates a new row configuration for a component configuration.

        :param component_id: The ID of the component.
        :param config_id: The ID of the configuration.
        :param name: The name of the row configuration.
        :param description: The description of the row configuration.
        :param configuration: The configuration data to create row configuration.
        :return: The SAPI call response - created row configuration or raise an error.
        """
        payload = {
            'name': name,
            'description': description,
            'configuration': configuration,
        }

        return cast(
            JsonDict,
            await self.post(
                endpoint=f'branch/{self.branch_id}/components/{component_id}/configs/{config_id}/rows', data=payload
            ),
        )

    async def configuration_row_update(
        self,
        component_id: str,
        config_id: str,
        configuration_row_id: str,
        configuration: dict[str, Any],
        change_description: str,
        updated_name: Optional[str] = None,
        updated_description: Optional[str] = None,
    ) -> JsonDict:
        """
        Updates a row configuration for a component configuration.

        :param configuration: The configuration data to update row configuration.
        :param component_id: The ID of the component.
        :param config_id: The ID of the configuration.
        :param configuration_row_id: The ID of the row.
        :param change_description: The description of the changes made.
        :param updated_name: The updated name of the configuration, if None, the original
            name is preserved.
        :param updated_description: The updated description of the configuration, if None, the original
            description is preserved.
        :return: The SAPI call response - updated row configuration or raise an error.
        """

        payload = {
            'configuration': configuration,
            'changeDescription': change_description,
        }
        if updated_name:
            payload['name'] = updated_name

        if updated_description:
            payload['description'] = updated_description

        return cast(
            JsonDict,
            await self.put(
                endpoint=f'branch/{self.branch_id}/components/{component_id}/configs/{config_id}'
                f'/rows/{configuration_row_id}',
                data=payload,
            ),
        )

    async def flow_create(
        self,
        name: str,
        description: str,
        flow_configuration: dict[str, Any],
    ) -> JsonDict:
        """
        Creates a new flow (orchestrator) configuration.

        Note: Flow configurations are special - they store phases/tasks directly
        under 'configuration', not under 'configuration.parameters' like other components.

        :param name: The name of the flow
        :param description: The description of the flow
        :param flow_configuration: The flow configuration containing phases and tasks directly
        :return: The SAPI call response - created flow configuration or raise an error
        """
        return await self.configuration_create(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            name=name,
            description=description,
            configuration=flow_configuration,
        )

    async def flow_detail(self, config_id: str) -> JsonDict:
        """
        Retrieves a specific flow (orchestrator) configuration.

        :param config_id: The ID of the flow configuration to retrieve
        :return: Flow configuration details
        """
        return await self.configuration_detail(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=config_id
        )

    async def flow_list(self) -> list[JsonDict]:
        """
        Lists all flow (orchestrator) configurations in the project.

        :return: List of flow configurations
        """
        return await self.configuration_list(component_id=ORCHESTRATOR_COMPONENT_ID)

    async def flow_update(
        self,
        config_id: str,
        name: str,
        description: str,
        change_description: str,
        flow_configuration: dict[str, Any],
    ) -> JsonDict:
        """
        Updates an existing flow (orchestrator) configuration.

        Note: Flow configurations store phases/tasks directly under 'configuration'.

        :param config_id: The ID of the flow configuration to update
        :param name: The updated name of the flow
        :param description: The updated description of the flow
        :param change_description: Description of the changes made
        :param flow_configuration: The updated flow configuration containing phases and tasks directly
        :return: The SAPI call response - updated flow configuration or raise an error
        """
        return await self.configuration_update(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=config_id,
            configuration=flow_configuration,
            change_description=change_description,
            updated_name=name,
            updated_description=description,
        )

    async def table_detail(self, table_id: str) -> JsonDict:
        """
        Retrieves information about a given table.

        :param table_id: The id of the table
        :return: Table details as dictionary
        """
        return cast(JsonDict, await self.get(endpoint=f'tables/{table_id}'))

    async def verify_token(self) -> JsonDict:
        """
        Checks the token privileges and returns information about the project to which the token belongs.

        :return: Token and project information
        """
        return cast(JsonDict, await self.get(endpoint='tokens/verify'))


class JobsQueueClient(KeboolaServiceClient):
    """
    Async client for Keboola Job Queue API.
    """

    @classmethod
    def create(cls, root_url: str, token: str, headers: dict[str, Any] | None = None) -> 'JobsQueueClient':
        """
        Creates a JobsQueue client.

        :param root_url: Root url of API. e.g. "https://queue.keboola.com/".
        :param token: A key for the Storage API. Can be found in the storage console.
        :param headers: Additional headers for the requests.
        :return: A new instance of JobsQueueClient.
        """
        return cls(raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token, headers=headers))

    async def get_job_detail(self, job_id: str) -> JsonDict:
        """
        Retrieves information about a given job.

        :param job_id: The id of the job.
        :return: Job details as dictionary.
        """

        return cast(JsonDict, await self.get(endpoint=f'jobs/{job_id}'))

    async def search_jobs_by(
        self,
        component_id: Optional[str] = None,
        config_id: Optional[str] = None,
        status: Optional[list[str]] = None,
        limit: int = 100,
        offset: int = 0,
        sort_by: Optional[str] = 'startTime',
        sort_order: Optional[str] = 'desc',
    ) -> JsonList:
        """
        Searches for jobs based on the provided parameters.

        :param component_id: The id of the component.
        :param config_id: The id of the configuration.
        :param status: The status of the jobs to filter by.
        :param limit: The number of jobs to return.
        :param offset: The offset of the jobs to return.
        :param sort_by: The field to sort the jobs by.
        :param sort_order: The order to sort the jobs by.
        :return: Dictionary containing matching jobs.
        """
        params = {
            'componentId': component_id,
            'configId': config_id,
            'status': status,
            'limit': limit,
            'offset': offset,
            'sortBy': sort_by,
            'sortOrder': sort_order,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return await self._search(params=params)

    async def create_job(
        self,
        component_id: str,
        configuration_id: str,
    ) -> JsonDict:
        """
        Creates a new job.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :return: The response from the API call - created job or raise an error.
        """
        payload = {
            'component': component_id,
            'config': configuration_id,
            'mode': 'run',
        }
        return cast(JsonDict, await self.post(endpoint='jobs', data=payload))

    async def _search(self, params: dict[str, Any]) -> JsonList:
        """
        Searches for jobs based on the provided parameters.

        :param params: The parameters to search for.
        :return: Dictionary containing matching jobs.

        Parameters (copied from the API docs):
            - id str/list[str]: Search jobs by id
            - runId str/list[str]: Search jobs by runId
            - branchId str/list[str]: Search jobs by branchId
            - tokenId str/list[str]: Search jobs by tokenId
            - tokenDescription str/list[str]: Search jobs by tokenDescription
            - componentId str/list[str]: Search jobs by componentId
            - component str/list[str]: Search jobs by componentId, alias for componentId
            - configId str/list[str]: Search jobs by configId
            - config str/list[str]: Search jobs by configId, alias for configId
            - configRowIds str/list[str]: Search jobs by configRowIds
            - status str/list[str]: Search jobs by status
            - createdTimeFrom str: The jobs that were created after the given date
                e.g. "2021-01-01, -8 hours, -1 week,..."
            - createdTimeTo str: The jobs that were created before the given date
                e.g. "2021-01-01, today, last monday,..."
            - startTimeFrom str: The jobs that were started after the given date
                e.g. "2021-01-01, -8 hours, -1 week,..."
            - startTimeTo str: The jobs that were started before the given date
                e.g. "2021-01-01, today, last monday,..."
            - endTimeTo str: The jobs that were finished before the given date
                e.g. "2021-01-01, today, last monday,..."
            - endTimeFrom str: The jobs that were finished after the given date
                e.g. "2021-01-01, -8 hours, -1 week,..."
            - limit int: The number of jobs returned, default 100
            - offset int: The jobs page offset, default 0
            - sortBy str: The jobs sorting field, default "id"
                values: id, runId, projectId, branchId, componentId, configId, tokenDescription, status, createdTime,
                updatedTime, startTime, endTime, durationSeconds
            - sortOrder str: The jobs sorting order, default "desc"
                values: asc, desc
        """
        return cast(JsonList, await self.get(endpoint='search/jobs', params=params))


class DocsQuestionResponse(BaseModel):
    """
    The AI service response to a request to `/docs/question` endpoint.
    """

    text: str = Field(description='Text of the answer to a documentation query.')
    source_urls: list[str] = Field(
        description='List of URLs to the sources of the answer.',
        default_factory=list,
        alias='sourceUrls',
    )


class SuggestedComponent(BaseModel):
    """The AI service response to a /docs/suggest-component request."""

    component_id: str = Field(description='The component ID.', alias='componentId')
    score: float = Field(description='Score of the component suggestion.')
    source: str = Field(description='Source of the component suggestion.')


class ComponentSuggestionResponse(BaseModel):
    """The AI service response to a /suggest/component request."""

    components: list[SuggestedComponent] = Field(description='List of suggested components.', default_factory=list)


class AIServiceClient(KeboolaServiceClient):
    """Async client for Keboola AI Service."""

    @classmethod
    def create(cls, root_url: str, token: str, headers: dict[str, Any] | None = None) -> 'AIServiceClient':
        """
        Creates an AIServiceClient from a Keboola Storage API token.

        :param root_url: The root URL of the AI service API.
        :param token: The Keboola Storage API token.
        :param headers: Additional headers for the requests.
        :return: A new instance of AIServiceClient.
        """
        return cls(raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token, headers=headers))

    async def get_component_detail(self, component_id: str) -> JsonDict:
        """
        Retrieves information about a given component.

        :param component_id: The id of the component.
        :return: Component details as dictionary.
        """
        return cast(JsonDict, await self.get(endpoint=f'docs/components/{component_id}'))

    async def docs_question(self, query: str) -> DocsQuestionResponse:
        """
        Answers a question using the Keboola documentation as a source.
        :param query: The query to answer.
        :return: Response containing the answer and source URLs.
        """
        response = await self.raw_client.post(
            endpoint='docs/question',
            data={'query': query},
            headers={'Accept': 'application/json'},
        )

        return DocsQuestionResponse.model_validate(response)

    async def suggest_component(self, query: str) -> ComponentSuggestionResponse:
        """
        Provides list of component suggestions based on natural language query.
        :param query: The query to answer.
        :return: Response containing the list of suggested component IDs, their score and source.
        """
        response = await self.raw_client.post(
            endpoint='suggest/component',
            data={'prompt': query},
            headers={'Accept': 'application/json'},
        )

        return ComponentSuggestionResponse.model_validate(response)
