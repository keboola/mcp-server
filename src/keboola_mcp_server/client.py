"""Keboola Storage API client wrapper."""

import logging
from typing import Any, Mapping, Optional, cast

import httpx
from kbcstorage.client import Client as SyncStorageClient
from pydantic import BaseModel, Field

LOG = logging.getLogger(__name__)


class KeboolaClient:
    """Helper class to interact with Keboola Storage API and Job Queue API."""

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
        storage_api_url: str = 'https://connection.keboola.com',
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
        self.storage_client_sync = SyncStorageClient(storage_api_url, self.token)
        self.storage_client = AsyncStorageClient.create(root_url=storage_api_url, token=self.token)
        self.jobs_queue_client = JobsQueueClient.create(queue_api_url, self.token)
        self.ai_service_client = AIServiceClient.create(root_url=ai_service_api_url, token=self.token)


class RawKeboolaClient:
    """
    Raw async client for Keboola services.

    Implements the basic HTTP methods (GET, POST, PUT, DELETE)
    and can be used to implement high-level functions in clients for individual services.
    """

    def __init__(self, base_api_url: str, api_token: str, headers: dict[str, Any] | None = None) -> None:
        self.base_api_url = base_api_url
        self.headers = {
            'X-StorageApi-Token': api_token,
            'Content-Type': 'application/json',
            'Accept-encoding': 'gzip',
        }
        if headers:
            self.headers.update(headers)

    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Makes a GET request to the service API.

        :param endpoint: API endpoint to call
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            return cast(dict[str, Any], response.json())

    async def post(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Makes a POST request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
                json=data or {},
            )
            response.raise_for_status()
            return cast(dict[str, Any], response.json())

    async def put(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Makes a PUT request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
                json=data or {},
            )
            response.raise_for_status()
            return cast(dict[str, Any], response.json())

    async def delete(
        self,
        endpoint: str,
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Makes a DELETE request to the service API.

        :param endpoint: API endpoint to call
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
            )
            response.raise_for_status()

            return cast(dict[str, Any], response.json())


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
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
        """
        Makes a POST request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :return: API response as dictionary
        """
        return await self.raw_client.post(endpoint=endpoint, data=data)

    async def put(
        self,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Makes a PUT request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :return: API response as dictionary
        """
        return await self.raw_client.put(endpoint=endpoint, data=data)

    async def delete(
        self,
        endpoint: str,
    ) -> dict[str, Any]:
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
    def create(cls, root_url: str, token: str, version: str = 'v2', branch_id: str = 'default') -> 'AsyncStorageClient':
        """
        Creates an AsyncStorageClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token
        :param version: The version of the API to use (default: 'v2')
        :return: A new instance of AsyncStorageClient
        """
        return cls(
            raw_client=RawKeboolaClient(base_api_url=f'{root_url}/{version}/storage', api_token=token),
            branch_id=branch_id,
        )

    async def update_component_configuration(
        self,
        component_id: str,
        configuration_id: str,
        configuration: dict[str, Any],
        change_description: str,
        updated_description: Optional[str] = None,
        is_disabled: bool = False,
    ) -> dict[str, Any]:
        """
        Updates a component configuration.

        :param component_id: The id of the component
        :param configuration_id: The id of the configuration
        :param configuration: The updated configuration dictionary
        :param change_description: The description of the modification to the configuration
        :param updated_description: The entire description of the updated configuration, if None, the original
            description is preserved
        :param is_disabled: Whether the configuration should be disabled
        :return: The response from the API call - updated configuration or raise an error
        """
        endpoint = f'branch/{self.branch_id}/components/{component_id}/configs/{configuration_id}'

        payload = {
            'configuration': configuration,
            'changeDescription': change_description,
        }
        if updated_description:
            payload['description'] = updated_description

        if is_disabled:
            payload['isDisabled'] = is_disabled

        return await self.raw_client.put(endpoint=endpoint, data=payload)


class JobsQueueClient(KeboolaServiceClient):
    """
    Async client for Keboola Job Queue API.
    """

    @classmethod
    def create(cls, root_url: str, token: str) -> 'JobsQueueClient':
        """
        Creates a JobsQueue client.

        :param root_url: Root url of API. e.g. "https://queue.keboola.com/"
        :param token: A key for the Storage API. Can be found in the storage console
        :return: A new instance of JobsQueueClient
        """
        return cls(raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token))

    async def get_job_detail(self, job_id: str) -> dict[str, Any]:
        """
        Retrieves information about a given job.

        :param job_id: The id of the job
        :return: Job details as dictionary
        """

        return await self.raw_client.get(endpoint=f'jobs/{job_id}')

    async def search_jobs_by(
        self,
        component_id: Optional[str] = None,
        config_id: Optional[str] = None,
        status: Optional[list[str]] = None,
        limit: int = 100,
        offset: int = 0,
        sort_by: Optional[str] = 'startTime',
        sort_order: Optional[str] = 'desc',
    ) -> dict[str, Any]:
        """
        Searches for jobs based on the provided parameters.

        :param component_id: The id of the component
        :param config_id: The id of the configuration
        :param status: The status of the jobs to filter by
        :param limit: The number of jobs to return
        :param offset: The offset of the jobs to return
        :param sort_by: The field to sort the jobs by
        :param sort_order: The order to sort the jobs by
        :return: Dictionary containing matching jobs
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
    ) -> dict[str, Any]:
        """
        Creates a new job.

        :param component_id: The id of the component
        :param configuration_id: The id of the configuration
        :return: The response from the API call - created job or raise an error
        """
        payload = {
            'component': component_id,
            'config': configuration_id,
            'mode': 'run',
        }
        return await self.raw_client.post(endpoint='jobs', data=payload)

    async def _search(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Searches for jobs based on the provided parameters.

        :param params: The parameters to search for
        :return: Dictionary containing matching jobs

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
        return await self.raw_client.get(endpoint='search/jobs', params=params)


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


class AIServiceClient(KeboolaServiceClient):
    """
    Async client for Keboola AI Service.
    """

    @classmethod
    def create(cls, root_url: str, token: str) -> 'AIServiceClient':
        """
        Creates an AIServiceClient from a Keboola Storage API token.

        :param root_url: The root URL of the AI service API
        :param token: The Keboola Storage API token
        :return: A new instance of AIServiceClient
        """
        return cls(raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token))

    async def get_component_detail(self, component_id: str) -> dict[str, Any]:
        """
        Retrieves information about a given component.

        :param component_id: The id of the component
        :return: Component details as dictionary
        """
        return await self.get(endpoint=f'docs/components/{component_id}')

    async def docs_question(self, query: str) -> DocsQuestionResponse:
        """
        Answers a question using the Keboola documentation as a source.

        :param query: The query to answer
        :return: Response containing the answer and source URLs
        """
        response = await self.raw_client.post(
            endpoint='docs/question',
            data={'query': query},
            headers={'Accept': 'application/json'},
        )

        return DocsQuestionResponse.model_validate(response)
