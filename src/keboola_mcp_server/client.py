"""Keboola Storage API client wrapper."""

import logging
from typing import Any, Mapping, Optional, cast

import httpx
from kbcstorage.client import Client
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

        Args:
            storage_api_token: Keboola Storage API token
            storage_api_url: Keboola Storage API URL
        """
        self.token = storage_api_token
        # Ensure the base URL has a scheme
        if not storage_api_url.startswith(('http://', 'https://')):
            storage_api_url = f'https://{storage_api_url}'

        # Construct the queue API URL from the storage API URL expecting the following format:
        # https://connection.REGION.keboola.com
        # Remove the prefix from the storage API URL https://connection.REGION.keboola.com -> REGION.keboola.com
        # and add the prefix for the queue API https://queue.REGION.keboola.com
        queue_api_url = (
            f'{self._PREFIX_QUEUE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}'
        )
        ai_service_api_url = f"{self._PREFIX_AISERVICE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}"

        # Initialize clients for individual services
        self.storage_client_sync = Client(storage_api_url, self.token)
        self.storage_client = StorageAsyncClient.create(root_url=storage_api_url, token=self.token)
        self.jobs_queue = JobsQueueClient.create(queue_api_url, self.token)
        self.ai_service_client = AIServiceClient.create(root_url=ai_service_api_url, token=self.token)


class AsyncKeboolaClient():
    """Async client for Keboola services"""

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
        """Make a GET request to a Keboola service API.

        Args:
            endpoint: API endpoint to call
            params: Query parameters for the request

        Returns:
            API response as dictionary
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
        """Make a POST request to a Keboola service API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
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
        """Make a PUT request to a Keboola service API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
                data=data or {},
            )
            response.raise_for_status()
            return cast(dict[str, Any], response.json())

    async def delete(
        self,
        endpoint: str,
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make a DELETE request to a Keboola service API.

        Args:
            endpoint: API endpoint to call

        Returns:
            API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
            )
            response.raise_for_status()

            return cast(dict[str, Any], response.json())


class StorageAsyncClient:
    def __init__(self, api_client: AsyncKeboolaClient) -> None:
        """
        Creates an StorageAsyncClient.
        :param api_client: The API client to use.
        """
        self.api_client = api_client

    @classmethod
    def create(cls, root_url: str, token: str) -> 'StorageAsyncClient':
        """Creates an StorageAsyncClient from a Keboola Storage API token."""
        return cls(api_client=AsyncKeboolaClient(base_api_url=f'{root_url}/v2/storage', api_token=token))

    async def get(self, endpoint: str, params: Optional[dict[str, Any]] = None,) -> dict[str, Any]:
        """Make a GET request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            params: Query parameters for the request

        Returns:
            API response as dictionary
        """
        return await self.api_client.get(endpoint=endpoint, params=params)

    async def post(self, endpoint: str, data: Optional[dict[str, Any]] = None,) -> dict[str, Any]:
        """Make a POST request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
        """
        return await self.api_client.post(endpoint=endpoint, data=data)

    async def put(self, endpoint: str, data: Optional[dict[str, Any]] = None,) -> dict[str, Any]:
        """Make a PUT request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
        """
        return await self.api_client.put(endpoint=endpoint, data=data)

    async def delete(self, endpoint: str,) -> dict[str, Any]:
        """Make a DELETE request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call

        Returns:
            API response as dictionary
        """
        return await self.api_client.delete(endpoint=endpoint)


class JobsQueueClient:
    """
    Class handling endpoints for interacting with the Keboola Job Queue API. This class extends the Endpoint class
    from the kbcstorage library to leverage its core functionality, while using a different base URL
    and the same Storage API token for authentication.

    Attributes:
        base_url (str): The base URL for this endpoint.
        token (str): A key for the Storage API.
    """
    def __init__(self, api_client: AsyncKeboolaClient):
        self.api_client = api_client


    @classmethod
    def create(cls, root_url: str, token: str) -> 'JobsQueueClient':
        """
        Create a JobsQueue client.
        :param root_url: Root url of API. e.g. "https://queue.keboola.com/"
        :param token: A key for the Storage API. Can be found in the storage console.
        """
        return cls(api_client=AsyncKeboolaClient(base_api_url=root_url, api_token=token))


    async def detail(self, job_id: str) -> dict[str, Any]:
        """
        Retrieves information about a given job.
        :param job_id: The id of the job.
        """

        return await self.api_client.get(endpoint=f'jobs/{job_id}')

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
        Search for jobs based on the provided parameters.
        :param component_id: The id of the component.
        :param config_id: The id of the configuration.
        :param status: The status of the jobs to filter by.
        :param limit: The number of jobs to return.
        :param offset: The offset of the jobs to return.
        :param sort_by: The field to sort the jobs by.
        :param sort_order: The order to sort the jobs by.
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
        return await self._search(params=params)

    async def create_job(
        self,
        component_id: str,
        configuration_id: str,
    ) -> dict[str, Any]:
        """
        Create a new job.
        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :return: The response from the API call - created job or raise an error.
        """
        payload = {
            'component': component_id,
            'config': configuration_id,
            'mode': 'run',
        }
        return await self.api_client.post(endpoint='jobs', data=payload)

    async def _search(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Search for jobs based on the provided parameters.
        :param params: The parameters to search for.
        :param kwargs: Additional parameters to .requests.get method

        params (copied from the API docs):
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
        return await self.api_client.get(endpoint='search/jobs', params=params)


class DocsQuestionResponse(BaseModel):
    """The AI service response to a /docs/question request."""

    text: str = Field(description='Text of the answer to a documentation query.')
    source_urls: list[str] = Field(
        description='List of URLs to the sources of the answer.',
        default_factory=list,
        alias='sourceUrls',
    )


class AIServiceClient:
    """Class handling endpoints for interacting with the Keboola AI Service."""

    def __init__(self, api_client: AsyncKeboolaClient) -> None:
        """
        Creates an AIServiceClient.
        :param api_client: The API client to use.
        """
        self.api_client = api_client

    @classmethod
    def create(cls, root_url: str, token: str) -> 'AIServiceClient':
        """Creates an AIServiceClient from a Keboola Storage API token."""
        return cls(api_client=AsyncKeboolaClient(base_api_url=root_url, api_token=token))

    async def get_component_detail(self, component_id: str) -> dict[str, Any]:
        """
        Retrieves information about a given component.
        :param component_id: The id of the component.
        """
        return await self.api_client.get(endpoint=f'docs/components/{component_id}')

    async def docs_question(self, query: str) -> DocsQuestionResponse:
        """
        Answers a question using the Keboola documentation as a source.
        :param query: The query to answer.
        """
        response = await self.api_client.post(
            endpoint='docs/question',
            data={'query': query},
            headers={'Accept': 'application/json'},
        )

        return DocsQuestionResponse.model_validate(response)
