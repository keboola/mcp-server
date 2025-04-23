"""Keboola Storage API client wrapper."""

import logging
import os
import tempfile

from typing import Annotated, Any, Dict, Mapping, Optional, cast, List


import httpx
from kbcstorage.client import Client
from kbcstorage.base import Endpoint

LOG = logging.getLogger(__name__)


class KeboolaClient:
    """Helper class to interact with Keboola Storage API and Job Queue API."""

    STATE_KEY = "sapi_client"
    # Prefixes for the storage and queue API URLs, we do not use http:// or https:// here since we split the storage
    # api url by `connection` word
    _PREFIX_STORAGE_API_URL = "connection."
    _PREFIX_QUEUE_API_URL = "https://queue."

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> "KeboolaClient":
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, KeboolaClient), f"Expected KeboolaClient, got: {instance}"
        return instance

    def __init__(
        self,
        storage_api_token: str,
        storage_api_url: str = "https://connection.keboola.com",
    ) -> None:
        """Initialize the client.

        Args:
            storage_api_token: Keboola Storage API token
            storage_api_url: Keboola Storage API URL
            queue_api_url: Keboola Job Queue API URL
        """
        self.token = storage_api_token
        # Ensure the base URL has a scheme
        if not storage_api_url.startswith(("http://", "https://")):
            storage_api_url = f"https://{storage_api_url}"

        # Construct the queue API URL from the storage API URL expecting the following format:
        # https://connection.REGION.keboola.com
        # Remove the prefix from the storage API URL https://connection.REGION.keboola.com -> REGION.keboola.com
        # and add the prefix for the queue API https://queue.REGION.keboola.com
        queue_api_url = (
            f"{self._PREFIX_QUEUE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}"
        )

        self.base_storage_api_url = storage_api_url
        self.base_queue_api_url = queue_api_url

        self.headers = {
            "X-StorageApi-Token": self.token,
            "Content-Type": "application/json",
            "Accept-encoding": "gzip",
        }
        # Initialize the official client for operations it handles well
        # The storage_client.jobs endpoint is for storage jobs
        # Use self.jobs_queue instead which provides access to the Job Queue API
        # that handles component/transformation jobs
        self.storage_client = Client(self.base_storage_api_url, self.token)

        self.jobs_queue = JobsQueue(self.base_queue_api_url, self.token)

    async def get(
        self,
        endpoint: str,
        params: Annotated[Optional[Dict[str, Any]], "Query parameters for the request"] = None,
    ) -> Dict[str, Any]:
        """Make a GET request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            params: Query parameters for the request

        Returns:
            API response as dictionary
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_storage_api_url}/v2/storage/{endpoint}",
                headers=self.headers,
                params=params,
            )
            response.raise_for_status()
            return cast(Dict[str, Any], response.json())

    async def post(
        self,
        endpoint: str,
        data: Annotated[
            Optional[Dict[str, Any]],
            "Request payload parameters as a dictionary.",
        ],
    ) -> Dict[str, Any]:
        """Make a POST request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_storage_api_url}/v2/storage/{endpoint}",
                headers=self.headers,
                json=data if data is not None else {},
            )
            response.raise_for_status()
            return cast(Dict[str, Any], response.json())

    async def put(
        self,
        endpoint: str,
        data: Annotated[
            Optional[Dict[str, Any]], "Request payload parameters as a dictionary."
        ] = None,
    ) -> Dict[str, Any]:
        """Make a PUT request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
        """
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"{self.base_storage_api_url}/v2/storage/{endpoint}",
                headers=self.headers,
                data=data if data is not None else {},
            )
            response.raise_for_status()
            return cast(Dict[str, Any], response.json())

    async def delete(
        self,
        endpoint: str,
        data: Annotated[
            Optional[Dict[str, Any]], "Request payload parameters as a dictionary."
        ] = None,
    ) -> Dict[str, Any]:
        """Make a DELETE request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
        """
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_storage_api_url}/v2/storage/{endpoint}",
                headers=self.headers,
            )
            response.raise_for_status()

            return cast(Dict[str, Any], response.json())

    async def download_table_data_async(self, table_id: str) -> str:
        """Download table data using the export endpoint.

        Args:
            table_id: ID of the table to download

        Returns:
            Table data as string
        """
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Get just the table name from the table_id
                table_name = table_id.split(".")[-1]
                # Export the table data
                self.storage_client.tables.export_to_file(table_id, temp_dir)
                # Read the exported file
                actual_file = os.path.join(temp_dir, table_name)
                with open(actual_file, "r") as f:
                    data = f.read()
                return data
        except Exception as e:
            LOG.error(f"Error downloading table {table_id}: {str(e)}")
            return f"Error downloading table: {str(e)}"


class JobsQueue(Endpoint):
    """
    Class handling endpoints for interacting with the Keboola Job Queue API. This class extends the Endpoint class
    from the kbcstorage library to leverage its core functionality, while using a different base URL
    and the same Storage API token for authentication.

    Attributes:
        base_url (str): The base URL for this endpoint.
        token (str): A key for the Storage API.
    """

    def __init__(self, root_url: str, token: str):
        """
        Create a JobsQueue endpoint.
        :param root_url: Root url of API. e.g. "https://queue.keboola.com/"
        :param token: A key for the Storage API. Can be found in the storage console.
        """
        super().__init__(root_url, "", token)

        # set the base url to the root url
        self.base_url = self.root_url.rstrip("/")

    def detail(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieves information about a given job.
        :param job_id: The id of the job.
        """
        url = f"{self.base_url}/jobs/{job_id}"

        return self._get(url)

    def search_jobs_by(
        self,
        component_id: Optional[str] = None,
        config_id: Optional[str] = None,
        status: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
        sort_by: Optional[str] = "startTime",
        sort_order: Optional[str] = "desc",
    ) -> Dict[str, Any]:
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
            "componentId": component_id,
            "configId": config_id,
            "status": status,
            "limit": limit,
            "offset": offset,
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }
        return self._search(params=params)

    def create_job(
        self,
        component_id: str,
        configuration_id: str,
    ) -> Dict[str, Any]:
        """
        Create a new job.
        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :return: The response from the API call - created job or raise an error.
        """
        url = f"{self.base_url}/jobs"
        payload = {
            "component": component_id,
            "config": configuration_id,
            "mode": "run",
        }
        return self._post(url, json=payload)

    def _search(self, params: Dict[str, Any], **kwargs) -> Dict[str, Any]:
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
        url = f"{self.base_url}/search/jobs"

        return self._get(url, params=params, **kwargs)
