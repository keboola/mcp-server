"""Keboola Storage API client wrapper."""

import logging
import os
import tempfile
from typing import Any, Dict, Optional, cast

import httpx
from kbcstorage.client import Client
from kbcstorage.base import Endpoint

logger = logging.getLogger(__name__)


class KeboolaClient:
    """Helper class to interact with Keboola Storage API and Job Queue API."""

    def __init__(
        self, storage_api_token: str, storage_api_url: str = "https://connection.keboola.com",
        queue_api_url: str = "https://queue.keboola.com"
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
        
        if not queue_api_url.startswith(("http://", "https://")):
            queue_api_url = f"https://{queue_api_url}"

        self.base_url = storage_api_url
        self.base_queue_api_url = queue_api_url

        self.headers = {
            "X-StorageApi-Token": self.token,
            "Content-Type": "application/json",
            "Accept-encoding": "gzip",
        }
        # Initialize the official client for operations it handles well
        # The storage_client.jobs endpoint is for legacy storage jobs
        # Use self.jobs_queue instead which provides access to the modern Job Queue API
        # that handles component/transformation jobs
        self.storage_client = Client(self.base_url, self.token)

        self.jobs_queue = JobsQueue(self.base_queue_api_url, self.token)
        

    async def get(self, endpoint: str) -> Dict[str, Any]:
        """Make a GET request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call

        Returns:
            API response as dictionary
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v2/storage/{endpoint}", headers=self.headers
            )
            response.raise_for_status()
            return cast(Dict[str, Any], response.json())

    async def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a POST request to Keboola Storage API.

        Args:
            endpoint: API endpoint to call
            data: Request payload

        Returns:
            API response as dictionary
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/storage/{endpoint}",
                headers=self.headers,
                json=data if data is not None else {},
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
            logger.error(f"Error downloading table {table_id}: {str(e)}")
            return f"Error downloading table: {str(e)}"


class JobsQueue(Endpoint):
    """
    Client for interacting with the Keboola Job Queue API. This class extends the Endpoint class
    from the kbcstorage library to leverage its core functionality, while using a different base URL
    and the same Storage API token for authentication.

    Attributes:
        base_url (str): The base URL for this endpoint.
        token (str): A key for the Storage API.
    """
    def __init__(self, root_url: str, token: str = None):
        """
        Create an JobsQueueClient.
        :param root_url: Root url of API. eg. "https://queue.keboola.com/"
        :param token: A key for the Storage API. Can be found in the storage console.
        """
        if not token:
            raise ValueError("Token is required.")
        if not root_url:
            raise ValueError("Root URL is required.")
        
        self.root_url = root_url
        # Rewrite the base url to remove the /v2/storage/ part
        self.base_url = self.root_url.rstrip("/")
        self.token = token
        self._auth_header = {'X-StorageApi-Token': self.token,
                             'Accept-Encoding': 'gzip',
                             'User-Agent': 'Keboola Job Queue API Python Client'}

    def list(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """
        List all jobs details.
        :param limit: Limit the number of jobs returned, default 100
        :param offset: Offset the number of jobs returned, page offset, default 0
        :return: The json from the HTTP response.
        :raise: requests.HTTPError: If the API request fails.
        """
        params = {"limit": limit, "offset": offset}

        return self.search(params)

    def detail(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieves information about a given job.
        :param job_id: The id of the job.
        :return: The json from the HTTP response.
        :raise: requests.HTTPError: If the API request fails.
        """
        url = '{}/jobs/{}'.format(self.base_url, job_id)

        return self._get(url)

    def search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search for jobs based on the provided parameters.
        :param params: The parameters to search for.
        :return: The json from the HTTP response.
        :raise: requests.HTTPError: If the API request fails.

        params:
            - id str/lsit[str]: Search jobs by id
            - runId str/list[str]: Search jobs by runId
            - branchId str/list[str]: Search jobs by branchId
            - tokenId str/list[str]: Search jobs by tokenId
            - tokenDescription str/list[str]: Search jobs by tokenDescription
            - componentId str/list[str]: Search jobs by componentId
            - component str/list[str]: Search jobs by componentId alias..
            - configId str/list[str]: Search jobs by configId
            - config str/list[str]: Search jobs by configId alias..
            - configRowIds str/list[str]: Search jobs by configRowIds
            - status str/list[str]: Search jobs by status
            - createdTimeFrom str: Jobs that were created after the given date
                e.g. "2021-01-01, -8 hours, -1 week,..."
            - createdTimeTo str: Jobs that were created before the given date
                e.g. "2021-01-01, today, last monday,..."
            - startTimeFrom str: Jobs that were started after the given date
                e.g. "2021-01-01, -8 hours, -1 week,..."
            - startTimeTo str: Jobs that were started before the given date
                e.g. "2021-01-01, today, last monday,..."
            - endTimeTo str: Jobs that were finished before the given date
                e.g. "2021-01-01, today, last monday,..."
            - endTimeFrom str: Jobs that were finished after the given date
                e.g. "2021-01-01, -8 hours, -1 week,..."
            - limit int: Limit the number of jobs returned, default 100
                e.g. 100
            - offset int: Offset the number of jobs returned, page offset, default 0
                e.g. 100
            - sortBy str: Sort the jobs by the given field, default "id"
                values: id, runId, projectId, branchId, componentId, configId, tokenDescription, status, createdTime,
                updatedTime, startTime, endTime, durationSeconds
            - sortOrder str: Sort the jobs by the given field, default "asc"
                values: asc, desc
        """
        url = '{}/search/jobs'.format(self.base_url)
        return self._get(url, params=params)
