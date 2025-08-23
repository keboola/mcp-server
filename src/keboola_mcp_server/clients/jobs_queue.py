from typing import Any, Optional, cast

from keboola_mcp_server.clients.base import JsonDict, JsonList, KeboolaServiceClient, RawKeboolaClient


class JobsQueueClient(KeboolaServiceClient):
    """
    Async client for Keboola Job Queue API.
    """

    def __init__(self, raw_client: RawKeboolaClient, branch_id: str | None = None) -> None:
        """
        Creates a JobsQueueClient from a RawKeboolaClient and a branch id.

        :param raw_client: The raw client to use
        :param branch_id: The id of the branch
        """
        super().__init__(raw_client=raw_client)
        self._branch_id = branch_id

    @classmethod
    def create(
        cls, root_url: str, token: str, branch_id: str | None = None, headers: dict[str, Any] | None = None
    ) -> 'JobsQueueClient':
        """
        Creates a JobsQueue client.

        :param root_url: Root url of API. e.g. "https://queue.keboola.com/".
        :param token: The Keboola Storage API token
        :param branch_id: The id of the Keboola project branch to work on
        :param headers: Additional headers for the requests.
        :return: A new instance of JobsQueueClient.
        """
        return cls(
            raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token, headers=headers), branch_id=branch_id
        )

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
            'branchId': self._branch_id,
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
        if self._branch_id:
            payload['branchId'] = self._branch_id
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
