from typing import cast

from keboola_mcp_server.clients import KeboolaServiceClient, RawKeboolaClient
from keboola_mcp_server.clients.base import JsonDict


class QueryServiceClient(KeboolaServiceClient):

    def __init__(self, raw_client: RawKeboolaClient, branch_id: str) -> None:
        """
        Creates a QueryServiceClient from a RawKeboolaClient and a branch id.

        :param raw_client: The raw client to use
        :param branch_id: The id of the Keboola project branch to work on
        """
        super().__init__(raw_client=raw_client)
        self._branch_id: str = branch_id
        if not self._branch_id:
            raise ValueError('Branch id is required')
        if self._branch_id in ['default', 'main']:
            raise ValueError(f'The real branch id is required, got: "{self._branch_id}"')

    @property
    def branch_id(self) -> str:
        """Returns the real branch ID (no symbolic names such as 'default' or 'main')."""
        return self._branch_id

    @classmethod
    def create(
        cls,
        *,
        root_url: str,
        version: str = 'v1',
        branch_id: str,
        token: str | None,
        headers: JsonDict | None = None,
    ) -> 'QueryServiceClient':
        """
        Creates a QueryServiceClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API.
        :param version: The version of the API to use (default: 'v1').
        :param branch_id: The id of the Keboola project branch to work on.
        :param token: The Keboola Storage API token, If None, the client will not send any authorization header.
        :param headers: Additional headers for the requests.
        :return: A new instance of QueryServiceClient.
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=f'{root_url}/api/{version}',
                api_token=token,
                headers=headers,
            ),
            branch_id=branch_id,
        )

    async def submit_job(
        self, statements: list[str], workspace_id: str, actor_type: str | None = None, transactional: bool | None = None
    ) -> str:
        """
        Creates a new query job with SQL statements in the specified branch and workspace.

        :param statements: The SQL statements to be executed.
        :param workspace_id: The id of the Keboola project workspace to work on.
        :param actor_type: The type of actor to use -- 'user' or 'system'.
        :param transactional: Whether the job should be executed in a transaction.
        :return: The unique identifier of the submitted job.
        """
        payload: JsonDict = {'statements': statements}
        if actor_type:
            payload['actorType'] = actor_type
        if transactional is not None:
            payload['transactional'] = transactional
        resp = cast(
            JsonDict,
            await self.post(endpoint=f'branches/{self._branch_id}/workspaces/{workspace_id}/queries', data=payload),
        )
        return resp['queryJobId']

    async def get_job_status(self, job_id: str) -> JsonDict:
        """
        Gets the status of a job by its job ID.

        :param job_id: The unique identifier for the job whose status is being retrieved.
        :return: A dictionary containing the status details of the specified job and its SQL statements.
        """
        return cast(JsonDict, await self.get(endpoint=f'queries/{job_id}'))

    async def get_job_results(self, job_id: str, statement_id: str) -> JsonDict:
        """
        Gets the results of a specific statement within a query job and returns data, rows affected count,
        and status information with pagination support.

        :param job_id: A unique identifier for the query job.
        :param statement_id: A unique identifier for the specific query statement within the job.
        :return: The query statement results.
        """
        return cast(JsonDict, await self.get(endpoint=f'queries/{job_id}/{statement_id}/results'))
