import asyncio
import logging
from typing import Any, cast

import httpx

from keboola_mcp_server.clients import KeboolaServiceClient, RawKeboolaClient
from keboola_mcp_server.clients.base import JsonDict

LOG = logging.getLogger(__name__)

# Some Query Service calls (e.g. large result fetches) can exceed the base client's default
# 60s read timeout. Give the MCP server's own HTTP calls to QS more headroom.
# Note: this does NOT change QS's own ~30s deadline for its internal call to Connection
# (e.g. workspace credential fetch) - that timeout lives in Query Service, not here.
# TODO: fixed 120s bump for now; move to a real config if per-deployment tuning is ever needed.
_QS_TIMEOUT = httpx.Timeout(connect=5.0, read=120.0, write=10.0, pool=5.0)

# submit_job is a POST, so the transport-level retry (base.py, GET/PUT/DELETE/etc only) never
# applies to it. Job submission can fail with 403 "Failed to get workspace credentials" when QS's
# own deadline to Connection is shorter than actual credential-provisioning time - a transient
# failure that happens before any job is created, so it's safe to retry. Scoped to this specific
# message (not any 403) so a real auth/permission failure isn't masked by pointless retries.
_SUBMIT_JOB_MAX_ATTEMPTS = 3
_SUBMIT_JOB_RETRY_DELAY_SECONDS = 1.0


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
                timeout=_QS_TIMEOUT,
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

        endpoint = f'branches/{self._branch_id}/workspaces/{workspace_id}/queries'
        for attempt in range(1, _SUBMIT_JOB_MAX_ATTEMPTS + 1):
            try:
                resp = cast(JsonDict, await self.post(endpoint=endpoint, data=payload))
                return resp['queryJobId']
            except httpx.HTTPStatusError as e:
                is_transient_credentials_failure = (
                    e.response.status_code == httpx.codes.FORBIDDEN and 'workspace credentials' in str(e).lower()
                )
                if not is_transient_credentials_failure or attempt == _SUBMIT_JOB_MAX_ATTEMPTS:
                    raise
                LOG.warning(
                    f'Job submission failed to fetch workspace credentials '
                    f'(attempt {attempt}/{_SUBMIT_JOB_MAX_ATTEMPTS}), retrying: workspace_id={workspace_id}'
                )
                await asyncio.sleep(_SUBMIT_JOB_RETRY_DELAY_SECONDS * attempt)
        raise AssertionError('unreachable: loop always returns or raises')

    async def get_job_status(self, job_id: str) -> JsonDict:
        """
        Gets the status of a job by its job ID.

        :param job_id: The unique identifier for the job whose status is being retrieved.
        :return: A dictionary containing the status details of the specified job and its SQL statements.
        """
        return cast(JsonDict, await self.get(endpoint=f'queries/{job_id}'))

    async def cancel_job(self, job_id: str, reason: str) -> JsonDict:
        """
        Cancels a running query job.

        :param job_id: The unique identifier for the query job to cancel.
        :param reason: The reason for cancellation (for audit trail).
        :return: The response from the API call.
        """
        payload: JsonDict = {'reason': reason}
        return cast(JsonDict, await self.post(endpoint=f'queries/{job_id}/cancel', data=payload))

    def build_cancel_url(self, job_id: str) -> str:
        """
        Returns the absolute URL clients should POST to in order to cancel the given query job.

        Used to surface the cancellation handle to MCP clients via a progress notification so
        that they can cancel the query directly against Query Service without routing through
        the originating MCP server replica. The endpoint accepts the same auth header the client
        already uses to talk to the MCP server (`X-StorageAPI-Token` or `Authorization: Bearer`).
        """
        return f'{self.raw_client.base_api_url}/queries/{job_id}/cancel'

    async def get_job_results(
        self, job_id: str, statement_id: str, *, offset: int | None = None, limit: int | None = None
    ) -> JsonDict:
        """
        Gets the results of a specific statement within a query job and returns data, rows affected count,
        and status information with pagination support.

        :param job_id: A unique identifier for the query job.
        :param statement_id: A unique identifier for the specific query statement within the job.
        :param offset: The offset of the first row to return.
        :param limit: The maximum number of rows to return.
        :return: The query statement results.
        """
        params: dict[str, Any] = {}
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['pageSize'] = limit
        return cast(JsonDict, await self.get(endpoint=f'queries/{job_id}/{statement_id}/results', params=params))
