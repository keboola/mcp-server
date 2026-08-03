import abc
import asyncio
import json
import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Literal, cast
from urllib.parse import urlunparse

from httpx import HTTPStatusError
from pydantic import Field
from pydantic.dataclasses import dataclass

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient
from keboola_mcp_server.clients.storage import AsyncStorageClient
from keboola_mcp_server.tools.storage_helpers import has_storage_branches

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobSubmittedInfo:
    """Information surfaced to the tool layer the moment a backend job becomes addressable.

    This fires immediately after Query Service returns a `queryJobId`. The tool layer turns
    this into an MCP `notifications/progress` so clients (e.g. Kai, Claude Code) can record
    the handle and use it to cancel out-of-band by POSTing to `cancellation_url` themselves,
    regardless of which MCP replica the cancel lands on.
    """

    job_id: str
    cancellation_url: str | None
    backend: str


# Async callback invoked exactly once per execute_query, immediately after the backend
# returns a job handle. Callbacks are best-effort: any exception raised inside is suppressed
# so a failed progress notification cannot abort the underlying query.
JobSubmittedCallback = Callable[[JobSubmittedInfo], Awaitable[None]]


def get_backend_path(table: Mapping[str, Any]) -> list[str] | None:
    """Extracts the backendPath from a table's bucket info if available."""
    bucket = table.get('bucket')
    if isinstance(bucket, dict):
        backend_path = bucket.get('backendPath')
        if isinstance(backend_path, list):
            return backend_path
    return None


@dataclass(frozen=True)
class TableFqn:
    """The properly quoted parts of a fully qualified table name."""

    # TODO: refactor this and probably use just a simple string
    # Snowflake FQNs are database.schema.table. BigQuery has no cross-project access, so the
    # database tier is meaningless there — `db_name` is empty and the FQN is just dataset.table.
    db_name: str  # database (Snowflake); empty for BigQuery
    schema_name: str  # schema (Snowflake); dataset (BigQuery)
    table_name: str
    quote_char: str = ''

    @property
    def identifier(self) -> str:
        """Returns the properly quoted database identifier."""
        return '.'.join(
            f'{self.quote_char}{n}{self.quote_char}' for n in [self.db_name, self.schema_name, self.table_name] if n
        )

    def __repr__(self) -> str:
        return self.identifier

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True)
class DbColumnInfo:
    name: str
    quoted_name: str
    native_type: str
    nullable: bool


@dataclass(frozen=True)
class DbTableInfo:
    id: str
    fqn: TableFqn
    columns: Mapping[str, DbColumnInfo]


QueryStatus = Literal['ok', 'error']
SqlSelectDataRow = Mapping[str, Any]


@dataclass(frozen=True)
class SqlSelectData:
    columns: Sequence[str] = Field(description='Names of the columns returned from SQL select.')
    rows: Sequence[SqlSelectDataRow] = Field(
        description='Selected rows, each row is a dictionary of column: value pairs.'
    )


@dataclass(frozen=True)
class QueryResult:
    status: QueryStatus = Field(description='Status of running the SQL query.')
    data: SqlSelectData | None = Field(default=None, description='Data selected by the SQL SELECT query.')
    message: str | None = Field(
        default=None, description='Either an error message or the information from non-SELECT queries.'
    )

    @property
    def is_ok(self) -> bool:
        return self.status == 'ok'

    @property
    def is_error(self) -> bool:
        return not self.is_ok


class _Workspace(abc.ABC):
    _QUERY_TIMEOUT = 300.0  # 5 minutes
    _CANCELLATION_TIMEOUT = 30.0  # 30 seconds to wait for cancellation
    _SELECTED_ROWS_MSG = 'Returning {rows} of {total} selected rows.'
    _PAGE_SIZE = 1_000

    @staticmethod
    def _next_poll_interval(elapsed_seconds: float) -> float:
        """Job-status polling interval for `execute_query`: fast at first, capped at 20s."""
        if elapsed_seconds < 10:
            return 1.0
        if elapsed_seconds < 30:
            return 2.0
        if elapsed_seconds < 120:
            return 5.0
        return 20.0

    def __init__(self, workspace_id: int, client: KeboolaClient) -> None:
        self._workspace_id = workspace_id
        self._client = client
        self._qsclient: QueryServiceClient | None = None

    @property
    def id(self) -> int:
        return self._workspace_id

    @abc.abstractmethod
    def get_sql_dialect(self) -> str:
        pass

    @abc.abstractmethod
    def get_quoted_name(self, name: str) -> str:
        pass

    @abc.abstractmethod
    async def get_table_info(self, table: Mapping[str, Any]) -> DbTableInfo | None:
        # TODO: use a pydantic class for the 'table' param
        pass

    async def _cancel_job_with_timeout(self, job_id: str, reason: str) -> tuple[bool, bool]:
        """
        Cancel a query job and poll until cancellation is confirmed.

        :param job_id: The query job ID to cancel.
        :param reason: The reason for cancellation (used in cancel request and logging).
        :return: Tuple of (cancellation_confirmed, query_completed).
                 cancellation_confirmed: True if cancellation was confirmed (or query completed),
                                        False if it failed or timed out.
                 query_completed: True if query completed successfully during cancellation polling,
                                 False otherwise.
        """
        try:
            await self._qsclient.cancel_job(job_id, reason=reason)
            LOG.info(f'Query cancellation requested: job_id={job_id}')

            # Poll for cancellation confirmation
            cancel_start = time.perf_counter()
            while True:
                job_status = await self._qsclient.get_job_status(job_id)
                if 'status' not in job_status:
                    LOG.warning(f'Query status response missing "status" field: job_id={job_id}')
                    return (False, False)
                status = job_status['status']

                if status == 'completed':
                    LOG.info(f'Query completed successfully during cancellation attempt: job_id={job_id}')
                    return (True, True)  # Cancellation confirmed, query completed
                elif status in ['failed', 'canceled', 'cancelled']:
                    LOG.info(f'Query job cancellation confirmed: job_id={job_id}, status={status}')
                    return (True, False)  # Cancellation confirmed, query not completed

                if time.perf_counter() - cancel_start > self._CANCELLATION_TIMEOUT:
                    LOG.info(
                        f'Query cancellation polling timed out after {self._CANCELLATION_TIMEOUT}s: '
                        f'job_id={job_id}, status={status}'
                    )
                    return (False, False)

                await asyncio.sleep(0.5)  # Poll every 500ms

        except HTTPStatusError as e:
            LOG.error(
                f'HTTP error during query cancellation: job_id={job_id}, '
                f'status_code={e.response.status_code}, error={e}'
            )
            return (False, False)
        except Exception:
            LOG.exception(f'Unexpected error during query cancellation: job_id={job_id}')
            return (False, False)

    async def execute_query(
        self,
        sql_query: str,
        *,
        max_rows: int | None = None,
        max_chars: int | None = None,
        on_job_submitted: JobSubmittedCallback | None = None,
    ) -> QueryResult:
        """
        Runs a given SQL query through the Query Service.

        The Query Service is backend-agnostic; the SQL itself must follow the dialect of the
        workspace backend (see :meth:`get_sql_dialect` / :meth:`get_quoted_name`).

        :param sql_query: The SQL query to be executed.
        :param max_rows: The maximum number of rows to fetch from the query results. If None, no limit is applied.
        :param max_chars: The maximum number of chars to fetch from the query results. If None, no limit is applied.
        :param on_job_submitted: Optional async callback invoked with the backend job handle as soon as the job is
            registered with the Query Service. Exceptions raised inside the callback are suppressed so a failed
            notification cannot abort the query.
        :return: The result of the executed query.
        """
        if max_rows is not None and max_rows <= 0:
            raise ValueError('The "max_rows" must be a positive integer or None.')
        if max_chars is not None and max_chars <= 0:
            raise ValueError('The "max_chars" must be a positive integer or None.')

        if not self._qsclient:
            self._qsclient = await self._create_qs_client()

        ts_start = time.perf_counter()
        job_id = await self._qsclient.submit_job(statements=[sql_query], workspace_id=str(self.id))
        # The job is now registered with Query Service, so everything from here on must run under
        # the CancelledError handler below: if the client cancels while we are still in the
        # `on_job_submitted` callback (e.g. emitting the progress notification), we must still
        # propagate the cancel to the backend rather than leak a running QS job.
        try:
            if on_job_submitted is not None:
                info = JobSubmittedInfo(
                    job_id=job_id,
                    cancellation_url=self._qsclient.build_cancel_url(job_id),
                    backend=self.get_sql_dialect().lower(),
                )
                # Best-effort: a failed progress notification must not kill the running query.
                # CancelledError is `BaseException` since Python 3.8, so `except Exception` already
                # lets it propagate on the supported Python (>=3.10). The explicit branch below
                # documents intent and re-raises so the outer CancelledError handler can cancel the
                # backend job; it also guards against a future refactor that might widen the catch
                # to `BaseException` and silently swallow cancellation.
                try:
                    await on_job_submitted(info)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOG.warning(f'on_job_submitted callback raised for job_id={job_id}: {exc!r} — continuing')
            while (job_status := await self._qsclient.get_job_status(job_id)) and job_status['status'] not in [
                'completed',
                'failed',
                'canceled',
                'cancelled',
            ]:
                elapsed_time = time.perf_counter() - ts_start
                # Back off polling frequency for long-running queries so a multi-minute query
                # isn't status-checked hundreds of times. Sleep is clamped to the time left so
                # it never overshoots the timeout, though the last status check before the
                # deadline may still land up to one full interval (max 20s) early.
                remaining = self._QUERY_TIMEOUT - elapsed_time
                sleep_for = max(min(self._next_poll_interval(elapsed_time), remaining), 0.0)
                await asyncio.sleep(sleep_for)
                elapsed_time = time.perf_counter() - ts_start
                if elapsed_time > self._QUERY_TIMEOUT:
                    # Cancel the query before raising timeout error. Inline the reason (rather than
                    # binding a `reason` local) so it can't be mistaken for an in-scope variable by
                    # the `except asyncio.CancelledError` handler below, which uses its own reason.
                    cancellation_confirmed, query_completed = await self._cancel_job_with_timeout(
                        job_id, f'Query timeout exceeded after {elapsed_time:.2f} seconds'
                    )

                    # If query completed during cancellation, fetch and return results
                    if query_completed:
                        LOG.info(f'Query completed during cancellation polling, returning results: job_id={job_id}')
                        # Break out of the polling loop to fetch results below
                        job_status = await self._qsclient.get_job_status(job_id)
                        break

                    # Query did not complete - raise timeout error
                    if cancellation_confirmed:
                        raise RuntimeError(
                            f'Query execution timed out after {elapsed_time:.2f} seconds. '
                            f'The query has been cancelled: job_id={job_id}'
                        )
                    else:
                        raise RuntimeError(
                            f'Query execution timed out after {elapsed_time:.2f} seconds. '
                            f'Cancellation was attempted but could not be confirmed. '
                            f'The query may still be running on the server: job_id={job_id}'
                        )
        except asyncio.CancelledError:
            # Client (e.g. MCP `notifications/cancelled`) cancelled the in-flight tool call.
            # Propagate the cancel to the backend so the query doesn't keep scanning data.
            # `asyncio.shield` keeps the cancel HTTP call alive even though our own task
            # is being cancelled; without it the request would be torn down immediately.
            LOG.info(f'Query cancelled by client: job_id={job_id}')
            try:
                await asyncio.shield(self._cancel_job_with_timeout(job_id, reason='Client cancelled the request'))
            except asyncio.CancelledError:
                # Outer scope was cancelled again while the shielded cancel was still running;
                # we did our best — let the original CancelledError propagate below.
                pass
            raise

        # Short-circuit when the poll loop exited because the job was cancelled out-of-band
        # (e.g. the user clicked STOP and the kai-agent backend POSTed
        # `POST /api/v1/queries/{job_id}/cancel` directly to Query Service, or the in-flight
        # request itself was aborted with notifications/cancelled). Going through the results
        # fetch path here surfaces QS's generic "Job is still running or not completed yet"
        # message, which is misleading — we already know the job reached a terminal CANCELLED
        # state. Return a clear cancel result instead and skip the results fetch entirely.
        terminal_status = job_status['status']
        if terminal_status in ('canceled', 'cancelled'):
            LOG.info(f'Query was cancelled (terminal status={terminal_status}): job_id={job_id}')
            return QueryResult(status='error', data=None, message='Query was cancelled')

        statement_id = cast(list[JsonDict], job_status['statements'])[0]['id']

        # Fetch results with pagination
        all_rows: list[list[Any]] = []
        all_rows_chars: int = 0
        columns: list[str] = []
        offset = 0
        page_size = self._PAGE_SIZE
        message: str | None = None
        total_query_rows: int | None = None

        while True:
            if max_rows is not None:
                remaining = max_rows - len(all_rows)
                if remaining <= 0:
                    break
                rows_to_fetch = min(page_size, remaining)
            else:
                rows_to_fetch = page_size

            results = await self._qsclient.get_job_results(
                job_id,
                statement_id,
                offset=offset,
                limit=max(rows_to_fetch, 100),  # QueryService requires 100 - 10_000
            )

            # Store message, total_query_rows and columns from the first response
            if offset == 0:
                status = results['status']
                message = results['message']
                total_query_rows = results.get('numberOfRows')

                if status in ['failed', 'canceled', 'cancelled']:
                    return QueryResult(status='error', data=None, message=self._format_error_message(message))
                elif status != 'completed':
                    raise ValueError(f'Unexpected query status: {status}')

                columns = [col['name'] for col in cast(list[JsonDict], results['columns'])]

            page_data = cast(list[list[Any]], results.get('data', []))
            if not page_data:
                break

            page_data = page_data[:rows_to_fetch]
            char_limit_reached = False
            if max_chars is not None:
                for row in page_data:
                    chars = sum(len(str(v)) for v in row if v is not None)
                    if all_rows_chars + chars <= max_chars:
                        all_rows.append(row)
                        all_rows_chars += chars
                    else:
                        # The first row that does not fit ends pagination so that the result
                        # is a contiguous prefix; we must not skip this row and then append
                        # later smaller rows that happen to fit.
                        char_limit_reached = True
                        break
            else:
                all_rows.extend(page_data)

            if len(page_data) < rows_to_fetch:
                break

            if max_rows is not None and len(all_rows) >= max_rows:
                break

            if char_limit_reached or (max_chars is not None and all_rows_chars >= max_chars):
                break

            offset += len(page_data)

        rows = [dict(zip(columns, row)) for row in all_rows]

        if columns:
            message = ' '.join(
                filter(None, [message, self._SELECTED_ROWS_MSG.format(rows=len(rows), total=total_query_rows)])
            )
            query_result = QueryResult(status='ok', data=SqlSelectData(columns=columns, rows=rows), message=message)
        else:
            query_result = QueryResult(status='ok', message=message)

        return query_result

    async def get_branch_id(self) -> str:
        if not self._qsclient:
            self._qsclient = await self._create_qs_client()
        return self._qsclient.branch_id

    async def _create_qs_client(self) -> QueryServiceClient:
        """
        Creates a QueryServiceClient for the workspace.

        Note: Currently, QueryServiceClient is not cached and sessions are not used, so bearer token
        expiration is not an issue. If sessions and caching are reintroduced in the future, token
        expiration handling will need to be considered.
        """
        real_branch_id = self._client.branch_id
        if not real_branch_id:
            for branch in await self._client.storage_client.branches_list():
                if (is_default := branch.get('isDefault')) and isinstance(is_default, bool) and is_default:
                    real_branch_id = branch['id']
                    break
        if not real_branch_id:
            raise RuntimeError('Cannot determine the default branch ID')

        # Prefer bearer token over storage token for Query Service
        token = f'Bearer {self._client.bearer_token}' if self._client.bearer_token else self._client.token

        return QueryServiceClient.create(
            root_url=urlunparse(('https', f'query.{self._client.hostname_suffix}', '', '', '', '')),
            branch_id=real_branch_id,
            token=token,
            headers=self._client.headers,
        )

    def _format_error_message(self, message: str | None) -> str | None:
        """
        Normalizes a failed-query error message returned by the Query Service into a clean,
        human-readable string. The base implementation passes the message through unchanged;
        backends whose Query Service responses wrap the error may override this.
        """
        return message

    @classmethod
    def _dump(cls, json_data: Mapping[str, Any]) -> str:
        return json.dumps(json_data, ensure_ascii=False, separators=(',', ':'))


class _SnowflakeWorkspace(_Workspace):
    def __init__(self, workspace_id: int, schema: str, client: KeboolaClient):
        super().__init__(workspace_id, client)
        self._schema = schema  # default schema created for the workspace

    def get_sql_dialect(self) -> str:
        return 'Snowflake'

    def get_quoted_name(self, name: str) -> str:
        return f'"{name}"'  # wrap name in double quotes

    async def get_table_info(self, table: Mapping[str, Any]) -> DbTableInfo | None:
        table_id = table['id']

        # The table's own bucket backendPath resolves to the database + schema where the table
        # physically lives. For a linked bucket — including a materialized alias shared from another
        # project — Storage propagates that backendPath onto the linked table itself, so the FQN is
        # directly queryable from this workspace.
        bp = get_backend_path(table)
        if not bp or len(bp) < 2:
            LOG.warning(f'No backendPath available for table {table_id}, cannot construct FQN')
            return None

        return DbTableInfo(
            id=table_id,
            fqn=TableFqn(bp[0], bp[1], table['name'], quote_char='"'),
            columns={},
        )


class _BigQueryWorkspace(_Workspace):
    # The Query Service surfaces BigQuery errors as a serialized error object, e.g.
    #   {Location: "query"; Message: "Syntax error: Unexpected identifier ..."; Reason: "invalidQuery"}
    # Extract the human-readable `Message: "..."` part so the error reads like Snowflake's plain text.
    _BQ_ERROR_MESSAGE_RE = re.compile(r'Message:\s*"((?:[^"\\]|\\.)*)"')

    def __init__(self, workspace_id: int, dataset_id: str, project_id: str, client: KeboolaClient):
        super().__init__(workspace_id, client)
        self._dataset_id = dataset_id  # default dataset created for the workspace
        self._project_id = project_id

    def get_sql_dialect(self) -> str:
        return 'BigQuery'

    def get_quoted_name(self, name: str) -> str:
        return f'`{name}`'  # wrap name in back tick

    async def get_table_info(self, table: Mapping[str, Any]) -> DbTableInfo | None:
        table_id = table['id']

        # BigQuery has no cross-project data sharing: a table that is an alias in its source project
        # (sourceTable.isAlias) is not materialized into this project's dataset and cannot be queried
        # from this workspace. Materialized aliases are a Snowflake-only capability.
        if table.get('sourceTable', {}).get('isAlias'):
            return None

        bp = get_backend_path(table)
        if not bp:
            LOG.warning(f'No backendPath available for table {table_id}, cannot construct FQN')
            return None

        # BigQuery backendPath[0] is the dataset name; normalize separators for BQ dataset naming.
        # BigQuery has no cross-project access, so the FQN is dataset.table with no project/database
        # tier (db_name is left empty) — see editor-service SapiDataProvider::parseBackendPath.
        schema_name = bp[0].replace('.', '_').replace('-', '_')
        table_name = table['name']

        return DbTableInfo(
            id=table_id,
            fqn=TableFqn(db_name='', schema_name=schema_name, table_name=table_name, quote_char='`'),
            columns={},
        )

    def _format_error_message(self, message: str | None) -> str | None:
        if message and (m := self._BQ_ERROR_MESSAGE_RE.search(message)):
            return m.group(1).replace('\\"', '"')
        return message


@dataclass(frozen=True)
class _WspInfo:
    id: int
    schema: str
    backend: str
    credentials: str | None  # the backend credentials; it can contain serialized JSON data
    readonly: bool | None

    @staticmethod
    def from_sapi_info(sapi_wsp_info: Mapping[str, Any]) -> '_WspInfo':
        _id = sapi_wsp_info.get('id')
        backend = sapi_wsp_info.get('connection', {}).get('backend')
        _schema = sapi_wsp_info.get('connection', {}).get('schema')
        credentials = sapi_wsp_info.get('connection', {}).get('user')
        readonly = sapi_wsp_info.get('readOnlyStorageAccess')
        return _WspInfo(id=_id, schema=_schema, backend=backend, credentials=credentials, readonly=readonly)


class WorkspaceManager:
    STATE_KEY = 'workspace_manager'
    MCP_WORKSPACE_COMPONENT_ID = 'keboola.mcp-server-tool'

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'WorkspaceManager':
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, WorkspaceManager), f'Expected WorkspaceManager, got: {instance}'
        return instance

    @classmethod
    async def create(
        cls,
        client: KeboolaClient,
        workspace_schema: str | None = None,
        kubernetes_token_path: str | None = None,
        workspace_id: str | int | None = None,
    ) -> 'WorkspaceManager':
        # On projects with the `storage-branches` feature, each dev branch needs its own
        # workspace so the agent's queries (FQN paths, `query_data`) see that branch's
        # table versions. The workspace ID is stored under the same metadata key but in
        # the branch's own metadata, which is per-branch (`branch/{id}/metadata`).
        # On legacy projects (no `storage-branches`) and on the default branch, fall back
        # to the production-branch workspace shared by the whole project.
        # `has_storage_branches` already requires `branch_id is not None`, so the default
        # branch always takes the prod-client path.
        if await has_storage_branches(client):
            return cls(client, workspace_schema, kubernetes_token_path=kubernetes_token_path, workspace_id=workspace_id)
        prod_client = await client.with_branch_id(None)
        return cls(
            prod_client, workspace_schema, kubernetes_token_path=kubernetes_token_path, workspace_id=workspace_id
        )

    def __init__(
        self,
        client: KeboolaClient,
        workspace_schema: str | None = None,
        kubernetes_token_path: str | None = None,
        workspace_id: str | int | None = None,
    ):
        """
        Initializes the WorkspaceManager.

        :param client: The KeboolaClient bound to the branch whose workspace this manager
            owns. On default-branch or legacy-project paths this is the production-branch
            client; on a `storage-branches` project bound to a dev branch this is the
            dev-branch client (see :meth:`create`).
        :param workspace_schema: The schema of the workspace to use.
        :param kubernetes_token_path: Optional path to the projected Kubernetes
            ServiceAccount token. When set, workspace provisioning requests carry the
            JWT as the X-Kubernetes-Authorization step-up header alongside the user's
            own token, so Connection can waive permissions the user's token lacks
            (e.g. read-only users).
        :param workspace_id: The ID of the workspace to use (e.g. a Data App's own workspace).
            Takes precedence over `workspace_schema` when both are set.
        """
        self._client = client
        self._workspace_schema = workspace_schema
        self._workspace_id = workspace_id
        self._kubernetes_token_path = kubernetes_token_path
        self._provisioning_client: AsyncStorageClient | None = None
        self._workspace: _Workspace | None = None
        self._table_info_cache: dict[str, DbTableInfo] = {}

    async def _provisioning_storage_client(self) -> AsyncStorageClient:
        """
        Returns the Storage client used for workspace provisioning (configuration and
        workspace creation).

        When a Kubernetes ServiceAccount token path is configured (Keboola-deployed MCP
        server), the provisioning client keeps the user's own Storage token and
        additionally sends the projected SA JWT as the X-Kubernetes-Authorization
        step-up header — Connection waives permissions the user's token lacks when the
        ServiceAccount is authorized for workspace provisioning. No privileged token
        is ever minted; the audit trail stays on the user's token.
        Otherwise the user's own Storage client is used unchanged. The SA JWT is attached
        only when this manager's client talks to the server's own stack;
        `KeboolaClient.step_up_storage_client()` falls back to the user's own client
        otherwise.

        The step-up client is cached for this manager's lifetime, so the token file is
        read once — when the client is first built — not on every provisioning attempt.
        Provisioning happens at most once per manager, well within the projected token's
        rotation window; a fresh manager (new session) re-reads the file, so kubelet
        rotation is picked up without restarting the server.
        """
        if not self._kubernetes_token_path:
            return self._client.storage_client
        if self._provisioning_client is None:
            self._provisioning_client = self._client.step_up_storage_client(self._kubernetes_token_path)
            LOG.debug('Workspace provisioning storage client created.')
        return self._provisioning_client

    async def _find_ws_by_schema(self, schema: str) -> _WspInfo | None:
        """Finds the workspace info by its schema."""

        for sapi_wsp_info in await self._client.storage_client.workspace_list():
            assert isinstance(sapi_wsp_info, dict)
            wi = _WspInfo.from_sapi_info(sapi_wsp_info)  # type: ignore[attr-defined]
            if wi.id and wi.backend and wi.schema and wi.schema == schema:
                return wi

        return None

    async def _find_ws_by_id(self, workspace_id: str | int) -> _WspInfo | None:
        """Finds the workspace info by its ID."""

        try:
            sapi_wsp_info = await self._client.storage_client.workspace_detail(workspace_id)
            assert isinstance(sapi_wsp_info, dict)
            wi = _WspInfo.from_sapi_info(sapi_wsp_info)  # type: ignore[attr-defined]

            if wi.id and wi.backend and wi.schema:
                return wi
            else:
                raise ValueError(f'Invalid workspace info: {sapi_wsp_info}')

        except HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise

    async def _find_ws_in_branch(self) -> _WspInfo | None:
        """Finds the shared read-only MCP workspace in the current branch.

        The MCP server creates its workspace under a configuration of the
        MCP_WORKSPACE_COMPONENT_ID component, so it is rediscovered by listing that
        component's configurations and fetching each config's workspaces through the
        config-scoped endpoint, then matching read-only storage access. This needs only
        read access to the MCP component's own configs — no project-wide workspace
        listing, no branch-metadata pointer, and therefore no elevated metadata write
        that a read-only user's token would be denied.
        """
        component_id = self.MCP_WORKSPACE_COMPONENT_ID
        for config in await self._client.storage_client.configuration_list(component_id):
            config_id = str(config['id'])
            for sapi_wsp_info in await self._client.storage_client.workspace_list_for_config(component_id, config_id):
                assert isinstance(sapi_wsp_info, dict)
                info = _WspInfo.from_sapi_info(sapi_wsp_info)
                if info.id and info.backend and info.schema and info.readonly:
                    return info

        return None

    async def _create_ws(self, *, timeout_sec: float = 300.0) -> _WspInfo | None:
        """
        Creates a new workspace under a component configuration and returns its info.

        The workspace is created under the MCP_WORKSPACE_COMPONENT_ID component so that
        it is correctly attributed for billing. This method creates the configuration,
        creates the workspace under it, and cleans up the configuration on failure.

        :param timeout_sec: The number of seconds to wait for the workspace creation job to finish.
        :return: The workspace info if the workspace was created successfully, None otherwise.
        """

        # Verify token before creating workspace to ensure it has proper permissions
        token_info = await self._client.storage_client.verify_token()

        # Check for defaultBackend parameter in token info under owner object
        owner_info = token_info.get('owner', {})
        default_backend = owner_info.get('defaultBackend')

        if default_backend == 'snowflake':
            login_type = 'snowflake-person-sso'
        elif default_backend == 'bigquery':
            login_type = 'default'
        else:
            raise ValueError(f'Unexpected default backend: {default_backend}')

        provisioning_client = await self._provisioning_storage_client()

        component_id = self.MCP_WORKSPACE_COMPONENT_ID
        config_name = f'mcp-workspace-{uuid.uuid4().hex[:8]}'
        config_resp = await provisioning_client.configuration_create(
            component_id=component_id,
            name=config_name,
            description='Auto-created by MCP server for workspace billing.',
            configuration={},
        )
        config_id = str(config_resp['id'])

        try:
            resp = await provisioning_client.workspace_create_for_config(
                component_id=component_id,
                config_id=config_id,
                login_type=login_type,
                backend=default_backend,
                async_run=True,
                read_only_storage_access=True,
            )
        except Exception:
            try:
                await provisioning_client.configuration_delete(component_id, config_id)
            except Exception as cleanup_err:
                LOG.warning(
                    f'Failed to clean up configuration {component_id}/{config_id} '
                    f'after workspace creation failure: {cleanup_err}',
                    exc_info=True,
                )
            raise

        assert 'id' in resp, f'Expected job ID in response: {resp}'
        assert isinstance(resp['id'], int)

        job_id = resp['id']
        start_ts = time.perf_counter()
        LOG.info(f'Requested new workspace: job_id={job_id}, timeout={timeout_sec:.2f} seconds')

        while True:
            job_info = await self._client.storage_client.job_detail(job_id)
            job_status = job_info['status']

            duration = time.perf_counter() - start_ts
            LOG.info(
                f'Job info: job_id={job_id}, status={job_status}, '
                f'duration={duration:.2f} seconds, timeout={timeout_sec:.2f} seconds'
            )

            if job_info['status'] == 'success':
                assert 'results' in job_info, f'Expected `results` in job info: {job_info}'
                job_results = job_info['results']
                assert isinstance(job_results, dict)
                assert 'id' in job_results, f'Expected `id` in `results` in job info: {job_info}'
                assert isinstance(job_results['id'], int)

                workspace_id = job_results['id']
                LOG.info(f'Created workspace: {workspace_id}')
                return await self._find_ws_by_id(workspace_id)

            elif duration > timeout_sec:
                LOG.info(f'Workspace creation timed out after {duration:.2f} seconds.')
                return None

            else:
                remaining_time = max(0.0, timeout_sec - duration)
                await asyncio.sleep(min(5.0, remaining_time))

    def _init_workspace(self, info: _WspInfo) -> _Workspace:
        """Creates a new `Workspace` instance based on the workspace info."""

        if info.backend == 'snowflake':
            return _SnowflakeWorkspace(workspace_id=info.id, schema=info.schema, client=self._client)

        elif info.backend == 'bigquery':
            credentials = json.loads(info.credentials or '{}')
            if project_id := credentials.get('project_id'):
                return _BigQueryWorkspace(
                    workspace_id=info.id,
                    dataset_id=info.schema,
                    project_id=project_id,
                    client=self._client,
                )

            else:
                raise ValueError(f'No credentials or no project ID in workspace: {info.schema}')

        else:
            raise ValueError(f'Unexpected backend type "{info.backend}" in workspace: {info.schema}')

    async def _get_workspace(self) -> _Workspace:
        if self._workspace:
            return self._workspace

        if self._workspace_id:
            # use the workspace that was explicitly requested (e.g. a Data App's own workspace)
            # this workspace must never be written to the default branch metadata
            LOG.info(f'Looking up workspace by id: {self._workspace_id}')
            if info := await self._find_ws_by_id(self._workspace_id):
                LOG.info(f'Found workspace: {info}')
                self._workspace = self._init_workspace(info)
                return self._workspace
            else:
                raise ValueError(f'No Keboola workspace found: workspace_id={self._workspace_id}')

        if self._workspace_schema:
            # use the workspace that was explicitly requested
            # this workspace must never be written to the default branch metadata
            LOG.info(f'Looking up workspace by schema: {self._workspace_schema}')
            if info := await self._find_ws_by_schema(self._workspace_schema):
                LOG.info(f'Found workspace: {info}')
                self._workspace = self._init_workspace(info)
                return self._workspace
            else:
                raise ValueError(
                    f'No Keboola workspace found or the workspace has no read-only storage access: '
                    f'workspace_schema={self._workspace_schema}'
                )

        LOG.info('Looking up workspace in the default branch.')
        if info := await self._find_ws_in_branch():
            # use the workspace that has already been created by the MCP server and noted to the branch
            LOG.info(f'Found workspace: {info}')
            self._workspace = self._init_workspace(info)
            return self._workspace

        # create a new workspace under the MCP component
        LOG.info('Creating workspace in the default branch.')
        if info := await self._create_ws():
            # All tokens share the same read-only workspace, rediscovered by its
            # component id (see _find_ws_in_branch) — no branch-metadata pointer is
            # written, so no elevated metadata write is needed. Concurrent first-use
            # may create more than one workspace; that is acceptable, _find_ws_in_branch
            # returns the first match on the next lookup.
            self._workspace = self._init_workspace(info)
            return self._workspace
        else:
            raise ValueError('Failed to initialize Keboola Workspace.')

    async def execute_query(
        self,
        sql_query: str,
        *,
        max_rows: int | None = None,
        max_chars: int | None = None,
        on_job_submitted: JobSubmittedCallback | None = None,
    ) -> QueryResult:
        workspace = await self._get_workspace()
        return await workspace.execute_query(
            sql_query,
            max_rows=max_rows,
            max_chars=max_chars,
            on_job_submitted=on_job_submitted,
        )

    async def get_table_info(self, table: Mapping[str, Any]) -> DbTableInfo | None:
        # Whether an alias table is queryable depends on the backend (Snowflake materializes aliases
        # from linked buckets, BigQuery does not), so each workspace implementation makes that call.
        table_id = table['id']
        if table_id in self._table_info_cache:
            return self._table_info_cache[table_id]

        workspace = await self._get_workspace()
        if info := await workspace.get_table_info(table):
            self._table_info_cache[table_id] = info

        return info

    async def get_quoted_name(self, name: str) -> str:
        workspace = await self._get_workspace()
        return workspace.get_quoted_name(name)

    async def get_sql_dialect(self) -> str:
        workspace = await self._get_workspace()
        return workspace.get_sql_dialect()

    async def get_workspace_id(self) -> int:
        workspace = await self._get_workspace()
        return workspace.id

    async def get_branch_id(self) -> str:
        workspace = await self._get_workspace()
        return await workspace.get_branch_id()
