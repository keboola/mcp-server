import abc
import asyncio
import json
import logging
import time
import uuid
from typing import Any, Literal, Mapping, Sequence, cast
from urllib.parse import urlunparse

from httpx import HTTPStatusError
from pydantic import Field, TypeAdapter
from pydantic.dataclasses import dataclass

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient

LOG = logging.getLogger(__name__)


def _get_backend_path(table: Mapping[str, Any]) -> list[str] | None:
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
    db_name: str  # project_id in a BigQuery
    schema_name: str  # dataset in a BigQuery
    table_name: str
    quote_char: str = ''

    @property
    def identifier(self) -> str:
        """Returns the properly quoted database identifier."""
        return '.'.join(
            f'{self.quote_char}{n}{self.quote_char}' for n in [self.db_name, self.schema_name, self.table_name]
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

    def __init__(self, workspace_id: int) -> None:
        self._workspace_id = workspace_id

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
    async def get_table_info(
        self, table: Mapping[str, Any], backend_path: list[str] | None = None
    ) -> DbTableInfo | None:
        # TODO: use a pydantic class for the 'table' param
        pass

    @abc.abstractmethod
    async def execute_query(
        self, sql_query: str, *, max_rows: int | None = None, max_chars: int | None = None
    ) -> QueryResult:
        """
        Runs a given SQL query.

        :param sql_query: The SQL query to be executed.
        :param max_rows: The maximum number of rows to fetch from the query results. If None, no limit is applied.
        :param max_chars: The maximum number of chars to fetch from the query results. If None, no limit is applied.
        :return: The result of the executed query.
        """
        pass

    @abc.abstractmethod
    async def get_branch_id(self) -> str:
        """Returns the branch ID."""
        pass

    @classmethod
    def _dump(cls, json_data: Mapping[str, Any]) -> str:
        return json.dumps(json_data, ensure_ascii=False, separators=(',', ':'))


class _SnowflakeWorkspace(_Workspace):
    _PAGE_SIZE = 1_000

    def __init__(self, workspace_id: int, schema: str, client: KeboolaClient):
        super().__init__(workspace_id)
        self._schema = schema  # default schema created for the workspace
        self._client = client
        self._qsclient: QueryServiceClient | None = None

    def get_sql_dialect(self) -> str:
        return 'Snowflake'

    def get_quoted_name(self, name: str) -> str:
        return f'"{name}"'  # wrap name in double quotes

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

    async def get_table_info(
        self, table: Mapping[str, Any], backend_path: list[str] | None = None
    ) -> DbTableInfo | None:
        table_id = table['id']

        db_name: str | None = None
        schema_name: str | None = None
        table_name: str | None = None

        if source_table := table.get('sourceTable'):
            # a table linked from some other project
            schema_name, table_name = source_table['id'].rsplit(sep='.', maxsplit=1)
            source_project_id = source_table['project']['id']
            # sql = f"show databases like '%_{source_project_id}';"
            sql = (
                f'select "DATABASE_NAME" from "INFORMATION_SCHEMA"."DATABASES" '
                f'where "DATABASE_NAME" like \'%^_{source_project_id}\' escape \'^\';'
            )
            result = await self.execute_query(sql)
            if result.is_ok:
                if result.data and result.data.rows:
                    db_name = result.data.rows[0]['DATABASE_NAME']
                else:
                    LOG.warning(
                        f'No database found for {source_project_id} project: {sql}, SAPI response: {result}\n'
                        f'Table: {self._dump(table)}'
                    )
            else:
                LOG.error(f'Failed to run SQL: {sql}, SAPI response: {result}')

        else:
            bp = backend_path or _get_backend_path(table)
            if bp and len(bp) >= 2:
                db_name = bp[0]
                schema_name = bp[1]
                table_name = table['name']
            else:
                LOG.warning(f'No backendPath available for table {table_id}, cannot construct FQN')
                return None

        if db_name and schema_name and table_name:
            sql = (
                f'SELECT "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE" '
                f'FROM "INFORMATION_SCHEMA"."COLUMNS" '
                f'WHERE "TABLE_CATALOG" = \'{db_name}\' AND "TABLE_SCHEMA" = \'{schema_name}\' '
                f'AND "TABLE_NAME" = \'{table_name}\' '
                f'ORDER BY "ORDINAL_POSITION";'
            )
            result = await self.execute_query(sql)
            if result.is_ok:
                fqn = TableFqn(db_name, schema_name, table_name, quote_char='"')
                if result.data and result.data.rows:
                    return DbTableInfo(
                        id=table_id,
                        fqn=fqn,
                        columns={
                            row['COLUMN_NAME']: DbColumnInfo(
                                name=row['COLUMN_NAME'],
                                quoted_name=self.get_quoted_name(row['COLUMN_NAME']),
                                native_type=row['DATA_TYPE'],
                                nullable=row['IS_NULLABLE'] == 'YES',
                            )
                            for row in result.data.rows
                        },
                    )
                else:
                    # the sql shows the db_name, schema_name and table_name
                    LOG.warning(
                        f'No "{table_id}" table in the database: {sql}, SAPI response: {result}\n'
                        f'Table: {self._dump(table)}'
                    )

                    # The linked tables are not visible in INFORMATION_SCHEMA.COLUMNS. Fall back to count(*) query
                    # to verify whether the fqn is valid.
                    sql = f'SELECT count(*) FROM {fqn};'
                    result = await self.execute_query(sql)
                    if result.is_ok:
                        if result.data and result.data.rows:
                            return DbTableInfo(id=table_id, fqn=fqn, columns={})
                        else:
                            LOG.warning(
                                f'Unexpected empty result from count(*): {sql}, SAPI response: {result}\n'
                                f'Table: {self._dump(table)}'
                            )
                    else:
                        LOG.error(f'Failed to run SQL: {sql}, SAPI response: {result}')
            else:
                LOG.error(f'Failed to run SQL: {sql}, SAPI response: {result}')

        return None

    async def execute_query(
        self, sql_query: str, *, max_rows: int | None = None, max_chars: int | None = None
    ) -> QueryResult:
        if max_rows is not None and max_rows <= 0:
            raise ValueError('The "max_rows" must be a positive integer or None.')
        if max_chars is not None and max_chars <= 0:
            raise ValueError('The "max_chars" must be a positive integer or None.')

        if not self._qsclient:
            self._qsclient = await self._create_qs_client()

        ts_start = time.perf_counter()
        job_id = await self._qsclient.submit_job(statements=[sql_query], workspace_id=str(self.id))
        while (job_status := await self._qsclient.get_job_status(job_id)) and job_status['status'] not in [
            'completed',
            'failed',
            'canceled',
            'cancelled',
        ]:
            await asyncio.sleep(1)
            elapsed_time = time.perf_counter() - ts_start
            if elapsed_time > self._QUERY_TIMEOUT:
                # Cancel the query before raising timeout error
                reason = f'Query timeout exceeded after {elapsed_time:.2f} seconds'
                cancellation_confirmed, query_completed = await self._cancel_job_with_timeout(job_id, reason)

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
                    return QueryResult(status='error', data=None, message=message)
                elif status != 'completed':
                    raise ValueError(f'Unexpected query status: {status}')

                columns = [col['name'] for col in cast(list[JsonDict], results['columns'])]

            page_data = cast(list[list[Any]], results.get('data', []))
            if not page_data:
                break

            page_data = page_data[:rows_to_fetch]
            if max_chars is not None:
                for row in page_data:
                    chars = sum(len(str(v)) for v in row if v is not None)
                    if all_rows_chars + chars <= max_chars:
                        all_rows.append(row)
                        all_rows_chars += chars
                    else:
                        break
            else:
                all_rows.extend(page_data)

            if len(page_data) < rows_to_fetch:
                break

            if max_rows is not None and len(all_rows) >= max_rows:
                break

            if max_chars is not None and all_rows_chars >= max_chars:
                break

            offset += len(page_data)

        rows = [{col_name: value for col_name, value in zip(columns, row)} for row in all_rows]

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


class _BigQueryWorkspace(_Workspace):
    _BQ_FIELDS = {'_timestamp'}

    def __init__(self, workspace_id: int, dataset_id: str, project_id: str, client: KeboolaClient):
        super().__init__(workspace_id)
        self._dataset_id = dataset_id  # default dataset created for the workspace
        self._project_id = project_id
        self._client = client

    def get_sql_dialect(self) -> str:
        return 'BigQuery'

    def get_quoted_name(self, name: str) -> str:
        return f'`{name}`'  # wrap name in back tick

    async def get_table_info(
        self, table: Mapping[str, Any], backend_path: list[str] | None = None
    ) -> DbTableInfo | None:
        table_id = table['id']

        # BigQuery cannot query tables from other projects — linked bucket tables have sourceTable set
        if table.get('sourceTable'):
            return None

        bp = backend_path or _get_backend_path(table)
        if bp:
            # BigQuery backendPath is a single-element list containing the dataset name
            schema_name = bp[0].replace('.', '_').replace('-', '_')
            table_name = table['name']
        elif '.' in table_id:
            # fallback: derive schema from table_id when backendPath is unavailable
            schema_name, table_name = table_id.rsplit(sep='.', maxsplit=1)
            schema_name = schema_name.replace('.', '_').replace('-', '_')
        else:
            LOG.warning(f'No backendPath available for table {table_id}, cannot construct FQN')
            return None

        if schema_name and table_name:
            sql = (
                f'SELECT column_name, data_type, is_nullable '
                f'FROM `{self._project_id}`.`{schema_name}`.`INFORMATION_SCHEMA`.`COLUMNS` '
                f"WHERE table_name = '{table_name}' "
                f'ORDER BY ordinal_position;'
            )
            result = await self.execute_query(sql)
            if result.is_ok and result.data:
                return DbTableInfo(
                    id=table_id,
                    fqn=TableFqn(self._project_id, schema_name, table_name, quote_char='`'),
                    columns={
                        row['column_name']: DbColumnInfo(
                            name=row['column_name'],
                            quoted_name=self.get_quoted_name(row['column_name']),
                            native_type=row['data_type'],
                            nullable=row['is_nullable'] == 'YES',
                        )
                        for row in result.data.rows
                    },
                )
            else:
                LOG.error(f'Failed to run SQL: {sql}, SAPI response: {result}')

        return None

    async def execute_query(
        self, sql_query: str, *, max_rows: int | None = None, max_chars: int | None = None
    ) -> QueryResult:
        if max_rows is not None and max_rows <= 0:
            raise ValueError('The "max_rows" must be a positive integer or None.')
        if max_chars is not None and max_chars <= 0:
            raise ValueError('The "max_chars" must be a positive integer or None.')

        resp = await self._client.storage_client.workspace_query(workspace_id=self.id, query=sql_query)
        qr = cast(QueryResult, TypeAdapter(QueryResult).validate_python(resp))
        if qr.data:
            total_query_rows = len(qr.data.rows)
            max_rows = max_rows or total_query_rows
            if max_chars is not None:
                rows: list[SqlSelectDataRow] = []
                total_chars = 0
                for row in qr.data.rows[:max_rows]:
                    chars = sum(len(str(v)) for v in row.values() if v is not None)
                    if total_chars + chars <= max_chars:
                        rows.append(row)
                        total_chars += chars
            else:
                rows = cast(list[SqlSelectDataRow], qr.data.rows[:max_rows])

            qr = QueryResult(
                status=qr.status,
                data=SqlSelectData(columns=qr.data.columns, rows=rows),
                message=' '.join(
                    filter(None, [qr.message, self._SELECTED_ROWS_MSG.format(rows=len(rows), total=total_query_rows)])
                ),
            )

        return qr

    async def get_branch_id(self) -> str:
        return self._client.branch_id or 'default'


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
    MCP_META_KEY = 'KBC.McpServer.v2.workspaceId'
    MCP_WORKSPACE_COMPONENT_ID = 'keboola.mcp-server-tool'

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'WorkspaceManager':
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, WorkspaceManager), f'Expected WorkspaceManager, got: {instance}'
        return instance

    @classmethod
    async def create(cls, client: KeboolaClient, workspace_schema: str | None = None) -> 'WorkspaceManager':
        # We use the read-only workspace with access to all project data which lives in the production branch.
        # Hence, we need KeboolaClient bound to the production/default branch.
        prod_client = await client.with_branch_id(None)
        return cls(prod_client, workspace_schema)

    def __init__(self, client: KeboolaClient, workspace_schema: str | None = None):
        """
        Initializes the WorkspaceManager.

        :param client: The KeboolaClient bound to the production/default branch.
        :param workspace_schema: The schema of the workspace to use.
        """
        if client.branch_id is not None:
            raise ValueError(
                'WorkspaceManager cannot be created for a branch other than the production/default branch.'
            )
        self._client = client
        self._workspace_schema = workspace_schema
        self._workspace: _Workspace | None = None
        self._table_info_cache: dict[str, DbTableInfo] = {}

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
                raise e

    async def _find_ws_in_branch(self) -> _WspInfo | None:
        """Finds the workspace info in the current branch."""

        meta_key = self.MCP_META_KEY
        metadata = await self._client.storage_client.branch_metadata_get()
        for m in metadata:
            if m.get('key') == meta_key and (raw_value := m.get('value')):
                if (info := await self._find_ws_by_id(raw_value)) and info.readonly:
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

        component_id = self.MCP_WORKSPACE_COMPONENT_ID
        config_name = f'mcp-workspace-{uuid.uuid4().hex[:8]}'
        config_resp = await self._client.storage_client.configuration_create(
            component_id=component_id,
            name=config_name,
            description='Auto-created by MCP server for workspace billing.',
            configuration={},
        )
        config_id = str(config_resp['id'])

        try:
            resp = await self._client.storage_client.workspace_create_for_config(
                component_id=component_id,
                config_id=config_id,
                login_type=login_type,
                backend=default_backend,
                async_run=True,
                read_only_storage_access=True,
            )
        except Exception:
            try:
                await self._client.storage_client.configuration_delete(component_id, config_id)
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

        # create a new workspace and note its ID to the branch
        LOG.info('Creating workspace in the default branch.')
        if info := await self._create_ws():
            # All tokens share the same read-only workspace
            # Race conditions during initialization are acceptable (last-write-wins)
            meta = await self._client.storage_client.branch_metadata_update({self.MCP_META_KEY: info.id})
            LOG.info(f'Set metadata in the default branch: {meta}')
            # use the newly created workspace
            self._workspace = self._init_workspace(info)
            return self._workspace
        else:
            raise ValueError('Failed to initialize Keboola Workspace.')

    async def execute_query(
        self, sql_query: str, *, max_rows: int | None = None, max_chars: int | None = None
    ) -> QueryResult:
        workspace = await self._get_workspace()
        return await workspace.execute_query(sql_query, max_rows=max_rows, max_chars=max_chars)

    async def get_table_info(
        self, table: Mapping[str, Any], backend_path: list[str] | None = None
    ) -> DbTableInfo | None:
        # Alias tables (isAlias=true in the source project) are not queryable from any workspace backend
        if table.get('sourceTable', {}).get('isAlias'):
            return None

        table_id = table['id']
        if table_id in self._table_info_cache:
            return self._table_info_cache[table_id]

        workspace = await self._get_workspace()
        if info := await workspace.get_table_info(table, backend_path=backend_path):
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
