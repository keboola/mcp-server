import abc
import asyncio
import json
import logging
import time
from typing import Any, Literal, Mapping, Optional, Sequence

from httpx import HTTPStatusError
from pydantic import Field, TypeAdapter
from pydantic.dataclasses import dataclass

from keboola_mcp_server.client import KeboolaClient

LOG = logging.getLogger(__name__)


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
    data: SqlSelectData | None = Field(None, description='Data selected by the SQL SELECT query.')
    message: str | None = Field(None, description='Either an error message or the information from non-SELECT queries.')

    @property
    def is_ok(self) -> bool:
        return self.status == 'ok'

    @property
    def is_error(self) -> bool:
        return not self.is_ok


class _Workspace(abc.ABC):
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
    async def get_table_fqn(self, table: Mapping[str, Any]) -> TableFqn | None:
        """Gets the fully qualified name of a Keboola table."""
        # TODO: use a pydantic class for the 'table' param
        pass

    @abc.abstractmethod
    async def execute_query(self, sql_query: str) -> QueryResult:
        """Runs a SQL SELECT query."""
        pass


class _SnowflakeWorkspace(_Workspace):
    def __init__(self, workspace_id: int, schema: str, client: KeboolaClient):
        super().__init__(workspace_id)
        self._schema = schema  # default schema created for the workspace
        self._client = client

    def get_sql_dialect(self) -> str:
        return 'Snowflake'

    def get_quoted_name(self, name: str) -> str:
        return f'"{name}"'  # wrap name in double quotes

    async def get_table_fqn(self, table: Mapping[str, Any]) -> TableFqn | None:
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
                f'where "DATABASE_NAME" like \'%_{source_project_id}\';'
            )
            result = await self.execute_query(sql)
            if result.is_ok and result.data and result.data.rows:
                db_name = result.data.rows[0]['DATABASE_NAME']
            else:
                LOG.error(f'Failed to run SQL: {sql}, SAPI response: {result}')

        else:
            sql = 'select CURRENT_DATABASE() as "current_database";'
            result = await self.execute_query(sql)
            if result.is_ok and result.data and result.data.rows:
                row = result.data.rows[0]
                db_name = row['current_database']
                if '.' in table_id:
                    # a table local in a project for which the snowflake connection/workspace is open
                    schema_name, table_name = table_id.rsplit(sep='.', maxsplit=1)
                else:
                    # a table not in the project, but in the writable schema created for the workspace
                    # TODO: we should never come here, because the tools for listing tables can only see
                    #  tables that are in the project
                    schema_name = self._schema
                    table_name = table['name']
            else:
                LOG.error(f'Failed to run SQL: {sql}, SAPI response: {result}')

        if db_name and schema_name and table_name:
            fqn = TableFqn(db_name, schema_name, table_name, quote_char='"')
            return fqn
        else:
            return None

    async def execute_query(self, sql_query: str) -> QueryResult:
        resp = await self._client.storage_client.workspace_query(workspace_id=self.id, query=sql_query)
        return TypeAdapter(QueryResult).validate_python(resp)


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

    async def get_table_fqn(self, table: Mapping[str, Any]) -> TableFqn | None:
        table_id = table['id']

        schema_name: str | None = None
        table_name: str | None = None

        if '.' in table_id:
            # a table local in a project for which the workspace is open
            schema_name, table_name = table_id.rsplit(sep='.', maxsplit=1)
            schema_name = schema_name.replace('.', '_').replace('-', '_')
        else:
            # a table not in the project, but in the writable schema created for the workspace
            # TODO: we should never come here, because the tools for listing tables can only see
            #  tables that are in the project
            schema_name = self._dataset_id
            table_name = table['name']

        if schema_name and table_name:
            fqn = TableFqn(self._project_id, schema_name, table_name, quote_char='`')
            return fqn
        else:
            return None

    async def execute_query(self, sql_query: str) -> QueryResult:
        resp = await self._client.storage_client.workspace_query(workspace_id=self.id, query=sql_query)
        return TypeAdapter(QueryResult).validate_python(resp)


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
    MCP_META_KEY = 'KBC.MCP.workspaceId'

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'WorkspaceManager':
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, WorkspaceManager), f'Expected WorkspaceManager, got: {instance}'
        return instance

    def __init__(self, client: KeboolaClient, workspace_schema: str | None = None):
        self._client = client
        self._workspace_schema = workspace_schema
        self._workspace: _Workspace | None = None
        self._table_fqn_cache: dict[str, TableFqn] = {}

    async def _find_ws_by_schema(self, schema: str) -> _WspInfo | None:
        """Finds the workspace info by its schema."""

        for sapi_wsp_info in await self._client.storage_client.workspace_list():
            assert isinstance(sapi_wsp_info, dict)
            wi = _WspInfo.from_sapi_info(sapi_wsp_info)
            if wi.id and wi.backend and wi.schema and wi.schema == schema:
                return wi

        return None

    async def _find_ws_by_id(self, workspace_id: int) -> _WspInfo | None:
        """Finds the workspace info by its ID."""

        try:
            sapi_wsp_info = await self._client.storage_client.workspace_detail(workspace_id)
            assert isinstance(sapi_wsp_info, dict)
            wi = _WspInfo.from_sapi_info(sapi_wsp_info)

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

        metadata = await self._client.storage_client.branch_metadata_get()
        for m in metadata:
            if m.get('key') == self.MCP_META_KEY:
                workspace_id = m.get('value')
                if workspace_id and (info := await self._find_ws_by_id(workspace_id)) and info.readonly:
                    return info

        return None

    async def _create_ws(self, *, timeout_sec: float = 300.0) -> _WspInfo | None:
        """
        Creates a new workspace in the current branch and returns its info.

        :param timeout_sec: The number of seconds to wait for the workspace creation job to finish.
        :return: The workspace info if the workspace was created successfully, None otherwise.
        """

        resp = await self._client.storage_client.workspace_create(async_run=True, read_only_storage_access=True)
        assert 'id' in resp, f'Expected job ID in response: {resp}'
        assert isinstance(resp['id'], int)

        job_id = resp['id']
        start_ts = time.perf_counter()
        LOG.info(f'Requested new workspace: job_id={job_id}, timeout={timeout_sec:.2f} seconds')

        while True:
            job_info = await self._client.storage_client.job_detail(job_id)
            job_status = job_info['status']

            duration = time.perf_counter() - start_ts
            LOG.info(f'Job info: job_id={job_id}, status={job_status}, '
                     f'duration={duration:.2f} seconds, timeout={timeout_sec:.2f} seconds')

            if job_info['status'] == 'success':
                assert 'results' in job_info, f'Expected `results` in job info: {job_info}'
                assert isinstance(job_info['results'], dict)
                assert 'id' in job_info['results'], f'Expected `id` in `results` in job info: {job_info}'
                assert isinstance(job_info['results']['id'], int)

                workspace_id = job_info['results']['id']
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
            # update the branch metadata with the workspace ID
            meta = await self._client.storage_client.branch_metadata_update({self.MCP_META_KEY: info.id})
            LOG.info(f'Set metadata in the default branch: {meta}')
            # use the newly created workspace
            self._workspace = self._init_workspace(info)
            return self._workspace
        else:
            raise ValueError('Failed to initialize Keboola Workspace.')

    async def execute_query(self, sql_query: str) -> QueryResult:
        workspace = await self._get_workspace()
        return await workspace.execute_query(sql_query)

    async def get_table_fqn(self, table: Mapping[str, Any]) -> Optional[TableFqn]:
        table_id = table['id']
        if table_id in self._table_fqn_cache:
            return self._table_fqn_cache[table_id]

        workspace = await self._get_workspace()
        fqn = await workspace.get_table_fqn(table)
        if fqn:
            self._table_fqn_cache[table_id] = fqn

        return fqn

    async def get_quoted_name(self, name: str) -> str:
        workspace = await self._get_workspace()
        return workspace.get_quoted_name(name)

    async def get_sql_dialect(self) -> str:
        workspace = await self._get_workspace()
        return workspace.get_sql_dialect()
