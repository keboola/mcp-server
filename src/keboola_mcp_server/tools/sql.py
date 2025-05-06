import abc
import csv
import json
import logging
from io import StringIO
from typing import Annotated, Any, Literal, Mapping, Optional, Sequence

from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import Client, Row
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field, TypeAdapter
from pydantic.dataclasses import dataclass

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors

LOG = logging.getLogger(__name__)


def add_sql_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(query_table)
    mcp.add_tool(get_sql_dialect)
    LOG.info('SQL tools added to the MCP server.')


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

    def __repr__(self):
        return self.identifier

    def __str__(self):
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
    def __init__(self, workspace_id: str) -> None:
        self._workspace_id = workspace_id

    @property
    def id(self) -> str:
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
    def __init__(self, workspace_id: str, schema: str, client: KeboolaClient):
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
        resp = await self._client.storage_client.post(
            f'branch/default/workspaces/{self.id}/query', {'query': sql_query}
        )
        return TypeAdapter(QueryResult).validate_python(resp)


class _BigQueryWorkspace(_Workspace):
    _BQ_FIELDS = {'_timestamp'}

    def __init__(self, workspace_id: str, dataset_id: str, project_id: str):
        super().__init__(workspace_id)
        self._dataset_id = dataset_id  # default dataset created for the workspace
        self._project_id = project_id

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
        # TODO: make this code async; Google's BigQuery client for python doesn't seem to use async/await,
        #  but it provides callbacks.
        try:
            client = Client()
            bq_job = client.query(query=sql_query)  # API request
            bq_result = bq_job.result()  # Waits for query to finish

            columns: dict[str, Any] = {}  # unique column names, keeps insertion order
            data: list[Mapping[str, Any]] = []
            for bq_row in bq_result:
                assert isinstance(bq_row, Row)
                for k in bq_row.keys():
                    columns[k] = None
                data.append({field: value for field, value in bq_row.items() if field not in self._BQ_FIELDS})

            result = QueryResult(status='ok', data=SqlSelectData(columns=list(columns.keys()), rows=data))

        except BadRequest as e:
            LOG.exception(f'Failed to run query: {sql_query}')
            result = QueryResult(status='error', message=str(e))

        return result


class WorkspaceManager:
    STATE_KEY = 'workspace_manager'

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'WorkspaceManager':
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, WorkspaceManager), f'Expected WorkspaceManager, got: {instance}'
        return instance

    def __init__(self, client: KeboolaClient, workspace_schema: str):
        self._client = client
        self._workspace_schema = workspace_schema
        self._workspace: _Workspace | None = None
        self._table_fqn_cache: dict[str, TableFqn] = {}

    async def _get_workspace(self) -> _Workspace:
        if self._workspace:
            return self._workspace

        for wsp_info in await self._client.storage_client.get('workspaces'):
            assert isinstance(wsp_info, dict)
            _id = wsp_info.get('id')
            backend = wsp_info.get('connection', {}).get('backend')
            schema = wsp_info.get('connection', {}).get('schema')
            if _id and backend and schema and schema == self._workspace_schema:
                if backend == 'snowflake':
                    self._workspace = _SnowflakeWorkspace(workspace_id=_id, schema=schema, client=self._client)
                    return self._workspace

                elif backend == 'bigquery':
                    credentials = json.loads(wsp_info.get('connection', {}).get('user') or '{}')
                    if project_id := credentials.get('project_id'):
                        self._workspace = _BigQueryWorkspace(
                            workspace_id=_id,
                            dataset_id=schema,
                            project_id=project_id,
                        )
                        return self._workspace
                    else:
                        raise ValueError(f'No credentials or no project ID in workspace: {self._workspace_schema}')

                else:
                    raise ValueError(f'Unexpected backend type "{backend}" in workspace: {self._workspace_schema}')

        raise ValueError(f'No Keboola workspace found for user: {self._workspace_schema}')

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


@tool_errors()
async def get_sql_dialect(
    ctx: Context,
) -> Annotated[str, Field(description='The SQL dialect of the project database')]:
    """Gets the name of the SQL dialect used by Keboola project's underlying database."""
    return await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()


@tool_errors()
async def query_table(
    sql_query: Annotated[str, Field(description='SQL SELECT query to run.')],
    ctx: Context,
) -> Annotated[str, Field(description='The retrieved data in a CSV format.')]:
    """
    Executes an SQL SELECT query to get the data from the underlying database.
    * When constructing the SQL SELECT query make sure to check the SQL dialect
      used by the Keboola project's underlying database.
    * When referring to tables always use fully qualified table names that include the database name,
      schema name and the table name.
    * The fully qualified table name can be found in the table information, use a tool to get the information
      about tables. The fully qualified table name can be found in the response from that tool.
    * Always use quoted column names when referring to table columns. The quoted column names can also be found
      in the response from the table information tool.
    """
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    result = await workspace_manager.execute_query(sql_query)
    if result.is_ok:
        if result.data:
            data = result.data
        else:
            # non-SELECT query, this should not really happen, because this tool is for running SELECT queries
            data = SqlSelectData(columns=['message'], rows=[{'message': result.message}])

        # Convert to CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=data.columns)
        writer.writeheader()
        writer.writerows(data.rows)

        return output.getvalue()

    else:
        raise ValueError(f'Failed to run SQL query, error: {result.message}')
