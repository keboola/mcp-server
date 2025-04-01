import csv
import logging
from io import StringIO
from typing import Annotated, Any, Literal, Mapping, Optional, Sequence

from mcp.server.fastmcp import Context
from pydantic import Field, TypeAdapter
from pydantic.dataclasses import dataclass

from keboola_mcp_server.client import KeboolaClient

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class TableFqn:
    db_name: str
    schema_name: str
    table_name: str

    @property
    def snowflake_fqn(self) -> str:
        """Returns the properly quoted Snowflake identifier."""
        return f'"{self.db_name}"."{self.schema_name}"."{self.table_name}"'


QueryStatus = Literal["ok", "error"]
SqlSelectDataRow = Mapping[str, Any]


@dataclass(frozen=True)
class SqlSelectData:
    columns: Sequence[str] = Field(description="Names of the columns returned from SQL select.")
    rows: Sequence[SqlSelectDataRow] = Field(
        description="Selected rows, each row is a dictionary of column: value pairs."
    )


@dataclass(frozen=True)
class QueryResult:
    status: QueryStatus = Field(description="Status of running the SQL query.")
    data: SqlSelectData | None = Field(None, description="Data selected by the SQL SELECT query.")
    message: str | None = Field(
        None, description="Either an error message or the information from non-SELECT queries."
    )

    @property
    def is_ok(self) -> bool:
        return self.status == "ok"

    @property
    def is_error(self) -> bool:
        return not self.is_ok


class WorkspaceManager:
    STATE_KEY = "workspace_manager"

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> "WorkspaceManager":
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, WorkspaceManager)
        return instance

    def __init__(self, client: KeboolaClient, workspace_user: str):
        self._client = client
        self._workspace_user = workspace_user
        self._workspace_id: str | None = None
        self._table_fqn_cache: dict[str, TableFqn] = {}

    async def _get_workspace_id(self) -> str:
        if self._workspace_id:
            return self._workspace_id

        for workspace in await self._client.get("workspaces"):
            assert isinstance(workspace, dict)
            _id = workspace.get("id")
            user = workspace.get("connection", {}).get("user")
            if _id and user and user == self._workspace_user:
                self._workspace_id = _id
                return self._workspace_id

        raise ValueError(f"No Keboola workspace found for user: {self._workspace_user}")

    async def execute_query(self, sql_query: str) -> QueryResult:
        wspid = await self._get_workspace_id()
        resp = await self._client.post(
            f"branch/default/workspaces/{wspid}/query", {"query": sql_query}
        )
        return TypeAdapter(QueryResult).validate_python(resp)

    async def get_table_fqn(self, table: dict[str, Any]) -> Optional[TableFqn]:
        """Gets the fully qualified name of a Keboola table."""
        # TODO: use a pydantic class for the 'table' param
        table_id = table["id"]
        if table_id in self._table_fqn_cache:
            return self._table_fqn_cache[table_id]

        db_name: str | None = None
        schema_name: str | None = None
        table_name: str | None = None

        if source_table := table.get("sourceTable"):
            # a table linked from some other project
            schema_name, table_name = source_table["id"].rsplit(sep=".", maxsplit=1)
            source_project_id = source_table["project"]["id"]
            # sql = f"show databases like '%_{source_project_id}';"
            sql = (
                f'select "DATABASE_NAME" from "INFORMATION_SCHEMA"."DATABASES" '
                f"where \"DATABASE_NAME\" like '%_{source_project_id}';"
            )
            result = await self.execute_query(sql)
            if result.is_ok and result.data and result.data.rows:
                db_name = result.data.rows[0]["DATABASE_NAME"]
            else:
                LOG.error(f"Failed to run SQL: {sql}, SAPI response: {result}")

        else:
            sql = f'select CURRENT_DATABASE() as "current_database", CURRENT_SCHEMA() as "current_schema";'
            result = await self.execute_query(sql)
            if result.is_ok and result.data and result.data.rows:
                row = result.data.rows[0]
                db_name = row["current_database"]
                if "." in table_id:
                    # a table local in a project for which the snowflake connection/workspace is open
                    schema_name, table_name = table_id.rsplit(sep=".", maxsplit=1)
                else:
                    # a table not in the project, but in the writable schema created for the workspace
                    # TODO: we should never come here, because the tools for listing tables can only see
                    #  tables that are in the project
                    schema_name = row["current_schema"]
                    table_name = table["name"]
            else:
                LOG.error(f"Failed to run SQL: {sql}, SAPI response: {result}")

        if db_name and schema_name and table_name:
            fqn = TableFqn(db_name, schema_name, table_name)
            self._table_fqn_cache[table_id] = fqn
            return fqn
        else:
            return None


async def query_table(
    sql_query: Annotated[str, Field(description="SQL SELECT query to run.")], ctx: Context
) -> Annotated[str, Field(description="The retrieved data in a CSV format.")]:
    """
    Executes an SQL SELECT query to get the data from the underlying snowflake database.
    * When constructing the SQL SELECT query make sure to use the fully qualified table names
      that include the database name, schema name and the table name.
    * The fully qualified table name can be found in the table information, use a tool to get the information
      about tables. The fully qualified table name can be found in the response for that tool.
    * Snowflake is case-sensitive so always wrap the column names in double quotes.

    Examples:
    * SQL queries must include the fully qualified table names including the database name, e.g.:
      SELECT * FROM "db_name"."db_schema_name"."table_name";
    """
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    result = await workspace_manager.execute_query(sql_query)
    if result.is_ok:
        if result.data:
            data = result.data
        else:
            # non-SELECT query, this should not really happen, because this tool is for running SELECT queries
            data = SqlSelectData(columns=["message"], rows=[{"message": result.message}])

        # Convert to CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=data.columns)
        writer.writeheader()
        writer.writerows(data.rows)

        return output.getvalue()

    else:
        raise ValueError(f"Failed to run SQL query, error: {result.message}")
