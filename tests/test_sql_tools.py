from typing import Any

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server import sql_tools
from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.mcp import StatefullServerSession
from keboola_mcp_server.sql_tools import QueryResult, SqlSelectData, TableFqn, WorkspaceManager


@pytest.fixture()
def client(mocker) -> KeboolaClient:
    return mocker.AsyncMock(KeboolaClient)


@pytest.fixture()
def context(client: KeboolaClient, mocker) -> Context:
    client.get.return_value = [{"id": "workspace_1234", "connection": {"user": "user_1234"}}]

    ctx = mocker.MagicMock(Context)
    ctx.session = (session := mocker.MagicMock(StatefullServerSession))
    type(session).state = (state := mocker.PropertyMock())
    state.return_value = {
        "sapi_client": client,
        "workspace_manager": WorkspaceManager(client=client, workspace_user="user_1234"),
    }

    return ctx


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query, sapi_result, expected",
    [
        (
            "select 1;",
            QueryResult(status="ok", data=SqlSelectData(columns=["a"], rows=[{"a": 1}])),
            "a\r\n1\r\n",  # CSV
        ),
        (
            "select id, name, email from user;",
            QueryResult(
                status="ok",
                data=SqlSelectData(
                    columns=["id", "name", "email"],
                    rows=[
                        {"id": 1, "name": "John", "email": "john@foo.com"},
                        {"id": 2, "name": "Joe", "email": "joe@bar.com"},
                    ],
                ),
            ),
            "id,name,email\r\n1,John,john@foo.com\r\n2,Joe,joe@bar.com\r\n",  # CSV
        ),
        (
            "create table foo (id integer, name varchar);",
            QueryResult(status="ok", message="1 table created"),
            "message\r\n1 table created\r\n",  # CSV
        ),
    ],
)
async def test_query_table(
    query: str, sapi_result: QueryResult, expected: str, client: KeboolaClient, context: Context
):
    client.post.return_value = sapi_result
    actual = await sql_tools.query_table(sql_query=query, ctx=context)
    assert actual == expected


class TestWorkspaceManager:

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "table, sapi_result, expected",
        [
            (
                # table in.c-foo.bar in its own project
                {"id": "in.c-foo.bar", "name": "bar"},
                {"current_database": "db_xyz", "current_schema": "workspace_123"},
                TableFqn(db_name="db_xyz", schema_name="in.c-foo", table_name="bar"),
            ),
            (
                # temporary table not in a project, but in the writable schema of the workspace
                {"id": "bar", "name": "bar"},
                {"current_database": "db_xyz", "current_schema": "workspace_123"},
                TableFqn(db_name="db_xyz", schema_name="workspace_123", table_name="bar"),
            ),
            (
                # table out.c-baz.bam exported from project 1234
                # and imported as table in.c-foo.bar in some other project
                {
                    "id": "in.c-foo.bar",
                    "name": "bar",
                    "sourceTable": {"project": {"id": "1234"}, "id": "out.c-baz.bam"},
                },
                {"DATABASE_NAME": "sapi_1234"},
                TableFqn(db_name="sapi_1234", schema_name="out.c-baz", table_name="bam"),
            ),
        ],
    )
    async def test_get_table_fqn(
        self, table: dict[str, Any], sapi_result, expected: TableFqn, client: KeboolaClient, mocker
    ):
        client = mocker.AsyncMock(KeboolaClient)
        client.get.return_value = [{"id": "workspace_1234", "connection": {"user": "user_1234"}}]
        client.post.return_value = QueryResult(
            status="ok", data=SqlSelectData(columns=list(sapi_result.keys()), rows=[sapi_result])
        )

        dpm = WorkspaceManager(client=client, workspace_user="user_1234")
        fqn = await dpm.get_table_fqn(table)
        assert fqn == expected
