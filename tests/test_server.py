import pytest

from keboola_mcp_server.server import create_server
from keboola_mcp_server.component_tools import (
    RETRIEVE_COMPONENT_CONFIGURATIONS_TOOL_NAME,
    RETRIEVE_TRANSFORMATION_CONFIGURATIONS_TOOL_NAME,
    GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
)


class TestServer:
    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server()
        tools = await server.list_tools()
        assert sorted(t.name for t in tools) == [
            "create_sql_transformation",
            "get_bucket_metadata",
            GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
            "get_job_details",
            "get_sql_dialect",
            "get_table_metadata",
            "list_bucket_info",
            "list_bucket_tables",
            "query_table",
            RETRIEVE_COMPONENT_CONFIGURATIONS_TOOL_NAME,
            "retrieve_jobs_in_project",
            RETRIEVE_TRANSFORMATION_CONFIGURATIONS_TOOL_NAME,
        ]

    @pytest.mark.asyncio
    async def test_tools_have_descriptions(self):
        server = create_server()
        tools = await server.list_tools()

        missing_descriptions: list[str] = []
        for t in tools:
            if not t.description:
                missing_descriptions.append(t.name)

        missing_descriptions.sort()
        assert not missing_descriptions, f"These tools have no description: {missing_descriptions}"
