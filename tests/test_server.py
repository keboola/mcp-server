import pytest

from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components import (
    GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
    RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME,
    RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
)


class TestServer:
    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server()
        tools = await server.list_tools()
        assert sorted(t.name for t in tools) == [
            'create_sql_transformation',
            'docs_query',
            'get_bucket_detail',
            GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
            'get_job_detail',
            'get_sql_dialect',
            'get_table_detail',
            'query_table',
            'retrieve_bucket_tables',
            'retrieve_buckets',
            RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME,
            'retrieve_jobs',
            RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
            'start_job',
            'update_bucket_description',
            'update_column_description',
            'update_sql_transformation_configuration',
            'update_table_description',
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
        assert not missing_descriptions, f'These tools have no description: {missing_descriptions}'
