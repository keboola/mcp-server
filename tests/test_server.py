import pytest

from keboola_mcp_server.server import create_server


class TestServer:
    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server()
        tools = await server.list_tools()
        assert sorted(t.name for t in tools) == [
            "get_bucket_metadata",
            "get_table_metadata",
            "list_bucket_info",
            "list_bucket_tables",
            "list_component_configs",
            "list_components",
            "query_table",
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
