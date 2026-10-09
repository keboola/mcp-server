import logging
import uuid

import httpx
import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.streams import DATA_STREAMS_FEATURE, create_stream, get_streams

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_data_streams(mcp_context: Context, keboola_client: KeboolaClient) -> None:
    """
    Without the `data-streams` feature, both tools point the user to the Data Streams page to unlock it.
    With it, an HTTP stream is created, accepts an event, and is listed by `get_streams`.
    """
    project_id = await keboola_client.storage_client.project_id()
    streams_page = f'{keboola_client.storage_api_url}/admin/projects/{project_id}/storage/data-streams'

    if not await keboola_client.has_feature(DATA_STREAMS_FEATURE):
        for call in (get_streams(ctx=mcp_context), create_stream(ctx=mcp_context, name='integtest')):
            with pytest.raises(ToolError, match='Unlock Data Streams') as exc_info:
                await call
            assert streams_page in str(exc_info.value)
        return

    name = f'integtest-{uuid.uuid4().hex[:8]}'
    created = await create_stream(ctx=mcp_context, name=name, description='MCP integration test')
    try:
        assert created.name == name
        assert created.type == 'http'
        assert created.endpoint_url
        assert [s.table_id for s in created.sinks] == [f'in.c-data-stream-{created.source_id}.events']
        assert created.links[0].url == f'{streams_page}/{created.source_id}'

        async with httpx.AsyncClient() as http:
            response = await http.post(created.endpoint_url, json={'event': 'integtest'})
        assert response.status_code == 200, response.text

        listed = await get_streams(ctx=mcp_context, source_ids=[created.source_id])
        assert [s.source_id for s in listed.streams] == [created.source_id]
        assert listed.streams[0].sinks == created.sinks
    finally:
        await keboola_client.stream_client.delete_source(created.source_id)
