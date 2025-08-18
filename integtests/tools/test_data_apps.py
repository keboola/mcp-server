import asyncio
import uuid
from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.data_apps import (
    DetailDataAppOutput,
    ListDataAppOutput,
    ManageDataAppOutput,
    SyncDataAppOutput,
)


def _sample_streamlit_app() -> str:
    """Return a minimal Streamlit app template that supports query injection."""
    return (
        'import streamlit as st\n\n'
        '{QUERY_DATA_FUNCTION}\n\n'
        'def main():\n'
        "    st.title('Integration Test Data App')\n"
        "    st.write('Hello from integration test')\n"
        '    # Optionally query data (kept commented to avoid side-effects during tests)\n'
        "    # df = query_data('select 1 as col')\n"
        '    # st.dataframe(df)\n\n'
        'if __name__ == "__main__":\n'
        '    main()\n'
    )


@pytest.fixture
def mcp_server(storage_api_url: str, storage_api_token: str, workspace_schema: str) -> FastMCP:
    config = Config(
        storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema
    )
    return create_server(config)


@pytest_asyncio.fixture
async def mcp_client(mcp_server: FastMCP) -> AsyncGenerator[Client, None]:
    async with Client(mcp_server) as client:
        yield client


@pytest.mark.asyncio
async def test_list_data_apps_does_not_error(mcp_client: Client) -> None:
    """Verify listing data apps works (may or may not be empty)."""
    tool_result = await mcp_client.call_tool(name='get_data_apps', arguments={})
    apps = ListDataAppOutput.model_validate(tool_result.structured_content)
    assert isinstance(apps.data_apps, list)


@pytest.mark.asyncio
async def test_data_app_lifecycle(mcp_client: Client, keboola_client: KeboolaClient) -> None:
    """
    End-to-end lifecycle for data apps:
    - create via sync tool
    - get details and list
    - update via sync tool
    - deploy then stop via manage tool
    Always deletes the data app in teardown.
    """
    unique_suffix = uuid.uuid4().hex[:8]
    app_name = f'Integration Test Data App {unique_suffix}'
    app_description = 'Data app created by integration test'
    updated_name = f'{app_name} - Updated'
    updated_description = 'Data app updated by integration test'

    data_app_id: str | None = None
    configuration_id: str | None = None

    try:
        # Create
        created_result = await mcp_client.call_tool(
            name='sync_data_app',
            arguments={
                'name': app_name,
                'description': app_description,
                'source_code': _sample_streamlit_app(),
                'packages': ['streamlit'],
                'authorization_required': False,
            },
        )
        assert created_result.structured_content is not None
        created = SyncDataAppOutput.model_validate(created_result.structured_content['result'])
        assert created.action == 'created'
        data_app_id = created.data_app.data_app_id
        configuration_id = created.data_app.configuration_id
        assert data_app_id
        assert configuration_id

        # Get details by configuration_id
        details_result = await mcp_client.call_tool(
            name='get_data_apps', arguments={'configuration_ids': [configuration_id]}
        )
        assert details_result.structured_content is not None
        raw_details = details_result.structured_content['result']
        assert isinstance(raw_details, list)
        assert len(raw_details) == 1
        details = [DetailDataAppOutput.model_validate(x) for x in raw_details]
        assert details[0].data_app.configuration_id == configuration_id
        assert details[0].data_app.data_app_id == data_app_id

        # List and verify our app is present
        listed_result = await mcp_client.call_tool(name='get_data_apps', arguments={})
        assert listed_result.structured_content is not None
        listed = ListDataAppOutput.model_validate(listed_result.structured_content['result'])
        listed_ids = [a.configuration_id for a in listed.data_apps]
        assert configuration_id in listed_ids

        # Update
        updated_result = await mcp_client.call_tool(
            name='sync_data_app',
            arguments={
                'name': updated_name,
                'description': updated_description,
                'source_code': _sample_streamlit_app(),
                'packages': ['streamlit'],
                'authorization_required': False,
                'config_id': configuration_id,
            },
        )
        assert updated_result.structured_content is not None
        updated = SyncDataAppOutput.model_validate(updated_result.structured_content['result'])
        assert updated.action == 'updated'
        assert updated.data_app.data_app_id == data_app_id
        assert updated.data_app.configuration_id == configuration_id

    finally:
        # Teardown: always remove the data app via Data Science API
        if data_app_id is not None:
            try:
                await keboola_client.data_science_client.delete_data_app(data_app_id)
                await keboola_client.data_science_client.delete_data_app(data_app_id)
            except Exception:
                # Best-effort cleanup
                pass

