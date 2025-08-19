import uuid
from typing import Any, AsyncGenerator, Mapping, cast

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient, get_metadata_property
from keboola_mcp_server.config import Config, MetadataField
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.data_apps import (
    _DEFAULT_PACKAGES,
    _QUERY_DATA_FUNCTION_CODE,
    DataApp,
    DataAppSummary,
    GetDataAppsOutput,
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
    config = Config(storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema)
    return create_server(config)


@pytest_asyncio.fixture
async def mcp_client(mcp_server: FastMCP) -> AsyncGenerator[Client, None]:
    async with Client(mcp_server) as client:
        yield client


@pytest.mark.asyncio
async def test_get_data_apps_listing(mcp_client: Client) -> None:
    """Test listing data apps does not error."""
    tool_result = await mcp_client.call_tool(name='get_data_apps', arguments={})
    apps = GetDataAppsOutput.model_validate(tool_result.structured_content)
    assert isinstance(apps.data_apps, list)
    assert all(isinstance(app, DataAppSummary) for app in apps.data_apps)


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
                'packages': ['numpy', 'streamlit'],
                'authorization_required': False,
            },
        )
        # Check created app basic details
        assert created_result.structured_content is not None
        created = SyncDataAppOutput.model_validate(created_result.structured_content['result'])
        assert created.action == 'created'
        data_app_id = created.data_app.data_app_id
        configuration_id = created.data_app.configuration_id
        assert data_app_id
        assert configuration_id

        # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
        metadata = await keboola_client.storage_client.configuration_metadata_get(
            component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
        )
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        assert MetadataField.CREATED_BY_MCP in metadata_dict
        assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'

        # Check created app details by configuration_id
        details_result = await mcp_client.call_tool(
            name='get_data_apps', arguments={'configuration_ids': [configuration_id]}
        )
        assert details_result.structured_content is not None
        details = GetDataAppsOutput.model_validate(details_result.structured_content['result'])
        assert len(details.data_apps) == 1
        assert isinstance(details.data_apps[0], DataApp)
        assert details.data_apps[0].configuration_id == configuration_id
        assert details.data_apps[0].data_app_id == data_app_id
        assert details.data_apps[0].name == app_name
        assert details.data_apps[0].description == app_description
        # Check code and code injection
        assert _sample_streamlit_app() in details.data_apps[0].parameters['script']
        assert _QUERY_DATA_FUNCTION_CODE in details.data_apps[0].parameters['script']
        # Check packages
        assert set(details.data_apps[0].parameters['packages']) == set(['numpy', 'streamlit'] + _DEFAULT_PACKAGES)

        # Check listing contains our app
        listed_result = await mcp_client.call_tool(name='get_data_apps', arguments={})
        assert listed_result.structured_content is not None
        listed = GetDataAppsOutput.model_validate(listed_result.structured_content['result'])
        assert len(listed.data_apps) > 0
        assert all(isinstance(app, DataAppSummary) for app in listed.data_apps)
        assert configuration_id in [a.configuration_id for a in listed.data_apps]

        # Update app
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
        # Check updated app basic details
        assert updated_result.structured_content is not None
        updated = SyncDataAppOutput.model_validate(updated_result.structured_content['result'])
        assert updated.action == 'updated'
        assert updated.data_app.data_app_id == data_app_id
        assert updated.data_app.configuration_id == configuration_id

        # Check that KBC.MCP.updatedBy.version.{version} is set to 'true'
        metadata = cast(
            list[Mapping[str, Any]],
            await keboola_client.storage_client.configuration_metadata_get(
                component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
            ),
        )
        meta_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{updated.data_app.config_version}'
        meta_value = get_metadata_property(metadata, meta_key)
        assert meta_value == 'true'
        # Check that the original creation metadata is still there
        assert get_metadata_property(metadata, MetadataField.CREATED_BY_MCP) == 'true'

        # Check updated app details by configuration_id
        fetched_app = await mcp_client.call_tool(
            name='get_data_apps', arguments={'configuration_ids': [configuration_id]}
        )
        assert fetched_app.structured_content is not None
        fetched = GetDataAppsOutput.model_validate(fetched_app.structured_content['result'])
        assert len(fetched.data_apps) == 1
        assert isinstance(fetched.data_apps[0], DataApp)
        assert fetched.data_apps[0].name == updated_name
        assert fetched.data_apps[0].description == updated_description
        assert fetched.data_apps[0].parameters['packages'] == ['streamlit'] + _DEFAULT_PACKAGES

    finally:
        # Teardown: always remove the data app via Data Science API
        if data_app_id is not None:
            try:
                await keboola_client.data_science_client.delete_data_app(data_app_id)
                await keboola_client.data_science_client.delete_data_app(data_app_id)
            except Exception:
                # Best-effort cleanup
                pass
