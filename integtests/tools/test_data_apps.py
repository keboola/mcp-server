import logging
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
    ModifiedDataAppOutput,
)

LOG = logging.getLogger(__name__)


@pytest.fixture
def streamlit_app_imports() -> str:
    return 'import streamlit as st\n\n'


@pytest.fixture
def streamlit_app_entrypoint() -> str:
    return (
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
def sample_streamlit_app(streamlit_app_imports: str, streamlit_app_entrypoint: str) -> str:
    """Return a minimal Streamlit app template that supports query injection."""
    return f'{streamlit_app_imports}' '{QUERY_DATA_FUNCTION}\n\n' f'{streamlit_app_entrypoint}'


@pytest.fixture
def mcp_server(storage_api_url: str, storage_api_token: str, workspace_schema: str) -> FastMCP:
    config = Config(storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema)
    mcp_server = create_server(config)
    assert isinstance(mcp_server, FastMCP)
    return mcp_server


@pytest_asyncio.fixture
async def mcp_client(mcp_server: FastMCP) -> AsyncGenerator[Client, None]:
    async with Client(mcp_server) as client:
        yield client


@pytest.fixture
def app_name() -> str:
    unique_suffix = uuid.uuid4().hex[:8]
    return f'Integration Test Data App {unique_suffix}'


@pytest.fixture
def app_description() -> str:
    return 'Data app created by integration test'


@pytest_asyncio.fixture
async def initial_data_app(
    mcp_client: Client,
    keboola_client: KeboolaClient,
    app_name: str,
    app_description: str,
    sample_streamlit_app: str,
) -> AsyncGenerator[ModifiedDataAppOutput, None]:
    sync_output: ModifiedDataAppOutput | None = None
    try:
        # Create
        created_result = await mcp_client.call_tool(
            name='modify_data_app',
            arguments={
                'name': app_name,
                'description': app_description,
                'source_code': sample_streamlit_app,
                'packages': ['numpy', 'streamlit'],
                'authorization_required': False,
            },
        )
        assert created_result.structured_content is not None
        sync_output = ModifiedDataAppOutput.model_validate(created_result.structured_content)
        yield sync_output
    finally:
        if sync_output:
            try:
                # Delete the data app from the data science API and the configuration from the storage API as well.
                await keboola_client.data_science_client.delete_data_app(sync_output.data_app.data_app_id)
            except Exception as e:
                LOG.error(f'Error deleting data app: {e}')
        else:
            LOG.error('No data app to delete')


@pytest.mark.asyncio
async def test_get_data_apps_listing(mcp_client: Client, initial_data_app: ModifiedDataAppOutput) -> None:
    """Test listing data apps does not error."""
    tool_result = await mcp_client.call_tool(name='get_data_apps', arguments={})
    assert tool_result.structured_content is not None
    apps = GetDataAppsOutput.model_validate(tool_result.structured_content)
    assert isinstance(apps.data_apps, list)
    assert all(isinstance(app, DataAppSummary) for app in apps.data_apps)


@pytest.mark.asyncio
async def test_data_app_lifecycle(
    mcp_client: Client,
    keboola_client: KeboolaClient,
    app_name: str,
    app_description: str,
    initial_data_app: ModifiedDataAppOutput,
    streamlit_app_imports: str,
    streamlit_app_entrypoint: str,
) -> None:
    """
    End-to-end lifecycle for data apps:
    Starts with a created app.
    - get details and list of created app
    - update app
    - get details and list of updated app
    Always deletes the data app in teardown.
    """

    # Check created app basic details
    assert initial_data_app.response == 'created'
    data_app_id = initial_data_app.data_app.data_app_id
    configuration_id = initial_data_app.data_app.configuration_id
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
    details = GetDataAppsOutput.model_validate(details_result.structured_content)
    assert len(details.data_apps) == 1
    data_app_details = details.data_apps[0]
    assert isinstance(data_app_details, DataApp)

    assert data_app_details.configuration_id == configuration_id
    assert data_app_details.data_app_id == data_app_id
    assert data_app_details.name == app_name
    assert data_app_details.description == app_description
    # Check code and code injection
    assert streamlit_app_imports in data_app_details.parameters['script'][0]
    assert streamlit_app_entrypoint in data_app_details.parameters['script'][0]
    assert _QUERY_DATA_FUNCTION_CODE in data_app_details.parameters['script'][0]
    # Check packages
    assert set(data_app_details.parameters['packages']) == set(['numpy', 'streamlit'] + _DEFAULT_PACKAGES)

    # Check listing contains our app
    # TODO(REMOVE): Set the limit back to the default value once DSAPI is fixed. The limit is temporarily increased to
    # 500 to prevent listing only the leftover data apps from previous tests (100). These apps cannot be deleted
    # because their configurations were removed in SAPI first, causing the DSAPI delete endpoint to return a 500 error
    # afterward.
    listed_result = await mcp_client.call_tool(name='get_data_apps', arguments={'limit': 500})
    assert listed_result.structured_content is not None
    listed = GetDataAppsOutput.model_validate(listed_result.structured_content)
    assert len(listed.data_apps) > 0
    assert all(isinstance(app, DataAppSummary) for app in listed.data_apps)
    assert configuration_id in [a.configuration_id for a in listed.data_apps]
    # TODO(REMOVE): Remove this assertion once DSAPI is fixed. This only checks that we do not leave any data apps
    # in the CI project after test executions except those which are already there and cannot be deleted.
    assert len(listed.data_apps) < 110

    # Update app
    updated_name = f'{app_name} - Updated'
    updated_description = 'Data app updated by integration test'
    updated_source_code = 'import numpy as np\n\n'
    updated_result = await mcp_client.call_tool(
        name='modify_data_app',
        arguments={
            'name': updated_name,
            'description': updated_description,
            'source_code': updated_source_code,
            'packages': ['streamlit'],
            'authorization_required': False,
            'configuration_id': configuration_id,
            'change_description': 'Update Code',
        },
    )
    # Check updated app basic details
    assert updated_result.structured_content is not None
    updated = ModifiedDataAppOutput.model_validate(updated_result.structured_content)
    assert updated.response == 'updated'
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
    fetched_app = await mcp_client.call_tool(name='get_data_apps', arguments={'configuration_ids': [configuration_id]})
    assert fetched_app.structured_content is not None
    fetched = GetDataAppsOutput.model_validate(fetched_app.structured_content)
    assert len(fetched.data_apps) == 1
    assert isinstance(fetched.data_apps[0], DataApp)
    assert fetched.data_apps[0].name == updated_name
    assert fetched.data_apps[0].description == updated_description
    # Check that the source code is updated
    assert _QUERY_DATA_FUNCTION_CODE in fetched.data_apps[0].parameters['script'][0]
    assert updated_source_code in fetched.data_apps[0].parameters['script'][0]
    assert streamlit_app_imports not in fetched.data_apps[0].parameters['script'][0]
    assert streamlit_app_entrypoint not in fetched.data_apps[0].parameters['script'][0]
    # Check that the packages are updated
    assert set(fetched.data_apps[0].parameters['packages']) == set(['streamlit'] + _DEFAULT_PACKAGES)
