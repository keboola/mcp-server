import logging
from typing import AsyncGenerator

import pytest
import pytest_asyncio

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import DataAppConfig, DataAppResponse, DataScienceClient

LOG = logging.getLogger(__name__)


def _minimal_parameters(slug: str) -> dict[str, object]:
    """Build minimal valid parameters for a code-based Streamlit data app."""
    return {
        'size': 'tiny',
        'autoSuspendAfterSeconds': 600,
        'dataApp': {
            'slug': slug,
            'streamlit': {
                'config.toml': '[theme]\nbase = "light"',
            },
        },
        'script': [
            'import streamlit as st',
            "st.write('Hello from integration test')",
        ],
    }


def _public_access_authorization() -> dict[str, object]:
    """Allow public access to all paths; no providers required."""
    return {
        'app_proxy': {
            'auth_providers': [],
            'auth_rules': [
                {'type': 'pathPrefix', 'value': '/', 'auth_required': False},
            ],
        }
    }


@pytest.fixture
def ds_client(keboola_client: KeboolaClient) -> DataScienceClient:
    return keboola_client.data_science_client


@pytest_asyncio.fixture
async def initial_data_app(ds_client: DataScienceClient, unique_id: str) -> AsyncGenerator[DataAppResponse, None]:
    data_app: DataAppResponse | None = None
    try:
        slug = f'test-app-{unique_id}'
        config = DataAppConfig.model_validate(
            {'parameters': _minimal_parameters(slug), 'authorization': _public_access_authorization()}
        )
        data_app = await ds_client.create_data_app(
            name=f'IntegTest {slug}',
            description='Created by integration tests',
            configuration=config,
        )
        assert isinstance(data_app, DataAppResponse)
        yield data_app
    finally:
        if data_app:
            try:
                # The DSAPI delete endpoint removes a data app only if its desired and current states match.
                # Otherwise, it returns a 400 Bad Request.
                # When deploying/deleting/suspending/updating/etc. the data app, the desired state is set according to
                # the action. Then there is a background task that runs for the given action and after it finishes,
                # the current state is updated to match the desired state.
                await ds_client.delete_data_app(data_app.id)
            except Exception as e:
                LOG.exception(f'Failed to delete data app: {e}')
                raise


@pytest.mark.asyncio
async def test_create_and_fetch_data_app(
    ds_client: DataScienceClient, initial_data_app: DataAppResponse, keboola_client: KeboolaClient
) -> None:
    """Test creating a data app and fetching it from detail and list endpoints"""
    # Check if the created data app is valid
    created = initial_data_app
    assert isinstance(created, DataAppResponse)
    assert created.id
    assert created.state == 'created'
    assert created.type == 'streamlit'
    assert created.component_id == DATA_APP_COMPONENT_ID

    # Fetch the data app from data science
    fetched_ds = await ds_client.get_data_app(created.id)
    assert fetched_ds.id == created.id
    assert fetched_ds.type == created.type
    assert fetched_ds.component_id == created.component_id
    assert fetched_ds.project_id == created.project_id
    assert fetched_ds.config_id == created.config_id
    assert fetched_ds.config_version == created.config_version

    # Fetch the data app config from storage
    fetched_s = await keboola_client.storage_client.configuration_detail(
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id=created.config_id,
    )

    # check if the data app ids are the same (data app from data science and config from storage)
    assert 'configuration' in fetched_s
    assert isinstance(fetched_s['configuration'], dict)
    assert 'parameters' in fetched_s['configuration']
    assert isinstance(fetched_s['configuration']['parameters'], dict)
    assert 'id' in fetched_s['configuration']['parameters']
    assert fetched_ds.id == fetched_s['configuration']['parameters']['id']

    # Fetch the all data apps and check if the created data app is in the list
    # TODO: Remove this limit once DSAPI is fixed.
    # The limit is temporarily increased to 500 to prevent leftover data apps from previous tests.
    # These apps cannot be deleted because their configurations were removed in SAPI first,
    # causing the DSAPI delete endpoint to return a 500 error afterward.
    data_apps = await ds_client.list_data_apps(limit=500)
    assert isinstance(data_apps, list)
    assert len(data_apps) > 0
    assert any(app.id == created.id for app in data_apps)
    # TODO(REMOVE): Remove this assertion once DSAPI is fixed. This only checks that we do not leave any data apps
    # in the CI project after test executions except those which are already there and cannot be deleted.
    assert len(data_apps) < 110
