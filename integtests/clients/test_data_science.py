import logging

import httpx
import pytest

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import AsyncDataScienceClient, DataAppResponse

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
def ds_client(keboola_client: KeboolaClient) -> AsyncDataScienceClient:
    return keboola_client.data_science_client


@pytest.mark.asyncio
async def test_create_and_fetch_data_app(
    ds_client: AsyncDataScienceClient, unique_id: str, keboola_client: KeboolaClient
) -> None:
    """Test creating a data app and fetching it from detail and list endpoints"""
    slug = f'test-app-{unique_id}'
    created: DataAppResponse = await ds_client.create_data_app(
        name=f'IntegTest {slug}',
        description='Created by integration tests',
        parameters=_minimal_parameters(slug),
        authorization=_public_access_authorization(),
    )

    try:
        # Check if the created data app is valid
        assert isinstance(created, DataAppResponse)
        assert created.id
        assert created.type == 'streamlit'
        assert created.component_id == DATA_APP_COMPONENT_ID

        # Deploy the data app
        response = await ds_client.deploy_data_app(created.id, created.config_version)
        assert response.id == created.id

        # Fetch the data app from data science
        fethced_ds = await ds_client.get_data_app(created.id)
        assert fethced_ds.id == created.id
        assert fethced_ds.type == created.type
        assert fethced_ds.component_id == created.component_id
        assert fethced_ds.project_id == created.project_id
        assert fethced_ds.config_id == created.config_id
        assert fethced_ds.config_version == created.config_version

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
        assert fethced_ds.id == fetched_s['configuration']['parameters']['id']

        # Fetch the all data apps and check if the created data app is in the list
        data_apps = await ds_client.list_data_apps()
        assert isinstance(data_apps, list)
        assert len(data_apps) > 0
        assert any(app.id == created.id for app in data_apps)

    finally:
        for _ in range(2):  # Delete configuration 2 times (from storage and then from temporal bin)
            try:
                await ds_client.delete_data_app(created.id)
            except Exception as e:
                LOG.exception(f'Failed to delete data app: {e}')
                pass
