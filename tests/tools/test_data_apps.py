from typing import Literal, cast

import pytest
from fastmcp import Context

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import DataAppResponse
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.data_apps import (
    _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE,
    _STORAGE_QUERY_DATA_FUNCTION_CODE,
    DataApp,
    DataAppSummary,
    _build_data_app_config,
    _fetch_data_app,
    _get_authorization,
    _get_data_app_slug,
    _get_query_function_code,
    _get_secrets,
    _inject_query_to_source_code,
    _update_existing_data_app_config,
    _uses_basic_authentication,
    deploy_data_app,
    get_data_apps,
)


@pytest.fixture
def data_app() -> DataApp:
    return DataApp(
        name='test',
        component_id='test',
        configuration_id='test',
        data_app_id='test',
        project_id='test',
        branch_id='test',
        config_version='test',
        type='test',
        auto_suspend_after_seconds=3600,
        parameters={},
        authorization={},
        state='test',
    )


def _make_data_app_response(
    component_id: str = DATA_APP_COMPONENT_ID,
    data_app_id: str = 'app-123',
    config_id: str = 'cfg-123',
) -> DataAppResponse:
    """Helper to create a DataAppResponse with sensible defaults."""
    return DataAppResponse(
        id=data_app_id,
        project_id='proj-1',
        component_id=component_id,
        branch_id='branch-1',
        config_id=config_id,
        config_version='1',
        type='streamlit',
        state='running',
        desired_state='running',
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('current_state', 'action', 'error_match'),
    [
        ('starting', 'stop', 'Data app is currently "starting", could not be stopped at the moment.'),
        ('restarting', 'stop', 'Data app is currently "starting", could not be stopped at the moment.'),
        ('stopping', 'deploy', 'Data app is currently "stopping", could not be started at the moment.'),
    ],
)
async def test_deploy_data_app_when_current_state_contradicts_with_action(
    mocker,
    data_app: DataApp,
    current_state: str,
    action: Literal['deploy', 'stop'],
    error_match: str,
    mcp_context_client: Context,
) -> None:
    """call deploy_data_app with mocked data_app and given state expecting ValueError with proper error message."""
    data_app.state = current_state
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', return_value=data_app)
    with pytest.raises(ValueError, match=error_match):
        await deploy_data_app(
            ctx=mcp_context_client, action=cast(Literal['deploy', 'stop'], action), configuration_id='cfg-123'
        )


def test_get_data_app_slug():
    assert _get_data_app_slug('My Cool App') == 'my-cool-app'
    assert _get_data_app_slug('App 123') == 'app-123'
    assert _get_data_app_slug('Weird!@# Name$$$') == 'weird-name'


def test_get_authorization_mapping():
    auth_true = _get_authorization(True)
    assert auth_true['app_proxy']['auth_providers'] == [{'id': 'simpleAuth', 'type': 'password'}]
    assert auth_true['app_proxy']['auth_rules'] == [
        {'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['simpleAuth']}
    ]

    auth_false = _get_authorization(False)
    assert auth_false['app_proxy']['auth_providers'] == []
    assert auth_false['app_proxy']['auth_rules'] == [{'type': 'pathPrefix', 'value': '/', 'auth_required': False}]


def test_is_authorized_behavior():
    assert _uses_basic_authentication(_get_authorization(True)) is True
    assert _uses_basic_authentication(_get_authorization(False)) is False


def test_inject_query_to_source_code_when_already_included():
    query_code = _STORAGE_QUERY_DATA_FUNCTION_CODE
    backend = 'bigquery'
    source_code = f"""prelude{query_code}postlude"""
    result = _inject_query_to_source_code(source_code, backend)
    assert result == source_code


def test_inject_query_to_source_code_with_markers():
    src = (
        'import pandas as pd\n\n'
        '# ### INJECTED_CODE ####\n'
        '# will be replaced\n'
        '# ### END_OF_INJECTED_CODE ####\n\n'
        "print('hello')\n"
    )
    backend = 'bigquery'
    query_code = _STORAGE_QUERY_DATA_FUNCTION_CODE
    result = _inject_query_to_source_code(src, backend)

    assert result.startswith('import pandas as pd')
    assert query_code in result
    assert result.endswith("print('hello')\n")


def test_inject_query_to_source_code_with_placeholder():
    src = 'header\n{QUERY_DATA_FUNCTION}\nfooter\n'
    query_code = _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE
    backend = 'snowflake'
    result = _inject_query_to_source_code(src, backend)

    # Injected once via format(), original source (with placeholder) appended afterwards
    assert query_code in result
    assert '{QUERY_DATA_FUNCTION}' not in result
    assert result.startswith('header')
    assert result.strip().endswith('footer')


def test_inject_query_to_source_code_default_path():
    src = "print('x')\n"
    query_code = _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE
    backend = 'snowflake'
    result = _inject_query_to_source_code(src, backend)
    assert result.startswith(query_code)
    assert result.endswith(src)


def test_build_data_app_config_merges_defaults_and_secrets():
    name = 'My App'
    src = "print('hello')"
    pkgs = ['pandas']
    secrets = {'FOO': 'bar'}
    backend = 'snowflake'

    config = _build_data_app_config(name, src, pkgs, 'basic-auth', secrets, backend)

    params = config['parameters']
    assert params['dataApp']['slug'] == 'my-app'
    assert params['script'] == [_inject_query_to_source_code(src, backend)]
    # Default packages are included and deduplicated
    assert 'pandas' in params['packages']
    assert 'httpx' in params['packages']
    # Secrets carried over
    assert params['dataApp']['secrets'] == secrets
    # Authentication reflects flag
    assert config['authorization'] == _get_authorization(True)


def test_update_existing_data_app_config():
    existing = {
        'parameters': {
            'dataApp': {
                'slug': 'old-slug',
                'secrets': {'FOO': 'old', 'KEEP': 'x'},
            },
            'script': ['old'],
            'packages': ['numpy'],
        },
        'authorization': {},
    }

    new = _update_existing_data_app_config(
        existing_config=existing,
        name='New Name',
        source_code='new-code',
        packages=['pandas'],
        authentication_type='basic-auth',
        secrets={'FOO': 'new', 'NEW': 'y'},
        sql_dialect='snowflake',
    )

    assert new['parameters']['dataApp']['slug'] == 'new-name'
    assert new['parameters']['script'] == [_inject_query_to_source_code('new-code', 'snowflake')]
    # Removed previous packages
    assert 'numpy' not in new['parameters']['packages']
    # Packages combined with defaults
    assert 'pandas' in new['parameters']['packages']
    assert 'httpx' in new['parameters']['packages']
    # Secrets merged
    assert new['parameters']['dataApp']['secrets']['FOO'] == 'new'
    assert new['parameters']['dataApp']['secrets']['NEW'] == 'y'
    assert new['parameters']['dataApp']['secrets']['KEEP'] == 'x'
    # Authentication updated
    assert new['authorization'] == _get_authorization(True)


def test_get_secrets():
    secrets = _get_secrets(workspace_id='wid-1234', branch_id='123')
    assert secrets == {
        'WORKSPACE_ID': 'wid-1234',
        'BRANCH_ID': '123',
    }


def test_update_existing_data_app_config_keeps_previous_properties_when_undefined():
    existing_authorization = {
        'app_proxy': {
            'auth_providers': [{'id': 'oidc', 'type': 'oidc', 'issuer_url': 'https://issuer'}],
            'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['oidc']}],
        }
    }
    existing = {
        'parameters': {
            'dataApp': {
                'slug': 'old-slug',
                'secrets': {'KEEP': 'secret'},
            },
            'script': ['old'],
            'packages': ['numpy'],
        },
        'authorization': existing_authorization,
    }

    new = _update_existing_data_app_config(
        existing_config=existing,
        name='',
        source_code='',
        packages=[],
        authentication_type='default',
        secrets={},
        sql_dialect='snowflake',
    )

    assert new['authorization'] is existing_authorization
    assert new['parameters']['script'] == ['old']
    # verify the rest of the config is still updated
    assert new['parameters']['dataApp']['slug'] == 'old-slug'
    assert 'numpy' in new['parameters']['packages']
    assert 'httpx' in new['parameters']['packages']
    assert new['parameters']['dataApp']['secrets']['KEEP'] == 'secret'


def test_get_query_function_code_selects_snippets():
    assert _get_query_function_code('snowflake') == _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE
    assert _get_query_function_code('bigquery') == _STORAGE_QUERY_DATA_FUNCTION_CODE
    with pytest.raises(ValueError, match='Unsupported SQL dialect'):
        _get_query_function_code('UNKNOWN')


@pytest.mark.parametrize(
    'values',
    [
        {
            'type': 'streamlit',
            'state': 'created',
        },
        {
            'type': 'streamlit',
            'state': 'running',
        },
        {
            'type': 'streamlit',
            'state': 'stopped',
        },
        {
            'type': 'something else',
            'state': 'something else',
        },
    ],
)
def test_data_app_summary_from_dict_minimal(values: JsonDict) -> None:
    """Test creating DataAppSummary from dict with required fields."""
    data_app = {
        'component_id': 'comp-1',
        'configuration_id': 'cfg-1',
        'data_app_id': 'app-1',
        'project_id': 'proj-1',
        'branch_id': 'branch-1',
        'config_version': 'v1',
        'deployment_url': 'https://example.com/app',
        'auto_suspend_after_seconds': 3600,
    }
    data_app.update(values)
    model = DataAppSummary.model_validate(data_app)
    assert model.component_id == 'comp-1'
    assert model.configuration_id == 'cfg-1'
    assert model.state == values['state']
    assert model.type == values['type']
    assert model.deployment_url == 'https://example.com/app'
    assert model.auto_suspend_after_seconds == 3600


class TestGetDataAppsFiltering:
    """Tests for get_data_apps filtering behavior by component_id."""

    @pytest.mark.asyncio
    async def test_get_data_apps_filters_by_component_id(self, mocker, mcp_context_client: Context) -> None:
        """When listing data apps, only apps with DATA_APP_COMPONENT_ID are returned."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock list_data_apps to return apps with different component_ids
        keboola_client.data_science_client.list_data_apps = mocker.AsyncMock(
            return_value=[
                _make_data_app_response(component_id=DATA_APP_COMPONENT_ID, data_app_id='app-1'),
                _make_data_app_response(component_id='keboola.sandboxes', data_app_id='app-2'),
                _make_data_app_response(component_id=DATA_APP_COMPONENT_ID, data_app_id='app-3'),
                _make_data_app_response(component_id='other.component', data_app_id='app-4'),
            ]
        )

        # Mock ProjectLinksManager
        mock_link = Link(type='ui-dashboard', title='Data Apps', url='https://example.com/data-apps')
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.ProjectLinksManager.from_client',
            return_value=mocker.AsyncMock(get_data_app_dashboard_link=mocker.MagicMock(return_value=mock_link)),
        )

        result = await get_data_apps(ctx=mcp_context_client)

        # Only apps with DATA_APP_COMPONENT_ID should be returned
        assert len(result.data_apps) == 2
        data_app_ids = [app.data_app_id for app in result.data_apps]
        assert 'app-1' in data_app_ids
        assert 'app-3' in data_app_ids
        assert 'app-2' not in data_app_ids
        assert 'app-4' not in data_app_ids

    @pytest.mark.asyncio
    async def test_get_data_apps_returns_empty_when_no_matching_apps(self, mocker, mcp_context_client: Context) -> None:
        """When no apps match DATA_APP_COMPONENT_ID, an empty list is returned."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock list_data_apps to return apps with different component_ids
        keboola_client.data_science_client.list_data_apps = mocker.AsyncMock(
            return_value=[
                _make_data_app_response(component_id='keboola.sandboxes', data_app_id='app-1'),
                _make_data_app_response(component_id='other.component', data_app_id='app-2'),
            ]
        )

        mock_link = Link(type='ui-dashboard', title='Data Apps', url='https://example.com/data-apps')
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.ProjectLinksManager.from_client',
            return_value=mocker.AsyncMock(get_data_app_dashboard_link=mocker.MagicMock(return_value=mock_link)),
        )

        result = await get_data_apps(ctx=mcp_context_client)

        assert len(result.data_apps) == 0


class TestFetchDataAppValidation:
    """Tests for _fetch_data_app component_id validation."""

    @pytest.mark.asyncio
    async def test_fetch_data_app_by_data_app_id_validates_component_id(
        self, mocker, keboola_client: KeboolaClient
    ) -> None:
        """When fetching by data_app_id, raises ValueError if component_id doesn't match."""
        wrong_component_id = 'keboola.sandboxes'
        data_app_id = 'app-123'

        keboola_client.data_science_client.get_data_app = mocker.AsyncMock(
            return_value=_make_data_app_response(component_id=wrong_component_id, data_app_id=data_app_id)
        )

        with pytest.raises(ValueError, match=f'Data app tools only support {DATA_APP_COMPONENT_ID} component'):
            await _fetch_data_app(keboola_client, data_app_id=data_app_id, configuration_id=None)

    @pytest.mark.asyncio
    async def test_fetch_data_app_by_configuration_id_validates_component_id(
        self, mocker, keboola_client: KeboolaClient
    ) -> None:
        """When fetching by configuration_id, raises ValueError if component_id doesn't match."""
        wrong_component_id = 'keboola.sandboxes'
        configuration_id = 'cfg-123'
        data_app_id = 'app-123'

        # Mock configuration_detail to return valid config
        keboola_client.storage_client.configuration_detail = mocker.AsyncMock(
            return_value={
                'id': configuration_id,
                'name': 'test-app',
                'description': 'test',
                'configuration': {'parameters': {'id': data_app_id}},
                'version': 1,
            }
        )

        # Mock get_data_app to return app with wrong component_id
        keboola_client.data_science_client.get_data_app = mocker.AsyncMock(
            return_value=_make_data_app_response(
                component_id=wrong_component_id, data_app_id=data_app_id, config_id=configuration_id
            )
        )

        with pytest.raises(ValueError, match=f'Data app tools only support {DATA_APP_COMPONENT_ID} component'):
            await _fetch_data_app(keboola_client, data_app_id=None, configuration_id=configuration_id)

    @pytest.mark.asyncio
    async def test_fetch_data_app_by_data_app_id_succeeds_with_correct_component(
        self, mocker, keboola_client: KeboolaClient
    ) -> None:
        """When component_id matches DATA_APP_COMPONENT_ID, fetch succeeds."""
        data_app_id = 'app-123'
        config_id = 'cfg-123'

        data_app_response = _make_data_app_response(
            component_id=DATA_APP_COMPONENT_ID, data_app_id=data_app_id, config_id=config_id
        )

        keboola_client.data_science_client.get_data_app = mocker.AsyncMock(return_value=data_app_response)
        keboola_client.storage_client.configuration_detail = mocker.AsyncMock(
            return_value={
                'id': config_id,
                'name': 'test-app',
                'description': 'test',
                'configuration': {'parameters': {'id': data_app_id}, 'authorization': {}, 'storage': {}},
                'version': 1,
            }
        )

        result = await _fetch_data_app(keboola_client, data_app_id=data_app_id, configuration_id=None)

        assert result.data_app_id == data_app_id
        assert result.component_id == DATA_APP_COMPONENT_ID

    @pytest.mark.asyncio
    async def test_fetch_data_app_requires_either_id(self, keboola_client: KeboolaClient) -> None:
        """When neither data_app_id nor configuration_id is provided, raises ValueError."""
        with pytest.raises(ValueError, match='Either data_app_id or configuration_id must be provided'):
            await _fetch_data_app(keboola_client, data_app_id=None, configuration_id=None)
