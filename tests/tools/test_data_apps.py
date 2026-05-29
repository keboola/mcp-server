import sys
from types import ModuleType
from typing import Literal, cast

import pytest
from fastmcp import Context

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import DataAppResponse
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.data_apps import (
    _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE,
    _STORAGE_QUERY_DATA_FUNCTION_CODE,
    MAX_DNS_LABEL_LENGTH,
    DataApp,
    DataAppSlugTooLongError,
    DataAppSummary,
    ModifiedDataAppOutput,
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
    modify_streamlit_data_app,
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
        configuration={},
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


@pytest.mark.parametrize(
    ('name', 'expected_slug', 'expected_error'),
    [
        pytest.param('a' * MAX_DNS_LABEL_LENGTH, 'a' * MAX_DNS_LABEL_LENGTH, None, id='at_max_length'),
        pytest.param('a' * (MAX_DNS_LABEL_LENGTH + 1), None, DataAppSlugTooLongError, id='exceeds_max_length'),
        pytest.param('a' * 70 + '!!!', None, DataAppSlugTooLongError, id='long_name_with_special_chars'),
        pytest.param('a' * 30 + '!' * 50 + 'b' * 30, 'a' * 30 + 'b' * 30, None, id='shortened_by_special_chars'),
    ],
)
def test_get_data_app_slug_length_validation(name, expected_slug, expected_error):
    """Test DNS label length validation in slug generation."""
    if expected_error:
        with pytest.raises(expected_error):
            _get_data_app_slug(name)
    else:
        slug = _get_data_app_slug(name)
        assert slug == expected_slug


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


def _load_query_data_function(code: str, result_pages: list[JsonDict], mocker):
    """Load injected query_data code with mocked httpx/pandas modules for isolated testing."""
    calls: JsonDict = {'get': [], 'post': []}
    result_pages = [page.copy() for page in result_pages]

    class FakeResponse:
        def __init__(self, payload: JsonDict) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> JsonDict:
            return self._payload

    class FakeClient:
        def __init__(self, *, timeout, limits) -> None:
            self.timeout = timeout
            self.limits = limits

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, url: str, json: JsonDict, headers: JsonDict) -> FakeResponse:
            calls['post'].append({'url': url, 'json': json, 'headers': headers})
            return FakeResponse({'queryJobId': 'job-1'})

        def get(self, url: str, headers: JsonDict, params: JsonDict | None = None) -> FakeResponse:
            calls['get'].append({'url': url, 'headers': headers, 'params': params})
            if url.endswith('/queries/job-1'):
                return FakeResponse({'status': 'completed', 'statements': [{'id': 'stmt-1'}]})
            if url.endswith('/results'):
                return FakeResponse(result_pages.pop(0))
            raise AssertionError(f'Unexpected GET URL: {url}')

    httpx_module = ModuleType('httpx')
    httpx_module.Timeout = lambda **kwargs: kwargs
    httpx_module.Limits = lambda **kwargs: kwargs
    httpx_module.Client = FakeClient

    pandas_module = ModuleType('pandas')
    pandas_module.DataFrame = lambda rows: rows

    mocker.patch.dict(sys.modules, {'httpx': httpx_module, 'pandas': pandas_module})

    namespace: dict[str, object] = {}
    exec(code, namespace)
    return namespace['query_data'], calls


def test_query_service_query_data_paginates_results(mocker, monkeypatch) -> None:
    query_data, calls = _load_query_data_function(
        _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE,
        [
            {
                'status': 'completed',
                'columns': [{'name': 'id'}],
                'numberOfRows': 3,
                'data': [['1'], ['2']],
            },
            {
                'status': 'completed',
                'columns': [{'name': 'id'}],
                'numberOfRows': 3,
                'data': [['3']],
            },
        ],
        mocker,
    )
    monkeypatch.setenv('BRANCH_ID', '123')
    monkeypatch.setenv('WORKSPACE_ID', '456')
    monkeypatch.setenv('KBC_TOKEN', 'test-token')
    monkeypatch.setenv('KBC_URL', 'https://connection.keboola.com')

    result = query_data('SELECT * FROM test')

    assert result == [{'id': '1'}, {'id': '2'}, {'id': '3'}]
    result_calls = [call for call in calls['get'] if call['url'].endswith('/results')]
    assert len(result_calls) == 2
    assert result_calls[0]['params']['offset'] == 0
    assert result_calls[1]['params']['offset'] == 2
    assert 'pageSize' in result_calls[0]['params']


def test_query_service_query_data_stops_on_short_page_without_total_count(mocker, monkeypatch) -> None:
    query_data, calls = _load_query_data_function(
        _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE,
        [
            {
                'status': 'completed',
                'columns': [{'name': 'id'}],
                'data': [['1'], ['2']],
            }
        ],
        mocker,
    )
    monkeypatch.setenv('BRANCH_ID', '123')
    monkeypatch.setenv('WORKSPACE_ID', '456')
    monkeypatch.setenv('KBC_TOKEN', 'test-token')
    monkeypatch.setenv('KBC_URL', 'https://connection.keboola.com')

    result = query_data('SELECT * FROM test')

    assert result == [{'id': '1'}, {'id': '2'}]
    result_calls = [call for call in calls['get'] if call['url'].endswith('/results')]
    assert len(result_calls) == 1


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
    assert sorted(new['parameters']['packages']) == sorted(['pandas', 'httpx'])
    # Secrets merged
    assert new['parameters']['dataApp']['secrets'] == {'FOO': 'old', 'KEEP': 'x', 'NEW': 'y'}
    # Authentication updated
    assert new['authorization'] == _get_authorization(True)


def test_update_existing_data_app_config_preserves_existing_secrets():
    existing = {
        'parameters': {
            'dataApp': {
                'slug': 'old-slug',
                'secrets': {
                    'WORKSPACE_ID': 'wid-old',
                    'BRANCH_ID': 'branch-old',
                    'KEEP': 'x',
                },
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
        secrets={'WORKSPACE_ID': 'wid-new', 'BRANCH_ID': 'branch-new', 'NEW': 'y'},
        sql_dialect='snowflake',
    )

    assert new['parameters']['dataApp']['secrets'] == {
        'WORKSPACE_ID': 'wid-old',
        'BRANCH_ID': 'branch-old',
        'KEEP': 'x',
        'NEW': 'y',
    }


def test_get_secrets():
    secrets = _get_secrets(
        workspace_id='wid-1234',
        branch_id='123',
    )
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

    # Deepcopy makes it equal-but-not-identical.
    assert new['authorization'] == existing_authorization
    assert new['parameters']['script'] == ['old']
    # verify the rest of the config is still updated
    assert new['parameters']['dataApp']['slug'] == 'old-slug'
    assert 'numpy' in new['parameters']['packages']
    assert 'httpx' in new['parameters']['packages']
    assert new['parameters']['dataApp']['secrets']['KEEP'] == 'secret'


def test_update_existing_data_app_config_no_authorization_key():
    """Existing configs that lack an `authorization` key must not crash when authentication_type='default'."""
    existing = {
        'parameters': {
            'dataApp': {'slug': 'x', 'secrets': {}},
            'script': ['old'],
            'packages': [],
        },
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
    assert 'authorization' not in new


def test_update_existing_data_app_config_basic_auth_overwrites_oidc():
    """Explicit 'basic-auth' must replace an existing OIDC block."""
    existing = {
        'parameters': {
            'dataApp': {'slug': 'x', 'secrets': {}},
            'script': ['old'],
            'packages': [],
        },
        'authorization': {
            'app_proxy': {
                'auth_providers': [{'id': 'oidc', 'type': 'oidc'}],
                'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['oidc']}],
            }
        },
    }
    new = _update_existing_data_app_config(
        existing_config=existing,
        name='',
        source_code='',
        packages=[],
        authentication_type='basic-auth',
        secrets={},
        sql_dialect='snowflake',
    )
    assert new['authorization']['app_proxy']['auth_rules'] == [
        {'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['simpleAuth']}
    ]


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


# =============================================================================
# FOLDER METADATA TESTS
# =============================================================================


@pytest.mark.parametrize(
    (
        'configuration_id',
        'folder',
        'app_count',
        'app_folders',
        'expect_folder_metadata',
        'expect_folder_delete',
        'expect_hint',
    ),
    [
        # Create path (no configuration_id)
        ('', 'Analytics', 0, [], True, False, False),
        ('', '  Analytics  ', 0, [], True, False, False),  # whitespace stripped
        ('', None, 5, [], False, False, False),
        ('', None, 25, ['Analytics'], False, False, True),
        # Update path (with configuration_id)
        ('cfg-1', 'Analytics', 0, [], True, False, False),
        ('cfg-1', None, 5, [], False, False, False),
        ('cfg-1', None, 25, ['Analytics'], False, False, True),
        ('cfg-1', '', 5, [], False, True, False),  # empty string → delete
    ],
    ids=[
        'create_folder_provided',
        'create_folder_whitespace_stripped',
        'create_no_folder_few',
        'create_no_folder_many_with_hint',
        'update_folder_provided',
        'update_no_folder_few',
        'update_no_folder_many_with_hint',
        'update_folder_empty_deletes',
    ],
)
@pytest.mark.asyncio
async def test_modify_streamlit_data_app_folder(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    configuration_id: str,
    folder,
    app_count: int,
    app_folders: list[str],
    expect_folder_metadata: bool,
    expect_folder_delete: bool,
    expect_hint: bool,
) -> None:
    """Test folder metadata and change_summary hint for modify_streamlit_data_app (create and update paths)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

    workspace_manager.get_workspace_id = mocker.AsyncMock(return_value=1)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value='snowflake')
    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='default')

    keboola_client.storage_client.project_id = mocker.AsyncMock(return_value='proj-1')

    # Dummy encrypted config
    encrypted_config = {
        'parameters': {'script': ['SELECT 1']},
        'storage': {},
        'authorization': {'app_proxy': {'auth_providers': [], 'auth_rules': []}},
    }
    keboola_client.encryption_client = mocker.AsyncMock()
    keboola_client.encryption_client.encrypt = mocker.AsyncMock(return_value=encrypted_config)

    data_app_response = _make_data_app_response(config_id=configuration_id or 'new-cfg-1')

    if configuration_id:
        # Update path
        existing_data_app = DataApp(
            name='My App',
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            data_app_id='app-1',
            project_id='proj-1',
            branch_id='default',
            config_version='2',
            type='streamlit',
            auto_suspend_after_seconds=900,
            configuration=encrypted_config,
            state='stopped',
        )
        mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', return_value=existing_data_app)
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.modify_streamlit_data_app_internal',
            mocker.AsyncMock(return_value=(existing_data_app, encrypted_config, None)),
        )
        keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    else:
        # Create path
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.DataAppConfig.model_validate',
            return_value=mocker.MagicMock(authorization={'app_proxy': {'auth_providers': [], 'auth_rules': []}}),
        )
        keboola_client.data_science_client = mocker.AsyncMock()
        keboola_client.data_science_client.create_data_app = mocker.AsyncMock(return_value=data_app_response)

    mocker.patch(
        'keboola_mcp_server.tools.components.utils.get_config_folders',
        mocker.AsyncMock(return_value=(app_count, app_folders, False)),
    )
    keboola_client.storage_client.configuration_metadata_get = mocker.AsyncMock(
        return_value=[{'id': 'meta-1', 'key': MetadataField.CONFIGURATION_FOLDER_NAME, 'value': 'OldFolder'}]
    )
    keboola_client.storage_client.configuration_metadata_delete = mocker.AsyncMock()

    result = await modify_streamlit_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        source_code='import streamlit as st\n{QUERY_DATA_FUNCTION}\nst.write("hello")',
        packages=[],
        authentication_type='no-auth',
        configuration_id=configuration_id,
        change_description='test',
        folder=folder,
    )

    assert isinstance(result, ModifiedDataAppOutput)
    metadata_calls = [
        call
        for call in keboola_client.storage_client.configuration_metadata_update.call_args_list
        if call.kwargs.get('metadata', {}).get(MetadataField.CONFIGURATION_FOLDER_NAME)
    ]
    if expect_folder_metadata:
        assert len(metadata_calls) == 1
        assert metadata_calls[0].kwargs['metadata'] == {MetadataField.CONFIGURATION_FOLDER_NAME: folder.strip()}
    else:
        assert len(metadata_calls) == 0
    if expect_folder_delete:
        keboola_client.storage_client.configuration_metadata_delete.assert_called_once_with(
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            metadata_id='meta-1',
        )
    else:
        keboola_client.storage_client.configuration_metadata_delete.assert_not_called()
    if expect_hint:
        assert result.change_summary is not None
        assert str(app_count) in result.change_summary
    else:
        assert result.change_summary is None


# ===== Tests for modify_python_js_data_app =====


from keboola_mcp_server.clients.data_science import (  # noqa: E402
    AppGitRepoResponse,
    CreatedGitCredentialResponse,
)
from keboola_mcp_server.tools.data_apps import (  # noqa: E402
    CreatedGitCredentialOutput,
    ModifiedPythonJsDataAppOutput,
    _update_existing_code_data_app_config,
    create_python_js_data_app_git_credential,
    modify_python_js_data_app,
)


def _make_python_js_data_app_response(
    data_app_id: str = 'app-pyjs-1',
    config_id: str = 'cfg-pyjs-1',
) -> DataAppResponse:
    return DataAppResponse(
        id=data_app_id,
        project_id='proj-1',
        component_id=DATA_APP_COMPONENT_ID,
        branch_id='branch-1',
        config_id=config_id,
        config_version='1',
        type='python-js',
        state='created',
        desired_state='created',
        url='https://demo.canary-orion.keboola.dev',
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('missing_arg', 'kwargs', 'error_match'),
    [
        (
            'slug',
            {'name': 'A', 'description': ''},
            'slug is required',
        ),
    ],
)
async def test_modify_python_js_data_app_create_validates_required_args(
    mcp_context_client: Context,
    missing_arg: str,
    kwargs: dict,
    error_match: str,
) -> None:
    """Create path raises clear ValueError when slug is missing."""
    with pytest.raises(ValueError, match=error_match):
        await modify_python_js_data_app(ctx=mcp_context_client, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('disallowed_arg', 'kwargs', 'error_match'),
    [
        (
            'slug',
            {'name': 'A', 'description': '', 'configuration_id': 'cfg-1', 'slug': 'new'},
            'slug cannot be changed',
        ),
    ],
)
async def test_modify_python_js_data_app_update_rejects_create_only_args(
    mcp_context_client: Context,
    disallowed_arg: str,
    kwargs: dict,
    error_match: str,
) -> None:
    """Update path rejects slug (immutable subdomain)."""
    with pytest.raises(ValueError, match=error_match):
        await modify_python_js_data_app(ctx=mcp_context_client, **kwargs)


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_calls_full_provisioning_chain(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Create path: POST /apps with type=python-js + useManagedGitRepo, fetch repo URL. Git
    credential creation is now a separate tool — not exercised here."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)

    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    app_response = _make_python_js_data_app_response()
    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(return_value=app_response)
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )

    # avoid hitting Storage API for metadata helpers
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    result = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        slug='my-app',
        auto_suspend_after_seconds=300,
    )

    assert isinstance(result, ModifiedPythonJsDataAppOutput)
    assert result.response == 'created'
    assert result.repo_url == 'https://managed.repo/org/app.git'
    assert result.data_app.repo_url == 'https://managed.repo/org/app.git'
    assert result.data_app.type == 'python-js'

    # Verify the create payload was python-js + managed repo
    create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
    assert create_kwargs['app_type'] == 'python-js'
    assert create_kwargs['use_managed_git_repo'] is True
    # Verify auto_suspend_after_seconds flows through and we don't pin runtime.image (the
    # platform now picks a default for python-js apps).
    serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
    assert serialized['parameters']['autoSuspendAfterSeconds'] == 300
    assert serialized['parameters']['dataApp']['slug'] == 'my-app'
    assert 'image' not in serialized.get('runtime', {})
    # Created with the auto-workspace flag so the platform provisions a per-app workspace
    # and sets WORKSPACE_ID itself.
    assert serialized['runtime']['workspace'] == {'enabled': True}
    # KBC_TOKEN / KBC_URL / BRANCH_ID are injected by the platform at runtime — the MCP must
    # not bake them into the stored config.
    assert 'secrets' not in serialized['parameters']['dataApp']
    # Prod apps must NOT be marked as draft (UT-4000).
    assert 'isDraft' not in serialized['parameters']['dataApp']
    # Default `authentication_type='default'` produces basic-auth on create (safe-by-default).
    assert serialized['authorization']['app_proxy']['auth_providers'] == [{'id': 'simpleAuth', 'type': 'password'}]
    assert serialized['authorization']['app_proxy']['auth_rules'] == [
        {'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['simpleAuth']}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('authentication_type', 'expect_basic_auth'),
    [
        ('default', True),
        ('basic-auth', True),
        ('no-auth', False),
    ],
)
async def test_modify_python_js_data_app_create_authentication_type(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    authentication_type: str,
    expect_basic_auth: bool,
) -> None:
    """Create path translates authentication_type to the right authorization block:
    'default' and 'basic-auth' → password-protected; 'no-auth' → public."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)

    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    app_response = _make_python_js_data_app_response()
    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(return_value=app_response)
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    _ = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        slug='my-app',
        authentication_type=cast(Literal['no-auth', 'basic-auth', 'default'], authentication_type),
    )

    serialized = keboola_client.data_science_client.create_data_app.await_args.kwargs['configuration'].model_dump(
        by_alias=True, exclude_none=True
    )
    auth_rule = serialized['authorization']['app_proxy']['auth_rules'][0]
    if expect_basic_auth:
        assert auth_rule['auth_required'] is True
        assert auth_rule['auth'] == ['simpleAuth']
    else:
        assert auth_rule['auth_required'] is False


@pytest.mark.asyncio
async def test_modify_python_js_data_app_update_patches_storage_config(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Update path: fetch storage config → merge updates → PATCH via configuration_update."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)

    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    existing_data_app = DataApp(
        name='Old',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='2',
        type='python-js',
        configuration={
            'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'old-slug'}},
            'runtime': {'image': {'version': 'old-version'}},
        },
        state='stopped',
    )
    updated_data_app = existing_data_app.model_copy(update={'config_version': '3', 'name': 'New'})

    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(side_effect=[existing_data_app, updated_data_app]),
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_update_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    result = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='New',
        description='new desc',
        configuration_id='cfg-1',
        auto_suspend_after_seconds=600,
    )

    assert isinstance(result, ModifiedPythonJsDataAppOutput)
    assert result.response == 'updated'
    # The PATCH should carry merged config
    patch_kwargs = keboola_client.storage_client.configuration_update.await_args.kwargs
    new_cfg = patch_kwargs['configuration']
    assert new_cfg['parameters']['autoSuspendAfterSeconds'] == 600
    # The platform now picks a default image for python-js apps; the MCP must NOT overwrite a
    # legacy image pin already in the config, but must NOT force it to any new value either.
    assert new_cfg['runtime']['image']['version'] == 'old-version'
    # slug must remain untouched (immutable)
    assert new_cfg['parameters']['dataApp']['slug'] == 'old-slug'
    # Update does NOT backfill `runtime.workspace` — only the create path sets it.
    assert 'workspace' not in new_cfg['runtime']
    # KBC_TOKEN / KBC_URL / BRANCH_ID are injected by the platform at runtime — the MCP must
    # not write them back into the stored config on update either.
    assert 'secrets' not in new_cfg['parameters']['dataApp']


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_without_workspace_feature(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """When the project lacks `data-apps-storage-workspace`, the create path omits
    `runtime.workspace` and falls back to injecting WORKSPACE_ID via `parameters.dataApp.secrets`."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)

    workspace_manager.get_workspace_id = mocker.AsyncMock(return_value='wid-legacy')

    app_response = _make_python_js_data_app_response()
    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(return_value=app_response)
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    _ = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        slug='my-app',
    )

    serialized = keboola_client.data_science_client.create_data_app.await_args.kwargs['configuration'].model_dump(
        by_alias=True, exclude_none=True
    )
    # Without the workspace feature, the `runtime` block carries no fields the MCP wants to
    # set (image is platform-default, workspace is off), so it's omitted entirely.
    assert 'runtime' not in serialized
    assert serialized['parameters']['dataApp']['secrets'] == {'WORKSPACE_ID': 'wid-legacy'}


@pytest.mark.asyncio
async def test_modify_python_js_data_app_update_injects_workspace_id_without_feature(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """When the project lacks `data-apps-storage-workspace`, the update path merges WORKSPACE_ID
    into the existing secrets map without overwriting any keys already present."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)

    workspace_manager.get_workspace_id = mocker.AsyncMock(return_value='wid-legacy')

    existing_data_app = DataApp(
        name='Old',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='2',
        type='python-js',
        configuration={
            'parameters': {
                'autoSuspendAfterSeconds': 900,
                'dataApp': {'slug': 'old-slug', 'secrets': {'KEEP': 'x'}},
            },
            'runtime': {'image': {'version': 'old-version'}},
        },
        state='stopped',
    )
    updated_data_app = existing_data_app.model_copy(update={'config_version': '3'})

    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(side_effect=[existing_data_app, updated_data_app]),
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_update_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='Old',
        description='desc',
        configuration_id='cfg-1',
        auto_suspend_after_seconds=600,
    )

    patch_kwargs = keboola_client.storage_client.configuration_update.await_args.kwargs
    new_cfg = patch_kwargs['configuration']
    assert new_cfg['parameters']['dataApp']['secrets'] == {'KEEP': 'x', 'WORKSPACE_ID': 'wid-legacy'}


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_passes_storage_through(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Create path forwards a caller-supplied `storage` block (with direct-grant) into the DSAPI payload."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    app_response = _make_python_js_data_app_response()
    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(return_value=app_response)
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    storage = {
        'output': {
            'tables': [
                {'destination': 'in.c-ex-generic-v2.earthquake_events', 'unload_strategy': 'direct-grant'},
            ],
        },
    }

    _ = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        slug='my-app',
        storage=storage,
    )

    serialized = keboola_client.data_science_client.create_data_app.await_args.kwargs['configuration'].model_dump(
        by_alias=True, exclude_none=True
    )
    assert serialized['storage'] == storage


@pytest.mark.asyncio
async def test_modify_python_js_data_app_update_replaces_storage(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Update path: a non-empty `storage` argument replaces the entire stored storage block."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    existing_data_app = DataApp(
        name='Old',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='2',
        type='python-js',
        configuration={
            'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'old-slug'}},
            'runtime': {'image': {'version': 'old-version'}},
            'storage': {'input': {'tables': [{'source': 'in.c-main.stale', 'destination': 'stale.csv'}]}},
        },
        state='stopped',
    )
    updated_data_app = existing_data_app.model_copy(update={'config_version': '3', 'name': 'New'})

    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(side_effect=[existing_data_app, updated_data_app]),
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_update_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    new_storage = {
        'output': {
            'tables': [
                {'destination': 'in.c-ex-generic-v2.earthquake_events', 'unload_strategy': 'direct-grant'},
            ],
        },
    }

    await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='New',
        description='new desc',
        configuration_id='cfg-1',
        auto_suspend_after_seconds=600,
        storage=new_storage,
    )

    patch_kwargs = keboola_client.storage_client.configuration_update.await_args.kwargs
    assert patch_kwargs['configuration']['storage'] == new_storage


@pytest.mark.asyncio
async def test_modify_python_js_data_app_storage_validation_rejects_missing_source(
    mcp_context_client: Context,
) -> None:
    """An output table with neither `source` nor `unload_strategy='direct-grant'` must be rejected.

    The `@tool_errors()` decorator wraps the underlying RecoverableValidationError into a
    fastmcp ToolError before it surfaces to the caller.
    """
    from fastmcp.exceptions import ToolError

    with pytest.raises(ToolError, match="'source' is a required property"):
        await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='My App',
            description='desc',
            slug='my-app',
            storage={'output': {'tables': [{'destination': 'in.c-ex.foo'}]}},
        )


def test_update_existing_code_data_app_config_leaves_legacy_image_pin_alone() -> None:
    """The MCP no longer manages `runtime.image.version` — the platform picks a default for
    python-js apps. A legacy pin already in the stored config must survive the deepcopy
    verbatim (we don't overwrite it, even though we also don't set it ourselves)."""
    existing = {
        'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}},
        'runtime': {'image': {'version': 'old'}},
    }
    new = _update_existing_code_data_app_config(existing, auto_suspend_after_seconds=600)
    assert new['runtime']['image']['version'] == 'old'
    assert new['parameters']['autoSuspendAfterSeconds'] == 600
    # original must not be mutated
    assert existing['parameters']['autoSuspendAfterSeconds'] == 900


def test_update_existing_code_data_app_config_default_auth_preserves_existing() -> None:
    """`authentication_type='default'` must not touch an existing authorization block (e.g. OIDC)."""
    existing_authorization = {
        'app_proxy': {
            'auth_providers': [{'id': 'oidc', 'type': 'oidc', 'issuer_url': 'https://issuer'}],
            'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['oidc']}],
        }
    }
    existing = {
        'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}},
        'authorization': existing_authorization,
    }
    new = _update_existing_code_data_app_config(existing, auto_suspend_after_seconds=900, authentication_type='default')
    # Deepcopy makes it equal-but-not-identical.
    assert new['authorization'] == existing_authorization


def test_update_existing_code_data_app_config_basic_auth_overwrites() -> None:
    existing = {
        'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}},
        'authorization': {'app_proxy': {'auth_providers': [], 'auth_rules': []}},
    }
    new = _update_existing_code_data_app_config(
        existing, auto_suspend_after_seconds=900, authentication_type='basic-auth'
    )
    assert new['authorization']['app_proxy']['auth_rules'] == [
        {'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['simpleAuth']}
    ]


def test_update_existing_code_data_app_config_preserves_legacy_secrets() -> None:
    """Legacy configs written by older MCP versions may carry a `secrets` block. We no longer
    write secrets (platform injects KBC_TOKEN/KBC_URL/BRANCH_ID at runtime), but the deepcopy
    of the existing config must leave any pre-existing keys untouched."""
    existing = {
        'parameters': {
            'autoSuspendAfterSeconds': 900,
            'dataApp': {
                'slug': 'x',
                'secrets': {'WORKSPACE_ID': 'wid-legacy', 'KEEP': 'x'},
            },
        },
    }
    new = _update_existing_code_data_app_config(
        existing,
        auto_suspend_after_seconds=900,
    )
    assert new['parameters']['dataApp']['secrets'] == {'WORKSPACE_ID': 'wid-legacy', 'KEEP': 'x'}


def test_update_existing_code_data_app_config_no_auth_overwrites() -> None:
    existing = {
        'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}},
        'authorization': {
            'app_proxy': {
                'auth_providers': [{'id': 'simpleAuth', 'type': 'password'}],
                'auth_rules': [{'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['simpleAuth']}],
            }
        },
    }
    new = _update_existing_code_data_app_config(existing, auto_suspend_after_seconds=900, authentication_type='no-auth')
    assert new['authorization']['app_proxy']['auth_rules'] == [
        {'type': 'pathPrefix', 'value': '/', 'auth_required': False}
    ]


@pytest.mark.parametrize(
    ('passed_storage', 'expected_storage_key_present', 'expected_storage'),
    [
        # None preserves the existing storage block untouched
        (None, True, {'input': {'tables': [{'source': 'in.c-main.kept', 'destination': 'kept.csv'}]}}),
        # Empty dict is an explicit wipe
        ({}, True, {}),
        # Non-empty dict replaces the existing block wholesale
        (
            {'output': {'tables': [{'destination': 'in.c-main.new', 'unload_strategy': 'direct-grant'}]}},
            True,
            {'output': {'tables': [{'destination': 'in.c-main.new', 'unload_strategy': 'direct-grant'}]}},
        ),
    ],
)
def test_update_existing_code_data_app_config_storage_semantics(
    passed_storage, expected_storage_key_present, expected_storage
) -> None:
    """`storage=None` preserves; `storage={}` wipes; a non-empty dict replaces wholesale."""
    existing = {
        'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}},
        'storage': {'input': {'tables': [{'source': 'in.c-main.kept', 'destination': 'kept.csv'}]}},
    }
    new = _update_existing_code_data_app_config(
        existing,
        auto_suspend_after_seconds=900,
        storage=passed_storage,
    )
    assert ('storage' in new) is expected_storage_key_present
    assert new['storage'] == expected_storage


# ===== Tests for deploy_data_app with mode and python-js =====


@pytest.mark.asyncio
async def test_deploy_data_app_python_js_skips_storage_config_version_and_passes_mode(
    mocker,
    mcp_context_client: Context,
) -> None:
    """python-js deploy: no configVersion fetch from Storage, mode forwarded to DSAPI."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    pyjs_app = DataApp(
        name='py-app',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}}},
        state='stopped',
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=pyjs_app))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))

    # If the code accidentally calls storage_client.configuration_version_latest, this AsyncMock raises.
    keboola_client.storage_client.configuration_version_latest = mocker.AsyncMock(
        side_effect=AssertionError('Should not call configuration_version_latest for python-js apps')
    )

    _ = await deploy_data_app(
        ctx=mcp_context_client,
        action='deploy',
        configuration_id='cfg-1',
        mode='dev',
    )

    keboola_client.data_science_client.deploy_data_app.assert_awaited_once_with('app-1', None, mode='dev', branch=None)
    keboola_client.storage_client.configuration_version_latest.assert_not_called()


@pytest.mark.asyncio
async def test_deploy_data_app_streamlit_still_passes_config_version(
    mocker,
    mcp_context_client: Context,
    data_app: DataApp,  # streamlit fixture
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    data_app.state = 'stopped'
    data_app.type = 'streamlit'
    data_app.configuration = {'authorization': {'app_proxy': {'auth_providers': [], 'auth_rules': []}}}
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=data_app))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))
    keboola_client.storage_client.configuration_version_latest = mocker.AsyncMock(return_value=7)

    _ = await deploy_data_app(ctx=mcp_context_client, action='deploy', configuration_id='cfg-streamlit')

    keboola_client.data_science_client.deploy_data_app.assert_awaited_once_with(
        data_app.data_app_id, '7', mode=None, branch=None
    )


# ===== Tests for modify_python_js_data_app draft create path =====


def _make_python_js_parent_data_app(
    *,
    data_app_id: str = 'app-prod-1',
    configuration_id: str = 'cfg-prod-1',
    repo_url: str | None = 'https://managed.repo/org/prod.git',
    type: str = 'python-js',
) -> DataApp:
    """Build a DataApp the way `_fetch_data_app` would when looking up the parent."""
    return DataApp(
        name='Prod App',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id=configuration_id,
        data_app_id=data_app_id,
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type=type,
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'demo'}}},
        state='running',
        repo_url=repo_url,
    )


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_draft_uses_external_git(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """When `parent_configuration_id` is set, the new app is a draft: no managed repo of its own,
    `parameters.dataApp.git` populated with the parent's repo URL + a freshly minted prod-app token,
    and the config is encrypted before being sent to data-science."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    parent_repo = 'https://managed.repo/org/prod.git'
    parent_data_app_id = 'app-prod-1'
    parent = _make_python_js_parent_data_app(
        data_app_id=parent_data_app_id, configuration_id='cfg-prod-1', repo_url=parent_repo
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=parent))

    keboola_client.data_science_client.create_app_git_credential = mocker.AsyncMock(
        return_value=CreatedGitCredentialResponse(
            id='cred-1', type='http_token', permissions='readWrite', secret='token-xyz'
        )
    )
    twin_response = _make_python_js_data_app_response(data_app_id='app-dev-1', config_id='cfg-dev-1')
    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(return_value=twin_response)
    # The draft has no managed repo, so get_app_git_repo must NOT be called for it.
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        side_effect=AssertionError('Should not fetch a git repo URL for a draft')
    )

    keboola_client.storage_client.project_id = mocker.AsyncMock(return_value='proj-1')

    # Mock encryption: walk the dict and prefix `KBC::cipher::` onto any value whose key starts with '#'.
    async def fake_encrypt(value, *, project_id=None, component_id=None, config_id=None):
        def walk(node):
            if isinstance(node, dict):
                return {
                    k: (f'KBC::cipher::{v}' if k.startswith('#') and isinstance(v, str) else walk(v))
                    for k, v in node.items()
                }
            return node

        return walk(value)

    keboola_client.encryption_client = mocker.AsyncMock()
    keboola_client.encryption_client.encrypt = mocker.AsyncMock(side_effect=fake_encrypt)

    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    result = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='Dev Twin',
        description='dev iteration twin',
        slug='demo-dev-abc123',
        parent_configuration_id='cfg-prod-1',
        branch='iter-feat',
    )

    assert isinstance(result, ModifiedPythonJsDataAppOutput)
    assert result.response == 'created'
    assert result.repo_url == parent_repo
    assert result.branch == 'iter-feat'
    assert result.git_clone_url is not None
    assert result.git_clone_url.startswith('https://kai:token-xyz@managed.repo/')

    # Credential was minted on the parent, not the new dev twin.
    keboola_client.data_science_client.create_app_git_credential.assert_awaited_once_with(parent_data_app_id)

    # create_data_app received use_managed_git_repo=False and the external-git block.
    create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
    assert create_kwargs['app_type'] == 'python-js'
    assert create_kwargs['use_managed_git_repo'] is False
    serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
    git_block = serialized['parameters']['dataApp']['git']
    assert git_block == {
        'repository': parent_repo,
        'username': 'kai',
        '#password': 'KBC::cipher::token-xyz',
        'branch': 'iter-feat',
    }

    # Dev twin is marked as draft so the UI hides it from the main data-apps list (UT-4000).
    assert serialized['parameters']['dataApp']['isDraft'] is True

    # Encryption was actually called (so `#password` is ciphertext on the wire).
    keboola_client.encryption_client.encrypt.assert_awaited_once()
    encrypt_kwargs = keboola_client.encryption_client.encrypt.await_args.kwargs
    assert encrypt_kwargs['component_id'] == DATA_APP_COMPONENT_ID
    assert encrypt_kwargs['project_id'] == 'proj-1'

    # No managed-repo lookup happened on the dev twin.
    keboola_client.data_science_client.get_app_git_repo.assert_not_called()


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_draft_defaults_branch_to_init(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Omitting `branch` pins the draft to the literal `init` branch (sensible default for the very
    first draft of a brand-new prod app — descriptive branches are agent-supplied on edits)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    parent = _make_python_js_parent_data_app()
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=parent))
    keboola_client.data_science_client.create_app_git_credential = mocker.AsyncMock(
        return_value=CreatedGitCredentialResponse(
            id='cred-1', type='http_token', permissions='readWrite', secret='token-xyz'
        )
    )
    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(
        return_value=_make_python_js_data_app_response()
    )
    keboola_client.storage_client.project_id = mocker.AsyncMock(return_value='proj-1')
    keboola_client.encryption_client = mocker.AsyncMock()
    keboola_client.encryption_client.encrypt = mocker.AsyncMock(side_effect=lambda v, **_: v)
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    result = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='Draft',
        description='draft iteration',
        slug='demo-draft',
        parent_configuration_id='cfg-prod-1',
    )

    assert result.branch == 'init'
    # And the stored config carries the same branch as the pin and the parent linkage.
    create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
    serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
    assert serialized['parameters']['dataApp']['git']['branch'] == 'init'
    assert serialized['parameters']['dataApp']['parentConfigurationId'] == 'cfg-prod-1'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('kwargs', 'error_match'),
    [
        (
            {
                'name': 'A',
                'description': '',
                'parent_configuration_id': 'cfg-prod-1',
            },
            'slug is required',
        ),
    ],
)
async def test_modify_python_js_data_app_create_draft_still_requires_slug(
    mcp_context_client: Context,
    kwargs: dict,
    error_match: str,
) -> None:
    """`parent_configuration_id` does not waive any required create-time argument."""
    with pytest.raises(ValueError, match=error_match):
        await modify_python_js_data_app(ctx=mcp_context_client, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('disallowed_arg', 'kwargs', 'error_match'),
    [
        (
            'parent_configuration_id',
            {
                'name': 'A',
                'description': '',
                'configuration_id': 'cfg-1',
                'parent_configuration_id': 'cfg-prod-1',
            },
            'parent_configuration_id is only valid when creating a draft',
        ),
        (
            'branch',
            {
                'name': 'A',
                'description': '',
                'configuration_id': 'cfg-1',
                'branch': 'add-revenue-filter',
            },
            'branch is only valid when creating a draft',
        ),
    ],
)
async def test_modify_python_js_data_app_update_rejects_draft_args(
    mcp_context_client: Context,
    disallowed_arg: str,
    kwargs: dict,
    error_match: str,
) -> None:
    """The update path rejects draft-only args (`parent_configuration_id`, `branch`)."""
    with pytest.raises(ValueError, match=error_match):
        await modify_python_js_data_app(ctx=mcp_context_client, **kwargs)


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_draft_rejects_when_parent_is_streamlit(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """A Streamlit `parent_configuration_id` is rejected — only python-js prods can parent a draft."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    streamlit_parent = _make_python_js_parent_data_app(type='streamlit', repo_url=None)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=streamlit_parent))

    with pytest.raises(ValueError, match='only python-js prod apps can parent a draft'):
        await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='Draft',
            description='',
            slug='demo-draft',
            parent_configuration_id='cfg-prod-1',
        )


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_draft_rejects_when_parent_missing_repo_url(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Defensive: parent's repo lookup returned no URL — surface a clear error."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    parent = _make_python_js_parent_data_app(repo_url=None)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=parent))

    with pytest.raises(ValueError, match='has no managed git repo URL'):
        await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='Dev Twin',
            description='',
            slug='demo-dev',
            parent_configuration_id='cfg-prod-1',
        )


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_prod_calls_get_app_git_repo_for_url(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Prod creates always go through get_app_git_repo (no short-circuit branch)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_branch_id = mocker.AsyncMock(return_value='branch-1')

    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(
        return_value=_make_python_js_data_app_response()
    )
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url=None,
            https_url='https://managed.repo/org/prod.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    result = await modify_python_js_data_app(ctx=mcp_context_client, name='Prod', description='', slug='demo')

    assert result.repo_url == 'https://managed.repo/org/prod.git'
    keboola_client.data_science_client.get_app_git_repo.assert_awaited_once()
    create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
    assert create_kwargs['use_managed_git_repo'] is True
    # No git block on prod create.
    serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
    assert 'git' not in serialized['parameters']['dataApp']


# ===== Tests for deploy_data_app branch parameter =====


@pytest.mark.asyncio
async def test_deploy_data_app_python_js_with_branch_forwards_to_client(
    mocker,
    mcp_context_client: Context,
) -> None:
    """For python-js drafts, `branch` must be forwarded to the underlying DSAPI deploy call."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    pyjs_app = DataApp(
        name='twin',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-twin',
        data_app_id='app-twin',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'demo-dev'}}},
        state='stopped',
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=pyjs_app))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))

    _ = await deploy_data_app(
        ctx=mcp_context_client,
        action='deploy',
        configuration_id='cfg-twin',
        mode='dev',
        branch='feature-x',
    )

    keboola_client.data_science_client.deploy_data_app.assert_awaited_once_with(
        'app-twin', None, mode='dev', branch='feature-x'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize('bad_mode', [None, 'production'])
async def test_deploy_data_app_branch_without_dev_mode_rejected(
    mcp_context_client: Context,
    bad_mode,
) -> None:
    """`branch` is only meaningful with mode='dev' — anything else must raise."""
    with pytest.raises(ValueError, match='branch is only meaningful with mode="dev"'):
        await deploy_data_app(
            ctx=mcp_context_client,
            action='deploy',
            configuration_id='cfg-1',
            mode=bad_mode,
            branch='feature-x',
        )


@pytest.mark.asyncio
async def test_deploy_data_app_streamlit_silently_ignores_branch_param(
    mocker,
    mcp_context_client: Context,
    data_app: DataApp,
) -> None:
    """Streamlit apps don't carry a branch on deploy — the `branch` arg is dropped silently."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    data_app.state = 'stopped'
    data_app.type = 'streamlit'
    data_app.configuration = {'authorization': {'app_proxy': {'auth_providers': [], 'auth_rules': []}}}
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=data_app))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))
    keboola_client.storage_client.configuration_version_latest = mocker.AsyncMock(return_value=7)

    # Streamlit only allows mode='dev' to legitimately pass branch through, but on Streamlit it's a no-op.
    _ = await deploy_data_app(
        ctx=mcp_context_client,
        action='deploy',
        configuration_id='cfg-streamlit',
        mode='dev',
        branch='ignored-on-streamlit',
    )

    keboola_client.data_science_client.deploy_data_app.assert_awaited_once_with(
        data_app.data_app_id, '7', mode='dev', branch=None
    )


# ===== Tests for create_python_js_data_app_git_credential =====


@pytest.mark.asyncio
async def test_create_python_js_data_app_git_credential_happy_path(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Resolves configuration_id → data_app_id, mints an http_token credential, and embeds the
    one-time secret into a ready-to-use HTTPS clone URL."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    pyjs_app = DataApp(
        name='my-app',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-pyjs-1',
        data_app_id='app-pyjs-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'my-app'}}},
        state='running',
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=pyjs_app))
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    keboola_client.data_science_client.create_app_git_credential = mocker.AsyncMock(
        return_value=CreatedGitCredentialResponse(
            id='cred-99',
            type='http_token',
            name='',
            permissions='readWrite',
            secret='token-xyz',
        )
    )

    result = await create_python_js_data_app_git_credential(
        ctx=mcp_context_client,
        configuration_id='cfg-pyjs-1',
    )

    assert isinstance(result, CreatedGitCredentialOutput)
    assert result.response == 'created'
    assert result.configuration_id == 'cfg-pyjs-1'
    assert result.data_app_id == 'app-pyjs-1'
    assert result.credential_id == 'cred-99'
    assert result.secret == 'token-xyz'
    assert result.git_clone_url == 'https://kai:token-xyz@managed.repo/org/app.git'
    assert result.permissions == 'readWrite'

    keboola_client.data_science_client.create_app_git_credential.assert_awaited_once_with(
        data_app_id='app-pyjs-1',
    )


@pytest.mark.asyncio
async def test_create_python_js_data_app_git_credential_url_encodes_secret(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Secrets containing URL-reserved characters must be percent-encoded in `git_clone_url`."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    pyjs_app = DataApp(
        name='my-app',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-pyjs-1',
        data_app_id='app-pyjs-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'my-app'}}},
        state='running',
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=pyjs_app))
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url=None,
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    keboola_client.data_science_client.create_app_git_credential = mocker.AsyncMock(
        return_value=CreatedGitCredentialResponse(
            id='cred-99',
            type='http_token',
            name='',
            permissions='readWrite',
            secret='ab/cd:ef@gh',
        )
    )

    result = await create_python_js_data_app_git_credential(
        ctx=mcp_context_client,
        configuration_id='cfg-pyjs-1',
    )

    # Reserved characters in the secret (/, :, @) must be percent-encoded so the URL parses
    # back to the original token when git authenticates.
    assert result.git_clone_url == 'https://kai:ab%2Fcd%3Aef%40gh@managed.repo/org/app.git'


@pytest.mark.asyncio
async def test_create_python_js_data_app_git_credential_rejects_streamlit_app(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Streamlit apps have no managed git repo — must raise a clear ValueError."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    streamlit_app = DataApp(
        name='streamlit-app',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-streamlit-1',
        data_app_id='app-streamlit-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='streamlit',
        configuration={'parameters': {'dataApp': {'slug': 'streamlit-app'}}},
        state='running',
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=streamlit_app))

    with pytest.raises(ValueError, match='only supports python-js data apps'):
        await create_python_js_data_app_git_credential(
            ctx=mcp_context_client,
            configuration_id='cfg-streamlit-1',
        )

    keboola_client.data_science_client.create_app_git_credential.assert_not_called()


@pytest.mark.asyncio
async def test_create_python_js_data_app_git_credential_invalid_configuration_id(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Regression smoke test: _fetch_data_app's component_id validation still surfaces through the new tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    # Simulate a configuration_id that resolves to a non-data-app component_id, mirroring how
    # _fetch_data_app raises today.
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(
            side_effect=ValueError(
                f'Data app tools only support {DATA_APP_COMPONENT_ID} component, but the data app '
                f'"app-x" has component_id "keboola.sandboxes".'
            )
        ),
    )

    with pytest.raises(ValueError, match=f'Data app tools only support {DATA_APP_COMPONENT_ID} component'):
        await create_python_js_data_app_git_credential(
            ctx=mcp_context_client,
            configuration_id='cfg-bogus',
        )

    keboola_client.data_science_client.create_app_git_credential.assert_not_called()


# ===== Tests for get_data_apps drafts list (detail path, python-js prod) =====


from keboola_mcp_server.tools.data_apps import (  # noqa: E402
    DeletedDraftOutput,
    delete_python_js_data_app_draft,
)


def _make_python_js_prod_data_app(
    *,
    configuration_id: str = 'cfg-prod-1',
    data_app_id: str = 'app-prod-1',
    repo_url: str | None = 'https://managed.repo/org/prod.git',
    state: str = 'running',
) -> DataApp:
    """A python-js **prod** app — no `isDraft` flag, no `parentConfigurationId`."""
    return DataApp(
        name='Prod App',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id=configuration_id,
        data_app_id=data_app_id,
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'prod'}}},
        state=state,
        repo_url=repo_url,
    )


def _make_python_js_draft_data_app(
    *,
    configuration_id: str,
    data_app_id: str,
    parent_configuration_id: str,
    branch: str = 'init',
    state: str = 'created',
) -> DataApp:
    """A python-js **draft** app — `isDraft=true` and `parentConfigurationId` set."""
    return DataApp(
        name=f'Draft {configuration_id}',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id=configuration_id,
        data_app_id=data_app_id,
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration={
            'parameters': {
                'autoSuspendAfterSeconds': 900,
                'dataApp': {
                    'slug': f'draft-{configuration_id}',
                    'isDraft': True,
                    'parentConfigurationId': parent_configuration_id,
                    'git': {'repository': 'https://managed.repo/org/prod.git', 'branch': branch},
                },
            },
        },
        state=state,
        repo_url=None,
    )


def _build_storage_config_entry(*, cfg_id: str, parent_configuration_id: str | None) -> dict:
    """Mirror the shape returned by `storage_client.configuration_list` for python-js apps."""
    data_app_block: dict = {'slug': f'app-{cfg_id}'}
    if parent_configuration_id is not None:
        data_app_block['isDraft'] = True
        data_app_block['parentConfigurationId'] = parent_configuration_id
    return {
        'id': cfg_id,
        'name': f'app-{cfg_id}',
        'configuration': {
            'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': data_app_block},
        },
        'version': 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'n_drafts',
    [0, 1, 3],
    ids=['no_drafts', 'one_draft', 'three_drafts'],
)
async def test_get_data_apps_detail_for_prod_lists_drafts(
    mocker,
    mcp_context_client: Context,
    n_drafts: int,
) -> None:
    """When fetching detail for a python-js prod, the response includes a `drafts: [...]` array
    of every draft configured against it. Includes the 0-draft case to guard the empty path."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    prod_cfg_id = 'cfg-prod-1'
    prod = _make_python_js_prod_data_app(configuration_id=prod_cfg_id)
    draft_cfg_ids = [f'cfg-draft-{i}' for i in range(n_drafts)]
    drafts = {
        cfg_id: _make_python_js_draft_data_app(
            configuration_id=cfg_id,
            data_app_id=f'app-{cfg_id}',
            parent_configuration_id=prod_cfg_id,
        )
        for cfg_id in draft_cfg_ids
    }
    # Throw in a config that's parented to a DIFFERENT prod to verify we filter properly.
    foreign_cfg = _build_storage_config_entry(cfg_id='cfg-other-draft', parent_configuration_id='cfg-prod-other')
    configs = [
        _build_storage_config_entry(cfg_id=cfg_id, parent_configuration_id=prod_cfg_id) for cfg_id in draft_cfg_ids
    ]
    configs.append(foreign_cfg)
    # Also include the prod's own config (no parentConfigurationId) — must not be matched.
    configs.append(_build_storage_config_entry(cfg_id=prod_cfg_id, parent_configuration_id=None))

    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=configs)

    async def fake_fetch(client, *, configuration_id, data_app_id):
        if configuration_id == prod_cfg_id:
            return prod
        return drafts[configuration_id]

    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', side_effect=fake_fetch)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))

    result = await get_data_apps(ctx=mcp_context_client, configuration_ids=[prod_cfg_id])
    assert len(result.data_apps) == 1
    detail = result.data_apps[0]
    assert isinstance(detail, DataApp)
    returned_draft_ids = sorted(d.configuration_id for d in detail.drafts)
    assert returned_draft_ids == sorted(draft_cfg_ids)


@pytest.mark.asyncio
async def test_get_data_apps_detail_for_draft_returns_empty_drafts(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Fetching detail for a draft must not recurse — its `drafts` array stays empty and the
    cheap `configuration_list` lookup is skipped entirely."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    draft = _make_python_js_draft_data_app(
        configuration_id='cfg-draft-1', data_app_id='app-draft-1', parent_configuration_id='cfg-prod-1'
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=draft))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))
    keboola_client.storage_client.configuration_list = mocker.AsyncMock(
        side_effect=AssertionError('Drafts must not trigger a drafts lookup')
    )

    result = await get_data_apps(ctx=mcp_context_client, configuration_ids=['cfg-draft-1'])
    detail = result.data_apps[0]
    assert isinstance(detail, DataApp)
    assert detail.drafts == []
    keboola_client.storage_client.configuration_list.assert_not_called()


@pytest.mark.asyncio
async def test_get_data_apps_detail_for_streamlit_returns_empty_drafts(
    mocker,
    mcp_context_client: Context,
    data_app: DataApp,
) -> None:
    """Streamlit apps have no draft concept — the detail path must not call `configuration_list`."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    data_app.type = 'streamlit'
    data_app.configuration = {'parameters': {'dataApp': {'slug': 'sl'}}}
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=data_app))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))
    keboola_client.storage_client.configuration_list = mocker.AsyncMock(
        side_effect=AssertionError('Streamlit apps must not trigger a drafts lookup')
    )

    result = await get_data_apps(ctx=mcp_context_client, configuration_ids=['cfg-streamlit-1'])
    detail = result.data_apps[0]
    assert isinstance(detail, DataApp)
    assert detail.drafts == []
    keboola_client.storage_client.configuration_list.assert_not_called()


# ===== Tests for delete_python_js_data_app_draft =====


@pytest.mark.asyncio
async def test_delete_python_js_data_app_draft_success(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Happy path: deletes the data app via DSAPI and the Storage config (with skip_trash=False),
    and returns the parent configuration_id so the agent can pivot back."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    draft = _make_python_js_draft_data_app(
        configuration_id='cfg-draft-1', data_app_id='app-draft-1', parent_configuration_id='cfg-prod-1'
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=draft))
    keboola_client.data_science_client.delete_data_app = mocker.AsyncMock(return_value=None)
    keboola_client.storage_client.configuration_delete = mocker.AsyncMock(return_value=None)

    result = await delete_python_js_data_app_draft(ctx=mcp_context_client, configuration_id='cfg-draft-1')

    assert isinstance(result, DeletedDraftOutput)
    assert result.response == 'deleted'
    assert result.configuration_id == 'cfg-draft-1'
    assert result.data_app_id == 'app-draft-1'
    assert result.parent_configuration_id == 'cfg-prod-1'
    keboola_client.data_science_client.delete_data_app.assert_awaited_once_with('app-draft-1')
    keboola_client.storage_client.configuration_delete.assert_awaited_once_with(
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-draft-1',
        skip_trash=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('configuration', 'error_match'),
    [
        (
            {'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'prod'}}},
            'is a python-js .*prod.* app, not a draft',
        ),
        (
            {'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'prod', 'isDraft': False}}},
            'is a python-js .*prod.* app, not a draft',
        ),
    ],
    ids=['no_isDraft_key', 'isDraft_false'],
)
async def test_delete_python_js_data_app_draft_refuses_prod(
    mocker,
    mcp_context_client: Context,
    configuration: dict,
    error_match: str,
) -> None:
    """Refusing to delete prod apps is the single safety check — both shapes (missing flag or
    explicit `false`) must be rejected, and neither delete endpoint must be called."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    prod = DataApp(
        name='Prod',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-prod-1',
        data_app_id='app-prod-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration=configuration,
        state='running',
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=prod))
    keboola_client.data_science_client.delete_data_app = mocker.AsyncMock()
    keboola_client.storage_client.configuration_delete = mocker.AsyncMock()

    with pytest.raises(ValueError, match=error_match):
        await delete_python_js_data_app_draft(ctx=mcp_context_client, configuration_id='cfg-prod-1')

    keboola_client.data_science_client.delete_data_app.assert_not_called()
    keboola_client.storage_client.configuration_delete.assert_not_called()


@pytest.mark.asyncio
async def test_delete_python_js_data_app_draft_refuses_streamlit(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Streamlit apps have no draft concept — the tool must refuse and never call delete."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    streamlit_app = DataApp(
        name='SL',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-sl-1',
        data_app_id='app-sl-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='streamlit',
        configuration={'parameters': {'dataApp': {'slug': 'sl'}}},
        state='running',
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=streamlit_app))
    keboola_client.data_science_client.delete_data_app = mocker.AsyncMock()
    keboola_client.storage_client.configuration_delete = mocker.AsyncMock()

    with pytest.raises(ValueError, match='only supports python-js data apps'):
        await delete_python_js_data_app_draft(ctx=mcp_context_client, configuration_id='cfg-sl-1')

    keboola_client.data_science_client.delete_data_app.assert_not_called()
    keboola_client.storage_client.configuration_delete.assert_not_called()
