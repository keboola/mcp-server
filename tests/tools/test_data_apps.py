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
    _CONTINUOUS_PULL_BASE_REPO,
    _DEFAULT_CODE_APP_AUTO_SUSPEND_SECONDS,
    _DEFAULT_CODE_APP_SIZE,
    _DEFAULT_PULL_PERIOD_SECONDS,
    _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE,
    _STORAGE_QUERY_DATA_FUNCTION_CODE,
    MAX_DNS_LABEL_LENGTH,
    DataApp,
    DataAppSlugTooLongError,
    DataAppSummary,
    ModifiedDataAppOutput,
    _build_code_data_app_config,
    _build_data_app_config,
    _fetch_data_app,
    _get_authorization,
    _get_data_app_slug,
    _get_query_function_code,
    _get_secrets,
    _inject_query_to_source_code,
    _update_existing_code_data_app_config,
    _update_existing_data_app_config,
    _uses_basic_authentication,
    deploy_data_app,
    get_data_apps,
    modify_code_data_app,
    modify_data_app,
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


# =============================================================================
# FOLDER METADATA TESTS
# =============================================================================


@pytest.mark.parametrize(
    ('configuration_id', 'folder', 'app_count', 'app_folders', 'expect_folder_metadata', 'expect_hint'),
    [
        # Create path (no configuration_id)
        ('', 'Analytics', 0, [], True, False),
        ('', '  Analytics  ', 0, [], True, False),  # whitespace stripped
        ('', '', 5, [], False, False),
        ('', '', 25, ['Analytics'], False, True),
        # Update path (with configuration_id)
        ('cfg-1', 'Analytics', 0, [], True, False),
        ('cfg-1', '', 5, [], False, False),
        ('cfg-1', '', 25, ['Analytics'], False, True),
    ],
    ids=[
        'create_folder_provided',
        'create_folder_whitespace_stripped',
        'create_no_folder_few',
        'create_no_folder_many_with_hint',
        'update_folder_provided',
        'update_no_folder_few',
        'update_no_folder_many_with_hint',
    ],
)
@pytest.mark.asyncio
async def test_modify_data_app_folder(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    configuration_id: str,
    folder: str,
    app_count: int,
    app_folders: list[str],
    expect_folder_metadata: bool,
    expect_hint: bool,
) -> None:
    """Test folder metadata and change_summary hint for modify_data_app (create and update paths)."""
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
            'keboola_mcp_server.tools.data_apps.modify_data_app_internal',
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
        'keboola_mcp_server.tools.data_apps.get_config_folders',
        mocker.AsyncMock(return_value=(app_count, app_folders)),
    )

    result = await modify_data_app(
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
    if expect_hint:
        assert result.change_summary is not None
        assert str(app_count) in result.change_summary
    else:
        assert result.change_summary is None


# =============================================================================
# PYTHON-JS (CODE DATA APP) TESTS
# =============================================================================


class TestBuildCodeDataAppConfig:
    """Tests for _build_code_data_app_config helper."""

    def test_builds_watching_mode_config(self):
        secrets = {'WORKSPACE_ID': '123', 'BRANCH_ID': 'default'}
        config = _build_code_data_app_config(
            name='My Code App',
            watched_repo_url='https://github.com/user/repo.git',
            watched_repo_branch='main',
            secrets=secrets,
        )

        params = config['parameters']
        assert params['autoSuspendAfterSeconds'] == _DEFAULT_CODE_APP_AUTO_SUSPEND_SECONDS

        data_app = params['dataApp']
        assert data_app['slug'] == 'my-code-app'
        assert data_app['type'] == 'python-js'
        assert data_app['git'] == {'repository': _CONTINUOUS_PULL_BASE_REPO}
        assert data_app['watchedRepo'] == {
            'pullPeriod': _DEFAULT_PULL_PERIOD_SECONDS,
            'url': 'https://github.com/user/repo.git',
            'branch': 'main',
            'autoReSetup': True,
        }
        assert data_app['secrets'] == secrets

        # No script, no packages
        assert 'script' not in params
        assert 'packages' not in params

        # Authorization: no-auth
        assert config['authorization'] == _get_authorization(False)

        # Runtime backend size
        assert config['runtime'] == {'backend': {'size': _DEFAULT_CODE_APP_SIZE}}

    def test_uses_default_slug_when_name_produces_empty(self):
        config = _build_code_data_app_config(
            name='!!!',
            watched_repo_url='https://github.com/user/repo.git',
            watched_repo_branch='dev',
            secrets={'WORKSPACE_ID': '1', 'BRANCH_ID': 'default'},
        )
        assert config['parameters']['dataApp']['slug'] == 'Data-App'


class TestUpdateExistingCodeDataAppConfig:
    """Tests for _update_existing_code_data_app_config helper."""

    @pytest.fixture
    def existing_code_config(self) -> dict:
        return {
            'parameters': {
                'autoSuspendAfterSeconds': 900,
                'dataApp': {
                    'slug': 'old-slug',
                    'type': 'python-js',
                    'git': {'repository': _CONTINUOUS_PULL_BASE_REPO},
                    'watchedRepo': {
                        'pullPeriod': 1,
                        'url': 'https://github.com/user/old-repo.git',
                        'branch': 'old-branch',
                        'autoReSetup': True,
                    },
                },
            },
            'authorization': _get_authorization(False),
            'runtime': {'backend': {'size': 'tiny'}},
        }

    def test_update_watched_repo(self, existing_code_config):
        secrets = {'WORKSPACE_ID': '123', 'BRANCH_ID': 'default'}
        updated = _update_existing_code_data_app_config(
            existing_code_config,
            name='New Name',
            watched_repo_url='https://github.com/user/new-repo.git',
            watched_repo_branch='feature',
            finalize=False,
            secrets=secrets,
        )

        data_app = updated['parameters']['dataApp']
        assert data_app['slug'] == 'new-name'
        # git.repository unchanged — still points at base image
        assert data_app['git'] == {'repository': _CONTINUOUS_PULL_BASE_REPO}
        assert data_app['watchedRepo'] == {
            'pullPeriod': _DEFAULT_PULL_PERIOD_SECONDS,
            'url': 'https://github.com/user/new-repo.git',
            'branch': 'feature',
            'autoReSetup': True,
        }
        assert data_app['secrets'] == secrets
        # rest of config untouched
        assert updated['authorization'] == existing_code_config['authorization']
        assert updated['runtime'] == existing_code_config['runtime']

    def test_finalize_replaces_git_and_removes_watched_repo(self, existing_code_config):
        secrets = {'WORKSPACE_ID': '456', 'BRANCH_ID': 'main'}
        updated = _update_existing_code_data_app_config(
            existing_code_config,
            name='',
            watched_repo_url='https://github.com/user/final-repo.git',
            watched_repo_branch='main',
            finalize=True,
            secrets=secrets,
        )

        data_app = updated['parameters']['dataApp']
        # git.repository now points to user's repo
        assert data_app['git'] == {'repository': 'https://github.com/user/final-repo.git'}
        # watchedRepo completely removed
        assert 'watchedRepo' not in data_app
        # slug preserved when name is empty
        assert data_app['slug'] == 'old-slug'
        assert data_app['secrets'] == secrets

    def test_does_not_mutate_original(self, existing_code_config):
        import copy

        original = copy.deepcopy(existing_code_config)
        _update_existing_code_data_app_config(
            existing_code_config,
            name='Changed',
            watched_repo_url='https://github.com/user/x.git',
            watched_repo_branch='y',
            finalize=True,
            secrets={'WORKSPACE_ID': '1', 'BRANCH_ID': 'default'},
        )
        assert existing_code_config == original


class TestModifyCodeDataApp:
    """Tests for the modify_code_data_app MCP tool."""

    @pytest.mark.asyncio
    async def test_create_python_js_data_app(self, mocker, mcp_context_client: Context) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        keboola_client.storage_client.project_id = mocker.AsyncMock(return_value='proj-1')
        keboola_client.storage_client.configuration_version_latest = mocker.AsyncMock(return_value=1)
        keboola_client.encryption_client = mocker.AsyncMock()
        keboola_client.encryption_client.encrypt = mocker.AsyncMock(side_effect=lambda config, **kw: config)

        data_app_response = DataAppResponse(
            id='app-new',
            project_id='proj-1',
            component_id=DATA_APP_COMPONENT_ID,
            branch_id='default',
            config_id='cfg-new',
            config_version='1',
            type='python-js',
            state='created',
            desired_state='created',
        )
        deployed_data_app = DataApp(
            name='Test Code App',
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id='cfg-new',
            data_app_id='app-new',
            project_id='proj-1',
            branch_id='default',
            config_version='1',
            type='python-js',
            state='running',
            deployment_url='https://test-code-app.app.keboola.com',
            configuration={},
        )
        keboola_client.data_science_client = mocker.AsyncMock()
        keboola_client.data_science_client.create_data_app = mocker.AsyncMock(return_value=data_app_response)
        keboola_client.data_science_client.deploy_data_app = mocker.AsyncMock(return_value=data_app_response)

        mocker.patch(
            'keboola_mcp_server.tools.data_apps._fetch_data_app',
            return_value=deployed_data_app,
        )
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.ProjectLinksManager.from_client',
            return_value=mocker.AsyncMock(
                get_data_app_links=mocker.MagicMock(return_value=[]),
            ),
        )
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.get_config_folders',
            mocker.AsyncMock(return_value=(0, [])),
        )

        result = await modify_code_data_app(
            ctx=mcp_context_client,
            name='Test Code App',
            description='A test python-js app',
            watched_repo_url='https://github.com/user/repo.git',
            watched_repo_branch='main',
        )

        assert isinstance(result, ModifiedDataAppOutput)
        assert result.response == 'created'
        assert result.data_app.type == 'python-js'
        assert result.data_app.deployment_url == 'https://test-code-app.app.keboola.com'

        # Verify create_data_app was called with type='python-js'
        create_call = keboola_client.data_science_client.create_data_app
        create_call.assert_awaited_once()
        call_kwargs = create_call.call_args
        assert call_kwargs.kwargs.get('type') == 'python-js'

        # Verify deploy was called after creation
        keboola_client.data_science_client.deploy_data_app.assert_awaited_once_with('app-new', '1')

    @pytest.mark.asyncio
    async def test_finalize_requires_configuration_id(self, mcp_context_client: Context) -> None:
        with pytest.raises(ValueError, match='finalize=True.*requires.*configuration_id'):
            await modify_code_data_app(
                ctx=mcp_context_client,
                name='Test',
                description='desc',
                watched_repo_url='https://github.com/user/repo.git',
                watched_repo_branch='main',
                finalize=True,
                configuration_id='',
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('finalize', 'app_state', 'expected_response'),
        [
            (False, 'stopped', 'updated'),
            (False, 'running', 'updated'),
            (True, 'stopped', 'finalized'),
            (True, 'running', 'finalized'),
        ],
    )
    async def test_update_and_finalize_responses(
        self,
        mocker,
        mcp_context_client: Context,
        finalize: bool,
        app_state: str,
        expected_response: str,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        keboola_client.storage_client.project_id = mocker.AsyncMock(return_value='proj-1')
        keboola_client.storage_client.configuration_version_latest = mocker.AsyncMock(return_value=3)
        keboola_client.encryption_client = mocker.AsyncMock()
        keboola_client.encryption_client.encrypt = mocker.AsyncMock(side_effect=lambda config, **kw: config)
        keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
        keboola_client.data_science_client = mocker.AsyncMock()
        keboola_client.data_science_client.deploy_data_app = mocker.AsyncMock()

        existing_data_app = DataApp(
            name='Existing App',
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id='cfg-1',
            data_app_id='app-1',
            project_id='proj-1',
            branch_id='default',
            config_version='2',
            type='python-js',
            auto_suspend_after_seconds=900,
            configuration={
                'parameters': {
                    'autoSuspendAfterSeconds': 900,
                    'dataApp': {
                        'slug': 'existing',
                        'type': 'python-js',
                        'git': {'repository': _CONTINUOUS_PULL_BASE_REPO},
                        'watchedRepo': {
                            'pullPeriod': 1,
                            'url': 'https://github.com/user/repo.git',
                            'branch': 'main',
                            'autoReSetup': True,
                        },
                    },
                },
                'authorization': _get_authorization(False),
                'runtime': {'backend': {'size': 'tiny'}},
            },
            state=app_state,
        )
        mocker.patch(
            'keboola_mcp_server.tools.data_apps._fetch_data_app',
            return_value=existing_data_app,
        )
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.ProjectLinksManager.from_client',
            return_value=mocker.AsyncMock(
                get_data_app_links=mocker.MagicMock(return_value=[]),
            ),
        )
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.get_config_folders',
            mocker.AsyncMock(return_value=(0, [])),
        )

        result = await modify_code_data_app(
            ctx=mcp_context_client,
            name='Existing App',
            description='desc',
            watched_repo_url='https://github.com/user/repo.git',
            watched_repo_branch='main',
            finalize=finalize,
            configuration_id='cfg-1',
        )

        assert isinstance(result, ModifiedDataAppOutput)
        assert result.response == expected_response
