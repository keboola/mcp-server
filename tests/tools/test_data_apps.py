import re
import sys
from types import ModuleType
from typing import Literal, cast

import pytest
from fastmcp import Context

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.data_science import AppRunResponse, DataAppConfig, DataAppResponse, RuntimeResponse
from keboola_mcp_server.clients.storage import ConfigurationAPIResponse
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.data_apps import (
    _APP_RUN_LOG_LINES,
    _APP_RUN_MESSAGE_LIMIT,
    _QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE,
    _STORAGE_QUERY_DATA_FUNCTION_CODE,
    MAX_DNS_LABEL_LENGTH,
    AppRunInfo,
    DataApp,
    DataAppSlugTooLongError,
    DataAppSummary,
    ModifiedDataAppOutput,
    RuntimeImage,
    _build_data_app_config,
    _fetch_data_app,
    _fetch_latest_run,
    _get_authorization,
    _get_data_app_slug,
    _get_query_function_code,
    _get_secrets,
    _inject_query_to_source_code,
    _prune_empty_storage_objects,
    _resolve_app_image,
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


@pytest.mark.parametrize(
    ('block', 'expected'),
    [
        # An empty storage block collapses to empty so the caller omits the `storage` key.
        ({}, {}),
        # Empty `input`/`output` objects are dropped — PHP would serialize them as `[]` and the
        # mapping editor (Writable Tables) then silently fails to add entries (AI-3135).
        ({'input': {}, 'output': {}}, {}),
        # The canonical empty state uses empty *arrays*, which must be preserved verbatim.
        ({'output': {'tables': []}}, {'output': {'tables': []}}),
        # Mixed: drop the empty `input`, keep the populated `output`.
        (
            {
                'input': {},
                'output': {'tables': [{'destination': 'in.c-main.t', 'unload_strategy': 'direct-grant'}]},
            },
            {'output': {'tables': [{'destination': 'in.c-main.t', 'unload_strategy': 'direct-grant'}]}},
        ),
        # Nested empty objects inside a table entry are pruned; scalars and arrays survive.
        (
            {'output': {'tables': [{'destination': 'in.c-main.t', 'table_metadata': {}}]}},
            {'output': {'tables': [{'destination': 'in.c-main.t'}]}},
        ),
    ],
)
def test_prune_empty_storage_objects(block, expected) -> None:
    """Empty objects are stripped (they collapse to `[]` server-side); empty arrays are kept."""
    assert _prune_empty_storage_objects(block) == expected


def test_build_data_app_config_create_omits_empty_storage() -> None:
    """Streamlit create must not persist an empty `storage` object (it becomes `[]` server-side, AI-3135)."""
    config = _build_data_app_config('My App', "print('hi')", [], 'no-auth', {}, 'snowflake')
    serialized = DataAppConfig.model_validate(config).model_dump(by_alias=True, exclude_none=True)
    assert 'storage' not in serialized


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


@pytest.mark.parametrize(
    ('existing_storage', 'storage_key_present', 'expected_storage'),
    [
        # Leftover empty object (serialized as `[]` server-side) is removed on re-save (AI-3135).
        ({}, False, None),
        # Leftover array-shaped storage is removed.
        ([], False, None),
        # Empty `input`/`output` containers are pruned away to nothing -> key removed.
        ({'input': {}, 'output': {}}, False, None),
        # A valid storage block is preserved untouched.
        (
            {'output': {'tables': [{'destination': 'in.c-main.t', 'unload_strategy': 'direct-grant'}]}},
            True,
            {'output': {'tables': [{'destination': 'in.c-main.t', 'unload_strategy': 'direct-grant'}]}},
        ),
    ],
)
def test_update_existing_data_app_config_normalizes_storage(
    existing_storage, storage_key_present, expected_storage
) -> None:
    """Streamlit re-save repairs a broken/empty leftover `storage` shape instead of preserving it."""
    existing = {
        'parameters': {'dataApp': {'slug': 'x', 'secrets': {}}, 'script': ['old'], 'packages': []},
        'storage': existing_storage,
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
    assert ('storage' in new) is storage_key_present
    if storage_key_present:
        assert new['storage'] == expected_storage


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

    workspace_manager.get_data_app_workspace_id = mocker.AsyncMock(return_value=1)
    workspace_manager.get_data_app_sql_dialect = mocker.AsyncMock(return_value='snowflake')
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='default')

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


@pytest.mark.parametrize(
    ('configuration_id', 'fail_at', 'state', 'expected_response'),
    [
        # Update: failure at the re-fetch step, running app keeps the "redeploy required" hint.
        ('cfg-1', '_fetch_data_app', 'running', 'updated (redeploy required to apply changes in the running app)'),
        # Update: failure at the FIRST post-write step (metadata) on a stopped app -> still partial, no redeploy hint.
        ('cfg-1', 'set_cfg_update_metadata', 'stopped', 'updated'),
        # Create: failure at a post-write step.
        ('', 'set_cfg_creation_metadata', None, 'created'),
    ],
    ids=['update_refetch_running', 'update_metadata_stopped', 'create_metadata'],
)
@pytest.mark.asyncio
async def test_modify_streamlit_data_app_partial_success_when_response_building_fails(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    configuration_id: str,
    fail_at: str,
    state: str | None,
    expected_response: str,
) -> None:
    """Regression for AJDA-2852: when the API write commits but a post-write response-building step raises,
    the tool must return a truthful partial success (not a ToolError) so the agent does not retry and
    double-apply the change. Parametrized over which post-write step fails to prove the partial path is reached
    regardless of failure point, and that the response wording still reflects the app state."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

    workspace_manager.get_data_app_workspace_id = mocker.AsyncMock(return_value=1)
    workspace_manager.get_data_app_sql_dialect = mocker.AsyncMock(return_value='snowflake')
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='default')
    keboola_client.storage_client.project_id = mocker.AsyncMock(return_value='proj-1')

    encrypted_config = {
        'parameters': {'script': ['SELECT 1']},
        'storage': {},
        'authorization': {'app_proxy': {'auth_providers': [], 'auth_rules': []}},
    }
    keboola_client.encryption_client = mocker.AsyncMock()
    keboola_client.encryption_client.encrypt = mocker.AsyncMock(return_value=encrypted_config)

    boom = RuntimeError('boom while building response')

    if configuration_id:
        existing_data_app = DataApp(
            name='My App',
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id=configuration_id,
            data_app_id='app-1',
            project_id='proj-1',
            branch_id='default',
            config_version='27',
            type='streamlit',
            auto_suspend_after_seconds=900,
            configuration=encrypted_config,
            state=state,
        )
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.modify_streamlit_data_app_internal',
            mocker.AsyncMock(return_value=(existing_data_app, encrypted_config, None)),
        )
        # The write commits and returns the new version...
        keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={'version': 28})
        # ...but a post-write enrichment step blows up.
        mocker.patch(f'keboola_mcp_server.tools.data_apps.{fail_at}', side_effect=boom)
        committing_mock = keboola_client.storage_client.configuration_update
    else:
        mocker.patch(
            'keboola_mcp_server.tools.data_apps.DataAppConfig.model_validate',
            return_value=mocker.MagicMock(authorization={'app_proxy': {'auth_providers': [], 'auth_rules': []}}),
        )
        keboola_client.data_science_client = mocker.AsyncMock()
        keboola_client.data_science_client.create_data_app = mocker.AsyncMock(
            return_value=_make_data_app_response(config_id='new-cfg-1')
        )
        # The app is created, but a post-write enrichment step blows up.
        mocker.patch(f'keboola_mcp_server.tools.data_apps.{fail_at}', side_effect=boom)
        committing_mock = keboola_client.data_science_client.create_data_app

    # Must NOT raise (no ToolError) even though a post-write step failed.
    result = await modify_streamlit_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        source_code='import streamlit as st\n{QUERY_DATA_FUNCTION}\nst.write("hello")',
        packages=['pandas'],
        authentication_type='no-auth',
        configuration_id=configuration_id,
        change_description='test',
    )

    assert isinstance(result, ModifiedDataAppOutput)
    # The committing write happened exactly once and is never retried within the call.
    committing_mock.assert_awaited_once()
    assert result.change_summary is not None
    # The response truthfully reports the change landed and warns against retrying.
    assert 'do not retry' in result.change_summary.lower()
    # Response wording mirrors the success path (redeploy hint only for running/starting apps).
    assert result.response == expected_response
    if configuration_id:
        assert 'WAS updated' in result.change_summary
        # New version surfaced from the write response despite the failed re-fetch.
        assert '28' in result.change_summary
        assert result.data_app.config_version == '28'
    else:
        assert 'WAS created' in result.change_summary


@pytest.mark.asyncio
async def test_partial_output_helpers_never_raise_when_summary_construction_fails(mocker) -> None:
    """The partial-output helpers promise 'MUST NOT raise' even if the primary DataAppSummary construction
    fails -- they must fall back to a summary built from primitives instead of letting a committed write
    surface as a ToolError (AJDA-2852)."""
    from keboola_mcp_server.tools.data_apps import _partial_create_output, _partial_update_output

    links_manager = mocker.MagicMock()
    links_manager.get_data_app_links.return_value = []

    # --- update helper: force the primary model_validate to raise ---
    data_app_pre = DataApp(
        name='My App',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='default',
        config_version='27',
        type='streamlit',
        configuration={'authorization': {}},
        state='running',
    )
    mocker.patch.object(DataAppSummary, 'model_validate', side_effect=RuntimeError('validate boom'))
    update_out = _partial_update_output(
        data_app_pre=data_app_pre,
        links_manager=links_manager,
        configuration_id='cfg-1',
        name='My App',
        new_version='28',
    )
    assert isinstance(update_out, ModifiedDataAppOutput)
    assert update_out.data_app.configuration_id == 'cfg-1'
    assert update_out.data_app.data_app_id == 'app-1'
    assert update_out.data_app.config_version == '28'  # falls back to the new version from the write response
    assert update_out.response == 'updated (redeploy required to apply changes in the running app)'

    # --- create helper: force the primary from_api_response to raise ---
    data_app_resp = _make_data_app_response(config_id='new-cfg-1', data_app_id='app-2')
    mocker.patch.object(DataAppSummary, 'from_api_response', side_effect=RuntimeError('from_api boom'))
    create_out = _partial_create_output(
        data_app_resp=data_app_resp,
        links_manager=links_manager,
        validated_config=mocker.MagicMock(authorization={}),
        name='My App',
    )
    assert isinstance(create_out, ModifiedDataAppOutput)
    assert create_out.data_app.configuration_id == 'new-cfg-1'
    assert create_out.data_app.data_app_id == 'app-2'
    assert 'WAS created' in (create_out.change_summary or '')


@pytest.mark.asyncio
async def test_modify_streamlit_data_app_update_skips_metadata_when_version_missing(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """When the update response carries no numeric version, set_cfg_update_metadata must be SKIPPED rather than
    stamped with the (now-stale) pre-update version, which would record a misleading UPDATED_BY_MCP version
    (review hardening on AJDA-2852). The tool still returns a normal success."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

    workspace_manager.get_data_app_workspace_id = mocker.AsyncMock(return_value=1)
    workspace_manager.get_data_app_sql_dialect = mocker.AsyncMock(return_value='snowflake')
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='default')
    keboola_client.storage_client.project_id = mocker.AsyncMock(return_value='proj-1')

    encrypted_config = {
        'parameters': {'script': ['SELECT 1']},
        'storage': {},
        'authorization': {'app_proxy': {'auth_providers': [], 'auth_rules': []}},
    }
    keboola_client.encryption_client = mocker.AsyncMock()
    keboola_client.encryption_client.encrypt = mocker.AsyncMock(return_value=encrypted_config)

    existing_data_app = DataApp(
        name='My App',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='default',
        config_version='27',
        type='streamlit',
        auto_suspend_after_seconds=900,
        configuration=encrypted_config,
        state='stopped',
    )
    mocker.patch(
        'keboola_mcp_server.tools.data_apps.modify_streamlit_data_app_internal',
        mocker.AsyncMock(return_value=(existing_data_app, encrypted_config, None)),
    )
    # Committing write succeeds but the response carries no version.
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    # Let the rest of the (best-effort) response building succeed.
    set_meta = mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_update_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=existing_data_app))

    result = await modify_streamlit_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        source_code='import streamlit as st\n{QUERY_DATA_FUNCTION}\nst.write("hello")',
        packages=['pandas'],
        authentication_type='no-auth',
        configuration_id='cfg-1',
        change_description='test',
    )

    assert isinstance(result, ModifiedDataAppOutput)
    assert result.response == 'updated'
    # The misleading pre-update-version stamp must NOT happen when the new version is unknown.
    set_meta.assert_not_awaited()


# ===== Tests for modify_python_js_data_app =====


from keboola_mcp_server.clients.data_science import (  # noqa: E402
    AppGitRepoResponse,
    CreatedGitCredentialResponse,
)
from keboola_mcp_server.tools.data_apps import (  # noqa: E402
    CreatedGitCredentialOutput,
    ModifiedPythonJsDataAppOutput,
    _is_draft_config,
    _update_existing_code_data_app_config,
    _validate_branch_update,
    create_python_js_data_app_git_credential,
    modify_python_js_data_app,
)

_NODE_20 = '1.7.2_python-3.11_node-20'
_NODE_24 = '1.7.2_python-3.13_node-24'
_RUNTIMES = [
    # The live catalog lists the default tag twice; the default entry is the one reported.
    RuntimeResponse(
        type='python-js', description='Python 3.11 + Node.js 20 (pinned)', is_type_default=False, image_tag=_NODE_20
    ),
    RuntimeResponse(type='python-js', description='Python 3.11 + Node.js 20', is_type_default=True, image_tag=_NODE_20),
    RuntimeResponse(
        type='python-js', description='Python 3.13 + Node.js 24', is_type_default=False, image_tag=_NODE_24
    ),
    RuntimeResponse(type='streamlit', description='Streamlit', is_type_default=True, image_tag='streamlit-1.0'),
]
_PYTHON_JS_IMAGES = [
    RuntimeImage(version=_NODE_20, description='Python 3.11 + Node.js 20', is_default=True),
    RuntimeImage(version=_NODE_24, description='Python 3.13 + Node.js 24'),
]


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
    ('name', 'slug_kwargs', 'expected_slug', 'expected_error'),
    [
        # Explicit slug is honored verbatim (unchanged behavior).
        ('My App', {'slug': 'custom-slug'}, 'custom-slug', None),
        # Omitted slug is auto-derived from `name` (AI-3634) — clean base on the prod path.
        ('My App', {}, 'my-app', None),
        # Empty-string slug is treated as omitted and derived.
        ('My App', {'slug': ''}, 'my-app', None),
        # Special characters collapse to single hyphens and leading/trailing hyphens are stripped.
        ('  Sales & Revenue!! ', {}, 'sales-revenue', None),
        # A name that slugifies to nothing falls back to a sensible default slug.
        ('!!!', {}, 'data-app', None),
        # A long (~60-char) name is capped at MAX_DATA_APP_SLUG_LENGTH (50) so the derived slug
        # stays within the data-app URL-prefix limit enforced by the UI (AI-3634).
        ('a' * 60, {}, 'a' * 50, None),
        # An explicit slug at the DNS-label max (63 chars) is accepted and written verbatim (AI-3634).
        ('My App', {'slug': 'a' * 63}, 'a' * 63, None),
        # An explicit slug over 63 chars is rejected with a clear length error (AI-3634); the message
        # also flags the tighter 50-char UI URL-prefix limit.
        ('My App', {'slug': 'a' * 64}, None, 'slug must be at most 63 characters'),
    ],
)
async def test_modify_python_js_data_app_create_prod_derives_or_honors_slug(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    name: str,
    slug_kwargs: dict,
    expected_slug: str,
    expected_error: str | None,
) -> None:
    """Prod create path (no `parent_configuration_id`): an explicit slug is written verbatim (when it
    is at most 63 chars, else rejected), and an omitted/empty slug is auto-derived from `name` as a
    clean DNS-label-safe slug (AI-3634)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(
        return_value=_make_python_js_data_app_response()
    )
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(
            ssh_url='git@managed.repo:org/app.git',
            https_url='https://managed.repo/org/app.git',
            is_managed_git_repo=True,
        )
    )
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_creation_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    if expected_error is not None:
        with pytest.raises(ValueError, match=expected_error):
            await modify_python_js_data_app(ctx=mcp_context_client, name=name, description='desc', **slug_kwargs)
        keboola_client.data_science_client.create_data_app.assert_not_awaited()
        return

    await modify_python_js_data_app(ctx=mcp_context_client, name=name, description='desc', **slug_kwargs)

    create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
    serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
    assert serialized['parameters']['dataApp']['slug'] == expected_slug


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('stored_name', 'stored_slug', 'is_draft', 'state', 'desired_state', 'kwargs', 'expected_slug'),
    [
        # Never deployed, generic slug from the UI → the slug follows the new name.
        ('New App', 'new-app', False, 'created', 'stopped', {'name': 'Sales Dashboard'}, 'sales-dashboard'),
        # Never deployed and already renamed once → it keeps following.
        ('Sales Dashboard', 'sales-dashboard', False, 'created', None, {'name': 'Revenue'}, 'revenue'),
        # A UI-derived slug that kept repeated hyphens still counts as following the name.
        ('Sales & Ops', 'sales---ops', False, 'created', 'stopped', {'name': 'Ops Board'}, 'ops-board'),
        # First deploy in progress → the URL is being served, the slug stays.
        ('New App', 'new-app', False, 'created', 'running', {'name': 'Sales'}, 'new-app'),
        # Deployed before → a rename keeps the slug.
        ('New App', 'new-app', False, 'stopped', 'stopped', {'name': 'Sales'}, 'new-app'),
        # A custom slug that never followed the name → a rename keeps it.
        ('Sales Dashboard', 'sales', False, 'created', 'stopped', {'name': 'Revenue'}, 'sales'),
        # Drafts never change their slug on rename.
        ('New App', 'new-app-draft-a1b2c3', True, 'created', 'stopped', {'name': 'Sales'}, 'new-app-draft-a1b2c3'),
        # Same name → nothing to follow.
        ('New App', 'new-app', False, 'created', 'stopped', {'name': 'New App'}, 'new-app'),
        # An explicit slug on a deployed prod app is written.
        ('Sales', 'sales', False, 'running', 'running', {'name': 'Sales', 'slug': 'kpis'}, 'kpis'),
    ],
)
async def test_modify_python_js_data_app_update_slug(
    mocker,
    mcp_context_client: Context,
    stored_name: str,
    stored_slug: str,
    is_draft: bool,
    state: str,
    desired_state: str | None,
    kwargs: dict,
    expected_slug: str,
) -> None:
    """Update path: a rename moves a name-following slug until first deploy; then only an explicit slug."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    data_app_block: dict = {'slug': stored_slug}
    if is_draft:
        data_app_block |= {'isDraft': True, 'parentConfigurationId': 'cfg-prod'}
    existing = DataApp(
        name=stored_name,
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='2',
        type='python-js',
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': data_app_block}},
        state=state,
        desired_state=desired_state,
    )
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(side_effect=[existing, existing.model_copy(update={'config_version': '3'})]),
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_update_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    result = await modify_python_js_data_app(ctx=mcp_context_client, description='', configuration_id='cfg-1', **kwargs)

    new_cfg = keboola_client.storage_client.configuration_update.await_args.kwargs['configuration']
    assert new_cfg['parameters']['dataApp']['slug'] == expected_slug
    if expected_slug == stored_slug:
        assert 'slug' not in (result.change_summary or '')
    else:
        assert f"'{expected_slug}'" in result.change_summary
        # The explicit-slug row is a deployed app; every other change is a rename of a never-deployed one.
        expected_phrase = 'old URL stops working' if 'slug' in kwargs else 'Nothing to do or undo'
        assert expected_phrase in result.change_summary


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('slug', 'is_draft', 'error_match'),
    [
        ('kpis', True, 'slug cannot be changed on a draft'),
        ('Not A Label', False, 'only lowercase letters, digits and hyphens'),
        ('-kpis', False, 'must not start or end with a hyphen'),
        ('a' * 64, False, 'at most 63 characters'),
    ],
)
async def test_modify_python_js_data_app_update_rejects_invalid_slug(
    mocker,
    mcp_context_client: Context,
    slug: str,
    is_draft: bool,
    error_match: str,
) -> None:
    """Update path rejects an explicit slug on a draft and any slug that is not a DNS label."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    existing = (
        _make_python_js_draft_data_app(configuration_id='cfg-1', data_app_id='app-1', parent_configuration_id='p')
        if is_draft
        else _make_python_js_prod_data_app(configuration_id='cfg-1')
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=existing))

    with pytest.raises(ValueError, match=error_match):
        await modify_python_js_data_app(
            ctx=mcp_context_client, name=existing.name, description='', configuration_id='cfg-1', slug=slug
        )
    keboola_client.storage_client.configuration_update.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(('auto_suspend_after_seconds', 'expected_auto_suspend'), [(300, 300), (None, 900)])
async def test_modify_python_js_data_app_create_calls_full_provisioning_chain(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    auto_suspend_after_seconds: int | None,
    expected_auto_suspend: int,
) -> None:
    """Create path: POST /apps with type=python-js + useManagedGitRepo, fetch repo URL. Git
    credential creation is now a separate tool — not exercised here."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)

    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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
        auto_suspend_after_seconds=auto_suspend_after_seconds,
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
    assert serialized['parameters']['autoSuspendAfterSeconds'] == expected_auto_suspend
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

    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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
@pytest.mark.parametrize(
    ('configuration_id', 'parent_configuration_id', 'target_is_draft', 'expect_rejected'),
    [
        # Create path: parent_configuration_id set → the new app is a draft; 'no-auth' is
        # rejected by the top validation block before any client call.
        ('', 'cfg-prod-1', False, True),
        # Update path: the target config is a draft → rejected after _fetch_data_app, before
        # configuration_update.
        ('cfg-draft-1', None, True, True),
        # Update path on a prod app: 'no-auth' stays allowed — public prod apps are legitimate.
        ('cfg-prod-1', None, False, False),
    ],
    ids=['create_draft_rejected', 'update_draft_rejected', 'update_prod_allowed'],
)
async def test_modify_python_js_data_app_no_auth_rejected_only_on_drafts(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    configuration_id: str,
    parent_configuration_id: str | None,
    target_is_draft: bool,
    expect_rejected: bool,
) -> None:
    """'no-auth' is only forbidden on drafts: a draft inherits the prod app's data access, so a
    public draft would expose Storage-reading/writing endpoints. Prod apps may still be public."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

    data_app_block: dict = {'slug': 'old-slug'}
    if target_is_draft:
        data_app_block['isDraft'] = True
        data_app_block['parentConfigurationId'] = 'cfg-prod-1'
    existing_data_app = DataApp(
        name='Old',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id=configuration_id or 'cfg-draft-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='2',
        type='python-js',
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': data_app_block}},
        state='stopped',
    )
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(return_value=existing_data_app),
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_update_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    if expect_rejected:
        with pytest.raises(ValueError, match='not allowed on draft data apps'):
            await modify_python_js_data_app(
                ctx=mcp_context_client,
                name='My App',
                description='desc',
                configuration_id=configuration_id,
                parent_configuration_id=parent_configuration_id,
                authentication_type='no-auth',
            )
        keboola_client.data_science_client.create_data_app.assert_not_called()
        keboola_client.storage_client.configuration_update.assert_not_called()
    else:
        result = await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='My App',
            description='desc',
            configuration_id=configuration_id,
            authentication_type='no-auth',
        )
        assert result.response == 'updated'
        keboola_client.storage_client.configuration_update.assert_awaited_once()
        new_cfg = keboola_client.storage_client.configuration_update.await_args.kwargs['configuration']
        assert new_cfg['authorization']['app_proxy']['auth_rules'] == [
            {'type': 'pathPrefix', 'value': '/', 'auth_required': False}
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize(('auto_suspend_after_seconds', 'expected_auto_suspend'), [(600, 600), (None, 3600)])
async def test_modify_python_js_data_app_update_patches_storage_config(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    auto_suspend_after_seconds: int | None,
    expected_auto_suspend: int,
) -> None:
    """Update path: fetch storage config → merge updates → PATCH via configuration_update.

    An omitted `auto_suspend_after_seconds` keeps the app's value, so a rename leaves it alone.
    """
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)

    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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
            'parameters': {'autoSuspendAfterSeconds': 3600, 'dataApp': {'slug': 'old-slug'}},
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
        auto_suspend_after_seconds=auto_suspend_after_seconds,
    )

    assert isinstance(result, ModifiedPythonJsDataAppOutput)
    assert result.response == 'updated'
    # The PATCH should carry merged config
    patch_kwargs = keboola_client.storage_client.configuration_update.await_args.kwargs
    new_cfg = patch_kwargs['configuration']
    assert new_cfg['parameters']['autoSuspendAfterSeconds'] == expected_auto_suspend
    # The platform now picks a default image for python-js apps; the MCP must NOT overwrite a
    # legacy image pin already in the config, but must NOT force it to any new value either.
    assert new_cfg['runtime']['image']['version'] == 'old-version'
    # slug must remain untouched (immutable)
    assert new_cfg['parameters']['dataApp']['slug'] == 'old-slug'
    # An update that does not pass `storage_access` leaves `runtime.workspace` alone — see
    # `test_modify_python_js_data_app_update_storage_access` for the full matrix.
    assert 'workspace' not in new_cfg['runtime']
    # KBC_TOKEN / KBC_URL / BRANCH_ID are injected by the platform at runtime — the MCP must
    # not write them back into the stored config on update either.
    assert 'secrets' not in new_cfg['parameters']['dataApp']


@pytest.mark.asyncio
async def test_modify_python_js_data_app_update_repoints_external_git_branch(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Update path repoints an external-git app's branch: the PATCH carries the new
    `parameters.dataApp.git.branch`, the encrypted `#password`/`repository`/`username` are
    preserved, no re-encryption happens, and the change_summary hints at redeploy (CFTL-714)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

    git_block = {
        'repository': 'https://github.com/org/repo.git',
        'username': 'kai',
        '#password': 'KBC::cipher::secret',
        'branch': 'main',
    }
    existing_data_app = DataApp(
        name='Repo App',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='2',
        type='python-js',
        configuration={
            'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'repo-app', 'git': git_block}},
        },
        state='stopped',
        is_managed_git_repo=False,
    )
    updated_data_app = existing_data_app.model_copy(update={'config_version': '3'})

    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(side_effect=[existing_data_app, updated_data_app]),
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(ssh_url=None, https_url=None, is_managed_git_repo=False)
    )
    # Encryption must NOT be called on a branch-only repoint (the token stays encrypted verbatim).
    keboola_client.encryption_client = mocker.AsyncMock()
    mocker.patch('keboola_mcp_server.tools.data_apps.set_cfg_update_metadata', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    result = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='',
        description='',
        configuration_id='cfg-1',
        change_description='Flip to feature branch',
        branch='feature-x',
    )

    assert isinstance(result, ModifiedPythonJsDataAppOutput)
    assert result.response == 'updated'
    new_cfg = keboola_client.storage_client.configuration_update.await_args.kwargs['configuration']
    assert new_cfg['parameters']['dataApp']['git'] == {
        'repository': 'https://github.com/org/repo.git',
        'username': 'kai',
        '#password': 'KBC::cipher::secret',
        'branch': 'feature-x',
    }
    keboola_client.encryption_client.encrypt.assert_not_called()
    assert result.change_summary is not None
    assert 'feature-x' in result.change_summary


@pytest.mark.asyncio
async def test_modify_python_js_data_app_update_branch_rejected_on_managed_repo_app(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """A Keboola-managed-repo app is rejected on the gate `is_managed_git_repo=True`, NOT on the
    git block — it carries one here to prove block-presence is no longer the discriminator. Nothing
    is written (CFTL-714 review)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

    managed_repo_app = DataApp(
        name='Prod App',
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
                'dataApp': {
                    'slug': 'prod-app',
                    'git': {'repository': 'r', 'username': 'kai', '#password': 'p', 'branch': 'main'},
                },
            }
        },
        state='stopped',
        is_managed_git_repo=True,
    )
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(return_value=managed_repo_app),
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})

    with pytest.raises(ValueError, match='Keboola-managed git repo'):
        await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='',
            description='',
            configuration_id='cfg-1',
            branch='feature-x',
        )
    keboola_client.storage_client.configuration_update.assert_not_called()


def _assert_legacy_fallback_hint(change_summary: str | None, *, expected: bool) -> None:
    """The legacy WORKSPACE_ID fallback is deprecated, so every write through it must tell the
    agent so -- and nothing else may claim it did."""
    hint = 'deprecated' in (change_summary or '') and 'data-apps-storage-workspace' in (change_summary or '')
    assert hint is expected, change_summary


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('has_feature', 'storage_access', 'expected_workspace', 'expected_secrets'),
    [
        (True, None, {'enabled': True}, None),
        (True, True, {'enabled': True}, None),
        (True, False, None, None),
        (False, None, None, {'WORKSPACE_ID': 'wid-legacy'}),
        (False, True, None, {'WORKSPACE_ID': 'wid-legacy'}),
        (False, False, None, None),
    ],
    ids=[
        'feature_omitted_defaults_on',
        'feature_explicit_on',
        'feature_opted_out',
        'legacy_omitted_defaults_on',
        'legacy_explicit_on',
        'legacy_opted_out',
    ],
)
async def test_modify_python_js_data_app_create_storage_access(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    has_feature: bool,
    storage_access: bool | None,
    expected_workspace: JsonDict | None,
    expected_secrets: JsonDict | None,
) -> None:
    """`storage_access` drives Storage access on create through whichever mechanism the project
    supports: `runtime.workspace.enabled` when `data-apps-storage-workspace` is on, and the legacy
    `parameters.dataApp.secrets.WORKSPACE_ID` fallback when it is off. Omitting it keeps the
    historical create default (Storage access on); `False` opts out on both mechanisms."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=has_feature)

    workspace_manager.get_data_app_workspace_id = mocker.AsyncMock(return_value='wid-legacy')

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

    result = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        slug='my-app',
        storage_access=storage_access,
    )

    serialized = keboola_client.data_science_client.create_data_app.await_args.kwargs['configuration'].model_dump(
        by_alias=True, exclude_none=True
    )
    # `exclude_none=True` prunes the whole `runtime` block when the MCP sets no fields in it
    # (image is platform-default, workspace off), hence the `.get` chain rather than indexing.
    assert serialized.get('runtime', {}).get('workspace') == expected_workspace
    assert serialized['parameters']['dataApp'].get('secrets') == expected_secrets
    # The agent must be able to verify Storage access rather than assume it.
    assert result.data_app.storage_access_enabled is (storage_access is not False)
    _assert_legacy_fallback_hint(result.change_summary, expected=expected_secrets is not None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        'has_feature',
        'storage_access',
        'existing_workspace',
        'existing_secrets',
        'expected_workspace',
        'expected_secrets',
        'expect_legacy_fallback',
    ),
    [
        # Omitting `storage_access` never changes Storage access, on either mechanism — an
        # unrelated edit (rename, auto-suspend) must not grant or revoke it.
        (True, None, None, {'KEEP': 'x'}, None, {'KEEP': 'x'}, False),
        (True, None, {'enabled': True}, {'KEEP': 'x'}, {'enabled': True}, {'KEEP': 'x'}, False),
        (False, None, None, {'KEEP': 'x'}, None, {'KEEP': 'x'}, False),
        # Explicit enable — an existing flagless app becomes
        # Storage-enabled through MCP alone, with no UI step.
        (True, True, None, {'KEEP': 'x'}, {'enabled': True}, {'KEEP': 'x'}, False),
        (False, True, None, {'KEEP': 'x'}, None, {'KEEP': 'x', 'WORKSPACE_ID': 'wid-legacy'}, True),
        # An app that already carries a WORKSPACE_ID keeps it, and the fallback is never resolved:
        # resolving provisions a workspace the app will not use, and raises outright in a
        # workspace-pinned session.
        (
            False,
            True,
            None,
            {'KEEP': 'x', 'WORKSPACE_ID': 'wid-user'},
            None,
            {'KEEP': 'x', 'WORKSPACE_ID': 'wid-user'},
            False,
        ),
        # Explicit disable.
        (True, False, {'enabled': True}, {'KEEP': 'x'}, {'enabled': False}, {'KEEP': 'x'}, False),
        (False, False, None, {'KEEP': 'x', 'WORKSPACE_ID': 'wid-old'}, None, {'KEEP': 'x'}, False),
        # Disabling must strip a stale legacy secret even on a feature-enabled project: secrets
        # reach the app as env vars regardless of the feature, so leaving it behind would keep
        # WORKSPACE_ID flowing into an app we just reported as having no Storage access.
        (
            True,
            False,
            {'enabled': True},
            {'KEEP': 'x', 'WORKSPACE_ID': 'wid-stale'},
            {'enabled': False},
            {'KEEP': 'x'},
            False,
        ),
    ],
    ids=[
        'feature_omitted_stays_off',
        'feature_omitted_stays_on',
        'legacy_omitted_no_backfill',
        'feature_explicit_enable',
        'legacy_explicit_enable',
        'legacy_explicit_enable_preserves_existing_id',
        'feature_explicit_disable',
        'legacy_explicit_disable',
        'feature_explicit_disable_strips_stale_secret',
    ],
)
async def test_modify_python_js_data_app_update_storage_access(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    has_feature: bool,
    storage_access: bool | None,
    existing_workspace: JsonDict | None,
    existing_secrets: JsonDict,
    expected_workspace: JsonDict | None,
    expected_secrets: JsonDict,
    expect_legacy_fallback: bool,
) -> None:
    """On update, `storage_access` is the only thing that moves Storage access. Omitting it leaves
    the stored config alone — no silent backfill on either mechanism."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=has_feature)

    workspace_manager.get_data_app_workspace_id = mocker.AsyncMock(return_value='wid-legacy')

    existing_runtime: JsonDict = {'image': {'version': 'old-version'}}
    if existing_workspace is not None:
        existing_runtime['workspace'] = existing_workspace
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
                'dataApp': {'slug': 'old-slug', 'secrets': dict(existing_secrets)},
            },
            'runtime': existing_runtime,
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

    result = await modify_python_js_data_app(
        ctx=mcp_context_client,
        name='Old',
        description='desc',
        configuration_id='cfg-1',
        auto_suspend_after_seconds=600,
        storage_access=storage_access,
    )

    new_cfg = keboola_client.storage_client.configuration_update.await_args.kwargs['configuration']
    assert new_cfg['runtime'].get('workspace') == expected_workspace
    assert new_cfg['parameters']['dataApp']['secrets'] == expected_secrets
    # A legacy `runtime.image.version` pin stays untouched whatever happens to the workspace flag.
    assert new_cfg['runtime']['image']['version'] == 'old-version'
    expected_enabled = expected_workspace == {'enabled': True} or 'WORKSPACE_ID' in expected_secrets
    assert result.data_app.storage_access_enabled is expected_enabled
    # The deprecation hint fires only when the MCP itself writes the legacy secret -- never when it
    # merely preserves one that is already there.
    _assert_legacy_fallback_hint(result.change_summary, expected=expect_legacy_fallback)
    # Resolving the fallback id has side effects (it provisions the shared workspace when the
    # project has none, and raises in a workspace-pinned session), so it must happen only when the
    # id is actually going to be written.
    assert workspace_manager.get_data_app_workspace_id.await_count == (1 if expect_legacy_fallback else 0)


@pytest.mark.asyncio
async def test_modify_python_js_data_app_update_storage_access_survives_unavailable_workspace(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """A workspace-pinned session (the Data App flow, `X-Workspace-Id`) has no MCP-managed workspace
    to fall back on -- `get_data_app_workspace_id()` raises rather than provisioning one. Enabling
    Storage access on an app that already carries a WORKSPACE_ID must still succeed, because nothing
    needs resolving in the first place."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)

    workspace_manager.get_data_app_workspace_id = mocker.AsyncMock(
        side_effect=ValueError('No MCP-managed workspace exists for this project/branch')
    )

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
                'dataApp': {'slug': 'old-slug', 'secrets': {'WORKSPACE_ID': 'wid-pinned'}},
            },
        },
        state='stopped',
    )
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(side_effect=[existing_data_app, existing_data_app.model_copy(update={'config_version': '3'})]),
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
        name='Old',
        description='desc',
        configuration_id='cfg-1',
        storage_access=True,
    )

    new_cfg = keboola_client.storage_client.configuration_update.await_args.kwargs['configuration']
    assert new_cfg['parameters']['dataApp']['secrets'] == {'WORKSPACE_ID': 'wid-pinned'}
    assert result.data_app.storage_access_enabled is True
    workspace_manager.get_data_app_workspace_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_passes_storage_through(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Create path forwards a caller-supplied `storage` block (with direct-grant) into the DSAPI payload."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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


@pytest.mark.parametrize('passed_storage', [{}, {'input': {}, 'output': {}}], ids=['empty', 'all_empty_objects'])
@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_omits_empty_storage(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    passed_storage,
) -> None:
    """Create path never persists an empty `storage` block (it collapses to `[]` server-side, AI-3135)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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
        storage=passed_storage,
    )

    serialized = keboola_client.data_science_client.create_data_app.await_args.kwargs['configuration'].model_dump(
        by_alias=True, exclude_none=True
    )
    assert 'storage' not in serialized


@pytest.mark.asyncio
async def test_modify_python_js_data_app_update_replaces_storage(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Update path: a non-empty `storage` argument replaces the entire stored storage block."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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


@pytest.mark.parametrize(
    ('existing_runtime', 'image_version', 'expected_runtime'),
    [
        pytest.param({'image': {'version': 'old'}}, None, {'image': {'version': 'old'}}, id='unset_keeps_pin'),
        pytest.param(
            {'workspace': {'enabled': True}},
            _NODE_24,
            {'workspace': {'enabled': True}, 'image': {'version': _NODE_24}},
            id='sets_pin',
        ),
        pytest.param({'image': {'version': 'old'}}, '', None, id='empty_drops_pin_and_empty_runtime'),
        pytest.param(
            {'image': {'version': 'old'}, 'workspace': {'enabled': True}},
            '',
            {'workspace': {'enabled': True}},
            id='empty_drops_pin_keeps_workspace',
        ),
    ],
)
def test_update_existing_code_data_app_config_image_version(
    existing_runtime: JsonDict, image_version: str | None, expected_runtime: JsonDict | None
) -> None:
    existing = {
        'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}, 'imageVersion': 'legacy'},
        'runtime': existing_runtime,
    }
    new = _update_existing_code_data_app_config(existing, auto_suspend_after_seconds=600, image_version=image_version)
    assert new.get('runtime') == expected_runtime
    # Setting or dropping a pin also drops the legacy `parameters.imageVersion` spelling.
    assert ('imageVersion' in new['parameters']) is (image_version is None)
    assert new['parameters']['autoSuspendAfterSeconds'] == 600
    # original must not be mutated
    assert existing['parameters']['autoSuspendAfterSeconds'] == 900


@pytest.mark.parametrize(
    ('configuration', 'expected_image', 'expected_pinned'),
    [
        pytest.param({'runtime': {'image': {'version': _NODE_24}}}, _PYTHON_JS_IMAGES[1], True, id='offered_pin'),
        pytest.param({'parameters': {'imageVersion': _NODE_24}}, _PYTHON_JS_IMAGES[1], True, id='legacy_pin'),
        pytest.param(
            {'runtime': {'image': {'version': 'gone'}}},
            RuntimeImage(version='gone', description='no longer offered by the platform'),
            True,
            id='dropped_pin',
        ),
        pytest.param({'parameters': {}}, _PYTHON_JS_IMAGES[0], False, id='no_pin_follows_default'),
    ],
)
def test_resolve_app_image(configuration: JsonDict, expected_image: RuntimeImage, expected_pinned: bool) -> None:
    assert _resolve_app_image(configuration, _PYTHON_JS_IMAGES) == (expected_image, expected_pinned)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('configuration_id', 'image_version', 'expected_error'),
    [
        pytest.param('', _NODE_24, None, id='create_pins'),
        pytest.param('cfg-1', _NODE_24, None, id='update_pins'),
        pytest.param('', 'node-24', 'Unknown image_version "node-24"', id='create_rejects_unknown'),
        # A tag of another app type is not a python-js image.
        pytest.param('cfg-1', 'streamlit-1.0', 'Unknown image_version "streamlit-1.0"', id='update_rejects_other_type'),
    ],
)
async def test_modify_python_js_data_app_image_version(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    configuration_id: str,
    image_version: str,
    expected_error: str | None,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.data_science_client.list_runtimes = mocker.AsyncMock(return_value=_RUNTIMES)
    keboola_client.data_science_client.create_data_app = mocker.AsyncMock(
        return_value=_make_python_js_data_app_response()
    )
    keboola_client.data_science_client.get_app_git_repo = mocker.AsyncMock(
        return_value=AppGitRepoResponse(https_url='https://managed.repo/org/app.git', is_managed_git_repo=True)
    )
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value={})
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(return_value=_make_python_js_prod_data_app(configuration_id='cfg-1')),
    )
    for helper in ('set_cfg_creation_metadata', 'set_cfg_update_metadata'):
        mocker.patch(f'keboola_mcp_server.tools.data_apps.{helper}', mocker.AsyncMock())
    mocker.patch('keboola_mcp_server.tools.data_apps.apply_folder_metadata', mocker.AsyncMock(return_value=None))

    call = modify_python_js_data_app(
        ctx=mcp_context_client,
        name='My App',
        description='desc',
        configuration_id=configuration_id,
        image_version=image_version,
    )

    if expected_error:
        with pytest.raises(ValueError, match=re.escape(expected_error)) as exc:
            await call
        # The error lists the valid tags, so the agent can retry with one.
        assert _NODE_20 in str(exc.value) and _NODE_24 in str(exc.value)
        keboola_client.data_science_client.create_data_app.assert_not_awaited()
        keboola_client.storage_client.configuration_update.assert_not_awaited()
        return

    result = await call
    if configuration_id:
        written = keboola_client.storage_client.configuration_update.await_args.kwargs['configuration']
        assert result.change_summary is not None
        assert f"Backend set to image '{image_version}'" in result.change_summary
    else:
        written = keboola_client.data_science_client.create_data_app.await_args.kwargs['configuration'].model_dump(
            by_alias=True, exclude_none=True
        )
    assert written['runtime']['image'] == {'version': image_version}


def test_update_existing_code_data_app_config_keeps_auto_suspend_when_omitted() -> None:
    existing = {'parameters': {'autoSuspendAfterSeconds': 3600, 'dataApp': {'slug': 'x'}}}
    new = _update_existing_code_data_app_config(existing)
    assert new['parameters']['autoSuspendAfterSeconds'] == 3600


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
        # Empty dict is an explicit wipe — the `storage` key is removed entirely (never persisted as
        # `{}`, which the backend collapses to `[]` and breaks the mapping editor, AI-3135).
        ({}, False, None),
        # A block that prunes down to nothing (only empty objects) is treated like a wipe.
        ({'input': {}, 'output': {}}, False, None),
        # Non-empty dict replaces the existing block wholesale.
        (
            {'output': {'tables': [{'destination': 'in.c-main.new', 'unload_strategy': 'direct-grant'}]}},
            True,
            {'output': {'tables': [{'destination': 'in.c-main.new', 'unload_strategy': 'direct-grant'}]}},
        ),
        # Empty mapping containers are pruned from an otherwise-populated block.
        (
            {
                'input': {},
                'output': {'tables': [{'destination': 'in.c-main.new', 'unload_strategy': 'direct-grant'}]},
            },
            True,
            {'output': {'tables': [{'destination': 'in.c-main.new', 'unload_strategy': 'direct-grant'}]}},
        ),
    ],
)
def test_update_existing_code_data_app_config_storage_semantics(
    passed_storage, expected_storage_key_present, expected_storage
) -> None:
    """`storage=None` preserves; an empty/all-empty block wipes (removes the key); a non-empty dict
    replaces wholesale (with empty mapping containers pruned)."""
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
    if expected_storage_key_present:
        assert new['storage'] == expected_storage


def test_update_existing_code_data_app_config_repoints_git_branch() -> None:
    """`branch` rewrites only `parameters.dataApp.git.branch`; the encrypted `#password`,
    `repository`, and `username` are preserved verbatim (no re-encryption). CFTL-714."""
    existing = {
        'parameters': {
            'autoSuspendAfterSeconds': 900,
            'dataApp': {
                'slug': 'x',
                'git': {
                    'repository': 'https://github.com/org/repo.git',
                    'username': 'kai',
                    '#password': 'KBC::cipher::secret',
                    'branch': 'main',
                },
            },
        },
    }
    new = _update_existing_code_data_app_config(existing, auto_suspend_after_seconds=900, branch='feature-x')
    assert new['parameters']['dataApp']['git'] == {
        'repository': 'https://github.com/org/repo.git',
        'username': 'kai',
        '#password': 'KBC::cipher::secret',
        'branch': 'feature-x',
    }
    # branch=None must leave the existing branch untouched.
    unchanged = _update_existing_code_data_app_config(existing, auto_suspend_after_seconds=900)
    assert unchanged['parameters']['dataApp']['git']['branch'] == 'main'
    # original must not be mutated
    assert existing['parameters']['dataApp']['git']['branch'] == 'main'


def test_update_existing_code_data_app_config_branch_without_git_block_raises() -> None:
    """Setting `branch` on a config without a git block is a programming error the helper rejects
    (the tool validates external-git-ness first, but the invariant holds here too)."""
    existing = {'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}}}
    with pytest.raises(ValueError, match='no parameters.dataApp.git block'):
        _update_existing_code_data_app_config(existing, auto_suspend_after_seconds=900, branch='feature-x')


def _make_external_git_data_app(*, is_managed_git_repo: bool | None = False) -> DataApp:
    """A python-js DataApp with an external-git block, as `_fetch_data_app` would return it."""
    return DataApp(
        name='Repo App',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id='cfg-1',
        data_app_id='app-1',
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type='python-js',
        configuration={
            'parameters': {
                'dataApp': {
                    'slug': 'repo-app',
                    'git': {'repository': 'r', 'username': 'kai', '#password': 'p', 'branch': 'main'},
                }
            }
        },
        state='stopped',
        is_managed_git_repo=is_managed_git_repo,
    )


@pytest.mark.parametrize('bad_branch', ['   ', 'has space', '\t'])
def test_validate_branch_update_rejects_invalid_branch_name(bad_branch: str) -> None:
    """On the update path an external-git app rejects an empty/whitespace-containing branch name."""
    app = _make_external_git_data_app(is_managed_git_repo=False)
    with pytest.raises(ValueError, match='not a valid git branch name'):
        _validate_branch_update(bad_branch, app, 'cfg-1')


@pytest.mark.parametrize(
    ('is_managed_git_repo', 'error_match'),
    [
        (True, 'Keboola-managed git repo'),
        (None, 'could not determine'),
    ],
)
def test_validate_branch_update_rejects_managed_and_unknown(is_managed_git_repo: bool | None, error_match: str) -> None:
    """The repoint is gated on `is_managed_git_repo`, NOT on the presence of a git block: a managed
    app (which can carry a git block too) is rejected, and an undetermined flag is refused rather
    than risking a change to a managed app (CFTL-714 review)."""
    app = _make_external_git_data_app(is_managed_git_repo=is_managed_git_repo)
    with pytest.raises(ValueError, match=error_match):
        _validate_branch_update('feature-x', app, 'cfg-1')


# ===== Tests for deploy_data_app config publishing =====


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('app_type', 'mode', 'published_after', 'latest_after', 'expected_unpublished'),
    [
        # python-js used to omit configVersion, so DSAPI kept the old published version while the tool
        # reported the latest one as deployed.
        pytest.param('python-js', 'dev', '5', '5', False, id='python_js_dev_publishes_latest'),
        pytest.param('python-js', None, '5', '5', False, id='python_js_prod_publishes_latest'),
        pytest.param('streamlit', None, '5', '5', False, id='streamlit_publishes_latest'),
        # The config was saved again after the deploy was triggered: report the drift, not the latest version.
        pytest.param('python-js', None, '5', '6', True, id='config_changed_after_deploy'),
    ],
)
async def test_deploy_data_app_publishes_latest_config_version_and_reports_published(
    mocker,
    mcp_context_client: Context,
    app_type: str,
    mode: Literal['dev', 'production'] | None,
    published_after: str,
    latest_after: str,
    expected_unpublished: bool,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.data_science_client.list_runtimes = mocker.AsyncMock(return_value=_RUNTIMES)
    keboola_client.storage_client.configuration_version_latest = mocker.AsyncMock(return_value=5)

    def make_app(config_version: str, published_config_version: str) -> DataApp:
        return DataApp(
            name='app',
            component_id=DATA_APP_COMPONENT_ID,
            configuration_id='cfg-1',
            data_app_id='app-1',
            project_id='proj-1',
            branch_id='branch-1',
            config_version=config_version,
            published_config_version=published_config_version,
            type=app_type,
            configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': {'slug': 'x'}}},
            state='running',
        )

    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app',
        mocker.AsyncMock(side_effect=[make_app('5', '3'), make_app(latest_after, published_after)]),
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_latest_run', mocker.AsyncMock(return_value=None))

    result = await deploy_data_app(ctx=mcp_context_client, action='deploy', configuration_id='cfg-1', mode=mode)

    keboola_client.storage_client.configuration_version_latest.assert_awaited_once_with(DATA_APP_COMPONENT_ID, 'cfg-1')
    keboola_client.data_science_client.deploy_data_app.assert_awaited_once_with('app-1', '5', mode=mode)
    assert result.deployment_info is not None
    assert result.deployment_info.version == published_after
    assert result.deployment_info.latest_config_version == latest_after
    assert result.deployment_info.has_unpublished_changes is expected_unpublished
    # A python-js app reports the image it runs; the unpinned app follows the catalog default.
    is_python_js = app_type == 'python-js'
    assert result.deployment_info.image == (_PYTHON_JS_IMAGES[0] if is_python_js else None)
    assert result.deployment_info.image_pinned is (False if is_python_js else None)


@pytest.mark.parametrize(
    ('config_version', 'published_config_version', 'expected_version', 'expected_unpublished'),
    [
        pytest.param('5', '5', '5', False, id='up_to_date'),
        pytest.param('5', '3', '3', True, id='unpublished_changes'),
        pytest.param('5', None, '5', False, id='published_unknown_falls_back_to_latest'),
    ],
)
def test_with_deployment_info_reports_published_version(
    data_app: DataApp,
    config_version: str,
    published_config_version: str | None,
    expected_version: str,
    expected_unpublished: bool,
) -> None:
    data_app.config_version = config_version
    data_app.published_config_version = published_config_version

    info = data_app.with_deployment_info(logs=[]).deployment_info

    assert info is not None
    assert info.version == expected_version
    assert info.latest_config_version == config_version
    assert info.has_unpublished_changes is expected_unpublished


def test_data_app_from_api_responses_keeps_published_and_latest_versions_apart() -> None:
    api_response = _make_data_app_response().model_copy(update={'config_version': '3'})
    api_configuration = ConfigurationAPIResponse.model_validate(
        {'componentId': DATA_APP_COMPONENT_ID, 'id': 'cfg-123', 'name': 'app', 'version': 5, 'configuration': {}}
    )

    data_app = DataApp.from_api_responses(api_response, api_configuration)

    assert data_app.config_version == '5'
    assert data_app.published_config_version == '3'


# ===== Tests for modify_python_js_data_app draft create path =====


def _make_python_js_parent_data_app(
    *,
    data_app_id: str = 'app-prod-1',
    configuration_id: str = 'cfg-prod-1',
    repo_url: str | None = 'https://managed.repo/org/prod.git',
    type: str = 'python-js',
    is_draft: bool = False,
) -> DataApp:
    """Build a DataApp the way `_fetch_data_app` would when looking up the parent.

    `is_draft=True` models a caller mistakenly passing a draft as the parent — drafts cannot
    parent another draft.
    """
    data_app_block: dict = {'slug': 'demo'}
    if is_draft:
        data_app_block['isDraft'] = True
        data_app_block['parentConfigurationId'] = 'cfg-grandparent'
    return DataApp(
        name='Prod App',
        component_id=DATA_APP_COMPONENT_ID,
        configuration_id=configuration_id,
        data_app_id=data_app_id,
        project_id='proj-1',
        branch_id='branch-1',
        config_version='1',
        type=type,
        configuration={'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': data_app_block}},
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
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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

    # An agent-supplied branch cannot be uniquified by the server, so the response spells out the
    # safe checkout instead — a bare `git checkout iter-feat` would resolve to an existing
    # `origin/iter-feat` and serve its stale tip.
    assert result.change_summary is not None
    assert 'git checkout -B iter-feat --no-track origin/main' in result.change_summary
    assert 'git rev-list --count iter-feat..origin/main' in result.change_summary
    # On an empty repo `main` goes first, so the draft branch never becomes the repo's undeletable default.
    assert (
        'git checkout -b main && git commit --allow-empty -m "init main" && git push origin main && '
        'git checkout -b iter-feat' in result.change_summary
    )

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
async def test_modify_python_js_data_app_create_draft_rejects_main_branch(
    mcp_context_client: Context,
    mocker,
) -> None:
    """A draft create on `main` is rejected by default — `main` is the prod app's branch."""
    parent = _make_python_js_parent_data_app()
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=parent))

    with pytest.raises(ValueError, match='reserved for the prod app'):
        await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='View',
            description='view draft',
            slug='demo-view',
            parent_configuration_id='cfg-prod-1',
            branch='main',
        )


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_draft_allows_main_branch_with_flag(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """`allow_main_branch=True` lets the platform create a read-only view draft pinned to `main`
    (the AI workspace preview needs a deployable draft that tracks the published app)."""
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
        name='View',
        description='view draft',
        slug='demo-view',
        parent_configuration_id='cfg-prod-1',
        branch='main',
        allow_main_branch=True,
    )

    assert result.branch == 'main'
    create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
    serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
    assert serialized['parameters']['dataApp']['git']['branch'] == 'main'
    assert serialized['parameters']['dataApp']['parentConfigurationId'] == 'cfg-prod-1'


@pytest.mark.asyncio
async def test_modify_python_js_data_app_create_draft_defaults_branch_to_unique_name(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """Omitting `branch` pins the draft to a freshly generated, unique `draft-<hex>` branch.

    Regression test: the default used to be the fixed literal `init`, so every default-branch
    draft of the same prod app reused one branch name. A later
    draft's `git checkout init` then resolved to the stale `origin/init` left behind by an earlier
    one instead of branching off `main`, and the draft silently previewed outdated code. Two
    consecutive default creates must therefore yield two different branch names.

    `secrets.token_hex` is stubbed with two fixed values so the assertion is deterministic: what
    matters is that the name is derived fresh on every create (the old default was a constant),
    not that two real random draws happen to differ.
    """
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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

    hex_suffixes = iter(('a1b2c3', 'd4e5f6'))
    mocker.patch(
        'keboola_mcp_server.tools.data_apps.secrets.token_hex',
        side_effect=lambda _nbytes: next(hex_suffixes),
    )

    branches: list[str] = []
    for _ in range(2):
        result = await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='Draft',
            description='draft iteration',
            slug='demo-draft',
            parent_configuration_id='cfg-prod-1',
        )

        assert result.branch is not None
        assert re.fullmatch(r'draft-[0-9a-f]{6}', result.branch), result.branch
        branches.append(result.branch)

        # The stored config carries the same branch as the pin, plus the parent linkage.
        create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
        serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
        assert serialized['parameters']['dataApp']['git']['branch'] == result.branch
        assert serialized['parameters']['dataApp']['parentConfigurationId'] == 'cfg-prod-1'

        # The agent is told to branch off `origin/main` explicitly — a bare `git checkout` is what
        # silently resolves to a stale remote tip.
        assert result.change_summary is not None
        assert f'git checkout -B {result.branch} --no-track origin/main' in result.change_summary
        assert f'git rev-list --count {result.branch}..origin/main' in result.change_summary

    # The heart of the regression: consecutive default creates must not share a branch name.
    assert branches == ['draft-a1b2c3', 'draft-d4e5f6']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('name', 'slug_kwargs', 'expected_slug_pattern'),
    [
        # Explicit slug on the draft path is honored verbatim (unchanged behavior).
        ('My Draft App', {'slug': 'demo-draft'}, r'^demo-draft$'),
        # Omitted slug is derived from `name` plus a unique `-draft-<hex>` suffix so draft slugs
        # don't collide with the parent prod app or with each other (AI-3634).
        ('My Draft App', {}, r'^my-draft-app-draft-[0-9a-f]{6}$'),
        # Degenerate name still yields a valid, suffixed draft slug.
        ('!!!', {}, r'^data-app-draft-[0-9a-f]{6}$'),
        # A long (~60-char) name has its base truncated to 37 chars so that, with the 13-char
        # `-draft-<hex>` suffix, the final slug is exactly 50 — within the data-app URL-prefix
        # limit enforced by the UI (AI-3634). 37 + len('-draft-') + 6 == 50.
        ('a' * 60, {}, r'^a{37}-draft-[0-9a-f]{6}$'),
    ],
)
async def test_modify_python_js_data_app_create_draft_auto_derives_slug(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
    name: str,
    slug_kwargs: dict,
    expected_slug_pattern: str,
) -> None:
    """Draft create (with `parent_configuration_id`) no longer requires `slug`: it is auto-derived
    from `name` with a unique `-draft-<hex>` suffix, while an explicit slug is honored (AI-3634)."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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

    await modify_python_js_data_app(
        ctx=mcp_context_client,
        name=name,
        description='draft iteration',
        parent_configuration_id='cfg-prod-1',
        **slug_kwargs,
    )

    create_kwargs = keboola_client.data_science_client.create_data_app.await_args.kwargs
    serialized = create_kwargs['configuration'].model_dump(by_alias=True, exclude_none=True)
    assert re.match(expected_slug_pattern, serialized['parameters']['dataApp']['slug'])


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
    ],
)
async def test_modify_python_js_data_app_update_rejects_draft_args(
    mcp_context_client: Context,
    disallowed_arg: str,
    kwargs: dict,
    error_match: str,
) -> None:
    """The update path rejects create-only draft args (`parent_configuration_id`). `branch` is
    NOT rejected here — on update it repoints an external-git app's branch (CFTL-714)."""
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
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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
async def test_modify_python_js_data_app_create_draft_rejects_when_parent_is_draft(
    mocker,
    mcp_context_client: Context,
    workspace_manager,
) -> None:
    """A python-js *draft* parent is rejected with a clear message (not the misleading 'no repo URL'
    error): drafts can't parent another draft, so no credential is minted."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()
    keboola_client.has_feature = mocker.AsyncMock(return_value=True)
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

    # A draft has no repo_url of its own; the guard must fire before the repo_url check below.
    draft_parent = _make_python_js_parent_data_app(is_draft=True, repo_url=None)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=draft_parent))

    with pytest.raises(ValueError, match=r'is itself a python-js \*\*draft\*\*'):
        await modify_python_js_data_app(
            ctx=mcp_context_client,
            name='Draft',
            description='',
            slug='demo-draft',
            parent_configuration_id='cfg-prod-1',
        )

    keboola_client.data_science_client.create_app_git_credential.assert_not_called()


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
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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
    workspace_manager.get_data_app_branch_id = mocker.AsyncMock(return_value='branch-1')

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
async def test_create_python_js_data_app_git_credential_rejects_draft(
    mocker,
    mcp_context_client: Context,
) -> None:
    """Drafts have no managed repo of their own — the tool must reject them early (before touching
    get_app_git_repo) and point at the parent prod app, rather than falling through to the
    misleading https_url=None 'platform bug' error."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client = mocker.AsyncMock()

    draft = _make_python_js_draft_data_app(
        configuration_id='cfg-draft-1', data_app_id='app-draft-1', parent_configuration_id='cfg-prod-1'
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=draft))

    with pytest.raises(ValueError, match=r'is a python-js \*\*draft\*\*') as excinfo:
        await create_python_js_data_app_git_credential(
            ctx=mcp_context_client,
            configuration_id='cfg-draft-1',
        )

    # The error must steer the caller to the parent prod app, and no repo/credential calls happen.
    assert 'parentConfigurationId="cfg-prod-1"' in str(excinfo.value)
    keboola_client.data_science_client.get_app_git_repo.assert_not_called()
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


def _build_storage_config_entry(*, cfg_id: str, parent_configuration_id: str | None, is_draft: bool = True) -> dict:
    """Mirror the shape returned by `storage_client.configuration_list` for python-js apps.

    `is_draft=False` with a `parent_configuration_id` set models a misconfigured non-draft that
    points at a prod but lacks the `isDraft` flag — it must NOT be surfaced as a draft.
    """
    data_app_block: dict = {'slug': f'app-{cfg_id}'}
    if parent_configuration_id is not None:
        data_app_block['parentConfigurationId'] = parent_configuration_id
        if is_draft:
            data_app_block['isDraft'] = True
    return {
        'id': cfg_id,
        'name': f'app-{cfg_id}',
        'configuration': {
            'parameters': {'autoSuspendAfterSeconds': 900, 'dataApp': data_app_block},
        },
        'version': 1,
    }


@pytest.mark.parametrize(
    ('configuration', 'expected'),
    [
        ({'parameters': {'dataApp': {'isDraft': True}}}, True),
        ({'parameters': {'dataApp': {'isDraft': False}}}, False),
        ({'parameters': {'dataApp': {'slug': 'prod'}}}, False),
        ({'parameters': {}}, False),
        ({}, False),
        # Malformed/corrupted shapes must be treated as "not a draft", never raise AttributeError.
        ({'parameters': {'dataApp': 'corrupted'}}, False),
        ({'parameters': 'corrupted'}, False),
        ({'parameters': None}, False),
    ],
    ids=[
        'is_draft',
        'not_draft',
        'no_flag',
        'no_data_app',
        'empty',
        'data_app_not_mapping',
        'parameters_not_mapping',
        'parameters_none',
    ],
)
def test_is_draft_config(configuration: dict, expected: bool) -> None:
    """`_is_draft_config` is true only for `isDraft=true` and is shape-safe against malformed configs."""
    assert _is_draft_config(configuration) is expected


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
    # A config that points at THIS prod but lacks `isDraft` is a misconfiguration, not a draft —
    # it must be excluded (and never even fetched, or fake_fetch below would KeyError).
    configs.append(
        _build_storage_config_entry(cfg_id='cfg-non-draft-child', parent_configuration_id=prod_cfg_id, is_draft=False)
    )
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
    # All drafts fetched successfully, so nothing was omitted.
    assert detail.drafts_unavailable == 0


@pytest.mark.asyncio
async def test_get_data_apps_detail_for_prod_counts_unavailable_drafts(
    mocker,
    mcp_context_client: Context,
) -> None:
    """A transient DSAPI failure on one draft's detail fetch must NOT silently shrink the list:
    the surviving draft is still returned and `drafts_unavailable` counts the omission so the
    caller can tell "temporarily unreachable" from "deleted"."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    prod_cfg_id = 'cfg-prod-1'
    prod = _make_python_js_prod_data_app(configuration_id=prod_cfg_id)
    ok_cfg_id, failing_cfg_id = 'cfg-draft-ok', 'cfg-draft-fail'
    ok_draft = _make_python_js_draft_data_app(
        configuration_id=ok_cfg_id, data_app_id=f'app-{ok_cfg_id}', parent_configuration_id=prod_cfg_id
    )
    configs = [
        _build_storage_config_entry(cfg_id=ok_cfg_id, parent_configuration_id=prod_cfg_id),
        _build_storage_config_entry(cfg_id=failing_cfg_id, parent_configuration_id=prod_cfg_id),
        _build_storage_config_entry(cfg_id=prod_cfg_id, parent_configuration_id=None),
    ]
    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=configs)

    async def fake_fetch(client, *, configuration_id, data_app_id):
        if configuration_id == prod_cfg_id:
            return prod
        if configuration_id == failing_cfg_id:
            raise RuntimeError('transient DSAPI failure (timeout)')
        return ok_draft

    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', side_effect=fake_fetch)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))

    result = await get_data_apps(ctx=mcp_context_client, configuration_ids=[prod_cfg_id])
    detail = result.data_apps[0]
    assert isinstance(detail, DataApp)
    assert [d.configuration_id for d in detail.drafts] == [ok_cfg_id]
    assert detail.drafts_unavailable == 1


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
    """Happy path: deletes the data app via DSAPI only and returns the parent configuration_id so
    the agent can pivot back. The Storage config must NOT be deleted by the tool — DSAPI already
    moves it to the trash, and a second delete would purge it from the trash permanently."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    draft = _make_python_js_draft_data_app(
        configuration_id='cfg-draft-1', data_app_id='app-draft-1', parent_configuration_id='cfg-prod-1'
    )
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=draft))
    keboola_client.data_science_client.delete_data_app = mocker.AsyncMock(return_value=None)
    keboola_client.storage_client.configuration_delete = mocker.AsyncMock()

    result = await delete_python_js_data_app_draft(ctx=mcp_context_client, configuration_id='cfg-draft-1')

    assert isinstance(result, DeletedDraftOutput)
    assert result.response == 'deleted'
    assert result.configuration_id == 'cfg-draft-1'
    assert result.data_app_id == 'app-draft-1'
    assert result.parent_configuration_id == 'cfg-prod-1'
    keboola_client.data_science_client.delete_data_app.assert_awaited_once_with('app-draft-1')
    keboola_client.storage_client.configuration_delete.assert_not_called()
    # The config link pivots to the parent prod and is labelled as such — not with the draft's name,
    # which would mislabel a link pointing at a different configuration.
    config_link = next(link for link in result.links if 'Data App Configuration' in link.title)
    assert 'data-apps/cfg-prod-1' in config_link.url
    assert 'parent prod app' in config_link.title
    assert draft.name not in config_link.title


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


def _make_failed_app_run(**overrides) -> AppRunResponse:
    payload = {
        'id': 'run-1',
        'appId': 'app-prod-1',
        'state': 'failed',
        'createdAt': '2026-06-12T10:36:20+00:00',
        'startedAt': None,
        'stoppedAt': '2026-06-12T10:36:21+00:00',
        'startupLogs': None,
        'failureReason': {
            'reason': 'ConfigDecryptionFailed',
            'message': 'failed to decrypt key "#API_KEY"',
        },
        'mode': 'prod',
    }
    payload.update(overrides)
    return AppRunResponse.model_validate(payload)


def test_app_run_info_flattens_failure_reason() -> None:
    info = AppRunInfo.from_api_response(_make_failed_app_run())
    assert info.state == 'failed'
    assert info.created_at == '2026-06-12T10:36:20+00:00'
    assert info.stopped_at == '2026-06-12T10:36:21+00:00'
    assert info.failure_reason == 'ConfigDecryptionFailed'
    assert info.failure_message == 'failed to decrypt key "#API_KEY"'
    assert info.startup_logs == []


def test_app_run_info_handles_successful_run_without_failure_reason() -> None:
    info = AppRunInfo.from_api_response(
        _make_failed_app_run(state='finished', failureReason=None, startupLogs='booting\nready')
    )
    assert info.failure_reason is None
    assert info.failure_message is None
    assert info.startup_logs == ['booting', 'ready']


def test_app_run_info_truncates_long_logs_and_message() -> None:
    long_logs = '\n'.join(f'line-{i}' for i in range(100))
    long_message = 'x' * (_APP_RUN_MESSAGE_LIMIT + 1000)
    info = AppRunInfo.from_api_response(
        _make_failed_app_run(
            startupLogs=long_logs,
            failureReason={'reason': 'StartupProbeFailed', 'message': long_message},
        )
    )
    # The error tail is what matters: keep the LAST lines/chars, marking message truncation with an ellipsis.
    assert info.startup_logs == [f'line-{i}' for i in range(100 - _APP_RUN_LOG_LINES, 100)]
    assert len(info.failure_message) == _APP_RUN_MESSAGE_LIMIT
    assert info.failure_message.startswith('…')


@pytest.mark.asyncio
async def test_fetch_latest_run_returns_newest_run_info(mocker, mcp_context_client: Context) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.data_science_client.list_app_runs = mocker.AsyncMock(return_value=[_make_failed_app_run()])

    info = await _fetch_latest_run(keboola_client, 'app-prod-1')

    keboola_client.data_science_client.list_app_runs.assert_awaited_once_with('app-prod-1', limit=1)
    assert info is not None
    assert info.failure_reason == 'ConfigDecryptionFailed'


@pytest.mark.asyncio
@pytest.mark.parametrize('list_app_runs_behavior', ['empty', 'raises'])
async def test_fetch_latest_run_degrades_to_none(
    mocker, mcp_context_client: Context, list_app_runs_behavior: str
) -> None:
    """Diagnostics must not break the detail fetch: no runs (brand-new app) and a failing runs
    endpoint (older DSAPI) both surface as `None` rather than an error."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    if list_app_runs_behavior == 'empty':
        mock = mocker.AsyncMock(return_value=[])
    else:
        mock = mocker.AsyncMock(side_effect=RuntimeError('404 Not Found'))
    keboola_client.data_science_client.list_app_runs = mock

    assert await _fetch_latest_run(keboola_client, 'app-prod-1') is None


@pytest.mark.asyncio
async def test_get_data_apps_detail_includes_last_run_failure(mocker, mcp_context_client: Context) -> None:
    """The detail path must surface the latest AppRun's failure so agents can diagnose apps whose
    setup-phase failures (e.g. invalid secrets) produce no container logs at all."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    prod_cfg_id = 'cfg-prod-1'
    prod = _make_python_js_prod_data_app(configuration_id=prod_cfg_id, state='stopped')
    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=[])
    keboola_client.data_science_client.list_app_runs = mocker.AsyncMock(return_value=[_make_failed_app_run()])
    keboola_client.data_science_client.list_runtimes = mocker.AsyncMock(return_value=_RUNTIMES)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=prod))
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_logs', mocker.AsyncMock(return_value=[]))

    result = await get_data_apps(ctx=mcp_context_client, configuration_ids=[prod_cfg_id])

    assert len(result.data_apps) == 1
    detail = result.data_apps[0]
    assert isinstance(detail, DataApp)
    assert detail.deployment_info is not None
    last_run = detail.deployment_info.last_run
    assert last_run is not None
    assert last_run.state == 'failed'
    assert last_run.failure_reason == 'ConfigDecryptionFailed'
    assert last_run.failure_message == 'failed to decrypt key "#API_KEY"'
    # The image catalog and the image the app runs come with the detail.
    assert detail.available_images == _PYTHON_JS_IMAGES
    assert detail.deployment_info.image == _PYTHON_JS_IMAGES[0]


# ===== Tests for get_data_app_preview_link =====

import logging  # noqa: E402

import httpx  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from keboola_mcp_server.clients.base import RawKeboolaClient  # noqa: E402
from keboola_mcp_server.clients.data_science import AppPreviewLinkResponse, DataScienceClient  # noqa: E402
from keboola_mcp_server.tools.data_apps import (  # noqa: E402
    DataAppPreviewLinkOutput,
    get_data_app_preview_link,
)

_PREVIEW_TOKEN = 'SENTINEL-PREVIEW-TOKEN-4f2a'
_PREVIEW_URL = f'https://draft-cfg-draft-1-123.hub.test.keboola.com/_proxy/preview#t={_PREVIEW_TOKEN}'


def _make_streamlit_data_app() -> DataApp:
    return DataApp(
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


def _preview_app(kind: str) -> DataApp:
    if kind == 'prod':
        return _make_python_js_prod_data_app()
    if kind == 'streamlit':
        return _make_streamlit_data_app()
    return _make_python_js_draft_data_app(
        configuration_id='cfg-draft-1', data_app_id='app-draft-1', parent_configuration_id='cfg-prod-1'
    )


def _sandboxes_error(status: int, message: str) -> dict:
    return {'error': message, 'code': status, 'exceptionId': 'exc-1', 'status': 'error', 'context': {}}


def _http_error(status: int, body: dict | str) -> httpx.HTTPStatusError:
    """Builds the exception exactly as the real client raises it (via `_raise_for_status`)."""
    request = httpx.Request('POST', 'https://data-science.test.keboola.com/apps/app-1/preview-link')
    if isinstance(body, dict):
        response = httpx.Response(status, json=body, request=request)
    else:
        response = httpx.Response(status, text=body, request=request)
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        RawKeboolaClient._raise_for_status(response)
    return exc_info.value


@pytest.mark.asyncio
async def test_get_data_app_preview_link_malformed_response_does_not_leak_link(
    mocker,
    mcp_context_client: Context,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=_preview_app('draft'))
    )
    keboola_client.data_science_client = DataScienceClient.create('https://api.example.com', token=None)
    keboola_client.data_science_client.post = mocker.AsyncMock(return_value={'url': _PREVIEW_URL})

    with pytest.raises(Exception) as exc_info:
        await get_data_app_preview_link(ctx=mcp_context_client, configuration_id='cfg-draft-1')

    assert _PREVIEW_TOKEN not in str(exc_info.value)
    chain: list[BaseException] = []
    pending: list[BaseException | None] = [exc_info.value]
    while pending:
        current = pending.pop()
        if current is None or any(current is seen for seen in chain):
            continue
        chain.append(current)
        pending.extend([current.__cause__, current.__context__])
    for link in chain:
        assert _PREVIEW_TOKEN not in str(link)
        assert _PREVIEW_TOKEN not in repr(link)
        if isinstance(link, ValidationError):
            assert _PREVIEW_TOKEN not in repr(link.errors())
    assert _PREVIEW_TOKEN not in caplog.text
    assert _PREVIEW_TOKEN not in str(keboola_client.storage_client.trigger_event.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('app_kind', 'data_app_id'),
    [('draft', 'app-draft-1'), ('streamlit', 'app-sl-1')],
)
async def test_get_data_app_preview_link_returns_link_without_leaking_it(
    mocker,
    mcp_context_client: Context,
    caplog: pytest.LogCaptureFixture,
    app_kind: str,
    data_app_id: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    data_app = _preview_app(app_kind)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=data_app))
    keboola_client.data_science_client.create_app_preview_link = mocker.AsyncMock(
        return_value=AppPreviewLinkResponse(url=_PREVIEW_URL, link_expires_at='2026-09-27T10:01:00+00:00')
    )

    result = await get_data_app_preview_link(ctx=mcp_context_client, configuration_id=data_app.configuration_id)

    assert isinstance(result, DataAppPreviewLinkOutput)
    assert result.url == _PREVIEW_URL
    assert result.link_expires_at == '2026-09-27T10:01:00+00:00'
    keboola_client.data_science_client.create_app_preview_link.assert_awaited_once_with(data_app_id)
    assert _PREVIEW_TOKEN not in repr(result)
    assert _PREVIEW_TOKEN not in str(result)
    assert _PREVIEW_TOKEN not in caplog.text
    keboola_client.storage_client.trigger_event.assert_awaited_once()
    assert _PREVIEW_TOKEN not in str(keboola_client.storage_client.trigger_event.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('app_kind', 'status', 'body', 'expected_match', 'not_expected'),
    [
        (
            'draft',
            400,
            _sandboxes_error(400, 'App "app-draft-1" is not in dev mode.'),
            r'not running in dev mode.*`deploy_data_app`.*mode="dev", configuration_id="cfg-draft-1"\)',
            None,
        ),
        (
            'prod',
            400,
            _sandboxes_error(400, 'App "app-prod-1" is not in dev mode.'),
            r'is a production app.*Do not switch it to dev mode.*parent_configuration_id="cfg-prod-1"',
            'Deploy it in dev mode first',
        ),
        (
            'streamlit',
            400,
            _sandboxes_error(400, 'App "app-sl-1" is not in dev mode.'),
            r'"cfg-sl-1" \(streamlit\) is not in dev mode.*Tell the user; do not change its deploy mode',
            'mode="dev"',
        ),
        (
            'draft',
            400,
            _sandboxes_error(400, 'App "app-draft-1" has no URL yet.'),
            r'has no URL yet.*wait until `get_data_apps` reports it running',
            'not running in dev mode',
        ),
        (
            'draft',
            403,
            _sandboxes_error(403, "You don't have access to the resource."),
            r'The token cannot manage data app "cfg-draft-1", so it cannot get a preview link',
            'project_id',
        ),
        (
            'draft',
            400,
            _sandboxes_error(400, "Token is not authorized to manage app 'app-draft-1', app is from different project"),
            r'The token cannot manage data app "cfg-draft-1", so it cannot get a preview link',
            'project_id',
        ),
        (
            'draft',
            404,
            _sandboxes_error(404, 'App "app-draft-1" not found.'),
            r'"cfg-draft-1" \(data app ID "app-draft-1"\) was not found by the data-science service',
            None,
        ),
        (
            'draft',
            404,
            _sandboxes_error(404, 'No route found for "POST http://localhost/apps/app-draft-1/preview-link"'),
            r'not available on this Keboola stack yet',
            'was not found by the data-science service',
        ),
        (
            'draft',
            503,
            _sandboxes_error(503, 'App preview links are not configured.'),
            r"not configured on this Keboola stack.*Do not try the app's password login",
            None,
        ),
    ],
    ids=[
        'draft_not_dev',
        'prod_not_dev',
        'streamlit_not_dev',
        'no_url',
        'forbidden',
        'forbidden_400',
        'not_found',
        'no_route',
        'not_configured',
    ],
)
async def test_get_data_app_preview_link_maps_errors(
    mocker,
    mcp_context_client: Context,
    app_kind: str,
    status: int,
    body: dict,
    expected_match: str,
    not_expected: str | None,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    data_app = _preview_app(app_kind)
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=data_app))
    keboola_client.data_science_client.create_app_preview_link = mocker.AsyncMock(side_effect=_http_error(status, body))

    with pytest.raises(ValueError, match=expected_match) as exc_info:
        await get_data_app_preview_link(ctx=mcp_context_client, configuration_id=data_app.configuration_id)

    assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
    if not_expected:
        assert not_expected not in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('status', 'body'),
    [
        (400, _sandboxes_error(400, 'Some other validation error.')),
        (503, '<html><body>503 Service Temporarily Unavailable</body></html>'),
        (500, _sandboxes_error(500, 'Internal Server Error occurred.')),
    ],
    ids=['other_400', 'html_503', 'server_error'],
)
async def test_get_data_app_preview_link_passes_through_unmapped_errors(
    mocker,
    mcp_context_client: Context,
    status: int,
    body: dict | str,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    mocker.patch(
        'keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(return_value=_preview_app('draft'))
    )
    error = _http_error(status, body)
    keboola_client.data_science_client.create_app_preview_link = mocker.AsyncMock(side_effect=error)

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await get_data_app_preview_link(ctx=mcp_context_client, configuration_id='cfg-draft-1')

    assert exc_info.value is error


@pytest.mark.asyncio
async def test_get_data_app_preview_link_does_not_mint_when_lookup_fails(
    mocker,
    mcp_context_client: Context,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    error = _http_error(404, {'error': 'Configuration cfg-missing not found', 'code': 404})
    mocker.patch('keboola_mcp_server.tools.data_apps._fetch_data_app', mocker.AsyncMock(side_effect=error))
    keboola_client.data_science_client.create_app_preview_link = mocker.AsyncMock()

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await get_data_app_preview_link(ctx=mcp_context_client, configuration_id='cfg-missing')

    assert exc_info.value is error
    keboola_client.data_science_client.create_app_preview_link.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('status', [400, 403], ids=['400', '403'])
async def test_get_data_app_preview_link_maps_other_project_refusal_from_lookup(
    mocker,
    mcp_context_client: Context,
    status: int,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(
        return_value={
            'id': 'cfg-draft-1',
            'name': 'draft',
            'description': 'draft',
            'configuration': {'parameters': {'id': 'app-draft-1'}},
            'version': 1,
        }
    )
    keboola_client.data_science_client.get_data_app = mocker.AsyncMock(
        side_effect=_http_error(
            status,
            _sandboxes_error(
                status, "Token is not authorized to manage app 'app-draft-1', app is from different project"
            ),
        )
    )
    keboola_client.data_science_client.create_app_preview_link = mocker.AsyncMock()

    with pytest.raises(ValueError, match=r'The token cannot manage data app "cfg-draft-1"') as exc_info:
        await get_data_app_preview_link(ctx=mcp_context_client, configuration_id='cfg-draft-1')

    assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)
    assert 'project_id' not in str(exc_info.value)
    keboola_client.data_science_client.create_app_preview_link.assert_not_called()


def test_get_data_app_preview_link_description_allows_a_browser_run_from_the_shell():
    """Kai has no browser tool, only a headless browser CLI in its shell; the text must not rule that out."""
    doc = get_data_app_preview_link.__doc__ or ''
    url_description = DataAppPreviewLinkOutput.model_fields['url'].description or ''
    for text in (doc, url_description):
        assert 'your browser tool' not in text
        assert 'other than your browser' not in text
    assert 'a headless browser CLI run from your shell' in doc
    assert 'chrome-devtools-axi' not in doc, 'the description must not assume a specific browser tool'
    assert 'any command other than the one that opens the browser' in doc
    assert 'Do not fetch it with an HTTP client or curl' in doc
