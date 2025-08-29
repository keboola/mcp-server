import base64
from typing import Literal, cast

import pytest
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from fastmcp import Context

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.data_apps import (
    _QUERY_DATA_FUNCTION_CODE,
    DataApp,
    DataAppSummary,
    _build_data_app_config,
    _get_authorization,
    _get_data_app_slug,
    _get_secrets,
    _inject_query_to_source_code,
    _is_authorized,
    _update_existing_data_app_config,
    deploy_data_app,
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
        is_authorized=True,
        parameters={},
        authorization={},
        state='test',
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
    assert _is_authorized(_get_authorization(True)) is True
    assert _is_authorized(_get_authorization(False)) is False


def test_inject_query_to_source_code_when_already_included():
    source_code = f"""prelude{_QUERY_DATA_FUNCTION_CODE}postlude"""
    result = _inject_query_to_source_code(source_code)
    assert result == source_code


def test_inject_query_to_source_code_with_markers():
    src = (
        'import pandas as pd\n\n'
        '### INJECTED_CODE ###\n'
        '# will be replaced\n'
        '### END_OF_INJECTED_CODE ###\n\n'
        "print('hello')\n"
    )
    result = _inject_query_to_source_code(src)

    assert result.startswith('import pandas as pd')
    assert _QUERY_DATA_FUNCTION_CODE in result
    assert result.endswith("print('hello')\n")


def test_inject_query_to_source_code_with_placeholder():
    src = 'header\n{QUERY_DATA_FUNCTION}\nfooter\n'
    result = _inject_query_to_source_code(src)

    # Injected once via format(), original source (with placeholder) appended afterwards
    assert _QUERY_DATA_FUNCTION_CODE in result
    assert '{QUERY_DATA_FUNCTION}' not in result


def test_inject_query_to_source_code_default_path():
    src = "print('x')\n"
    result = _inject_query_to_source_code(src)
    assert result.startswith(_QUERY_DATA_FUNCTION_CODE)
    assert result.endswith(src)


def test_build_data_app_config_merges_defaults_and_secrets():
    name = 'My App'
    src = "print('hello')"
    pkgs = ['pandas']
    secrets = {'FOO': 'bar'}

    config = _build_data_app_config(name, src, pkgs, True, secrets)

    params = config['parameters']
    assert params['dataApp']['slug'] == 'my-app'
    assert params['script'] == [src]
    # Default packages are included and deduplicated
    assert 'pandas' in params['packages']
    assert 'httpx' in params['packages']
    assert 'cryptography' in params['packages']
    # Secrets carried over
    assert params['dataApp']['secrets'] == secrets
    # Authorization reflects flag
    assert config['authorization'] == _get_authorization(True)


def test_update_existing_data_app_config_merges_and_preserves_existing_on_conflict():
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
        authorize_with_password=False,
        secrets={'FOO': 'new', 'NEW': 'y'},
    )

    assert new['parameters']['dataApp']['slug'] == 'new-name'
    assert new['parameters']['script'] == ['new-code']
    # Removed previous packages
    assert 'numpy' not in new['parameters']['packages']
    # Packages combined with defaults
    assert 'pandas' in new['parameters']['packages']
    assert 'httpx' in new['parameters']['packages']
    assert 'cryptography' in new['parameters']['packages']
    assert new['parameters']['dataApp']['secrets']['FOO'] == 'new'
    assert new['parameters']['dataApp']['secrets']['NEW'] == 'y'
    assert new['parameters']['dataApp']['secrets']['KEEP'] == 'x'
    assert new['authorization'] == _get_authorization(False)


def test_get_secrets_encrypts_token_and_sets_metadata(mocker):

    keboola_client = mocker.Mock(KeboolaClient)
    keboola_client.storage_api_url = 'https://example.com'
    keboola_client.token = 'TOKEN123'
    keboola_client.branch_id = 'bid123'

    workspace_id = 'wid-1234'

    secrets = _get_secrets(keboola_client, workspace_id)

    assert set(secrets.keys()) == {
        'BRANCH_ID',
        'WORKSPACE_ID',
        'STORAGE_API_URL',
        '#STORAGE_API_TOKEN',
        '#SAPI_RANDOM_SEED',
    }
    assert secrets['BRANCH_ID'] == 'bid123'
    assert secrets['WORKSPACE_ID'] == 'wid-1234'
    assert secrets['STORAGE_API_URL'] == 'https://example.com'

    # Get the seed (decode it)
    seed = base64.urlsafe_b64decode(secrets['#SAPI_RANDOM_SEED'].encode())
    # Get the encrypted token (decode it)
    encrypted_token = base64.urlsafe_b64decode(secrets['#STORAGE_API_TOKEN'].encode())
    # Derive the key from the seed
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,  # Fernet requires 32-byte keys
        salt=workspace_id.encode('utf-8'),
        iterations=390000,
        backend=default_backend(),
    )
    key = base64.urlsafe_b64encode(kdf.derive(seed))
    # Decrypt the token
    decrypted_token = Fernet(key).decrypt(encrypted_token).decode()
    assert decrypted_token == 'TOKEN123'


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
