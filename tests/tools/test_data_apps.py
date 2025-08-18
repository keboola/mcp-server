import base64
import os
from typing import cast

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.data_apps import (
    _QUERY_DATA_FUNCTION_CODE,
    _build_data_app_config,
    _contains_placeholder_named,
    _get_authorization,
    _get_data_app_slug,
    _get_secrets,
    _inject_query_to_source_code,
    _is_authorized,
    _update_existing_data_app_config,
)


def test_get_data_app_slug():
    assert _get_data_app_slug('My Cool App') == 'my-cool-app'
    assert _get_data_app_slug('App 123') == 'app-123'
    assert _get_data_app_slug('Weird!@# Name$$$') == 'weird-name'


def test_contains_placeholder_named():
    assert _contains_placeholder_named('Hello {NAME}', 'NAME') is True
    assert _contains_placeholder_named('Hello {OTHER}', 'NAME') is False
    assert _contains_placeholder_named('Hello {}', 'NAME') is False


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
    # Secrets merged: current implementation keeps existing values on key conflict
    assert new['parameters']['dataApp']['secrets']['FOO'] == 'old'
    assert new['parameters']['dataApp']['secrets']['NEW'] == 'y'
    assert new['parameters']['dataApp']['secrets']['KEEP'] == 'x'
    assert new['authorization'] == _get_authorization(False)


def test_get_secrets_encrypts_token_and_sets_metadata(mocker):
    # Deterministic salt
    # salt = os.urandom(32)
    # expected_seed = base64.urlsafe_b64encode(salt).decode()
    # mocker.patch('keboola_mcp_server.tools.data_apps.os.urandom', return_value=salt)

    class _StorageClient:
        base_api_url = 'https://example.com'
        branch_id = 'bid123'

    class _Client:
        token = 'TOKEN123'
        storage_client = _StorageClient()

    workspace_id = 'wid-1234'

    secrets = _get_secrets(cast(KeboolaClient, _Client()), workspace_id)

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
        salt=workspace_id.encode("utf-8"),
        iterations=390000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(seed))
    # Decrypt the token
    decrypted_token = Fernet(key).decrypt(encrypted_token).decode()
    assert decrypted_token == 'TOKEN123'
