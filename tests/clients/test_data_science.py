from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from keboola_mcp_server.clients.data_science import (
    AppGitRepoResponse,
    CodeDataAppConfig,
    CreatedGitCredentialResponse,
    DataScienceClient,
)


@pytest.mark.asyncio
async def test_tail_app_logs_with_lines_calls_get_text_with_lines() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    result = await client.tail_app_logs('app-123', since=None, lines=5)

    assert result == 'LOGS'
    client.get_text.assert_awaited_once_with(endpoint='apps/app-123/logs/tail', params={'lines': 5})


@pytest.mark.asyncio
async def test_tail_app_logs_with_lines_minimum_enforced() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    _ = await client.tail_app_logs('app-123', since=None, lines=0)

    client.get_text.assert_awaited_once_with(endpoint='apps/app-123/logs/tail', params={'lines': 1})


@pytest.mark.asyncio
async def test_tail_app_logs_with_since_calls_get_text_with_since_param() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    since = datetime.now(timezone.utc) - timedelta(days=1)
    result = await client.tail_app_logs('app-xyz', since=since, lines=None)

    assert result == 'LOGS'
    client.get_text.assert_awaited_once_with(
        endpoint='apps/app-xyz/logs/tail', params={'since': since.isoformat(timespec='microseconds')}
    )


@pytest.mark.asyncio
async def test_tail_app_logs_raises_when_both_since_and_lines_provided() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)

    with pytest.raises(ValueError, match='You cannot use both "since" and "lines"'):
        await client.tail_app_logs('app-123', since=datetime.now(timezone.utc), lines=10)


@pytest.mark.asyncio
async def test_tail_app_logs_raises_when_neither_param_provided() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)

    with pytest.raises(ValueError, match='Either "since" or "lines" must be provided.'):
        await client.tail_app_logs('app-123', since=None, lines=None)


# ---------- Tests for python-js / managed-git-repo support ----------


def _code_app_config() -> CodeDataAppConfig:
    return CodeDataAppConfig(
        parameters=CodeDataAppConfig.Parameters(
            auto_suspend_after_seconds=900,
            data_app=CodeDataAppConfig.Parameters.DataApp(slug='my-app'),
        ),
        runtime=CodeDataAppConfig.Runtime(
            image=CodeDataAppConfig.Runtime.Image(version='dev-1.0.0'),
        ),
    )


def _create_app_response(app_id: str = 'app-123', config_id: str = 'cfg-456') -> dict:
    return {
        'id': app_id,
        'projectId': 'p-1',
        'componentId': 'keboola.data-apps',
        'branchId': None,
        'configId': config_id,
        'configVersion': '1',
        'type': 'python-js',
        'state': 'created',
        'desiredState': 'created',
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('app_type', 'use_managed_git_repo', 'existing_repo_url', 'expected_extra_payload'),
    [
        ('streamlit', False, None, {}),
        ('python-js', True, None, {'useManagedGitRepo': True}),
        ('python-js', False, None, {}),  # python-js without managed repo (unusual but allowed)
        (
            'python-js',
            True,
            'git@managed:org/repo.git',
            {'useManagedGitRepo': True, 'existingRepoUrl': 'git@managed:org/repo.git'},
        ),
    ],
)
async def test_create_data_app_passes_type_and_managed_repo_flag(
    app_type: str,
    use_managed_git_repo: bool,
    existing_repo_url: str | None,
    expected_extra_payload: dict,
) -> None:
    client = DataScienceClient.create('https://api.example.com', token=None, branch_id='br-1')
    client.post = AsyncMock(return_value=_create_app_response())  # type: ignore[assignment]

    config = _code_app_config()
    _ = await client.create_data_app(
        name='Demo',
        description='desc',
        configuration=config,
        app_type=app_type,
        use_managed_git_repo=use_managed_git_repo,
        existing_repo_url=existing_repo_url,
    )

    expected_payload = {
        'branchId': 'br-1',
        'name': 'Demo',
        'type': app_type,
        'description': 'desc',
        'config': config.model_dump(exclude_none=True, by_alias=True),
        **expected_extra_payload,
    }
    client.post.assert_awaited_once_with(endpoint='apps', data=expected_payload)


@pytest.mark.asyncio
async def test_create_data_app_defaults_to_streamlit_without_managed_repo_flag() -> None:
    """Backwards compatibility: a call with no app_type/use_managed_git_repo behaves like before."""
    client = DataScienceClient.create('https://api.example.com', token=None, branch_id='br-1')
    client.post = AsyncMock(return_value=_create_app_response())  # type: ignore[assignment]
    config = _code_app_config()

    _ = await client.create_data_app(name='Demo', description='desc', configuration=config)

    sent = client.post.await_args.kwargs['data']
    assert sent['type'] == 'streamlit'
    assert 'useManagedGitRepo' not in sent


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('config_version', 'mode', 'branch', 'expected_extra'),
    [
        ('42', None, None, {'configVersion': '42'}),  # Streamlit
        (None, 'dev', None, {'mode': 'dev'}),  # python-js dev preview from main
        (None, 'production', None, {'mode': 'production'}),
        (None, None, None, {}),  # bare deploy (python-js without explicit mode)
        ('5', 'dev', None, {'configVersion': '5', 'mode': 'dev'}),  # both
        (None, 'dev', 'feature-x', {'mode': 'dev', 'branch': 'feature-x'}),  # python-js dev twin on a branch
    ],
)
async def test_deploy_data_app_payload_with_mode_and_optional_config_version(
    config_version: str | None,
    mode: str | None,
    branch: str | None,
    expected_extra: dict,
) -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.patch = AsyncMock(return_value=_create_app_response())  # type: ignore[assignment]

    _ = await client.deploy_data_app('app-123', config_version, mode=mode, branch=branch)

    expected_payload = {
        'desiredState': 'running',
        'restartIfRunning': True,
        'updateDependencies': False,
        **expected_extra,
    }
    client.patch.assert_awaited_once_with(endpoint='apps/app-123', data=expected_payload)


@pytest.mark.parametrize('permissions', ['readWrite', 'readOnly'])
@pytest.mark.asyncio
async def test_create_app_git_credential_posts_to_expected_endpoint(permissions: str) -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.post = AsyncMock(  # type: ignore[assignment]
        return_value={
            'id': 'cred-1',
            'type': 'http_token',
            'name': '',
            'permissions': permissions,
            'ownerAdminId': 'admin-1',
            'createdAt': '2026-05-13T00:00:00Z',
            'secret': 'one-time-token-xyz',
        }
    )

    result = await client.create_app_git_credential('app-123', permissions=permissions)

    assert isinstance(result, CreatedGitCredentialResponse)
    assert result.id == 'cred-1'
    assert result.type == 'http_token'
    assert result.permissions == permissions
    assert result.secret == 'one-time-token-xyz'
    client.post.assert_awaited_once_with(
        endpoint='apps/app-123/git-repo/credentials',
        data={'type': 'http_token', 'permissions': permissions},
    )


@pytest.mark.asyncio
async def test_get_app_git_repo_returns_urls() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get = AsyncMock(  # type: ignore[assignment]
        return_value={
            'sshUrl': 'git@managed.repo:org/app.git',
            'httpsUrl': 'https://managed.repo/org/app.git',
            'isManagedGitRepo': True,
        }
    )

    result = await client.get_app_git_repo('app-123')

    assert isinstance(result, AppGitRepoResponse)
    assert result.ssh_url == 'git@managed.repo:org/app.git'
    assert result.https_url == 'https://managed.repo/org/app.git'
    assert result.is_managed_git_repo is True
    client.get.assert_awaited_once_with(endpoint='apps/app-123/git-repo')


def test_code_data_app_config_serializes_to_expected_shape() -> None:
    """CodeDataAppConfig must match the data-science API payload exactly (aliased keys)."""
    config = _code_app_config()
    payload = config.model_dump(exclude_none=True, by_alias=True)
    assert payload == {
        'parameters': {
            'autoSuspendAfterSeconds': 900,
            'dataApp': {'slug': 'my-app'},
        },
        'runtime': {'image': {'version': 'dev-1.0.0'}},
    }
