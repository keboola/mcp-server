from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, call

import httpx
import pytest

from keboola_mcp_server.clients.base import RawKeboolaClient
from keboola_mcp_server.clients.data_science import (
    AppGitRepoResponse,
    AppPreviewLinkResponse,
    AppRunResponse,
    CodeDataAppConfig,
    CreatedGitCredentialResponse,
    DataScienceClient,
    RuntimeResponse,
)


@pytest.mark.asyncio
async def test_tail_app_logs_with_lines_calls_get_text_with_lines() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    result = await client.tail_app_logs('app-123', since=None, lines=5)

    assert result == 'LOGS'
    client.get_text.assert_awaited_once_with(endpoint='v2/apps/app-123/logs/tail', params={'lines': 5})


@pytest.mark.asyncio
async def test_tail_app_logs_with_lines_minimum_enforced() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    _ = await client.tail_app_logs('app-123', since=None, lines=0)

    client.get_text.assert_awaited_once_with(endpoint='v2/apps/app-123/logs/tail', params={'lines': 1})


@pytest.mark.asyncio
async def test_tail_app_logs_with_since_calls_get_text_with_since_param() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get_text = AsyncMock(return_value='LOGS')  # type: ignore[assignment]

    since = datetime.now(timezone.utc) - timedelta(days=1)
    result = await client.tail_app_logs('app-xyz', since=since, lines=None)

    assert result == 'LOGS'
    client.get_text.assert_awaited_once_with(
        endpoint='v2/apps/app-xyz/logs/tail', params={'since': since.isoformat(timespec='microseconds')}
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
        'type': 'python-js',
        'url': None,
        'hasManagedGitRepo': True,
        'production': {'desiredDeploymentId': None, 'actualDeploymentId': None},
        'state': 'notDeployed',
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('app_type', 'use_managed_git_repo', 'expected_extra_payload'),
    [
        ('streamlit', False, {}),
        ('python-js', True, {'useManagedGitRepo': True}),
        # Dev twin path: python-js app with an external-git binding lives inside the configuration
        # body (`parameters.dataApp.git`), not as a top-level request field — so no extra payload
        # keys are expected.
        ('python-js', False, {}),
    ],
)
async def test_create_data_app_passes_type_and_managed_repo_flag(
    app_type: str,
    use_managed_git_repo: bool,
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
    )

    expected_payload = {
        'branchId': 'br-1',
        'name': 'Demo',
        'type': app_type,
        'description': 'desc',
        'config': config.model_dump(exclude_none=True, by_alias=True),
        **expected_extra_payload,
    }
    client.post.assert_awaited_once_with(endpoint='v2/apps', data=expected_payload)


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
async def test_deploy_data_app_creates_a_deployment_of_the_config_version() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.post = AsyncMock(  # type: ignore[assignment]
        return_value={'id': 'dep-1', 'configVersion': '42', 'createdAt': '2026-10-26T10:00:00+00:00'}
    )

    deployment = await client.deploy_data_app('app-123', '42')

    client.post.assert_awaited_once_with(endpoint='v2/apps/app-123/deployments', data={'configVersion': '42'})
    assert deployment.id == 'dep-1'
    assert deployment.config_version == '42'


@pytest.mark.asyncio
async def test_deploy_data_app_in_mode_patches_v1() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.patch = AsyncMock(return_value=_create_app_response())  # type: ignore[assignment]

    await client.deploy_data_app_in_mode('app-123', '42', 'dev')

    client.patch.assert_awaited_once_with(
        endpoint='apps/app-123',
        data={'desiredState': 'running', 'restartIfRunning': True, 'configVersion': '42', 'mode': 'dev'},
    )


@pytest.mark.asyncio
async def test_suspend_data_app_posts_to_suspend() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.post = AsyncMock(return_value=_create_app_response() | {'state': 'suspended'})  # type: ignore[assignment]

    await client.suspend_data_app('app-123')

    client.post.assert_awaited_once_with(endpoint='v2/apps/app-123/suspend', data={})


@pytest.mark.asyncio
async def test_get_data_app_parses_the_v2_record() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get = AsyncMock(  # type: ignore[assignment]
        return_value=_create_app_response()
        | {
            'state': 'running',
            'production': {
                'desiredDeploymentId': 'dep-2',
                'actualDeploymentId': 'dep-1',
                'desiredConfigVersion': '3',
                'actualConfigVersion': '2',
                'desiredCreatedAt': '2026-10-27T10:00:00Z',
            },
        }
    )

    app = await client.get_data_app('app-123')

    client.get.assert_awaited_once_with(endpoint='v2/apps/app-123')
    assert app.reported_state == 'starting'
    assert app.production.desired_deployment_id == 'dep-2'
    assert app.production.actual_deployment_id == 'dep-1'
    assert app.production.desired_config_version == '3'
    assert app.production.actual_config_version == '2'
    assert app.production.desired_created_at == datetime(2026, 10, 27, 10, tzinfo=timezone.utc)


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
        endpoint='v2/apps/app-123/git-repo/credentials',
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
    client.get.assert_awaited_once_with(endpoint='v2/apps/app-123/git-repo')


def test_app_git_repo_response_missing_managed_flag_defaults_to_none() -> None:
    """Fail-closed: a git-repo response without `isManagedGitRepo` yields None, not False, so
    callers gating on it (branch repoint) refuse rather than assume "external" on spec drift."""
    assert AppGitRepoResponse.model_validate({'httpsUrl': 'https://x/y.git'}).is_managed_git_repo is None


@pytest.mark.asyncio
async def test_list_runtimes_parses_catalog() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get = AsyncMock(  # type: ignore[assignment]
        return_value=[
            {
                'type': 'python-js',
                'description': 'Python 3.13 + Node.js 24',
                'isTypeDefault': False,
                'releaseDate': '2026-09-01T00:00:00+00:00',
                'endOfLifeDate': None,
                'imageName': 'keboola/data-app-python-js',
                'imageTag': '1.7.2_python-3.13_node-24',
            },
            {'type': 'python', 'description': 'Python 3.10', 'isTypeDefault': True, 'imageTag': None},
        ]
    )

    runtimes = await client.list_runtimes()

    client.get.assert_awaited_once_with(endpoint='runtimes')
    assert runtimes[0] == RuntimeResponse(
        type='python-js',
        description='Python 3.13 + Node.js 24',
        is_type_default=False,
        image_tag='1.7.2_python-3.13_node-24',
    )
    assert runtimes[1].image_tag is None


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


@pytest.mark.asyncio
async def test_list_app_runs_gets_expected_endpoint_and_parses_failure_reason() -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.get = AsyncMock(  # type: ignore[assignment]
        return_value=[
            {
                'id': 'run-2',
                'appId': 'app-123',
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
            },
            {
                'id': 'run-1',
                'appId': 'app-123',
                'state': 'finished',
                'createdAt': '2026-06-12T09:00:00+00:00',
                'startedAt': '2026-06-12T09:00:05+00:00',
                'stoppedAt': '2026-06-12T09:30:00+00:00',
                'startupLogs': 'booting\nready',
                'failureReason': None,
                'mode': 'prod',
            },
            {
                'id': 'run-0',
                'appId': 'app-123',
                'state': None,
                'createdAt': '2026-06-11T09:00:00+00:00',
                'stoppedAt': '2026-06-11T09:30:00+00:00',
            },
        ]
    )

    runs = await client.list_app_runs('app-123', limit=2)

    client.get.assert_awaited_once_with(endpoint='v2/apps/app-123/runs', params={'limit': 2, 'offset': 0})
    assert len(runs) == 3
    assert runs[2].state == 'finished'
    assert all(isinstance(run, AppRunResponse) for run in runs)
    assert runs[0].state == 'failed'
    assert runs[0].started_at is None
    assert runs[0].failure_reason is not None
    assert runs[0].failure_reason.reason == 'ConfigDecryptionFailed'
    assert runs[0].failure_reason.message == 'failed to decrypt key "#API_KEY"'
    assert runs[1].failure_reason is None
    assert runs[1].startup_logs == 'booting\nready'


@pytest.mark.asyncio
async def test_create_app_preview_link_posts_without_body_and_hides_url_in_repr() -> None:
    token = 'SENTINEL-PREVIEW-TOKEN'
    url = f'https://my-app-123.hub.example.com/_proxy/preview#t={token}'
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.post = AsyncMock(  # type: ignore[assignment]
        return_value={'url': url, 'linkExpiresAt': '2026-09-27T10:01:00+00:00'}
    )

    result = await client.create_app_preview_link('app-123')

    assert isinstance(result, AppPreviewLinkResponse)
    assert result.url == url
    assert result.link_expires_at == '2026-09-27T10:01:00+00:00'
    assert token not in repr(result)
    assert token not in str(result)
    client.post.assert_awaited_once_with(endpoint='apps/app-123/preview-link')


@pytest.mark.asyncio
async def test_create_app_preview_link_hides_url_in_validation_error() -> None:
    token = 'SENTINEL-PREVIEW-TOKEN-' + 'x' * 40 + 'TAIL42'
    url = f'https://my-app-123.hub.example.com/_proxy/preview#t={token}'
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.post = AsyncMock(return_value={'url': url})  # type: ignore[assignment]

    with pytest.raises(ValueError) as exc_info:
        await client.create_app_preview_link('app-123')

    assert token not in str(exc_info.value)
    assert token[-6:] not in str(exc_info.value)
    assert 'linkExpiresAt' in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def _dsapi_error(status: int, code: int | str | None) -> httpx.HTTPStatusError:
    """Builds the exception exactly as the real client raises it (via `_raise_for_status`)."""
    request = httpx.Request('DELETE', 'https://api.example.com/v2/apps/app-123')
    response = httpx.Response(status, json={'error': 'boom', 'code': code, 'exceptionId': 'exc-1'}, request=request)
    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        RawKeboolaClient._raise_for_status(response)
    return exc_info.value


_DEADLOCK = _dsapi_error(500, 1213)
_DEADLOCK_STR_CODE = _dsapi_error(500, '1213')
_OTHER_500 = _dsapi_error(500, 2000)
_NOT_FOUND = _dsapi_error(404, 404)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('delete_outcomes', 'expected_error', 'expected_sleeps'),
    [
        ([None], None, []),
        ([_DEADLOCK, None], None, [2.0]),
        ([_DEADLOCK_STR_CODE, _DEADLOCK, None], None, [2.0, 5.0]),
        # A 404 on a retry means the deadlocked attempt went through after all.
        ([_DEADLOCK, _NOT_FOUND], None, [2.0]),
        # A 404 on the first attempt is a real "no such app" and is raised as before.
        ([_NOT_FOUND], _NOT_FOUND, []),
        ([_OTHER_500], _OTHER_500, []),
        ([_DEADLOCK, _OTHER_500], _OTHER_500, [2.0]),
        ([_DEADLOCK, _DEADLOCK, _DEADLOCK_STR_CODE], _DEADLOCK_STR_CODE, [2.0, 5.0]),
    ],
    ids=[
        'success',
        'deadlock_then_success',
        'two_deadlocks_then_success',
        'deadlock_then_404',
        'first_404_raised',
        'other_500_not_retried',
        'deadlock_then_other_500',
        'deadlocks_exhausted',
    ],
)
async def test_delete_data_app_retries_dsapi_deadlock(
    mocker,
    delete_outcomes: list[httpx.HTTPStatusError | None],
    expected_error: httpx.HTTPStatusError | None,
    expected_sleeps: list[float],
) -> None:
    client = DataScienceClient.create('https://api.example.com', token=None)
    client.delete = AsyncMock(side_effect=delete_outcomes)  # type: ignore[assignment]
    sleep = mocker.patch('keboola_mcp_server.clients.data_science.asyncio.sleep', new=AsyncMock())

    if expected_error is None:
        await client.delete_data_app('app-123')
    else:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client.delete_data_app('app-123')
        assert exc_info.value is expected_error

    assert client.delete.await_args_list == [call(endpoint='v2/apps/app-123')] * len(delete_outcomes)
    assert sleep.await_args_list == [call(delay) for delay in expected_sleeps]
