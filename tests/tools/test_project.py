import time
from types import SimpleNamespace

import httpx
import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, MetadataField, ServerRuntimeInfo
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import ServerState, SessionStateMiddleware
from keboola_mcp_server.scope import (
    OAUTH_SESSION_ID_KEY,
    SCOPE_KEY,
    SCOPE_TOKEN_ARG,
    SessionScope,
    resolve_scope_binding_aad,
    resolve_scope_key,
)
from keboola_mcp_server.tools.project import (
    ProjectInfo,
    _get_toolset_restrictions,
    _parent_subject_token,
    _resolve_branch_context,
    get_accessible_projects,
    get_project_info,
    set_project_scope,
    update_project_description,
)
from keboola_mcp_server.workspace import WorkspaceManager

STACK = 'https://connection.test.keboola.com'


@pytest.mark.parametrize(
    ('branch_id', 'project_base_url'),
    [
        (None, f'{STACK}/admin/projects/proj-123'),
        ('456', f'{STACK}/admin/projects/proj-123/branch/456'),
    ],
)
def test_get_project_links(branch_id: str | None, project_base_url: str) -> None:
    links_manager = ProjectLinksManager(base_url=STACK, project_id='proj-123', branch_id=branch_id)

    assert links_manager.get_project_links() == [
        Link.detail(title='Project Dashboard', url=f'{project_base_url}/'),
        Link.detail(title='Project Settings', url=f'{project_base_url}/project-settings'),
    ]


@pytest.mark.parametrize(
    ('role', 'expected_substring', 'expect_none'),
    [
        # readonly: all writes blocked
        ('readonly', 'read-only tools are available', False),
        ('READONLY', 'read-only tools are available', False),
        # regular roles: no schedules
        ('guest', 'can manage flows', False),
        ('guest', 'cannot set their schedules', False),
        # empty role: no schedules
        ('', 'cannot set their schedules', False),
        # admin/share: no restrictions
        ('admin', None, True),
        ('share', None, True),
    ],
)
def test_get_toolset_restrictions(role: str, expected_substring: str | None, expect_none: bool) -> None:
    result = _get_toolset_restrictions(role)
    if expect_none:
        assert result is None
    else:
        assert result is not None
        assert expected_substring in result
        if role:
            assert role.lower() in result
        else:
            assert 'unknown' in result


_DEFAULT_BRANCH = {'id': 123, 'name': 'Main', 'isDefault': True}
_DEV_BRANCH = {'id': 456, 'name': 'feature-x', 'isDefault': False}


@pytest.mark.parametrize(
    (
        'token_role',
        'expected_user_role',
        'expected_restriction_substrings',
        'restriction_is_none',
        'sql_dialect',
        'expected_fqn_example',
        'client_branch_id',
        'expected_branch_id',
        'expected_branch_name',
        'expected_is_dev',
    ),
    [
        # developer role on default branch
        (
            'developer',
            'developer',
            ['cannot set their schedules'],
            False,
            'Snowflake',
            '"DATABASE"."SCHEMA"."TABLE"',
            None,
            123,
            'Main',
            False,
        ),
        # guest role on default branch
        (
            'guest',
            'guest',
            ['cannot set their schedules'],
            False,
            'BigQuery',
            '`project`.`dataset`.`table`',
            None,
            123,
            'Main',
            False,
        ),
        # no role on default branch
        (
            None,
            'unknown',
            ['cannot set their schedules'],
            False,
            'Snowflake',
            '"DATABASE"."SCHEMA"."TABLE"',
            None,
            123,
            'Main',
            False,
        ),
        # readonly role on default branch
        (
            'readonly',
            'readonly',
            ['read-only'],
            False,
            'BigQuery',
            '`project`.`dataset`.`table`',
            None,
            123,
            'Main',
            False,
        ),
        # admin role on default branch
        ('admin', 'admin', [], True, 'Snowflake', '"DATABASE"."SCHEMA"."TABLE"', None, 123, 'Main', False),
        # admin role on a dev branch — exercises the dev-branch resolution path
        ('admin', 'admin', [], True, 'Snowflake', '"DATABASE"."SCHEMA"."TABLE"', '456', 456, 'feature-x', True),
    ],
)
@pytest.mark.asyncio
async def test_get_project_info(
    mocker: MockerFixture,
    mcp_context_client: Context,
    token_role: str | None,
    expected_user_role: str,
    expected_restriction_substrings: list[str],
    restriction_is_none: bool,
    sql_dialect: str,
    expected_fqn_example: str,
    client_branch_id: str | None,
    expected_branch_id: int,
    expected_branch_name: str,
    expected_is_dev: bool,
) -> None:
    admin_data = {'role': token_role} if token_role is not None else {}
    token_data = {
        'owner': {'id': 'proj-123', 'name': 'Test Project'},
        'organization': {'id': 'org-456'},
        'admin': admin_data,
    }
    metadata = [
        {'key': MetadataField.PROJECT_DESCRIPTION, 'value': 'A test project.'},
        {'key': 'other', 'value': 'ignore'},
    ]
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = client_branch_id
    keboola_client.storage_client.verify_token = mocker.AsyncMock(return_value=token_data)
    keboola_client.storage_client.branch_metadata_get = mocker.AsyncMock(return_value=metadata)
    keboola_client.storage_client.branches_list = mocker.AsyncMock(return_value=[_DEFAULT_BRANCH, _DEV_BRANCH])
    workspace_manager = WorkspaceManager.from_state(mcp_context_client.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)
    workspace_manager.get_workspace_id = mocker.AsyncMock(return_value=789)

    project_id = 'proj-123'
    base_url = 'https://connection.test.keboola.com'
    links = [Link(type='ui-detail', title='Project Dashboard', url=f'{base_url}/admin/projects/{project_id}')]
    mock_links_manager = mocker.Mock()
    mock_links_manager.get_project_links.return_value = links
    mocker.patch(
        'keboola_mcp_server.tools.project.ProjectLinksManager.from_client',
        new=mocker.AsyncMock(return_value=mock_links_manager),
    )

    result = await get_project_info(mcp_context_client)

    assert isinstance(result, ProjectInfo)
    assert result.project_id == 'proj-123'
    assert result.project_name == 'Test Project'
    assert result.organization_id == 'org-456'
    assert result.project_description == 'A test project.'
    assert result.sql_dialect == sql_dialect
    assert result.workspace_id == 789
    assert result.links == links
    assert result.user_role == expected_user_role
    assert expected_fqn_example in result.llm_instruction
    assert result.branch_id == expected_branch_id
    assert result.branch_name == expected_branch_name
    assert result.is_development_branch is expected_is_dev

    if restriction_is_none:
        assert result.toolset_restrictions is None
    else:
        assert result.toolset_restrictions is not None
        for substring in expected_restriction_substrings:
            assert substring in result.toolset_restrictions


@pytest.mark.parametrize(
    ('client_branch_id', 'branches', 'expected_id', 'expected_name', 'expected_is_dev'),
    [
        # default branch resolution
        (None, [_DEFAULT_BRANCH, _DEV_BRANCH], 123, 'Main', False),
        # dev branch resolution by id (string vs int safe)
        ('456', [_DEFAULT_BRANCH, _DEV_BRANCH], 456, 'feature-x', True),
        (456, [_DEFAULT_BRANCH, _DEV_BRANCH], 456, 'feature-x', True),
        # defensive: branch id present but not in list (should not happen, but covered)
        ('999', [_DEFAULT_BRANCH], '999', 'unknown', True),
        # defensive: empty list when on default
        (None, [], 'default', 'unknown', False),
    ],
)
@pytest.mark.asyncio
async def test_resolve_branch_context(
    mocker: MockerFixture,
    client_branch_id: str | int | None,
    branches: list[dict],
    expected_id: str | int,
    expected_name: str,
    expected_is_dev: bool,
) -> None:
    client = mocker.Mock()
    client.branch_id = client_branch_id
    client.storage_client.branches_list = mocker.AsyncMock(return_value=branches)

    branch_id, branch_name, is_dev = await _resolve_branch_context(client)

    assert branch_id == expected_id
    assert branch_name == expected_name
    assert is_dev is expected_is_dev


@pytest.mark.parametrize(
    'description',
    [
        'New description',
        '',
    ],
)
@pytest.mark.asyncio
async def test_update_project_description(
    mocker: MockerFixture,
    mcp_context_client: Context,
    description: str,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.branch_metadata_update = mocker.AsyncMock(
        return_value=[{'key': 'KBC.projectDescription', 'value': description}]
    )

    result = await update_project_description(mcp_context_client, description=description)

    assert result is None
    keboola_client.storage_client.branch_metadata_update.assert_called_once_with(
        {MetadataField.PROJECT_DESCRIPTION: description}
    )


# --- multi-project scope tools (PSGO-261 increment 2) ---


def _prep_client(mcp_context_client: Context, mocker: MockerFixture, *, bearer: str | None = 'kbc_at_parent'):
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.bearer_token = bearer
    client.storage_api_url = STACK
    mocker.patch(
        'keboola_mcp_server.tools.project.get_access_token',
        new=mocker.AsyncMock(return_value='kbc_at_parent'),
    )
    return client


@pytest.mark.asyncio
async def test_parent_subject_token_ignores_local_store_when_deployed(mocker: MockerFixture) -> None:
    # On the deployed (multi-tenant) server, the local PKCE credential store must never be consulted
    # -- it holds no session for this request's caller, and since it's shared across every concurrent
    # request on the pod, reading (or refresh-writing) it here would risk leaking one tenant's session
    # into another's. Only the request's own bearer token may be used.
    mocker.patch('keboola_mcp_server.tools.project.deployed_sa_token_path', return_value='/var/run/secrets/token')
    get_access_token = mocker.patch(
        'keboola_mcp_server.tools.project.get_access_token',
        new=mocker.AsyncMock(return_value='kbc_at_wrong_tenant'),
    )
    client = mocker.Mock()
    client.bearer_token = 'Bearer kbc_at_this_request'

    token = await _parent_subject_token(client)

    assert token == 'kbc_at_this_request'
    get_access_token.assert_not_called()


@pytest.mark.asyncio
async def test_get_accessible_projects(mcp_context_client: Context, mocker: MockerFixture) -> None:
    _prep_client(mcp_context_client, mocker)
    introspection = SimpleNamespace(
        user_email='m@k.com',
        projects=[SimpleNamespace(id=18, name='A', role='admin'), SimpleNamespace(id=83, name='B', role='admin')],
    )
    introspect = mocker.patch(
        'keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection)
    )
    # Per-project SQL dialect + organization are resolved via a token verify narrowed by
    # X-KBC-ProjectId; mock that.
    mocker.patch(
        'keboola_mcp_server.tools.project.ServerState.from_context',
        return_value=SimpleNamespace(config=Config(), runtime_info=ServerRuntimeInfo(transport='stdio')),
    )
    verify_info = {18: ('BigQuery', 'org-1', 'Org One'), 83: ('Snowflake', 'org-2', 'Org Two')}
    mocker.patch(
        'keboola_mcp_server.tools.project._project_verify_info',
        new=mocker.AsyncMock(side_effect=lambda _ss, _url, _tok, pid: (pid, *verify_info[pid])),
    )

    # No scope confirmed yet.
    result = await get_accessible_projects(mcp_context_client)

    introspect.assert_awaited_once_with(STACK, subject_token='kbc_at_parent')
    assert result.user_email == 'm@k.com'
    assert [(p.id, p.name, p.role, p.sql_dialect, p.organization_id, p.organization_name) for p in result.projects] == [
        (18, 'A', 'admin', 'BigQuery', 'org-1', 'Org One'),
        (83, 'B', 'admin', 'Snowflake', 'org-2', 'Org Two'),
    ]
    assert result.scoped_project_ids is None
    assert result.read_only is None
    assert result.base_instructions is None  # not requested
    assert result.scope_token is None
    assert all(not p.in_scope for p in result.projects)

    # Once scoped, the current scope is surfaced on the projects and at the top level. On this
    # (stdio) transport ctx.session.state persists across requests, so no scope_token is needed.
    mcp_context_client.session.state[SCOPE_KEY] = SessionScope(project_ids=[83], read_only=True, confirmed=True)
    result = await get_accessible_projects(mcp_context_client)
    assert result.scoped_project_ids == [83]
    assert result.read_only is True
    assert [(p.id, p.in_scope) for p in result.projects] == [(18, False), (83, True)]
    assert result.scope_token is None


@pytest.mark.asyncio
async def test_get_accessible_projects_llm_instructions_grouped_by_dialect(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    _prep_client(mcp_context_client, mocker)
    introspection = SimpleNamespace(
        user_email='m@k.com',
        projects=[
            SimpleNamespace(id=18, name='A', role='admin'),
            SimpleNamespace(id=86, name='B', role='admin'),
            SimpleNamespace(id=95, name='C', role='admin'),
        ],
    )
    mocker.patch('keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection))
    mocker.patch('keboola_mcp_server.tools.project.ServerState.from_context', return_value=mocker.Mock())
    dialects = {18: 'BigQuery', 86: 'BigQuery', 95: 'Snowflake'}
    mocker.patch(
        'keboola_mcp_server.tools.project._project_verify_info',
        new=mocker.AsyncMock(side_effect=lambda _ss, _url, _tok, pid: (pid, dialects[pid], None, None)),
    )

    result = await get_accessible_projects(mcp_context_client, with_llm_instruction=True)

    assert result.base_instructions is not None
    # One group per distinct dialect, projects deduplicated into their dialect group (no per-project copies).
    groups = {g.sql_dialect: g.project_ids for g in result.base_instructions}
    assert groups == {'BigQuery': [18, 86], 'Snowflake': [95]}
    assert all(g.instructions for g in result.base_instructions)


@pytest.mark.asyncio
async def test_get_accessible_projects_unknown_dialect_omits_snowflake_guidance(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    # A project whose dialect can't be resolved (None) must NOT fall back to Snowflake guidance —
    # that would mislead the assistant into Snowflake-specific SQL for a non-Snowflake project.
    from keboola_mcp_server.resources.prompts import get_project_system_prompt

    _prep_client(mcp_context_client, mocker)
    introspection = SimpleNamespace(user_email='m@k.com', projects=[SimpleNamespace(id=42, name='X', role='admin')])
    mocker.patch('keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection))
    mocker.patch('keboola_mcp_server.tools.project.ServerState.from_context', return_value=mocker.Mock())
    mocker.patch(
        'keboola_mcp_server.tools.project._project_verify_info',
        new=mocker.AsyncMock(side_effect=lambda _ss, _url, _tok, pid: (pid, None, None, None)),
    )

    result = await get_accessible_projects(mcp_context_client, with_llm_instruction=True)

    assert result.base_instructions is not None
    (group,) = result.base_instructions
    assert group.sql_dialect is None
    # The unknown-dialect group gets the no-dialect prompt, not the Snowflake one.
    assert group.instructions == get_project_system_prompt('')
    assert group.instructions != get_project_system_prompt('Snowflake')


@pytest.mark.asyncio
async def test_get_accessible_projects_logs_dialect_failure_with_traceback(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    # A per-project dialect-resolution failure is swallowed (best-effort), but must still log with
    # exc_info so the traceback isn't lost.
    _prep_client(mcp_context_client, mocker)
    introspection = SimpleNamespace(user_email='m@k.com', projects=[SimpleNamespace(id=42, name='X', role='admin')])
    mocker.patch('keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection))
    mocker.patch('keboola_mcp_server.tools.project.ServerState.from_context', return_value=mocker.Mock())
    mocker.patch(
        'keboola_mcp_server.tools.project._project_verify_info',
        new=mocker.AsyncMock(side_effect=RuntimeError('verify failed')),
    )
    log_warning = mocker.patch('keboola_mcp_server.tools.project.LOG.warning')

    result = await get_accessible_projects(mcp_context_client)

    assert result.projects[0].sql_dialect is None
    log_warning.assert_called_once()
    assert log_warning.call_args.kwargs.get('exc_info') is not None


@pytest.mark.asyncio
async def test_set_project_scope_subset_exchanges_and_stores(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    _prep_client(mcp_context_client, mocker)
    minted = SimpleNamespace(access_token='kbc_at_scoped', expires_at=time.time() + 3600, read_only=False)
    exch = mocker.patch(
        'keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted)
    )

    result = await set_project_scope(mcp_context_client, project_ids=[18, 83])

    exch.assert_awaited_once_with(STACK, subject_token='kbc_at_parent', project_ids=[18, 83], read_only=False)
    assert result.project_ids == [18, 83]
    scope = mcp_context_client.session.state[SCOPE_KEY]
    assert scope.scoped_token == 'kbc_at_scoped'
    assert scope.project_ids == [18, 83]
    # mcp_context_client's runtime is transport='stdio', which persists ctx.session.state across
    # requests (ServerRuntimeInfo.session_state_persists) -- no scope_token needed to keep it in effect.
    assert result.scope_token is None
    assert 'persists this scope server-side' in result.llm_instruction


@pytest.mark.asyncio
async def test_set_project_scope_returns_scope_token_when_session_does_not_persist(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    # Deployed default: stateless-http streamable-http, a fresh ctx.session per request -- nothing
    # server-side survives between calls, so the caller must resend scope_token.
    mcp_context_client.request_context.lifespan_context = ServerState(
        Config(), ServerRuntimeInfo(transport='http-compat/streamable-http', stateless_http=True)
    )
    _prep_client(mcp_context_client, mocker)
    minted = SimpleNamespace(access_token='kbc_at_scoped', expires_at=time.time() + 3600, read_only=False)
    mocker.patch('keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted))

    result = await set_project_scope(mcp_context_client, project_ids=[18, 83])

    scope = mcp_context_client.session.state[SCOPE_KEY]
    assert result.scope_token is not None
    assert SessionScope.from_token(result.scope_token, resolve_scope_key(Config())) == scope
    assert 'does not remember this scope' in result.llm_instruction


@pytest.mark.asyncio
async def test_set_project_scope_binds_scope_token_to_caller_on_deployed_server(
    mcp_context_client: Context, mocker: MockerFixture, monkeypatch
) -> None:
    # The replay fix: on a deployed server, the returned scope_token must only decrypt alongside
    # the same caller's own bearer token (client.bearer_token, the un-narrowed whole-stack subject
    # token -- see PSGO-280: it must NOT be client.legacy_storage_token, which on a project-scoped
    # session can be a server-minted, per-project token) it was minted for -- see
    # resolve_scope_binding_aad. A different caller's token must fail, even with the right key.
    monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
    mcp_context_client.request_context.lifespan_context = ServerState(
        Config(), ServerRuntimeInfo(transport='http-compat/streamable-http', stateless_http=True)
    )
    _prep_client(mcp_context_client, mocker)
    minted = SimpleNamespace(access_token='kbc_at_scoped', expires_at=time.time() + 3600, read_only=False)
    mocker.patch('keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted))

    result = await set_project_scope(mcp_context_client, project_ids=[18, 83])

    scope = mcp_context_client.session.state[SCOPE_KEY]
    key = resolve_scope_key(Config())
    assert SessionScope.from_token(result.scope_token, key, aad=resolve_scope_binding_aad('kbc_at_parent')) == scope
    with pytest.raises(Exception, match='.+'):
        SessionScope.from_token(result.scope_token, key, aad=resolve_scope_binding_aad('kbc_at_someone_else'))


@pytest.mark.asyncio
async def test_set_project_scope_token_round_trips_through_read_scope_from_request(
    mcp_context_client: Context, mocker: MockerFixture, monkeypatch
) -> None:
    """Regression (PSGO-280): the scope_token minted by set_project_scope must actually decrypt
    on the very next request via `SessionStateMiddleware._read_scope_from_request` -- not just
    against a hand-picked AAD in isolation (see the test above). That method seals with
    `config.storage_token` -- the caller's raw presented credential -- which on a deployed,
    project-scoped programmatic session is NOT the same string as `client.legacy_storage_token`
    (the server-minted, per-project legacy Storage token). Sealing with the wrong one here would
    silently evaporate every confirmed scope on the next call.
    """
    monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
    mcp_context_client.request_context.lifespan_context = ServerState(
        Config(), ServerRuntimeInfo(transport='http-compat/streamable-http', stateless_http=True)
    )
    client = _prep_client(mcp_context_client, mocker)
    # Simulate a resolved session: the caller presented 'kbc_at_parent', but the active project's
    # legacy_storage_token has already been swapped for a server-minted per-project token by
    # KeboolaClient.create -- a different string from the caller's own credential.
    client.legacy_storage_token = 'legacy-storage-token-for-active-project'
    minted = SimpleNamespace(access_token='kbc_at_scoped', expires_at=time.time() + 3600, read_only=False)
    mocker.patch('keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted))

    result = await set_project_scope(mcp_context_client, project_ids=[18, 83])
    scope = mcp_context_client.session.state[SCOPE_KEY]

    # The next request presents the same raw credential the caller always had.
    config = Config(storage_token='kbc_at_parent')
    context = SimpleNamespace(
        message=SimpleNamespace(name='get_tables', arguments={SCOPE_TOKEN_ARG: result.scope_token}),
        method='tools/call',
    )
    assert SessionStateMiddleware._read_scope_from_request(context, config) == scope


@pytest.mark.asyncio
async def test_set_project_scope_persists_to_db_and_omits_scope_token_for_oauth_session(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    # An OAuth-authenticated session (OAUTH_SESSION_ID_KEY present) persists the scope on its
    # oauth_sessions row instead of minting a scope_token -- the opaque OAuth access token already
    # resolves back to that row on every subsequent call, so there's nothing left to resend.
    _prep_client(mcp_context_client, mocker)
    mcp_context_client.session.state[OAUTH_SESSION_ID_KEY] = 'session-1'
    session_store = mocker.Mock()
    session_store.update_scope = mocker.AsyncMock()
    mcp_context_client.request_context.lifespan_context = ServerState(
        config=Config(), runtime_info=ServerRuntimeInfo(transport='stdio'), session_store=session_store
    )
    minted = SimpleNamespace(access_token='kbc_at_scoped', expires_at=1234.0, read_only=False)
    mocker.patch('keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted))

    result = await set_project_scope(mcp_context_client, project_ids=[18, 83])

    session_store.update_scope.assert_awaited_once()
    call = session_store.update_scope.await_args
    assert call.args == ('session-1',)
    assert call.kwargs['project_ids'] == [18, 83]
    assert call.kwargs['scoped_token'] == 'kbc_at_scoped'
    assert call.kwargs['confirmed'] is True
    # Nothing left for the caller to resend -- the server persisted the scope itself.
    assert result.scope_token is None
    assert 'no need to resend' in result.llm_instruction


@pytest.mark.asyncio
async def test_set_project_scope_persists_to_kai_scope_store_and_omits_scope_token(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    # A deployed, non-OAuth, programmatic-token session (Kai) persists the confirmed scope to
    # kai_scope_store, keyed by (conversation_id, introspected user id) -- pat_token_support/RFC.md
    # "Kai (header-token) session-scope persistence". No scope_token is needed afterward.
    _prep_client(mcp_context_client, mocker)
    mocker.patch('keboola_mcp_server.tools.project.deployed_sa_token_path', return_value='/var/run/secrets/token')
    kai_scope_store = mocker.Mock()
    kai_scope_store.upsert = mocker.AsyncMock()
    mcp_context_client.request_context.lifespan_context = ServerState(
        config=Config(),
        runtime_info=ServerRuntimeInfo(transport='http-compat/streamable-http'),
        kai_scope_store=kai_scope_store,
    )
    introspection = SimpleNamespace(user_id=42, user_email='kai@keboola.com', projects=[])
    mocker.patch('keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection))
    minted = SimpleNamespace(access_token='kbc_at_scoped', expires_at=1234.0, read_only=False)
    mocker.patch('keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted))

    result = await set_project_scope(mcp_context_client, project_ids=[18, 83])

    kai_scope_store.upsert.assert_awaited_once_with(
        'convo-1234', 42, project_ids=[18, 83], read_only=False, confirmed=True
    )
    assert result.scope_token is None
    assert 'no need to resend' in result.llm_instruction


@pytest.mark.asyncio
async def test_set_project_scope_all_introspects_then_exchanges(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    _prep_client(mcp_context_client, mocker)
    introspection = SimpleNamespace(
        user_email=None,
        projects=[SimpleNamespace(id=18, name='A', role='admin'), SimpleNamespace(id=83, name='B', role='x')],
    )
    mocker.patch('keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection))
    minted = SimpleNamespace(access_token='kbc_at_all', expires_at=time.time() + 3600, read_only=False)
    exch = mocker.patch(
        'keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted)
    )

    result = await set_project_scope(mcp_context_client, project_ids=None)

    exch.assert_awaited_once_with(STACK, subject_token='kbc_at_parent', project_ids=[18, 83], read_only=False)
    assert result.project_ids == [18, 83]


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [400, 401, 403])
async def test_set_project_scope_reraises_client_error(
    mcp_context_client: Context, mocker: MockerFixture, status_code: int
) -> None:
    # A 400/401/403 from the exchange means bad input/auth, not an unavailable endpoint — it must
    # surface to the caller rather than silently downgrading to an unscoped whole-stack token.
    _prep_client(mcp_context_client, mocker)
    response = httpx.Response(status_code, request=httpx.Request('POST', 'https://x/v1/auth/pat/exchange'))
    mocker.patch(
        'keboola_mcp_server.tools.project.exchange_scoped_token',
        new=mocker.AsyncMock(side_effect=httpx.HTTPStatusError('bad', request=response.request, response=response)),
    )

    with pytest.raises(httpx.HTTPStatusError):
        await set_project_scope(mcp_context_client, project_ids=[18])


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [500, 502, 503])
async def test_set_project_scope_falls_back_on_server_error(
    mcp_context_client: Context, mocker: MockerFixture, status_code: int
) -> None:
    # A 5xx (endpoint unavailable) still falls back to the whole-stack token so scoping keeps working.
    _prep_client(mcp_context_client, mocker)
    response = httpx.Response(status_code, request=httpx.Request('POST', 'https://x/v1/auth/pat/exchange'))
    mocker.patch(
        'keboola_mcp_server.tools.project.exchange_scoped_token',
        new=mocker.AsyncMock(side_effect=httpx.HTTPStatusError('down', request=response.request, response=response)),
    )

    result = await set_project_scope(mcp_context_client, project_ids=[18])

    assert result.project_ids == [18]
    scope = mcp_context_client.session.state[SCOPE_KEY]
    assert scope.scoped_token is None


@pytest.mark.asyncio
async def test_set_project_scope_falls_back_on_network_error(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    _prep_client(mcp_context_client, mocker)
    mocker.patch(
        'keboola_mcp_server.tools.project.exchange_scoped_token',
        new=mocker.AsyncMock(side_effect=httpx.ConnectTimeout('timed out')),
    )

    result = await set_project_scope(mcp_context_client, project_ids=[18])

    assert result.project_ids == [18]
    scope = mcp_context_client.session.state[SCOPE_KEY]
    assert scope.scoped_token is None


@pytest.mark.asyncio
async def test_set_project_scope_read_only_fallback_notes_local_only_enforcement(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    # Security hardening RFC increment: when the exchange fails, read_only has no server-side
    # backing (no scoped_token) -- the caller must be told explicitly, not left assuming the same
    # guarantee the success path gets.
    _prep_client(mcp_context_client, mocker)
    mocker.patch(
        'keboola_mcp_server.tools.project.exchange_scoped_token',
        new=mocker.AsyncMock(side_effect=httpx.ConnectTimeout('timed out')),
    )

    result = await set_project_scope(mcp_context_client, project_ids=[18], read_only=True)

    assert result.read_only is True
    assert 'enforced by this server only' in result.llm_instruction


@pytest.mark.asyncio
async def test_set_project_scope_read_only_success_omits_local_only_note(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    _prep_client(mcp_context_client, mocker)
    minted = SimpleNamespace(access_token='kbc_at_scoped', expires_at=time.time() + 3600, read_only=True)
    mocker.patch('keboola_mcp_server.tools.project.exchange_scoped_token', new=mocker.AsyncMock(return_value=minted))

    result = await set_project_scope(mcp_context_client, project_ids=[18], read_only=True)

    assert result.read_only is True
    assert 'enforced by this server only' not in result.llm_instruction


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'bearer',
    [
        None,  # no bearer at all
        'legacy-sapi-token-123',  # a non-programmatic bearer must not be accepted either
        'Bearer kbc_at_prefixed',  # accepted, but exercises the strip_bearer normalization path
    ],
    ids=['no_bearer', 'non_programmatic_bearer', 'bearer_prefixed'],
)
async def test_scope_requires_programmatic_token(
    mcp_context_client: Context, mocker: MockerFixture, bearer: str | None
) -> None:
    _prep_client(mcp_context_client, mocker, bearer=bearer)
    mocker.patch('keboola_mcp_server.tools.project.get_access_token', new=mocker.AsyncMock(side_effect=RuntimeError))
    if bearer == 'Bearer kbc_at_prefixed':
        introspection = SimpleNamespace(user_email='m@k.com', projects=[])
        introspect = mocker.patch(
            'keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection)
        )
        mocker.patch('keboola_mcp_server.tools.project.ServerState.from_context', return_value=mocker.Mock())
        await get_accessible_projects(mcp_context_client)
        # The inbound bearer's `Bearer ` scheme must be stripped before use as a subject token.
        introspect.assert_awaited_once_with(STACK, subject_token='kbc_at_prefixed')
    else:
        with pytest.raises(ValueError, match='programmatic token'):
            await get_accessible_projects(mcp_context_client)


@pytest.mark.asyncio
async def test_set_project_scope_rejects_explicit_empty_list(
    mcp_context_client: Context, mocker: MockerFixture
) -> None:
    # An explicit [] must NOT be treated like null (all projects) — it's almost certainly a mistake.
    _prep_client(mcp_context_client, mocker)
    with pytest.raises(ValueError, match='non-empty'):
        await set_project_scope(mcp_context_client, project_ids=[])
