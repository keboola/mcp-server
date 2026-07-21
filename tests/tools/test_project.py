import time
from types import SimpleNamespace

import httpx
import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.mcp import SCOPE_KEY, SessionScope
from keboola_mcp_server.tools.project import (
    ProjectInfo,
    _get_toolset_restrictions,
    _resolve_branch_context,
    get_accessible_projects,
    get_project_info,
    set_project_scope,
    update_project_description,
)
from keboola_mcp_server.workspace import WorkspaceManager

STACK = 'https://connection.test.keboola.com'


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
async def test_get_accessible_projects(mcp_context_client: Context, mocker: MockerFixture) -> None:
    _prep_client(mcp_context_client, mocker)
    introspection = SimpleNamespace(
        user_email='m@k.com',
        projects=[SimpleNamespace(id=18, name='A', role='admin'), SimpleNamespace(id=83, name='B', role='admin')],
    )
    introspect = mocker.patch(
        'keboola_mcp_server.tools.project.introspect_token', new=mocker.AsyncMock(return_value=introspection)
    )
    # Per-project SQL dialect is resolved via a token verify narrowed by X-KBC-ProjectId; mock that.
    mocker.patch('keboola_mcp_server.tools.project.ServerState.from_context', return_value=mocker.Mock())
    dialects = {18: 'BigQuery', 83: 'Snowflake'}
    mocker.patch(
        'keboola_mcp_server.tools.project._project_sql_dialect',
        new=mocker.AsyncMock(side_effect=lambda _ss, _tok, pid: (pid, dialects[pid])),
    )

    # No scope confirmed yet.
    result = await get_accessible_projects(mcp_context_client)

    introspect.assert_awaited_once_with(STACK, subject_token='kbc_at_parent')
    assert result.user_email == 'm@k.com'
    assert [(p.id, p.name, p.role, p.sql_dialect) for p in result.projects] == [
        (18, 'A', 'admin', 'BigQuery'),
        (83, 'B', 'admin', 'Snowflake'),
    ]
    assert result.scoped_project_ids is None
    assert result.read_only is None
    assert result.base_instructions is None  # not requested
    assert all(not p.in_scope for p in result.projects)

    # Once scoped, the current scope is surfaced on the projects and at the top level.
    mcp_context_client.session.state[SCOPE_KEY] = SessionScope(project_ids=[83], read_only=True, confirmed=True)
    result = await get_accessible_projects(mcp_context_client)
    assert result.scoped_project_ids == [83]
    assert result.read_only is True
    assert [(p.id, p.in_scope) for p in result.projects] == [(18, False), (83, True)]


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
        'keboola_mcp_server.tools.project._project_sql_dialect',
        new=mocker.AsyncMock(side_effect=lambda _ss, _tok, pid: (pid, dialects[pid])),
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
        'keboola_mcp_server.tools.project._project_sql_dialect',
        new=mocker.AsyncMock(side_effect=lambda _ss, _tok, pid: (pid, None)),
    )

    result = await get_accessible_projects(mcp_context_client, with_llm_instruction=True)

    assert result.base_instructions is not None
    (group,) = result.base_instructions
    assert group.sql_dialect is None
    # The unknown-dialect group gets the no-dialect prompt, not the Snowflake one.
    assert group.instructions == get_project_system_prompt('')
    assert group.instructions != get_project_system_prompt('Snowflake')


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
async def test_scope_requires_programmatic_token(mcp_context_client: Context, mocker: MockerFixture) -> None:
    _prep_client(mcp_context_client, mocker, bearer=None)
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
