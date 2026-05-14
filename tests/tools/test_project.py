import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.project import (
    ProjectInfo,
    _get_toolset_restrictions,
    _resolve_branch_context,
    get_project_info,
    update_project_description,
)
from keboola_mcp_server.workspace import WorkspaceManager


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
