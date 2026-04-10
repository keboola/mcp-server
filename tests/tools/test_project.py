import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.resources.prompts import get_project_system_prompt
from keboola_mcp_server.tools.project import (
    ProjectInfo,
    _get_toolset_restrictions,
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


@pytest.mark.parametrize(
    ('token_role', 'expected_user_role', 'expected_restriction_substrings', 'restriction_is_none'),
    [
        # developer role: schedules not available
        ('developer', 'developer', ['cannot set their schedules'], False),
        # guest role: schedules not available
        ('guest', 'guest', ['cannot set their schedules'], False),
        # no role: schedules not available
        (None, 'unknown', ['cannot set their schedules'], False),
        # readonly role: only read-only tools
        ('readonly', 'readonly', ['read-only'], False),
        # admin role: no restrictions
        ('admin', 'admin', [], True),
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
    keboola_client.storage_client.verify_token = mocker.AsyncMock(return_value=token_data)
    keboola_client.storage_client.branch_metadata_get = mocker.AsyncMock(return_value=metadata)
    workspace_manager = WorkspaceManager.from_state(mcp_context_client.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value='Snowflake')

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
    assert result.sql_dialect == 'Snowflake'
    assert result.links == links
    assert result.user_role == expected_user_role
    assert result.llm_instruction == get_project_system_prompt()

    if restriction_is_none:
        assert result.toolset_restrictions is None
    else:
        assert result.toolset_restrictions is not None
        for substring in expected_restriction_substrings:
            assert substring in result.toolset_restrictions


@pytest.mark.asyncio
async def test_update_project_description(
    mocker: MockerFixture,
    mcp_context_client: Context,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.branch_metadata_update = mocker.AsyncMock(
        return_value=[{'key': 'KBC.projectDescription', 'value': 'New description'}]
    )

    result = await update_project_description(mcp_context_client, description='New description')

    assert result is None
    keboola_client.storage_client.branch_metadata_update.assert_called_once_with(
        {MetadataField.PROJECT_DESCRIPTION: 'New description'}
    )


@pytest.mark.asyncio
async def test_update_project_description_empty(
    mocker: MockerFixture,
    mcp_context_client: Context,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.branch_metadata_update = mocker.AsyncMock(
        return_value=[{'key': 'KBC.projectDescription', 'value': ''}]
    )

    result = await update_project_description(mcp_context_client, description='')

    assert result is None
    keboola_client.storage_client.branch_metadata_update.assert_called_once_with(
        {MetadataField.PROJECT_DESCRIPTION: ''}
    )
