import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.project import ProjectInfo, get_project_info
from keboola_mcp_server.workspace import WorkspaceManager


@pytest.mark.asyncio
async def test_get_project_info(mocker: MockerFixture, mcp_context_client: Context) -> None:
    """
    Test get_project_info returns correct ProjectInfo with mocked dependencies.
    :return: None
    """
    token_data = {
        'owner': {'id': 'proj-123', 'name': 'Test Project'},
        'organization': {'id': 'org-456'},
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

    # Call the tool
    result = await get_project_info(mcp_context_client)
    assert isinstance(result, ProjectInfo)
    assert result.project_id == 'proj-123'
    assert result.project_name == 'Test Project'
    assert result.organization_id == 'org-456'
    assert result.project_description == 'A test project.'
    assert result.sql_dialect == 'Snowflake'
    assert result.links == links
