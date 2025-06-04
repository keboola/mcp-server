import pytest

from keboola_mcp_server.tools.workspace import ProjectManager


@pytest.mark.asyncio
async def test_project_manager_get_project(project_manager: ProjectManager, keboola_project) -> None:
    """
    Test that ProjectManager.get_project returns the correct project id.

    :param project_manager: The ProjectManager instance.
    :param keboola_project: The keboola_project fixture providing the expected project id.
    :return: None
    """
    project_id = await project_manager.get_project_id()
    assert project_id == str(keboola_project.project_id)
