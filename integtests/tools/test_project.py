import pytest
from fastmcp import Context

from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.project import ProjectInfo, get_project_info


@pytest.mark.asyncio
async def test_get_project_info(mcp_context: Context, keboola_project) -> None:
    result = await get_project_info(mcp_context)

    assert isinstance(result, ProjectInfo)
    assert str(result.project_id) == str(keboola_project.project_id)
    assert isinstance(result.project_name, str)
    assert isinstance(result.organization_id, (str, int))
    assert isinstance(result.project_description, str)
    assert isinstance(result.sql_dialect, str)
    assert result.sql_dialect in {'Snowflake', 'BigQuery'}
    assert isinstance(result.links, list)
    assert result.links, 'Links list should not be empty.'
    for link in result.links:
        assert isinstance(link, Link)
        assert link.type in {'ui-detail', 'ui-dashboard', 'docs'}
        assert isinstance(link.title, str)
        assert isinstance(link.url, str)
