"""Integration tests that run the SAME questions under different authentication (PSGO-261).

Instead of a separate PAT project pool, these reuse the existing project pool + per-project lock and
vary only the auth used to drive the MCP tools:

- ``sapi``       — the pool project's legacy Storage API token (today's single-project path).
- ``pat_single`` — a programmatic token (kbc_pat_/kbc_at_) scoped to the one locked pool project.
- ``pat_mpa``    — the same PAT scoped to two locked pool projects (fan-out), or one when the pool
                   has no second free project.

The PAT (INTEGTEST_STORAGE_PAT) must be a member of the pool projects; PAT modes skip when it is not
set. Lock + cleanup always use the project's SAPI token, so the PAT only needs read access here.
"""

import logging
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP

from integtests.conftest import INTEGTEST_CLIENT_INFO
from integtests.project_lock import AcquiredProject
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.project import AccessibleProjects, ProjectScope

LOG = logging.getLogger(__name__)

AUTH_MODES = ['sapi', 'pat_single', 'pat_mpa']


def _make_server(config: Config) -> FastMCP:
    server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(server, FastMCP)
    return server


@pytest_asyncio.fixture(params=AUTH_MODES)
async def auth_client(
    request: pytest.FixtureRequest,
    storage_api_url: str,
    storage_api_token: str,
    workspace_schema: str,
    project_lock: AcquiredProject,
) -> AsyncGenerator[tuple[str, Client, list[int]], None]:
    """Yields (auth_mode, ready-to-use Client, scoped_project_ids) for each auth mode.

    For PAT modes the client is already scoped (read-only) to the locked pool project(s), so the test
    body is identical across modes.
    """
    auth_mode = request.param
    primary_id = int(project_lock.endpoint.project_id)

    if auth_mode == 'sapi':
        config = Config(
            storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema
        )
        async with Client(_make_server(config), client_info=INTEGTEST_CLIENT_INFO) as client:
            yield auth_mode, client, [primary_id]
        return

    # PAT modes: skip when no programmatic token is configured.
    pat = request.getfixturevalue('programmatic_token')
    target_ids = [primary_id]
    if auth_mode == 'pat_mpa':
        second = request.getfixturevalue('mpa_second_project')
        if second is not None:
            target_ids.append(int(second.endpoint.project_id))

    config = Config(storage_api_url=storage_api_url, storage_token=pat)
    async with Client(_make_server(config), client_info=INTEGTEST_CLIENT_INFO) as client:
        scope = ProjectScope.model_validate(
            (
                await client.call_tool('set_project_scope', {'project_ids': target_ids, 'read_only': True})
            ).structured_content
        )
        assert set(scope.project_ids) == set(target_ids)
        yield auth_mode, client, target_ids


@pytest.mark.asyncio
async def test_read_buckets_across_auth_modes(auth_client: tuple[str, Client, list[int]]):
    """The same read question (list buckets) works under sapi / pat_single / pat_mpa."""
    auth_mode, client, target_ids = auth_client
    response = await client.call_tool('get_buckets', {})
    sc = response.structured_content
    assert sc is not None, f'{auth_mode}: expected structured content'
    # Both single-project (raw) and MPA (merged fan-out) shapes expose a top-level "buckets" list.
    assert 'buckets' in sc, f'{auth_mode}: expected a buckets list, got keys {list(sc)}'
    LOG.info(f'{auth_mode}: get_buckets returned {len(sc["buckets"])} bucket(s) across {target_ids}')


@pytest.mark.asyncio
async def test_pat_accessible_projects_enrichment(
    programmatic_token: str,
    storage_api_url: str,
):
    """PAT bootstrap: per-project SQL dialect + dialect-grouped base instructions."""
    config = Config(storage_api_url=storage_api_url, storage_token=programmatic_token)
    async with Client(_make_server(config), client_info=INTEGTEST_CLIENT_INFO) as client:
        result = AccessibleProjects.model_validate(
            (await client.call_tool('get_accessible_projects', {'with_llm_instruction': True})).structured_content
        )

    assert result.projects, 'the PAT should reach at least one project'
    assert all(p.sql_dialect in ('Snowflake', 'BigQuery') for p in result.projects)
    assert result.base_instructions, 'base_instructions expected when with_llm_instruction=true'
    assert {g.sql_dialect for g in result.base_instructions} == {p.sql_dialect for p in result.projects}
