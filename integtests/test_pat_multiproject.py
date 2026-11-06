"""Integration tests that run the SAME questions under different programmatic-token scopes (PSGO-261, AI-3774).

They reuse the existing project pool + per-project lock and vary only the scope of the service user's token:

- ``pat_single`` — a programmatic token scoped to the one locked pool project.
- ``pat_mpa``    — the whole-stack service-user token scoped (set_project_scope) to two locked pool projects
                   (fan-out), or one when the pool has no second free project.

The service user (see integtests/service_login.py) must be a member of the pool projects. Lock + cleanup use
the project's own Storage token, so the service user only needs read access here.
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

AUTH_MODES = ['pat_single', 'pat_mpa']


def _make_server(config: Config) -> FastMCP:
    server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(server, FastMCP)
    return server


@pytest_asyncio.fixture(params=AUTH_MODES)
async def auth_client(
    request: pytest.FixtureRequest,
    storage_api_url: str,
    service_session_token: str,
    project_lock: AcquiredProject,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[tuple[str, Client, list[int]], None]:
    """Yields (auth_mode, ready-to-use Client, scoped_project_ids) for each auth mode.

    For PAT modes the client is already scoped (read-only) to the locked pool project(s), so the test
    body is identical across modes.
    """
    # `env_init` pins a single-project token/project in the environment, which would override these configs
    monkeypatch.delenv('KBC_PROJECT_ID', raising=False)
    auth_mode = request.param
    primary_id = int(project_lock.endpoint.project_id)

    target_ids = [primary_id]
    if auth_mode == 'pat_mpa':
        second = request.getfixturevalue('mpa_second_project')
        if second is not None:
            target_ids.append(int(second.endpoint.project_id))

    config = Config(storage_api_url=storage_api_url, bearer_token=service_session_token)
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
    """The same read question (list buckets) works under pat_single / pat_mpa."""
    auth_mode, client, target_ids = auth_client
    response = await client.call_tool('get_buckets', {})
    sc = response.structured_content
    assert sc is not None, f'{auth_mode}: expected structured content'
    # Both single-project (raw) and MPA (merged fan-out) shapes expose a top-level "buckets" list.
    assert 'buckets' in sc, f'{auth_mode}: expected a buckets list, got keys {list(sc)}'
    LOG.info(f'{auth_mode}: get_buckets returned {len(sc["buckets"])} bucket(s) across {target_ids}')


@pytest.mark.asyncio
async def test_pat_accessible_projects_enrichment(
    service_session_token: str,
    storage_api_url: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """PAT bootstrap: per-project SQL dialect + dialect-grouped base instructions."""
    monkeypatch.delenv('KBC_PROJECT_ID', raising=False)
    config = Config(storage_api_url=storage_api_url, bearer_token=service_session_token)
    async with Client(_make_server(config), client_info=INTEGTEST_CLIENT_INFO) as client:
        result = AccessibleProjects.model_validate(
            (await client.call_tool('get_accessible_projects', {'with_llm_instruction': True})).structured_content
        )

    assert result.projects, 'the service user should reach at least one project'
    assert all(p.sql_dialect in ('Snowflake', 'BigQuery') for p in result.projects)
    assert result.base_instructions, 'base_instructions expected when with_llm_instruction=true'
    assert {g.sql_dialect for g in result.base_instructions} == {p.sql_dialect for p in result.projects}
