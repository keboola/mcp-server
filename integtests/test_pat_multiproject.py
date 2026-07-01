"""Integration tests for the multi-project PAT flow (PSGO-261).

These exercise the real introspect + scoped-exchange + fan-out path, which only activates for a
Keboola programmatic token (kbc_pat_/kbc_at_) in local (non-deployed) mode. They are gated on the
INTEGTEST_STORAGE_PAT secret via the `programmatic_token` fixture and skip when it is absent, so CI
stays green until the token is wired in.
"""

import logging

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP

from integtests.conftest import INTEGTEST_CLIENT_INFO
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.project import AccessibleProjects, ProjectScope

LOG = logging.getLogger(__name__)


@pytest.fixture
def pat_mcp_server(programmatic_token: str, programmatic_token_url: str) -> FastMCP:
    # No workspace_schema and no project_id → the server auto-leases a multi-project scope from the
    # PAT introspection (local programmatic mode).
    config = Config(storage_api_url=programmatic_token_url, storage_token=programmatic_token)
    server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(server, FastMCP)
    return server


@pytest_asyncio.fixture
async def pat_mcp_client(pat_mcp_server: FastMCP):
    async with Client(pat_mcp_server, client_info=INTEGTEST_CLIENT_INFO) as client:
        yield client


@pytest.mark.asyncio
async def test_pat_get_accessible_projects(pat_mcp_client: Client):
    # get_accessible_projects is a bootstrap tool: callable before any scope is confirmed.
    response = await pat_mcp_client.call_tool('get_accessible_projects', {'with_llm_instruction': True})
    result = AccessibleProjects.model_validate(response.structured_content)

    assert result.projects, 'the PAT should reach at least one project'
    # Every project is enriched with its SQL dialect (derived from the per-project token verify).
    assert all(p.sql_dialect in ('Snowflake', 'BigQuery') for p in result.projects)
    # with_llm_instruction=True → base instructions grouped by dialect (one group per distinct dialect).
    assert result.base_instructions, 'base_instructions expected when with_llm_instruction=true'
    dialects_in_groups = {g.sql_dialect for g in result.base_instructions}
    dialects_in_projects = {p.sql_dialect for p in result.projects}
    assert dialects_in_groups == dialects_in_projects


@pytest.mark.asyncio
async def test_pat_scope_then_read_fans_out(pat_mcp_client: Client):
    # Discover reachable projects, then confirm a scope over all of them.
    accessible = AccessibleProjects.model_validate(
        (await pat_mcp_client.call_tool('get_accessible_projects', {})).structured_content
    )
    project_ids = [p.id for p in accessible.projects]

    scope_response = await pat_mcp_client.call_tool('set_project_scope', {'read_only': True})
    scope = ProjectScope.model_validate(scope_response.structured_content)
    assert set(scope.project_ids) == set(project_ids)
    assert scope.read_only is True
    assert scope.active_project_id == project_ids[0]

    # A read tool now runs against the confirmed scope instead of raising the ask-first gate error.
    buckets_response = await pat_mcp_client.call_tool('get_buckets', {})
    assert buckets_response.structured_content is not None
