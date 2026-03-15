import pytest

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server


@pytest.fixture
def mcp_server():
    """Create a server instance for registration tests."""
    config = Config(storage_api_url='https://connection.keboola.com')
    runtime_info = ServerRuntimeInfo(transport='stdio')
    return create_server(config, runtime_info=runtime_info, custom_routes_handling='add')


@pytest.mark.asyncio
async def test_job_monitor_tools_registered(mcp_server):
    """Test that job_monitor and poll_job_monitor tools are registered."""
    tools = await mcp_server.get_tools()
    assert 'job_monitor' in tools
    assert 'poll_job_monitor' in tools


@pytest.mark.asyncio
async def test_job_monitor_resource_registered(mcp_server):
    """Test that the ui://keboola/job-monitor resource is registered."""
    resources = await mcp_server.get_resources()
    resource_keys = list(resources.keys())
    assert any('job-monitor' in str(k) for k in resource_keys), f'Expected job-monitor resource, got: {resource_keys}'


@pytest.mark.asyncio
async def test_job_monitor_tool_has_app_meta(mcp_server):
    """Test that job_monitor tool has _meta.ui with resourceUri."""
    tools = await mcp_server.get_tools()
    tool = tools['job_monitor']
    meta = tool.meta
    assert meta is not None
    assert 'ui' in meta
    assert meta['ui']['resourceUri'] == 'ui://keboola/job-monitor'


@pytest.mark.asyncio
async def test_poll_job_monitor_is_app_only(mcp_server):
    """Test that poll_job_monitor has visibility: ['app']."""
    tools = await mcp_server.get_tools()
    tool = tools['poll_job_monitor']
    meta = tool.meta
    assert meta is not None
    assert meta['ui']['visibility'] == ['app']
