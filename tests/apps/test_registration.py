import pytest

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server


@pytest.fixture
def mcp_server():
    """Create a server instance for registration tests."""
    config = Config(storage_api_url='https://connection.keboola.com')
    runtime_info = ServerRuntimeInfo(transport='stdio')
    return create_server(config, runtime_info=runtime_info, custom_routes_handling='add')


# ── Data Chart App ──


@pytest.mark.asyncio
async def test_data_chart_tool_registered(mcp_server):
    """Test that visualize_data tool is registered."""
    tools = await mcp_server.get_tools()
    assert 'visualize_data' in tools


@pytest.mark.asyncio
async def test_data_chart_resource_registered(mcp_server):
    """Test that the ui://keboola/data-chart resource is registered."""
    resources = await mcp_server.get_resources()
    resource_keys = list(resources.keys())
    assert any('data-chart' in str(k) for k in resource_keys), f'Expected data-chart resource, got: {resource_keys}'


@pytest.mark.asyncio
async def test_data_chart_tool_has_app_meta(mcp_server):
    """Test that visualize_data tool has _meta.ui with resourceUri."""
    tools = await mcp_server.get_tools()
    tool = tools['visualize_data']
    meta = tool.meta
    assert meta is not None
    assert 'ui' in meta
    assert meta['ui']['resourceUri'] == 'ui://keboola/data-chart'


@pytest.mark.asyncio
async def test_data_chart_tool_meta_has_no_csp(mcp_server):
    """CSP must be on the resource, not the tool (per MCP Apps spec)."""
    tools = await mcp_server.get_tools()
    tool = tools['visualize_data']
    assert 'csp' not in tool.meta['ui']


@pytest.mark.asyncio
async def test_data_chart_resource_meta_has_csp(mcp_server):
    """The data-chart resource should carry CSP for unpkg.com and cdn.jsdelivr.net."""
    resources = await mcp_server.get_resources()
    resource = None
    for key, res in resources.items():
        if 'data-chart' in str(key):
            resource = res
            break
    assert resource is not None
    meta = resource.meta
    assert meta is not None
    assert 'ui' in meta
    assert 'csp' in meta['ui']
    assert 'https://unpkg.com' in meta['ui']['csp']['resourceDomains']
    assert 'https://cdn.jsdelivr.net' in meta['ui']['csp']['resourceDomains']


# ── Config Diff App ──


@pytest.mark.asyncio
async def test_config_diff_tool_registered(mcp_server):
    """Test that preview_config_diff tool is registered."""
    tools = await mcp_server.get_tools()
    assert 'preview_config_diff' in tools


@pytest.mark.asyncio
async def test_config_diff_resource_registered(mcp_server):
    """Test that the ui://keboola/config-diff resource is registered."""
    resources = await mcp_server.get_resources()
    resource_keys = list(resources.keys())
    assert any('config-diff' in str(k) for k in resource_keys), f'Expected config-diff resource, got: {resource_keys}'


@pytest.mark.asyncio
async def test_config_diff_tool_has_app_meta(mcp_server):
    """Test that preview_config_diff tool has _meta.ui with resourceUri."""
    tools = await mcp_server.get_tools()
    tool = tools['preview_config_diff']
    meta = tool.meta
    assert meta is not None
    assert 'ui' in meta
    assert meta['ui']['resourceUri'] == 'ui://keboola/config-diff'


@pytest.mark.asyncio
async def test_config_diff_tool_meta_has_no_csp(mcp_server):
    """CSP must be on the resource, not the tool (per MCP Apps spec)."""
    tools = await mcp_server.get_tools()
    tool = tools['preview_config_diff']
    assert 'csp' not in tool.meta['ui']


@pytest.mark.asyncio
async def test_config_diff_resource_meta_has_csp(mcp_server):
    """The config-diff resource should carry CSP for unpkg.com."""
    resources = await mcp_server.get_resources()
    resource = None
    for key, res in resources.items():
        if 'config-diff' in str(key):
            resource = res
            break
    assert resource is not None
    meta = resource.meta
    assert meta is not None
    assert 'ui' in meta
    assert 'csp' in meta['ui']
    assert 'https://unpkg.com' in meta['ui']['csp']['resourceDomains']
