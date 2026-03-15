from keboola_mcp_server.apps import build_app_tool_meta


def test_build_app_tool_meta_model_visible():
    """Test build_app_tool_meta generates correct _meta.ui for model-visible tools."""
    meta = build_app_tool_meta(
        resource_uri='ui://keboola/job-monitor',
        csp_resource_domains=['https://unpkg.com'],
    )
    assert meta == {
        'ui': {
            'resourceUri': 'ui://keboola/job-monitor',
            'csp': {
                'resource_domains': ['https://unpkg.com'],
            },
        },
    }


def test_build_app_tool_meta_app_only():
    """Test that build_app_tool_meta includes visibility for app-only tools."""
    meta = build_app_tool_meta(
        resource_uri='ui://keboola/job-monitor',
        visibility=['app'],
        csp_resource_domains=['https://unpkg.com'],
    )
    assert meta['ui']['visibility'] == ['app']


def test_build_app_tool_meta_no_csp():
    """Test that build_app_tool_meta works without CSP domains."""
    meta = build_app_tool_meta(resource_uri='ui://keboola/test')
    assert 'csp' not in meta['ui']
    assert meta['ui']['resourceUri'] == 'ui://keboola/test'
