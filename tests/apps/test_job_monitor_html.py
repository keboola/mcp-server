import importlib.resources


def _load_html() -> str:
    """Load the job monitor HTML from the package."""
    return importlib.resources.read_text('keboola_mcp_server.apps', 'job_monitor.html')


def test_job_monitor_html_is_valid_mcp_app():
    """Verify the HTML contains required MCP App SDK wiring."""
    html = _load_html()
    assert '<!DOCTYPE html>' in html
    assert '@modelcontextprotocol/ext-apps' in html
    assert 'app.connect()' in html
    assert 'ontoolresult' in html


def test_job_monitor_html_has_poll_call():
    """Verify the HTML calls poll_job_monitor for auto-refresh."""
    html = _load_html()
    assert 'poll_job_monitor' in html


def test_job_monitor_html_supports_dark_mode():
    """Verify the HTML supports dark mode theming."""
    html = _load_html()
    assert 'prefers-color-scheme' in html or 'color-scheme' in html
