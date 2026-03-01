import importlib.resources


def _load_html() -> str:
    """Load the config diff HTML from the package."""
    return importlib.resources.read_text('keboola_mcp_server.apps', 'config_diff.html')


def test_config_diff_html_is_valid_mcp_app():
    """Verify the HTML contains required MCP App SDK wiring."""
    html = _load_html()
    assert '<!DOCTYPE html>' in html
    assert 'app.connect()' in html
    assert 'ontoolresult' in html


def test_config_diff_html_loads_jsondiffpatch():
    """Verify the HTML loads jsondiffpatch library."""
    html = _load_html()
    assert 'jsondiffpatch' in html
    assert 'unpkg.com' in html


def test_config_diff_html_has_diff_container():
    """Verify the HTML has a container for the diff output."""
    html = _load_html()
    assert 'diff-container' in html


def test_config_diff_html_supports_dark_mode():
    """Verify the HTML supports dark mode theming."""
    html = _load_html()
    assert 'prefers-color-scheme' in html or 'color-scheme' in html


def test_config_diff_html_handles_errors():
    """Verify the HTML has error display handling."""
    html = _load_html()
    assert 'isValid' in html or 'validationErrors' in html
