import importlib.resources


def _load_html() -> str:
    """Load the data chart HTML from the package."""
    return importlib.resources.read_text('keboola_mcp_server.apps', 'data_chart.html')


def test_data_chart_html_is_valid_mcp_app():
    """Verify the HTML contains required MCP App SDK wiring."""
    html = _load_html()
    assert '<!DOCTYPE html>' in html
    assert '@modelcontextprotocol/ext-apps@1.1.2' in html
    assert 'app.connect()' in html
    assert 'ontoolresult' in html


def test_data_chart_html_loads_chartjs():
    """Verify the HTML loads Chart.js."""
    html = _load_html()
    assert 'chart.js@4' in html
    assert 'cdn.jsdelivr.net' in html


def test_data_chart_html_has_canvas():
    """Verify the HTML has a canvas element for Chart.js."""
    html = _load_html()
    assert 'chart-canvas' in html
    assert '<canvas' in html


def test_data_chart_html_supports_dark_mode():
    """Verify the HTML supports dark mode theming."""
    html = _load_html()
    assert 'prefers-color-scheme' in html or 'color-scheme' in html


def test_data_chart_html_has_no_polling():
    """Data chart does not poll — it's a single render."""
    html = _load_html()
    assert 'poll_' not in html
    assert 'setInterval' not in html
