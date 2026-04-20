"""Unit tests for local data app — DataAppConfig CRUD, HTML generation, and process management."""

import os
import signal
from unittest.mock import MagicMock, patch

import pytest

from keboola_mcp_server.tools.local.backend import LocalBackend
from keboola_mcp_server.tools.local.dataapp import (
    DataAppChartConfig,
    DataAppConfig,
    generate_dashboard_html,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def backend(tmp_path):
    return LocalBackend(data_dir=str(tmp_path))


def _make_config(name='my-app', charts=None):
    charts = charts or [
        DataAppChartConfig(id='c1', title='Count', sql='SELECT 1 AS n', type='bar'),
    ]
    return DataAppConfig(
        name=name,
        title='My App',
        description='Test dashboard',
        tables=['iris'],
        charts=charts,
        created_at='2026-01-01T00:00:00+00:00',
        updated_at='2026-01-01T00:00:00+00:00',
    )


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------


def test_generate_dashboard_html_structure():
    config = _make_config()
    data = {'c1': {'columns': ['n'], 'rows': [{'n': 1}]}}
    html = generate_dashboard_html(config, data)

    assert '<!DOCTYPE html>' in html
    assert 'echarts' in html
    assert 'pico' in html
    # config embedded as JSON
    assert 'My App' in html
    assert 'Test dashboard' in html  # embedded in the JSON config block
    assert '"id": "c1"' in html


def test_generate_dashboard_html_escapes_script_tag():
    config = _make_config()
    # A value that would break HTML parsing if not escaped
    data = {'c1': {'columns': ['x'], 'rows': [{'x': '</script><script>alert(1)'}]}}
    html = generate_dashboard_html(config, data)
    assert '</script><script>' not in html
    assert '<\\/script>' in html


def test_generate_dashboard_html_error_chart():
    config = _make_config()
    data = {'c1': {'error': 'Table not found', 'columns': [], 'rows': []}}
    html = generate_dashboard_html(config, data)
    assert 'Table not found' in html


# ---------------------------------------------------------------------------
# Backend — DataApp CRUD
# ---------------------------------------------------------------------------


def test_save_and_load_data_app(backend):
    config = _make_config()
    chart_data = {'c1': {'columns': ['n'], 'rows': [{'n': 1}]}}
    app_dir = backend.save_data_app(config, chart_data)

    assert (app_dir / 'app.json').exists()
    assert (app_dir / 'index.html').exists()

    loaded = backend.load_data_app('my-app')
    assert loaded.name == 'my-app'
    assert loaded.title == 'My App'
    assert len(loaded.charts) == 1


def test_load_data_app_not_found(backend):
    with pytest.raises(FileNotFoundError):
        backend.load_data_app('nonexistent')


def test_list_data_apps_empty(backend):
    assert backend.list_data_apps() == []


def test_list_data_apps_multiple(backend):
    for name in ('app-a', 'app-b'):
        backend.save_data_app(_make_config(name=name), {'c1': {'columns': [], 'rows': []}})
    names = [c.name for c in backend.list_data_apps()]
    assert sorted(names) == ['app-a', 'app-b']


def test_delete_data_app(backend):
    config = _make_config()
    backend.save_data_app(config, {})
    assert (backend.apps_dir / 'my-app').exists()

    result = backend.delete_data_app('my-app')
    assert result is True
    assert not (backend.apps_dir / 'my-app').exists()


def test_delete_data_app_not_found(backend):
    assert backend.delete_data_app('ghost') is False


# ---------------------------------------------------------------------------
# Backend — running apps registry
# ---------------------------------------------------------------------------


def test_running_apps_empty(backend):
    assert backend._running_apps() == {}


def test_save_and_read_running_apps(backend):
    data = {'my-app': {'pid': 9999, 'port': 8101, 'started_at': '2026-01-01T00:00:00+00:00'}}
    backend._save_running_apps(data)
    assert backend._running_apps() == data


def test_get_running_port_not_running(backend):
    assert backend.get_running_port('my-app') is None


def test_get_running_port_stale_pid(backend):
    # Write a stale entry with a non-existent PID
    backend._save_running_apps({'my-app': {'pid': 999999999, 'port': 8101, 'started_at': 'x'}})
    # Should clean up the stale entry and return None
    assert backend.get_running_port('my-app') is None
    assert 'my-app' not in backend._running_apps()


def test_get_running_port_live_process(backend):
    # Use our own PID — guaranteed alive
    own_pid = os.getpid()
    backend._save_running_apps({'my-app': {'pid': own_pid, 'port': 8101, 'started_at': 'x'}})
    assert backend.get_running_port('my-app') == 8101


# ---------------------------------------------------------------------------
# Backend — start / stop
# ---------------------------------------------------------------------------


def test_start_data_app_not_found(backend):
    with pytest.raises(FileNotFoundError):
        backend.start_data_app('nonexistent')


def test_start_data_app_success(backend):
    config = _make_config()
    backend.save_data_app(config, {})

    mock_proc = MagicMock()
    mock_proc.pid = 12345

    with patch('subprocess.Popen', return_value=mock_proc) as mock_popen:
        pid, port = backend.start_data_app('my-app')

    assert pid == 12345
    assert 8101 <= port <= 8199
    mock_popen.assert_called_once()
    call_args = mock_popen.call_args
    assert 'python' in call_args[0][0]
    assert str(port) in call_args[0][0]

    running = backend._running_apps()
    assert 'my-app' in running
    assert running['my-app']['pid'] == 12345
    assert running['my-app']['port'] == port


def test_start_data_app_already_running(backend):
    config = _make_config()
    backend.save_data_app(config, {})
    own_pid = os.getpid()
    backend._save_running_apps({'my-app': {'pid': own_pid, 'port': 8101, 'started_at': 'x'}})

    with pytest.raises(RuntimeError, match='already running'):
        backend.start_data_app('my-app')


def test_stop_data_app_not_running(backend):
    assert backend.stop_data_app('ghost') is False


def test_stop_data_app_running(backend):
    backend._save_running_apps({'my-app': {'pid': 99999, 'port': 8101, 'started_at': 'x'}})

    with patch('os.kill') as mock_kill:
        result = backend.stop_data_app('my-app')

    assert result is True
    mock_kill.assert_called_once_with(99999, signal.SIGTERM)
    assert 'my-app' not in backend._running_apps()


def test_stop_data_app_already_dead(backend):
    backend._save_running_apps({'my-app': {'pid': 99999, 'port': 8101, 'started_at': 'x'}})

    with patch('os.kill', side_effect=ProcessLookupError):
        result = backend.stop_data_app('my-app')

    assert result is True  # still reports stopped (entry cleaned up)
    assert 'my-app' not in backend._running_apps()


def test_delete_data_app_removes_running_entry(backend):
    config = _make_config()
    backend.save_data_app(config, {})
    backend._save_running_apps({'my-app': {'pid': 99999, 'port': 8101, 'started_at': 'x'}})

    with patch('os.kill'):
        backend.delete_data_app('my-app')

    assert 'my-app' not in backend._running_apps()


# ---------------------------------------------------------------------------
# Backend — query_local_structured
# ---------------------------------------------------------------------------


def test_query_local_structured_returns_columns_and_rows(backend, tmp_path):
    # Write a tiny CSV for DuckDB to read
    csv_path = tmp_path / 'tables' / 'nums.csv'
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text('x,y\n1,2\n3,4\n')

    result = backend.query_local_structured('SELECT x, y FROM nums ORDER BY x')
    assert result['columns'] == ['x', 'y']
    assert len(result['rows']) == 2
    assert result['rows'][0] == {'x': 1, 'y': 2}


def test_query_local_structured_error(backend):
    result = backend.query_local_structured('SELECT * FROM nonexistent_table')
    assert 'error' in result
    assert result['rows'] == []


def test_query_local_structured_non_select(backend):
    result = backend.query_local_structured('SELECT 42 AS answer')
    assert result['columns'] == ['answer']
    assert result['rows'][0]['answer'] == 42
