"""Tests for docker.py helper functions and main functions (subprocess mocked)."""

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest
import yaml

from keboola_mcp_server.tools.local.docker import (
    collect_output_tables,
    exit_code_to_status,
    get_dep_install_commands,
    get_repo_name,
    prepare_data_dir,
    read_compose_command,
    run_image_component,
    run_source_component,
    scan_component_schema,
    setup_component,
)

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('git_url', 'expected'),
    [
        ('https://github.com/keboola/generic-extractor.git', 'generic-extractor'),
        ('https://github.com/keboola/python-transformation', 'python-transformation'),
        ('git@github.com:keboola/ex-http.git', 'ex-http'),
        ('https://example.com/repo/', 'repo'),
    ],
)
def test_get_repo_name(git_url, expected):
    assert get_repo_name(git_url) == expected


@pytest.mark.parametrize(
    ('exit_code', 'expected'),
    [
        (0, 'success'),
        (1, 'user_error'),
        (2, 'application_error'),
        (137, 'application_error'),
    ],
)
def test_exit_code_to_status(exit_code, expected):
    assert exit_code_to_status(exit_code) == expected


def test_prepare_data_dir_creates_structure(tmp_path):
    run_dir = tmp_path / 'run-001'
    catalog = tmp_path / 'tables'
    catalog.mkdir()
    (catalog / 'orders.csv').write_text('id,amount\n1,100\n')

    prepare_data_dir(run_dir, {'key': 'val'}, ['orders'], catalog)

    assert (run_dir / 'in/tables').is_dir()
    assert (run_dir / 'in/files').is_dir()
    assert (run_dir / 'out/tables').is_dir()
    assert (run_dir / 'out/files').is_dir()

    config = json.loads((run_dir / 'config.json').read_text())
    assert config['parameters'] == {'key': 'val'}
    assert config['action'] == 'run'
    assert config['storage']['input']['tables'] == [{'source': 'orders', 'destination': 'orders'}]
    assert (run_dir / 'in/tables/orders.csv').exists()


def test_prepare_data_dir_missing_table_is_warned(tmp_path, caplog):
    import logging

    run_dir = tmp_path / 'run'
    catalog = tmp_path / 'tables'
    catalog.mkdir()

    with caplog.at_level(logging.WARNING):
        prepare_data_dir(run_dir, {}, ['missing_table'], catalog)

    assert 'missing_table' in caplog.text


def test_collect_output_tables(tmp_path):
    out_dir = tmp_path / 'out/tables'
    out_dir.mkdir(parents=True)
    (out_dir / 'result.csv').write_text('a,b\n1,2\n')
    (out_dir / 'summary.csv').write_text('x\n1\n')
    catalog = tmp_path / 'catalog'

    names = collect_output_tables(out_dir, catalog)

    assert sorted(names) == ['result', 'summary']
    assert (catalog / 'result.csv').exists()
    assert (catalog / 'summary.csv').exists()


def test_collect_output_tables_empty_dir(tmp_path):
    out_dir = tmp_path / 'out/tables'
    out_dir.mkdir(parents=True)
    catalog = tmp_path / 'catalog'

    names = collect_output_tables(out_dir, catalog)

    assert names == []


def test_collect_output_tables_missing_dir(tmp_path):
    out_dir = tmp_path / 'nonexistent'
    catalog = tmp_path / 'catalog'

    names = collect_output_tables(out_dir, catalog)

    assert names == []


@pytest.mark.parametrize(
    ('files', 'existing_dirs', 'expected_cmds'),
    [
        (['composer.json'], [], [['docker', 'compose', 'run', '--rm', 'dev', 'composer', 'install']]),
        (['composer.json'], ['vendor'], []),
        (['package.json'], [], [['docker', 'compose', 'run', '--rm', 'dev', 'npm', 'ci']]),
        (['package.json'], ['node_modules'], []),
        (
            ['requirements.txt'],
            [],
            [['docker', 'compose', 'run', '--rm', 'dev', 'pip', 'install', '-r', 'requirements.txt']],
        ),
        (['requirements.txt'], ['.venv'], []),
        ([], [], []),
    ],
)
def test_get_dep_install_commands(tmp_path, files, existing_dirs, expected_cmds):
    for f in files:
        (tmp_path / f).write_text('')
    for d in existing_dirs:
        (tmp_path / d).mkdir()

    assert get_dep_install_commands(tmp_path) == expected_cmds


@pytest.mark.parametrize(
    ('compose_content', 'expected'),
    [
        ({'services': {'dev': {'command': 'python main.py'}}}, ['python', 'main.py']),
        ({'services': {'dev': {'command': ['php', 'run.php']}}}, ['php', 'run.php']),
        ({'services': {'dev': {}}}, None),
        ({'services': {}}, None),
    ],
)
def test_read_compose_command(tmp_path, compose_content, expected):
    (tmp_path / 'docker-compose.yml').write_text(yaml.dump(compose_content))
    assert read_compose_command(tmp_path) == expected


def test_read_compose_command_no_file(tmp_path):
    assert read_compose_command(tmp_path) is None


def test_scan_component_schema_both(tmp_path):
    (tmp_path / 'component.json').write_text('{"type": "extractor"}')
    (tmp_path / 'README.md').write_text('# My Component\nDoes stuff.')

    result = scan_component_schema(tmp_path)

    assert result is not None
    assert result['component_json'] == {'type': 'extractor'}
    assert 'My Component' in result['readme_excerpt']


def test_scan_component_schema_none(tmp_path):
    assert scan_component_schema(tmp_path) is None


# ---------------------------------------------------------------------------
# setup_component (subprocess mocked)
# ---------------------------------------------------------------------------


def _make_proc(returncode=0, stdout='', stderr=''):
    proc = MagicMock()
    proc.returncode = returncode
    proc.stdout = stdout
    proc.stderr = stderr
    return proc


def test_setup_component_fresh_clone(tmp_path):
    components_dir = tmp_path / 'components'
    git_url = 'https://github.com/keboola/ex-http.git'
    clone_dir = components_dir / 'ex-http'

    def fake_run(cmd, **kwargs):
        # simulate git clone creating the directory
        if 'clone' in cmd:
            clone_dir.mkdir(parents=True, exist_ok=True)
        return _make_proc()

    with patch('subprocess.run', side_effect=fake_run) as mock_run:
        result = setup_component(components_dir, git_url)

    assert result.status == 'ready'
    assert 'ex-http' in result.path
    # clone + build called
    assert mock_run.call_count == 2


def test_setup_component_skip_clone_if_exists(tmp_path):
    components_dir = tmp_path / 'components'
    clone_dir = components_dir / 'ex-http'
    clone_dir.mkdir(parents=True)
    git_url = 'https://github.com/keboola/ex-http.git'

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc()
        setup_component(components_dir, git_url)

    # only build called (no clone)
    assert mock_run.call_count == 1
    first_cmd = mock_run.call_args_list[0][0][0]
    assert first_cmd == ['docker', 'compose', 'build']


def test_setup_component_host_network_override_file(tmp_path):
    components_dir = tmp_path / 'components'
    clone_dir = components_dir / 'ex-http'
    clone_dir.mkdir(parents=True)
    git_url = 'https://github.com/keboola/ex-http.git'
    override_path = clone_dir / 'docker-compose.override.yml'

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc()
        setup_component(components_dir, git_url, network='host')

    # build was called without --network flag
    assert mock_run.call_args_list[0][0][0] == ['docker', 'compose', 'build']
    # override file was cleaned up after build
    assert not override_path.exists()


def test_setup_component_host_network_respects_existing_override(tmp_path):
    components_dir = tmp_path / 'components'
    clone_dir = components_dir / 'ex-http'
    clone_dir.mkdir(parents=True)
    override_path = clone_dir / 'docker-compose.override.yml'
    override_path.write_text('# existing override\n')
    git_url = 'https://github.com/keboola/ex-http.git'

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc()
        setup_component(components_dir, git_url, network='host')

    # existing override file must not be deleted
    assert override_path.exists()
    assert override_path.read_text() == '# existing override\n'


def test_setup_component_skip_build_if_sentinel(tmp_path):
    components_dir = tmp_path / 'components'
    clone_dir = components_dir / 'ex-http'
    clone_dir.mkdir(parents=True)
    (clone_dir / '.keboola_image_built').touch()
    git_url = 'https://github.com/keboola/ex-http.git'

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc()
        setup_component(components_dir, git_url)

    # nothing called — already cloned and built
    assert mock_run.call_count == 0


def test_setup_component_force_rebuild(tmp_path):
    components_dir = tmp_path / 'components'
    clone_dir = components_dir / 'ex-http'
    clone_dir.mkdir(parents=True)
    (clone_dir / '.keboola_image_built').touch()
    git_url = 'https://github.com/keboola/ex-http.git'

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc()
        setup_component(components_dir, git_url, force_rebuild=True)

    # build re-runs despite sentinel
    assert mock_run.call_count == 1
    assert 'build' in mock_run.call_args_list[0][0][0]


def test_setup_component_clone_failure_raises(tmp_path):
    components_dir = tmp_path / 'components'

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc(returncode=1, stderr='fatal: repo not found')
        with pytest.raises(RuntimeError, match='git clone failed'):
            setup_component(components_dir, 'https://bad.url/repo.git')


def test_setup_component_build_failure_raises(tmp_path):
    components_dir = tmp_path / 'components'
    clone_dir = components_dir / 'repo'
    clone_dir.mkdir(parents=True)

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc(returncode=2, stderr='build error')
        with pytest.raises(RuntimeError, match='docker compose build failed'):
            setup_component(components_dir, 'https://example.com/repo.git')


# ---------------------------------------------------------------------------
# run_image_component (subprocess mocked)
# ---------------------------------------------------------------------------


def test_run_image_component_success(tmp_path):
    catalog = tmp_path / 'tables'
    catalog.mkdir()

    with patch('subprocess.run') as mock_run:
        proc = _make_proc(stdout='done', stderr='')
        mock_run.return_value = proc
        result = run_image_component(tmp_path, 'keboola/ex-http:latest', {}, None, '2g')

    assert result.status == 'success'
    assert result.exit_code == 0
    assert result.stdout == 'done'


def test_run_image_component_user_error(tmp_path):
    catalog = tmp_path / 'tables'
    catalog.mkdir()

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc(returncode=1, stderr='bad config')
        result = run_image_component(tmp_path, 'keboola/ex-http:latest', {}, None, '2g')

    assert result.status == 'user_error'
    assert result.exit_code == 1


def test_run_image_component_timeout(tmp_path):
    catalog = tmp_path / 'tables'
    catalog.mkdir()

    with patch('subprocess.run', side_effect=subprocess.TimeoutExpired(cmd='docker', timeout=300)):
        result = run_image_component(tmp_path, 'keboola/ex-http:latest', {}, None, '2g', timeout=300)

    assert result.status == 'application_error'
    assert result.exit_code == -1
    assert '300' in (result.message or '')


def test_run_image_component_collects_output(tmp_path):
    catalog = tmp_path / 'tables'
    catalog.mkdir()

    def fake_run(cmd, **kwargs):
        # simulate component writing output
        run_dirs = list((tmp_path / 'runs').glob('run-*'))
        if run_dirs:
            out = run_dirs[0] / 'out/tables'
            out.mkdir(parents=True, exist_ok=True)
            (out / 'output.csv').write_text('col\nval\n')
        return _make_proc()

    with patch('subprocess.run', side_effect=fake_run):
        result = run_image_component(tmp_path, 'keboola/ex-http:latest', {}, None, '2g')

    assert result.output_tables == ['output']
    assert (catalog / 'output.csv').exists()


# ---------------------------------------------------------------------------
# run_source_component (subprocess mocked)
# ---------------------------------------------------------------------------


def test_run_source_component_success(tmp_path):
    catalog = tmp_path / 'tables'
    catalog.mkdir()
    components_dir = tmp_path / 'components'
    repo_dir = components_dir / 'my-component'
    repo_dir.mkdir(parents=True)
    (repo_dir / '.keboola_image_built').touch()

    with patch('subprocess.run') as mock_run:
        mock_run.return_value = _make_proc(stdout='ok')
        result = run_source_component(tmp_path, 'https://github.com/keboola/my-component.git', {}, None, '2g')

    assert result.status == 'success'


def test_run_source_component_timeout(tmp_path):
    catalog = tmp_path / 'tables'
    catalog.mkdir()
    components_dir = tmp_path / 'components'
    repo_dir = components_dir / 'my-component'
    repo_dir.mkdir(parents=True)
    # sentinel present so setup_component makes no subprocess calls
    (repo_dir / '.keboola_image_built').touch()

    with patch('subprocess.run', side_effect=subprocess.TimeoutExpired(cmd='docker', timeout=300)):
        result = run_source_component(
            tmp_path, 'https://github.com/keboola/my-component.git', {}, None, '2g', timeout=300
        )

    assert result.status == 'application_error'
    assert result.exit_code == -1
