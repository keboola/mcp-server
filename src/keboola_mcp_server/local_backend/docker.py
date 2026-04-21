"""Docker component execution for local-backend mode (Common Interface)."""

import json
import logging
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

LOG = logging.getLogger(__name__)

SUBPROCESS_TIMEOUT = 300  # 5 minutes

_MEMORY_RE = re.compile(r'^\d+[kmgKMG][bB]?$')
_NETWORK_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')


def _validate_memory(val: str) -> str:
    if not _MEMORY_RE.match(val):
        raise ValueError(f'Invalid memory_limit {val!r}. Expected format: 512m, 4g, 2gb.')
    return val


def _validate_network(val: str) -> str:
    if not _NETWORK_RE.match(val):
        raise ValueError(f'Invalid network {val!r}. Must be alphanumeric with dashes/underscores only.')
    return val


# ---------------------------------------------------------------------------
# Output models
# ---------------------------------------------------------------------------


class ComponentSetupResult(BaseModel):
    status: Literal['ready'] = 'ready'
    path: str = Field(description='Local path to the cloned component directory.')
    component_schema: dict | None = Field(
        default=None,
        description='Component schema from repo introspection (component.json excerpt, README excerpt).',
    )


class ComponentRunResult(BaseModel):
    status: Literal['success', 'user_error', 'application_error'] = Field(
        description='Run status derived from the container exit code (0=success, 1=user_error, >1=application_error).'
    )
    exit_code: int = Field(description='Docker container exit code (-1 on timeout).')
    output_tables: list[str] = Field(
        default_factory=list, description='Names (stems) of output CSV tables collected into the local catalog.'
    )
    stdout: str = Field(default='', description='Last 5000 chars of combined container output (stdout + stderr).')
    stderr: str = Field(
        default='', description='Deprecated — output is now combined in stdout. Reserved for error messages.'
    )
    message: str | None = Field(default=None, description='Human-readable status message (set on error/timeout).')
    log_file: str | None = Field(
        default=None,
        description='Path to the live output log file. Run `tail -f <path>` to watch progress.',
    )


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------


def _run_subprocess_logging(
    cmd: list[str],
    log_path: Path,
    cwd: str | None = None,
    timeout: int = SUBPROCESS_TIMEOUT,
) -> tuple[int, str]:
    """Run a subprocess, streaming stdout+stderr to log_path in real-time.

    Returns (exit_code, log_content). exit_code=-1 signals a timeout (process killed).
    The log file is written incrementally so callers can `tail -f` it while waiting.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'w', encoding='utf-8', errors='replace') as log_fh:
        proc = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, text=True, cwd=cwd)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            log_content = log_path.read_text(encoding='utf-8', errors='replace')
            return -1, log_content
    log_content = log_path.read_text(encoding='utf-8', errors='replace')
    return proc.returncode, log_content


# ---------------------------------------------------------------------------
# Pure helper functions (module-level, fully testable)
# ---------------------------------------------------------------------------


def get_repo_name(git_url: str) -> str:
    """Extract the repository name from a git URL.

    >>> get_repo_name("https://github.com/keboola/generic-extractor.git")
    'generic-extractor'
    >>> get_repo_name("https://github.com/keboola/python-transformation")
    'python-transformation'
    """
    url = git_url.rstrip('/')
    if url.endswith('.git'):
        url = url[:-4]
    return url.split('/')[-1]


def exit_code_to_status(exit_code: int) -> Literal['success', 'user_error', 'application_error']:
    """Map a container exit code to a semantic status string."""
    if exit_code == 0:
        return 'success'
    elif exit_code == 1:
        return 'user_error'
    else:
        return 'application_error'


def prepare_data_dir(
    run_dir: Path,
    parameters: dict,
    input_tables: list[str] | None,
    catalog_tables: Path,
    authorization: dict | None = None,
) -> None:
    """Create the Common Interface /data directory and write config.json."""
    for subdir in ('in/tables', 'in/files', 'out/tables', 'out/files'):
        (run_dir / subdir).mkdir(parents=True, exist_ok=True)

    config: dict = {
        'storage': {
            'input': {
                'tables': [{'source': t, 'destination': t} for t in (input_tables or [])],
                'files': [],
            },
            'output': {'tables': [], 'files': []},
        },
        'parameters': parameters,
        'action': 'run',
    }
    if authorization:
        config['authorization'] = authorization
    (run_dir / 'config.json').write_text(json.dumps(config, indent=2), encoding='utf-8')

    for table_name in input_tables or []:
        src = catalog_tables / f'{table_name}.csv'
        if src.exists():
            shutil.copy2(src, run_dir / 'in' / 'tables' / f'{table_name}.csv')
        else:
            LOG.warning(f'Input table not found in catalog: {table_name}')


def collect_output_tables(out_tables_dir: Path, catalog_dir: Path) -> list[str]:
    """Copy output CSVs from the run dir back to the local catalog.

    Collects from both out/tables/ (standard output) and out/files/ (file-based
    extractors like keboola.ex-http that write CSVs to the files output).
    Returns the list of collected table name stems.
    """
    catalog_dir.mkdir(parents=True, exist_ok=True)
    names: list[str] = []
    for search_dir in (out_tables_dir, out_tables_dir.with_name('files')):
        if search_dir.exists():
            for csv_file in sorted(search_dir.glob('*.csv')):
                shutil.copy2(csv_file, catalog_dir / csv_file.name)
                names.append(csv_file.stem)
    return names


def get_dep_install_commands(clone_dir: Path) -> list[list[str]]:
    """Return dependency install commands based on files present in the clone dir.

    Each command is suitable for `subprocess.run(cmd, cwd=clone_dir)`.
    Commands are skipped if the dependency directory already exists (idempotent).
    Network is controlled via docker-compose.override.yml by the caller when needed.
    """
    cmds: list[list[str]] = []
    if (clone_dir / 'composer.json').exists() and not (clone_dir / 'vendor').exists():
        cmds.append(['docker', 'compose', 'run', '--rm', 'dev', 'composer', 'install'])
    if (clone_dir / 'package.json').exists() and not (clone_dir / 'node_modules').exists():
        cmds.append(['docker', 'compose', 'run', '--rm', 'dev', 'npm', 'ci'])
    if (clone_dir / 'requirements.txt').exists() and not (clone_dir / '.venv').exists():
        cmds.append(['docker', 'compose', 'run', '--rm', 'dev', 'pip', 'install', '-r', 'requirements.txt'])
    return cmds


def read_compose_command(clone_dir: Path) -> list[str] | None:
    """Return the 'command' for the 'dev' service from docker-compose.yml, or None."""
    for filename in ('docker-compose.yml', 'docker-compose.yaml'):
        compose_file = clone_dir / filename
        if not compose_file.exists():
            continue
        try:
            data = yaml.safe_load(compose_file.read_text(encoding='utf-8'))
            dev = (data or {}).get('services', {}).get('dev', {})
            cmd = dev.get('command')
            if cmd is None:
                return None
            if isinstance(cmd, str):
                return cmd.split()
            return list(cmd)
        except Exception as exc:
            LOG.warning(f'Could not parse {compose_file}: {exc}')
    return None


def scan_component_schema(clone_dir: Path) -> dict | None:
    """Scan a cloned repo for component.json and README.md for schema hints."""
    result: dict = {}
    component_json = clone_dir / 'component.json'
    if component_json.exists():
        try:
            result['component_json'] = json.loads(component_json.read_text(encoding='utf-8'))
        except Exception as exc:
            LOG.warning(f'Could not parse component.json: {exc}')
    readme = clone_dir / 'README.md'
    if readme.exists():
        try:
            result['readme_excerpt'] = readme.read_text(encoding='utf-8', errors='replace')[:3000]
        except Exception:
            pass
    return result or None


# ---------------------------------------------------------------------------
# Main functions
# ---------------------------------------------------------------------------


def setup_component(
    components_dir: Path,
    git_url: str,
    force_rebuild: bool = False,
    network: str = 'bridge',
) -> ComponentSetupResult:
    """Clone a component repo and build its Docker image.

    - Skips clone if <components_dir>/<repo-name>/ already exists.
    - Skips build if .keboola_image_built sentinel exists and force_rebuild=False.
    - Installs dependencies only if the dependency directory is absent.
    """
    components_dir.mkdir(parents=True, exist_ok=True)
    repo_name = get_repo_name(git_url)
    clone_dir = components_dir / repo_name
    build_marker = clone_dir / '.keboola_image_built'

    if not clone_dir.exists():
        LOG.info(f'Cloning {git_url} → {clone_dir}')
        proc = subprocess.run(
            ['git', 'clone', git_url, str(clone_dir)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if proc.returncode != 0:
            raise RuntimeError(f'git clone failed (exit {proc.returncode}):\n{proc.stderr}')

    # docker compose build/run don't accept --network as a CLI flag; inject via
    # an auto-discovered override file that covers both build and dep-install runs.
    override_path = clone_dir / 'docker-compose.override.yml'
    had_override = override_path.exists()
    if not had_override and network != 'bridge':
        override_path.write_text(
            f'services:\n  dev:\n    build:\n      network: {network}\n    network_mode: {network}\n'
        )
    try:
        if force_rebuild or not build_marker.exists():
            LOG.info(f'Building Docker image for {repo_name}')
            proc = subprocess.run(
                ['docker', 'compose', 'build'],
                cwd=str(clone_dir),
                capture_output=True,
                text=True,
                timeout=SUBPROCESS_TIMEOUT,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f'docker compose build failed (exit {proc.returncode}):\n'
                    f'stdout: {proc.stdout[-2000:]}\nstderr: {proc.stderr[-2000:]}'
                )
            build_marker.touch()

        for cmd in get_dep_install_commands(clone_dir):
            LOG.info(f'Installing deps: {" ".join(cmd)}')
            proc = subprocess.run(
                cmd,
                cwd=str(clone_dir),
                capture_output=True,
                text=True,
                timeout=SUBPROCESS_TIMEOUT,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f'Dependency install failed (exit {proc.returncode}):\n'
                    f'stdout: {proc.stdout[-2000:]}\nstderr: {proc.stderr[-2000:]}'
                )
    finally:
        if not had_override:
            override_path.unlink(missing_ok=True)

    component_schema = scan_component_schema(clone_dir)
    return ComponentSetupResult(path=str(clone_dir), component_schema=component_schema)


def run_image_component(
    data_dir: Path,
    component_image: str,
    parameters: dict,
    input_tables: list[str] | None,
    memory_limit: str,
    timeout: int = SUBPROCESS_TIMEOUT,
    network: str = 'bridge',
    authorization: dict | None = None,
) -> ComponentRunResult:
    """Run a Keboola component from a pre-built Docker registry image."""
    _validate_memory(memory_limit)
    _validate_network(network)
    run_id = f'run-{int(time.time())}'
    run_dir = data_dir / 'runs' / run_id
    catalog_tables = data_dir / 'tables'

    prepare_data_dir(run_dir, parameters, input_tables, catalog_tables, authorization)

    env_flags: list[str] = []
    for key, val in (
        ('KBC_DATADIR', '/data/'),
        ('KBC_RUNID', f'local-{run_id}'),
        ('KBC_PROJECTID', '0'),
        ('KBC_CONFIGID', 'local-config'),
        ('KBC_COMPONENTID', component_image),
    ):
        env_flags += ['-e', f'{key}={val}']

    cmd = (
        [
            'docker',
            'run',
            '--rm',
            f'--volume={run_dir.resolve()}:/data',
            f'--memory={memory_limit}',
            f'--network={network}',
        ]
        + env_flags
        + [component_image]
    )

    log_path = run_dir / 'run.log'
    exit_code, log_content = _run_subprocess_logging(cmd, log_path, timeout=timeout)

    if exit_code == -1:
        return ComponentRunResult(
            status='application_error',
            exit_code=-1,
            stdout=log_content[-5000:],
            log_file=str(log_path),
            message=f'Component timed out after {timeout} seconds',
        )

    output_tables = collect_output_tables(run_dir / 'out' / 'tables', catalog_tables)
    return ComponentRunResult(
        status=exit_code_to_status(exit_code),
        exit_code=exit_code,
        output_tables=output_tables,
        stdout=log_content[-5000:],
        log_file=str(log_path),
    )


def run_source_component(
    data_dir: Path,
    git_url: str,
    parameters: dict,
    input_tables: list[str] | None,
    memory_limit: str,
    timeout: int = SUBPROCESS_TIMEOUT,
    network: str = 'bridge',
    authorization: dict | None = None,
) -> ComponentRunResult:
    """Run a Keboola component from source using docker compose."""
    _validate_memory(memory_limit)
    _validate_network(network)
    setup_result = setup_component(data_dir / 'components', git_url, network=network)
    clone_dir = Path(setup_result.path)
    catalog_tables = data_dir / 'tables'

    # Use a fresh user-owned temp directory for /data instead of clone_dir/data.
    # Docker creates files as root inside bind-mounted directories; using a temp dir
    # we create ourselves ensures we always have write access on the host side.
    run_dir = data_dir / 'runs' / f'src-{int(time.time())}'
    prepare_data_dir(run_dir, parameters, input_tables, catalog_tables, authorization)

    compose_cmd = read_compose_command(clone_dir)
    # Force KBC_DATADIR=/data/ so components that hardcode a relative path in
    # their docker-compose.yml (e.g. KBC_DATADIR=./data) still find config.json
    # at the bind-mounted run_dir we control.
    cmd = ['docker', 'compose', 'run', '--rm', '-e', 'KBC_DATADIR=/data/', 'dev']
    if compose_cmd:
        cmd.extend(compose_cmd)

    # docker compose run doesn't accept --memory or --network as CLI flags;
    # inject both (plus the data volume override) via the auto-discovered override file.
    override_path = clone_dir / 'docker-compose.override.yml'
    had_override = override_path.exists()
    if not had_override:
        override_lines = ['services:', '  dev:']
        override_lines.append(f'    mem_limit: {memory_limit}')
        if network != 'bridge':
            override_lines.append(f'    network_mode: {network}')
        # Override the ./data:/data volume so Docker uses our user-owned run_dir.
        override_lines.append('    volumes:')
        override_lines.append(f'      - {run_dir.resolve()}:/data')
        override_path.write_text('\n'.join(override_lines) + '\n')

    log_path = run_dir / 'run.log'
    exit_code, log_content = _run_subprocess_logging(cmd, log_path, cwd=str(clone_dir), timeout=timeout)

    if not had_override:
        override_path.unlink(missing_ok=True)

    if exit_code == -1:
        return ComponentRunResult(
            status='application_error',
            exit_code=-1,
            stdout=log_content[-5000:],
            log_file=str(log_path),
            message=f'Component timed out after {timeout} seconds',
        )

    output_tables = collect_output_tables(run_dir / 'out' / 'tables', catalog_tables)
    return ComponentRunResult(
        status=exit_code_to_status(exit_code),
        exit_code=exit_code,
        output_tables=output_tables,
        stdout=log_content[-5000:],
        log_file=str(log_path),
    )
