"""Docker component execution for local-backend mode (Common Interface)."""

import json
import logging
import shutil
import subprocess
import time
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field

LOG = logging.getLogger(__name__)

SUBPROCESS_TIMEOUT = 300  # 5 minutes


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
    stdout: str = Field(default='', description='Last 5000 chars of container stdout.')
    stderr: str = Field(default='', description='Last 5000 chars of container stderr.')
    message: str | None = Field(default=None, description='Human-readable status message (set on error/timeout).')


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
) -> None:
    """Create the Common Interface /data directory and write config.json."""
    for subdir in ('in/tables', 'in/files', 'out/tables', 'out/files'):
        (run_dir / subdir).mkdir(parents=True, exist_ok=True)

    config = {
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
    (run_dir / 'config.json').write_text(json.dumps(config, indent=2), encoding='utf-8')

    for table_name in input_tables or []:
        src = catalog_tables / f'{table_name}.csv'
        if src.exists():
            shutil.copy2(src, run_dir / 'in' / 'tables' / f'{table_name}.csv')
        else:
            LOG.warning(f'Input table not found in catalog: {table_name}')


def collect_output_tables(out_tables_dir: Path, catalog_dir: Path) -> list[str]:
    """Copy output CSVs from the run dir back to the local catalog.

    Returns the list of collected table name stems.
    """
    catalog_dir.mkdir(parents=True, exist_ok=True)
    names: list[str] = []
    if out_tables_dir.exists():
        for csv_file in sorted(out_tables_dir.glob('*.csv')):
            shutil.copy2(csv_file, catalog_dir / csv_file.name)
            names.append(csv_file.stem)
    return names


def get_dep_install_commands(clone_dir: Path, network: str = 'bridge') -> list[list[str]]:
    """Return dependency install commands based on files present in the clone dir.

    Each command is suitable for `subprocess.run(cmd, cwd=clone_dir)`.
    Commands are skipped if the dependency directory already exists (idempotent).
    """
    cmds: list[list[str]] = []
    if (clone_dir / 'composer.json').exists() and not (clone_dir / 'vendor').exists():
        cmds.append(['docker', 'compose', 'run', '--rm', f'--network={network}', 'dev', 'composer', 'install'])
    if (clone_dir / 'package.json').exists() and not (clone_dir / 'node_modules').exists():
        cmds.append(['docker', 'compose', 'run', '--rm', f'--network={network}', 'dev', 'npm', 'ci'])
    if (clone_dir / 'requirements.txt').exists() and not (clone_dir / '.venv').exists():
        cmds.append(
            ['docker', 'compose', 'run', '--rm', f'--network={network}', 'dev', 'pip', 'install', '-r', 'requirements.txt']
        )
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

    if force_rebuild or not build_marker.exists():
        LOG.info(f'Building Docker image for {repo_name}')
        # docker compose build doesn't accept --network as a CLI flag; inject via
        # an auto-discovered override file instead (only needed for non-bridge networks).
        override_path = clone_dir / 'docker-compose.override.yml'
        had_override = override_path.exists()
        if not had_override and network != 'bridge':
            override_path.write_text(
                f'services:\n  dev:\n    build:\n      network: {network}\n'
            )
        try:
            proc = subprocess.run(
                ['docker', 'compose', 'build'],
                cwd=str(clone_dir),
                capture_output=True,
                text=True,
                timeout=SUBPROCESS_TIMEOUT,
            )
        finally:
            if not had_override:
                override_path.unlink(missing_ok=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f'docker compose build failed (exit {proc.returncode}):\n'
                f'stdout: {proc.stdout[-2000:]}\nstderr: {proc.stderr[-2000:]}'
            )
        build_marker.touch()

    for cmd in get_dep_install_commands(clone_dir, network=network):
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
) -> ComponentRunResult:
    """Run a Keboola component from a pre-built Docker registry image."""
    run_id = f'run-{int(time.time())}'
    run_dir = data_dir / 'runs' / run_id
    catalog_tables = data_dir / 'tables'

    prepare_data_dir(run_dir, parameters, input_tables, catalog_tables)

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
            f'--volume={run_dir}:/data',
            f'--memory={memory_limit}',
            f'--network={network}',
        ]
        + env_flags
        + [component_image]
    )

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return ComponentRunResult(
            status='application_error',
            exit_code=-1,
            stdout='',
            stderr=f'Timed out after {timeout}s',
            message=f'Component timed out after {timeout} seconds',
        )

    output_tables = collect_output_tables(run_dir / 'out' / 'tables', catalog_tables)
    return ComponentRunResult(
        status=exit_code_to_status(proc.returncode),
        exit_code=proc.returncode,
        output_tables=output_tables,
        stdout=proc.stdout[-5000:],
        stderr=proc.stderr[-5000:],
    )


def run_source_component(
    data_dir: Path,
    git_url: str,
    parameters: dict,
    input_tables: list[str] | None,
    memory_limit: str,
    timeout: int = SUBPROCESS_TIMEOUT,
    network: str = 'bridge',
) -> ComponentRunResult:
    """Run a Keboola component from source using docker compose."""
    setup_result = setup_component(data_dir / 'components', git_url, network=network)
    clone_dir = Path(setup_result.path)
    catalog_tables = data_dir / 'tables'

    component_data_dir = clone_dir / 'data'
    prepare_data_dir(component_data_dir, parameters, input_tables, catalog_tables)

    compose_cmd = read_compose_command(clone_dir)
    cmd = ['docker', 'compose', 'run', '--rm', f'--memory={memory_limit}', f'--network={network}', 'dev']
    if compose_cmd:
        cmd.extend(compose_cmd)

    try:
        proc = subprocess.run(cmd, cwd=str(clone_dir), capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return ComponentRunResult(
            status='application_error',
            exit_code=-1,
            stdout='',
            stderr=f'Timed out after {timeout}s',
            message=f'Component timed out after {timeout} seconds',
        )

    output_tables = collect_output_tables(component_data_dir / 'out' / 'tables', catalog_tables)
    return ComponentRunResult(
        status=exit_code_to_status(proc.returncode),
        exit_code=proc.returncode,
        output_tables=output_tables,
        stdout=proc.stdout[-5000:],
        stderr=proc.stderr[-5000:],
    )
