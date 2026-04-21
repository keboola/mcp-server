"""Local backend: filesystem catalog and DuckDB-powered SQL execution."""

import csv
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path

LOG = logging.getLogger(__name__)

_RUNNING_APPS_FILE = '.running.json'
_DATA_APP_PORT_RANGE = range(8101, 8200)


class LocalBackend:
    """Filesystem-backed data catalog with DuckDB SQL support."""

    def __init__(
        self,
        data_dir: str = './keboola_data',
        docker_network: str = 'bridge',
        storage_api_url: str | None = None,
        storage_token: str | None = None,
    ):
        self.data_dir = Path(data_dir)
        self.docker_network = docker_network
        self.storage_api_url = storage_api_url
        self.storage_token = storage_token
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # Create sub-dirs eagerly so glob() never raises OSError on Python 3.13+.
        (self.data_dir / 'tables').mkdir(parents=True, exist_ok=True)
        (self.data_dir / 'configs').mkdir(parents=True, exist_ok=True)

    def list_csv_tables(self) -> list[Path]:
        """Return sorted list of .csv files under <data_dir>/tables/."""
        return sorted((self.data_dir / 'tables').glob('*.csv'))

    def read_csv_headers(self, csv_path: Path) -> list[str]:
        """Return column names from the first line of a CSV file."""
        try:
            with open(csv_path, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                return next(reader, [])
        except (OSError, StopIteration):
            return []

    def query_local(self, sql_query: str) -> str:
        """Execute SQL against local CSV files using native DuckDB."""
        con = self._duckdb_connection()
        cursor = con.execute(sql_query)
        # DB-API 2.0 (PEP 249): cursor.description is None for non-SELECT
        # statements (DDL, DML). Guard before iterating to avoid TypeError.
        if cursor.description is None:
            con.close()
            return 'Query executed successfully (no rows returned).'

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        con.close()

        header = '| ' + ' | '.join(columns) + ' |'
        separator = '| ' + ' | '.join(['---'] * len(columns)) + ' |'
        body = ['| ' + ' | '.join('' if v is None else str(v) for v in row) + ' |' for row in rows]
        return '\n'.join([header, separator, *body])

    def query_local_structured(self, sql_query: str) -> dict:
        """Execute SQL and return {columns, rows} dicts for embedding in HTML dashboards.

        Values are coerced to JSON-safe types (int/float kept, everything else str).
        Returns {error} dict on failure.
        """
        try:
            con = self._duckdb_connection()
            cursor = con.execute(sql_query)
            if cursor.description is None:
                con.close()
                return {'columns': [], 'rows': []}
            columns = [col[0] for col in cursor.description]
            raw_rows = cursor.fetchall()
            con.close()
        except Exception as exc:
            return {'error': str(exc), 'columns': [], 'rows': []}

        rows = []
        for raw in raw_rows:
            row = {}
            for col, val in zip(columns, raw):
                if val is None:
                    row[col] = None
                elif isinstance(val, (int, float)):
                    row[col] = val
                else:
                    row[col] = str(val)
            rows.append(row)
        return {'columns': columns, 'rows': rows}

    def _duckdb_connection(self):
        """Open a DuckDB connection with all local CSV tables registered."""
        try:
            import duckdb
        except ImportError:
            raise RuntimeError(
                'duckdb is required for local mode SQL queries. '
                'Install it with: pip install "keboola-mcp-server[local]"'
            )

        con = duckdb.connect()
        tables_dir = self.data_dir / 'tables'
        for csv_file in tables_dir.glob('*.csv'):
            # Sanitize table name: substitute double-quote characters with '_'
            # (not deletion) to prevent identifier injection while preserving
            # name uniqueness and avoiding empty-identifier crashes.
            # CREATE OR REPLACE ensures stale tables are refreshed on each call.
            table_name = csv_file.stem.replace('"', '_')
            con.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM read_csv_auto(?)',
                [str(csv_file)],
            )
        return con

    # ------------------------------------------------------------------
    # Table write / delete
    # ------------------------------------------------------------------

    def write_csv_table(self, name: str, csv_content: str) -> Path:
        """Write CSV content to <data_dir>/tables/<name>.csv.

        Raises ValueError if the name is empty or contains path-separator characters.
        """
        name = name.strip()
        if not name or '/' in name or '\\' in name or '..' in name:
            raise ValueError(f'Invalid table name: {name!r}')
        # Strip .csv extension if provided — stem is the canonical form.
        if name.endswith('.csv'):
            name = name[:-4]
        path = self.data_dir / 'tables' / f'{name}.csv'
        path.write_text(csv_content, encoding='utf-8')
        return path

    def delete_csv_table(self, name: str) -> bool:
        """Delete <data_dir>/tables/<name>.csv. Returns True if deleted, False if not found."""
        if name.endswith('.csv'):
            name = name[:-4]
        path = self.data_dir / 'tables' / f'{name}.csv'
        if path.exists():
            path.unlink()
            return True
        return False

    # ------------------------------------------------------------------
    # Component configuration persistence (delegates to config.py)
    # ------------------------------------------------------------------

    @property
    def configs_dir(self) -> Path:
        return self.data_dir / 'configs'

    def save_config(self, config):
        """Save a ComponentConfig to <data_dir>/configs/<config_id>.json."""
        from keboola_mcp_server.local_backend.config import save_config

        return save_config(self.configs_dir, config)

    def list_configs(self):
        """List all saved ComponentConfigs."""
        from keboola_mcp_server.local_backend.config import list_configs

        return list_configs(self.configs_dir)

    def load_config(self, config_id: str):
        """Load a saved ComponentConfig by ID."""
        from keboola_mcp_server.local_backend.config import load_config

        return load_config(self.configs_dir, config_id)

    def delete_config(self, config_id: str) -> bool:
        """Delete a saved ComponentConfig. Returns True if deleted."""
        from keboola_mcp_server.local_backend.config import delete_config

        return delete_config(self.configs_dir, config_id)

    # ------------------------------------------------------------------
    # Docker component execution (delegates to local_backend/docker.py)
    # ------------------------------------------------------------------

    def setup_component(self, git_url: str, force_rebuild: bool = False):
        """Clone a component repo and build its Docker image.

        Returns a ComponentSetupResult with the clone path and optional schema.
        """
        from keboola_mcp_server.local_backend.docker import setup_component

        return setup_component(
            self.data_dir / 'components',
            git_url,
            force_rebuild=force_rebuild,
            network=self.docker_network,
        )

    def run_docker_component(
        self,
        component_image: str,
        parameters: dict,
        input_tables: list[str] | None = None,
        memory_limit: str = '4g',
        authorization: dict | None = None,
    ):
        """Run a Keboola component from a Docker registry image.

        Returns a ComponentRunResult.
        """
        from keboola_mcp_server.local_backend.docker import run_image_component

        return run_image_component(
            self.data_dir,
            component_image,
            parameters,
            input_tables,
            memory_limit,
            network=self.docker_network,
            authorization=authorization,
        )

    def run_source_component(
        self,
        git_url: str,
        parameters: dict,
        input_tables: list[str] | None = None,
        memory_limit: str = '4g',
        authorization: dict | None = None,
    ):
        """Clone + build + run a component from source via docker compose.

        Returns a ComponentRunResult.
        """
        from keboola_mcp_server.local_backend.docker import run_source_component

        return run_source_component(
            self.data_dir,
            git_url,
            parameters,
            input_tables,
            memory_limit,
            network=self.docker_network,
            authorization=authorization,
        )

    # ------------------------------------------------------------------
    # Local data apps
    # ------------------------------------------------------------------

    @property
    def apps_dir(self) -> Path:
        return self.data_dir / 'apps'

    def _running_apps(self) -> dict:
        path = self.apps_dir / _RUNNING_APPS_FILE
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_running_apps(self, data: dict) -> None:
        self.apps_dir.mkdir(parents=True, exist_ok=True)
        (self.apps_dir / _RUNNING_APPS_FILE).write_text(json.dumps(data, indent=2), encoding='utf-8')

    def _find_free_port(self) -> int:
        used = {entry['port'] for entry in self._running_apps().values() if 'port' in entry}
        for port in _DATA_APP_PORT_RANGE:
            if port in used:
                continue
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('127.0.0.1', port))
                    return port
                except OSError:
                    continue
        raise RuntimeError('No free port available in range 8101-8199.')

    def _is_process_alive(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def list_data_apps(self) -> list:
        """List all saved DataAppConfigs from <data_dir>/apps/."""
        from keboola_mcp_server.local_backend.dataapp import DataAppConfig

        if not self.apps_dir.exists():
            return []
        configs = []
        for path in sorted(self.apps_dir.glob('*/app.json')):
            try:
                configs.append(DataAppConfig.model_validate_json(path.read_text(encoding='utf-8')))
            except Exception:
                LOG.warning('Could not load data app config: %s', path)
        return configs

    def load_data_app(self, name: str):
        """Load a DataAppConfig by name. Raises FileNotFoundError if not found."""
        from keboola_mcp_server.local_backend.dataapp import DataAppConfig

        path = self.apps_dir / name / 'app.json'
        if not path.exists():
            raise FileNotFoundError(f'Data app not found: {name!r}')
        return DataAppConfig.model_validate_json(path.read_text(encoding='utf-8'))

    def save_data_app(self, config) -> Path:
        """Persist DataAppConfig and regenerate index.html. Returns the app directory."""
        from keboola_mcp_server.local_backend.dataapp import generate_dashboard_html

        app_dir = self.apps_dir / config.name
        app_dir.mkdir(parents=True, exist_ok=True)
        (app_dir / 'app.json').write_text(config.model_dump_json(indent=2), encoding='utf-8')
        (app_dir / 'index.html').write_text(generate_dashboard_html(config), encoding='utf-8')
        return app_dir

    def delete_data_app(self, name: str) -> bool:
        """Delete <data_dir>/apps/<name>/. Returns True if deleted."""
        app_dir = self.apps_dir / name
        if not app_dir.exists():
            return False
        shutil.rmtree(app_dir)
        running = self._running_apps()
        if name in running:
            running.pop(name)
            self._save_running_apps(running)
        return True

    def get_running_port(self, name: str) -> int | None:
        """Return the port the app is serving on, or None if stopped."""
        entry = self._running_apps().get(name)
        if not entry:
            return None
        if not self._is_process_alive(entry['pid']):
            running = self._running_apps()
            running.pop(name, None)
            self._save_running_apps(running)
            return None
        return entry['port']

    def start_data_app(self, name: str) -> tuple[int, int]:
        """Start a Python HTTP server for the app. Returns (pid, port).

        Raises FileNotFoundError if the app does not exist.
        Raises RuntimeError if already running.
        """
        app_dir = self.apps_dir / name
        if not (app_dir / 'index.html').exists():
            raise FileNotFoundError(f'Data app not found: {name!r}')

        running = self._running_apps()
        if name in running and self._is_process_alive(running[name]['pid']):
            raise RuntimeError(f'App {name!r} is already running on port {running[name]["port"]}.')

        port = self._find_free_port()
        proc = subprocess.Popen(
            ['python', '-m', 'keboola_mcp_server.local_backend.appserver', str(port), str(self.data_dir)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        running[name] = {
            'pid': proc.pid,
            'port': port,
            'started_at': datetime.now(timezone.utc).isoformat(),
        }
        self._save_running_apps(running)
        return proc.pid, port

    def stop_data_app(self, name: str) -> bool:
        """Stop the HTTP server for the named app. Returns True if stopped."""
        running = self._running_apps()
        entry = running.pop(name, None)
        if not entry:
            return False
        try:
            os.kill(entry['pid'], signal.SIGTERM)
        except ProcessLookupError:
            pass
        self._save_running_apps(running)
        return True

    # ------------------------------------------------------------------
    # Platform migration (delegates to migrate.py)
    # ------------------------------------------------------------------

    async def migrate_to_keboola(
        self,
        storage_api_url: str,
        storage_token: str,
        table_names: list[str] | None = None,
        config_ids: list[str] | None = None,
        bucket_id: str = 'in.c-local',
    ):
        """Upload local tables and configs to Keboola Storage.

        Returns a MigrateResult.
        """
        from keboola_mcp_server.local_backend.migrate import migrate_to_keboola

        return await migrate_to_keboola(
            self.data_dir, storage_api_url, storage_token, table_names, config_ids, bucket_id
        )
