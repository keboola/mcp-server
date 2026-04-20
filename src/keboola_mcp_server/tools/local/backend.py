"""Local backend: filesystem catalog and DuckDB-powered SQL execution."""

import csv
import logging
from pathlib import Path

LOG = logging.getLogger(__name__)


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
                f'CREATE OR REPLACE TABLE "{table_name}" AS ' 'SELECT * FROM read_csv_auto(?)',
                [str(csv_file)],
            )

        # Use cursor-based result formatting (no pandas dependency).
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
        from keboola_mcp_server.tools.local.config import save_config

        return save_config(self.configs_dir, config)

    def list_configs(self):
        """List all saved ComponentConfigs."""
        from keboola_mcp_server.tools.local.config import list_configs

        return list_configs(self.configs_dir)

    def load_config(self, config_id: str):
        """Load a saved ComponentConfig by ID."""
        from keboola_mcp_server.tools.local.config import load_config

        return load_config(self.configs_dir, config_id)

    def delete_config(self, config_id: str) -> bool:
        """Delete a saved ComponentConfig. Returns True if deleted."""
        from keboola_mcp_server.tools.local.config import delete_config

        return delete_config(self.configs_dir, config_id)

    # ------------------------------------------------------------------
    # Docker component execution (delegates to tools/local/docker.py)
    # ------------------------------------------------------------------

    def setup_component(self, git_url: str, force_rebuild: bool = False):
        """Clone a component repo and build its Docker image.

        Returns a ComponentSetupResult with the clone path and optional schema.
        """
        from keboola_mcp_server.tools.local.docker import setup_component

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
    ):
        """Run a Keboola component from a Docker registry image.

        Returns a ComponentRunResult.
        """
        from keboola_mcp_server.tools.local.docker import run_image_component

        return run_image_component(
            self.data_dir, component_image, parameters, input_tables, memory_limit, network=self.docker_network
        )

    def run_source_component(
        self,
        git_url: str,
        parameters: dict,
        input_tables: list[str] | None = None,
        memory_limit: str = '4g',
    ):
        """Clone + build + run a component from source via docker compose.

        Returns a ComponentRunResult.
        """
        from keboola_mcp_server.tools.local.docker import run_source_component

        return run_source_component(
            self.data_dir, git_url, parameters, input_tables, memory_limit, network=self.docker_network
        )

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
        from keboola_mcp_server.tools.local.migrate import migrate_to_keboola

        return await migrate_to_keboola(
            self.data_dir, storage_api_url, storage_token, table_names, config_ids, bucket_id
        )
