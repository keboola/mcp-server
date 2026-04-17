"""Local backend: filesystem catalog and DuckDB-powered SQL execution."""

import csv
import logging
from pathlib import Path

LOG = logging.getLogger(__name__)


class LocalBackend:
    """Filesystem-backed data catalog with DuckDB SQL support."""

    def __init__(self, data_dir: str = './keboola_data'):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # Create tables/ eagerly so glob() never raises OSError on Python 3.13+.
        (self.data_dir / 'tables').mkdir(parents=True, exist_ok=True)

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

        return run_image_component(self.data_dir, component_image, parameters, input_tables, memory_limit)

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

        return run_source_component(self.data_dir, git_url, parameters, input_tables, memory_limit)
