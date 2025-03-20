"""Database connection management for Keboola MCP server."""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor

from keboola_mcp_server.config import Config

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection fails after trying all available patterns."""

    pass


@dataclass
class ConnectionPattern:
    """Represents a database connection pattern to try."""

    name: str
    get_database: callable
    required_config: List[str] = None

    def __post_init__(self):
        if self.required_config is None:
            self.required_config = []


@dataclass(frozen=True)
class TableFqn:
    db_name: str
    schema_name: str
    table_name: str

    @property
    def snowflake_fqn(self) -> str:
        """Returns the properly quoted Snowflake identifier."""
        return f'"{self.db_name}"."{self.schema_name}"."{self.table_name}"'


class DatabasePathManager:
    """Manages database paths and connections for Keboola tables."""

    def __init__(self, config: Config, connection_manager: "ConnectionManager"):
        self.config = config
        self.connection_manager = connection_manager
        self._table_fqn_cache: dict[str, TableFqn] = {}

    def get_table_fqn(self, table: dict[str, Any]) -> Optional[TableFqn]:
        """Gets the fully qualified name of a Keboola table."""
        # TODO: use a pydantic class for the 'table' param
        table_id = table["id"]
        if table_id in self._table_fqn_cache:
            return self._table_fqn_cache[table_id]

        try:
            with self.connection_manager.create_snowflake_connection() as conn:
                cursor = conn.cursor(DictCursor)

                if source_table := table.get("sourceTable"):
                    schema_name, table_name = source_table["id"].rsplit(sep=".", maxsplit=1)
                    source_project_id = source_table["project"]["id"]
                    result = cursor.execute(
                        f"show databases like '%_{source_project_id}';"
                    ).fetchone()
                    if result:
                        db_name = result["name"]
                    else:
                        raise ValueError(
                            f"No database found for Keboola project: {source_project_id}"
                        )

                else:
                    result = cursor.execute(
                        f'select CURRENT_DATABASE() as "current_database", CURRENT_SCHEMA() as "current_schema";'
                    ).fetchone()
                    db_name, schema_name = result["current_database"], result["current_schema"]
                    table_name = table["name"]

                fqn = TableFqn(db_name, schema_name, table_name)
                self._table_fqn_cache[table_id] = fqn

                return fqn

        except snowflake.connector.errors.Error:
            # most likely no connection to the DB, perhaps the database credentials were not specified
            return None


class ConnectionManager:
    """Manages database connections and connection string patterns for Keboola."""

    def __init__(self, config):
        self.config = config
        self.patterns = self._initialize_patterns()

    def _initialize_patterns(self) -> List[ConnectionPattern]:
        """Initialize the list of connection patterns to try."""
        return [
            ConnectionPattern(
                name="Configured Environment",
                get_database=lambda: self.config.snowflake_database,
                required_config=["snowflake_database"],
            ),
            ConnectionPattern(
                name="KEBOOLA Token Pattern",
                get_database=lambda: f"KEBOOLA_{self.config.storage_token.split('-')[0]}",
                required_config=["storage_token"],
            ),
            ConnectionPattern(
                name="SAPI Token Pattern",
                get_database=lambda: f"SAPI_{self.config.storage_token.split('-')[0]}",
                required_config=["storage_token"],
            ),
        ]

    def _validate_config_for_pattern(self, pattern: ConnectionPattern) -> bool:
        """Check if all required configuration is present for a pattern."""
        return all(
            hasattr(self.config, attr) and getattr(self.config, attr)
            for attr in pattern.required_config
        )

    @contextmanager
    def _create_test_connection(self, database: str):
        """Create a test connection with the given database name."""
        if not self.config.has_snowflake_config():
            raise ValueError("Snowflake credentials are not fully configured")

        conn = snowflake.connector.connect(
            account=self.config.snowflake_account,
            user=self.config.snowflake_user,
            password=self.config.snowflake_password,
            warehouse=self.config.snowflake_warehouse,
            database=database,
            schema=self.config.snowflake_schema,
            role=self.config.snowflake_role,
        )

        try:
            yield conn
        finally:
            conn.close()

    def _test_connection(self, database: str) -> bool:
        """Test if connection works with a simple query."""
        try:
            with self._create_test_connection(database) as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                return True
        except Exception as e:
            logger.debug(f"Connection test failed for {database}: {str(e)}")
            return False

    def find_working_connection(self) -> str:
        """
        Try different connection patterns and return the first working one.

        Returns:
            str: Working database name

        Raises:
            DatabaseConnectionError: If no working connection pattern is found
        """
        results = []

        for pattern in self.patterns:
            try:
                if not self._validate_config_for_pattern(pattern):
                    results.append((pattern.name, "N/A", "Missing required configuration"))
                    continue

                database = pattern.get_database()
                logger.debug(f"Testing ${pattern.name}: {database}")

                if self._test_connection(database):
                    logger.info(f"Successfully connected using {pattern.name}: {database}")
                    return database

                results.append((pattern.name, database, "Connection test failed"))
            except Exception as e:
                results.append((pattern.name, "N/A", str(e)))
                continue

        error_msg = "No working connection pattern found:\n"
        for pattern, db, error in results:
            error_msg += f" - {pattern} ({db}): {error}\n"
        raise DatabaseConnectionError(error_msg)

    @contextmanager
    def create_snowflake_connection(self) -> snowflake.connector.connection:
        """Create and return a Snowflake connection using configured credentials."""
        database = self.find_working_connection()
        yield snowflake.connector.connect(
            account=self.config.snowflake_account,
            user=self.config.snowflake_user,
            password=self.config.snowflake_password,
            warehouse=self.config.snowflake_warehouse,
            database=database,
            schema=self.config.snowflake_schema,
            role=self.config.snowflake_role,
        )
