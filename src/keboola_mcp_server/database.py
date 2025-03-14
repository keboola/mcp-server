"""Database connection management for Keboola MCP server."""

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List

import snowflake.connector

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


class DatabasePathManager:
    """Manages database paths and connections for Keboola tables."""

    def __init__(self, config, connection_manager):
        self.config = config
        self.connection_manager = connection_manager
        self._db_path_cache = {}

    def get_current_db(self) -> str:
        """
        Get the current database path, using the connection manager's patterns.

        Returns:
            str: The database path to use
        """
        try:
            # Try to get a working DB connection from our patterns
            return self.connection_manager.find_working_connection()
        except Exception as e:
            logger.warning(
                f"Failed to get working connection, falling back to token-based path: {e}"
            )
            return f"KEBOOLA_{self.config.storage_token.split('-')[0]}"

    def get_table_db_path(self, table: Dict[str, Any]) -> str:
        """
        Get the database path for a specific table.

        Args:
            table: Dictionary containing table information

        Returns:
            str: The full database path for the table
        """
        table_id = table["id"]
        if table_id in self._db_path_cache:
            return self._db_path_cache[table_id]

        db_path = self.get_current_db()
        table_name = table["name"]
        table_path = table["id"]

        if table.get("sourceTable"):
            db_path = f"KEBOOLA_{table['sourceTable']['project']['id']}"
            table_path = table["sourceTable"]["id"]

        table_identifier = f'"{db_path}"."{".".join(table_path.split(".")[:-1])}"."{table_name}"'

        self._db_path_cache[table_id] = table_identifier
        return table_identifier


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

    def create_snowflake_connection(self) -> snowflake.connector.connection:
        """Create and return a Snowflake connection using configured credentials.
        Raises:
            ValueError: If credentials are not fully configured or connection fails
        """
        try:
            database = self.find_working_connection()
            conn = snowflake.connector.connect(
                account=self.config.snowflake_account,
                user=self.config.snowflake_user,
                password=self.config.snowflake_password,
                warehouse=self.config.snowflake_warehouse,
                database=database,
                schema=self.config.snowflake_schema,
                role=self.config.snowflake_role,
            )

            return conn
        except DatabaseConnectionError as e:
            raise ValueError(f"Failed to find working database connection: {str(e)}")
        except Exception as e:
            raise ValueError(f"Failed to create Snowflake connection: {str(e)}")
