"""MCP server implementation for Keboola Connection."""

import logging
import os
import tempfile
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from mcp.server.fastmcp import FastMCP

from .client import KeboolaClient
from .config import Config

logger = logging.getLogger(__name__)


def create_server(config: Optional[Config] = None) -> FastMCP:
    """Create and configure the MCP server.

    Args:
        config: Server configuration. If None, loads from environment.

    Returns:
        Configured FastMCP server instance
    """
    if config is None:
        config = Config.from_env()
    config.validate()

    # Configure logging
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(config.log_level)

    # Initialize FastMCP server
    mcp = FastMCP(
        "Keboola Explorer", dependencies=["keboola.storage-api-client", "httpx", "pandas"]
    )

    # Create Keboola client instance
    try:
        keboola = KeboolaClient(config.storage_token, config.storage_api_url)
    except Exception as e:
        logger.error(f"Failed to initialize Keboola client: {e}")
        raise
    logger.info("Successfully initialized Keboola client")

    # Resources
    @mcp.resource("keboola://buckets")
    async def list_buckets() -> str:
        """List all available buckets in Keboola project."""
        buckets = cast(List[Dict[str, Any]], keboola.storage_client.buckets.list())
        return "\n".join(
            f"- {bucket['id']}: {bucket['name']} ({bucket.get('description', 'No description')})"
            for bucket in buckets
        )

    @mcp.resource("keboola://buckets/{bucket_id}/tables")
    async def list_bucket_tables(bucket_id: str) -> str:
        """List all tables in a specific bucket."""
        tables = cast(List[Dict[str, Any]], keboola.storage_client.buckets.list_tables(bucket_id))
        return "\n".join(
            f"- {table['id']}: {table['name']} (Rows: {table.get('rowsCount', 'unknown')})"
            for table in tables
        )

    @mcp.resource("keboola://components")
    async def list_components() -> str:
        """List all available components and their configurations."""
        components = cast(List[Dict[str, Any]], await keboola.get("components"))
        return "\n".join(f"- {comp['id']}: {comp['name']}" for comp in components)

    # Tools
    @mcp.tool()
    async def list_all_buckets() -> str:
        """List all buckets in the project with their basic information."""
        buckets = cast(List[Dict[str, Any]], keboola.storage_client.buckets.list())
        return "\n".join(
            f"Bucket: {bucket['id']}\n"
            f"Name: {bucket.get('name', 'N/A')}\n"
            f"Description: {bucket.get('description', 'N/A')}\n"
            f"Stage: {bucket.get('stage', 'N/A')}\n"
            f"Created: {bucket.get('created', 'N/A')}\n"
            f"---"
            for bucket in buckets
        )

    @mcp.tool()
    async def get_bucket_info(bucket_id: str) -> str:
        """Get detailed information about a specific bucket."""
        bucket = cast(Dict[str, Any], keboola.storage_client.buckets.detail(bucket_id))
        return (
            f"Bucket Information:\n"
            f"ID: {bucket['id']}\n"
            f"Name: {bucket['name']}\n"
            f"Description: {bucket.get('description', 'No description')}\n"
            f"Created: {bucket['created']}\n"
            f"Tables Count: {bucket.get('tablesCount', 0)}\n"
            f"Data Size Bytes: {bucket.get('dataSizeBytes', 0)}"
        )

    @mcp.tool()
    async def get_table_preview(table_id: str, limit: int = 100) -> str:
        """Get a preview of data from a specific table as CSV."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                parts = table_id.split(".")
                table_name = parts[-1] if parts else table_id
                keboola.storage_client.tables.export_to_file(table_id, temp_dir)
                actual_file = os.path.join(temp_dir, table_name)
                df = pd.read_csv(actual_file)
                df = df.head(limit)
                return (
                    "Here's a preview of the data in CSV format:\n\n"
                    f"Number of rows: {len(df)}\n"
                    f"Columns: {', '.join(df.columns)}\n\n"
                    "```csv\n"
                    f"{df.to_csv(index=False)}"
                    "```\n\n"
                    "You can analyze this data to create visualizations or perform statistical analysis."
                )
        except Exception as e:
            logger.error(f"Error previewing table: {str(e)}")
            return f"Error previewing table: {str(e)}"

    @mcp.tool()
    async def get_table_info(table_id: str) -> str:
        """Get detailed information about a specific table."""
        table = cast(Dict[str, Any], keboola.storage_client.tables.detail(table_id))
        return (
            f"Table Information:\n"
            f"ID: {table['id']}\n"
            f"Name: {table.get('name', 'N/A')}\n"
            f"Primary Key: {', '.join(table.get('primaryKey', []))}\n"
            f"Created: {table.get('created', 'N/A')}\n"
            f"Row Count: {table.get('rowsCount', 'N/A')}\n"
            f"Data Size Bytes: {table.get('dataSizeBytes', 'N/A')}\n"
            f"Columns: {', '.join(table.get('columns', []))}"
        )

    @mcp.tool()
    async def list_component_configs(component_id: str) -> str:
        """List all configurations for a specific component."""
        configs = cast(
            List[Dict[str, Any]], await keboola.get(f"components/{component_id}/configs")
        )
        return "\n".join(
            f"Configuration: {config['id']}\n"
            f"Name: {config['name']}\n"
            f"Description: {config.get('description', 'No description')}\n"
            f"Created: {config['created']}\n"
            f"---"
            for config in configs
        )

    @mcp.tool()
    async def list_bucket_tables_tool(bucket_id: str) -> str:
        """List all tables in a specific bucket with their basic information."""
        tables = cast(List[Dict[str, Any]], keboola.storage_client.buckets.list_tables(bucket_id))
        return "\n".join(
            f"Table: {table['id']}\n"
            f"Name: {table.get('name', 'N/A')}\n"
            f"Rows: {table.get('rowsCount', 'N/A')}\n"
            f"Size: {table.get('dataSizeBytes', 'N/A')} bytes\n"
            f"Columns: {', '.join(table.get('columns', []))}\n"
            f"---"
            for table in tables
        )

    @mcp.tool()
    async def export_table_to_csv(table_id: str) -> str:
        """Export a table as CSV for analysis in Claude."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                parts = table_id.split(".")
                table_name = parts[-1] if parts else table_id
                keboola.storage_client.tables.export_to_file(table_id, temp_dir)
                actual_file = os.path.join(temp_dir, table_name)
                df = pd.read_csv(actual_file)
                return (
                    "Here's the complete table data in CSV format:\n\n"
                    f"Number of rows: {len(df)}\n"
                    f"Columns: {', '.join(df.columns)}\n\n"
                    "```csv\n"
                    f"{df.to_csv(index=False)}"
                    "```\n\n"
                    "You can analyze this data to create visualizations or perform statistical analysis. "
                    "The data includes numerical columns that can be used for metrics and categorical columns for grouping."
                )
        except Exception as e:
            logger.error(f"Error exporting table: {str(e)}")
            return f"Error exporting table: {str(e)}"

    return mcp
