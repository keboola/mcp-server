from mcp.server.fastmcp import FastMCP, Context
from kbcstorage.client import Client
import httpx
from typing import List, Dict, Any, Optional
import pandas as pd
from io import StringIO
import asyncio
import time
import os
import logging
import tempfile

# Set up logging
logger = logging.getLogger("keboola_mcp_server")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

class KeboolaClient:
    """Helper class to interact with Keboola Storage API"""
    def __init__(self, storage_api_token: str, storage_api_url: str = "https://connection.keboola.com"):
        self.token = storage_api_token
        # Ensure the base URL has a scheme
        if not storage_api_url.startswith(('http://', 'https://')):
            storage_api_url = f"https://{storage_api_url}"
        self.base_url = storage_api_url
        self.headers = {
            "X-StorageApi-Token": self.token,
            "Content-Type": "application/json",
            "Accept-encoding": "gzip"
        }
        # Initialize the official client for operations it handles well
        self.storage_client = Client(self.base_url, self.token)
    
    async def get(self, endpoint: str) -> Dict[str, Any]:
        """Make a GET request to Keboola Storage API"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v2/storage/{endpoint}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()

    async def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a POST request to Keboola Storage API"""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/storage/{endpoint}",
                headers=self.headers,
                json=data if data is not None else {}
            )
            response.raise_for_status()
            return response.json()

    async def download_table_data_async(self, table_id: str) -> str:
        """Download table data using the export endpoint"""
        try:
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                self.storage_client.tables.export_to_file(table_id, temp_file.name)
                temp_file.seek(0)
                data = temp_file.read()
            os.unlink(temp_file.name)  # Clean up the temp file
            return data
        except Exception as e:
            logger.error(f"Error downloading table {table_id}: {str(e)}")
            return f"Error downloading table: {str(e)}"

def initialize_client():
    """Initialize Keboola client from environment variables"""
    token = os.getenv("KEBOOLA_STORAGE_TOKEN")
    if not token:
        raise ValueError("KEBOOLA_STORAGE_TOKEN environment variable is required")
    
    # Get the API URL, defaulting to Keboola Connection
    api_url = os.getenv("KEBOOLA_STORAGE_API_URL")
    if not api_url:
        api_url = "https://connection.keboola.com"
    elif not api_url.startswith(('http://', 'https://')):
        api_url = f"https://{api_url}"
        
    logger.debug(f"Initializing client with API URL: {api_url}")
    return KeboolaClient(token, api_url)

# Initialize FastMCP server
mcp = FastMCP(
    "Keboola Explorer",
    dependencies=["keboola.storage-api-client", "httpx", "pandas"]
)

# Create Keboola client instance
try:
    keboola = initialize_client()
except Exception as e:
    logger.error(f"Failed to initialize Keboola client: {e}")
    raise
logger.info("Successfully initialized Keboola client")

# Resources
@mcp.resource("keboola://buckets")
async def list_buckets() -> str:
    """List all available buckets in Keboola project"""
    buckets = keboola.storage_client.buckets.list()
    return "\n".join(f"- {bucket['id']}: {bucket['name']} ({bucket.get('description', 'No description')})" 
                    for bucket in buckets)

@mcp.resource("keboola://buckets/{bucket_id}/tables")
async def list_bucket_tables(bucket_id: str) -> str:
    """List all tables in a specific bucket"""
    tables = keboola.storage_client.buckets.list_tables(bucket_id)
    return "\n".join(f"- {table['id']}: {table['name']} (Rows: {table.get('rowsCount', 'unknown')})" 
                    for table in tables)

@mcp.resource("keboola://components")
async def list_components() -> str:
    """List all available components and their configurations"""
    components = await keboola.get("components")
    return "\n".join(f"- {comp['id']}: {comp['name']}" 
                    for comp in components)

# Tools
@mcp.tool()
async def list_all_buckets() -> str:
    """List all buckets in the project with their basic information"""
    buckets = keboola.storage_client.buckets.list()
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
    """Get detailed information about a specific bucket"""
    bucket = keboola.storage_client.buckets.detail(bucket_id)
    return (f"Bucket Information:\n"
            f"ID: {bucket['id']}\n"
            f"Name: {bucket['name']}\n"
            f"Description: {bucket.get('description', 'No description')}\n"
            f"Created: {bucket['created']}\n"
            f"Tables Count: {bucket.get('tablesCount', 0)}\n"
            f"Data Size Bytes: {bucket.get('dataSizeBytes', 0)}")

@mcp.tool()
async def get_table_preview(table_id: str, limit: int = 100) -> str:
    """Get a preview of data from a specific table as CSV"""
    try:
        # Create a temporary directory to store the file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Get just the table name from the table_id (last part after dot)
            table_name = table_id.split('.')[-1]
            # Export the table data
            keboola.storage_client.tables.export_to_file(table_id, temp_dir)
            # The actual file will be in temp_dir with just the table name
            actual_file = os.path.join(temp_dir, table_name)
            df = pd.read_csv(actual_file)
            df = df.head(limit)
            # Format the data in a way that's easy for Claude to analyze
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
    """Get detailed information about a specific table"""
    table = keboola.storage_client.tables.detail(table_id)
    return (f"Table Information:\n"
            f"ID: {table['id']}\n"
            f"Name: {table.get('name', 'N/A')}\n"
            f"Primary Key: {', '.join(table.get('primaryKey', []))}\n"
            f"Created: {table.get('created', 'N/A')}\n"
            f"Row Count: {table.get('rowsCount', 'N/A')}\n"
            f"Data Size Bytes: {table.get('dataSizeBytes', 'N/A')}\n"
            f"Columns: {', '.join(table.get('columns', []))}")

@mcp.tool()
async def list_component_configs(component_id: str) -> str:
    """List all configurations for a specific component"""
    configs = await keboola.get(f"components/{component_id}/configs")
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
    """List all tables in a specific bucket with their basic information"""
    tables = keboola.storage_client.buckets.list_tables(bucket_id)
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
    """Export a table as CSV for analysis in Claude"""
    try:
        # Create a temporary directory to store the file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Get just the table name from the table_id (last part after dot)
            table_name = table_id.split('.')[-1]
            # Export the table data
            keboola.storage_client.tables.export_to_file(table_id, temp_dir)
            # The actual file will be in temp_dir with just the table name
            actual_file = os.path.join(temp_dir, table_name)
            df = pd.read_csv(actual_file)
            # Format the data in a way that's easy for Claude to analyze
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

if __name__ == "__main__":
    mcp.run(transport="stdio")