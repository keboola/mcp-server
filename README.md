# Keboola Explorer MCP Server

A Claude MCP server for exploring and managing your Keboola Connection project. This server provides tools and resources to interact with Keboola's Storage API, allowing you to browse buckets, tables, and components.

## Setup

1. Install the required dependencies:
```bash
pip3 install fastmcp httpx pandas
```

2. Set up your environment variables:
```bash
export KEBOOLA_STORAGE_TOKEN="your-storage-api-token"
export KEBOOLA_STORAGE_API_URL="https://connection.keboola.com"  # or your specific endpoint
```

## Available Tools

### Bucket Management
- `list_all_buckets`: Lists all buckets in your project with their basic information
- `get_bucket_info`: Get detailed information about a specific bucket

### Table Management
- `list_bucket_tables_tool`: Lists all tables in a specific bucket
- `get_table_info`: Get detailed information about a specific table
- `get_table_preview`: Preview data from a specific table (up to 100 rows by default)

### Component Management
- `list_component_configs`: List all configurations for a specific component

## Available Resources

- `keboola://buckets`: List all available buckets
- `keboola://buckets/{bucket_id}/tables`: List all tables in a specific bucket
- `keboola://components`: List all available components

## Example Usage

```python
# List all buckets
result = await list_all_buckets()

# Get information about a specific bucket
bucket_info = await get_bucket_info("in.c-my-bucket")

# List tables in a bucket
tables = await list_bucket_tables_tool("in.c-my-bucket")

# Preview table data
preview = await get_table_preview("in.c-my-bucket.my-table", limit=10)
```

## Error Handling

The server includes error handling for:
- Missing environment variables
- API authentication issues
- Invalid bucket or table IDs
- Network connectivity problems

## Security

- Your Storage API token is required and should be kept secure
- The token is passed securely through environment variables
- All API requests are made over HTTPS
