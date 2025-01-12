# Keboola Explorer MCP Server

A Claude MCP server for exploring and managing your Keboola Connection project. This server provides tools and resources to interact with Keboola's Storage API, allowing you to browse buckets, tables, and components.

## Prerequisites

- [Claude Desktop](https://claude.ai/download) installed and updated to the latest version
- Python 3.10 or higher
- A Keboola Storage API token

## Installation

1. First, install the `uv` package manager:
```bash
# On macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

2. Clone the repository and set up the environment:
```bash
# Clone the repository
git clone https://github.com/jordanrburger/keboola-mcp-server.git
cd keboola-mcp-server

# Create virtual environment
uv venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install dependencies
uv add "mcp[cli]" httpx pandas
```

## Claude Desktop Configuration

1. Find the full path to your `uv` executable:
```bash
which uv  # On macOS/Linux
where uv  # On Windows
```

2. Create or edit the Claude Desktop configuration file:

**Location**:
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`

**Content**:
```json
{
  "mcpServers": {
    "keboola": {
      "command": "/absolute/path/to/uv",
      "args": [
        "--directory",
        "/absolute/path/to/your/keboola-mcp-server",
        "run",
        "server.py"
      ],
      "env": {
        "KEBOOLA_STORAGE_TOKEN": "your-token-here",
        "KEBOOLA_STORAGE_API_URL": "https://connection.keboola.com"  // Optional, defaults to this value
      }
    }
  }
}
```

Replace:
- `/absolute/path/to/uv` with the output from `which uv` or `where uv`
- `/absolute/path/to/your/keboola-mcp-server` with the full path to your project directory
- `your-token-here` with your Keboola Storage API token

## Starting the Server

1. After setting up the configuration:
   - Completely quit Claude Desktop (don't just close the window)
   - Restart Claude Desktop

2. Look for the hammer icon in the bottom right corner of Claude's input box. This indicates the server is connected successfully.

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

Once connected, you can ask Claude to perform various operations. Here are some example prompts:
- "List all buckets in my Keboola project"
- "Show me the contents of table X"
- "What configurations are available for component Y?"

For programmatic usage:
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

## Troubleshooting

### Common Issues

1. **No hammer icon appears**:
   - Verify your configuration file is in the correct location
   - Check that all paths in the config are absolute paths
   - Make sure `uv` is using its full path
   - Restart Claude Desktop completely

2. **Connection errors**:
   - Check Claude's logs at:
     - macOS: `~/Library/Logs/Claude/mcp*.log`
     - Windows: `%APPDATA%\Claude\logs\mcp*.log`
   - Verify your Keboola token is valid
   - Ensure all dependencies are installed

3. **Server startup issues**:
   Test the server directly:
   ```bash
   KEBOOLA_STORAGE_TOKEN=your-token-here mcp dev server.py
   ```

### Getting Logs

To view logs in real-time:
```bash
# On macOS/Linux
tail -n 50 -f ~/Library/Logs/Claude/mcp*.log

# On Windows
type "%APPDATA%\Claude\logs\mcp*.log"
```

## Security

- Your Storage API token is required and should be kept secure
- The token is passed securely through environment variables
- All API requests are made over HTTPS

## Error Handling

The server includes error handling for:
- Missing environment variables
- API authentication issues
- Invalid bucket or table IDs
- Network connectivity problems
