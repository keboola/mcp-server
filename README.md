# Keboola MCP Server

[![CI](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/keboola/keboola-mcp-server/branch/main/graph/badge.svg)](https://codecov.io/gh/keboola/keboola-mcp-server)
<a href="https://glama.ai/mcp/servers/72mwt1x862"><img width="380" height="200" src="https://glama.ai/mcp/servers/72mwt1x862/badge" alt="Keboola Explorer Server MCP server" /></a>
[![smithery badge](https://smithery.ai/badge/keboola-mcp-server)](https://smithery.ai/server/keboola-mcp-server)

A Model Context Protocol (MCP) server for interacting with Keboola Connection. This server provides tools for listing and accessing data from Keboola Storage API.

## Requirements

- Keboola Storage API token
- Snowflake Read Only Workspace

> Note: The Snowflake package doesn't work with the latest version of Python. If you're using Python 3.12 and above, you'll need to downgrade to Python 3.11.

## Installation

### Installing via Smithery

To install Keboola Explorer for Claude Desktop automatically via [Smithery](https://smithery.ai/server/keboola-mcp-server):

```bash
npx -y @smithery/cli install keboola-mcp-server --client claude
```

### Manual Installation

First, clone the repository and create a virtual environment:

```bash
git clone https://github.com/keboola/keboola-mcp-server.git
cd keboola-mcp-server
python3 -m venv .venv
source .venv/bin/activate
```

Install the package in development mode:

```bash
pip3 install -e .
```

For development dependencies:

```bash
pip3 install -e ".[dev]"
```

## Claude Desktop Setup

To use this server with Claude Desktop, follow these steps:

1. Create or edit the Claude Desktop configuration file:
   - **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

2. Add the following configuration (adjust paths according to your setup):

```json
{
  "mcpServers": {
    "keboola": {
      "command": "/path/to/keboola-mcp-server/.venv/bin/python",
      "args": [
        "-m",
        "keboola_mcp_server.cli",
        "--log-level",
        "DEBUG",
        "--api-url",
        "https://connection.YOUR_REGION.keboola.com"
      ],
      "env": {
        "KBC_STORAGE_TOKEN": "your-keboola-storage-token",
        "PYTHONPATH": "/path/to/keboola-mcp-server/src",
        "KBC_SNOWFLAKE_ACCOUNT": "your-snowflake-account",
        "KBC_SNOWFLAKE_USER": "your-snowflake-user",
        "KBC_SNOWFLAKE_PASSWORD": "your-snowflake-password",
        "KBC_SNOWFLAKE_WAREHOUSE": "your-snowflake-warehouse",
        "KBC_SNOWFLAKE_DATABASE": "your-snowflake-database",
        "KBC_SNOWFLAKE_SCHEMA": "your-snowflake-schema",
        "KBC_SNOWFLAKE_ROLE": "your-snowflake-role"
      }
    }
  }
}
```

Replace:
- `/path/to/keboola-mcp-server` with your actual path to the cloned repository
- `your-keboola-storage-token` with your Keboola Storage API token
- `YOUR_REGION` with your Keboola region (e.g., `north-europe.azure`, etc.). You can remove it if you region is just `connection` explicitly
- `your-snowflake-account` with your Snowflake account identifier
- `your-snowflake-user` with your Snowflake username
- `your-snowflake-password` with your Snowflake password
- `your-snowflake-warehouse` with your Snowflake warehouse name
- `your-snowflake-database` with your Snowflake database name
- `your-snowflake-schema` with your Snowflake schema name
- `your-snowflake-role` with your Snowflake role name

> Note: If you are using a specific version of Python (e.g. 3.11 due to some package compatibility issues), you'll need to update the `command` into using that specific version, e.g. `/path/to/keboola-mcp-server/.venv/bin/python3.11`

Note: The Snowflake credentials can be obtained by creating a Read Only Snowflake Workspace in your Keboola project (the same project where you got your Storage Token). The workspace will provide all the necessary Snowflake connection parameters.

3. After updating the configuration:
   - Completely quit Claude Desktop (don't just close the window)
   - Restart Claude Desktop
   - Look for the hammer icon in the bottom right corner, indicating the server is connected

### Troubleshooting

If you encounter connection issues:
1. Check the logs in Claude Desktop for any error messages
2. Verify your Keboola Storage API token is correct
3. Ensure all paths in the configuration are absolute paths
4. Confirm the virtual environment is properly activated and all dependencies are installed
5. Make sure the PYTHONPATH points to the `src` directory

## Cursor AI Setup

To use this server with Cursor AI, you have two options for configuring the transport method: Server-Sent Events (SSE) or Standard I/O (stdio).

1. Create or edit the Cursor AI configuration file:
   - Location: `~/.cursor/mcp.json`

2. Add one of the following configurations (or all) based on your preferred transport method:

### Option 1: Using Server-Sent Events (SSE)

```json
{
  "mcpServers": {
    "keboola": {
      "url": "http://localhost:8000/sse?storage_token=YOUR_STORAGE_TOKEN&snowflake_account=YOUR_ACCOUNT&snowflake_user=YOUR_USER&snowflake_password=YOUR_PASSWORD&snowflake_database=YOUR_DATABASE&snowflake_schema=YOUR_SCHEMA&snowflake_warehouse=YOUR_WAREHOUSE"
    }
  }
}
```

### Option 2a: Using Standard I/O (stdio)

```json
{
  "mcpServers": {
    "keboola": {
      "command": "/path/to/keboola-mcp-server/venv/bin/python",
      "args": [
        "-m",
        "keboola_mcp_server.cli",
        "--transport",
        "stdio"
      ],
      "env": {
        "KBC_STORAGE_TOKEN": "your-keboola-storage-token",
        "KBC_SNOWFLAKE_ACCOUNT": "your-snowflake-account",
        "KBC_SNOWFLAKE_USER": "your-snowflake-user",
        "KBC_SNOWFLAKE_PASSWORD": "your-snowflake-password",
        "KBC_SNOWFLAKE_DATABASE": "your-snowflake-database",
        "KBC_SNOWFLAKE_SCHEMA": "your-snowflake-schema",
        "KBC_SNOWFLAKE_WAREHOUSE": "your-snowflake-warehouse"
      }
    }
  }
}
```

### Option 2b: Using WSL Standard I/O (wsl stdio)
When running the MCP server from Windows Subsystem for Linux, use this.

```json
{
  "mcpServers": {
    "keboola_wsl": {
        "command": "wsl.exe",
        "args": [
            "bash",
            "-c",
            "'source ~/wsl/path/to/the/script/run_mcp.sh'"
        ],
    }
  }
}
```

- where `run_mcp.sh` includes:
```shell
#!/bin/bash
source /wsl/path/to/the/file/.env
/wsl/path/to/keboola-mcp-server/.venv/bin/python -m keboola_mcp_server.cli --log-level DEBUG --api-url https://connection.keboola.com
```

- and where `.env` file contains following lines

```shell
export KBC_STORAGE_TOKEN="your-keboola-storage-token"
export PYTHONPATH="/wsl/path/to/your/project/src"
export KBC_SNOWFLAKE_ACCOUNT="your-snowflake-account"
export KBC_SNOWFLAKE_USER="your-snowflake-user"
export KBC_SNOWFLAKE_PASSWORD="your-snowflake-password"
export KBC_SNOWFLAKE_WAREHOUSE="your-snowflake-warehouse"
export KBC_SNOWFLAKE_DATABASE="your-snowflake-database"
export KBC_SNOWFLAKE_SCHEMA="your-snowflake-schema"
export KBC_SNOWFLAKE_ROLE="your-snowflake-role"
```

Replace all placeholder values (YOUR_*) with your actual Keboola and Snowflake credentials. These can be obtained from your Keboola project's Read Only Snowflake Workspace.

After updating the configuration:
1. Restart Cursor AI
2. The MCP server should be automatically detected and available for use

## Available Tools

The server provides the following tools for interacting with Keboola Connection:

- List buckets and tables
- Get bucket and table information
- Preview table data
- Export table data to CSV
- List components and configurations

## Development

Run tests:

```bash
pytest
```

Format code:

```bash
black .
isort .
```

Type checking:

```bash
mypy .
```

## License

MIT License - see LICENSE file for details.
