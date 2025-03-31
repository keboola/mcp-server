# Keboola MCP Server

[![CI](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/keboola/keboola-mcp-server/branch/main/graph/badge.svg)](https://codecov.io/gh/keboola/keboola-mcp-server)
<a href="https://glama.ai/mcp/servers/72mwt1x862"><img width="380" height="200" src="https://glama.ai/mcp/servers/72mwt1x862/badge" alt="Keboola Explorer Server MCP server" /></a>
[![smithery badge](https://smithery.ai/badge/keboola-mcp-server)](https://smithery.ai/server/keboola-mcp-server)

A Model Context Protocol (MCP) server for interacting with Keboola Connection. This server provides tools for listing and accessing data from Keboola Storage API.

## Requirements

- Keboola Storage API token
- Snowflake Read Only Workspace

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
        "keboola_mcp_server",
        "--api-url",
        "https://connection.YOUR_REGION.keboola.com"
      ],
      "env": {
        "KBC_STORAGE_TOKEN": "your-keboola-storage-token",
        "KBC_WORKSPACE_USER": "your-workspace-user"
      }
    }
  }
}
```

Replace:
- `/path/to/keboola-mcp-server` with your actual path to the cloned repository
- `your-keboola-storage-token` with your Keboola Storage API token
- `YOUR_REGION` with your Keboola region (e.g., `north-europe.azure`, etc.). You can remove it if your region is just `connection` explicitly
- `your-workspace-user` with your Snowflake workspace username

> Note: If you are using a specific version of Python (e.g. 3.11 due to some package compatibility issues), 
> you'll need to update the `command` into using that specific version, e.g. `/path/to/keboola-mcp-server/.venv/bin/python3.11`

> Note: The Read Only Snowflake Workspace can be created in your Keboola project. It is the same project where you got 
> your Storage Token. The workspace will provide all the necessary Snowflake connection parameters including the username.

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
      "url": "http://localhost:8000/sse?storage_token=YOUR-KEBOOLA-STORAGE-TOKEN&workspace_user=YOUR-WORKSPACE-USER"
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
        "keboola_mcp_server",
        "--transport",
        "stdio",
         "--api-url",
         "https://connection.YOUR_REGION.keboola.com"
      ],
      "env": {
        "KBC_STORAGE_TOKEN": "your-keboola-storage-token", 
        "KBC_WORKSPACE_USER": "your-workspace-user"         
      }
    }
  }
}
```

### Option 2b: Using WSL Standard I/O (wsl stdio)
When running the MCP server from Windows Subsystem for Linux with Cursor AI, use this.

```json
{
  "mcpServers": {
    "keboola": {
      "command": "wsl.exe",
      "args": [
        "bash",
        "-c",
        "'source /wsl_path/to/keboola-mcp-server/.env",
        "&&",
        "/wsl_path/to/keboola-mcp-server/venv/bin/python -m keboola_mcp_server.cli --transport stdio'"
      ]
    }
  }
}
```
- where `/wsl_path/to/keboola-mcp-server/.env` file contains environment variables:
```shell
export KBC_STORAGE_TOKEN="your-keboola-storage-token"
export KBC_SNOWFLAKE_ACCOUNT="your-snowflake-account"
export KBC_SNOWFLAKE_USER="your-snowflake-user"
export KBC_SNOWFLAKE_PASSWORD="your-snowflake-password"
export KBC_SNOWFLAKE_WAREHOUSE="your-snowflake-warehouse"
export KBC_SNOWFLAKE_DATABASE="your-snowflake-database"
export KBC_SNOWFLAKE_SCHEMA="your-snowflake-schema"
export KBC_SNOWFLAKE_ROLE="your-snowflake-role"
```

Replace all placeholder values (`your_*`) with your actual Keboola and Snowflake credentials. These can be obtained from your Keboola project's Read Only Snowflake Workspace.
Replace `YOUR_REGION` with your Keboola region (e.g., `north-europe.azure`, etc.). You can remove it if your region is just `connection` explicitly.

After updating the configuration:
1. Restart Cursor AI
2. If you use the `sse` transport make sure to start your MCP server. You can do so by running this in the activated
   virtual environment where you built the server:
   ```
   /path/to/keboola-mcp-server/venv/bin/python -m keboola_mcp_server --transport sse --api-url https://connection.YOUR_REGION.keboola.com
   ```
3. Cursor AI should be automatically detect your MCP server and enable it.

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
