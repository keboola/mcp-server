# Keboola MCP Server

[![CI](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/keboola/keboola-mcp-server/branch/main/graph/badge.svg)](https://codecov.io/gh/keboola/keboola-mcp-server)
<a href="https://glama.ai/mcp/servers/72mwt1x862"><img width="380" height="200" src="https://glama.ai/mcp/servers/72mwt1x862/badge" alt="Keboola Explorer Server MCP server" /></a>
[![smithery badge](https://smithery.ai/badge/keboola-mcp-server)](https://smithery.ai/server/keboola-mcp-server)

A Model Context Protocol (MCP) server for interacting with Keboola Connection. This server provides tools for listing and accessing data from Keboola Storage API.

## Requirements

- Keboola Storage API token
- Snowflake or BigQuery Read Only Workspace

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
        "KBC_WORKSPACE_SCHEMA": "your-workspace-schema"
      }
    }
  }
}
```

Replace:
- `/path/to/keboola-mcp-server` with your actual path to the cloned repository
- `YOUR_REGION` with your Keboola region (e.g., `north-europe.azure`, etc.). You can remove it if your region is just `connection` explicitly
- `your-keboola-storage-token` with your Keboola Storage API token
- `your-workspace-schema` with your Snowflake schema or BigQuery dataset of your workspace

> Note: If you are using a specific version of Python (e.g. 3.11 due to some package compatibility issues), 
> you'll need to update the `command` into using that specific version, e.g. `/path/to/keboola-mcp-server/.venv/bin/python3.11`

> Note: The Workspace can be created in your Keboola project. It is the same project where you got 
> your Storage Token. The workspace will provide all the necessary connection parameters including the schema or dataset name.

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
      "url": "http://localhost:8000/sse?storage_token=YOUR-KEBOOLA-STORAGE-TOKEN&workspace_schema=YOUR-WORKSPACE-SCHEMA"
    }
  }
}
```

### Option 2a: Using Standard I/O (stdio)

```json
{
  "mcpServers": {
    "keboola": {
      "command": "/path/to/keboola-mcp-server/.venv/bin/python",
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
        "KBC_WORKSPACE_SCHEMA": "your-workspace-schema"         
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
        "/wsl_path/to/keboola-mcp-server/.venv/bin/python -m keboola_mcp_server.cli --transport stdio'"
      ]
    }
  }
}
```
- where `/wsl_path/to/keboola-mcp-server/.env` file contains environment variables:
```shell
export KBC_STORAGE_TOKEN="your-keboola-storage-token"
export KBC_WORKSPACE_SCHEMA="your-workspace-schema"
```

Replace:
- `/path/to/keboola-mcp-server` with your actual path to the cloned repository
- `YOUR_REGION` with your Keboola region (e.g., `north-europe.azure`, etc.). You can remove it if your region is just `connection` explicitly
- `your-keboola-storage-token` with your Keboola Storage API token
- `your-workspace-schema` with your Snowflake schema or BigQuery dataset of your workspace

After updating the configuration:
1. Restart Cursor AI
2. If you use the `sse` transport make sure to start your MCP server. You can do so by running this in the activated
   virtual environment where you built the server:
   ```
   /path/to/keboola-mcp-server/.venv/bin/python -m keboola_mcp_server --transport sse --api-url https://connection.YOUR_REGION.keboola.com
   ```
3. Cursor AI should be automatically detect your MCP server and enable it.

## BigQuery support

If your Keboola project uses BigQuery backend you will need to set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
in addition to `KBC_STORAGE_TOKEN` and `KBC_WORKSPACE_SCHEMA`.

1. Go to your Keboola BigQuery workspace and display its credentials (click `Connect` button).
2. Download the credentials file to your local disk. It is a plain JSON file.
3. Set the full path of the downloaded JSON credentials file to `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

This will give your MCP server instance permissions to access your BigQuery workspace in Google Cloud.

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
