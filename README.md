# Keboola MCP Server

[![CI](https://github.com/jordanburger/keboola-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/jordanburger/keboola-mcp-server/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/jordanburger/keboola-mcp-server/branch/main/graph/badge.svg)](https://codecov.io/gh/jordanburger/keboola-mcp-server)
<a href="https://glama.ai/mcp/servers/72mwt1x862"><img width="380" height="200" src="https://glama.ai/mcp/servers/72mwt1x862/badge" alt="Keboola Explorer Server MCP server" /></a>
[![smithery badge](https://smithery.ai/badge/keboola-mcp-server)](https://smithery.ai/server/keboola-mcp-server)

A Model Context Protocol (MCP) server for interacting with Keboola Connection. This server provides tools for listing and accessing data from Keboola Storage API.

## Installation

### Installing via Smithery

To install Keboola Explorer for Claude Desktop automatically via [Smithery](https://smithery.ai/server/keboola-mcp-server):

```bash
npx -y @smithery/cli install keboola-mcp-server --client claude
```

### Manual Installation
First, clone the repository and create a virtual environment:

```bash
git clone https://github.com/jordanburger/keboola-mcp-server.git
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
        "PYTHONPATH": "/path/to/keboola-mcp-server/src"
      }
    }
  }
}
```

Replace:
- `/path/to/keboola-mcp-server` with your actual path to the cloned repository
- `your-keboola-storage-token` with your Keboola Storage API token
- `YOUR_REGION` with your Keboola region (e.g., `north-europe.azure`, `connection`, etc.)

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
