# Keboola MCP Server

[![CI](https://github.com/jordanburger/keboola-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/jordanburger/keboola-mcp-server/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/jordanrburger/keboola-mcp-server/branch/master/graph/badge.svg?token=4JMT6ZBZMO)](https://codecov.io/gh/jordanrburger/keboola-mcp-server)

<a href="https://glama.ai/mcp/servers/72mwt1x862"><img width="380" height="200" src="https://glama.ai/mcp/servers/72mwt1x862/badge" alt="Keboola Explorer Server MCP server" /></a>

A Model Context Protocol (MCP) server for interacting with Keboola Connection. This server provides tools for listing and accessing data from Keboola Storage API.

## Installation

Install from source:

```bash
pip install .
```

For development:

```bash
pip install -e ".[dev]"
```

## Usage

The server requires a Keboola Storage API token to be set in the environment:

```bash
export KBC_STORAGE_TOKEN=your-token
```

Optional environment variables:
- `KBC_STORAGE_API_URL`: Keboola Storage API URL (defaults to https://connection.keboola.com)
- `KBC_LOG_LEVEL`: Logging level (defaults to INFO)

Run the server:

```bash
keboola-mcp-server
```

Or with custom options:

```bash
keboola-mcp-server --transport stdio --log-level DEBUG --api-url https://connection.north-europe.azure.keboola.com
```

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
