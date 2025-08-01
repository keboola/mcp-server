[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/keboola/mcp-server)
[![smithery badge](https://smithery.ai/badge/keboola-mcp-server)](https://smithery.ai/server/keboola-mcp-server)


# Keboola MCP Server

> Connect your AI agents, MCP clients (**Cursor**, **Claude**, **Windsurf**, **VS Code** ...) and other AI assistants to Keboola. Expose data, transformations, SQL queries, and job triggers—no glue code required. Deliver the right data to agents when and where they need it.

## Overview

Keboola MCP Server is an open-source bridge between your Keboola project and modern AI tools. It turns Keboola features—like storage access, SQL transformations, and job triggers—into callable tools for Claude, Cursor, CrewAI, LangChain, Amazon Q, and more.

## 🚀 Quick Start: Remote MCP Server (Easiest Way)

The easiest way to use Keboola MCP Server is through our **Remote MCP Server**. This hosted solution eliminates the need for local setup, configuration, or installation.

### What is the Remote MCP Server?

Our remote server is hosted on every multi-tenant Keboola stack and supports OAuth authentication. You can connect to it from any AI assistant that supports remote SSE connection and OAuth authentication.

### How to Connect

1. **Get your remote server URL**: Navigate to your Keboola Project Settings → `MCP Server` tab
2. **Copy the server URL**: It will look like `https://mcp.<YOUR_REGION>.keboola.com/sse`
3. **Configure your AI assistant**: Paste the URL into your AI assistant's MCP settings
4. **Authenticate**: You'll be prompted to authenticate with your Keboola account and select your project

### Supported Clients

- **[Cursor](https://cursor.com)**: Use the "Install In Cursor" button in your project's MCP Server settings or click
  this button
  [![Install MCP Server](https://cursor.com/deeplink/mcp-install-dark.svg)](https://cursor.com/install-mcp?name=keboola&config=eyJ1cmwiOiJodHRwczovL21jcC51cy1lYXN0NC5nY3Aua2Vib29sYS5jb20vc3NlIn0%3D)
- **[Claude Desktop](https://claude.ai)**: Add the integration via Settings → Integrations
- **[Windsurf](https://windsurf.ai)**: Configure with the remote server URL
- **[Make](https://make.com)**: Configure with the remote server URL
- **Other MCP clients**: Configure with the remote server URL

For detailed setup instructions and region-specific URLs, see our [Remote Server Setup documentation](https://help.keboola.com/ai/mcp-server/#remote-server-setup).

---

## Features

- **Storage**: Query tables directly and manage table or bucket descriptions
- **Components**: Create, List and inspect extractors, writers, data apps, and transformation configurations
- **SQL**: Create SQL transformations with natural language
- **Jobs**: Run components and transformations, and retrieve job execution details
- **Metadata**: Search, read, and update project documentation and object metadata using natural language

## Preparations

Make sure you have:

- [ ] Python 3.10+ installed
- [ ] Access to a Keboola project with admin rights
- [ ] Your preferred MCP client (Claude, Cursor, etc.)

**Note**: Make sure you have `uv` installed. The MCP client will use it to automatically download and run the Keboola MCP Server.
**Installing uv**:

*macOS/Linux*:

```bash
#if homebrew is not installed on your machine use:
# /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install using Homebrew
brew install uv
```

*Windows*:

```powershell
# Using the installer script
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or using pip
pip install uv

# Or using winget
winget install --id=astral-sh.uv -e
```

For more installation options, see the [official uv documentation](https://docs.astral.sh/uv/getting-started/installation/).

Before setting up the MCP server, you need three key pieces of information:

### KBC_STORAGE_TOKEN

This is your authentication token for Keboola:

For instructions on how to create and manage Storage API tokens, refer to the [official Keboola documentation](https://help.keboola.com/management/project/tokens/).

**Note**: If you want the MCP server to have limited access, use custom storage token, if you want the MCP to access everything in your project, use the master token.

### KBC_WORKSPACE_SCHEMA

This identifies your workspace in Keboola and is used for SQL queries. However, this is **only required if you're using a custom storage token** instead of the Master Token:

- If using [Master Token](https://help.keboola.com/management/project/tokens/#master-tokens): The workspace is created automatically behind the scenes
- If using [custom storage token](https://help.keboola.com/management/project/tokens/#limited-tokens): Follow this [Keboola guide](https://help.keboola.com/tutorial/manipulate/workspace/) to get your KBC_WORKSPACE_SCHEMA

**Note**: When creating a workspace manually, check Grant read-only access to all Project data option

**Note**: KBC_WORKSPACE_SCHEMA is called Dataset Name in BigQuery workspaces, you simply click connect and copy the Dataset Name

### Keboola Region

Your Keboola API URL depends on your deployment region. You can determine your region by looking at the URL in your browser when logged into your Keboola project:

| Region | API URL |
|--------|---------|
| AWS North America | `https://connection.keboola.com` |
| AWS Europe | `https://connection.eu-central-1.keboola.com` |
| Google Cloud EU | `https://connection.europe-west3.gcp.keboola.com` |
| Google Cloud US | `https://connection.us-east4.gcp.keboola.com` |
| Azure EU | `https://connection.north-europe.azure.keboola.com` |

## Running Keboola MCP Server

There are four ways to use the Keboola MCP Server, depending on your needs:

### Option A: Integrated Mode (Recommended)

In this mode, Claude or Cursor automatically starts the MCP server for you. **You do not need to run any commands in your terminal**.

1. Configure your MCP client (Claude/Cursor) with the appropriate settings
2. The client will automatically launch the MCP server when needed

#### Claude Desktop Configuration

1. Go to Claude (top left corner of your screen) -> Settings → Developer → Edit Config (if you don't see the claude_desktop_config.json, create it)
2. Add the following configuration:
3. Restart Claude desktop for changes to take effect

```json
{
  "mcpServers": {
    "keboola": {
      "command": "uvx",
      "args": ["keboola_mcp_server"],
      "env": {
        "KBC_STORAGE_API_URL": "https://connection.YOUR_REGION.keboola.com",
        "KBC_STORAGE_TOKEN": "your_keboola_storage_token",
        "KBC_WORKSPACE_SCHEMA": "your_workspace_schema"
      }
    }
  }
}
```

Config file locations:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

#### Cursor Configuration

1. Go to Settings → MCP
2. Click "+ Add new global MCP Server"
3. Configure with these settings:

```json
{
  "mcpServers": {
    "keboola": {
      "command": "uvx",
      "args": ["keboola_mcp_server"],
      "env": {
        "KBC_STORAGE_API_URL": "https://connection.YOUR_REGION.keboola.com",
        "KBC_STORAGE_TOKEN": "your_keboola_storage_token",
        "KBC_WORKSPACE_SCHEMA": "your_workspace_schema"
      }
    }
  }
}
```

**Note**: Use short, descriptive names for MCP servers. Since the full tool name includes the server name and must stay under ~60 characters, longer names may be filtered out in Cursor and will not be displayed to the Agent.


#### Cursor Configuration for Windows WSL

When running the MCP server from Windows Subsystem for Linux with Cursor AI, use this configuration:

```json
{
  "mcpServers": {
    "keboola":{
      "command": "wsl.exe",
      "args": [
          "bash",
          "-c '",
          "export KBC_STORAGE_API_URL=https://connection.YOUR_REGION.keboola.com &&",
          "export KBC_STORAGE_TOKEN=your_keboola_storage_token &&",
          "export KBC_WORKSPACE_SCHEMA=your_workspace_schema &&",
          "/snap/bin/uvx keboola_mcp_server",
          "'"
      ]
    }
  }
}
```

### Option B: Local Development Mode

For developers working on the MCP server code itself:

1. Clone the repository and set up a local environment
2. Configure Claude/Cursor to use your local Python path:

```json
{
  "mcpServers": {
    "keboola": {
      "command": "/absolute/path/to/.venv/bin/python",
      "args": [
        "-m",
        "keboola_mcp_server"
      ],
      "env": {
        "KBC_STORAGE_API_URL": "https://connection.YOUR_REGION.keboola.com",
        "KBC_STORAGE_TOKEN": "your_keboola_storage_token",
        "KBC_WORKSPACE_SCHEMA": "your_workspace_schema"
      }
    }
  }
}
```

### Option C: Manual CLI Mode (For Testing Only)

You can run the server manually in a terminal for testing or debugging:

```bash
# Set environment variables
export KBC_STORAGE_API_URL=https://connection.YOUR_REGION.keboola.com
export KBC_STORAGE_TOKEN=your_keboola_storage_token
export KBC_WORKSPACE_SCHEMA=your_workspace_schema

uvx keboola_mcp_server --transport sse
```

> **Note**: This mode is primarily for debugging or testing. For normal use with Claude or Cursor,
> you do not need to manually run the server.

> **Note**: The server will use the SSE transport and listen on `localhost:8000` for the incoming SSE connections.
> You can use `--port` and `--host` parameters to make it listen elsewhere.

### Option D: Using Docker

```shell
docker pull keboola/mcp-server:latest

docker run \
  --name keboola_mcp_server \
  --rm \
  -it \
  -p 127.0.0.1:8000:8000 \
  -e KBC_STORAGE_API_URL="https://connection.YOUR_REGION.keboola.com" \
  -e KBC_STORAGE_TOKEN="YOUR_KEBOOLA_STORAGE_TOKEN" \
  -e KBC_WORKSPACE_SCHEMA="YOUR_WORKSPACE_SCHEMA" \
  keboola/mcp-server:latest \
  --transport sse \
  --host 0.0.0.0
```

> **Note**: The server will use the SSE transport and listen on `localhost:8000` for the incoming SSE connections.
> You can change `-p` to map the container's port somewhere else.

### Do I Need to Start the Server Myself?

| Scenario | Need to Run Manually? | Use This Setup |
|----------|----------------------|----------------|
| Using Claude/Cursor | No | Configure MCP in app settings |
| Developing MCP locally | No (Claude starts it) | Point config to python path |
| Testing CLI manually | Yes | Use terminal to run |
| Using Docker | Yes | Run docker container |

## Using MCP Server

Once your MCP client (Claude/Cursor) is configured and running, you can start querying your Keboola data:

### Verify Your Setup

You can start with a simple query to confirm everything is working:

```text
What buckets and tables are in my Keboola project?
```

### Examples of What You Can Do

**Data Exploration:**

- "What tables contain customer information?"
- "Run a query to find the top 10 customers by revenue"

**Data Analysis:**

- "Analyze my sales data by region for the last quarter"
- "Find correlations between customer age and purchase frequency"

**Data Pipelines:**

- "Create a SQL transformation that joins customer and order tables"
- "Start the data extraction job for my Salesforce component"

## Compatibility

### MCP Client Support

| **MCP Client** | **Support Status** | **Connection Method** |
|----------------|-------------------|----------------------|
| Claude (Desktop & Web) | ✅ supported | stdio |
| Cursor | ✅ supported | stdio |
| Windsurf, Zed, Replit | ✅ Supported | stdio |
| Codeium, Sourcegraph | ✅ Supported | HTTP+SSE |
| Custom MCP Clients | ✅ Supported | HTTP+SSE or stdio |

## Supported Tools

**Note:** Your AI agents will automatically adjust to new tools.

| Category | Tool | Description |
|----------|------|-------------|
| **Project** | `get_project_info` | Gets structured information about your Keboola project |
| **Storage** | `list_buckets` | Lists all storage buckets in your Keboola project |
| | `get_bucket` | Retrieves detailed information about a specific bucket |
| | `list_tables` | Returns all tables within a specific bucket |
| | `get_table` | Provides detailed information for a specific table |
| | `update_bucket_description` | Updates the description of a bucket |
| | `update_column_description` | Updates the description for a given column in a table |
| | `update_table_description` | Updates the description of a table |
| **SQL** | `query_table` | Executes custom SQL queries against your data |
| | `get_sql_dialect` | Identifies whether your workspace uses Snowflake or BigQuery SQL dialect |
| **Component** | `create_config` | Creates a component configuration with custom parameters |
| | `add_config_row` | Creates a component configuration row with custom parameters |
| | `create_sql_transformation` | Creates an SQL transformation with custom queries |
| | `find_component_id` | Returns list of component IDs that match the given query |
| | `get_component` | Gets information about a specific component given its ID |
| | `get_config` | Gets information about a specific component/transformation configuration |
| | `get_config_examples` | Retrieves sample configuration examples for a specific component |
| | `list_configs` | Retrieves configurations of components present in the project |
| | `list_transformations` | Retrieves transformation configurations in the project |
| | `update_config` | Updates a specific component configuration |
| | `update_config_row` | Updates a specific component configuration row |
| | `update_sql_transformation` | Updates an existing SQL transformation configuration |
| **Job** | `retrieve_jobs` | Lists and filters jobs by status, component, or configuration |
| | `get_job_detail` | Returns comprehensive details about a specific job |
| | `start_job` | Triggers a component or transformation job to run |
| **Flow** | `create_flow` | Creates a new flow configuration in Keboola |
|  | `get_flow` | Gets detailed information about a specific flow configuration. |
|  | `get_flow_schema` | Returns the JSON schema that defines the structure of Flow configurations |
|  | `list_flows` | Retrieves flow configurations from the project |
|  | `update_flow` | Updates an existing flow configuration in Keboola |
| **Documentation** | `docs_query` | Searches Keboola documentation based on natural language queries |

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Authentication Errors** | Verify `KBC_STORAGE_TOKEN` is valid |
| **Workspace Issues** | Confirm `KBC_WORKSPACE_SCHEMA` is correct |
| **Connection Timeout** | Check network connectivity |

## Development

### Installation

Basic setup:

```bash
uv sync --extra dev
```

With the basic setup, you can use `uv run tox` to run tests and check code style.

Recommended setup:

```bash
uv sync --extra dev --extra tests --extra integtests --extra codestyle
```

With the recommended setup, packages for testing and code style checking will be installed which allows IDEs like
VsCode or Cursor to check the code or run tests during development.

### Integration tests

To run integration tests locally, use `uv run tox -e integtests`.
NOTE: You will need to set the following environment variables:

- `INTEGTEST_STORAGE_API_URL`
- `INTEGTEST_STORAGE_TOKEN`
- `INTEGTEST_WORKSPACE_SCHEMA`

In order to get these values, you need a dedicated Keboola project for integration tests.

### Updating `uv.lock`

Update the `uv.lock` file if you have added or removed dependencies. Also consider updating the lock with newer dependency
versions when creating a release (`uv lock --upgrade`).

## Support and Feedback

**⭐ The primary way to get help, report bugs, or request features is by [opening an issue on GitHub](https://github.com/keboola/mcp-server/issues/new). ⭐**

The development team actively monitors issues and will respond as quickly as possible. For general information about Keboola, please use the resources below.

## Resources

- [User Documentation](https://help.keboola.com/)
- [Developer Documentation](https://developers.keboola.com/)
- [Keboola Platform](https://www.keboola.com)
- [Issue Tracker](https://github.com/keboola/mcp-server/issues/new) ← **Primary contact method for MCP Server**

## Connect

- [LinkedIn](https://www.linkedin.com/company/keboola)
- [Twitter](https://x.com/keboola)
- [Changelog](https://changelog.keboola.com/)
