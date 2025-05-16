# Keboola MCP Server
> Connect your AI agents, MCP clients (**Cursor**, **Claude**, **Windsurf**, **VS Code** ...) and other AI assistants to Keboola. Expose data, transformations, SQL queries, and job triggers—no glue code required. Deliver the right data to agents when and where they need it.

## Overview

Keboola MCP Server is an open-source bridge between your Keboola project and modern AI tools. It turns Keboola features—like storage access, SQL transformations, and job triggers—into callable tools for Claude, Cursor, CrewAI, LangChain, Amazon Q, and more.

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
This identifies your workspace in Keboola and is required for SQL queries:

Follow this [Keboola guide](https://help.keboola.com/tutorial/manipulate/workspace/) to get your KBC_WORKSPACE_SCHEMA.

**Note**: Check Grant read-only access to all Project data option when creating the workspace

### Keboola Region
Your Keboola API URL depends on your deployment region. You can determine your region by looking at the URL in your browser when logged into your Keboola project:

| Region | API URL |
|--------|---------|
| AWS North America | `https://connection.keboola.com` |
| AWS Europe | `https://connection.eu-central-1.keboola.com` |
| Google Cloud EU | `https://connection.europe-west3.gcp.keboola.com` |
| Google Cloud US | `https://connection.us-east4.gcp.keboola.com` |
| Azure EU | `https://connection.north-europe.azure.keboola.com` |


### BigQuery-Specific Setup

If your Keboola project uses BigQuery backend, you will need to set `GOOGLE_APPLICATION_CREDENTIALS` environment variable in addition to `KBC_STORAGE_TOKEN` and `KBC_WORKSPACE_SCHEMA`:

1. Go to your Keboola BigQuery workspace and display its credentials (click Connect button)
2. Download the credentials file to your local disk. It is a plain JSON file
3. Set the full path of the downloaded JSON credentials file to `GOOGLE_APPLICATION_CREDENTIALS` environment variable
4. This will give your MCP server instance permissions to access your BigQuery workspace in Google Cloud
**Note**: KBC_WORKSPACE_SCHEMA is called Dataset Name in the BigQuery workspace, you simply click connect and copy the Dataset Name. 

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
      "args": [
        "keboola_mcp_server",
        "--api-url", "https://connection.YOUR_REGION.keboola.com"
      ],
      "env": {
        "KBC_STORAGE_TOKEN": "your_keboola_storage_token",
        "KBC_WORKSPACE_SCHEMA": "your_workspace_schema"
      }
    }
  }
}
```
> **Note**: For BigQuery users, add the following line into "env": {}:
>"GOOGLE_APPLICATION_CREDENTIALS": "/full/path/to/credentials.json"

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
      "args": [
        "keboola_mcp_server",
        "--api-url", "https://connection.YOUR_REGION.keboola.com"
      ],
      "env": {
        "KBC_STORAGE_TOKEN": "your_keboola_storage_token",
        "KBC_WORKSPACE_SCHEMA": "your_workspace_schema"
      }
    }
  }
}
```
> **Note**: For BigQuery users, add the following line into "env": {}:
>"GOOGLE_APPLICATION_CREDENTIALS": "/full/path/to/credentials.json"

#### Cursor Configuration for Windows WSL

When running the MCP server from Windows Subsystem for Linux with Cursor AI, use this configuration:

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

Where `/wsl_path/to/keboola-mcp-server/.env` file contains environment variables:
```bash
export KBC_STORAGE_TOKEN="your_keboola_storage_token"
export KBC_WORKSPACE_SCHEMA="your_workspace_schema"
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
        "-m", "keboola_mcp_server.cli",
        "--transport", "stdio",
        "--api-url", "https://connection.YOUR_REGION.keboola.com"
      ],
      "env": {
        "KBC_STORAGE_TOKEN": "your_keboola_storage_token",
        "KBC_WORKSPACE_SCHEMA": "your_workspace_schema",

      }
    }
  }
}
```
> **Note**: For BigQuery users, add the following line into "env": {}:
>"GOOGLE_APPLICATION_CREDENTIALS": "/full/path/to/credentials.json"

### Option C: Manual CLI Mode (For Testing Only)

You can run the server manually in a terminal for testing or debugging:

```bash
# Set environment variables
export KBC_STORAGE_TOKEN=your_keboola_storage_token
export KBC_WORKSPACE_SCHEMA=your_workspace_schema
# For BigQuery users
# export GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/credentials.json

# Run with uvx (no installation needed)
uvx keboola_mcp_server --api-url https://connection.YOUR_REGION.keboola.com

# OR, if developing locally
python -m keboola_mcp_server.cli --api-url https://connection.YOUR_REGION.keboola.com
```

> **Note**: This mode is primarily for debugging or testing. For normal use with Claude or Cursor, you do not need to manually run the server.

### Option D: Using Docker

```bash
docker pull keboola/mcp-server:latest

# For Snowflake users
docker run -it \
  -e KBC_STORAGE_TOKEN="YOUR_KEBOOLA_STORAGE_TOKEN" \
  -e KBC_WORKSPACE_SCHEMA="YOUR_WORKSPACE_SCHEMA" \
  keboola/mcp-server:latest \
  --api-url https://connection.YOUR_REGION.keboola.com

# For BigQuery users (add credentials volume mount)
# docker run -it \
#   -e KBC_STORAGE_TOKEN="YOUR_KEBOOLA_STORAGE_TOKEN" \
#   -e KBC_WORKSPACE_SCHEMA="YOUR_WORKSPACE_SCHEMA" \
#   -e GOOGLE_APPLICATION_CREDENTIALS="/creds/credentials.json" \
#   -v /local/path/to/credentials.json:/creds/credentials.json \
#   keboola/mcp-server:latest \
#   --api-url https://connection.YOUR_REGION.keboola.com
```

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
```
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
| Claude (Desktop & Web) | ✅ supported, tested | stdio |
| Cursor | ✅ supported, tested | stdio |
| Windsurf, Zed, Replit | ✅ Supported | stdio |
| Codeium, Sourcegraph | ✅ Supported | HTTP+SSE |
| Custom MCP Clients | ✅ Supported | HTTP+SSE or stdio |

## Supported Tools

**Note:** Keboola MCP is pre-1.0, so some breaking changes might occur. Your AI agents will automatically adjust to new tools.

| Category | Tool | Description |
|----------|------|-------------|
| **Storage** | `retrieve_buckets` | Lists all storage buckets in your Keboola project |
| | `get_bucket_detail` | Retrieves detailed information about a specific bucket |
| | `retrieve_bucket_tables` | Returns all tables within a specific bucket |
| | `get_table_detail` | Provides detailed information for a specific table |
| | `update_bucket_description` | Updates the description of a bucket |
| | `update_column_description` | Updates the description for a given column in a table. |
| | `update_table_description` | Updates the description of a table |
| **SQL** | `query_table` | Executes custom SQL queries against your data |
| | `get_sql_dialect` | Identifies whether your workspace uses Snowflake or BigQuery SQL dialect |
| **Component** | `retrieve_components` | Lists all available extractors, writers, and applications |
| | `get_component_details` | Retrieves detailed configuration information for a specific component |
| | `retrieve_transformations` | Returns all transformation configurations in your project |
| | `create_sql_transformation` | Creates a new SQL transformation with custom queries |
| | `update_sql_transformation` | Updates an existing SQL transformation configuration, sql query, description or disables the configuration |
| **Job** | `retrieve_jobs` | Lists and filters jobs by status, component, or configuration |
| | `get_job_detail` | Returns comprehensive details about a specific job |
| | `start_job` | Triggers a component or transformation job to run |
| **Documentation** | `docs_query` | Searches Keboola documentation based on natural language queries |

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Authentication Errors** | Verify `KBC_STORAGE_TOKEN` is valid |
| **Workspace Issues** | Confirm `KBC_WORKSPACE_SCHEMA` is correct |
| **Connection Timeout** | Check network connectivity |

## Support and Feedback

**⭐ The primary way to get help, report bugs, or request features is by [opening an issue on GitHub](https://github.com/keboola/mcp-server/issues/new). ⭐**

The development team actively monitors issues and will respond as quickly as possible. For general information about Keboola, please use the resources below.

## Resources

- [User Documentation](https://docs.keboola.com/)
- [Developer Documentation](https://developers.keboola.com/)
- [Keboola Platform](https://www.keboola.com)
- [Issue Tracker](https://github.com/keboola/mcp-server/issues/new) ← **Primary contact method for MCP Server**

## Connect

- [LinkedIn](https://www.linkedin.com/company/keboola)
- [Twitter](https://x.com/keboola)
- [Changelog](https://changelog.keboola.com/)