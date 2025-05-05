<p align="center">
  <img src="https://cdn.prod.website-files.com/5e21dc6f4c5acf29c35bb32c/5e21e66410e34945f7f25add_Keboola_logo.svg"  alt="Keboola Logo" align="right">
</p>

# Keboola MCP Server

[![PyPI version](https://badge.fury.io/py/keboola-mcp-server.svg)](https://badge.fury.io/py/keboola-mcp-server)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/keboola-mcp-server)](https://pypi.org/project/keboola-mcp-server/)
[![Tests](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![MCP Version](https://img.shields.io/badge/MCP-0.2-blue)
![Docker Image](https://img.shields.io/docker/image-size/keboola/mcp-server)

Keboola MCP transforms complex data operations into simple conversations. It lets **users** and AI agents like **Claude**, **Cursor**, **Windsurf**, or **VS Code** create, update, maintain and operate rich data-driven workflows and analysis without even knowing. Using natural language.

> This is where data engineering becomes "let me just write one last prompt" type of job.

## 💡 Example Usage

Ask your AI assistant to:

- 📊 **Exploration** - "What sales data do we have in Keboola? Show me the main tables and their origins, trustworthiness."
- 🔍 **Analysis** - "Analyze our customer retention - which segments had the highest churn last quarter?"
- 🧮 **Calculations** - "Calculate our monthly recurring revenue by product category, showing growth trends year-over-year."
- 🔄 **Data Processing** - "Create a transformation that cleans our CRM data - remove duplicates and orphaned records, consolidate addresses, and join it with our project delivery metrics."
- 📈 **Visual Reporting** - "Build a weekly sales dashboard with comparisons to previous periods and regional breakdowns."
- 🤝 **Data Integration** - "Connect Woocomerce data to our I_CUSTOMERS database and create unified customer profiles."
- 🚀 **Workflow Automation** - "Schedule our marketing data pipeline to run daily at 6am and send an email when it completes."
- 📝 **Documentation & Governance** - "Document all tables in our GENERAL_LEDGER_FLOW with their purposes, update frequencies, and data owners."

## Give It a Shot
Sign-up to [Keboola Playground](https://chat.canary-orion.keboola.dev/).
> If you wanna use it with existing Keboola project, we keep this invite only so far, feel free to reach out through [GitHub Issues](https://github.com/keboola/mcp-server/issues)!


## ✅ Compatibility

| Environment | Support Status |
|-------------|---------------|
| **Data Backends** | |
| Snowflake | ✅ Fully supported |
| BigQuery | ✅ Fully supported |
| **Data integrations** | 700+ |
| **SQL** | SQL fully supported |
| **Python** | In development |
| **dbt** | In development |
| **Operating Systems** | |
| macOS | ✅ Fully supported |
| Linux | ✅ Fully supported |
| Windows | ✅ Fully supported |
| **AI Assistants** | |
| Claude (Desktop & Web) | ✅ Fully supported |
| Cursor | ✅ Fully supported |
| Perplexity | ✅ Fully supported |
| Custom Agents | ✅ Via MCP standard |

---

## 🚀 Quick Start Guide

### 1. Prerequisites

- Python 3.10 or newer
- A Keboola account with Storage API token
- (Optional) A Keboola workspace with Snowflake or BigQuery

### 2. Installation Options

#### Option A: Using pip (Recommended)

```bash
# Setup virtual environment
python3 -m venv --upgrade-deps .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the MCP server
pip install keboola_mcp_server

# Start the server
uvx --from keboola-mcp-server keboola-mcp  --api-url https://connection.YOUR_REGION.keboola.com
```

#### Option B: Using Docker

```bash
docker pull keboola/mcp-server:latest

docker run -it \
  -e KBC_STORAGE_TOKEN="your-token" \
  -e KBC_WORKSPACE_SCHEMA="your-schema" \
  keboola/mcp-server:latest \
  --api-url https://connection.YOUR_REGION.keboola.com
```

> Replace `YOUR_REGION` with your Keboola deployment region.
> | Region | URL |
> |--------|-----|
> |AWS North America|`https://connection.keboola.com`|
> |AWS Europe|`https://connection.eu-central-1.keboola.com`|
> |Google Cloud EU|`https://connection.europe-west3.gcp.keboola.com`|
> |Google Cloud US (Pay As You Go)|`https://connection.us-east4.gcp.keboola.com`|
> |Azure EU (Pay As You Go)|`https://connection.north-europe.azure.keboola.com`|


### 3. Required Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KBC_STORAGE_TOKEN` | Yes | Your Keboola Storage API token |
| `KBC_WORKSPACE_SCHEMA` | For queries | Your Keboola workspace schema name |
| `GOOGLE_APPLICATION_CREDENTIALS` | For BigQuery | Path to Google credentials JSON file |

---

## 💻 AI Assistant Integration

### Claude Desktop Configuration

```json
{
  "mcpServers": {
    "keboola": {
      "command": "uvx",
      "args": [
        "--from",
        "keboola_mcp_server",
        "keboola-mcp",
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

### Claude Desktop Configuration (with Docker)

```json
{
  "mcpServers": {
    "keboola": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-e", "KBC_STORAGE_TOKEN=your-keboola-storage-token",
        "-e", "KBC_WORKSPACE_SCHEMA=your-workspace-schema",
        "keboola/mcp-server:latest",
        "--api-url", "https://connection.YOUR_REGION.keboola.com"
      ]
    }
  }
}
```

### Cursor Integration

1. Open Settings → Features → MCP Servers
2. Click "+ Add new global MCP server"
3. Fill in the configuration details similar to the Claude example above

### Integration with Other Platforms

The Keboola MCP Server can be integrated with nearly any platform that supports the MCP protocol or HTTP/SSE:

#### 🤖 Agent Frameworks
- **[CrewAI](https://github.com/joaomdmoura/crewAI)**: Add Keboola as a custom tool provider to your agent crew
- **[AutoGen](https://github.com/microsoft/autogen)**: Register Keboola tools with AutoGen's `AssistantAgent`
- **[LangChain](https://python.langchain.com/)**: Use Keboola as a tool provider in your LangChain applications

#### 🔄 Automation Platforms
- **[Zapier](https://zapier.com/)**: Connect via webhooks to trigger Keboola jobs based on events
- **[n8n](https://n8n.io/)**: Use HTTP nodes to query data or trigger transformations
- **[Make](https://www.make.com/)**: Create scenarios that leverage Keboola data operations

#### 🧠 AI Applications
- **[Perplexity](https://www.perplexity.ai/)**: Connect via MCP for data-backed answers
- **[Discord Bots](https://discord.com/developers/docs/intro)**: Create data-aware bots that query your Keboola project
- **Custom Apps**: Use Keboola for data operations in your web/mobile applications

**Integration Method**: All platforms connect to Keboola MCP Server via HTTP + Server-Sent Events (SSE) or stdio, making integration seamless regardless of programming language or environment.

---

## 🧰 Supported Tools
LLMs, agents and users can combine all these tools to help you achieve your goals.
| Category | Tool | Description |
|----------|------|-------------|
| **Storage** | `retrieve_buckets` | Lists all storage buckets in your Keboola project with their IDs, names, and metadata |
| | `get_bucket_detail` | Retrieves comprehensive information about a specific bucket including tables, permissions, and statistics |
| | `retrieve_bucket_tables` | Returns all tables within a specific bucket along with their row counts and last update times |
| | `get_table_detail` | Provides detailed schema information, column types, primary keys, and database identifiers for SQL queries |
| | `update_bucket_description` | Adds or modifies the description for a bucket to improve documentation and governance |
| | `update_table_description` | Sets or updates the description text for tables to document their purpose and structure |
| **SQL** | `query_table` | Executes custom SQL queries against tables in your workspace and returns formatted results |
| | `get_sql_dialect` | Identifies whether your workspace uses Snowflake or BigQuery SQL dialect for proper query syntax |
| **Component** | `retrieve_components` | Lists all available extractors, writers, and applications with their configurations |
| | `retrieve_transformations` | Returns all transformation configurations available in your project organized by type |
| | `get_component_details` | Provides detailed configuration information and parameters for a specific component |
| | `create_sql_transformation` | Creates a new SQL transformation with custom queries, scheduling, and input/output mapping |
| | `create_component_configuration` | Creates a new component configuration, allowing to extract and write data |
| **Job** | `retrieve_jobs` | Lists and filters jobs by status, component, configuration ID with support for pagination |
| | `get_job_detail` | Returns comprehensive details about a specific job including logs, performance metrics, and results |
| | `start_job` | Triggers a component or transformation job to run with specified parameters and configurations |
| **Documentation** | `docs_query` | Searches and retrieves relevant Keboola documentation based on natural language queries |

---

## 🛠️ Troubleshooting & Debugging

### Common Issues

| Issue | Solution |
|-------|----------|
| **Authentication Errors**, **Token Not Found** | Verify your `KBC_STORAGE_TOKEN` environment variable is set with a valid token |
| **Workspace Issues** | Confirm `KBC_WORKSPACE_SCHEMA` is correct and accessible |
| **Connection Timeout** | Check network connectivity to your Keboola region |
| **Missing Tables** | Ensure your token has access to the required buckets |
| **SQL Query Errors** | Verify SQL dialect matches your backend (Snowflake/BigQuery) |


### Debugging Tools

```bash
# Set your Keboola token (required)
export KBC_STORAGE_TOKEN=your_token_here

# Run with debug logging
uvx --from keboola-mcp-server keboola-mcp --api-url https://connection.YOUR_REGION.keboola.com --log-level DEBUG

# Use MCP Inspector to test
npx @modelcontextprotocol/inspector uvx --from keboola-mcp-server keboola-mcp
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KBC_STORAGE_TOKEN` | Yes | Your Keboola Storage API token |
| `KBC_WORKSPACE_SCHEMA` | For queries | Your Keboola workspace schema name |
| `GOOGLE_APPLICATION_CREDENTIALS` | For BigQuery | Path to Google credentials JSON file |

---
## 🧪 Development & Contribution

```bash
# Clone repository
git clone https://github.com/keboola/keboola-mcp-server.git
cd keboola-mcp-server

# Setup development environment
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black . && isort .

# Type check
mypy .
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed development guidelines.

---

# Keboola Core
A high level overview of what capabilities are exposed via the MCP Server from the underlying Keboola Core.
<p align="center">
  <img src="https://help.keboola.com/overview/project-structure1.png" alt="Platform Overview">
</p>

# Claude Integration
A few screenshots from Claude using Keboola MCP Server.
<p align="center">
  <img src="assets/keboola_animation_medium_delay.gif" alt="Claude Integration">
</p>

## 📄 License

[MIT License](./LICENSE) — See the LICENSE file for details.

---

## 🔗 Stay in Touch

  <a href="https://www.linkedin.com/company/keboola">LinkedIn</a> •
  <a href="https://x.com/keboola">X</a> •
  <a href="https://changelog.keboola.com/">Changelog</a>


---

## 🧭 Want to Go Further?

- 📘 [User Docs](https://docs.keboola.com/)
- 📘 [developer Docs](https://developers.keboola.com/)
- 🌐 [Keboola Platform](https://www.keboola.com)
- 🛠 [Keboola Playground](https://chat.canary-orion.keboola.dev/)
