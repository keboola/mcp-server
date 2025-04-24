# Keboola MCP Server

[![CI](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/keboola/keboola-mcp-server/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/keboola/keboola-mcp-server/branch/main/graph/badge.svg)](https://codecov.io/gh/keboola/keboola-mcp-server)
<a href="https://glama.ai/mcp/servers/72mwt1x862"><img width="380" height="200" src="https://glama.ai/mcp/servers/72mwt1x862/badge" alt="Keboola Explorer Server MCP server" /></a>
[![smithery badge](https://smithery.ai/badge/keboola-mcp-server)](https://smithery.ai/server/keboola-mcp-server)

A Model Context Protocol (MCP) server for interacting with Keboola Connection. This server provides tools for listing and accessing data from Keboola Storage API.

## Requirements

- Python 3.10 or newer
- Keboola Storage API token
- Snowflake or BigQuery Read Only Workspace

## Installation

### Installing via Pip

First, create a virtual environment and then install 
the [keboola_mcp_server](https://pypi.org/project/keboola-mcp-server/) package:

```bash
python3 -m venv --upgrade-deps .venv
source .venv/bin/activate

pip3 install keboola_mcp_server
```

### Installing via Smithery

To install Keboola MCP Server for Claude Desktop automatically via [Smithery](https://smithery.ai/server/keboola-mcp-server):

```bash
npx -y @smithery/cli install keboola-mcp-server --client claude
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

## Kubernetes Deployment (Helm + Operator)

This repository provides resources for deploying the Keboola MCP Server to a Kubernetes cluster using Helm and a Kubernetes Operator.

### Helm Chart

A Helm chart is available in the `charts/keboola-mcp-server` directory. This chart defines the Kubernetes resources (Deployment, Service, etc.) required to run the MCP server.

Key configuration options (set via `values.yaml` or `--set` arguments):
- `image.repository`, `image.tag`: Container image details.
- `replicaCount`: Number of server replicas.
- `service.type`, `service.port`: How the server is exposed within the cluster.
- `ingress.*`: Configuration for exposing the server publicly via an Ingress controller.
- `keboola.apiUrl`: Keboola Connection API URL (including region).
- `keboola.storageTokenSecretName`, `keboola.storageTokenSecretKey`: Reference to a Kubernetes secret containing the `KBC_STORAGE_TOKEN`.
- `keboola.workspaceSchema`: The Snowflake schema or BigQuery dataset for the workspace.
- `keboola.useGoogleCredentials`, `keboola.googleCredentialsSecretName`, `keboola.googleCredentialsSecretKey`: Configuration for using BigQuery backend credentials stored in a secret.
- `existingSecret`: Name of an existing secret containing `KBC_STORAGE_TOKEN` and optionally `GOOGLE_APPLICATION_CREDENTIALS`.
- `mcpTransport`: Transport method (`sse` or `stdio`).

You can install the chart directly using Helm, for example:

```bash
# Create necessary secrets first (replace placeholders)
kubectl create secret generic kbc-credentials --from-literal=token='YOUR_KBC_STORAGE_TOKEN'
# kubectl create secret generic gcp-sa-key --from-file=credentials.json=./path/to/your/credentials.json # If using BigQuery

helm install mcp-server ./charts/keboola-mcp-server \\
  --set keboola.apiUrl="https://connection.YOUR_REGION.keboola.com" \\
  --set keboola.storageTokenSecretName=kbc-credentials \\
  --set keboola.workspaceSchema="YOUR_WORKSPACE_SCHEMA"
  # --set keboola.useGoogleCredentials=true \\ # Uncomment if using BigQuery
  # --set keboola.googleCredentialsSecretName=gcp-sa-key # Uncomment if using BigQuery
```

### Kubernetes Operator (Helm-based)

For automated management and deployment triggered via Kubernetes resources, you can build a Helm-based operator.

The `operator/` directory contains instructions (`operator/README.md`) on how to use the Operator SDK to scaffold an operator based on the Helm chart in this repository.

**Note:** Building and deploying the operator itself is intended to be done in a separate process or repository, potentially triggered by a UI action that creates the `MCPServer` Custom Resource. This repository only provides the Helm chart and the instructions for generating the operator code.

The typical workflow would involve:
1.  Building the operator container image (using the instructions in `operator/README.md`).
2.  Deploying the operator to your Kubernetes cluster.
3.  Creating an `MCPServer` Custom Resource (CR) instance. The operator watches for these CRs and deploys/manages the MCP server based on the spec defined in the CR, which maps to the Helm chart's values.

Example `MCPServer` CR (`config/samples/mcp_v1alpha1_mcpserver.yaml` generated by `operator-sdk`):

```yaml
apiVersion: mcp.yourdomain.com/v1alpha1 # Replace with your domain
kind: MCPServer
metadata:
  name: mcpserver-sample
spec:
  # Fields here map to values.yaml in the Helm chart
  replicaCount: 1
  image:
    repository: ghcr.io/keboola/keboola-mcp-server
    tag: latest # Or specify a version
  service:
    type: LoadBalancer # Or ClusterIP + Ingress
    port: 8000
  keboola:
    apiUrl: "https://connection.keboola.com" # Set your region
    # Reference the secret containing the token
    storageTokenSecretName: "kbc-credentials"
    workspaceSchema: "YOUR_WORKSPACE_SCHEMA"
    # useGoogleCredentials: true # If using BigQuery
    # googleCredentialsSecretName: "gcp-sa-key" # If using BigQuery
  # Use an existing secret for both KBC token and GCP creds
  # existingSecret: "my-combined-secrets"
```

Applying this CR (`kubectl apply -f ...`) would trigger the operator to deploy the MCP server using the specified Helm chart values.

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
