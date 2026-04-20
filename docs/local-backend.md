# Local Backend Mode (`--local-backend`)

**Linear**: [AI-2922](https://linear.app/keboola/issue/AI-2922/local-backend-mode-for-keboola-mcp-server-local-backend-flag)
**Status**: Implemented — Phases 1, 2, 3, and partial Phase 4

---

## Overview

The Keboola MCP server can be extended with a `--local-backend` flag that replaces all Keboola platform API calls with local implementations:

- Components run via Docker using the [Common Interface](https://developers.keboola.com/extend/common-interface/) contract
- Data is stored as CSV files on disk
- Queries execute via native Python `duckdb` (server-side)
- Dashboard apps are generated as self-contained HTML files (ECharts + Pico CSS) — no npm, no Node.js required

**The server remains Python. No rewrite.** The MCP protocol contract is identical for any AI client (Claude, Cursor, VS Code, etc.). A single CLI flag at startup switches the entire tool surface.

```
┌──────────────────────────────────────────────────────────────────┐
│               AI Client (Claude, Cursor, VS Code…)               │
│                    ▼  MCP Protocol (stdio / HTTP)                │
├──────────────────────────────────────────────────────────────────┤
│                  Keboola MCP Server (Python)                     │
│                                                                  │
│   ┌──────────────────┐         ┌──────────────────────┐         │
│   │ Platform Backend │  ←OR→   │   Local Backend       │         │
│   │   (default)      │         │  (--local-backend)    │         │
│   │                  │         │                       │         │
│   │ Storage API      │         │ Filesystem catalog    │         │
│   │ Keboola Jobs API │         │ Docker component exec │         │
│   │ Snowflake/BQ SQL │         │ CSV file management   │         │
│   │ Streamlit apps   │         │ HTML dashboard gen    │         │
│   └──────────────────┘         └──────────────────────┘         │
└──────────────────────────────────────────────────────────────────┘
         │ (platform)                      │ (local)
         ▼                                 ▼
   Keboola Platform               Local Docker + Filesystem
   (Storage API, Jobs,            keboola_data/
    Snowflake workspace)          ├── tables/   (CSV catalog)
                                  ├── configs/  (component configs)
                                  ├── components/ (cloned repos)
                                  ├── runs/     (temp run dirs)
                                  └── apps/     (HTML dashboards)
```

---

## CLI Design

### Arguments

```
--local-backend          Run in local mode. No KBC_STORAGE_TOKEN required.
                         Components execute via Docker. Data stored as CSV on disk.
                         (env: KBC_LOCAL_BACKEND=true|1|yes)

--data-dir PATH          Root directory for local data storage.
                         Default: ./keboola_data
                         (env: KBC_DATA_DIR)

--docker-network NETWORK Docker network for component execution.
                         Default: bridge
                         Use "host" if bridge DNS resolution fails (common on Linux).
                         (env: KBC_DOCKER_NETWORK)

--api-url URL            Keboola Storage API URL.
                         Pre-fills migrate_to_keboola so credentials don't need
                         to be passed in the tool call.

--storage-token STR      Keboola Storage API token.
                         Pre-fills migrate_to_keboola. Never logged or exposed.
```

### Mode comparison

| Property | Platform (default) | Local (`--local-backend`) |
|----------|--------------------|---------------------------|
| Token required | `KBC_STORAGE_TOKEN` | None |
| Components run via | Keboola Jobs API | `docker run` locally |
| Data stored in | Keboola Storage (Snowflake/BQ) | CSV files on disk (`--data-dir`) |
| SQL queries | Keboola workspace (Snowflake/BQ) | Native `duckdb` (Python) |
| Dashboard apps | Streamlit on Keboola platform | HTML + ECharts on localhost |

---

## Tool Surface by Mode

### Shared tools (same interface, different backend)

| Tool | Platform behaviour | Local behaviour |
|------|--------------------|-----------------|
| `get_tables` | Lists tables in Keboola Storage | Scans `<data-dir>/tables/*.csv` |
| `get_buckets` | Lists Storage buckets | Returns single virtual bucket |
| `query_data` | Executes SQL on Snowflake/BQ workspace | Executes SQL via native `duckdb` on CSV files |
| `search` | Semantic search via AI service | Searches filenames and CSV headers |
| `get_project_info` | Returns Keboola project metadata | Returns local metadata (table count, config count, data dir) |

### Local-only tools (not registered in platform mode)

| Tool | Description |
|------|-------------|
| `setup_component` | Clones a component git repo and builds its Docker image — one-time setup before `run_component` |
| `run_component` | Runs a Keboola component locally (source-based via `docker compose` or registry image via `docker run`) |
| `get_component_schema` | Fetches component config schema from the public Developer Portal API (no token needed) |
| `find_component_id` | Searches the Developer Portal for components by name |
| `write_table` | Write or overwrite a CSV file in the local catalog |
| `delete_table` | Delete a CSV file from the local catalog |
| `save_config` | Persist a component configuration to `<data-dir>/configs/<id>.json` |
| `list_configs` | List all saved component configurations |
| `delete_config` | Delete a saved component configuration |
| `run_saved_config` | Run a previously saved config (loads params and image/git_url from disk) |
| `migrate_to_keboola` | Uploads local CSV tables and saved configs to Keboola platform via Storage API |
| `create_data_app` | Generate a self-contained HTML dashboard with SQL-driven ECharts visualizations |
| `run_data_app` | Start a local HTTP server for the dashboard; returns the browser URL |
| `list_data_apps` | List all local dashboards with running status |
| `stop_data_app` | Kill the HTTP server for a dashboard |
| `delete_data_app` | Permanently remove a dashboard directory |

### Platform-only tools (not registered in local mode)

`run_job`, `get_job`, `list_jobs`, `deploy_data_app`, `modify_data_app`, `create_flow`, `update_flow`,
`create_conditional_flow`, `create_sql_transformation`, `update_sql_transformation`, `create_oauth_url`,
`docs_query`, `search_semantic_context`, `get_semantic_context`, `get_semantic_schema`, `validate_semantic_query`

These tools are simply not registered at startup. The LLM client never sees them in the tool list.

> ⚠️ **Documentation and semantic tools are unavailable in local mode.** `docs_query` and all semantic tools call the Keboola AI Service API which requires a platform token. There is no local fallback. In local mode, component documentation is available via `get_component_schema` (public Developer Portal, no token) and `search` (filename + column header matching).

---

## Pillar 1 — Local Component Execution via Docker

### The Common Interface contract

Every Keboola component is a Docker image that reads configuration and input data from a mounted `/data` directory and writes outputs back to `/data/out/`.

### `/data` directory layout

```
/data/
├── config.json                     # Component configuration
├── in/
│   ├── tables/                     # Input CSV tables
│   │   └── customers.csv
│   └── files/                      # Input binary files
└── out/
    ├── tables/                     # Component writes output CSVs here
    └── files/                      # Output binary/CSV files
```

Output CSVs are collected from **both** `out/tables/` and `out/files/` back into the local catalog — some components (e.g. HTTP extractors) write CSV to `out/files/` rather than `out/tables/`.

### `config.json`

```json
{
  "storage": {
    "input": {
      "tables": [
        { "source": "customers.csv", "destination": "customers.csv" }
      ]
    },
    "output": { "tables": [] }
  },
  "parameters": {
    "query": "SELECT id, name FROM customers WHERE status = 'active'"
  }
}
```

### Source-based execution (typical — most components)

```bash
# 1. Clone the component repository
git clone https://github.com/keboola/<component-name>.git

# 2. Build the local Docker image (network injected via override file if --docker-network != bridge)
docker compose build

# 3. Install component dependencies (detected automatically by presence of lock files)
docker compose run --rm dev composer install        # PHP
docker compose run --rm dev npm ci                  # Node.js
docker compose run --rm dev pip install -r requirements.txt  # Python

# 4. Run the component (data dir injected via docker-compose.override.yml)
docker compose run --rm dev <entrypoint>
```

**Network handling**: `docker compose build` and `docker compose run` do not accept `--network` as a CLI flag.
Instead, when `--docker-network` is not `bridge`, the server writes a temporary
`docker-compose.override.yml` injecting `build.network` and `network_mode` before each build/run and
removes it afterward (unless the repo already has an override file).

### Registry image execution (pre-built / CI images)

```bash
docker run \
  --rm \
  --volume=/path/to/run-dir:/data \
  --memory=4g \
  --network=bridge \
  -e KBC_DATADIR=/data/ \
  quay.io/keboola/python-transformation:latest
```

`docker run` supports `--network` as a CLI flag; the `--docker-network` value is passed directly here.

### Exit code semantics

| Exit code | Meaning |
|-----------|---------|
| `0` | Success |
| `1` | User error (bad config, invalid data) |
| `>1` | Application error (bug in component) |

---

## Component Schema Discovery in Local Mode

### Tier 1 — Cloned repo introspection (best)

When `setup_component` clones a repo, it scans for `component.json` (Developer Portal manifest with `configSchema`) and reads the first 3000 characters of `README.md`.

### Tier 2 — Keboola Developer Portal API (public, no auth)

```
GET https://apps-api.keboola.com/apps/{component_id}
```

Returns the component manifest including `configurationSchema`. The `get_component_schema` tool wraps this. `find_component_id` fetches all published apps and filters client-side by name/id/description.

### Tier 3 — Fallback

_Not implemented._ If Developer Portal is offline and the repo wasn't cloned, instruct the user to consult the component's README directly.

---

## Pillar 2 — HTML Dashboard Generation

### Approach

Instead of a Vite + React + DuckDB-WASM stack (requires Node.js), local data apps are generated as **self-contained HTML files** that:

- Load **Apache ECharts** and **Pico CSS** from CDN
- Embed chart data pre-computed server-side via Python DuckDB (no in-browser SQL needed)
- Serve via Python's stdlib `http.server` (no nginx, no Docker, no build step)
- Support dark/light auto-theme and a responsive CSS grid layout

### Supported chart types

`bar`, `line`, `scatter`, `pie`, `table`, `heatmap`

### `create_data_app` usage

```json
{
  "name": "iris-dashboard",
  "title": "Iris Dataset Analysis",
  "description": "Species distributions and measurements",
  "charts": [
    {
      "id": "species_pie",
      "title": "Species Distribution",
      "sql": "SELECT species, COUNT(*) AS count FROM iris GROUP BY species",
      "type": "pie",
      "x_column": "species",
      "y_column": "count"
    },
    {
      "id": "petal_scatter",
      "title": "Petal vs Sepal Length by Species",
      "sql": "SELECT sepal_length, petal_length, species FROM iris",
      "type": "scatter",
      "x_column": "sepal_length",
      "y_column": "petal_length",
      "color_column": "species"
    },
    {
      "id": "raw_data",
      "title": "Raw Data",
      "sql": "SELECT * FROM iris LIMIT 100",
      "type": "table"
    }
  ]
}
```

### Dashboard lifecycle

```
create_data_app  →  keboola_data/apps/iris-dashboard/
                        ├── app.json    (config + chart metadata)
                        └── index.html  (self-contained dashboard)

run_data_app     →  python -m http.server <port> (background, port 8101-8199)
                    returns http://localhost:8101/apps/iris-dashboard/

list_data_apps   →  [{name, title, chart_count, status, app_url, port}, …]

stop_data_app    →  SIGTERM to server process

delete_data_app  →  removes apps/<name>/ directory
```

### Security

All dynamic content is embedded in `<script type="application/json">` blocks and parsed with `JSON.parse()`. No `innerHTML` is ever used with untrusted content. `</` sequences in JSON are escaped to `<\/` to prevent premature tag termination.

---

## Local Data Catalog

### Filesystem layout

```
keboola_data/           # --data-dir root
├── tables/
│   ├── customers.csv
│   └── orders.csv
├── configs/
│   └── ex-http-iris.json   # saved ComponentConfig JSON
├── components/             # cloned repos for source-based execution
│   └── http-extractor/
│       ├── docker-compose.yml
│       ├── .keboola_image_built   # sentinel — skip rebuild if present
│       └── ...
├── runs/                   # temporary /data dirs per run (auto-cleaned after)
│   └── run-<timestamp>/
│       ├── in/tables/
│       └── out/tables/
└── apps/
    ├── .running.json       # PID registry {app_name: {pid, port, started_at}}
    └── iris-dashboard/
        ├── app.json
        └── index.html
```

---

## Migration to Keboola Platform

`migrate_to_keboola` calls the Keboola Storage API directly via `httpx`. No `keboola-cli` installation required.

### Credentials

Pass `--api-url` and `--storage-token` at server startup so the tool can be called without repeating credentials:

```bash
keboola-mcp-server --local-backend --data-dir ./keboola_data \
  --api-url https://connection.keboola.com \
  --storage-token <your-token>
```

Then call `migrate_to_keboola` with no arguments. Credentials can also be passed directly as tool arguments if the server was started without them.

### Parameters

| Parameter | Description |
|-----------|-------------|
| `storage_api_url` | Stack URL (omit if server started with `--api-url`) |
| `storage_token` | Write-access Storage token (omit if server started with `--storage-token`) |
| `table_names` | Optional list of table names to migrate (default: all) |
| `config_ids` | Optional list of config IDs to migrate (default: all) |
| `bucket_id` | Target bucket (default: `in.c-local`, created if absent) |

### Flow

1. Creates the target bucket (`POST /v2/storage/buckets`); 422 ignored (already exists).
2. Uploads each CSV as multipart `POST /v2/storage/buckets/{id}/tables`; 422 captured as `already_exists`.
3. Creates each saved config via `POST /v2/storage/components/{componentId}/configs`.

Returns `MigrateResult` with per-item status and summary counts.

---

## Implementation Phases

### Phase 1 — Foundation ✅ Done

- `--local-backend` and `--data-dir` CLI flags
- `LocalBackend` class with filesystem catalog
- Local versions of `get_tables`, `get_buckets`, `query_data`, `search`, `get_project_info`
- Integration tests and DuckDB optional dependency

### Phase 2 — Docker Component Execution ✅ Done

- `setup_component` — git clone, `docker compose build`, dependency install (PHP/Node/Python)
- `run_component` — source-based (`docker compose run`) and registry-image (`docker run`) paths
- Output collection from `out/tables/` and `out/files/`
- `--docker-network` flag with override file injection for build and run
- `write_table`, `delete_table`, `save_config`, `list_configs`, `delete_config`, `run_saved_config`
- `get_component_schema`, `find_component_id` via public Developer Portal API

### Phase 3 — HTML Dashboard Generation ✅ Done

- `create_data_app` — generates `app.json` + `index.html`; chart data pre-computed via DuckDB
- `run_data_app` — background Python HTTP server, returns browser URL
- `list_data_apps`, `stop_data_app`, `delete_data_app`
- Apache ECharts + Pico CSS; no npm/Node.js required
- PID registry in `apps/.running.json`; stale-PID auto-cleanup

### Phase 4 — Polish and Migration 🔶 Partial

Done:
- ✅ `migrate_to_keboola` with optional CLI-provided credentials
- ✅ Error handling: per-item error capture, timeout (5 min default), input validation

Not done:
- ❌ Progress reporting for long-running Docker component runs (stdout/stderr only available after completion)
- ❌ Stub messages for platform-only tools (LLM never sees helpful hints like "use migrate_to_keboola")
- ❌ CI test matrix covering local mode with Docker

---

## Running the Server

```bash
# HTTP transport (recommended for testing with MCP Inspector)
keboola-mcp-server --local-backend --data-dir ./keboola_data \
  --transport streamable-http --port 8000

# With host networking (if bridge DNS fails)
keboola-mcp-server --local-backend --data-dir ./keboola_data \
  --transport streamable-http --port 8000 --docker-network host

# With migration credentials pre-loaded
keboola-mcp-server --local-backend --data-dir ./keboola_data \
  --transport streamable-http --port 8000 \
  --api-url https://connection.keboola.com \
  --storage-token <token>

# stdio mode (Claude Desktop / MCP Inspector CLI)
keboola-mcp-server --local-backend --data-dir ./keboola_data --transport stdio
```

**Claude Desktop config** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "keboola-local": {
      "command": "/path/to/3.12.venv/bin/keboola-mcp-server",
      "args": ["--local-backend", "--data-dir", "/path/to/keboola_data"]
    }
  }
}
```

---

## Automated Smoke Tests

`scripts/test-local-backend.sh` exercises the local-backend Python API directly (no MCP protocol):

```bash
./scripts/test-local-backend.sh            # all tests (requires Docker + internet)
./scripts/test-local-backend.sh --no-docker   # skip Docker tests (~2 s)
./scripts/test-local-backend.sh --no-portal   # skip Developer Portal network tests
```

Test sections: prerequisites, `LocalBackend` init, CSV catalog, DuckDB SQL, config persistence, `get_project_info`, tool registration (21 tools), Developer Portal API, Docker registry image.

---

## Dependency Summary

### Python (server)

| Package | Version | Purpose |
|---------|---------|---------|
| `duckdb` | `>=0.10.0` | Server-side SQL for `query_data` and dashboard data pre-computation |
| `httpx` | `>=0.27.0` | `migrate_to_keboola` (Storage API) and schema discovery (Developer Portal) |

`duckdb` is optional: `pip install "keboola-mcp-server[local]"`. `httpx` is already a core dependency.

### Browser (generated HTML apps)

Loaded from CDN — no npm install needed:

| Library | Version | Purpose |
|---------|---------|---------|
| Apache ECharts | `^5` | Chart rendering (pie/bar/line/scatter/heatmap/table) |
| Pico CSS | `^2` | Classless CSS framework (light/dark auto-theme) |

---

## Known Gaps

| Gap | Impact | Notes |
|-----|--------|-------|
| `docs_query` / semantic tools unavailable | No Keboola documentation search in local mode | All require Keboola AI Service token — no workaround |
| No progress reporting for Docker runs | Long-running components block silently | stdout/stderr available in `ComponentRunResult` after completion |
| No stub messages for platform-only tools | LLM never sees helpful "use migrate_to_keboola instead" hints | Could add a local-mode prompt instruction |
| No CI matrix for local mode | Docker-dependent tests not run in CI | Docker daemon required — consider separate workflow |
| `find_component_id` fetches entire catalog | ~366 apps on every search call (~300 KB) | One HTTP request, no pagination; acceptable for interactive use |
| Data app data is static snapshot | Re-run `create_data_app` to refresh after CSV changes | Pre-computed approach; no live in-browser DuckDB queries |
