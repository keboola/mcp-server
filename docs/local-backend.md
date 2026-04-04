# Local Backend Mode (`--local-backend`)

**Linear**: [AI-2922](https://linear.app/keboola/issue/AI-2922/local-backend-mode-for-keboola-mcp-server-local-backend-flag)
**Status**: RFC / Design spec — implementation not yet started

---

## Overview

The Keboola MCP server can be extended with a `--local-backend` flag that replaces all Keboola platform API calls with local implementations:

- Components run via Docker using the [Common Interface](https://developers.keboola.com/extend/common-interface/) contract
- Data is stored as CSV files on disk
- Queries execute via native Python `duckdb` (server-side) and `@duckdb/duckdb-wasm` (browser-side in generated apps)
- Dashboard apps are generated as native TypeScript/JavaScript (Vite + React + ECharts) instead of Streamlit

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
│   │ Streamlit apps   │         │ TS/JS app generation  │         │
│   └──────────────────┘         └──────────────────────┘         │
└──────────────────────────────────────────────────────────────────┘
         │ (platform)                      │ (local)
         ▼                                 ▼
   Keboola Platform               Local Docker + Filesystem
   (Storage API, Jobs,            ┌──────────────┐
    Snowflake workspace)          │  ./data/      │
                                  │  ├── in/      │
                                  │  │  └── tables│
                                  │  └── out/     │
                                  │     └── tables│
                                  └──────────────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │ Browser App   │
                                  │ (Vite+React)  │
                                  │ DuckDB-WASM   │
                                  │ ECharts       │
                                  └──────────────┘
```

---

## CLI Design

### New arguments

```
--local-backend          Run in local mode. No KBC_STORAGE_TOKEN required.
                         Components execute via Docker. Data stored as CSV on disk.
                         (env: KBC_LOCAL_BACKEND=true|1|yes)

--data-dir PATH          Root directory for local data storage.
                         Default: ./keboola_data
                         (env: KBC_DATA_DIR)
```

### Mode comparison

| Property | Platform (default) | Local (`--local-backend`) |
|----------|--------------------|---------------------------|
| Token required | `KBC_STORAGE_TOKEN` | None |
| Components run via | Keboola Jobs API | `docker run` locally |
| Data stored in | Keboola Storage (Snowflake/BQ) | CSV files on disk (`--data-dir`) |
| SQL queries | Keboola workspace (Snowflake/BQ) | Native `duckdb` (Python) |
| Dashboard apps | Streamlit on Keboola platform | Vite + React + DuckDB-WASM on localhost |

### Parsing sketch

```python
parser.add_argument(
    "--local-backend",
    action="store_true",
    default=os.environ.get("KBC_LOCAL_BACKEND", "").lower() in ("true", "1", "yes"),
    help="Run in local mode — no Storage token, components run via Docker",
)
parser.add_argument(
    "--data-dir",
    type=str,
    default=os.environ.get("KBC_DATA_DIR", "./keboola_data"),
    help="Root directory for local data storage (default: ./keboola_data)",
)
```

---

## Tool Surface by Mode

### Shared tools (same interface, different backend)

| Tool | Platform behaviour | Local behaviour |
|------|--------------------|-----------------|
| `get_tables` | Lists tables in Keboola Storage | Scans `<data-dir>/tables/*.csv` |
| `get_buckets` | Lists Storage buckets | Returns single virtual bucket |
| `query_data` | Executes SQL on Snowflake/BQ workspace | Executes SQL via native `duckdb` on CSV files |
| `search` | Semantic search via AI service | Searches filenames and CSV headers |
| `get_project_info` | Returns Keboola project metadata | Returns local project metadata |
| `create_data_app` | Generates and deploys Streamlit app | Generates Vite+React+DuckDB-WASM app |

### Local-only tools (new, not registered in platform mode)

| Tool | Description |
|------|-------------|
| `run_component` | Runs a Keboola component Docker image locally via `docker run` |
| `migrate_to_keboola` | _(Phase 4)_ Uploads local data and configs to Keboola platform |

### Platform-only tools (not registered in local mode)

`run_job`, `get_job`, `list_jobs`, `deploy_data_app`, `create_flow`, `update_flow`, `create_conditional_flow`, `create_sql_transformation`, `update_sql_transformation`, `create_oauth_url`

These tools are simply not registered at startup. The LLM client never sees them in the tool list.

### Conditional registration pattern

```python
# src/keboola_mcp_server/server.py
def create_server(args) -> FastMCP:
    mcp = FastMCP("Keboola MCP Server")

    register_common_tools(mcp)          # shared tools

    if args.local_backend:
        local_backend = LocalBackend(data_dir=args.data_dir)
        register_local_tools(mcp, local_backend)
    else:
        platform_backend = PlatformBackend(...)
        register_platform_tools(mcp, platform_backend)

    return mcp
```

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
│   │   ├── customers.csv
│   │   └── customers.csv.manifest  # Optional: column types, primary key
│   ├── files/                      # Input binary files
│   └── state.json                  # State from previous run (optional)
└── out/
    ├── tables/                     # Component writes output CSVs here
    │   ├── result.csv
    │   └── result.csv.manifest
    ├── files/                      # Output binary files
    └── state.json                  # State to persist (optional)
```

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

Only `parameters` is strictly required for local execution — the component reads it to drive its behaviour. `storage` sections are informational; files are mounted directly.

### Docker execution

```bash
docker run \
  --rm \
  --volume=/path/to/run-dir:/data \
  --memory=4g \
  --net=bridge \
  -e KBC_DATADIR=/data/ \
  -e KBC_RUNID=local-$(date +%s) \
  -e KBC_PROJECTID=0 \
  -e KBC_CONFIGID=local-config \
  -e KBC_COMPONENTID=keboola.python-transformation \
  quay.io/keboola/python-transformation:latest
```

`KBC_TOKEN` is **not set** — it is only needed when a component calls the Storage API, which local components do not.

### Exit code semantics

| Exit code | Meaning |
|-----------|---------|
| `0` | Success |
| `1` | User error (bad config, invalid data) |
| `>1` | Application error (bug in component) |

The `run_component` tool surfaces these distinctly so the LLM client can give the user accurate feedback.

### `run_component` tool signature

```python
@mcp.tool()
def run_component(
    component_image: str,
    parameters: dict,
    input_tables: list[str] | None = None,
    memory_limit: str = "4g",
) -> str:
    """
    Run a Keboola component locally via Docker.

    Uses the Keboola Common Interface: mounts a /data directory with config.json
    and input CSVs, runs the container, and collects output CSVs back into the
    local data catalog.

    Args:
        component_image: Docker image to run (e.g. quay.io/keboola/python-transformation:latest)
        parameters: Component-specific parameters written to config.json
        input_tables: Table names from the local catalog to mount as input
        memory_limit: Docker memory limit (default: 4g)

    Returns:
        JSON with status, output_tables list, and truncated stdout/stderr.
    """
```

---

## Pillar 2 — TypeScript/JavaScript App Generation

### Why TS/JS instead of Streamlit

The existing `create_data_app` tool generates Streamlit apps for the Keboola platform. In local mode the tool generates **native TypeScript/JavaScript single-page applications** that:

- Run anywhere via Docker Compose (no platform dependency)
- Query CSV files in the browser using DuckDB-WASM (no server-side database)
- Render charts with Apache ECharts (best LLM generation target — see below)
- Are self-contained and portable

### Recommended stack

| Layer | Technology | Package | Version |
|-------|-----------|---------|---------|
| Build tool | Vite | `vite` | `^6.0.0` |
| UI framework | React | `react` | `^18.3.1` |
| Language | TypeScript | `typescript` | `^5.6.3` |
| Analytics engine | DuckDB-WASM | `@duckdb/duckdb-wasm` | `>=1.30.0` |
| Arrow IPC | Apache Arrow | `apache-arrow` | `^17.0.0` |
| Charting | Apache ECharts | `echarts` + `echarts-for-react` | `^5.6.0` / `^3.0.2` |
| Container | Docker + nginx | `nginx:stable-alpine` | latest |

### Why ECharts for LLM-generated code

ECharts uses a **purely declarative JSON options object**. An LLM outputs one JSON structure and ECharts renders it — no imperative calls, no JSX component trees, no complex nesting:

```typescript
const option = {
  title: { text: 'Revenue by Region' },
  tooltip: { trigger: 'axis' },
  xAxis: { type: 'category', data: regions },
  yAxis: { type: 'value' },
  series: [{ name: 'Revenue', type: 'bar', data: values }],
};
```

ECharts supports 20+ chart types, handles 100K+ data points via Canvas rendering, and tree-shakes from ~300 KB to ~100 KB.

### Generated app structure

```
generated-app/
├── package.json
├── vite.config.ts
├── tsconfig.json
├── index.html
├── src/
│   ├── main.tsx
│   ├── App.tsx          # LLM-generated dashboard content
│   └── duckdb.ts        # DuckDB-WASM init + query helpers
├── public/
│   └── data/            # Symlinked or copied CSV files
├── Dockerfile
├── nginx.conf
└── docker-compose.yml
```

### Key nginx configuration

```nginx
# Required for DuckDB-WASM SharedArrayBuffer (multi-threaded variant)
add_header Cross-Origin-Opener-Policy "same-origin" always;
add_header Cross-Origin-Embedder-Policy "require-corp" always;

# Correct MIME type for WASM files
types { application/wasm wasm; }
```

### Docker Compose for serving

```yaml
services:
  dashboard:
    build: .
    ports:
      - "3000:80"
    volumes:
      - ./data:/usr/share/nginx/html/data:ro  # hot-reload CSVs without rebuild
    restart: unless-stopped
```

Adding new CSV files requires no rebuild — drop them into `./data/` and refresh.

---

## Pillar 3 — In-Browser DuckDB-WASM

### How it works

DuckDB-WASM compiles to WebAssembly and runs **inside the browser tab**, in a Web Worker. The frontend fetches CSV files from the same-origin nginx server and queries them client-side using standard SQL. There is no DuckDB process, no database server, no extra port.

### WASM variants

`selectBundle()` auto-detects the best build:

| Variant | Description |
|---------|-------------|
| MVP | Baseline WebAssembly — works in all WASM-supporting browsers |
| EH | Native WASM exception handling — recommended default |
| COI | Multi-threaded + SIMD — requires Cross-Origin Isolation headers |

### CSV loading pattern

```typescript
// 1. Fetch CSV from same-origin static file server
const csvText = await fetch('/data/customers.csv').then(r => r.text());

// 2. Register with DuckDB virtual filesystem
await db.registerFileText('customers.csv', csvText);

// 3. Create in-memory table
await conn.query(
  `CREATE TABLE customers AS SELECT * FROM read_csv_auto('customers.csv')`
);

// 4. Query with standard SQL
const result = await conn.query(`
  SELECT country, COUNT(*) AS count FROM customers
  GROUP BY country ORDER BY count DESC LIMIT 10
`);
```

CORS is a non-issue: the app and CSVs are served from the same origin.

### Known limitations vs native DuckDB

| Limitation | Impact for dashboards |
|------------|-----------------------|
| No disk spilling | Queries fail if working set exceeds ~4 GB browser memory — not a concern for typical dashboard datasets |
| Single-threaded by default | ~2–5× slower than native; sub-second for aggregations over millions of rows |
| ~6–7 MB first load | Browser caches after first visit; ~2–4 s on first load |
| No native filesystem | Files must be registered via `registerFileText` / `registerFileBuffer` / `registerFileURL` |
| Extension limits | Network-autoloaded extensions work; native-process extensions (postgres_scanner etc.) do not |

### Security note

DuckDB npm packages versions 1.3.3 and 1.29.2 were briefly compromised in September 2025 (malware). Safe versions were immediately re-published. Use `@duckdb/duckdb-wasm@>=1.30.0` — `^1.29.0` is **not** safe because it resolves to `>=1.29.0 <2.0.0` in npm semver, which includes the compromised 1.29.2.

---

## Local Data Catalog

### Filesystem layout

```
keboola_data/           # --data-dir root
├── tables/
│   ├── customers.csv
│   ├── customers.csv.manifest    # optional: column types, primary key
│   ├── orders.csv
│   └── orders.csv.manifest
├── configs/
│   └── ex-salesforce-001.json   # component configuration
├── apps/
│   └── sales-dashboard/         # generated TS/JS app
│       ├── package.json
│       ├── src/
│       └── docker-compose.yml
└── catalog.json                 # index of tables, configs, metadata
```

### `LocalBackend` class (server-side)

The `LocalBackend` class is the single dependency injected into all local-mode tool implementations:

```python
class LocalBackend:
    def __init__(self, data_dir: str = "./keboola_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # Create tables/ eagerly so glob() never raises OSError on Python 3.13+.
        (self.data_dir / "tables").mkdir(parents=True, exist_ok=True)

    def query_local(self, sql_query: str) -> str:
        """Execute SQL against local CSV files using native DuckDB."""
        con = duckdb.connect()
        tables_dir = self.data_dir / "tables"
        for csv_file in tables_dir.glob("*.csv"):
            # Sanitize table name: substitute double-quote characters with '_'
            # (not deletion) to prevent identifier injection while preserving
            # name uniqueness and avoiding empty-identifier crashes.
            # CREATE OR REPLACE ensures stale tables are refreshed on each call.
            table_name = csv_file.stem.replace('"', '_')
            con.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS '
                "SELECT * FROM read_csv_auto(?)",
                [str(csv_file)],
            )
        # Use cursor-based result formatting (no pandas dependency).
        cursor = con.execute(sql_query)
        # DB-API 2.0 (PEP 249): cursor.description is None for non-SELECT
        # statements (DDL, DML). Guard before iterating to avoid TypeError.
        if cursor.description is None:
            con.close()
            return "Query executed successfully (no rows returned)."
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        con.close()
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        body = [
            "| " + " | ".join("" if v is None else str(v) for v in row) + " |"
            for row in rows
        ]
        return "\n".join([header, separator, *body])

    def run_docker_component(
        self,
        component_image: str,
        parameters: dict,
        input_tables: list[str] | None = None,
        memory_limit: str = "4g",
    ) -> str:
        """Run a Keboola component locally via Docker."""
        ...  # see Pillar 1 above

    def generate_ts_app(
        self,
        name: str,
        description: str,
        sql_queries: list[str],
    ) -> str:
        """Generate a Vite+React+DuckDB-WASM+ECharts dashboard app."""
        ...  # see Pillar 2 above
```

---

## Implementation Phases

### Phase 1 — Foundation (weeks 1–2)

**Goal**: `--local-backend` flag works; local file catalog exists; `get_tables`, `get_buckets`, `query_data`, and `search` work locally.

- Add `--local-backend` and `--data-dir` to `cli.py`
- Implement `LocalBackend` class with filesystem catalog in `tools/local/backend.py`
- Implement local versions of `get_tables`, `get_buckets`, `query_data`, `search`
- Add conditional tool registration in `server.py`
- Integration tests: start server in local mode, verify tool list differs from platform mode, query a CSV
- **New Python dependency**: `duckdb >= 0.10.0` (optional dep, only required in local mode)

### Phase 2 — Docker Component Execution (weeks 3–4)

**Goal**: `run_component` executes Docker containers locally using the Common Interface.

- Implement `run_docker_component()` — creates `/data` temp dir, writes `config.json`, runs `docker run`, collects outputs
- Handle exit codes (0 / 1 / >1) distinctly
- Auto-catalog output tables (copy from `/data/out/tables/` to local catalog)
- Manifest file support (read input manifests, write output manifests)
- Test with real component images: `keboola/python-transformation`, `keboola/ex-http`
- **Requirement**: Docker daemon accessible from the Python process

### Phase 3 — TypeScript/JavaScript App Generation (weeks 5–7)

**Goal**: `create_data_app` generates a working Vite + React + DuckDB-WASM + ECharts app.

- App template directory with all boilerplate (package.json, vite.config.ts, Dockerfile, nginx.conf, docker-compose.yml, duckdb.ts)
- Template engine generates `App.tsx` with SQL queries and ECharts options specific to the user's data and intent
- `docker compose up -d` integration for one-command launch
- Test end-to-end: describe dashboard → generate app → run on localhost:3000 → DuckDB-WASM queries CSVs → ECharts renders
- **Requirements**: Node.js 20+, Docker Compose

### Phase 4 — Polish and Migration (weeks 8–10)

**Goal**: Production-quality local mode; `migrate_to_keboola` skeleton.

- Error handling: Docker not running, invalid CSVs, missing files, timeout
- Progress reporting for long-running Docker component runs
- `migrate_to_keboola` skeleton: CSV upload to Storage API, config conversion
- Stub messages for platform-only tools (`"⚠️ run_job requires the Keboola platform. Use migrate_to_keboola to move your project."`)
- README section on local mode with example workflows
- CI: test matrix covering both platform and local modes

---

## Dependency Summary

### Python (server)

| Package | Version | Purpose |
|---------|---------|---------|
| `duckdb` | `>=0.10.0` | Server-side SQL for `query_data` in local mode |

### npm (generated browser app)

| Package | Version | Purpose |
|---------|---------|---------|
| `@duckdb/duckdb-wasm` | `>=1.30.0` | In-browser SQL engine |
| `apache-arrow` | `^17.0.0` | DuckDB-WASM peer dependency |
| `echarts` | `^5.6.0` | Chart rendering |
| `echarts-for-react` | `^3.0.2` | React wrapper for ECharts |
| `vite` | `^6.0.0` | Build tool |
| `react` | `^18.3.1` | UI framework |
| `typescript` | `^5.6.3` | Type checking |

---

## Migration Path (Future)

A `migrate_to_keboola` tool helps users move local workflows to the Keboola platform:

1. Upload CSVs to Keboola Storage via Storage API (token required at this point)
2. Convert `configs/*.json` to Keboola component configurations via Components API
3. Map local table references in component parameters to Storage table IDs
4. Re-deploy data apps as Streamlit on the platform (or keep TS/JS via custom hosting)

The tool should be interactive and confirm before uploading. Flows and orchestration are platform-native concepts set up after migration.
