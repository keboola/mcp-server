# Local Backend Mode (`--local-backend`)

**Linear**: [AI-2922](https://linear.app/keboola/issue/AI-2922/local-backend-mode-for-keboola-mcp-server-local-backend-flag)
**Status**: Implemented — Phases 1, 2, and partial Phase 4; Phase 3 (TS/JS dashboard) deferred

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
| `get_project_info` | Returns Keboola project metadata | Returns local project metadata (table count, config count, data dir) |

### Local-only tools (new, not registered in platform mode)

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

### Platform-only tools (not registered in local mode)

`run_job`, `get_job`, `list_jobs`, `deploy_data_app`, `modify_data_app`, `create_flow`, `update_flow`, `create_conditional_flow`, `create_sql_transformation`, `update_sql_transformation`, `create_oauth_url`, `docs_query`, `search_semantic_context`, `get_semantic_context`, `get_semantic_schema`, `validate_semantic_query`

These tools are simply not registered at startup. The LLM client never sees them in the tool list.

> ⚠️ **Documentation and semantic tools are unavailable in local mode.** `docs_query` and all semantic tools call the Keboola AI Service API which requires a platform token. There is no local fallback. In local mode, component documentation is available via `get_component_schema` (public Developer Portal, no token) and `search` (filename + column header matching).

### Prompt / instruction filtering by mode

The two modes also serve different system prompt sets. `create_local_server` registers local-specific prompts instead of the platform prompts.

| Prompt category | Platform mode | Local mode |
|----------------|:---:|:---:|
| Storage & bucket management | ✓ | ✗ |
| Jobs & flows | ✓ | ✗ |
| SQL workspace (Snowflake/BQ) | ✓ | ✗ |
| DuckDB / local SQL | ✗ | ✓ |
| Component configuration & schemas | ✓ | ✓ (schema source differs — see §Component schema discovery) |
| Local data management | ✗ | ✓ |

### Server factory pattern

Rather than a single `create_server` that branches on `--local-backend`, the implementation uses two separate factory functions. This keeps the platform path completely untouched and lets the local path implement its own middleware, session handling, and prompt registration independently.

There is **no new `PlatformBackend` wrapper class** — `create_platform_server` simply encapsulates the existing server code as-is.

Even though shared tools (`get_tables`, `query_data`, etc.) expose the same MCP name and I/O signature in both modes, they are **registered separately** inside each factory with different implementations. This avoids a shared-registration abstraction that would couple the two paths.

```python
# src/keboola_mcp_server/server.py

def create_platform_server(config: Config) -> FastMCP:
    """Standard platform-backed server — unchanged from today."""
    mcp = FastMCP("Keboola MCP Server")
    # existing middleware: token auth, WorkspaceManager, ProjectLinksManager, …
    register_platform_tools(mcp, config)      # includes platform versions of shared tools
    register_platform_prompts(mcp)
    return mcp

def create_local_server(data_dir: str) -> FastMCP:
    """Local-backend server. No token required. Persistent single-instance state."""
    mcp = FastMCP("Keboola MCP Server (local)")
    local_backend = LocalBackend(data_dir=data_dir)
    # no auth middleware, no WorkspaceManager, no ProjectLinksManager
    # persistent in-memory session state (single instance — no multi-tenancy concern)
    register_local_tools(mcp, local_backend)  # includes local versions of shared tools
    return mcp
```

`cli.py` selects which factory to call at startup based on `--local-backend`.

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

### Source-based execution (typical — most components)

Most Keboola components are open source and designed to be built and run locally from source using the same `docker-compose.yml` the component authors use for development. This is the recommended path.

```bash
# 1. Clone the component repository
git clone https://github.com/keboola/<component-name>.git
cd <component-name>

# 2. Build the local Docker image
docker compose build

# 3. Install component dependencies (language-specific)
docker compose run --rm dev composer install        # PHP / Composer
# docker compose run --rm dev npm ci                # Node.js
# docker compose run --rm dev pip install -r requirements.txt  # Python

# 4. Prepare the data directory and write config.json
mkdir -p data
# place your config.json at ./data/config.json (see §config.json above)

# 5. Run the component
docker compose run --rm dev php run.php
# docker compose run --rm dev python main.py       # Python entry point
# (check the component's README for the correct entry point)
```

The component's `docker-compose.yml` typically defines a `dev` service that:
- Mounts the project root to `/code` inside the container (source hot-reload)
- Mounts `./data` → `/data` (satisfying the Common Interface contract)
- Sets `KBC_DATADIR=/data/`

No custom wiring is required — this is exactly how the component's own CI and developer docs use it.

### Registry image execution (pre-built / CI images)

When a pre-built image is available (e.g. a released component image on Quay, ECR, or Docker Hub) and you do not need the source, you can skip the clone+build steps and run via plain `docker run`:

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
async def run_component(
    parameters: dict,
    component_image: str | None = None,
    git_url: str | None = None,
    input_tables: list[str] | None = None,
    memory_limit: str = "4g",
) -> ComponentRunResult:
    """
    Run a Keboola component locally via Docker.

    Exactly one of `git_url` or `component_image` must be provided.

    Source-based mode (recommended — most components):
        Provide `git_url`. The tool clones the repo into
        <data-dir>/components/<repo-name>/ (or reuses an existing clone),
        runs `docker compose build`, installs dependencies, then executes the
        component via `docker compose run --rm dev`. setup_component is called
        automatically — you do not need to call it first.

    Registry image mode (pre-built images):
        Provide `component_image`. The tool runs the image directly via
        `docker run` without any clone or build step.

    In both modes the tool:
    - Writes config.json with the supplied `parameters` into the /data directory
    - Mounts input CSVs from the local catalog
    - Collects output CSVs back into the local catalog after the run

    Args:
        parameters: Component-specific parameters written to config.json
        component_image: Docker image to run directly
                         (e.g. quay.io/keboola/python-transformation:latest)
        git_url: Git repository URL of the component
                 (e.g. https://github.com/keboola/generic-extractor.git)
        input_tables: Table names from the local catalog to mount as input
        memory_limit: Docker memory limit (default: 4g)

    Returns:
        ComponentRunResult with status, exit_code, output_tables, stdout, stderr.
    """
```

Clones are cached under `<data-dir>/components/<repo-name>/` so subsequent `run_component` calls on the same component skip the clone and only re-run `docker compose build` if needed.

### `setup_component` tool signature

```python
@mcp.tool()
async def setup_component(
    git_url: str,
    force_rebuild: bool = False,
) -> ComponentSetupResult:
    """
    Clone a Keboola component repository and build its Docker image.

    This is an optional pre-warming step. run_component calls setup_component
    automatically when git_url is provided and the component is not yet built.
    Call setup_component explicitly only to force a rebuild or to pre-warm before
    a time-sensitive run_component call.

    The clone is stored at <data-dir>/components/<repo-name>/. Subsequent calls
    are no-ops unless force_rebuild=True.

    Args:
        git_url: Git repository URL
                 (e.g. https://github.com/keboola/generic-extractor.git)
        force_rebuild: Re-run docker compose build even if the image exists

    Returns:
        ComponentSetupResult with status, path, and component_schema (if found).
    """
```

---

## Component Schema Discovery in Local Mode

In platform mode, the agent fetches component config schemas and examples from the AI Service API (`get_config_examples`, `find_component_id`). That API is not available in local mode. The following three-tier fallback covers it:

### Tier 1 — Cloned repo introspection (best)

When `setup_component` clones a repo, it scans for schema files in well-known locations:
- `component.json` — Keboola Developer Portal manifest (includes `configSchema`)
- `src/component.py` / metadata annotations if present
- `README.md` — contains parameter documentation for most components

The `setup_component` tool surfaces the schema content so the agent can use it when composing `config.json`.

### Tier 2 — Keboola Developer Portal API (public, no auth)

```
GET https://components.keboola.com/components/{component_id}
```

Returns the component manifest including `configSchema` and `configRowSchema`. A new local-only tool `get_component_schema` wraps this endpoint:

```python
@mcp.tool()
def get_component_schema(component_id: str) -> str:
    """Fetch the config schema for a Keboola component from the public Developer Portal."""
```

`find_component_id` is also reimplemented in local mode to query this public API instead of the AI Service, so the agent can still map human-readable names to component IDs and Docker image paths.

### Tier 3 — Fallback

_Not implemented._ If neither Tier 1 nor Tier 2 yields a schema (e.g. Developer Portal is offline and the repo wasn't cloned), instruct the user to consult the component's README directly. Most components document their parameters under a **Configuration** or **Parameters** heading.

---

## Pillar 2 — TypeScript/JavaScript App Generation

### Why TS/JS instead of Streamlit

The existing `modify_data_app` tool generates Streamlit apps for the Keboola platform. In local mode the tool generates **native TypeScript/JavaScript single-page applications** that:

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
│   └── orders.csv
├── configs/
│   └── ex-salesforce-001.json   # saved ComponentConfig JSON
├── components/                  # cloned repos for source-based execution
│   └── generic-extractor/       # git clone of the component
│       ├── docker-compose.yml   # used to build and run the component
│       ├── .keboola_image_built # sentinel — skip rebuild if present
│       ├── data/                # /data dir mounted into the container
│       │   ├── config.json
│       │   ├── in/tables/
│       │   └── out/tables/
│       └── ...
└── runs/                        # temporary /data dirs for registry-image runs
    └── <timestamp>/             # auto-created per run_component call (image mode)
        ├── config.json
        ├── in/tables/
        └── out/tables/
```

Notes:
- No `catalog.json` — the filesystem is the catalog; `tables/*.csv` glob is the authoritative list.
- No `apps/` — Phase 3 (TS/JS dashboard generation) is deferred.
- `runs/` directories are created temporarily and output tables are collected back into `tables/` after each run.

### `LocalBackend` class (server-side)

The `LocalBackend` class is the single dependency injected into all local-mode tool implementations:

```python
class LocalBackend:
    def __init__(self, data_dir: str = "./keboola_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # Create sub-dirs eagerly so glob() never raises OSError on Python 3.13+.
        (self.data_dir / "tables").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "configs").mkdir(parents=True, exist_ok=True)

    # ---- Catalog ----
    def list_csv_tables(self) -> list[Path]: ...
    def read_csv_headers(self, csv_path: Path) -> list[str]: ...
    def write_csv_table(self, name: str, csv_content: str) -> Path: ...
    def delete_csv_table(self, name: str) -> bool: ...

    # ---- SQL ----
    def query_local(self, sql_query: str) -> str:
        """Execute SQL against local CSV files using native DuckDB.
        Registers every *.csv under tables/ as a DuckDB table, runs the query,
        and returns the result as a Markdown table. Non-SELECT statements return
        a plain success message. Table names are sanitised (double-quote → '_').
        """
        ...

    # ---- Component configurations ----
    def save_config(self, config: ComponentConfig) -> ComponentConfig: ...
    def list_configs(self) -> list[ComponentConfig]: ...
    def load_config(self, config_id: str) -> ComponentConfig: ...
    def delete_config(self, config_id: str) -> bool: ...

    # ---- Docker component execution ----
    def setup_component(self, git_url: str, force_rebuild: bool = False) -> ComponentSetupResult: ...
    def run_docker_component(self, component_image: str, parameters: dict,
                             input_tables: list[str] | None, memory_limit: str) -> ComponentRunResult: ...
    def run_source_component(self, git_url: str, parameters: dict,
                             input_tables: list[str] | None, memory_limit: str) -> ComponentRunResult: ...

    # ---- Platform migration ----
    async def migrate_to_keboola(self, storage_api_url: str, storage_token: str,
                                 table_names: list[str] | None = None,
                                 config_ids: list[str] | None = None,
                                 bucket_id: str = "in.c-local") -> MigrateResult: ...
```

---

## Implementation Phases

### Phase 1 — Foundation ✅ Done

**Goal**: `--local-backend` flag works; local file catalog exists; `get_tables`, `get_buckets`, `query_data`, and `search` work locally.

- Add `--local-backend` and `--data-dir` to `cli.py`
- Implement `LocalBackend` class with filesystem catalog in `tools/local/backend.py`
- Implement local versions of `get_tables`, `get_buckets`, `query_data`, `search`
- Add conditional tool registration in `server.py`
- Integration tests: start server in local mode, verify tool list differs from platform mode, query a CSV
- **New Python dependency**: `duckdb >= 0.10.0` (optional dep, only required in local mode)

### Phase 2 — Docker Component Execution ✅ Done

**Goal**: `run_component` executes Docker containers locally using the Common Interface.

- Implement `setup_component()` — git clone into `<data-dir>/components/<repo-name>/`, `docker compose build`, detect and install dependencies
  - PHP: `docker compose run --rm dev composer install` (presence of `composer.json` without `vendor/`)
  - Node.js: `docker compose run --rm dev npm ci` (presence of `package.json` without `node_modules/`)
  - Python: `docker compose run --rm dev pip install -r requirements.txt` (presence of `requirements.txt` without `.venv/`)
  - Skip reinstall on subsequent calls when the dependency directory already exists
- Implement `run_docker_component()` — two paths:
  - **Source-based** (`component_git_url`): call `setup_component`, write `config.json` into `<component-dir>/data/`, mount via component's own `docker-compose.yml`, run `docker compose run --rm dev <entrypoint>` (entry point read from the `dev` service command in the component's `docker-compose.yml`, fallback to component's README or user-supplied override)
  - **Registry image** (`component_image`): create a temp `/data` dir, write `config.json`, run plain `docker run --rm --volume=...:/data` as before
- Handle exit codes (0 / 1 / >1) distinctly in both paths
- Auto-catalog output tables (copy from `<data-dir>/out/tables/` or `<component-dir>/data/out/tables/` to local catalog)
- Manifest file support (read input manifests, write output manifests)
- Test with real component repos: `keboola/generic-extractor`, `keboola/python-transformation`
- **Requirement**: Docker daemon accessible from the Python process; `git` available on PATH

### Phase 3 — TypeScript/JavaScript App Generation ⏸ Deferred

**Goal**: `modify_data_app` generates a working Vite + React + DuckDB-WASM + ECharts app.

- App template directory with all boilerplate (package.json, vite.config.ts, Dockerfile, nginx.conf, docker-compose.yml, duckdb.ts)
- Template engine generates `App.tsx` with SQL queries and ECharts options specific to the user's data and intent
- `docker compose up -d` integration for one-command launch
- Test end-to-end: describe dashboard → generate app → run on localhost:3000 → DuckDB-WASM queries CSVs → ECharts renders
- **Requirements**: Node.js 20+, Docker Compose

### Phase 4 — Polish and Migration 🔶 Partial

**Goal**: Production-quality local mode; `migrate_to_keboola`.

Done:
- ✅ `migrate_to_keboola` — direct Storage API upload (tables + configs)
- ✅ `write_table`, `delete_table`, `save_config`, `list_configs`, `delete_config`, `run_saved_config`
- ✅ Error handling: per-item error capture, timeout (5 min default), invalid table name validation

Not done:
- ❌ Progress reporting for long-running Docker component runs
- ❌ Stub messages for platform-only tools
- ❌ CI test matrix covering local mode with Docker

---

## End-to-End Smoke Tests

Unit tests mock Docker/HTTP. These manual tests verify real execution:

### Test 1 — Registry image (Python transformation)

```bash
mkdir -p /tmp/smoke/tables
printf 'id,amount\n1,100\n2,200\n3,50\n' > /tmp/smoke/tables/sales.csv
```

Then use the MCP `run_component` tool:
```json
{
  "component_image": "quay.io/keboola/python-transformation:latest",
  "parameters": {
    "blocks": [{
      "name": "block1",
      "codes": [{
        "name": "transform",
        "script": [
          "import csv",
          "rows = list(csv.DictReader(open('/data/in/tables/sales.csv')))",
          "total = sum(float(r['amount']) for r in rows)",
          "open('/data/out/tables/result.csv','w').write('total\\n'+str(total)+'\\n')"
        ]
      }]
    }]
  },
  "input_tables": ["sales"]
}
```

Expected: `result.csv` appears in the local catalog with `total=350`.

### Test 2 — Source-based component (Generic Extractor)

```json
// setup_component
{ "git_url": "https://github.com/keboola/generic-extractor.git" }

// run_component
{
  "git_url": "https://github.com/keboola/generic-extractor.git",
  "parameters": {
    "api": { "baseUrl": "https://jsonplaceholder.typicode.com/" },
    "config": { "jobs": [{ "endpoint": "todos", "dataType": "todos" }] }
  }
}
```

Expected: `todos.csv` appears in the local catalog (~200 rows from the public API).

### Prerequisites for smoke tests
- Docker daemon running (`docker info` should succeed)
- `git` on PATH
- First clone + build takes several minutes; subsequent runs are fast

---

## Dependency Summary

### Python (server)

| Package | Version | Purpose |
|---------|---------|---------|
| `duckdb` | `>=0.10.0` | Server-side SQL for `query_data` in local mode |
| `httpx` | `>=0.27.0` | Async HTTP client for `migrate_to_keboola` (Storage API) and schema discovery (Developer Portal) |

`httpx` is already a dependency of the platform server (used by existing tools). `duckdb` is an optional extra: `pip install "keboola-mcp-server[local]"`.

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

## Migration — Direct Storage API

`migrate_to_keboola` calls the Keboola Storage API directly via `httpx`. No `keboola-cli` installation required.

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `storage_api_url` | Stack URL, e.g. `https://connection.keboola.com` |
| `storage_token` | Write-access Storage API token |
| `table_names` | Optional list of table names to migrate (default: all) |
| `config_ids` | Optional list of config IDs to migrate (default: all) |
| `bucket_id` | Target bucket (default: `in.c-local`, created if absent) |

**Flow:**

1. Creates the target bucket via `POST /v2/storage/buckets`; 422 responses are silently ignored (bucket already exists).
2. Uploads each CSV table as multipart `POST /v2/storage/buckets/{id}/tables`. HTTP 422 is captured as `status="already_exists"` — not treated as an error; the existing table is left untouched.
3. Creates each saved config via `POST /v2/storage/components/{componentId}/configs`.

**Returns** `MigrateResult`:
- Per-table entries: `status` one of `uploaded | already_exists | error`
- Per-config entries: `status` one of `created | error`
- Summary counts: `tables_ok`, `tables_error`, `configs_ok`, `configs_error`

Migration is atomic — there is no per-step confirmation. If a step fails, its error is captured in the result and remaining items continue processing.

---

## Known Gaps

| Gap | Impact | Notes |
|-----|--------|-------|
| `docs_query` / semantic tools unavailable | No Keboola documentation search in local mode | All require Keboola AI Service token — no workaround |
| Phase 3 (dashboard generation) deferred | No `modify_data_app` in local mode | Vite+React+DuckDB-WASM template not built |
| Schema Tier 3 not implemented | No example-file fallback if Developer Portal is offline | Instruct user to read component README |
| No progress reporting for Docker runs | Long-running components block silently | stdout/stderr available in `ComponentRunResult` after completion |
| No stub messages for platform-only tools | LLM never sees helpful "use migrate_to_keboola instead" hint | Could add a local-mode prompt instruction |
| No CI matrix for local mode | Docker-dependent tests not run in CI | Docker daemon required — consider separate workflow |
