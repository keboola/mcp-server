# Architecture Diagrams — Keboola MCP Server

All diagrams use [Mermaid](https://mermaid.js.org/) syntax (rendered by GitHub, VS Code, Obsidian, etc.).

---

## 1. Tech Stack

```mermaid
graph TB
    subgraph AI_Client["AI Client (Claude / Cursor / VS Code)"]
        LLM[LLM]
    end

    subgraph MCP_Server["Keboola MCP Server (Python 3.10–3.12)"]
        direction TB
        FastMCP["FastMCP 2.14.5\n(tool registry, transport)"]
        Pydantic["Pydantic 2.13\n(input/output models)"]
        PyJWT["PyJWT + cryptography\n(OAuth — platform mode only)"]

        subgraph Backends["Backend (selected at startup)"]
            direction LR
            PlatformBackend["Platform Backend\n(default)\nStorage API · Jobs API\nSnowflake / BigQuery SQL"]
            LocalBackend["Local Backend\n(--local-backend)\nFilesystem catalog\nDuckDB SQL\nDocker exec"]
        end

        FastMCP --> Pydantic
        FastMCP --> Backends
    end

    subgraph Local_Infra["Local Infrastructure"]
        DuckDB["DuckDB ≥0.10\n(CSV → SQL)"]
        Docker["Docker daemon\n(component execution)"]
        HTTPServer["Python http.server\n(dashboard serving)"]
        FS["Filesystem\nkeboola_data/\n  tables/*.csv\n  configs/*.json\n  apps/*.html"]
    end

    subgraph Platform["Keboola Platform"]
        StorageAPI["Storage API\n(httpx 0.28)"]
        DevPortal["Developer Portal\n(component schemas)"]
        Snowflake["Snowflake / BigQuery\n(workspace SQL)"]
    end

    subgraph Browser["Browser"]
        ECharts["Apache ECharts 5\n(CDN — charts)"]
        PicoCSS["Pico CSS 2\n(CDN — styling)"]
    end

    LLM <-->|"MCP Protocol\nstdio or streamable-HTTP"| FastMCP
    LocalBackend --> DuckDB
    LocalBackend --> Docker
    LocalBackend --> HTTPServer
    LocalBackend --> FS
    PlatformBackend -->|httpx| StorageAPI
    PlatformBackend -->|httpx| DevPortal
    PlatformBackend -->|httpx| Snowflake
    HTTPServer -->|serves index.html| Browser
    Browser --> ECharts
    Browser --> PicoCSS
```

---

## 2. Tool Surface — Local vs Platform

```mermaid
graph LR
    subgraph Shared["✅ Shared Tools (same interface, different backend)"]
        direction TB
        GT["get_tables"]
        GB["get_buckets"]
        QD["query_data"]
        SR["search"]
        PI["get_project_info"]
    end

    subgraph LocalOnly["🟡 Local-Only Tools"]
        direction TB
        subgraph Docker_Tools["Docker / Components"]
            SC["setup_component"]
            RC["run_component"]
            GCS["get_component_schema"]
            FCI["find_component_id"]
        end
        subgraph Catalog_Tools["CSV Catalog"]
            WT["write_table"]
            DT["delete_table"]
        end
        subgraph Config_Tools["Config Management"]
            SVC["save_config"]
            LC["list_configs"]
            DC["delete_config"]
            RSC["run_saved_config"]
        end
        subgraph Dashboard_Tools["HTML Dashboards"]
            CDA["create_data_app"]
            RDA["run_data_app"]
            LDA["list_data_apps"]
            SDA["stop_data_app"]
            DDA["delete_data_app"]
        end
        MTK["migrate_to_keboola"]
    end

    subgraph PlatformOnly["🔴 Platform-Only Tools (not in local mode)"]
        direction TB
        subgraph Jobs["Jobs / Flows"]
            RJ["run_job"]
            GJ["get_jobs"]
            CF["create_flow"]
            UF["update_flow"]
            CCF["create_conditional_flow"]
        end
        subgraph SQL_T["SQL Transformations"]
            CST["create_sql_transformation"]
            UST["update_sql_transformation"]
        end
        subgraph Apps["Streamlit Apps"]
            DAP["deploy_data_app"]
            MAP["modify_data_app"]
        end
        subgraph Docs["AI / Docs"]
            DQ["docs_query"]
            SSC["search_semantic_context"]
        end
        subgraph PlatConfig["Platform Configs"]
            CC["create_config"]
            UC["update_config"]
            ACR["add_config_row"]
        end
    end

    subgraph LocalAlts["🔁 Local Alternatives"]
        RSC2["run_saved_config\n run_component"]
        QD2["query_data\n(DuckDB SQL)"]
        CDA2["create_data_app\nrun_data_app"]
        SVC2["save_config\nlist_configs"]
    end

    RJ -.->|"use instead"| RSC2
    CST -.->|"use instead"| QD2
    DAP -.->|"use instead"| CDA2
    CC -.->|"use instead"| SVC2
```

---

## 3. Data App — End-to-End Flow

```mermaid
sequenceDiagram
    actor User
    participant LLM as LLM (Claude)
    participant MCP as MCP Server (Python)
    participant DuckDB as DuckDB Engine
    participant FS as Filesystem<br/>keboola_data/
    participant HTTP as Python http.server<br/>(background process)
    participant Browser as Browser

    User->>LLM: "Create a dashboard for iris data"
    LLM->>MCP: create_data_app(name, title, charts[{sql, type, ...}])

    loop For each chart
        MCP->>DuckDB: execute SQL query
        DuckDB->>FS: read tables/*.csv
        DuckDB-->>MCP: {columns, rows} as Python dict
    end

    MCP->>MCP: generate_dashboard_html(config, chart_data)<br/>embed data as JSON in &lt;script type="application/json"&gt;<br/>escape &lt;/ → &lt;\/ (XSS prevention)
    MCP->>FS: write apps/iris-dashboard/index.html
    MCP->>FS: write apps/iris-dashboard/app.json
    MCP-->>LLM: {status: "ok", app_url: "http://localhost:8101/..."}

    LLM->>MCP: run_data_app("iris-dashboard")
    MCP->>MCP: find free port 8101-8199<br/>(socket.bind test)
    MCP->>HTTP: Popen(['python','-m','http.server','8101'], cwd=keboola_data/)
    MCP->>FS: register PID in apps/.running.json
    MCP-->>LLM: {url: "http://localhost:8101/apps/iris-dashboard/", port: 8101}
    LLM-->>User: "Open http://localhost:8101/apps/iris-dashboard/"

    User->>Browser: open URL
    Browser->>HTTP: GET /apps/iris-dashboard/
    HTTP->>FS: read index.html from disk
    HTTP-->>Browser: 200 OK — raw HTML

    Note over Browser: Browser parses HTML

    Browser->>Browser: 1. Load Pico CSS from CDN<br/>   → classless CSS, auto dark/light theme

    Browser->>Browser: 2. Load Apache ECharts from CDN<br/>   → echarts object available in window

    Browser->>Browser: 3. JSON.parse(&lt;script id="app-config"&gt;)<br/>   → chart specs array

    loop For each chart
        Browser->>Browser: 4. JSON.parse(&lt;script id="chart-data-{id}"&gt;)<br/>   → {columns, rows} — pre-computed data
        Browser->>Browser: 5. echarts.init(div)<br/>   build option object from spec + data<br/>   chart.setOption(option)
        Browser->>Browser: 6. ECharts renders SVG/Canvas chart
    end

    Note over Browser: Dashboard visible — no server round-trips needed<br/>All data was baked in at create_data_app time
```

---

## 4. Guided Workflows for New Users

### Workflow A — "I have data, show me insights"

```mermaid
flowchart TD
    A([Start: I have a CSV file]) --> B["write_table(name, csv_content)\nUpload CSV to local catalog"]
    B --> C["get_tables()\nVerify table loaded,\ncheck column names"]
    C --> D["query_data(sql)\nExplore with DuckDB SQL\ne.g. SELECT * FROM table LIMIT 10"]
    D --> E{Happy with data?}
    E -->|No — fix data| B
    E -->|Yes| F["create_data_app(name, charts)\nBuild dashboard with SQL-driven charts\nbar / pie / scatter / table"]
    F --> G["run_data_app(name)\nStart local HTTP server"]
    G --> H([Open URL in browser\nhttp://localhost:810x/...])
    H --> I{Want to share\nwith team?}
    I -->|Yes| J["migrate_to_keboola(\n  storage_api_url,\n  storage_token\n)\nUpload tables to Keboola platform"]
    I -->|No| K([Done])
    J --> K
```

### Workflow B — "I need to extract data from an external source"

```mermaid
flowchart TD
    A([Start: I need data from API / FTP / DB]) --> B["find_component_id('http extractor')\nSearch Keboola component registry"]
    B --> C["get_component_schema(component_id)\nUnderstand config format\nfrom Developer Portal"]
    C --> D{Component\nalready built?}
    D -->|No — first time| E["setup_component(git_url)\nClone repo + docker compose build\n⏳ can take 2–10 min\ntail -f run.log to watch"]
    D -->|Yes — skip| F
    E --> F["run_component(\n  component_image or git_url,\n  parameters={...}\n)\nExecute via Docker\n→ CSV written to catalog"]
    F --> G["get_tables()\nVerify output tables appeared"]
    G --> H["query_data(sql)\nInspect extracted data"]
    H --> I{Need to transform?}
    I -->|Yes| J["query_data(CREATE TABLE ... AS SELECT ...)\nTransform with DuckDB SQL"]
    J --> K
    I -->|No| K["create_data_app(...)\nVisualize results"]
    K --> L["migrate_to_keboola(...)\nOptionally push to platform"]
    L --> M([Done])
```

### Workflow C — "Push local work to Keboola"

```mermaid
flowchart TD
    A([Start: I have local tables + configs]) --> B["get_project_info()\nSee table count, config count,\ndata directory"]
    B --> C["get_tables()\nList all local CSV tables"]
    C --> D["query_data(sql)\nValidate data quality\ne.g. SELECT COUNT(*) / check nulls"]
    D --> E{Data ready?}
    E -->|No — fix it| F["write_table() / query_data()\nClean or transform data"]
    F --> D
    E -->|Yes| G["list_configs()\nReview saved component configs"]
    G --> H["migrate_to_keboola(\n  storage_api_url='https://connection.keboola.com',\n  storage_token='your-token',\n  table_names=[...],   ← optional subset\n  config_ids=[...]     ← optional subset\n)\nUpload tables + create configs on platform"]
    H --> I{Check result}
    I -->|tables_error > 0| J[Check error messages\nin MigrateResult]
    J --> H
    I -->|All OK| K([Tables visible in\nKeboola Storage UI])
```

---

## 5. Component Docker Execution Flow

```mermaid
flowchart LR
    subgraph Local["Local Machine"]
        MCP["MCP Server\n(Python)"]
        subgraph RunDir["keboola_data/runs/run-{ts}/"]
            Config["config.json\n{parameters, storage}"]
            InTables["in/tables/\n*.csv  ← copied from catalog"]
            OutTables["out/tables/\n*.csv  ← written by component"]
            LogFile["run.log\n← streamed in real-time"]
        end
        Catalog["keboola_data/tables/\n*.csv (local catalog)"]
    end

    subgraph DockerContainer["Docker Container"]
        Component["Keboola Component\n(any language)\nreads /data/config.json\nreads /data/in/tables/*.csv\nwrites /data/out/tables/*.csv"]
    end

    MCP -->|"1. prepare_data_dir()\ncopy input CSVs"| RunDir
    MCP -->|"2. docker run --volume run_dir:/data\nor docker compose run"| DockerContainer
    DockerContainer -->|"stdout+stderr → "| LogFile
    DockerContainer -->|"writes output CSVs"| OutTables
    MCP -->|"3. collect_output_tables()\ncopy CSVs to catalog"| Catalog
    MCP -->|"4. return ComponentRunResult\n{status, exit_code, log_file}"| MCP
```
