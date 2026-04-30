# RFC-002: Live Data Apps via Query Service Emulator

**Status:** Implemented (v1.60.0)
**Branch:** AI-2922-local-backend-phase3

## Problem

Local data apps baked query results into the HTML at `create_data_app` time. This meant:
- Charts showed stale data unless the user recreated the app
- No `Refresh` button was useful (data never changed)
- The local HTML structure was completely different from production data apps that use live queries
- **Migration was a rewrite**, not a config change

The core requirement: **local data apps must be structurally identical to production data apps** so that migrating means only changing credentials/URL, not rewriting JS code.

## Decision

### Query Service HTTP API Contract

The production [Keboola Query Service](https://query.keboola.com) uses this 3-step HTTP API:

```
POST /api/v1/branches/{branchId}/workspaces/{workspaceId}/queries
     Body: {statements:[str], transactional:bool, actorType:str}
     → {queryJobId: str}

GET  /api/v1/queries/{queryJobId}
     → {status:"completed"|"failed", statements:[{id, status}]}

GET  /api/v1/queries/{queryJobId}/{statementId}/results
     → {status, columns:[{name,type,nullable}], data:[[val,...],...]}
```

### Architecture: Single-Port Combined Server

One Python process serves **both** static HTML files and the Query Service API:

```
http://localhost:810x/
  GET  /apps/iris-dashboard/     → static HTML
  POST /api/v1/branches/*/workspaces/*/queries  → DuckDB exec → jobId
  GET  /api/v1/queries/{jobId}                  → completed status
  GET  /api/v1/queries/{jobId}/{stmtId}/results → columns + data
```

**DuckDB is synchronous**, so the emulator executes queries immediately on POST and returns `completed` on the first GET poll.

### HTML Template: Live Queries

The HTML template was rewritten to:
1. Embed `queryService: {token, branchId, workspaceId}` in the app config JSON (no pre-baked data)
2. Use `window.location.origin` as `base_url` (auto-detects any port)
3. Call the Query Service API directly via `fetch()` — same HTTP contract as production

The JS client (`qs_execute`) makes the same three HTTP calls as the production SDK. When migrating to production, only the config values change:

| Setting | Local | Production |
|---------|-------|-----------|
| `base_url` | `window.location.origin` | `https://query.keboola.com` |
| `token` | `"local"` (ignored) | real Storage API token |
| `branchId` | `"local"` | real branch ID |
| `workspaceId` | `"local"` | real workspace ID |
| JS frontend | identical | identical |

### Refresh Button

The new template includes a `⟳ Refresh` button that re-executes all chart queries live. This is meaningful now that data is fetched dynamically.

## Files Changed

- `src/keboola_mcp_server/tools/local/appserver.py` — **new** — Query Service emulator + static file server
- `src/keboola_mcp_server/tools/local/dataapp.py` — new HTML template; `generate_dashboard_html(config)` no longer takes `chart_data`
- `src/keboola_mcp_server/tools/local/backend.py` — `save_data_app(config)` (no `chart_data`); `start_data_app()` spawns `appserver` instead of `http.server`
- `src/keboola_mcp_server/tools/local/tools.py` — `create_data_app_local()` no longer pre-executes queries
- `tests/tools/local/test_dataapp.py` — updated for new API (no `chart_data` param; assert `appserver` in cmd)

---

## Migrating Local Data Apps to the Keboola Platform

Local data apps (ECharts + Pico CSS HTML) and Keboola platform data apps (Streamlit)
are different runtimes. `migrate_to_keboola` intentionally skips data apps because a
direct file transfer is not meaningful. The SQL queries, however, transfer cleanly.

### App config source of truth

Every local data app persists its configuration to:

```
<data_dir>/apps/<name>/app.json
```

`app.json` is a JSON object matching `DataAppConfig` — it contains the full list of
chart definitions including SQL queries, chart type, x/y column assignments, and the
app title/description. This is the migration source.

### Migration steps (today — manual)

1. **Read the local app config:**

   ```bash
   cat keboola_data/apps/my-dashboard/app.json
   ```

2. **Extract the SQL queries** from each chart's `sql` field.

3. **Write a Streamlit script** using the extracted SQL. Each chart becomes a
   `st.dataframe(query_data(sql))` or an `st.altair_chart(...)` call. A minimal
   template:

   ```python
   import streamlit as st

   st.title("My Dashboard")

   with st.expander("Tickets by Stage"):
       df = query_data("SELECT stage, COUNT(*) AS n FROM tickets GROUP BY stage")
       st.bar_chart(df.set_index("stage"))
   ```

4. **Create the platform app** (run in platform mode, not local mode):

   ```
   modify_data_app(name="my-dashboard", script=<streamlit_source>, ...)
   deploy_data_app(config_id=<returned_id>, action="deploy")
   ```

   `modify_data_app` injects the production `query_data` function automatically;
   local SQL queries work unchanged since DuckDB and Snowflake/BigQuery dialects
   are compatible for simple SELECT/GROUP BY/aggregation patterns.

### Future tool: `migrate_data_app` (Phase 4)

A `migrate_data_app(name, storage_api_url, storage_token)` tool will automate steps
1–4:

1. Read `app.json` from the local apps directory
2. Generate a Streamlit script from the chart SQL queries using a standard template
3. Call `modify_data_app` (platform API) to create the app
4. Return the Keboola UI URL

This is tracked as a Phase 4 item in `docs/local-backend.md`.
