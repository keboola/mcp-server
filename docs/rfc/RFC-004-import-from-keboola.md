# RFC-004: Reverse Migration — Import from Keboola Platform to Local

**Status:** Proposed
**Follows:** RFC-003 (semantic layer), `_ensure_bucket` 400 fix in `migrate.py`

## Problem

`migrate_to_keboola` is a one-way operation: local → platform.  Users who develop
primarily on the Keboola platform and want to work locally on a subset of data or
test a component configuration must manually:

1. Export each table from the Keboola UI (one by one)
2. Download the CSV and copy it into `<data_dir>/tables/`
3. Copy component config JSON from the UI into `<data_dir>/configs/`

This friction prevents the local backend from serving as a true development sandbox —
the round-trip is the primary use-case for "test locally, push to prod."

## Decision

New MCP tool: **`import_from_keboola`**

```python
import_from_keboola(
    storage_api_url: str | None = None,   # pre-filled if server started with --api-url
    storage_token: str | None = None,     # pre-filled if server started with --storage-token
    table_ids: list[str] | None = None,   # e.g. ["in.c-crm.contacts", "in.c-orders.orders"]
    bucket_ids: list[str] | None = None,  # shorthand: import all tables from these buckets
    config_ids: list[str] | None = None,  # Keboola config IDs to import
) -> ImportResult
```

Omitting all of `table_ids`, `bucket_ids`, `config_ids` is an error — to avoid
accidentally pulling an entire project, the user must always be explicit.

### Table import flow

```
POST /v2/storage/tables/{table_id}/export-async
     → {id: jobId}

GET  /v2/storage/jobs/{jobId}          (poll until status=success)
     → {results: {url: <signed CSV URL>}}

GET  <signed CSV URL>                  (download CSV bytes)

write to <data_dir>/tables/<table_stem>.csv
detect_and_save_schema(csv_path)       (RFC-003 integration)
```

The table stem is derived from the last segment of the table ID:
`in.c-crm.contacts` → `contacts.csv`.

When two tables from different buckets share the same stem, the bucket name is
prepended: `in.c-crm.contacts` → `crm_contacts.csv`.

### Config import flow

```
GET /v2/storage/components/{component_id}/configs/{config_id}
    → {id, name, configuration: {parameters, ...}}

save to <data_dir>/configs/<local_id>.json
```

`local_id` defaults to the Keboola config ID. If a config with that ID already
exists locally, the import is skipped and reported as `skipped` (not overwritten)
unless `overwrite=True` is passed.

`component_id` must be supplied alongside each `config_id` (format:
`component_id/config_id`, e.g. `keboola.ex-daktela/01kpx89yzw3bvv2750yxxhzzw5`).

### Data app import (stretch goal — Phase 4)

```
GET /v2/storage/components/keboola.data-apps/configs/{id}
    → {configuration: {parameters: {dataApp: {script: <streamlit source>}}}}
```

The Streamlit source is stored as a reference file. Full local-to-ECharts conversion
is out of scope; the import stores the script so the user can read it and manually
rebuild a local `create_data_app`.

### Output model

```python
class TableImportResult(BaseModel):
    table_id: str           # Keboola table ID
    local_name: str         # local CSV stem
    status: Literal['downloaded', 'skipped', 'error']
    message: str | None

class ConfigImportResult(BaseModel):
    config_id: str          # Keboola config ID
    component_id: str
    local_id: str           # local config ID written to disk
    status: Literal['saved', 'skipped', 'error']
    message: str | None

class ImportResult(BaseModel):
    tables: list[TableImportResult]
    configs: list[ConfigImportResult]
    tables_ok: int
    tables_error: int
    configs_ok: int
    configs_error: int
```

## Files to Create / Modify

| File | Change |
|------|--------|
| `src/keboola_mcp_server/local_backend/migrate.py` | Add `import_from_keboola()`, `ImportResult`, `TableImportResult`, `ConfigImportResult` |
| `src/keboola_mcp_server/local_backend/tools.py` | Register `import_from_keboola` MCP tool; add `import_from_keboola_local()` |
| `docs/local-backend.md` | Add `import_from_keboola` to local-only tool table; add Phase 4 item |
| `tests/local_backend/test_migrate.py` | New import tests (mock httpx for export/poll/download) |
| `docs/rfc/RFC-004-import-from-keboola.md` | This document |

## `_ensure_bucket` 400 bug (prerequisite fix — already shipped)

Some Keboola stacks return HTTP 400 (not 422) when creating a bucket that already
exists. `migrate.py:_ensure_bucket` previously only accepted 422 as "already exists",
causing every second `migrate_to_keboola` call to the same bucket to fail. Fixed by
checking the response body for "already exist" when the status is 400.

## Verification

1. `import_from_keboola(table_ids=["in.c-mcp-migration.contacts"])` →
   `contacts.csv` written locally, `contacts.schema.json` created
2. `get_tables` → imported table appears with correct `column_types`
3. `query_data("SELECT * FROM contacts LIMIT 5")` → no type errors
4. `import_from_keboola(config_ids=["keboola.ex-daktela/01kpx89yzw3bvv2750yxxhzzw5"])` →
   config JSON saved under `configs/`
5. Re-running same import → status `skipped` (no overwrite)
6. `import_from_keboola()` with no table/config selectors → raises `ValueError`
