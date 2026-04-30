# RFC-003: Semantic Layer for CSV Type Inference

**Status:** Implemented (v1.62.0)
**Branch:** AI-2922-local-backend-phase3

## Problem

`_duckdb_connection()` runs `read_csv_auto()` for every CSV table on every query.
DuckDB's auto-detection is heuristic and version-sensitive; the detected type for a
given column can differ between DuckDB versions or depending on how many rows contain
a distinguishable value. The MCP client (AI) has no schema metadata to write correct
SQL until a query fails at runtime with a `ConversionException`.

Two concrete failures observed in production use:

```
ConversionException: Could not convert string 'true' to INT64
LINE:  SUM(CASE WHEN sla_overdue = 'true' THEN 1 ELSE 0 END)
                                    ^

ConversionException: invalid timestamp field format: "", expected YYYY-MM-DD HH:MM:SS
LINE:  SUM(CASE WHEN closed IS NOT NULL AND closed != '' THEN 1 ELSE 0 END)
                                                         ^
```

In both cases the AI generated valid generic SQL but was unaware of the actual DuckDB
types DuckDB had inferred (`BOOLEAN`, `TIMESTAMP WITH TIME ZONE`) for those columns.

## Decision

### Sidecar schema files

One `<name>.schema.json` per CSV in `tables/`:

```json
{
  "version": 1,
  "columns": [
    {"name": "sla_overdue", "duckdb_type": "BOOLEAN"},
    {"name": "created",     "duckdb_type": "TIMESTAMP WITH TIME ZONE"},
    {"name": "id",          "duckdb_type": "VARCHAR"}
  ]
}
```

The format is deliberately simple so external tools (dbt semantic layer, Keboola AI
semantic tools) can write sidecar files directly. The MCP server reads whatever is
present; it does not require the file to have been produced by itself.

### Where schemas come from

| Source | When |
|--------|------|
| `write_csv_table()` | After every CSV write, auto-detect via DuckDB `DESCRIBE` |
| `_duckdb_connection()` on first load | Auto-detect when no sidecar exists yet (e.g. externally-produced CSVs from `run_component`) |
| External tooling | Any tool can write a sidecar directly in the agreed format |

### How schemas are consumed

**`_duckdb_connection()`** — when a sidecar exists, use `read_csv(?, columns={...})`
with explicit types instead of `read_csv_auto`:

```sql
CREATE OR REPLACE TABLE "tickets" AS
SELECT * FROM read_csv(?, columns={
  'sla_overdue': 'BOOLEAN',
  'created': 'TIMESTAMP WITH TIME ZONE',
  'id': 'VARCHAR',
  ...
})
```

This ensures consistent type binding across every query connection and eliminates the
"stale sidecar" edge case at query time. If the explicit schema fails (columns changed
after the sidecar was written), `_duckdb_connection()` falls back to `read_csv_auto`
and refreshes the sidecar automatically.

**`get_tables`** — returns `column_types: [{name, duckdb_type}]` for every table that
has a sidecar. The AI client reads types before writing SQL.

**`write_table` return value** — includes `column_types` so the AI immediately knows
the detected types after writing a new table.

**`_LOCAL_PROJECT_INSTRUCTION`** — updated to advise checking `column_types` before
writing SQL and explains the BOOLEAN / TIMESTAMP patterns.

**`delete_table`** — removes the sidecar alongside the CSV.

## Files Changed

- `src/keboola_mcp_server/local_backend/backend.py`
  - `_schema_path()` — compute sidecar path from CSV path
  - `_load_schema()` — read sidecar or return None
  - `_save_schema_from_describe()` — persist schema from an open DuckDB connection
  - `detect_and_save_schema()` — public method; opens its own DuckDB connection for write-time detection; callable by external code after producing new CSVs
  - `_duckdb_connection()` — use explicit columns when sidecar available; generate sidecar on first load
  - `write_csv_table()` — call `detect_and_save_schema()` after writing
  - `delete_csv_table()` — remove sidecar alongside CSV
- `src/keboola_mcp_server/local_backend/tools.py`
  - `LocalColumnInfo` — new model `{name, duckdb_type}`
  - `LocalTableInfo` — new optional `column_types: list[LocalColumnInfo]`
  - `get_tables_local()` — populate `column_types` from sidecar
  - `write_table_local()` — populate `column_types` in returned info
  - `_LOCAL_PROJECT_INSTRUCTION` — SQL type guidance
- `tests/local_backend/test_backend.py` — schema creation, loading, explicit types, stale fallback, delete cleanup
- `tests/local_backend/test_tools.py` — `column_types` in `get_tables` and `write_table`
