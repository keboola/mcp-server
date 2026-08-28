# RFC: DuckDB transformation support

Linear: [AI-3112](https://linear.app/keboola/issue/AI-3112/kai-supporting-creation-of-duckdb-transformations)

## Problem

Keboola added `keboola.duckdb-transformation` as a third transformation backend alongside
Snowflake/BigQuery SQL transformations and Python/R transformations. The MCP server has no
awareness of it:

- `create_sql_transformation` / `update_sql_transformation` are hard-anchored to whichever
  backend the project's **workspace** uses. The component ID is derived via
  `WorkspaceManager.get_sql_dialect()` → `get_sql_transformation_id_from_sql_dialect()`
  (`src/keboola_mcp_server/tools/components/utils.py:281-299`), which only maps `'snowflake'`
  and `'bigquery'` and raises `ValueError` otherwise. Workspaces themselves
  (`src/keboola_mcp_server/workspace.py:714-813`) only support those two backends. DuckDB has
  no cloud workspace and was never going to plug into this path.
- `create_config`'s docstring (`tools.py:1121-1138`) tells the caller to use `create_sql_transformation`
  for Snowflake/BigQuery and says nothing about Python, R, or DuckDB — callers currently infer
  by precedent that non-SQL transformation types go through `create_config`.
- `update_sql_transformation_internal`'s 404 error message (`tools.py:996-1000`) already hints
  "if this is a Python or R transformation, use `update_config`..." — DuckDB hits the same 404
  and needs the same hint, or the guidance is incomplete/misleading.
- `FOLDER_SUPPORTING_COMPONENT_IDS` (`utils.py:74`) only contains the Python/R component IDs, so
  `update_config`'s folder metadata handling (`tools.py:1551-1555`) silently skips folder
  management for DuckDB configs even though the UI supports organizing DuckDB transformations
  into folders (per the DuckDB transformation docs).

Visible symptom: asking Kai to create or update a DuckDB transformation either fails outright
(if routed through the SQL-transformation tools, which will raise on non-snowflake/bigquery
workspaces) or succeeds via `create_config` by accident, without folder support and without any
tool guidance steering the model there in the first place.

## Required Behavior

| Scenario | Required behavior |
| --- | --- |
| User asks to create/update a `keboola.duckdb-transformation` config | Routed through `create_config` / `update_config`, same as Python/R today — **not** through `create_sql_transformation`/`update_sql_transformation`. |
| User asks for a transformation but doesn't specify backend, and the workspace default isn't what's implied | Kai should recognize DuckDB and Python as the two non-workspace-backed alternatives and ask the user which one, rather than silently guessing. (Prompt-level guidance change, not new code — see Scope.) |
| DuckDB config created/updated via `update_config` | Folder metadata (`folder` param) is applied, same as Python/R — i.e. `FOLDER_SUPPORTING_COMPONENT_IDS` includes the DuckDB component ID. |
| Caller mistakenly calls `update_sql_transformation` on a DuckDB config | 404 error message mentions DuckDB as a valid alternative alongside Python/R, so the caller self-corrects to `update_config`. |
| DuckDB-specific sync actions (`syntax_check`, `lineage_visualization`, `execution_plan_visualization`, `expected_input_tables`) | No new code — `run_sync_action` (`tools.py:1960-2030`) already dispatches generically to whatever sync action a component declares; this works automatically once the DuckDB component's API metadata lists them. |
| DuckDB-specific config parameters (`backend_size`, `timeout`, `duckdb_version`, `use_parquet`, `infer_input_table_data_types`, etc.) | No new code — `create_config`/`update_config` already treat `parameters` as an opaque JSON blob validated against the component's `configuration_schema` fetched via `get_components`/`get_config_examples`. |

## Resolution Strategy

Minimal, additive change mirroring the existing Python/R pattern — no new tools, no workspace
changes:

1. **`utils.py:68`** — add `DUCKDB_TRANSFORMATION_ID = 'keboola.duckdb-transformation'` next to
   `PYTHON_TRANSFORMATION_ID` / `R_TRANSFORMATION_ID`.
2. **`utils.py:74`** — add `DUCKDB_TRANSFORMATION_ID` to `FOLDER_SUPPORTING_COMPONENT_IDS`.
3. **`tools.py:1121-1138` (`create_config` docstring)** — extend the "Not for SQL transformations"
   line and "WHEN NOT TO USE" bullet to name Python/R/DuckDB explicitly as transformation types
   that *do* go through `create_config`, so the model doesn't have to infer this from precedent.
4. **`tools.py:996-1000` (`update_sql_transformation_internal` `ToolError`)** — extend the message
   to say "...Python, R, or DuckDB transformation, use `update_config` with component_id
   `keboola.python-transformation-v2`, `keboola.r-transformation-v2`, or
   `keboola.duckdb-transformation`...".
5. **`create_sql_transformation`/`update_sql_transformation` docstrings (`tools.py:440-473`,
   `563-...`)** — add one line clarifying these tools only ever produce Snowflake/BigQuery
   transformations (component ID derived from the workspace backend) and that DuckDB is handled
   via `create_config`/`update_config`.

No changes to `workspace.py`, `sql_utils.py`, or the sync-action dispatch — those are already
either backend-specific by design (workspace = Snowflake/BigQuery only, intentionally out of
scope for DuckDB) or already generic enough to cover DuckDB for free.

## Scope

In scope:

- The five code changes above (constants, docstrings, error message).
- Unit tests: `DUCKDB_TRANSFORMATION_ID` present in `FOLDER_SUPPORTING_COMPONENT_IDS`; folder
  metadata applied on `update_config` for a DuckDB component ID (extend the existing
  parametrized Python/R folder test with a DuckDB case rather than adding a new test function);
  updated error-message assertion in the `update_sql_transformation` 404 test.
- Version bump (minor — new tool-facing guidance/behavior) + `uv lock` + `TOOLS.md` regen.

Out of scope (tracked separately, each with its own RFC per CONTRIBUTING.md):

- Consolidating `create_sql_transformation`/`create_config`/`update_config` into fewer, more
  generic tools, and moving the guidance currently duplicated across tool docstrings into
  `project_system_prompt.md` (or a future Agent Skill). Tracked in a follow-up Linear issue,
  explicitly scoped to build on top of this change.
- Any DuckDB-specific workspace/backend support for `query_data` — DuckDB is not a workspace
  backend and this RFC does not add one.
- New dedicated sync-action tools — `run_sync_action` already covers this generically.

## Testing / Verification

1. `tox` — pytest, black, isort, flake8, check-tools-docs all exit 0.
2. Unit tests in `tests/tools/components/test_utils.py` / `test_tools.py`: extend existing
   parametrized folder-metadata and error-message tests with a DuckDB axis, rather than adding
   new test functions.
3. Manual E2E via local `.mcp.json` against a project with a DuckDB transformation: confirm
   `create_config`/`update_config` with `component_id='keboola.duckdb-transformation'` succeeds,
   folder metadata is applied, and `run_sync_action` executes `syntax_check` successfully.
4. `tox -e check-tools-docs` to regenerate `TOOLS.md` after docstring changes.
