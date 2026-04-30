# RFC: Shared Code Support

Linear: [AI-1167](https://linear.app/keboola/issue/AI-1167/add-support-for-shared-codes-to-sql-transformation-tooling)

## Problem

The MCP server has no awareness of Keboola's shared code feature. As a result:

- The LLM cannot discover what reusable snippets already exist in a project before writing
  transformation code.
- The LLM cannot create shared code entries even when the user explicitly asks for reusable
  code or when the same logic should span multiple transformations.
- Transformation tools (`create_sql_transformation`, `update_sql_transformation`,
  `create_config`) have no parameters for `shared_code_id` / `shared_code_row_ids` — the
  top-level config fields that wire a transformation to its shared code library — so there
  is no path from "write shared code" to "reference it in a transformation".
- Mustache placeholders (`{{ rowId }}`) placed in transformation scripts have no effect
  unless the linkage fields are also set on the configuration root.

This gap is actively blocking customers (e.g. Apify, see SUPPORT-15438).

## Background

### Keboola Shared Code API

Reference: https://developers.keboola.com/integrate/variables/#shared-code

Shared code is stored under the `keboola.shared-code` component. There is one parent
**configuration** per transformation type, with the config payload declaring which
transformation component the library belongs to:

```json
{ "componentId": "keboola.snowflake-transformation" }
```

Conventional config IDs (not enforced by the API, but used by default tooling):

| Transformation type | Conventional shared-code config ID |
|---|---|
| `keboola.snowflake-transformation` | `shared-codes.snowflake-transformation` |
| `keboola.google-bigquery-transformation` | `shared-codes.google-bigquery-transformation` |
| `keboola.python-transformation-v2` | `shared-codes.python-transformation-v2` |
| `keboola.r-transformation-v2` | `shared-codes.r-transformation-v2` |

Each **row** of the config is one reusable snippet:

- `rowId` — set explicitly at row-creation time; becomes the Mustache key (e.g. `dumpfiles`)
- `configuration.code_content` — array of code strings, e.g. `["SELECT 1"]`

### Using Shared Code in a Transformation

A transformation references shared code in two places:

1. **In the script** — via Mustache placeholder: `{{ dumpfiles }}`
2. **At the configuration root** — two top-level fields alongside `parameters` and
   `storage`:

```json
{
  "parameters": {
    "blocks": [
      {
        "name": "Main block",
        "codes": [
          { "name": "Reused logic", "script": ["{{ dumpfiles }}"] }
        ]
      }
    ]
  },
  "storage": {},
  "shared_code_id": "shared-codes.snowflake-transformation",
  "shared_code_row_ids": ["dumpfiles"]
}
```

At runtime, Keboola expands `{{ dumpfiles }}` to the `code_content` of that row. Multiple
rows can be referenced; each must appear in both the script placeholders and in
`shared_code_row_ids`.

### Current Codebase State

- Component constants in `src/keboola_mcp_server/tools/components/utils.py:64-67`:
  `SNOWFLAKE_TRANSFORMATION_ID`, `BIGQUERY_TRANSFORMATION_ID`,
  `PYTHON_TRANSFORMATION_ID`, `R_TRANSFORMATION_ID`
- `TransformationConfiguration` model (`model.py`) has no `shared_code_id` or
  `shared_code_row_ids` fields — these are silently dropped during
  deserialize/re-serialize cycles in `update_sql_transformation`
- `create_sql_transformation` (`tools.py:382`) and `update_sql_transformation`
  (`tools.py:537`) have no parameters for shared code linkage
- `create_config` (`tools.py:1005`) builds `configuration_payload` with only `storage`,
  `parameters`, and `processors` — no mechanism to inject root-level fields
- `update_config.parameter_updates` paths are relative to `parameters`, making it
  impossible to reach `shared_code_id` at the config root via the existing tool
- `storage.py` already exposes `configuration_create` (`line 487`) and
  `configuration_row_create` (`line 664`) — sufficient for all CRUD calls

## Proposed Changes

### 1. System Prompt (`project_system_prompt.md`)

Add a **"Shared Code"** section immediately after the existing "Transformations" section.

#### Discovery

Before writing or editing transformation code, call `get_shared_codes` filtered to the
relevant transformation component. This surfaces snippets the project already maintains.
Use existing shared code via Mustache references rather than duplicating logic inline.

#### When to Create Shared Code

Create a new shared code entry when:
- The user explicitly asks for reusable / shared code.
- The same logic needs to appear in multiple transformations.

Do **not** proactively convert every snippet to shared code — only do so when reuse intent
is clear.

#### How to Reference Shared Code

1. Write `{{ rowId }}` in the transformation script at the point where the snippet should
   be substituted.
2. When creating or updating the transformation, pass `shared_code_id` (the parent config
   ID) and `shared_code_row_ids` (list of row IDs referenced in the scripts).
3. `rowId` is case-sensitive and must match the row ID used when the snippet was created.
4. All row IDs referenced in scripts must appear in `shared_code_row_ids`; unused entries
   in the list cause a validation error.

#### Naming Convention

The parent config ID follows `shared-codes.<transformation-component-id>` by convention,
but always use the actual ID returned by `get_shared_codes` for an existing library rather
than assuming the conventional name.

### 2. New Tool: `get_shared_codes`

A dedicated read-only tool for discovering the project's shared code libraries.

**Signature**

```python
async def get_shared_codes(
    ctx: Context,
    transformation_component_ids: Sequence[str] = tuple(),
) -> GetSharedCodesOutput
```

**Parameters**

- `transformation_component_ids` — optional filter. When empty, returns all shared code
  configs. Accepted values are the transformation component IDs known to the server
  (`keboola.snowflake-transformation`, `keboola.google-bigquery-transformation`,
  `keboola.python-transformation-v2`, `keboola.r-transformation-v2`).

**Behavior**

1. Call `storage_client.configuration_list("keboola.shared-code")` to list all parent
   configs.
2. For each config, read `configuration.componentId` to know the transformation type.
3. Apply the `transformation_component_ids` filter if provided.
4. For each matching config call `storage_client.configuration_detail(...)` to get rows
   and their `code_content`.
5. Return a structured list of configs with their rows.

**New output models** (add to `model.py`):

```python
class SharedCodeRow(BaseModel):
    row_id: str       # the Mustache key (e.g. "dumpfiles")
    name: str
    code: str         # code_content array joined with "\n"

class SharedCodeConfig(BaseModel):
    config_id: str                        # e.g. "shared-codes.snowflake-transformation"
    transformation_component_id: str      # e.g. "keboola.snowflake-transformation"
    rows: list[SharedCodeRow]

class GetSharedCodesOutput(BaseModel):
    shared_codes: list[SharedCodeConfig]
```

**Annotations**: `readOnlyHint: true`

### 3. Shared Code CRUD — Reuse Existing Generic Tools

The existing `create_config`, `add_config_row`, and `update_config_row` tools are
sufficient for creating and editing shared code content. No new write tools are needed.
The system prompt (section 1) must document the exact parameters so the LLM can use them
correctly:

| Operation | Tool | Key parameters |
|---|---|---|
| Create parent config | `create_config` | `component_id="keboola.shared-code"`, `parameters={"componentId":"<tf-component-id>"}` |
| Add snippet row | `add_config_row` | `component_id="keboola.shared-code"`, `row_id="<mustache-key>"`, `parameters={"code_content":["<code>"]}` |
| Update snippet | `update_config_row` | `parameter_updates=[{"op":"set","path":"code_content","value":["<new code>"]}]` |
| Disable snippet | `update_config_row` | `is_disabled=True` (no `delete_config_row` tool exists) |

**Required change to `add_config_row`**: The Storage API's `configuration_row_create`
already accepts a `rowId` form parameter (distinct from the auto-generated numeric `id`).
The `add_config_row` tool currently has no `row_id` parameter, so the LLM cannot set the
Mustache key at creation time.

Add an optional `row_id: str = ""` parameter to `add_config_row`. When non-empty, pass it
as `rowId` to `storage_client.configuration_row_create`. Verify that
`storage.py:configuration_row_create` forwards the `rowId` field to the API; add it if
missing.

### 4. Linking Shared Code to Transformation Configurations

`shared_code_id` and `shared_code_row_ids` live at the **configuration root**, not under
`parameters`. None of the existing tools can currently write these fields.

#### 4a. `create_sql_transformation` (`tools.py:382`)

Add two optional parameters:

```python
shared_code_id: str = ""
shared_code_row_ids: Sequence[str] = tuple()
```

When non-empty, set them directly on `configuration_payload` (the dict passed to
`configuration_create`) alongside `parameters` and `storage`.

#### 4b. `update_sql_transformation` (`tools.py:537`)

Add two new operation types to the `TfParamUpdate` discriminated union:

- **`set_shared_code`** — sets `shared_code_id` and `shared_code_row_ids` on the
  configuration root; replaces any existing values
- **`remove_shared_code`** — removes both fields from the configuration root

These operations patch the raw configuration dict at the root level, outside the
`parameters.blocks` path that existing operations use.

#### 4c. `create_config` (`tools.py:1005`)

For Python, R, and DuckDB transformations (and any other component) created via the
generic tool, add:

```python
shared_code_id: str = ""
shared_code_row_ids: Sequence[str] = tuple()
```

Include them in `configuration_payload` when non-empty. These are Keboola meta-fields, not
component parameters, so no schema validation is needed.

#### 4d. `update_config` (`tools.py:1291`)

Add the same two optional parameters. Write them to the configuration root (not to
`parameters`) during the update. This handles adding or changing shared code linkage on
existing Python/R/DuckDB transformation configurations.

### 5. `TransformationConfiguration` Model (`model.py`)

Add optional fields so existing configurations with shared code linkage round-trip
correctly through `update_sql_transformation`'s deserialize/re-serialize cycle:

```python
shared_code_id: Optional[str] = None
shared_code_row_ids: list[str] = Field(default_factory=list)
```

Without this change, `update_sql_transformation` would silently drop `shared_code_id` and
`shared_code_row_ids` from any transformation that already has them set, breaking the
linkage on every update.

## Workflows

### Discovery (before writing transformation code)

```
get_shared_codes(transformation_component_ids=["keboola.snowflake-transformation"])
→ returns existing snippets with their row_ids and code
→ use {{ rowId }} in transformation scripts where appropriate
```

### Creating a New Shared Code Snippet

```
1. Check if a parent config already exists via get_shared_codes
2. If not:
   create_config(
     component_id="keboola.shared-code",
     name="Shared Codes for Snowflake Transformations",
     parameters={"componentId": "keboola.snowflake-transformation"}
   )
   → captures returned config_id

3. add_config_row(
     component_id="keboola.shared-code",
     configuration_id=<config_id>,
     name="Dump files helper",
     row_id="dumpfiles",
     parameters={"code_content": ["SELECT ..."]}
   )
   → snippet is now available as {{ dumpfiles }}
```

### Referencing Shared Code in a New Transformation

```
create_sql_transformation(
  name="My Transformation",
  sql_code_blocks=[
    Code(name="Reused logic", script="{{ dumpfiles }}")
  ],
  shared_code_id="shared-codes.snowflake-transformation",
  shared_code_row_ids=["dumpfiles"]
)
```

### Adding Shared Code Reference to an Existing Transformation

```
update_sql_transformation(
  component_id="keboola.snowflake-transformation",
  configuration_id="<id>",
  parameter_updates=[
    SetSharedCode(
      shared_code_id="shared-codes.snowflake-transformation",
      shared_code_row_ids=["dumpfiles"]
    )
  ]
)
```

## Critical Files

| File | Change |
|---|---|
| `feature_spec/shared_code_support/RFC.md` | This document |
| `src/keboola_mcp_server/resources/prompts/project_system_prompt.md` | Add "Shared Code" section after "Transformations" |
| `src/keboola_mcp_server/tools/components/model.py` | Add `shared_code_id`/`shared_code_row_ids` to `TransformationConfiguration`; add `SharedCodeRow`, `SharedCodeConfig`, `GetSharedCodesOutput` models |
| `src/keboola_mcp_server/tools/components/tools.py` | New `get_shared_codes`; extend `create_sql_transformation`, `update_sql_transformation` (new `TfParamUpdate` variants), `create_config`, `update_config`, `add_config_row` |
| `src/keboola_mcp_server/clients/storage.py` | Add `rowId` parameter to `configuration_row_create` if absent |
| `TOOLS.md` | Auto-regenerated via `tox -e check-tools-docs` |

## Testing

- **Unit tests for `get_shared_codes`**: mock `configuration_list` + `configuration_detail`
  for the `keboola.shared-code` component; assert filtering by `transformation_component_ids`
  works correctly; test empty project (no shared-code configs).
- **Unit tests for `create_sql_transformation`**: extend existing parametrized tests with
  `shared_code_id` / `shared_code_row_ids` variants; assert both fields appear at the
  config root in the API call payload.
- **Unit tests for `update_sql_transformation`**: test the new `set_shared_code` and
  `remove_shared_code` operations in isolation and combined with existing block operations.
- **Model round-trip tests**: verify `TransformationConfiguration` serializes and
  deserializes with and without shared code fields without data loss.
- **`add_config_row` unit tests**: extend with `row_id` parameter; assert `rowId` is
  forwarded to `configuration_row_create`.
- **Integration tests**: create a shared code config + row for `keboola.python-transformation-v2`,
  create a Python transformation referencing it, verify the stored config JSON contains
  `shared_code_id` and `shared_code_row_ids`, run the transformation and confirm output.
- Run `tox` (pytest + black + flake8 + check-tools-docs) before pushing.

## Out of Scope

- Automatic detection of duplicated code across transformations — the LLM decides based on
  user intent, no heuristics
- Variable support (`variables_id`, `variables_values_id`) — separate feature
- Shared code for non-transformation components
