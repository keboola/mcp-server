# RFC: Shared Code Support

Linear: [AI-1167](https://linear.app/keboola/issue/AI-1167/add-support-for-shared-codes-to-sql-transformation-tooling)

> **Status:** shipped. Amended 2026-05-14 to reflect post-implementation deltas
> — the original spec captured the high-level shape correctly but missed several
> wire-format and runtime-substitution details that only surfaced during live
> testing on real Snowflake / BigQuery projects. Sections marked **[amended]**
> were rewritten after the corresponding commit; see *Implementation Deltas* at
> the bottom for the change log keyed to commit SHAs.

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

### Keboola Shared Code API [amended]

Reference: https://developers.keboola.com/integrate/variables/#shared-code

Shared code is stored under the `keboola.shared-code` component. There is one parent
**configuration** per transformation type. The platform expects a **flat configuration
body** for shared-code — `componentId` sits at the configuration root, *not* under a
`"parameters"` key:

```json
{ "componentId": "keboola.snowflake-transformation" }
```

The conventional config ID is **required, not optional** — the UI and the runtime resolver
look up shared-code libraries by exact ID:

| Transformation type | Required shared-code config ID |
|---|---|
| `keboola.snowflake-transformation` | `shared-codes.snowflake-transformation` |
| `keboola.google-bigquery-transformation` | `shared-codes.google-bigquery-transformation` |
| `keboola.python-transformation-v2` | `shared-codes.python-transformation-v2` |
| `keboola.r-transformation-v2` | `shared-codes.r-transformation-v2` |

SAPI-auto-assigned UUIDs work as IDs but are invisible to the UI / runtime, so the tool
forwards `configurationId` explicitly and the system prompt instructs the LLM to pass the
conventional value.

Each **row** of the config is one reusable snippet (also flat-bodied):

- `rowId` — set explicitly at row-creation time; becomes the Mustache key (e.g. `dumpfiles`)
- `configuration.code_content` — array of code strings at the row configuration root,
  e.g. `{"code_content": ["SELECT 1"]}` (not wrapped under `"parameters"`)

Fetching a parent library's rows requires `?include=rows` on the Storage API detail call;
without it the rows array is omitted.

### Using Shared Code in a Transformation [amended]

A transformation references shared code via three coupled pieces:

1. **Configuration root** — two top-level fields alongside `parameters` and `storage`:
   `shared_code_id` (parent library ID) and `shared_code_row_ids` (list of row IDs).
2. **One marker code block per referenced row** — a dedicated code with name
   `Shared Code (<shared_code_id>-<row_id>)` and script `["{{ rowId }}"]`. The runtime
   substitutes the placeholder *only* when it is the sole array element of a script;
   inline `{{ rowId }}` inside a longer SQL string is **not** substituted.
3. **The user-authored code blocks** — these read the side-effects of the expanded
   snippet (e.g. a session variable set, a temp table created, a UDF defined).

```json
{
  "parameters": {
    "blocks": [
      {
        "name": "Blocks",
        "codes": [
          { "name": "Shared Code (shared-codes.snowflake-transformation-dumpfiles)",
            "script": ["{{ dumpfiles }}"] },
          { "name": "User code", "script": ["SELECT * FROM dumped_table"] }
        ]
      }
    ]
  },
  "storage": {},
  "shared_code_id": "shared-codes.snowflake-transformation",
  "shared_code_row_ids": ["dumpfiles"]
}
```

The LLM/agent never authors the marker blocks by hand — `create_sql_transformation`,
`update_sql_transformation`, `create_config`, and `update_config` emit them automatically
whenever the target component is a transformation backend AND linkage fields are set.
See *Marker Handling* in §6.

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
  `configuration_row_create` (`line 664`) — sufficient for all CRUD calls, but the
  initial implementations were missing form-field forwarding **[amended]**:
  - `configuration_create` did not forward `configurationId` — the SAPI auto-assigned
    UUIDs are unusable as shared-code library IDs (added in 67b893c7)
  - `configuration_row_create` did not forward `rowId` — the row ID is the Mustache
    key, must be settable explicitly (added in 9af709fc)
  - `configuration_detail` did not accept `include` — without `?include=rows` the
    Storage API omits row data and `get_shared_codes` returns empty rows (added in
    30faec45)

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

### 3. Shared Code CRUD — Reuse Existing Generic Tools (with extensions) [amended]

The existing `create_config`, `add_config_row`, and `update_config_row` tools cover
shared-code CRUD without introducing new write tools, but they needed targeted
extensions to handle the platform's flat-body wire format and explicit IDs:

| Operation | Tool | Key parameters |
|---|---|---|
| Create parent library | `create_config` | `component_id="keboola.shared-code"`, **`configuration_id="shared-codes.<tf-id>"`** (required for shared-code), `parameters={"componentId":"<tf-component-id>"}` |
| Add snippet row | `add_config_row` | `component_id="keboola.shared-code"`, `row_id="<mustache-key>"`, `parameters={"code_content":["<code>"]}` |
| Update snippet | `update_config_row` | `parameter_updates=[{"op":"set","path":"code_content","value":["<new code>"]}]` |
| Disable snippet | `update_config_row` | `is_disabled=True` (no `delete_config_row` tool exists) |

**Required extensions:**

1. **`add_config_row.row_id`** (9af709fc) — optional string parameter. When non-empty,
   forwarded to SAPI as `rowId`. Required so the LLM can set Mustache keys at row creation.
   `storage.py:configuration_row_create` extended to forward the field.

2. **`create_config.configuration_id`** (67b893c7) — optional string parameter, REQUIRED
   for `keboola.shared-code` parent libraries (caller passes the conventional
   `shared-codes.<tf-id>` value). Forwarded to SAPI as `configurationId`.
   `storage.py:configuration_create` extended to forward the field.

3. **Flat-body special case** (67b893c7) — `create_config` and `add_config_row`
   normally wrap the caller's `parameters` dict under a `"parameters"` key in the
   configuration body. For `component_id="keboola.shared-code"` they now write the
   caller's dict at the **configuration root** instead, so `componentId` /
   `code_content` land where the platform's resolver looks for them.

4. **`get_shared_codes` reads from root with parameters fallback** (67b893c7) — to
   keep legacy configs (created by the wrapped wire format) discoverable, the tool
   reads `componentId` / `code_content` from the configuration root first and falls
   back to `parameters.<field>` if absent.

5. **`configuration_detail.include`** (30faec45) — optional comma-separated CSV
   forwarded as `?include=...`. `get_shared_codes` and the bulk `get_configs` path
   pass `include=['rows']` so rows are populated.

6. **`ConfigToolOutput.configuration_row_id`** (91c112d9) — `add_config_row` surfaces
   the row ID assigned by SAPI and logs a warning when the assigned ID does not match
   the requested `row_id` (would indicate SAPI rejected or transformed the value).

### 4. Linking Shared Code to Transformation Configurations [amended]

`shared_code_id` and `shared_code_row_ids` live at the **configuration root**, not under
`parameters`. None of the existing tools could write these fields. In addition to writing
the linkage, every create/update path now also **auto-emits marker code blocks** (see §6).

#### 4a. `create_sql_transformation` (`tools.py:382`)

Add two optional parameters:

```python
shared_code_id: str = ""
shared_code_row_ids: Sequence[str] = tuple()
```

When non-empty: (1) set them directly on `configuration_payload` alongside `parameters`
and `storage`; (2) auto-emit a `Shared Code (<sid>-<rid>)` marker code block per row in
the first parameters block via `apply_shared_code_markers` (91c112d9).

#### 4b. `update_sql_transformation` (`tools.py:537`)

Add two new operation types to the `TfParamUpdate` discriminated union:

- **`set_shared_code`** — sets `shared_code_id` and `shared_code_row_ids` on the
  configuration root; replaces any existing values; **re-syncs marker blocks** to match
  the new linkage (adds missing markers, removes orphaned ones)
- **`remove_shared_code`** — removes both fields from the configuration root AND deletes
  any `Shared Code (...)` marker blocks from `parameters.blocks`

Marker synchronization is also performed after every other `parameter_updates`
application via `sync_shared_code_markers_in_dict`, so even unrelated block edits
maintain the marker invariant.

#### 4c. `create_config` (`tools.py:1005`)

For Python, R, and DuckDB transformations (and any other component) created via the
generic tool, add:

```python
shared_code_id: str = ""
shared_code_row_ids: Sequence[str] = tuple()
```

Include them in `configuration_payload` when non-empty. When the target `component_id`
is a transformation backend that supports shared code, also auto-emit marker code blocks
via `sync_shared_code_markers_in_dict` (aa8fc636) — same UI-canonical behavior as the
SQL path, so Python/R/DuckDB don't need a separate authoring pattern.

#### 4d. `update_config` (`tools.py:1291`)

Add the same two optional parameters. Write them to the configuration root (not to
`parameters`) during the update; also auto-emit marker blocks for transformation
components (aa8fc636). This handles adding, changing, or clearing shared code linkage on
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

### 6. Marker Handling and Linkage Validation [added post-implementation]

The platform's runtime expands `{{ rowId }}` **only** when it is the entire content of a
script array element. Three behaviors implement this contract:

#### 6a. Auto-emit canonical marker blocks (91c112d9, aa8fc636)

Whenever a create/update tool sees `shared_code_id` + non-empty `shared_code_row_ids` for
a transformation component, it appends one code block per row:

```json
{ "name": "Shared Code (<shared_code_id>-<row_id>)",
  "script": ["{{ row_id }}"] }
```

This is the same shape the Keboola UI produces, so configurations created via the MCP are
indistinguishable from UI-authored ones. The LLM never has to author these markers — it
just supplies the linkage fields.

Implementations:
- `apply_shared_code_markers(tf_cfg, sid, row_ids)` — Pydantic-model path (create)
- `sync_shared_code_markers_in_dict(updated_configuration)` — raw-dict path (update,
  also used by `create_config` / `update_config` for Python/R/DuckDB)
- `build_shared_code_marker_codes(sid, row_ids)`, `shared_code_marker_code_name(sid, rid)`,
  `is_shared_code_marker(name, script)` — shared helpers

#### 6b. Skip auto-emit when the user already references the row (31cee8b5)

If the user already authored a NON-marker code block whose script is exactly
`["{{ rowId }}"]` (a pure-placeholder script element), the tool **skips** emitting an
extra `Shared Code (...)` marker for that row — emitting one would execute the snippet
twice at run time.

Inline placeholders (e.g. `["SELECT 1; {{ rowId }}"]`) are deliberately ignored by this
skip rule because the platform does not substitute those — the marker is still required
for the placeholder to resolve at all.

`user_substitution_eligible_row_ids(blocks)` returns the set of row IDs that should be
skipped, and is consulted from both `apply_shared_code_markers` and
`sync_shared_code_markers_in_dict`.

#### 6c. Hard-reject placeholder without linkage (aa8fc636)

`validate_shared_code_linkage(parameters, shared_code_id, shared_code_row_ids)` scans
`parameters.blocks[*].codes[*].script` for `{{ rowId }}` placeholders and raises
`ValueError` when:
- a placeholder is referenced but `shared_code_id` is empty, or
- a referenced row is not in `shared_code_row_ids`.

Wired into `create_sql_transformation`, `update_sql_transformation_internal`,
`create_config`, and `update_config` for transformation components. This prevents the
silent-failure mode where a placeholder is left dangling and the snippet is never
substituted at run time.

#### 6d. Known caveat — marker ordering for dependent code

The auto-emit appends markers at the **end** of the first parameters block. If the
user's code DEPENDS on the side-effect of the shared snippet (e.g. shared code does
`SET (region) = ('EU')` and user code reads `$region`, or shared code creates a temp
table that user code selects from), the marker must run **before** the dependent code.

The tool does not reorder automatically — the caller must move the marker via
`update_sql_transformation` with `remove_code` + `add_code(position="start")`, or author
a pure-placeholder code block at the start themselves (which §6b then de-dups).

Tracked as a follow-up: either default the auto-emit position to `start`, or accept an
`auto_emit_position` parameter, or document the workaround prominently in the system
prompt.

## Workflows

### Discovery (before writing transformation code)

```
get_shared_codes(transformation_component_ids=["keboola.snowflake-transformation"])
→ returns existing snippets with their row_ids and code
→ use {{ rowId }} in transformation scripts where appropriate
```

### Creating a New Shared Code Snippet [amended]

```
1. Check if a parent config already exists via get_shared_codes
2. If not:
   create_config(
     component_id="keboola.shared-code",
     configuration_id="shared-codes.snowflake-transformation",   # REQUIRED — conventional ID
     name="Shared Codes for Snowflake Transformations",
     parameters={"componentId": "keboola.snowflake-transformation"}
       # platform stores `componentId` at the config root; the tool unwraps for you
   )
   → captures returned config_id (matches the conventional ID you passed in)

3. add_config_row(
     component_id="keboola.shared-code",
     configuration_id="shared-codes.snowflake-transformation",
     name="Dump files helper",
     row_id="dumpfiles",
     parameters={"code_content": ["SELECT ..."]}
       # the row body is also flat — `code_content` lands at the row config root
   )
   → snippet is now available as {{ dumpfiles }}
```

### Referencing Shared Code in a New Transformation [amended]

```
create_sql_transformation(
  name="My Transformation",
  sql_code_blocks=[
    Code(name="User code", script="SELECT * FROM dumped_table")
  ],
  shared_code_id="shared-codes.snowflake-transformation",
  shared_code_row_ids=["dumpfiles"]
)
# The tool auto-emits the marker block:
#   { name: "Shared Code (shared-codes.snowflake-transformation-dumpfiles)",
#     script: ["{{ dumpfiles }}"] }
# appended after the user-authored code. Reorder if the user code depends on
# side-effects of the shared snippet (see §6d).
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

## Testing [amended]

Original RFC coverage retained; added during implementation:

- **Wire format**: `test_create_config_flat_body_for_shared_code`,
  `test_add_config_row_flat_body_for_shared_code`,
  `test_get_shared_codes_reads_root_with_parameters_fallback` (67b893c7).
- **`include=rows`**: mock-signature updates pinning that `configuration_detail` is
  called with `include=['rows']` from `get_shared_codes` and the bulk `get_configs`
  detail path (30faec45).
- **Marker emission**: `test_create_sql_transformation_emits_shared_code_marker_blocks`
  (parametrized for snowflake / bigquery),
  `test_update_sql_transformation_set_then_remove_shared_code_syncs_markers`,
  `test_create_config_emits_markers_for_python_transformation` (91c112d9, aa8fc636).
- **Linkage validation**:
  `test_create_sql_transformation_rejects_placeholder_without_linkage`,
  `test_create_sql_transformation_rejects_placeholder_missing_from_row_ids` (aa8fc636).
- **Marker de-dup**: 151 lines covering pure-placeholder detection, marker
  de-duplication on create/update, inline-placeholder ignore (31cee8b5).
- **Row ID surfacing**: `test_add_config_row_surfaces_assigned_row_id` (91c112d9).
- **Integration tests**: 7 live-stack tests with a `shared_code_parent_factory`
  fixture for cleanup — covered in PR #499.
- Run `tox` (pytest + black + flake8 + check-tools-docs) before pushing.

## Implementation Deltas (post-RFC)

| Commit | Delta |
|---|---|
| 9af709fc | Initial implementation matching the original RFC. |
| 67b893c7 | Wire format: shared-code body is FLAT (`componentId` / `code_content` at config root, not under `parameters`). `create_config` accepts `configuration_id` (REQUIRED for shared-code parent libraries — auto-UUIDs not recognised by UI/runtime). `get_shared_codes` reads from root with parameters fallback. |
| 30faec45 | `configuration_detail.include` parameter added; `get_shared_codes` requests `include=['rows']` (Storage API omits rows by default). |
| 91c112d9 | Runtime substitution operates on a script ARRAY ELEMENT, not text inside a string. Tools auto-emit UI-canonical `Shared Code (<sid>-<rid>)` marker code blocks per linked row. `get_configs` bulk path passes `include=['rows']`. `add_config_row` surfaces the SAPI-assigned `configuration_row_id`. |
| aa8fc636 | Hard-reject `{{ rowId }}` placeholders without matching root linkage (`validate_shared_code_linkage`). Marker auto-emission extended to `create_config` / `update_config` for Python/R/DuckDB transformation components, mirroring the SQL path. System prompt rewritten and shortened from 103 → 32 lines. |
| 31cee8b5 | Skip auto-emit when the user already has a NON-marker code block with `["{{ rowId }}"]` as its pure script element — emitting another would execute the snippet twice. Inline placeholders inside other SQL strings are ignored. |

## Out of Scope

- Automatic detection of duplicated code across transformations — the LLM decides based on
  user intent, no heuristics
- Variable support (`variables_id`, `variables_values_id`) — separate feature
- Shared code for non-transformation components
- Auto-emit marker position (currently appended after user code; caller must reorder if
  user code depends on side-effects of the snippet) — see §6d
