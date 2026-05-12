# RFC: Variables support for configurations and transformations

Linear: [AI-1166](https://linear.app/keboola/issue/AI-1166/add-support-for-variables-to-sql-transformation-tooling)

## Problem

MCP has no way to manage Keboola variables — typed placeholders defined on any
configuration and resolved at run time. Users must set them up manually in the UI
even when everything else (transformations, flows, configs) is managed via MCP.

## Keboola Variables API Contract

Variables use the dedicated `keboola.variables` component. Each configuration that
needs variables gets its own variables config, linked back to the parent via a
`variables_id` field on the parent's configuration object.

### Variable definitions (root config)

```
POST /branch/{id}/components/keboola.variables/configs
{
  "name": "Variables definition for {component_id}/{config_id}",
  "changeDescription": "Create variable \"test\"",
  "configuration": "{\"variables\":[{\"name\":\"test\",\"type\":\"string\"}]}"
}
```

- The `name` convention embeds the owning component ID and config ID so the variables
  config can be found later without storing a foreign-key reference.
- `type` is typically `"string"`; `"vault"` is used for secrets.
- Updating definitions re-uses PUT on the same config:
  ```
  PUT .../keboola.variables/configs/{vars_config_id}
  {"configuration": "{\"variables\":[...]}", "changeDescription": "..."}
  ```
- Clearing all definitions: PUT with `{"variables": []}`.

### Variable values (rows)

Each variables config can have multiple named value sets, each stored as a row:

```
POST .../keboola.variables/configs/{vars_config_id}/rows
{
  "name": "Default Values",
  "configuration": "{\"values\":[{\"name\":\"test\",\"value\":\"abc\"}]}"
}
```

### Parent config link

The parent configuration JSON must carry `variables_id` pointing to the variables
config ID. This applies equally to SQL transformations and generic configs:

```json
{
  "parameters": { ... },
  "storage":    { ... },
  "variables_id": "{vars_config_id}"
}
```

## Design: variables as an argument on existing tools

No new MCP tools are introduced. A `variables` parameter is added to the four tools
that create or update configurations:

| Tool | New parameter | Semantics |
|---|---|---|
| `create_sql_transformation` | `variables: Optional[list[VariableDefinition]] = None` | Attach variables at creation; `None`/`[]` = skip |
| `update_sql_transformation` | `variables: Optional[list[VariableDefinition]] = None` | `None` = leave unchanged; `[]` = remove all; list = replace all |
| `update_sql_transformation` | `delete: bool = False` | `True` = permanently delete transformation + any linked vars config |
| `create_config` | `variables: Optional[list[VariableDefinition]] = None` | Same as create_sql_transformation |
| `update_config` | `variables: Optional[list[VariableDefinition]] = None` | Same as update_sql_transformation |
| `update_config` | `delete: bool = False` | `True` = permanently delete configuration + any linked vars config |

### `VariableDefinition` model

```python
class VariableDefinition(BaseModel):
    name: str                                      # variable name
    type: Literal["string", "vault"] = "string"   # constrained to valid API values
    default_value: Optional[str] = None            # if provided, creates a Default Values row
```

### `_apply_vars_to_parent_cfg` utility

All management logic is centralised in `_apply_vars_to_parent_cfg(client, component_id, config_id, variables, parent_cfg)` in `utils.py`.  It **mutates `parent_cfg` in-place** and returns `(changed, vars_config_id_to_delete)`.  The caller writes `parent_cfg` to Storage and — only after a successful write — deletes the vars config if `vars_config_id_to_delete` is set.

Existing vars config resolution: the function first checks `parent_cfg['variables_id']` and fetches that config directly; it falls back to a name-based scan only when that field is absent or the ID is stale.

**Set (non-empty list):**
1. Resolve existing vars config (by `variables_id`, then by name).
2. Create (POST) or update (PUT) it with the new definitions.
3. If any `default_value` is present → create/update a `"Default Values"` row; otherwise clear it.
4. Set `variables_id` (and `variables_values_id` if a Default Values row exists) on `parent_cfg`.
5. Return `(True, None)`.

**Clear (empty list):**
1. Resolve existing vars config.
2. Remove `variables_id` and `variables_values_id` from `parent_cfg`.
3. Return `(changed, existing_vars_config_id_or_None)`.
4. **After the caller successfully updates the parent config**, it deletes the vars config.
   (Deletion happens after the parent write to avoid a broken `variables_id` reference
   if the parent update fails.)
5. If no vars config was found → returns `(False, None)` — no-op (idempotent).

The public `apply_configuration_variables(client, component_id, config_id, variables)` function
wraps the above: it fetches the current parent from Storage, calls `_apply_vars_to_parent_cfg`,
updates the parent, and then performs the deferred vars-config deletion.  It is used by the
**create** tools (which don't have an existing config dict in memory).  The **update** tools call
`_apply_vars_to_parent_cfg` directly to fold the vars link into a single parent PUT.

### Error handling

Errors from `apply_configuration_variables` propagate through `@tool_errors()` and
surface to the caller — a failure to set variables should fail the whole operation
because the user explicitly requested it (unlike folder metadata, which is cosmetic).

## Scope

**In scope:**
- `VariableDefinition` Pydantic model in `model.py`
- `VARIABLES_COMPONENT_ID` constant, `_apply_vars_to_parent_cfg()`, and
  `apply_configuration_variables()` in `utils.py`
- `variables` parameter on `create_sql_transformation`, `update_sql_transformation`,
  `create_config`, `update_config`
- `delete` parameter on `update_sql_transformation` and `update_config`
- `variables_values_id` set on the parent config pointing to the "Default Values" row
- Unit tests: create with vars, update/set, update/clear, no-op (None), default values row,
  delete with and without linked vars config
- Version bump `1.60.0` → `1.62.0` (new feature → minor; 1.61.0 was taken by an earlier patch)
- `TOOLS.md` regeneration

**Out of scope:**
- Dedicated get/list variables tool — use `get_configs('keboola.variables')` for inspection
- Variables on row-based components (`add_config_row` / `update_config_row`)

## Verification

1. `tox` — pytest, black, flake8, check-tools-docs all exit 0.
2. Local MCP end-to-end via `.mcp.json` (see `feature_spec/variables/test_scenarios.txt`):
   - `create_sql_transformation(..., variables=[{name: "env", type: "string", default_value: "prod"}])`
     → `keboola.variables` config created in Storage; transformation config carries `variables_id`
     and `variables_values_id`.
   - `update_sql_transformation(..., variables=[])` → vars config **deleted** from Storage;
     `variables_id` removed from parent.
   - `update_sql_transformation(..., variables=None)` → existing variables untouched.
   - `update_sql_transformation(..., delete=True)` → linked vars config deleted first, then
     transformation deleted; result carries real `version`/`description` from pre-deletion state.
