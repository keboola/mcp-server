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
| `create_sql_transformation` | `variables: list[VariableDefinition] = []` | Attach variables at creation; empty = skip |
| `update_sql_transformation` | `variables: list[VariableDefinition] \| None = None` | `None` = leave unchanged; `[]` = remove all; list = replace all |
| `create_config` | `variables: list[VariableDefinition] = []` | Same as create_sql_transformation |
| `update_config` | `variables: list[VariableDefinition] \| None = None` | Same as update_sql_transformation |

### `VariableDefinition` model

```python
class VariableDefinition(BaseModel):
    name: str                           # variable name
    type: str = "string"                # "string" or "vault"
    default_value: Optional[str] = None # if provided, creates a Default Values row
```

### `apply_configuration_variables` utility

All management logic is centralised in `apply_configuration_variables(client, component_id, config_id, variables)` in `utils.py`. It is called after the main config write in each of the four tools.

**Set (non-empty list):**
1. List `keboola.variables` configs; find the one named
   `"Variables definition for {component_id}/{config_id}"`.
2. Create (POST) or update (PUT) it with the new definitions.
3. If any `default_value` is present → create/update a `"Default Values"` row.
4. Fetch the current parent config, add/overwrite `variables_id`, PUT back.

**Clear (empty list):**
1. Find the linked variables config by name convention.
2. If found → PUT `{"variables": []}`.
3. Fetch parent config, remove `variables_id`, PUT back.
4. If not found → no-op (idempotent).

### Error handling

Errors from `apply_configuration_variables` propagate through `@tool_errors()` and
surface to the caller — a failure to set variables should fail the whole operation
because the user explicitly requested it (unlike folder metadata, which is cosmetic).

## Scope

**In scope:**
- `VariableDefinition` Pydantic model in `model.py`
- `VARIABLES_COMPONENT_ID` constant and `apply_configuration_variables()` in `utils.py`
- `variables` parameter on `create_sql_transformation`, `update_sql_transformation`,
  `create_config`, `update_config`
- Unit tests: create with vars, update/set, update/clear, no-op (None), default values row
- Version bump `1.60.0` → `1.61.0` (new feature → minor)
- `TOOLS.md` regeneration

**Out of scope:**
- Dedicated get/list variables tool — use `get_configs('keboola.variables')` for inspection
- `variables_values_id` (pointing to a specific named values row beyond "Default Values")
- Variables on row-based components (`add_config_row` / `update_config_row`)

## Verification

1. `tox` — pytest, black, flake8, check-tools-docs all exit 0.
2. Local MCP end-to-end via `.mcp.json`:
   - `create_sql_transformation(..., variables=[{name: "env", type: "string", default_value: "prod"}])`
     → `keboola.variables` config created in Storage; transformation config carries `variables_id`.
   - `update_sql_transformation(..., variables=[])` → variables config cleared; `variables_id` removed.
   - `update_sql_transformation(..., variables=None)` → existing variables untouched.
