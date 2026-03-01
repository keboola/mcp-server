# Configuration Diff Viewer MCP App — Design

## Goal

Add a `preview_config_diff` MCP App tool that shows a side-by-side visual diff of configuration changes before the LLM executes a mutation tool. Covers all 6 mutation tools: `update_config`, `update_config_row`, `update_sql_transformation`, `update_flow`, `modify_flow`, `modify_data_app`.

## Architecture

**Data flow:**

```
LLM calls preview_config_diff(tool_name, tool_params)
  -> MCP tool gets client + workspace_manager from ctx.session.state
  -> Calls shared compute_config_diff() (extracted from preview.py)
  -> Returns {originalConfig, updatedConfig, coordinates, isValid, validationErrors}
  -> MCP App receives via ontoolresult
  -> jsondiffpatch computes diff client-side
  -> Renders side-by-side view with syntax-highlighted JSON
```

One tool covers all mutation types. The `tool_name` param identifies which mutation to preview, and `tool_params` contains the same parameters you'd pass to that mutation tool.

## Tool Design

**Signature:**
```python
async def preview_config_diff(
    ctx: Context,
    tool_name: str,       # e.g. "update_config", "modify_flow"
    tool_params: dict,    # same params as the mutation tool
) -> dict[str, Any]
```

**Return format:**
```json
{
  "coordinates": {
    "componentId": "keboola.ex-aws-s3",
    "configurationId": "123"
  },
  "originalConfig": { "...full config dict..." },
  "updatedConfig": { "...full config dict..." },
  "isValid": true,
  "validationErrors": null
}
```

**Properties:**
- `readOnlyHint=True`
- Tagged with `CONFIG_DIFF_TAG` (new tag, separate from `CONFIG_DIFF_PREVIEW_TAG`)
- Model-visible (default)
- Linked to `ui://keboola/config-diff` resource

## Refactoring preview.py

Extract the core diff computation from the HTTP handler into a shared function:

```python
async def compute_config_diff(
    tool_name: str,
    tool_params: dict[str, Any],
    client: KeboolaClient,
    workspace_manager: WorkspaceManager,
) -> PreviewConfigDiffResp
```

The existing HTTP endpoint becomes a thin wrapper (parse request, get client/workspace_manager from Starlette state, call `compute_config_diff`, return JSONResponse). The MCP tool calls the same function with client/workspace_manager from `ctx.session.state`.

## HTML App Design

**CDN dependencies:**
- `jsondiffpatch` from unpkg.com — structural JSON diff with built-in HTML formatter
- MCP Apps SDK 1.1.2 from unpkg.com

**Layout:**
- No header (host card provides title)
- Full-width side-by-side diff panel
- Left: "Original" with line numbers
- Right: "Updated" with line numbers
- Changed values highlighted yellow, additions green, removals red

**States:**
1. Loading — spinner
2. Valid diff — side-by-side rendered diff
3. Validation error — red error box with `validationErrors`
4. No changes — "No changes detected" message

**Theme:** CSS custom properties for light/dark, toggled via `onhostcontextchanged`.

## Files

| Action | File |
|--------|------|
| Create | `src/keboola_mcp_server/tools/config_diff.py` |
| Create | `src/keboola_mcp_server/apps/config_diff.html` |
| Create | `tests/tools/test_config_diff.py` |
| Create | `tests/apps/test_config_diff_html.py` |
| Modify | `src/keboola_mcp_server/preview.py` (extract `compute_config_diff`) |
| Modify | `src/keboola_mcp_server/server.py` (add import + call) |
| Modify | `src/keboola_mcp_server/generate_tool_docs.py` (add category) |
| Modify | `tests/apps/test_registration.py` (add config diff tests) |
| Modify | `tests/test_server.py` (add to tool list + parametrize) |

## Testing

- **`tests/tools/test_config_diff.py`**: Valid diff for mocked update_config, valid diff for mocked modify_flow, invalid tool_name returns error, mutator error returns error, coordinates extracted correctly per tool type
- **`tests/apps/test_config_diff_html.py`**: SDK import, app.connect(), ontoolresult, jsondiffpatch loaded, dark mode, error/loading states
- **`tests/apps/test_registration.py`**: Tool registered, resource registered, tool meta has resourceUri, resource meta has CSP for unpkg.com
- **`tests/test_server.py`**: Add to tool list + parametrize entry
- **`tests/test_preview.py`**: Existing tests still pass after refactor
