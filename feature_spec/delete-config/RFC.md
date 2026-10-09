# RFC: delete_config tool

Linear: [AI-1166](https://linear.app/keboola/issue/AI-1166/variables-support)

## Problem

The MCP server has no way to delete a component configuration. The capability was
originally implemented as a `delete=True` flag on `update_config` /
`update_sql_transformation` in PR #498 (variables support), but review feedback
asked for it to be split out:

1. Delete is orthogonal to variables — it deserves its own review and changelog
   visibility.
2. Folding delete into update tools means the `destructiveHint=True` annotation
   cannot distinguish "mutates a config" from "irreversibly deletes it".
3. The original implementation used `skip_trash=True`, making deletes
   unrecoverable, without a stated rationale.

## Required Behavior

A new dedicated `delete_config` tool:

| Aspect | Behavior |
| --- | --- |
| Input | `component_id`, `configuration_id` |
| Effect | Configuration is moved to the project **trash** (recoverable from the Keboola UI) — `skip_trash` is NOT used |
| SQL transformations | Deletable via this tool (`keboola.snowflake-transformation`, `keboola.google-bigquery-transformation`) — there is no separate transformation delete tool |
| Flows (`keboola.orchestrator`, `keboola.flow`) | Rejected with a pointer to the flows tools |
| Data apps (`keboola.data-apps`) | Rejected with a pointer to the data app tools (deleting only the config would leave a dangling deployment) |
| Related configs | NOT cascade-deleted — e.g. a linked `keboola.variables` configuration is left in place (orphaned). Cascade clean-up is deferred until variables support (PR #498) lands. |
| Output | `ConfigToolOutput` with the pre-deletion `description`/`version` and a `change_summary` noting the config can be restored from the trash |
| Annotation | `destructiveHint=True, idempotentHint=False` |

## Resolution Strategy

- `tools/components/tools.py`: new `delete_config` tool — fetches
  `configuration_detail` (for the response payload), calls
  `configuration_delete` without `skip_trash`, returns `ConfigToolOutput`.
- `tools/components/utils.py`: new `check_deletable()` +
  `_UNDELETABLE_COMPONENTS_MESSAGES` map. A separate map from
  `_UNSUITABLE_COMPONENTS_MESSAGES` because the exclusion sets differ:
  SQL transformations are deletable here but not updatable via `update_config`.
- Registration in `add_component_tools` with
  `ToolAnnotations(destructiveHint=True, idempotentHint=False)`.

Trade-off: trash-based delete means repeated deletes of the same config fail on
the second call (404) instead of being a silent no-op. That is intentional —
the agent should not believe it deleted something that was already gone.

## Scope

**In scope:**
- `delete_config` tool for root configurations and SQL transformations
- `check_deletable()` guard for flows and data apps
- Unit tests (happy path parametrized over generic config + SQL transformation;
  rejection cases added to `test_generic_tools_reject_specialized_components`)
- `TOOLS.md` regeneration, version bump (minor — new tool)

**Out of scope:**
- Cascade-deleting linked `keboola.variables` configurations (deferred; PR #498
  is not merged yet, so there is nothing to cascade from MCP-created configs)
- Deleting configuration rows
- Deleting flows or data apps
- Permanent (skip-trash) deletion
- HITL/approval gating beyond the `destructiveHint` annotation — clients are
  expected to gate destructive tools; server-side approval is a separate product
  decision

## Testing / Verification

1. `tox` — pytest, black, flake8, check-tools-docs all exit 0.
2. Unit tests in `tests/tools/components/test_tools.py`:
   - `test_delete_config` — generic config and SQL transformation: detail fetched,
     delete called once WITHOUT `skip_trash`, output carries pre-deletion state.
   - `test_generic_tools_reject_specialized_components` — flows and data apps
     rejected with the standard error message.
3. Manual: delete a test configuration via local MCP (`.mcp.json`), verify it
   appears in the project trash in the UI and can be restored.
