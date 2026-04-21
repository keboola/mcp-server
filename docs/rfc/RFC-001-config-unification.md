# RFC-001: Config Tool Unification

**Status:** Implemented (v1.60.0)
**Branch:** AI-2922-local-backend-phase3

## Problem

The local and platform modes used inconsistent tool names for config management:

| Concept | Platform tool | Local tool (before) | Unified name |
|---------|--------------|---------------------|--------------|
| Create/update config | `create_config` + `update_config` | `save_config` (upsert) | `save_config` (local) / `create_config` + `update_config` (platform) |
| List configs | `get_configs` | `list_configs` | **`get_configs`** |
| Delete config | *(missing)* | `delete_config` | **`delete_config`** |
| Run a config | `run_job` | `run_saved_config` | keep separate (different model) |

Two issues:
1. Local mode exposed `list_configs` while platform mode exposed `get_configs` — same operation, different names.
2. Platform mode had no `delete_config` tool even though the Storage API endpoint existed (`configuration_delete`).

## Decision

### Local mode: rename `list_configs` → `get_configs`

The registered tool name changes from `list_configs` to `get_configs`. The implementation function (`list_configs_local` in `tools/local/tools.py`) retains its name for internal use. The tool name change makes local and platform modes share the same verb for the same operation.

### Platform mode: add `delete_config`

New tool in `tools/components/tools.py` that calls `storage_client.configuration_delete(component_id, configuration_id)`. The operation moves the config to trash (recoverable via the Keboola UI). Registered with `destructiveHint=True`.

## Files Changed

- `src/keboola_mcp_server/tools/local/tools.py` — renamed inner function `list_configs` → `get_configs`
- `src/keboola_mcp_server/tools/components/tools.py` — added `delete_config` tool + registration
- `tests/tools/local/test_integration.py` — updated `EXPECTED_TOOLS`
- `tests/test_server.py` — added `delete_config` to expected platform tool list
- `scripts/test-local-backend.sh` — updated expected tool set
- `TOOLS.md` — regenerated
