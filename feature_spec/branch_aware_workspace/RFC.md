# RFC: Branch-aware MCP read-only workspace + expose `workspace_id`

Linear: [AI-3208](https://linear.app/keboola/issue/AI-3208/expose-default-ro-workspace-id-in-get-project-info)

## Problem

The MCP server maintains a single shared read-only workspace per project,
managed by `WorkspaceManager` (`src/keboola_mcp_server/workspace.py`). This
workspace is used by `query_data` and by every FQN-based path in the server.
Two issues follow.

### 1. Agents cannot query branched data when on a dev branch

`WorkspaceManager.create()` (`workspace.py:511-515`) unconditionally rebinds the
client to the production branch:

```python
prod_client = await client.with_branch_id(None)
return cls(prod_client, workspace_schema)
```

and `WorkspaceManager.__init__` (`workspace.py:524-527`) hard-asserts that the
client is on the default branch. As a result the workspace ID is read from and
written to **production-branch metadata** (`branch/<default>/metadata`, key
`KBC.McpServer.v2.workspaceId`, `workspace.py:501`, `:567`, `:719`), and the
workspace itself is owned by the production branch.

This has been acceptable while branched read-only workspaces did not exist on
the platform. With the new `storage-branches` feature
(`feature_spec/branched_storage_support/RFC.md`), branched tables have
**different FQNs** than production:

| Pattern | Schema |
|---|---|
| Production table | `in.c-bucketname` |
| `storage-branches` branched table | `BRANCH_ID_in.c-bucketname` |

Today an agent sitting on a dev branch in a `storage-branches`-enabled project
**cannot query its branched table versions**: `query_data` and any FQN-driven
lookup execute in a workspace that has no view of the dev branch's schemas /
permissions. The visible symptom is empty result sets or "table not found"
errors when the agent references a branched object that *does* exist in the
branch view.

### 2. The workspace ID is not surfaced to callers

The data-app authoring skill (`dataapp-developer:dataapp-dev`) and other RO
tooling need a workspace handle to run their own RO test queries. Today they
have no way to obtain the MCP-managed workspace ID and must provision a
private workspace per session, which is wasteful (creation latency, billing
attribution noise) and reinvents the workspace lifecycle the MCP server
already owns.

## Required Behavior

### Per-branch workspace on `storage-branches` projects

| Branch context | Project has `storage-branches`? | Workspace branch | Metadata branch |
|---|---|---|---|
| Default branch | n/a | default | default |
| Dev branch | **yes** | **current dev branch** | **current dev branch** |
| Dev branch | no (legacy) | default | default |

- The metadata key stays `KBC.McpServer.v2.workspaceId`. Branch metadata is
  fully per-branch (`branch/{branch_id}/metadata`, see
  `src/keboola_mcp_server/clients/storage.py:340-358`) so the same key holds an
  independent value per branch with no collision.
- Each dev branch on a `storage-branches` project gets its own workspace, lazily
  created on first MCP call and cached in that branch's metadata for reuse.
- **Data view (verified against the platform)**: a workspace created against a
  dev branch on a `storage-branches` project sees **both** production tables
  **and** the dev branch's branched table versions through the same FQNs the
  agent already uses. The Storage API enforces this branch visibility on the
  workspace itself — the MCP server does not implement any data isolation; it
  only needs to hand the agent the workspace whose visibility matches its
  branch context. This is the load-bearing fact that makes this change
  unblock branched-table queries via `query_data`.

### Legacy and default-branch behavior unchanged

- On the default branch, behavior is identical to today.
- On a dev branch in a project **without** `storage-branches`, behavior is
  identical to today (production-branch workspace, prod-branch metadata).

### Expose `workspace_id` on `get_project_info`

Add one field to `ProjectInfo` (`src/keboola_mcp_server/tools/project.py`):

| Field | Type | Meaning |
|---|---|---|
| `workspace_id` | `int` | ID of the read-only Keboola workspace bound to the current branch context (per the table above). |

The field is consumed by the data-app authoring skill so that the skill can
reuse the MCP workspace for RO test queries instead of provisioning its own.

The description must make the branch-binding rule explicit and call out the
current platform constraint that the workspace's data view tracks the branch
only on `storage-branches` projects.

## Resolution Strategy

### `WorkspaceManager` — relax the production-branch lock

`src/keboola_mcp_server/workspace.py`:

1. Import the existing helper:
   ```python
   from keboola_mcp_server.tools.storage_helpers import has_storage_branches
   ```
2. In `WorkspaceManager.create()` (line 511), branch on the feature:
   ```python
   if client.branch_id is not None and await has_storage_branches(client):
       return cls(client, workspace_schema)
   prod_client = await client.with_branch_id(None)
   return cls(prod_client, workspace_schema)
   ```
3. Remove the unconditional `branch_id is not None` guard in `__init__`
   (`workspace.py:524-527`). The branch-selection contract lives entirely in
   `create()`; the constructor stays internal.
4. `_find_ws_in_branch` (`workspace.py:563-573`) and `_create_ws`
   (`workspace.py:575-665`) need **no changes** — they already address
   `branch/{self._branch_id}/metadata` via the client's bound branch. Once
   `create()` stops forcing the production branch, they automatically resolve
   to the correct one.

### Contract with explicit `KBC_WORKSPACE_SCHEMA`

The branch-aware rule applies uniformly whether the workspace is auto-managed by the MCP
server or pinned via `KBC_WORKSPACE_SCHEMA`. `KBC_BRANCH_ID` is the single source of
truth for branch context: on a `storage-branches` project, the named workspace is
expected to live in the explicitly-bound branch; on legacy projects it is expected to
live in the production branch (since branched read-only workspaces don't exist there).
The user owns the placement of any pre-existing workspace they reference by schema;
there is no carve-out that auto-rebinds an explicit schema lookup to the production
client.

Trade-offs:

- **Workspace cleanup on branch deletion**: handled by the platform's existing
  branch-lifecycle mechanism for branch-scoped resources (same path that
  cleans up other configurations created within a dev branch). Out of scope
  for this RFC; no special-case cleanup code added here.
- **First-call latency per branch**: dev branches already exist before the MCP
  server is used (the MCP server never creates branches). What this change
  introduces is a one-off, lazy workspace init per **existing** branch — paid
  inside the first MCP call made within that branch via `_create_ws`
  (`workspace.py:583`, currently bounded by a 5-min `timeout_sec`). All
  subsequent MCP calls within the same branch reuse the cached workspace ID
  in branch metadata, so this is not a per-call cost. Considered acceptable.

### `ProjectInfo.workspace_id`

`src/keboola_mcp_server/tools/project.py`:

- Add the field after `sql_dialect` (already done on this branch in commit
  `e1c15318`).
- Populate it via `WorkspaceManager.from_state(...).get_workspace_id()`,
  reusing the `WorkspaceManager` instance already constructed for
  `get_sql_dialect()` (so this adds no extra round trip).
- Update the field description so it reflects the branch-aware behavior
  introduced here, rather than the production-branch-only wording from
  commit `667fccac`.

## Consumers

### Data-app authoring skill

The skill (`dataapp-developer:dataapp-dev`) currently provisions its own RO
workspace for the test-query loop. After this change the skill should call
`get_project_info` and reuse `workspace_id`. This change is on the skill side
and is **out of scope of this PR**; documenting it here so the
forward-compatibility contract is clear:

- `workspace_id` is stable for the lifetime of a branch.
- It is RO; writes will fail at the Storage API layer.
- It is the same workspace the MCP server uses for `query_data` — concurrent
  use by the skill and by MCP calls is safe (workspaces support concurrent
  queries).

### Kai / other agents (no Kai changes required)

`workspace_id` is additive on `ProjectInfo`. Existing Kai schemas
(`apps/kai-assistant-backend/lib/ai/mcp.ts` `GetProjectInfoResponseSchema`,
`apps/kai-agent/src/services/mcp-client.ts` `ProjectInfo`) drop unknown
fields, mirroring what was already verified for the `branch_id` /
`branch_name` / `is_development_branch` additions in
`feature_spec/project_info_branch_context/RFC.md`. No Kai-side work needed.

The branch-aware workspace fix benefits Kai automatically: when Kai is on a
dev branch in a `storage-branches` project, `query_data` now executes in the
right branch's workspace, so SQL generated from branched table names will
resolve.

## Scope

In scope:

- `WorkspaceManager.create()` branches on `has_storage_branches(client)`.
- `WorkspaceManager.__init__` no longer forbids non-default branches.
- `ProjectInfo.workspace_id` field added and populated (already in this PR;
  description revised to match the new branch-aware semantics).
- Unit tests in `tests/test_workspace.py` covering the three rows of the
  decision table above (extend the existing parametrized cases — see
  `CONTRIBUTING.md` § Testing).
- Integration test extension in `integtests/tools/test_project.py` asserting
  that `workspace_id` resolves correctly against the
  `INTEGTEST_STORAGE_TOKEN_STORAGE_BRANCHES` and
  `INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES` projects already set up in
  `integtests/tools/test_storage_branches.py:322-360`.
- Version bump `1.60.2` → `1.61.0` (minor — new field + behavior change in
  workspace selection; both backward-compatible additions). Already bumped on
  this PR; no further bump needed unless review requests it.
- `uv.lock` synced.
- `TOOLS.md` regenerated via `tox -e check-tools-docs` (input schema of
  `get_project_info` is unchanged, so no diff is expected — the check verifies
  this).

Out of scope:

- Special-case cleanup of per-branch workspaces and `keboola.mcp-server-tool`
  configurations. Branch deletion is handled by the platform's existing
  branch-lifecycle cleanup for branch-scoped resources; no MCP-side cleanup
  code is added.
- **Migration**: none required. This change is purely additive. The existing
  production-branch workspace stays in place and continues to serve
  default-branch contexts on every project (legacy or `storage-branches`).
  On `storage-branches` projects, new dev-branch workspaces are created
  lazily alongside it as branches are first hit by MCP. No metadata is
  rewritten, no existing workspace is deleted or moved.
- Changes to the data-app authoring skill to actually consume
  `workspace_id` — separate PR in the skill repo.
- Snowflake-vs-BigQuery differences in workspace creation. The fix is
  backend-agnostic because `WorkspaceManager._create_ws` already selects the
  right backend from token info (`workspace.py:594-599`).

## Testing / Verification

### Unit tests

- Extend `tests/test_workspace.py` with a parametrized axis on
  `WorkspaceManager.create()`:
  - default branch + storage-branches feature on/off → prod client
  - dev branch + feature on → dev-branch client (no rebind)
  - dev branch + feature off → prod client
- Verify the lookup branch resolved in `_find_ws_in_branch` matches the table
  above by asserting the `branch_metadata_get` call site rather than mocking
  away the whole `WorkspaceManager`.
- Existing `tests/tools/test_project.py` test of `get_project_info` keeps its
  mock of `WorkspaceManager.get_workspace_id` and asserts the field is
  populated (already added in this PR).

### Integration tests

- Reuse the two-project fixture
  (`integtests/tools/test_storage_branches.py:322-360`):
  - **storage-branches project**: assert that `get_project_info` from a dev
    branch returns a different `workspace_id` than from the default branch
    (different metadata bag → different workspace).
  - **legacy / old-branches project**: assert that `get_project_info` from a
    dev branch returns the **same** `workspace_id` as from the default branch
    (still locked to prod).
- Smoke-test that `query_data` against a branched table in the
  storage-branches project succeeds (proves the agent can now see its branched
  data).

### Manual verification

Local `.mcp.json` setup per `CLAUDE.md`, two server entries (already present
in the repo's local `.mcp.json`):
1. `keboola-branched-storage` (project WITH `storage-branches`, dev branch
   pinned via `KBC_BRANCH_ID`): `workspace_id` must differ from the default
   branch's workspace.
2. `keboola-old-branches` (project WITHOUT the feature, dev branch pinned):
   `workspace_id` must match the default branch's workspace.

### CI

- `tox` (pytest, black, isort, flake8, check-tools-docs) all exit 0.
- Integration tests gated by the existing `INTEGTEST_STORAGE_TOKEN_*` env
  vars.
