# RFC: Branch context in `get_project_info`

Linear: [AI-2391](https://linear.app/keboola/issue/AI-2391/kai-uses-dev-branch-bucket-names-when-generating-sql)

## Problem

The `project_system_prompt.md` returned by `get_project_info` contains guidance specific
to **development branches**:

- FQN of branched tables differs from production paths.
- Transformations should prefer input mapping over direct FQN references in branches.
- Data App tools are not supported in dev branches.

For agents (e.g. Kai) to apply that guidance correctly they must know whether the
current MCP call is operating on a dev branch. The server already resolves a per-request
branch from the `X-Branch-Id` HTTP header (or `KBC_BRANCH_ID` env var, see
`src/keboola_mcp_server/config.py:91-93` and `src/keboola_mcp_server/mcp.py:218-220`),
but `get_project_info` does not surface this. Today an agent has no reliable way to tell.

The visible symptom in AI-2391 is that Kai used dev-branch bucket names when generating
SQL — i.e. it could not distinguish branch vs production context.

## Required Behavior

`get_project_info` must return enough context for the agent to determine its branch
mode. Concretely, three new fields on `ProjectInfo`:

| Field                    | Type        | Meaning                                                              |
|--------------------------|-------------|----------------------------------------------------------------------|
| `branch_id`              | `str \| int` | Resolved branch ID, always populated (even on default).              |
| `branch_name`            | `str`       | Human-readable branch name (e.g. `"Main"` or the dev branch name).   |
| `is_development_branch`  | `bool`      | `True` iff this is **not** the default/production branch.            |

The fields must reflect the **per-request** branch resolution — i.e. two simultaneous
clients passing different `X-Branch-Id` headers must each see their own branch.

## Resolution Strategy

Use the existing `AsyncStorageClient.branches_list()`
(`src/keboola_mcp_server/clients/storage.py:327`):

- If `KeboolaClient.branch_id is None` → pick the entry with `isDefault is True`
  (default/production branch).
- Else → pick the entry where `id` matches `client.branch_id` (compare as strings to be
  safe across `int`/`str`).
- Populate `branch_id`, `branch_name` (`name`), `is_development_branch = not isDefault`.

`branches_list` is preferred over `dev_branch_detail` because the default branch's
numeric ID is unknown when `client.branch_id is None`, and the `dev-branches/{id}`
endpoint is not verified to accept the `'default'` shorthand (only
`branch/{id}/metadata` does).

## Compatibility with Kai (Keboola UI)

Verified against `/Users/esner/Documents/Prace/KBC/AI-TESTING/ui` — the consumer of this
tool. Adding the three fields is **non-breaking**:

- **Backend** (`apps/kai-assistant-backend/lib/ai/mcp.ts:148-155`, schema
  `GetProjectInfoResponseSchema` lines 265-276): Zod `.optional()` shape; unknown fields
  are silently dropped during validation. No break.
- **Agent** (`apps/kai-agent/src/services/mcp-client.ts:248-263`): TypeScript interface
  `ProjectInfo` (lines 31-40); `JSON.parse(...) as ProjectInfo` tolerates extra
  properties. No break.
- **Branch transport already wired**: Both Kai entry points already inject
  `X-Branch-Id` on MCP requests when `config.branchId` is present
  (`mcp.ts:314`, `mcp-client.ts:82`). The per-request branch resolution this RFC depends
  on is therefore live in the Kai integration today.
- **`llm_instruction` injection**: Both Kai paths concatenate `llm_instruction` verbatim
  under a `## Platform-Wide Instructions` section in the agent system prompt
  (`prompts.ts:25-27`, `prompt-builder.ts:56-59`). Existing dev-branch guidance in
  `project_system_prompt.md` is therefore already reaching the agent — what's missing is
  the data the agent needs to act on it.
- **Current Kai branch awareness**: Kai does **not** today read any branch field from
  `ProjectInfo`; it only passes the header through. So the symptom in AI-2391 (Kai
  using dev-branch bucket names in SQL) is consistent with the agent having no
  in-context signal about branch mode.

### Follow-up on the Kai side (separate PR, not in this scope)

To make the new fields useful end-to-end, Kai will need a small change:

1. Add the three fields as optional to both the Zod schema and the TS interface:
   - `apps/kai-assistant-backend/lib/ai/mcp.ts` `GetProjectInfoResponseSchema`
   - `apps/kai-agent/src/services/mcp-client.ts` `ProjectInfo`
2. Inject them (or a derived hint) into the system prompt builders alongside
   `llm_instruction`, e.g. a one-line "You are operating on development branch
   '<name>' (id: <id>). Apply branch-specific guidance." when
   `is_development_branch` is true.

This MCP server change is forward-compatible: until Kai is updated, the new fields are
simply ignored.

## Scope

In scope:

- Add the three fields to `ProjectInfo` (`src/keboola_mcp_server/tools/project.py`).
- Resolve them from `branches_list` inside `get_project_info`.
- Extend the existing parametrized unit test (`tests/tools/test_project.py`) with a
  branch-mode axis (default vs dev).
- Add type/sanity assertions to the integtest (`integtests/tools/test_project.py`).
- Bump version `1.58.1` → `1.59.0` (new field on a tool response = feature → minor
  bump per CLAUDE.md) and refresh `uv.lock`.
- Regenerate `TOOLS.md` via `tox -e check-tools-docs`.

Out of scope:

- Edits to `project_system_prompt.md`. Existing in-flight edits to that file already
  cover dev branch guidance; once the fields exist agents can key off them. A follow-up
  may add an explicit reference to `is_development_branch` if needed.

## Verification

1. `tox` — pytest, black, flake8, check-tools-docs all exit 0.
2. Manual end-to-end via local MCP (`.mcp.json` per CLAUDE.md):
   - Without `KBC_BRANCH_ID`: `is_development_branch=False`, branch_name = production
     branch name.
   - With `KBC_BRANCH_ID=<dev branch>`: `is_development_branch=True`, matching name/ID.
3. Optional: integtests against a real project.
