# RFC: Merge Request tools for Development Branches (non-SOX)

Linear: [DMD-1701](https://linear.app/keboola/issue/DMD-1701) — milestone
["Branches 2.0 - MCP integration"](https://linear.app/keboola/project/finalize-dev-branches-327e7756d2fd/overview)

## Problem

Keboola's "Branches 2.0" gives users a Git-like workflow: work in an isolated
development branch, then promote the changes to production through a **merge request**
(MR) with optional review and approval. The Connection backend
(`FEATURE_BRANCHES_MERGE_REQUESTS`) and the Keboola UI already implement the full
non-SOX flow. The MCP server does **not** — it can read/write objects on a branch (the
`storage-branches` support already landed) but has no way to *list, inspect, create,
review, or merge* merge requests.

The visible symptom: a user working with an AI chat (Kai) inside a development branch has
no way to say *"ship these changes to production"*. They must leave the chat, open the
Keboola UI, and drive the MR by hand — exactly the friction the initiative is meant to
remove. Because the MCP audience skews **less technical**, the raw nine-endpoint MR API is
also the wrong altitude to expose verbatim; the server should collapse the common path
into a few well-named tools.

Scope of this RFC is the **non-SOX** flow only (`branches-merge-requests` feature). The
SOX flow (`protected-default-branch`: mandatory reviewers, production managers, 2 required
approvals) is explicitly out of scope.

## Background — the Connection MR API

Reference (verified in `keboola/connection`,
`connection/src/Controller/Storage/MergeRequest/*`):

| Endpoint | Method / Path | Purpose |
|---|---|---|
| List | `GET /merge-request` | All MRs in project |
| Detail | `GET /merge-request/{id}?include=activityLog` | MR + `changeLog` + activity log |
| Conflicts | `GET /merge-request/{id}/conflicts` | `[]` if none, else conflict list |
| Create | `POST /merge-request` | `branchFromId`, `branchIntoId`, `title`, `description?`, `externalId?`, `autoMergeStrategy?`, `autoMergeAt?`, `reviewerIds?` |
| Update | `PUT /merge-request/{id}` | Same fields, all optional |
| Request review | `PUT /merge-request/{id}/request-review` | `development → in_review` |
| Approve | `PUT /merge-request/{id}/approve` | adds one approval |
| Request changes | `PUT /merge-request/{id}/request-changes` | `{reason?}`, sends MR back to `development` |
| Merge | `PUT /merge-request/{id}/merge` | async, returns a **Job** |

State machine:

```
development ─request-review─▶ in_review ─approve(×N)─▶ approved ─merge─▶ in_merge ─(job)─▶ published
     ▲                            │                        │
     └──────request-changes───────┴────────────────────────┘
                                                        (terminal: published, canceled)
```

Non-SOX specifics that shape the design:

- **Default required approvals = 0** (`RequiredApprovalsCountProvider`, metadata key
  `KBC.branches-merge-requests.required-approvals-count`). With the default,
  `request-review` transitions **straight to `approved`** and the approve step never runs.
- **Roles**: only `admin` / `share` may approve, request-changes, or merge; `admin` /
  `share` / `reviewer` / `developer` may create / update / request-review; detail,
  list and conflicts are read-only for any member.
- **Merge is asynchronous** — the endpoint returns a Storage Job; the MR reaches
  `published` only when the job succeeds.
- The `changeLog` is a JSON diff (added / modified / deleted configurations) computed at
  merge time.

## Required Behavior

### Guiding principles

1. **Meet the user where they are.** In the dominant scenario the MCP session is already
   on a development branch (Kai UI sets `X-Branch-Id`, or `KBC_BRANCH_ID` is set). MR tools
   default the source branch to the **current session branch** and the target to the
   **default/production branch**. No branch switching is required — and none is added in
   this RFC (see Scope).
2. **Simplify, don't mirror.** Expose the full lifecycle as explicit tools for parity, but
   add one orchestrator (`publish_branch`) that walks the happy path end-to-end and
   explains, in plain language, when a human must step in.
3. **Speak human.** Resolve branch/user **IDs → names**, and translate the raw `changeLog`
   into a plain summary (e.g. *"3 configurations added, 1 modified, 0 deleted"*).
4. **Gate by feature and role**, reusing the existing `ToolsFilteringMiddleware`, so users
   never see tools they cannot use.

### Tools

All tools live in a new module `src/keboola_mcp_server/tools/merge_requests.py`, tagged
`merge-request`, registered via `add_merge_request_tools(mcp)` in
`src/keboola_mcp_server/server.py`. All are gated behind the `branches-merge-requests`
project feature (below); write/approve/merge tools are additionally role-gated.

**Tier A — MVP (end-to-end happy path):**

| Tool | Annotation | Behavior |
|---|---|---|
| `get_merge_requests` | `readOnlyHint` | List MRs; optional `state` filter. Branch/user IDs resolved to names. |
| `get_merge_request` | `readOnlyHint` | Detail for one MR: metadata, reviewers + status, plain-language changelog summary, activity-log timeline, and conflicts — fetched in one call (`?include=activityLog` + `/conflicts`). |
| `create_merge_request` | (write) | Create an MR. `branch_from` defaults to the current session branch, `branch_into` to the default branch. `title` required; `description`, `reviewer_ids`, `auto_merge` optional. |
| `request_merge_request_review` | (write) | `development → in_review` (→ `approved` when 0 approvals required). |
| `merge_merge_request` | `destructiveHint` | Trigger merge **and await the returned Job** (reusing existing job-polling infra); return final MR state + changelog summary. |
| `publish_branch` | `destructiveHint` | **Orchestrator** — see below. |
| `get_branches` | `readOnlyHint` | List the project's branches (id, name, isDefault, created). Read-only context so the agent/user can see what exists. |

**Tier B — full UI parity (fast follow, same PR or immediate next):**

| Tool | Annotation | Behavior |
|---|---|---|
| `approve_merge_request` | (write) | Add an approval (`in_review`; relevant only when required approvals > 0). admin/share only. |
| `request_merge_request_changes` | (write) | Send MR back to `development` with an optional `reason`. admin/share only. |
| `update_merge_request` | (write) | Edit title / description / reviewers / auto-merge on a non-terminal MR. |

**`publish_branch` orchestrator** — the "MCP simplifies the process" tool. For the current
development branch it performs, stopping with a clear human-readable status at the first
gate it cannot pass:

1. Resolve the current branch; error if on the default/production branch.
2. Find an open MR for this branch, or create one from the given `title`/`description`.
3. Fetch conflicts — if any, return them explained and stop (advise resolving/rebasing).
4. If `development`, request review.
5. If the MR still needs approvals it doesn't have, stop and report *"waiting for approval
   from a project admin"* (do **not** self-approve — that would defeat review intent).
6. If `approved`, merge, await the job, and report what changed in plain language.

Its return value always includes the resulting `state` and a `next_step` string so the AI
can tell the user exactly what (if anything) a human needs to do next.

### Feature & role gating

- Add `'branches-merge-requests'` to the `ProjectFeature` literal
  (`src/keboola_mcp_server/clients/storage.py:23`).
- In `ToolsFilteringMiddleware.on_list_tools` / `on_call_tool`
  (`src/keboola_mcp_server/mcp.py`): hide **all** `merge-request` tools when the feature is
  absent; additionally hide `approve_merge_request`, `request_merge_request_changes`, and
  `merge_merge_request` / `publish_branch` when the token role is not `admin`/`share`
  (mirroring the Connection route guards).

### Response models

Lean Pydantic models, human-first:

- `MergeRequest`: `id`, `title`, `description`, `state`, `branch_from_name`,
  `branch_into_name`, `creator_name`, `reviewers` (name + status), `auto_merge`,
  `created_at`, `merged_at`.
- `MergeRequestDetail` extends it with `changelog_summary` (counts + short per-item list),
  `activity_log` (typed events), and `conflicts`.
- `MergeResult`: `merge_request_id`, `state`, `job_id`, `changelog_summary`, `next_step`.

## Resolution Strategy

1. **Storage client** (`src/keboola_mcp_server/clients/storage.py`): add thin methods
   wrapping each endpoint, following the existing `branches_list` / `dev_branch_detail`
   pattern and the base `get`/`post`/`put` helpers
   (`src/keboola_mcp_server/clients/base.py`): `merge_requests_list`,
   `merge_request_detail(id, include=...)`, `merge_request_conflicts(id)`,
   `merge_request_create(payload)`, `merge_request_update(id, payload)`,
   `merge_request_request_review(id)`, `merge_request_approve(id)`,
   `merge_request_request_changes(id, reason)`, `merge_request_merge(id)`.
2. **Branch resolution**: reuse `_resolve_branch_context`
   (`src/keboola_mcp_server/tools/project.py:46`) to get the current branch id/name and the
   default branch id (from `branches_list`, `isDefault=True`) — the same helper
   `get_project_info` uses. Factor it into a shared location if needed.
3. **Async merge**: the merge endpoint returns a Job; reuse the existing job utilities in
   `src/keboola_mcp_server/tools/jobs.py` to poll to completion inside `merge_merge_request`
   and `publish_branch` rather than reimplementing polling.
4. **Changelog humanization**: a small pure function maps the `changeLog` JSON
   (`addedConfigs` / `modifiedConfigs` / `deletedConfigs`) to counts + a truncated
   human list. Unit-tested in isolation.
5. **Gating**: extend `ProjectFeature` and the two `ToolsFilteringMiddleware` hooks as
   above; add a `merge-request` tag so filtering is a simple tag membership test.
6. **Docs & version**: regenerate `TOOLS.md` (`tox -e check-tools-docs`); bump
   `pyproject.toml` `1.73.1 → 1.74.0` (new tools = minor) and refresh `uv.lock`.

**Trade-offs called out:**

- *Explicit tools vs. one `action=` enum tool.* We choose explicit per-action tools:
  better LLM tool-selection and correct per-tool `readOnly`/`destructive` hints, at the
  cost of more tool entries. The orchestrator keeps the common path to a single call, so
  the granular tools are for parity/power, not the default path.
- *Not exposing `approve` on the happy path.* With the non-SOX default of 0 approvals,
  `publish_branch` never needs to approve; `approve_merge_request` exists only for projects
  that raise the required count. `publish_branch` deliberately never self-approves.
- *No branch create/switch.* Creating or switching branches mid-session would require
  mutating session state and reprovisioning the SQL workspace (branch-bound at session
  creation, `mcp.py:267-311`). That is a separate architecture decision and is out of
  scope; `get_branches` is read-only context only.

## Scope

**In scope:**

- New module `tools/merge_requests.py` with Tier A + Tier B tools listed above, plus
  read-only `get_branches`.
- Storage-client wrappers for the nine MR endpoints (+ branches list already exists).
- Feature gating (`branches-merge-requests`) and role gating for write/merge tools.
- Changelog humanization helper and lean response models.
- `TOOLS.md` regeneration, a minor version bump (from current `main` — `1.74.3 → 1.75.0`
  at the time of writing; new tools = minor), `uv.lock` refresh.

**Out of scope:**

- SOX flow (`protected-default-branch`: mandatory reviewers, production managers, required
  approvals ≥ 2). The tools must not misbehave there, but full SOX UX is a separate RFC.
- Creating, switching, or deleting branches; any mid-session branch context change or SQL
  workspace reprovisioning.
- Auto-merge scheduling UX beyond passing `autoMergeStrategy`/`autoMergeAt` through
  create/update.
- CLI and kbagent-specific wiring (tracked under the sibling milestones).

## Testing / Verification

- **Unit tests** (`tests/tools/test_merge_requests.py`), parametrized:
  - branch defaulting (current dev branch → default) in `create_merge_request`.
  - `publish_branch` decision table: on production branch (error), conflicts present
    (stop), 0 approvals (auto-approved → merge), approvals still required (stop with
    `next_step`), already `approved` (merge), non-dev branch, merge job success/failure.
  - changelog humanization: empty, mixed add/modify/delete, truncation.
  - feature/role gating: tool hidden without feature; approve/merge hidden for
    `developer`/`reviewer`; visible for `admin`/`share`.
- **Integration tests** (`integtests/tools/test_merge_requests.py`) against a project with
  `branches-merge-requests`: create a dev branch (via SAPI in fixture, not via MCP), make a
  config change, `create_merge_request` → `request_merge_request_review` →
  `merge_merge_request`, assert `published` + changelog, then clean up.
- **Manual E2E** via local `.mcp.json` (per CLAUDE.md) on a non-SOX branch project: run
  `publish_branch` from a dev branch and confirm the plain-language summary and final state.
- **`tox`** (pytest, black, flake8, check-tools-docs) green before pushing.
