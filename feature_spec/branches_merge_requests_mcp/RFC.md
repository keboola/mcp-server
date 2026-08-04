# RFC: Merge Request tools for Development Branches (non-SOX)

Linear: [DMD-1701](https://linear.app/keboola/issue/DMD-1701) — milestone
["Branches 2.0 - MCP integration"](https://linear.app/keboola/project/finalize-dev-branches-327e7756d2fd/overview)

## Problem

Keboola's "Branches 2.0" gives users a Git-like workflow: work in an isolated
development branch, then promote the changes to production through a **merge request**
(MR) with optional review, approval, and conflict resolution. The Connection backend
(`FEATURE_BRANCHES_MERGE_REQUESTS`) and the Keboola UI already implement the full
non-SOX flow. The MCP server does **not** — it can read/write objects on a branch (the
`storage-branches` support already landed) but has no way to *list, inspect, create,
review, resolve conflicts on, or merge* merge requests.

The visible symptom: a user working with an AI chat (Kai) inside a development branch has
no way to say *"ship these changes to production"*, and no way to be guided through a
conflict when production has moved on. They must leave the chat, open the Keboola UI, and
drive the MR by hand — exactly the friction the initiative is meant to remove. Because the
MCP audience skews **less technical**, the raw MR API is also the wrong altitude to expose
verbatim; the server should collapse the common path into a few well-named tools and let
the agent *guide* the user (especially through conflicts).

Scope of this RFC is the **non-SOX** flow only (`branches-merge-requests` feature). The
SOX flow (`protected-default-branch`: mandatory reviewers, production managers, ≥2 required
approvals) is explicitly out of scope.

## Background — the Connection API

### MR endpoints (project-level)

Verified in `keboola/connection`, `connection/src/Controller/Storage/MergeRequest/*`. All
carry `isAvailableInBranch: false, isAvailableWithoutBranch: true` — i.e. they are
**project-level**, called at the project root (not under a `branch/{id}/` path) and
identified by the MR `id`, **not** by any session branch.

| Endpoint | Method / Path | Purpose |
|---|---|---|
| List | `GET /merge-request` | All MRs in project |
| Detail | `GET /merge-request/{id}?include=activityLog` | MR + `changeLog` + activity log |
| Conflicts | `GET /merge-request/{id}/conflicts` | `[]` or the list of conflicting configs (identifiers, not diff content) |
| Create | `POST /merge-request` | `branchFromId`, `branchIntoId`, `title`, `description?`, `externalId?`, `autoMergeStrategy?`, `autoMergeAt?`, `reviewerIds?` |
| Update | `PUT /merge-request/{id}` | Same fields, all optional |
| Request review | `PUT /merge-request/{id}/request-review` | `development → in_review` |
| Approve | `PUT /merge-request/{id}/approve` | adds one approval |
| Request changes (reject) | `PUT /merge-request/{id}/request-changes` | `{reason?}`, sends MR back to `development`, **resets approvals** (controller: `MergeRequestRejectAction`) |
| Merge | `PUT /merge-request/{id}/merge` | async, returns a **Job** |

State machine:

```
development ─request-review─▶ in_review ─approve(×N)─▶ approved ─merge─▶ in_merge ─(job)─▶ published
     ▲                            │                        │
     └──────request-changes───────┴────────────────────────┘
                                                        (terminal: published, canceled)
```

### Conflict-resolution endpoints (branch-scoped)

Conflicts are resolved by a per-configuration **rebase** driven by a three-way diff. Both
endpoints carry `isAvailableInBranch: true, isAvailableWithoutBranch: false` — i.e. they
are **branch-scoped** and only work from within the dev branch.

| Endpoint | Method / Path | Purpose |
|---|---|---|
| Config diff | `GET /components/{componentId}/configs/{configurationId}/diff` | Three-way diff: `base` (dev branch v1), `ours` (dev head), `theirs` (default head) |
| Config rebase | `POST /components/{componentId}/configs/{configurationId}/rebase` | Submit the **resolved** config + complete `rows` set. `rows: []` deletes all rows; array order = sort order; a delete resolution writes a tombstone. `RequireFeature(branches-merge-requests)` |

### Verified behaviors that shape the design

- **Merge is atomic.** `PUT …/merge` only sets `in_merge` and enqueues one job
  (`MergeProcessor`). Inside a single DB transaction that job (`MergeDevBranchJob`) both
  applies the config changes (`mergeConfigurationsService->merge`) **and** transitions to
  `published` (`MergeRequestService::publish`, `…:197`). Any failure rolls back to
  `approved` (`rollbackMerge`). There is **no separate publish action/endpoint**.
- **Conflicts are computed live**, not stored as a merge gate. `DefaultConflictValidator`
  compares each dev-branch config's **version(1)** (base) against the default branch's
  current head; a mismatch is a conflict (`…:84-86`). The `conflictInReviewDetectedAt`
  latch is only for notification emails.
  → *Once every conflicting config is rebased, the live check passes and the MR becomes
  mergeable on its own — no MR-level "re-validate/mark-rebased" step.*
- **Rebase does not reset approvals.** Approvals reset only on `request-changes` (reject).
  So *resolve → rebase → merge* needs no re-approval.
- **The dev branch is locked for editing only during `in_merge`** (`StorageRouteGuard`:
  `$isBranchLocked = $mr->isInMerge()`). Editing/rebasing is therefore allowed while the MR
  is `development`, `in_review`, or `approved`.
- **Default required approvals = 0** for non-SOX (`RequiredApprovalsCountProvider`). With
  the default, `request-review` transitions **straight to `approved`** and the approve step
  never runs.
- **Roles**: only `admin` / `share` may approve, reject, or merge; `admin` / `share` /
  `reviewer` / `developer` may create / update / request-review; list / detail / conflicts
  are readable by any member.
- **Create target is forced to the default branch**
  (`MergeRequestCreateProcessor.php:53-54`, `InvalidBranchException::createTargetBranchNotDefault`).
- **No cancel endpoint.** An MR is canceled only as a side-effect of deleting its source
  dev branch (`DevBranchDelete.php:201` → `MergeRequestService::cancel`). There is no route
  to cancel an MR directly.

## Required Behavior

### Guiding principles

1. **Meet the user where they are.** MR *reads* and *review-state* actions work from any
   session (they are project-level). Anything that touches or promotes branch content
   (create / resolve / merge) runs **only from a dev-branch session** — see the gating
   model. No mid-session branch switching is added (see Scope).
2. **Simplify, don't mirror.** Expose the lifecycle as explicit, well-named tools, and let
   the agent guide the user through the branching (merge / approvals / conflicts) using
   structured status returned by the tools.
3. **Speak human.** Resolve branch/user **IDs → names**; translate the raw `changeLog` into
   a plain summary (e.g. *"3 configurations added, 1 modified, 0 deleted"*).
4. **Gate by feature, role, and session context**, reusing the existing
   `ToolsFilteringMiddleware`, so users never see tools they cannot use.

### The gating model (three axes)

| Axis | Rule |
|---|---|
| **Feature** | All MR tools require the `branches-merge-requests` project feature. |
| **Role** | Write / approve / reject / merge require `admin` or `share` (mirrors Connection route guards). |
| **Session context** | **Read + review-state** tools work from *any* session (production or branch). **Content / promotion** tools (`create`, `submit_for_review`, `merge`, conflict tools, `publish_branch`) work **only from a dev-branch session**; called from production they return a clear handoff error (*"open a session on branch '<name>' and do it there"*). |

Note on merge: the raw `merge` endpoint is project-level (`isAvailableWithoutBranch: true`),
so the backend *would* allow it from the root. **MCP deliberately keeps merge branch-only
via gating** — the whole promotion flow stays "inside the branch". (Consequence: there is
no cross-branch "merge any MR by id from production"; a dev branch has at most one open MR,
so `merge` is always "merge my branch's MR".)

### Tools

All tools live in a new module `src/keboola_mcp_server/tools/merge_requests.py`, tagged
`merge-request`, registered via `add_merge_request_tools(mcp)` in
`src/keboola_mcp_server/server.py`.

**Read-tool convention — a *simplified* variant of `get_configs`
(`tools/components/tools.py:205`), not a 1:1 mirror.** `get_merge_requests` takes a single
optional flat list `merge_request_ids` — empty → list **all** as summaries; IDs → full detail
per MR, **batched concurrently** (via `process_concurrently` / `unwrap_results`).
Lower-priority filters (e.g. `state`) are ignored when IDs are supplied.

What we borrow from `get_configs` is the *behavioral* contract (no IDs = list; IDs = batched
details; IDs outrank filters) and its concurrency helpers. What we deliberately do **not**
copy is its parameter shape: `get_configs` needs two separate inputs (`component_ids` for
summaries, `configs` for details) because a configuration is addressed by a
**component+config pair**. An MR id is a single flat integer, so one list covers both modes
and the extra parameter would be noise.

#### Read / diagnosis — available in any session

| Tool | Annotation | Behavior |
|---|---|---|
| `get_merge_requests` | `readOnlyHint` | No IDs → list MRs (summaries; optional `state` filter). With IDs → detail per MR: the **status object** (below) + plain-language changelog summary + activity-log timeline + the conflicts **list** (which configs conflict — cheap, from `/conflicts`; not the diff content). IDs → names. |

#### Review-state actions — available in any session (`admin`/`share`)

| Tool | Annotation | Behavior |
|---|---|---|
| `approve_merge_request` | (write) | Add an approval. Relevant only when required approvals > 0. |
| `reject_merge_request` | (write) | Send the MR back to `development` (resets approvals; unlocks branch), optional `reason`. Pairs with `approve` as *approve ↔ reject*. Not terminal — see `cancel` in Scope. |

#### Author / promotion — dev-branch session only (`admin`/`share`)

| Tool | Annotation | Behavior |
|---|---|---|
| `create_merge_request` | (write) | Create an MR for the **current session branch**. Signature: `title` (required), `description?`, `reviewer_ids?`, `auto_merge?` — **no branch parameters** (source = session branch, target = default, both resolved internally). Errors from production. |
| `submit_merge_request_for_review` | (write) | `merge_request_id?` (see MR-id convention below). `development → in_review` (→ `approved` when 0 approvals required). |
| `merge_merge_request` | `destructiveHint` | `merge_request_id?` (see below). Merges and **awaits the Job** → `published`. On refusal returns the **status object** (why it is blocked: approvals / conflicts) rather than a bare error. |

#### Conflict resolution — dev-branch session only (`admin`/`share`)

| Tool | Annotation | Behavior |
|---|---|---|
| `get_merge_request_conflicts` | `readOnlyHint` | For the given MR: the conflict list **plus the three-way diff** (`base`/`ours`/`theirs`) for each conflicting config. Backend: 1× `/conflicts` + N× per-config `/diff`, fetched concurrently. |
| `resolve_config_conflict` | (write) | Submit **one** resolved configuration (per-config rebase). Full config + complete `rows`; supports "resolve = delete" (tombstone). Called once per conflict; there is no batch rebase. |

**MR-id convention (applies to every tool that targets one MR).** The underlying endpoints
are project-level and always addressed by MR id, so every such tool accepts
`merge_request_id`. It differs only in whether it may be omitted:

| Tool group | `merge_request_id` | Resolution / validation |
|---|---|---|
| `approve_merge_request`, `reject_merge_request` | **required** | Any session; the MR is identified purely by id. No branch check. |
| `submit_merge_request_for_review`, `merge_merge_request`, `get_merge_request_conflicts`, `resolve_config_conflict` | **optional** | Dev-branch session only. When **omitted**, resolve the session branch's single open MR (a branch has at most one — the backend rejects a second, `InvalidBranchException::createMergeRequestExists`). When **given**, validate that its `branchFromId` equals the session branch and otherwise fail with the handoff error — never act on another branch's MR. |

This keeps both phrasings working: *"merge it"* (id omitted, resolved from the branch) and
*"merge #1234"* (id given and validated), which is what chat flow Y below does.

#### Optional orchestrator — dev-branch session only

| Tool | Annotation | Behavior |
|---|---|---|
| `publish_branch` | `destructiveHint` | One-shot "ship this branch": find-or-create the MR → submit for review → merge. On a conflict, hands off to the conflict-resolution flow (it does not auto-resolve). Nice-to-have, not required for the MVP. |

**MVP core** (makes the X/Y chat flows below + conflict resolution work): `get_merge_requests`,
`create_merge_request`, `submit_merge_request_for_review`, `merge_merge_request`,
`get_merge_request_conflicts`, `resolve_config_conflict`. `approve` / `reject` earn their
keep on projects with required approvals > 0. `publish_branch` is optional convenience.

**Dropped / deferred:** `update_merge_request` (low value for the chat audience — omitted
for now); `cancel_merge_request` (no endpoint — cancel = branch deletion = Tier C);
`get_branches` (deferred to Tier C branch management, where it first gains a use).

### The status object (drives the agent's guidance)

To make the conversational flows reliable, `get_merge_requests` detail **and**
`merge_merge_request` (on refusal) return the same explicit status so the agent branches on
data, not on its own guesswork:

```
state,                       # development | in_review | approved | in_merge | published
is_approved,
mergeable: bool,
approvals: { required, given_by: [names], missing_from: [names] },
conflicts: [ {component_id, configuration_id, name} ],   # list only, no diff content
blocked_reason: none | needs_approval | conflicts,
action_required: merge | submit_for_review | wait_for_approval | resolve_conflicts | none,
next_step: "<one human-readable sentence>",
```

The approvals breakdown maps directly to the API's `reviewers:[{name,status}]` + `approvals`.
This lets the agent produce, e.g.:

- *"MR #1234 is approved and conflict-free — merge?"* (`mergeable=true`)
- *"MR #1234 can't merge yet — conflicts in A, B. Resolve now?"* (`blocked_reason=conflicts`)
- *"You're missing approval from X, Y; Z already approved."* (`blocked_reason=needs_approval`)

### Chat flows the tools must support

**X — "Is there anything to merge?"** → `get_merge_requests(state=approved)`; detail's
status shows `mergeable`/`conflicts` so the agent proactively offers merge or conflict
resolution.

**Y — "Merge #1234."** → `merge_merge_request(1234)`; on `blocked_reason=conflicts` the
agent fetches `get_merge_request_conflicts`, walks the user through
`resolve_config_conflict` per config (propose merge from `base`/`ours`/`theirs`, user
confirms, rebase), then retries merge:

```
merge_merge_request(1234) → { blocked_reason: conflicts, conflicts:[A,B], next_step }
get_merge_request_conflicts(1234) → three-way diff for A, B
per A: agent proposes merge → user confirms → resolve_config_conflict(A, …)
per B: agent proposes merge → user confirms → resolve_config_conflict(B, …)
merge_merge_request(1234) → merged → published
```

**Guarantee boundary (be honest):** MCP does not hard-guarantee the *wording* of the
dialogue — the model mediates it. Reliability comes from putting the decision in the
returned data (`action_required` / `next_step`) and in prescriptive tool descriptions. What
**is** guaranteed is *safety*: the backend never merges an unapproved or conflicted MR, so
the worst case is a structured refusal, never a wrong merge. Detailed decision trees (all
branches of X/Y, the resolve loop) are a follow-up on top of this status object.

### Data asymmetry (why resolution is on the branch)

Conflict resolution needs the branch-scoped `diff`/`rebase` endpoints and, ideally, full
context on the branch. A **production** session can *diagnose* a conflict (the conflicts
list is project-level) but cannot read the branch's config `diff`, other branch configs, or
branch table data (its SQL workspace is production). A **dev-branch** session has all of it.
Therefore resolution runs on a dev-branch session the user simply **opens fresh** (Kai UI:
enter the branch → new `X-Branch-Id` → new session state) — **not** a mid-session switch
(that would need workspace reprovisioning = Tier C).

### Response models

Lean, human-first Pydantic models:

- `MergeRequest` (summary): `id`, `title`, `description`, `state`, `branch_from_name`,
  `branch_into_name`, `creator_name`, `reviewers` (name + status), `auto_merge`,
  `created_at`, `merged_at`.
- `MergeRequestStatus`: the status object above (embedded in detail and in merge refusals).
- `MergeRequestDetail` = `MergeRequest` + `MergeRequestStatus` + `changelog_summary`
  (counts + short per-item list) + `activity_log` (typed events).
- `get_merge_requests` returns a **union**: `MergeRequestsListOutput` (summaries + links) or
  `MergeRequestsDetailOutput` (list of `MergeRequestDetail`), mirroring
  `GetConfigsListOutput` / `GetConfigsDetailOutput`.
- `ConflictDiff`: `component_id`, `configuration_id`, `name`, `is_deleted`, `base`, `ours`,
  `theirs` (returned by `get_merge_request_conflicts`).
- `MergeResult`: `merge_request_id`, `state`, `job_id`, `changelog_summary`, `next_step`.

## Resolution Strategy

1. **Storage client** (`src/keboola_mcp_server/clients/storage.py`): add thin methods over
   the base `get`/`post`/`put` helpers (`clients/base.py`), following the existing
   `branches_list` / `dev_branch_detail` pattern.
   - MR (project-level, bare `/merge-request…` paths — **never** branch-prefixed, matching
     `isAvailableInBranch: false`): `merge_requests_list`,
     `merge_request_detail(id, include=...)`, `merge_request_conflicts(id)`,
     `merge_request_create(payload)`, `merge_request_request_review(id)`,
     `merge_request_approve(id)`, `merge_request_reject(id, reason)`,
     `merge_request_merge(id)`.
   - Conflict (branch-scoped, use the session `branch_id`): `configuration_diff(component_id,
     configuration_id)`, `configuration_rebase(component_id, configuration_id, payload)`.
2. **Branch resolution — needs a new helper, not a plain reuse.**
   `_resolve_branch_context` (`src/keboola_mcp_server/tools/project.py:46`) returns
   `(branch_id, branch_name, is_development_branch)` for **one** branch — the session branch,
   or the default one when the session has no branch id. It does **not** hand back the default
   branch's id while on a dev branch, which is exactly what `create_merge_request` needs.
   Plan: add a small shared helper (e.g. `resolve_branch_pair`) that calls `branches_list`
   **once** and returns both the session branch (matching `client.branch_id`) and the default
   branch (`isDefault=True`) from that single response. Factor it next to
   `_resolve_branch_context` and let `get_project_info` keep using its existing helper (or
   refactor that one on top of the new lookup) — either way this is **new code**, sized
   accordingly. `create_merge_request` then derives `branchFromId` = session branch,
   `branchIntoId` = default branch, and errors when the session is on the default/production
   branch.
3. **Status object**: a helper builds `MergeRequestStatus` from the MR payload +
   `/conflicts` + the required-approvals count. `merge_merge_request` catches the backend
   409/not-ready and returns this status (enriched with the conflicts list) instead of a
   raw error.
4. **Async merge — the await loop must be written from scratch.** The merge endpoint returns
   a Storage Job, and **no reusable job-polling helper exists**: `tools/jobs.py` only has
   `run_job` (creates a job and returns immediately) and `get_jobs` (reads status). The
   polling loops in the codebase are not applicable — `workspace.py:182-203` polls
   *query-service* jobs via `_qsclient`, and `sql.py:55` is an HTTP-disconnect watcher.
   Plan: implement a small `_await_storage_job(job_id)` in the new module that polls
   `client.storage_client` job status until a terminal state, following the **pattern** of
   `workspace.py` (fixed interval + overall timeout + explicit terminal-state set), and
   surface a timeout as a status object saying the merge is still running (`in_merge`) rather
   than as a hard failure. Merge itself is atomic server-side (merge + publish in one
   transaction, rollback to `approved` on failure), so the tool only awaits the terminal
   state — it never has to compensate.
5. **Conflict resolution**: `get_merge_request_conflicts` fans out `1× /conflicts + N× /diff`
   concurrently. `resolve_config_conflict` posts one per-config rebase (full config + `rows`,
   tombstone for delete). The agent orchestrates the per-config loop and user confirmations;
   after resolving, a retried `merge` passes the live conflict check (no MR-level step).
   Approvals survive rebase, so no re-approval is inserted.
6. **Gating**: add `'branches-merge-requests'` to the `ProjectFeature` literal
   (`clients/storage.py:23`); extend `ToolsFilteringMiddleware.on_list_tools` /
   `on_call_tool` (`mcp.py`) to hide MR tools without the feature, hide write/approve/merge
   for non-`admin`/`share`, and hide the **branch-only** tools (create / submit_for_review /
   merge / conflict tools / publish_branch) when the session is on the default branch
   (`is_client_using_main_branch`, `mcp.py`), returning a handoff error on direct call.
7. **Docs & version**: regenerate `TOOLS.md` (`tox -e check-tools-docs`); minor version bump
   (from current `main`, `1.74.3 → 1.75.0` at the time of writing; new tools = minor); refresh
   `uv.lock`.

**Trade-offs called out:**

- *Explicit tools vs. one `action=` enum.* Explicit per-action tools give better LLM
  tool-selection and correct `readOnly`/`destructive` hints; the optional `publish_branch`
  keeps the common path to a single call.
- *Merge kept branch-only by MCP even though the API allows root.* Keeps the whole promotion
  flow inside the branch and removes the "merge any MR from production" surface; worth
  aligning with the Connection team if they later tighten the endpoint too.
- *Conflict resolution on the branch, not production.* Avoids Tier C mid-session switching
  and gives the agent full context (diff + branch data). Cost: a reviewer on production must
  open a branch session to resolve — an explicit, guided handoff.
- *No branch create/switch.* Would require mutating session state and reprovisioning the SQL
  workspace (branch-bound at session creation, `mcp.py:267-311`). Deferred (Tier C), and
  with it `get_branches` and `cancel_merge_request`.

## Scope

**In scope:**

- New module `tools/merge_requests.py` with the tools above (MVP core + `approve`/`reject`;
  `publish_branch` optional).
- Storage-client wrappers for the MR endpoints and the branch-scoped `diff`/`rebase`.
- Three-axis gating (feature / role / session-branch) and the handoff error from production.
- The status-object helper, changelog humanization helper, and lean response models.
- `TOOLS.md` regeneration, minor version bump, `uv.lock` refresh.

**Out of scope:**

- SOX flow (`protected-default-branch`). Tools must not misbehave there, but full SOX UX is
  a separate RFC.
- Creating, switching, or deleting branches; any mid-session branch context change or SQL
  workspace reprovisioning. With it: `get_branches`, and `cancel_merge_request` (cancel =
  branch deletion).
- `update_merge_request` (editing MR metadata) — low value for the chat audience for now.
- Detailed conversational decision trees (all X/Y branches, the full resolve loop) — a
  follow-up built on the status object.
- Auto-merge scheduling UX beyond passing `autoMergeStrategy`/`autoMergeAt` through create.
- CLI and kbagent-specific wiring (sibling milestones).

## Testing / Verification

- **Unit tests** (`tests/tools/test_merge_requests.py`), parametrized:
  - `create_merge_request` branch defaulting (session dev branch → default); error on
    production. The new `resolve_branch_pair` helper: returns both branches from a single
    `branches_list` call, on a dev-branch session and on a default-branch session.
  - **MR-id convention**, parametrized over the optional-id tools: id **omitted** → resolves
    the session branch's open MR; id **given and matching** → acts on it; id **given but
    belonging to another branch** → handoff error, no call made; no open MR on the branch →
    clear error.
  - `_await_storage_job`: terminal success, terminal failure (rollback → `approved`), and
    timeout → status object reporting `in_merge` rather than raising.
  - status object / decision table: `mergeable` happy path; `blocked_reason=needs_approval`
    with `given_by`/`missing_from`; `blocked_reason=conflicts` with the conflicts list;
    `merge_merge_request` returns the status (not a raw error) on a 409.
  - conflict resolution: `get_merge_request_conflicts` fans out diffs; `resolve_config_conflict`
    posts a per-config rebase incl. the delete/tombstone case; a resolved MR then merges
    (live conflict check clears); approvals survive rebase.
  - changelog humanization: empty, mixed add/modify/delete, truncation.
  - gating: tools hidden without the feature; write/merge hidden for `developer`/`reviewer`;
    branch-only tools error / hidden on a production session; visible on a dev-branch session.
- **Integration tests** (`integtests/tools/test_merge_requests.py`) against a
  `branches-merge-requests` project: on a dev-branch session, make a config change,
  `create_merge_request` → `submit_merge_request_for_review` → `merge_merge_request`, assert
  `published` + changelog. A conflict scenario: change the same config in production, assert
  a conflict surfaces, `resolve_config_conflict`, then merge succeeds. Clean up branches.
- **Manual E2E** via local `.mcp.json` (per CLAUDE.md) on a non-SOX branch project: run the
  X and Y chat flows from a dev-branch session; confirm plain-language status and the guided
  conflict resolution.
- **`tox`** (pytest, black, flake8, check-tools-docs) green before pushing.
