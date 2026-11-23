# RFC: Merge Request tools for Development Branches (non-SOX)

Linear: [DMD-1701](https://linear.app/keboola/issue/DMD-1701) (design and implementation, PR #699),
[DMD-2246](https://linear.app/keboola/issue/DMD-2246) (this as-shipped revision)

> **Status: as shipped (v1.114+).** This document describes the merge-request tools as they are
> implemented on `main`. The original design was agreed on the closed RFC branch
> `ms/merge-requests/dmd-1701` (last commit `fa86e3e6`, PR #612). The implementation deviated from it
> in several places during review and the live test. Those deviations are listed in
> [Deviations from the original RFC](#deviations-from-the-original-rfc). When this document and the code
> disagree, the code wins and this document has a bug.

## Problem

Keboola's "Branches 2.0" gives users a Git-like workflow: they work in an isolated development branch,
then promote the changes to production through a **merge request** (MR), with optional review, approval
and conflict resolution. The Connection backend (`branches-merge-requests` project feature) and the Keboola
UI implement the non-SOX flow. Without MCP tools, a user chatting with Kai inside a development branch
cannot say *"ship these changes to production"* and cannot be guided through a conflict when production
has moved on.

Scope is the **non-SOX** flow only. The SOX flow (`protected-default-branch`) is out of scope.

## Required Behavior

### Toolset

Nine tools in `src/keboola_mcp_server/tools/merge_requests/` (`tools.py`, `status.py`, `conflicts.py`,
`models.py`), tagged `merge-request` and registered by `add_merge_request_tools`.

| Tool | Annotations (`readOnly` / `destructive` / `idempotent`) | `project_id` | Where it works | What it does |
|---|---|---|---|---|
| `get_merge_requests` | ✓ / – / ✓ | yes | any session | No ids: summaries of all MRs, optional `state` filter (raw or derived state). With ids: per-MR detail with the status block, changed configurations, activity log and live conflicts. |
| `create_merge_request` | – / – / – | no | development branch only | Creates the MR for the session branch into production. No branch parameters. A second MR on the same branch is refused and the existing one is reported with its `next_step`. |
| `update_merge_request` | – / ✓ / ✓ | yes | any session | Sets title, description, reviewers and auto-merge. Omitted fields are kept. `auto_merge='none'` is the only way to disarm auto-merge; `auto_merge_at` requires `auto_merge='scheduled'`. |
| `request_merge_request_review` | – / – / – | no | session-bound (see below) | `development → in_review`, or straight to `approved` when 0 approvals are required. Rarely needed. |
| `approve_merge_request` | – / – / – | yes | any session | Adds an approval. Valid only in `in_review`, never for the creator (the backend enforces both). |
| `request_merge_request_changes` | – / – / – | yes | any session | The reviewer's "reject": back to `development`, approvals reset, optional `reason`. Not terminal. |
| `merge_merge_request` | – / ✓ / – | no | session-bound | Merges and waits for the Storage job (up to `MERGE_JOB_TIMEOUT_SEC` = 600 s). On a refusal it returns a `MergeResult` with `refusal` and the status instead of an error. |
| `get_merge_request_conflicts` | ✓ / – / ✓ | no | session-bound | Live conflict list plus the three-way diff of each conflicting configuration, classified per path. |
| `resolve_merge_request_conflict` | – / ✓ / – | no | session-bound | Re-anchors one configuration onto the current production version (`/rebase`) with the chosen content. |

Annotation rules (`add_merge_request_tools`): creating an object is not destructive; replacing or deleting
existing content is. `resolve_merge_request_conflict` replaces a configuration version and can delete it.
Only `update_merge_request` is idempotent because it sets absolute values. The state transitions and the
merge are events, and a client must not retry them.

`project_id` is declared only on the tools that address an MR purely by id. The session-bound tools
(`MERGE_REQUEST_SESSION_BOUND_TOOLS` in `tools/constants.py`) declare none: the session's branch is their
implicit argument, so the multi-project middleware always routes them to the active project.

### Gating

Three axes, applied in `ToolsFilteringMiddleware` (`mcp.py`, `on_list_tools` and `authorize_tool_call`):

| Axis | Rule |
|---|---|
| Feature | Every MR tool is hidden and refused without the `branches-merge-requests` project feature. |
| Role | Reads are open to every role. Writes need token role `admin` or `share`, **or any OAuth/bearer session** (the same carve-out the flow tools have). Role `readonly` sees only the read tools. |
| Session branch | `create_merge_request` is the only branch-only tool (`MERGE_REQUEST_BRANCH_ONLY_TOOLS`). It stays visible on production and is refused at call time with a handoff message. |

The **MR-id convention** for the session-bound tools (`_resolve_branch_mr` in `tools.py`):

| Session | `merge_request_id` omitted | `merge_request_id` given |
|---|---|---|
| Development branch | The branch's single open MR. If none, an error, or a handoff to production when the MR is already merged and the branch is being deleted. | Must belong to the session branch. Otherwise a handoff error naming the MR's branch with its UI link. |
| Production | Error listing the open MRs. The tool never picks one itself. | Any MR whose source branch still exists. |

Production works with an explicit id because kbc-ui makes a branch read-only (and so switches Kai off
there) while its MR is `in_review` or `approved`, which is exactly when merge and conflict resolution are
needed. The kbagent CLI allows the same.

### The status block

Every tool except the list mode of `get_merge_requests` returns a `DerivedStatus` (`models.py`), built by
`build_status` in `status.py`:

| Field | Meaning |
|---|---|
| `state` | Raw API state: `development`, `in_review`, `approved`, `in_merge`, `published`, `canceled`. Typed `str`: an unknown value never fails a response. |
| `derived_state` | What the UI badge shows: `in_development`, `in_review`, `approved`, `in_merge`, `merged`, `closed`, `rejected`, or `unknown` for an unrecognized raw state. |
| `merge_blockers` | Everything currently blocking a merge: `conflicts`, `approvals`, `state` (plus any kind the backend serializes). Informational: the backend's merge check is the authority. |
| `mergeable` | `True` only when nothing blocks and conflicts were checked. `None` when conflicts were not fetched. `False` right after a backend refusal. |
| `allowed_actions` | Actions the state machine permits (state only): `request_review`, `approve`, `request_changes`, `merge`, `update`, `resolve_conflicts`. |
| `viewer` | `is_creator`, `has_approved`. Both `None` when the caller's admin id is unknown (OAuth or a scoped token). |
| `approved_by`, `pending_reviewers` | Reviewer names. |
| `conflicts` | The live conflict list, or `None` when not fetched. |
| `next_step` | One sentence: the single recommended next action (below). |

Derivation rules (`status.py`):

- `derive_state`: `rejected` = `development` plus a rejection by a non-creator reviewer. `closed` =
  `canceled`, or `development` plus the creator's own rejection (the UI "cancel" trick). Otherwise the raw
  state is mapped. An unknown raw state gives `unknown`.
- `derive_merge_blockers`: `conflicts` when the live list is non-empty; `approvals` when `state ==
  in_review`; `state` for `in_merge`, `published`, `canceled` **and any unknown state**, so the answer is
  "no" whenever the server is unsure, never "mergeable".
- **Server-first:** each derivation prefers the field Connection will serialize
  ([DMD-1988](https://linear.app/keboola/issue/DMD-1988)): `derivedState`, `mergeBlockers`,
  `allowedActions`, `viewer`. The local tables are fallbacks to delete once the backend ships the fields.
  The derivation is a port of the kbagent CLI's tables, so the two clients agree.

### `next_step` — the deterministic guide

`build_next_step` (`status.py`) is a decision table. The first matching row wins. Rows are evaluated with
the session context (`SessionContext`: on the MR's branch / on production / another development branch,
and whether the caller can write).

| # | Condition | Guidance |
|---|---|---|
| 0 | `derived_state == unknown` | Do not act; check the MR in the Keboola UI. |
| 1 | `merged` | Done. From the MR's branch chat: the chat is pinned to a branch being deleted, continue in production (link). |
| 2 | `closed` | Nothing to do. |
| 3 | `in_merge` | A merge is running: do not edit the branch, do not merge again. |
| 4 | `remaining_conflicts` non-empty (after a resolve) | Resolve the next conflict (*n* left). |
| 5 | Conflicts block, caller not on the MR's branch | From production: call the conflict tools with `merge_request_id`. From another branch: hand off to the MR's branch (link) or production. |
| 6 | Conflicts block, caller on the MR's branch | Call `get_merge_request_conflicts`, resolve one by one. |
| 7 | Approvals block, caller already approved | Wait for the other reviewers. |
| 8 | Approvals block, caller is the creator | Waiting for approval; you cannot approve your own MR. |
| 9 | Approvals block, caller can write | Approve or request changes. |
| 10 | Approvals block, caller cannot write | Waiting for a reviewer's approval. |
| 11 | The backend just refused with `not_ready` | In `development`: the project requires approvals, request a review. Otherwise a merge lock or another MR in progress: wait, then merge again. |
| 12 | `rejected` | Changes were requested (with the reason from the activity log); fix the branch, then merge again. |
| 13 | `merge` allowed, caller not on the MR's branch | From production: merge here with `merge_request_id` (irreversible). From another branch: hand off. |
| 14 | `merge` allowed, caller on the MR's branch and can write | Ready: merge (irreversible, the branch is deleted afterwards). |
| 15 | otherwise | Show the MR (read-only). |

Rows 6 and 14 append a note when the MR is `in_review` or `approved`: the branch is read-only in the UI,
so if Kai becomes unavailable there, continue from production with `merge_request_id`.

### Merge

`merge_merge_request` (`tools.py`):

- The merge `PUT` is sent **without the client retry policy** (`retry=False`): repeating it after a
  timeout would turn a started merge into a misleading "already running" refusal.
- 409 `notReadyToMerge` → `refusal='not_ready'`. The MR is then re-read, and if it is `in_merge` the
  result reports a running merge, not a refusal.
- Any other 409 → `refusal='conflicts'` with the conflict list from the error body, or from the live
  `/conflicts` call when the body has none. An empty live list downgrades the refusal to `not_ready`.
- A transport error or 5xx after the request was sent → the MR is re-read. `in_merge` reports a running
  merge, `published` a success; anything else is an error that tells the agent not to merge again before
  checking.
- The job is polled with a back-off of 1, 2, 5 then 20 s, up to 600 s. When it is still running, the result
  says `state='in_merge'` with "do not merge again". A failed job re-reads the MR, because the backend
  rolls a failed merge back to `approved`.
- On success, `source_branch_deleting=True`. From the branch's own chat, `next_step` hands the user off to
  production.

### Conflict resolution

`get_merge_request_conflicts` makes one `/conflicts` call plus one branch-scoped `/diff` per conflicting
configuration, concurrently. `conflicts.py` classifies each diff per JSON-pointer path:

- `changes`: each changed path with `changed_by` = `ours` (branch) | `theirs` (production) | `both`,
  `agreed` on `both` rows, and the `base` / `ours` / `theirs` values. Lists of objects with unique `id`s are
  aligned by id, other lists by index.
- `conflicting_paths`: `both` rows that are not agreed, plus ancestor/descendant collisions between a
  one-sided `ours` path and a one-sided `theirs` path.
- `suggested_take`: `ours` or `theirs` when only that side changed anything, otherwise `None`.
- An absent, deleted or key-incomplete side produces no rows (`changes=[]`) and a warning.

Known limitation: id-vs-index keying is decided per pair (base→ours, base→theirs). When one side breaks
the id rule (an item without an id, a duplicate id), a real clash can come back with
`conflicting_paths=[]`. The configuration is still in the live conflict list and still blocks the merge.
The cases are pinned in `test_conflicts.py::test_list_keying_across_sides`.

`resolve_merge_request_conflict` resolves **one** configuration. Exactly one of `take` or `resolved` is
required:

| Mode | Input | Body sent to `/rebase` |
|---|---|---|
| `ours` / `theirs` | `take='ours'` / `'theirs'` | That side's `name`, `description`, `isDisabled`, `configuration`, `rows`. Refused when the side is absent, has an envelope hole, a blank name or a non-boolean `isDisabled`. Taking a deleted side is the delete mode. |
| `delete` | `take='delete'` | Empty body (tombstone). `change_description` is ignored with a warning. |
| `custom` | `resolved` | The caller's full content. `resolved` is typed `dict[str, Any]` at the tool boundary and validated strictly into `ResolvedConfiguration`: `name` (non-empty), `description` (nullable but required), `is_disabled` (strict bool), `configuration`, `rows` (complete; `[]` deletes all rows). |

The target must be in the MR's live conflict set. The rebase goes to the production (`theirs`) version
from the diff. The result carries `remaining_conflicts` and the status. Approvals survive the rebase.

## Resolution Strategy

- Storage client (`clients/storage.py`): `merge_requests_list`, `merge_request_detail`,
  `merge_request_conflicts`, `merge_request_create`, `merge_request_update`,
  `merge_request_request_review`, `merge_request_approve`, `merge_request_request_changes`,
  `merge_request_merge` (`retry=False`), and the branch-scoped `configuration_diff` and
  `configuration_rebase`.
- Pure logic without I/O lives in `status.py` (status and `next_step`) and `conflicts.py` (classification),
  so both can be tested as decision tables.
- Write errors are mapped to plain messages (`_mapped_write_errors`): a backend 403 to a role message, a
  422 ("the marking does not enable the transition") to a state-machine message, both keeping the
  backend's text. Other statuses pass through.

## Scope

In scope: the nine tools above, the three-axis gating, the status block and `next_step`, merge-job
awaiting, per-configuration conflict resolution, the `update_merge_request` metadata and auto-merge
control.

Out of scope: the SOX flow; creating, switching or deleting branches; `cancel_merge_request` (no
endpoint: cancel means deleting the branch); a one-shot create→merge orchestrator (it would hide the
review gate).

Open follow-ups (DMD-2246): the OAuth carve-out on MR writes needs a live test of whether an OAuth
session's `verify_token` carries `admin.role`; the conflict-keying limitation above; MR integration tests
in CI ([DMD-2677](https://linear.app/keboola/issue/DMD-2677)); deleting the local derivations once
DMD-1988 ships.

## Testing / Verification

- Unit tests in `tests/tools/merge_requests/`: `test_status.py` (derivation tables, server-first
  overrides, every `next_step` row), `test_conflicts.py` (classification, keying, envelope holes),
  `test_tools.py` (tool flows, the MR-id convention, merge refusals, timeouts and re-reads, resolve modes).
- Integration tests in `integtests/tools/test_merge_requests.py` against a project with
  `branches-merge-requests` (needs `INTEGTEST_STORAGE_TOKEN_MERGE_REQUESTS`).
- Live E2E on 2026-10-14 (`dev-keboola-gcp-us-central1`, project 161): integration tests green, all four
  MCP scenarios through the deployed `mcp-server-agent`, non-admin and read-only gating verified (report in
  [DMD-1904](https://linear.app/keboola/issue/DMD-1904)).

## Deviations from the original RFC

| Original RFC | As shipped | Why |
|---|---|---|
| 8 tools; `update_merge_request` dropped | 9 tools, `update_merge_request` added | Disarming auto-merge and changing reviewers are needed in chat; arming auto-merge has no other control. |
| `submit_merge_request_for_review`, `reject_merge_request`, `resolve_config_conflict` | `request_merge_request_review`, `request_merge_request_changes`, `resolve_merge_request_conflict` Renamed during review of #699; "reject" is not terminal (the MR returns to `development`), which "request changes" states directly. |
| Single module `tools/merge_requests.py` | Package `tools/merge_requests/` (`tools`, `status`, `conflicts`, `models`) | Pure decision logic separated from I/O for table tests. |
| Status: `mergeable`, `blocked_reason`, `action_required`; precedence state → conflicts → approvals; required approvals read from project metadata | `derived_state`, `merge_blockers` (a list, so blockers do not mask each other), `allowed_actions`, `viewer`, and a 15-row `next_step` table; no required-approvals lookup (the `in_review` state carries it) | Mirrors the kbagent CLI and the planned backend fields (DMD-1988), so the clients cannot disagree and the polyfill can be deleted later. |
| Merge, request-review and the conflict tools are branch-only | Only `create_merge_request` is branch-only; the others also work from production with an explicit `merge_request_id` | kbc-ui switches Kai off on a branch whose MR is `in_review` or `approved`, exactly when merge and conflicts are needed. |
| Role gate: `admin` / `share` only | `admin` / `share` **or any OAuth session** | Same carve-out as the flow tools. Whether an OAuth session's `verify_token` carries `admin.role` is not verified yet; a live test is open in DMD-2246. |
| `create_merge_request` and `resolve_config_conflict` plain writes; only merge `destructiveHint` | `resolve_merge_request_conflict` and `update_merge_request` are `destructiveHint=True`; create stays non-destructive | Repo convention: replacing or deleting existing content is destructive. |
| `resolve_config_conflict` with typed full-content parameters and a `delete` flag | `take` (`ours` / `theirs` / `delete`) or `resolved: dict`, validated strictly | Most resolutions take one side; `take` removes the need to copy content back through the model. |
| Per-config diff returned raw | Per-path classification (`changes`, `conflicting_paths`, `suggested_take`) | The agent needs to know what actually clashes, not only three blobs. |
| No `project_id` discussion | `project_id` on the four tools addressed purely by id | Multi-project sessions; the session-bound tools always target the active project. |
| Merge `PUT` under the default retry policy | `retry=False` and a re-read after an unconfirmed outcome | A retried merge after a timeout reported a running merge as refused. |
| Happy path create → submit → merge | create → merge (0 required approvals skips review) | The backend allows merging from `development` when no approvals are required. |
