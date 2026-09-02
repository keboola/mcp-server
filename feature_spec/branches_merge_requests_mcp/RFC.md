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
- **Roles (non-SOX): every write is `admin` / `share` only.** Each controller carries two
  whitelists and `StorageRouteGuard` picks one by project feature, then does a strict
  membership test (`canAccessInMergeRequestsProject`:
  `in_array($role, $scope->mergeRequestsAllowedRoles)`):
  - `#[MergeRequestsAllowedRoles]` — the whitelist for `branches-merge-requests` (our flow).
    It is `[ADMIN, SHARE]` on **all** of create, request-review, approve, request-changes,
    and merge.
  - `#[ProtectedBranchAllowedRoles]` — the whitelist for the **SOX** flow only, and that is
    where `REVIEWER` / `DEVELOPER` / `PRODUCTION_MANAGER` appear. Those roles carry **no**
    MR privileges in a non-SOX project.
  - Reads (list, detail, conflicts) are `#[AsReadOnlyAction]` with no role whitelist —
    readable by any member.

  *Consequence worth naming:* in a non-SOX project a user with the `developer` or `reviewer`
  role cannot create or advance an MR at all — the persona this flow serves must hold `admin`
  or `share`. This is the backend's rule, not our restriction; a tool that offered `create`
  to a developer would only produce a 403.
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
3. **Speak human.** Resolve branch/user **IDs → names**. Pass structured data (e.g. the
   `changeLog` list) straight through and let the agent narrate it, rather than inventing
   aggregates the API cannot support.
4. **Gate by feature, role, and session context**, reusing the existing
   `ToolsFilteringMiddleware`, so users never see tools they cannot use.

### The gating model (three axes)

| Axis | Rule |
|---|---|
| **Feature** | All MR tools require the `branches-merge-requests` project feature. |
| **Role** | Reads are open to any member; **every write** (create, submit_for_review, approve, reject, merge, conflict rebase) requires `admin` or `share`. This mirrors `#[MergeRequestsAllowedRoles]`, which is `[ADMIN, SHARE]` on every non-SOX MR write — see *Verified behaviors*. Deliberately **not** split into a broader create/submit tier: `reviewer`/`developer` appear only in the SOX whitelist, so widening it here would just surface tools that 403. |
| **Session context** | **Read + review-state** tools work from *any* session (production or branch). **Content / promotion** tools (`create`, `submit_for_review`, `merge`, conflict tools) work **only from a dev-branch session**; called from production they return a clear handoff error (*"open a session on branch '<name>' and do it there"*). |

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
| `get_merge_requests` | `readOnlyHint` | No IDs → list MRs (summaries; optional `state` filter). With IDs → detail per MR: the **status object** (below) + the changed-configurations list + activity-log timeline + the conflicts **list** (which configs conflict — cheap, from `/conflicts`; not the diff content). IDs → names. |

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

One exception to note: the rebase endpoint is addressed by **component + config on the
session branch** and takes no MR id, so `resolve_config_conflict`'s `merge_request_id` is
**never sent to the backend** — it is used only as a guard (confirm the target really is in
that MR's conflict set) so an agent cannot overwrite an unrelated configuration.

That is the whole toolset — **8 tools, no orchestrator.** The author's happy path stays
three explicit calls (`create_merge_request` → `submit_merge_request_for_review` →
`merge_merge_request`), which an agent chains trivially.

**MVP core** (makes the X/Y chat flows below + conflict resolution work): `get_merge_requests`,
`create_merge_request`, `submit_merge_request_for_review`, `merge_merge_request`,
`get_merge_request_conflicts`, `resolve_config_conflict`. `approve` / `reject` earn their
keep on projects with required approvals > 0.

**Dropped / deferred:** `update_merge_request` (low value for the chat audience — omitted
for now); `cancel_merge_request` (no endpoint — cancel = branch deletion = Tier C);
`get_branches` (deferred to Tier C branch management, where it first gains a use);
`publish_branch`, a one-shot create→review→merge orchestrator, **dropped** — it would
collapse the review gate (creating *and* merging in one call means the user never sees the
changelog before production), it duplicates `merge_merge_request` as a second destructive
merge path, and because it must stop at every interesting case (conflicts, missing
approvals) and return the same status object anyway, it only orchestrated the trivial one.

### The status object (drives the agent's guidance)

To make the conversational flows reliable, `approve_merge_request`,
`reject_merge_request`, `submit_merge_request_for_review` and `get_merge_requests` detail all
return the same explicit `MergeRequestStatus` (fields defined under *Result models* below) so
the agent branches on data, not on its own guesswork. Its `approvals` breakdown maps directly
to the API's `reviewers:[{name,status}]` + `approvals`. This lets the agent produce, e.g.:

- *"MR #1234 is approved and conflict-free — merge?"* (`mergeable=true`)
- *"MR #1234 can't merge yet — conflicts in A, B. Resolve now?"* (`blocked_reason=conflicts`)
- *"You're missing approval from X, Y; Z already approved."* (`blocked_reason=needs_approval`)

**Derivation (normative).** This is our own logic — no backend field corresponds to it — so it
is specified here and unit-tested as a decision table. Inputs: the MR `state`, the conflicts
list, and `approvals.given` vs `approvals.required`.

| `state` | conflicts | approvals | `mergeable` | `blocked_reason` | `action_required` |
|---|---|---|---|---|---|
| `development` | — | — | `false` | `not_ready` | `submit_for_review` |
| `in_review` | non-empty | any | `false` | `conflicts` | `resolve_conflicts` |
| `in_review` | `[]` | `given < required` | `false` | `needs_approval` | `wait_for_approval` |
| `approved` | non-empty | — | `false` | `conflicts` | `resolve_conflicts` |
| `approved` | `[]` | — | **`true`** | `none` | `merge` |
| `in_merge` | — | — | `false` | `not_ready` | `wait_for_merge` |
| `published` | — | — | `false` | `none` | `none` |
| `canceled` | — | — | `false` | `not_ready` | `none` |

Rules that the table encodes:

- **Precedence is state → conflicts → approvals.** When an `in_review` MR is both short of
  approvals *and* conflicted, we report `conflicts`, because that is the part the current user
  can act on immediately; waiting for someone else's approval is not actionable.
- **`not_ready`** means the *lifecycle position* blocks the merge (`development`, `in_merge`,
  `canceled`) — as opposed to a specific fixable obstacle. It is never used for conflicts or
  approvals.
- **`wait_for_merge`** means a merge job is already running for this MR (`in_merge`); the
  correct behavior is to wait for it, never to start a second merge.
- `mergeable = (state == 'approved' and not conflicts)`. It is deliberately **not** true for
  `published` — there is nothing left to merge; `blocked_reason` is `none` because nothing is
  wrong.
- `in_review` + `[]` conflicts + enough approvals is absent by construction: the backend
  auto-transitions to `approved` once the required count is met.
- **`required` is read per project** (`KBC.branches-merge-requests.required-approvals-count`,
  default 0), never assumed to be 0.

### Chat flows the tools must support

**X — "Is there anything to merge?"** → two steps, because list mode returns summaries
without status: `get_merge_requests(state='approved')` to find candidates, then
`get_merge_requests(merge_request_ids=[...])` whose `status.mergeable` /
`status.conflicts` let the agent report what is ready and what is blocked.
(Keeping the conflicts probe out of list mode is deliberate — it would cost one extra
`/conflicts` call per MR on every listing.)

Both steps work from **any** session, but the follow-up action does not: merging and resolving
are branch-only. So from a **production** session flow X ends in a handoff — *"MR #1234 is
ready; open a session on branch 'x' to merge it"* — and only from a **dev-branch** session can
the agent go on to actually merge or start the resolve loop.

**Y — "Merge #1234."** → `merge_merge_request(1234)`; on
`status.blocked_reason == 'conflicts'` the agent fetches `get_merge_request_conflicts`, walks
the user through `resolve_config_conflict` per config (propose a merge from
`base`/`ours`/`theirs`, user confirms, rebase), then retries merge:

```
merge_merge_request(1234)
  -> MergeResult(merged=False, status=MergeRequestStatus(
         blocked_reason='conflicts', action_required='resolve_conflicts',
         conflicts=[A, B], next_step=...))
get_merge_request_conflicts(1234)
  -> MergeRequestConflictsOutput(conflicts=[ConfigDiff(A, base/ours/theirs),
                                            ConfigDiff(B, base/ours/theirs)])
per A: agent proposes a merge -> user confirms -> resolve_config_conflict(A, ...)
         -> ResolveConflictResult(resolved=True, remaining_conflicts=[B])
per B: agent proposes a merge -> user confirms -> resolve_config_conflict(B, ...)
         -> ResolveConflictResult(resolved=True, remaining_conflicts=[])
merge_merge_request(1234)
  -> MergeResult(merged=True, state='published', job_id=...)
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

### Tool signatures

Normative contract. Descriptions/`Annotated[..., Field(...)]` metadata are omitted here for
readability; the shapes and defaults are the spec. `ctx: Context` is the usual first
parameter on every tool.

```python
# ---- Read / diagnosis — any session -------------------------------------------------
async def get_merge_requests(
    merge_request_ids: Sequence[int] = (),          # empty -> list all; IDs -> batched details
    state: MergeRequestState | None = None,         # list mode only; ignored when IDs given
                                                    # filtered CLIENT-side (GET /merge-request takes no params)
) -> MergeRequestsListOutput | MergeRequestsDetailOutput: ...

# ---- Review-state actions — any session, admin/share --------------------------------
async def approve_merge_request(merge_request_id: int) -> MergeRequestStatus: ...

async def reject_merge_request(
    merge_request_id: int,
    reason: str | None = None,                      # stored in the activity log
) -> MergeRequestStatus: ...

# ---- Author / promotion — dev-branch session only, admin/share ----------------------
async def create_merge_request(
    title: str,
    description: str | None = None,
    reviewer_ids: Sequence[int] = (),
    auto_merge: AutoMergeStrategy = 'none',         # 'none' is a real API value, sent as-is
    auto_merge_at: str | None = None,               # ISO-8601; required iff auto_merge='scheduled'
) -> MergeRequestDetail: ...                        # costs +1 /conflicts call — see Resolution Strategy §2

async def submit_merge_request_for_review(
    merge_request_id: int | None = None,            # MR-id convention
) -> MergeRequestStatus: ...

async def merge_merge_request(
    merge_request_id: int | None = None,            # MR-id convention
) -> MergeResult: ...                               # awaits the job; refusal -> MergeResult.status

# ---- Conflict resolution — dev-branch session only, admin/share ---------------------
async def get_merge_request_conflicts(
    merge_request_id: int | None = None,            # MR-id convention
) -> MergeRequestConflictsOutput: ...               # conflict list + three-way diff per config

async def resolve_config_conflict(
    component_id: str,
    configuration_id: str,
    version: int,                                   # REQUIRED: default-branch version to rebase onto
    configuration: JsonDict | None = None,          # content fields: all required unless delete=True
    rows: Sequence[JsonDict] | None = None,         # complete resolved row set; [] clears all rows
    name: str | None = None,
    description: str | None = None,
    change_description: str | None = None,
    is_disabled: bool | None = None,                # None = "not sent" (must be distinguishable)
    delete: bool = False,                           # omit all content fields -> tombstone
    merge_request_id: int | None = None,            # guard only, never sent to the endpoint
) -> ResolveConflictResult: ...
```

Argument rules for `resolve_config_conflict` — these mirror `RebaseRequest`
(`connection/src/Storage/ComponentConfigurations/Rebase/Request/RebaseRequest.php`) exactly:

- **`version` is always required** — it is the *default-branch* configuration version the dev
  branch is re-anchored onto. The agent takes it from the diff's `theirs.version`. The backend
  rejects a target version that is not newer
  (`ConfigurationRebaseTargetVersionNotNewerException` → 400).
- **`theirs` is guaranteed present for a *conflicting* config,** so `theirs.version` is always
  available in the resolve loop: `DefaultConflictValidator` only reports a conflict when the
  configuration exists on **both** sides (a config present only in the default branch is
  explicitly *not* a conflict), and a config deleted in the default branch still has a version
  — it surfaces as `theirs.is_deleted = true`, not as `theirs = None`. The `None` sides in
  `ConfigDiff` therefore occur only when diffing a **non**-conflicting configuration. If
  `theirs` is nonetheless `None`, the tool must fail with an explicit message rather than
  guessing a version.
- **`is_disabled` must be tri-state** (`bool | None`). The content-field set is detected by
  presence, so a plain `bool` default of `False` would be indistinguishable from a caller
  explicitly sending `false` — breaking both the partial-payload check and the tombstone case.
- The rebase payload is a **full replacement, not a patch.** With `delete=False` the content
  fields (`configuration`, `rows`, `name`, `description`, `change_description`,
  `is_disabled`) are sent as the resolved result; `rows` is the complete row set, its array
  order becomes the row sort order, and `rows=[]` deletes all rows. The tool rejects a partial
  payload client-side rather than letting the agent silently drop fields.
- `delete=True`: send **only** `version`, omitting every content field — the backend then
  resolves the conflict by deleting the configuration (the new head version is a tombstone).
- Called **once per conflicting configuration** — there is no batch rebase.

### Result models

Lean, human-first Pydantic models. `Link` is the existing `keboola_mcp_server.links.Link`.

```python
MergeRequestState  = Literal['development', 'in_review', 'approved', 'in_merge', 'published', 'canceled']
AutoMergeStrategy  = Literal['none', 'immediately', 'scheduled']
ReviewerStatus     = Literal['approved', 'rejected', 'pending']
BlockedReason      = Literal['none', 'needs_approval', 'conflicts', 'not_ready']
ActionRequired     = Literal['none', 'submit_for_review', 'wait_for_approval',
                             'resolve_conflicts', 'merge', 'wait_for_merge']
ActivityEventType  = Literal['review_requested', 'approved', 'changes_requested',
                             'merged', 'canceled']

class Reviewer(BaseModel):
    id: int
    name: str
    status: ReviewerStatus                  # 'pending' when the API reports null

class Approvals(BaseModel):
    required: int                           # 0 by default on non-SOX
    given: int
    given_by: list[str]                     # names that already approved
    missing_from: list[str]                 # named reviewers yet to approve; [] when none are named

class ConflictRef(BaseModel):               # exactly what GET /merge-request/{id}/conflicts returns
    component_id: str
    configuration_id: str
    message: str                            # backend's human-readable conflict description — reuse it
    is_deleted: bool
    dev_branch_version_identifier: str
    default_branch_version_identifier: str
    # NOTE: no configuration *name* — the conflicts endpoint does not return one. Resolving a
    # name here would cost one extra config lookup per conflict, so we deliberately don't:
    # `message` already reads well, and ConfigDiff below carries the name for free.

class MergeRequestStatus(BaseModel):        # the decision object the agent branches on
    state: MergeRequestState
    is_approved: bool
    mergeable: bool
    approvals: Approvals
    conflicts: list[ConflictRef]
    blocked_reason: BlockedReason
    action_required: ActionRequired
    next_step: str                          # one human-readable sentence

class ChangedConfig(BaseModel):             # one entry of changeLog['configurations']
    component_id: str
    configuration_id: str
    is_deleted: bool
    # `lastVersionIdentifier` is dropped — an internal hash with no meaning in a chat.

class ActivityEvent(BaseModel):
    event_type: ActivityEventType
    admin_name: str | None                  # None for system events (e.g. auto-merge)
    note: str | None                        # e.g. the reject reason
    created_at: str

class MergeRequest(BaseModel):              # summary
    id: int
    title: str
    description: str | None
    state: MergeRequestState
    branch_from_id: int
    branch_from_name: str
    branch_into_name: str
    creator_name: str
    reviewers: list[Reviewer]
    auto_merge: AutoMergeStrategy
    auto_merge_at: str | None
    created_at: str
    merged_at: str | None
    merged_by: str | None                   # `merge.mergerName`; set for system merges too
    links: list[Link]

class MergeRequestDetail(MergeRequest):
    status: MergeRequestStatus
    changed_configurations: list[ChangedConfig]   # from changeLog; [] while state='development'
    activity_log: list[ActivityEvent]

class MergeRequestsListOutput(BaseModel):
    merge_requests: list[MergeRequest]
    links: list[Link]

class MergeRequestsDetailOutput(BaseModel):
    merge_requests: list[MergeRequestDetail]

class ConfigVersionSnapshot(BaseModel):     # = ConfigurationVersionResponse
    version: int                            # `theirs.version` is what resolve_config_conflict needs
    is_deleted: bool
    name: str | None                        # from the nested `diff` payload (ConfigurationDiffData)
    description: str | None
    change_description: str | None
    is_disabled: bool
    configuration: JsonDict
    rows: list[JsonDict]

class ConfigDiff(ConflictRef):              # one conflicting config + its three-way diff
    base: ConfigVersionSnapshot | None      # dev branch version 1
    ours: ConfigVersionSnapshot | None      # dev branch head
    theirs: ConfigVersionSnapshot | None    # default branch head
    # Each side is null when the configuration does not exist on that side. The API nests the
    # content under `diff` (ConfigurationDiffData); we flatten it into the snapshot above so the
    # agent reads one level less. When there is no conflict all three sides are equal.

class MergeRequestConflictsOutput(BaseModel):
    merge_request_id: int
    conflicts: list[ConfigDiff]             # [] means nothing to resolve
    next_step: str

class MergeResult(BaseModel):
    merge_request_id: int
    merged: bool
    state: MergeRequestState
    job_id: str | None                             # JobResponse types `id` as string — keep it a str
    status: MergeRequestStatus | None             # set when merged=False (why it was refused)
    next_step: str
    # Deliberately carries no change list: the merge endpoint returns a Job, not the MR, so
    # including it would cost an extra fetch. If the user asks what was merged, the agent
    # re-reads get_merge_requests(merge_request_ids=[id]).

class ResolveConflictResult(BaseModel):
    component_id: str
    configuration_id: str
    resolved: bool
    remaining_conflicts: list[ConflictRef]  # [] -> the MR is now mergeable
    next_step: str
```

Notes on deliberate choices:

- **`MergeRequestStatus` is returned by `approve`, `reject` and `submit_for_review`** and
  embedded in `MergeRequestDetail` and in `MergeResult` when a merge is refused, so the agent
  re-reads the same shape after those actions. The two exceptions are deliberate:
  `merge_merge_request` returns `MergeResult` (status only on refusal — on success the state is
  simply `published`) and `resolve_config_conflict` returns `ResolveConflictResult`, whose
  `remaining_conflicts` is the signal the loop needs.
- **`ResolveConflictResult.remaining_conflicts`** lets the agent drive the per-config loop
  without re-fetching the whole conflict set: empty list = ready to merge.

**Provenance — which fields are the API's and which are ours.** `MergeRequest`, `Reviewer`,
`ActivityEvent`, `ConflictRef`, `ConfigDiff` map 1:1 onto the Connection OpenAPI schemas
(`MergeRequestResponse`, `MergeRequestDetailResponse`, the conflicts action,
`ConfigurationDiffResponse`). **`MergeRequestStatus` has no backend counterpart** — `mergeable`,
`blocked_reason`, `action_required`, `next_step` and `approvals.missing_from` are *derived*
by this server from state + required-approvals + the conflicts list. That is the point (it is
what makes the agent's branching reliable), but it must be unit-tested as our own logic, not
trusted as API data.

Field translations we apply on purpose:

| API | Ours | Why |
|---|---|---|
| `reviewers[].status: 'approved'\|'rejected'\|null` | `ReviewerStatus` incl. `'pending'` | `null` is ambiguous for an LLM; `'pending'` states it |
| `activityLog[].note: string` (empty when none) | `note: str \| None` | normalize `''` → `None` |
| `approvals[].approverId: string` | `Approvals.given_by: list[str]` (names) | ids are noise in chat; the type mismatch (string id vs. int elsewhere) never surfaces |
| `merge.{mergedAt, mergerId, mergerName}` | `merged_at`, `merged_by` | keep who merged (`mergerName` is set even for system merges), drop the id |
| `externalId` | *dropped* | external-system reference, no meaning in a chat |
| `branches.{branchFromId, branchIntoId}` | `branch_from_id/name`, `branch_into_name` | names resolved via the branch pair lookup |

**`changeLog`: pass-through, no humanization.** The API types it as a bare `type: 'object'`
(`MergeRequestWithChangeLogResponse`), but its actual content is pinned by
`Model_Row_MergeRequest::updateChangeLog`:

```json
{"configurations": [
  {"componentId": "…", "configurationId": "…", "lastVersionIdentifier": "…", "isDeleted": false}
]}
```

Two consequences:

- **There is no added/modified distinction to summarize** — only a flat list with an
  `isDeleted` flag. An "N added, M modified" summary is not derivable, so we do **not** build
  one: `changed_configurations` is a 1:1 pass-through and the agent describes it in prose when
  the user asks *"what does this MR change?"*. This also removes any dependence on an
  unspecified schema — nothing to mis-parse. Parse defensively anyway (missing/renamed
  `configurations` key → empty list, never an exception).
- **It is populated at `request-review` / `skip-review`, not at merge**
  (`MergeRequestService.php:120,134`). So while the MR is still in `development` the change
  list is empty; it becomes available once the MR is sent for review. `next_step` should say
  so rather than implying the MR is empty.

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
   `/conflicts` + the required-approvals count, per the normative decision table above.
   `merge_merge_request` catches the backend 409/not-ready and returns this status (enriched
   with the conflicts list) instead of a raw error.
   **Request cost:** the status needs `/conflicts`, which `POST /merge-request` does not
   return — so `create_merge_request` is `POST /merge-request` **+ 1** `GET
   …/conflicts`. It needs nothing further: `changed_configurations` is legitimately `[]` in
   `development` (the changeLog is only written at request-review) and `activity_log` is `[]`
   because creation emits no lifecycle event. Likewise `get_merge_requests` in detail mode is
   `1 + 2N` requests (detail with `?include=activityLog`, plus `/conflicts`, per MR) — batched
   concurrently. **List mode stays 1 request**, which is why summaries carry no status.
   The required-approvals count comes from project metadata
   (`KBC.branches-merge-requests.required-approvals-count`, default 0) and is fetched once per
   call, not per MR.
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
   merge / conflict tools) when the session is on the default branch
   (`is_client_using_main_branch`, `mcp.py`), returning a handoff error on direct call.
7. **Docs & version**: regenerate `TOOLS.md` (`tox -e check-tools-docs`); minor version bump
   (from current `main`, `1.74.3 → 1.75.0` at the time of writing; new tools = minor); refresh
   `uv.lock`.

**Trade-offs called out:**

- *Explicit tools vs. one `action=` enum, and no orchestrator.* Explicit per-action tools
  give better LLM tool-selection and correct `readOnly`/`destructive` hints. We also
  deliberately ship **no** one-shot orchestrator: the happy path is three cheap calls an
  agent chains anyway, and collapsing them would hide the review gate (see
  *Dropped / deferred*). Exactly one destructive merge path exists.
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

- New module `tools/merge_requests.py` with the 8 tools above (MVP core + `approve`/`reject`).
- Storage-client wrappers for the MR endpoints and the branch-scoped `diff`/`rebase`.
- Three-axis gating (feature / role / session-branch) and the handoff error from production.
- The `resolve_branch_pair` helper and the `_await_storage_job` polling loop (both new code).
- The status-object helper and lean response models (no changelog humanizer — pass-through).
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
  - status object: **the full decision table above, parametrized row by row** (every `state`
    × conflicts × approvals combination, including `not_ready` for `development`/`in_merge`/
    `canceled` and `wait_for_merge` for `in_merge`), plus the precedence rule (conflicts win
    over `needs_approval` when both apply), `required` read from project metadata rather than
    assumed 0, and `merge_merge_request` returning the status (not a raw error) on a 409.
  - conflict resolution: `get_merge_request_conflicts` fans out diffs and flattens each side's
    nested `diff` payload; `resolve_config_conflict` sends `version` + the full content set for
    a keep rebase, sends **only** `version` for `delete=True`, and rejects a partial payload
    client-side; `is_disabled=False` is sent while `is_disabled=None` is omitted (tri-state);
    a missing `theirs` fails with an explicit message instead of guessing a version; a resolved
    MR then merges (live conflict check clears); approvals survive rebase.
  - `changeLog` parsing: normal `configurations` list; empty while `state='development'`;
    missing/renamed key → empty list, never an exception.
  - status object is *derived*, not passed through: `mergeable` / `blocked_reason` /
    `action_required` computed from state + required approvals + conflicts; `null` reviewer
    status maps to `'pending'`; empty activity-log `note` maps to `None`.
  - gating: tools hidden without the feature; **all writes** hidden for `developer` /
    `reviewer` / `readonly` and visible for `admin` / `share` (matching
    `#[MergeRequestsAllowedRoles]`, which is `[ADMIN, SHARE]` on every non-SOX write) while
    reads stay visible to any member; branch-only tools error / hidden on a production session
    and visible on a dev-branch session.
  - request counts: `create_merge_request` = POST + 1 `/conflicts`; detail mode = `1 + 2N`
    batched; list mode = exactly 1 request (no per-MR conflicts probe); `state` filtering
    happens client-side.
- **Integration tests** (`integtests/tools/test_merge_requests.py`) against a
  `branches-merge-requests` project: on a dev-branch session, make a config change,
  `create_merge_request` → `submit_merge_request_for_review` → `merge_merge_request`, assert
  `published` + the changed-configurations list. A conflict scenario: change the same config in production, assert
  a conflict surfaces, `resolve_config_conflict`, then merge succeeds. Clean up branches.
- **Manual E2E** via local `.mcp.json` (per CLAUDE.md) on a non-SOX branch project: run the
  X and Y chat flows from a dev-branch session; confirm plain-language status and the guided
  conflict resolution.
- **`tox`** (pytest, black, flake8, check-tools-docs) green before pushing.
