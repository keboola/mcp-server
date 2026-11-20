---
slug: data_catalog
status: ready-for-rfc
author: Martin Vaško
created: 2026-08-05
linear: AI-3692
---

# Brainstorm: Data Catalog sharing tools for MCP Server

> This document is the discovery artifact that precedes the formal RFC at `./RFC.md`. Anything in the RFC must be traceable to a decision recorded here.

## 1. Problem framing

### Trigger
The user wants a fully agent-driven Data Catalog loop:
1. Agent finishes a pipeline in project A (transformation → job run → bucket filled with data).
2. Agent shares the resulting bucket to the org, or to specific projects.
3. An agent in project B sees the shared bucket and links it in.
4. The agent in project B reuses that data in a new pipeline, based on semantics/naming.

Step 3 was already independently in flight as PR #646 (`get_shared_buckets` +
`link_shared_bucket`, Linear AI-3243, from a support ticket — SUPPORT-16377/Groupon). Steps 1 and
4 already work today with existing tools (components/flows for pipelines, `get_buckets`/
`get_tables`/`query_data` for reuse). **Step 2 — the producer side, sharing itself — has no tool
at all.** That's the actual gap this RFC closes.

### Pain
- An agent that just built something useful in project A has no way to publish it without the
  user manually clicking through the Keboola UI's sharing dialog — breaking the "agent does the
  whole workflow" promise for any cross-project data-sharing scenario.
- Without a producer tool, PR #646's consumer-side tools (`get_shared_buckets`/
  `link_shared_bucket`) are only half the loop: useful for *discovering* what a human already
  shared, useless for an agent trying to *complete* a share→link→reuse workflow end-to-end.

### Cost of inaction
- Cross-project reuse stays a manual, UI-only, human-in-the-loop step even as the rest of the
  pipeline (build → run → fill Storage) becomes fully agentic.
- PR #646 ships a discovery tool with no producer counterpart — the Data Catalog surface stays
  asymmetric (you can find what others shared, but an agent can never be the one sharing).

## 2. Constraints

### Hard constraints (can't change)
- **Always branch-scoped, always async** — explicit user instruction, and also the reality of
  the underlying endpoints: `share-organization`, `share-organization-project`, and the unshare
  `DELETE .../share` all return **only** `202` + a job envelope (confirmed against the live
  Storage OpenAPI spec) — there is no synchronous path to skip.
- **No naming parameter on share/unshare** — they act on an existing bucket by id; only
  `link_shared_bucket` (a *create*, not a share/unshare) takes a name, per user decision.
- **RFC before implementation** — `CONTRIBUTING.md`'s RFC Requirement table marks "New MCP tool"
  as RFC-required; three new tools here means an RFC is mandatory before any tool code lands.

### Prior art
- **PR #646** (`martinvasko-ai-3243-...`, `feature_spec/list_shared_buckets/RFC.md`) — the
  consumer-side sibling. Its module (`tools/storage/shared_buckets.py`), its
  `AsyncStorageClient.shared_bucket_list`/`.bucket_link` pattern, and its careful verification of
  `link_shared_bucket`'s real request body (the ticket's suggested endpoint was wrong; the actual
  one was found via the `storage-api-php-client` source) are the template for this RFC's
  resolution strategy and for flagging `share-organization-project`'s body as unverified rather
  than guessing.
- **Existing branch-scoped bucket reads** (`bucket_list`, `bucket_detail` in `clients/storage.py`)
  — the pattern new sharing methods must follow (`branch/{bid}/...`), as opposed to this
  codebase's legacy non-branch metadata methods (`bucket_metadata_update` etc.), which must not
  be copied.
- **PSGO-261's `ProjectIdArg`** (`keboola_mcp_server.scope`) — the existing multi-project
  write-targeting parameter every write tool in this codebase now takes; the new sharing tools
  reuse it rather than inventing a second project-targeting convention.
- **No existing Storage-job poller** — `jobs_queue.py` (Queue API jobs) and `workspace.py` (SQL
  job polling) both poll *different* APIs; neither is directly reusable for a Storage-API job id,
  so `wait_for_storage_job` is new code, informed by their backoff style but not extending them.
- **No skill/plugin mechanism in this codebase at all** (confirmed: zero references to "skill" or
  "plugin" in `src/`). The `keboola/ai-kit` pointer can only be a text recommendation in the
  system prompt, not an actual install trigger.

### Scope split (user-stated)
The user explicitly scoped this task to **RFC + brainstorm + a drafted system-prompt diff only**;
new tool/client code is a separate, later implementation PR gated on this RFC being agreed. PR
#646's rebase/merge is also explicitly a separate, already-identified follow-up — not part of
this RFC's deliverable.

## 3. Stakeholders

- **Kai / multi-project agents** — the direct beneficiary; this is what makes the share step
  agent-drivable instead of UI-only.
- **Customers who filed the underlying support ticket for PR #646** (SUPPORT-16377) — indirectly
  benefit once the loop is symmetric (their original ask was discovery; full sharing closes the
  loop their workaround component was trying to bridge).
- **Whoever implements the follow-up PR** — inherits the one open verification task
  (`share-organization-project`'s body shape) as a concrete first step, not a vague TODO.

## 4. Alternatives considered

**Async job handling:**
- **Option A (chosen): tool polls to completion internally.** One tool call, one final answer.
  Matches how a human/agent expects a "share this bucket" action to feel, and avoids adding a
  second "check job status" tool just for this feature.
- **Option B: tool returns the job id immediately**, agent must call a separate poll tool.
  More technically "correct" for a long-running op, but doubles the tool surface for no benefit
  here — share/unshare jobs are typically fast, and the agent gains nothing from manual polling
  it wouldn't also want automated.
- Rejected B in favor of A (this is also what the user chose when asked directly).

**Skill delivery for `ai-kit`:**
- **Option A (chosen): a text pointer inside the existing system prompt.** Zero new
  infrastructure; degrades gracefully when the plugin isn't installed.
- **Option B: build a real MCP resource/skill-loading mechanism first.** Rejected as far out of
  proportion to this RFC — this codebase has no resource-listing wiring at all today
  (`@mcp.resource`/`list_resources` unused), and building one is a separate architectural RFC in
  its own right, not a "pilot."

**Where the new tools live:**
- **Option A (chosen): extend PR #646's `tools/storage/shared_buckets.py`.** Same tool family
  (Data Catalog), avoids fragmenting related tools across files for no reason.
- **Option B: a brand-new `tools/storage/sharing.py`.** Left as a fallback only if #646's file
  has grown too large by implementation time — not decided now, deferred to that PR.

## 5. Impact analysis

### Files/symbols touched (implementation PR, not this one)
- `src/keboola_mcp_server/clients/storage.py` — three new `AsyncStorageClient` methods +
  `wait_for_storage_job`.
- `src/keboola_mcp_server/tools/storage/shared_buckets.py` (from #646) or a new `sharing.py` —
  three new tools.
- `src/keboola_mcp_server/resources/prompts/project_system_prompt.md` — extend the existing
  "Data Catalog & Data Sharing" section (lines 215-238 on current main).
- `TOOLS.md` — regenerated.
- Tests: `tests/clients/test_storage.py`, `tests/tools/storage/test_shared_buckets.py` (or
  equivalent), extending #646's existing test files.

### Services/APIs touched
- Keboola Storage API: `POST .../share-organization`, `POST .../share-organization-project`,
  `DELETE .../share`, `GET .../jobs/{id}` (existing `job_detail`, now polled).

### Cross-cutting checklist
- Version bump (minor — new tools) in the implementation PR, once it lands after #646.
- `uv.lock` sync after the version bump.
- No new dependencies, no new client classes, no new session/auth model — this is additive to
  the existing Storage tool surface.

## 6. Security pass

- Sharing/unsharing changes **who can access existing data** — this is exactly why the new tools
  get `ToolAnnotations(destructiveHint=True)` (unlike `link_shared_bucket`'s
  `destructiveHint=False`, which only creates a new local pointer, per #646's review thread).
- No new credentials or token types introduced — sharing runs on the same session token already
  used for other Storage writes; permission enforcement (403 on insufficient rights) is the
  Storage API's own, not something this RFC needs to reimplement.
- No secrets/tokens are logged by the new job-poll helper — it only logs job id/status, mirroring
  this codebase's existing convention of never logging token material.

## 7. Open questions

Carried into the RFC's own "Open Questions" section verbatim:
1. `share_bucket_to_project`'s target-project request body is undocumented in the Storage
   OpenAPI spec — needs live verification before implementation.
2. Job-timeout UX (report last-known status vs. raise) — not decided.
3. Final module placement (`shared_buckets.py` vs. a new `sharing.py`) — decide at
   implementation time based on #646's file size after merge.

## 8. Next step

RFC written (`./RFC.md`) and a system-prompt diff drafted inside it for review. Once agreed:
1. Rebase/merge PR #646 first (separate task, already identified).
2. Verify `share-organization-project`'s body shape (Open Question 1).
3. Open the implementation PR: new client methods, new tools, system-prompt edit, tests,
   `TOOLS.md` regen, version bump.
