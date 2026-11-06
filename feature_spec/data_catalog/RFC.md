# RFC: Data Catalog sharing tools (`share_bucket_to_organization` / `share_bucket_to_project` / `unshare_bucket`)

Linear: [AI-3692](https://linear.app/keboola/issue/AI-3692/data-catalog-rfc-support-sharinglinking-storage-buckets-across)

## Problem

An agent that finishes a pipeline (creates a transformation, runs the job, fills a Storage
bucket) has no way to publish that bucket into the Data Catalog so another project can reuse it.
The **consumer** half of this workflow already exists or is in flight:

- `get_buckets`/`get_tables` (`tools/storage/tools.py`) already surface `source_project` on a
  bucket/table that's already linked into the current project (read side, shipped).
- `get_shared_buckets` + `link_shared_bucket` (PR #646, `feature_spec/list_shared_buckets/RFC.md`,
  not yet merged — `CONFLICTING` against main, needs its own rebase, tracked separately) will let
  an agent discover what other projects have shared and link a chosen bucket in.

But there is no **producer** tool: nothing wraps the Storage API's `share-organization`,
`share-organization-project`, or unshare (`DELETE .../share`) operations. Without it, step 2 of
the intended workflow — *"agent shares data from the project it just built to other projects in
the organization"* — has to be done by hand in the Keboola UI, breaking the agent's end-to-end
loop:

1. Agent finishes a pipeline in project A (transformation → job run → bucket filled).
2. **Agent shares the resulting bucket** to the org, or to specific projects. ← missing (this RFC)
3. An agent in project B discovers and links the shared bucket (PR #646, separate track).
4. The agent in project B reuses the linked data in a new pipeline (already works — linked
   buckets/tables behave like any other Storage bucket/table for reads).

## Required Behavior

Three new write tools in the Storage tool group, all **branch-scoped** and **always async**
(the underlying endpoints have no synchronous option — see below), each **polling its Storage
job to completion inside the tool call** and returning one final result, not a job handle the
agent has to poll itself:

| Tool | Wraps | Params | Returns |
| --- | --- | --- | --- |
| `share_bucket_to_organization` | `POST /v2/storage/branch/{branchId}/buckets/{bucketId}/share-organization` | `bucket_id: str`, `project_id: ProjectIdArg = None` | `ShareBucketResult` (job id, final status, error message if failed) |
| `share_bucket_to_project` | `POST /v2/storage/branch/{branchId}/buckets/{bucketId}/share-organization-project` | `bucket_id: str`, `target_project_ids: list[int]` (**name/shape unverified — see Open Questions**), `project_id: ProjectIdArg = None` | `ShareBucketResult` |
| `unshare_bucket` | `DELETE /v2/storage/branch/{branchId}/buckets/{bucketId}/share` | `bucket_id: str`, `project_id: ProjectIdArg = None` | `ShareBucketResult` |

`project_id` is the existing multi-project write-targeting parameter
(`keboola_mcp_server.scope.ProjectIdArg`, introduced for PSGO-261) — required only when the
session is scoped to 2+ projects, otherwise defaults to the single scoped project. It selects
*which project's Storage API the call runs against*, not the sharing target (that's
`target_project_ids` for `share_bucket_to_project`).

**No naming parameter on any of these three tools.** They act on an existing bucket by id — the
bucket keeps its name; sharing/unsharing never renames anything. (This differs from
`link_shared_bucket` in PR #646, which *does* take a name, because linking creates a *new* local
bucket. See "Naming convention" below.)

**Confirmed from the Storage OpenAPI spec** (`https://api.keboola.com/specs/storage.json`):

- `share-organization`: no request body; only success response is `202` with a job envelope
  (`id`, `status`, `url`, `operationName`, `createdTime`, …). Errors: `400` (branch/dev bucket
  unsupported), `403` (insufficient permissions), `501` (Snowflake Partner Connect projects).
- `share-organization-project`: same job-envelope shape, only `202`. **The spec does not document
  a request body at all** — how the target project(s) are specified is unknown from the spec (see
  Open Questions).
- Unsharing is `DELETE .../buckets/{bucketId}/share` (there is no `.../unshare` path) — same
  job-envelope shape, only `202`. A sibling `PUT .../buckets/{bucketId}/share` exists but is an
  undocumented stub in the spec; not used here.
- None of the three endpoints accept an `?async=true` toggle — they are unconditionally async.
  ("Always async, always branch-scoped" also matches this feature's explicit design constraint,
  independent of the spec.)

### Naming convention (Data Catalog-wide, not just this RFC)

The historical convention at some customers was a `shared-` name prefix (for buckets holding
cherry-picked table aliases) or embedding the source project's name in a linked bucket's name.
That convention is now obsolete: the Keboola UI tags a bucket's shared-out/linked-in status
directly, so the name doesn't need to encode it. The only remaining naming decision is
`link_shared_bucket`'s bucket name (PR #646) and any future cherry-pick-tables tool — those
names should be **descriptive of the data/model the bucket holds** (e.g. `in.c-customer-360`,
not `in.c-shared-project-42`), never a lineage-encoding prefix.

## Resolution Strategy

1. **New `AsyncStorageClient` methods** in `clients/storage.py`, modeled on the existing
   branch-scoped bucket methods (`bucket_list`, `storage.py:442`) rather than the legacy
   non-branch metadata methods:
   - `bucket_share_organization(bucket_id, branch_id=None) -> JsonDict` — `POST
     branch/{bid}/buckets/{bucket_id}/share-organization`.
   - `bucket_share_organization_project(bucket_id, target_project_ids, branch_id=None) ->
     JsonDict` — `POST branch/{bid}/buckets/{bucket_id}/share-organization-project`. Body shape
     pending the verification in Open Questions.
   - `bucket_unshare(bucket_id, branch_id=None) -> JsonDict` — `DELETE
     branch/{bid}/buckets/{bucket_id}/share`.
   - All three return the raw job envelope (`{id, status, ...}`); none poll internally.

2. **New job-poll helper**, also on `AsyncStorageClient`: `wait_for_storage_job(job_id, *,
   timeout_seconds=60, poll_interval_seconds=2) -> JsonDict`. Repeatedly calls the existing
   `job_detail(job_id)` (`storage.py:848`) until `status` is a terminal value (`success`,
   `error`, `cancelled`) or the timeout elapses, then returns the final job dict (or raises/flags
   a timeout — exact contract in the implementation PR). **No existing helper does this** — the
   closest analogues (`clients/jobs_queue.py`'s Queue-API jobs, `workspace.py`'s SQL-job polling)
   poll a different API and aren't reusable directly, but their backoff/cadence style should
   inform this one.

3. **New tools**, added to PR #646's `tools/storage/shared_buckets.py` module (same "Data
   Catalog" tool family as `get_shared_buckets`/`link_shared_bucket` — kept in one place rather
   than forking a second module, unless that file has grown unreasonably large by the time this
   is implemented, in which case split into `tools/storage/sharing.py`):
   - `share_bucket_to_organization`, `share_bucket_to_project`, `unshare_bucket` — each calls its
     client method, then `wait_for_storage_job`, then maps the final job status to
     `ShareBucketResult`.
   - All three get `ToolAnnotations(destructiveHint=True)` (they change who can access existing
     data, unlike `link_shared_bucket`'s `destructiveHint=False` create-only semantics agreed in
     PR #646's review thread).

4. **System prompt.** Extend (not replace) the existing "Data Catalog & Data Sharing" section of
   `resources/prompts/project_system_prompt.md` (currently lines 215-238) with the full 4-step
   workflow, the naming guidance above, and a pointer to the `keboola/ai-kit` plugin for a fuller
   guided skill. Drafted text for review (final wording may be adjusted at implementation time):

   ```markdown
   ### Data Catalog & Data Sharing

   ... (existing Core Concepts / Read-Write Rules paragraphs unchanged) ...

   **Sharing workflow (producer → consumer)**
   1. After a pipeline finishes and fills a bucket, share it with `share_bucket_to_organization`
      (whole org) or `share_bucket_to_project` (specific projects) if the user wants to publish it.
   2. In the target project, discover what's available with `get_shared_buckets`, then bring a
      chosen bucket in with `link_shared_bucket`.
   3. Reuse the linked bucket's tables like any other Storage tables (read-only; see Read/Write
      Rules above).
   4. `unshare_bucket` revokes sharing; existing links elsewhere are unaffected unless the source
      project's admin also removes them.

   **Naming:** when linking, choose a descriptive name for what the bucket contains (e.g.
   `in.c-customer-360`) — the Keboola UI already tags shared/linked status, so don't encode
   lineage or "shared" in the name.

   **Fuller guidance:** if the `keboola/ai-kit` plugin's data-catalog skill is loaded, defer to
   it for detailed walkthroughs; if not, tell the user it's available at
   https://github.com/keboola/ai-kit and proceed with the guidance above.
   ```

## Semantic layer interaction (AI-3737)

Raised in review: a bare `share_bucket_to_project`/`share_bucket_to_organization` shares the
bucket but not any semantic definitions (datasets, metrics, relationships) built on top of it —
a consumer gets tables with no meaning attached, silently.

The Keboola UI has already worked through this exact problem
([ui#7885](https://github.com/keboola/ui/pull/7885), also currently unshippable — no backend
model-sharing endpoint exists yet). Its resolved design, which this RFC now mirrors per
@tomasfejfar's review comment ("I vote to mirror the UI"):

- **"Share" is a model-level action composed over two independent APIs** — Storage (buckets) and
  the Metastore (semantic-object scope) — not a bucket-only action. There is no single endpoint
  that spans both; the composition lives in a dedicated SDK layer
  (`semanticLayerSharingSdk.shareModel`/`unshareModel` in the UI's case) rather than in each
  caller, so kbc-ui, Kai's tools, and this MCP server don't drift apart on the semantics.
- **Order is deliberate and asymmetric.** Share: buckets first, then widen semantic-object scope.
  Rationale — data without meaning is a degraded-but-usable share; meaning pointing at
  unreadable tables is worse. Unshare: reverse order — revoke semantic scope first, then unshare
  buckets, so a consumer is never left holding metric definitions over tables that just vanished.
- **Not atomic, and reported rather than thrown.** Two independent, partially-async APIs cannot be
  made transactional. Each bucket gets an outcome (`shared` / `failed` / `excluded`); the overall
  result carries a `degraded` reason (`partial-failure`, `buckets-held-back`,
  `definitions-not-shareable-to-users`) instead of a bare success/error. Nothing is rolled back on
  partial failure — the caller re-runs to retry just the failed parts.
- **Raw bucket-only sharing stays a legitimate, explicit path** (data with no model behind it),
  it's just no longer the *default* one the system prompt should steer an agent toward.

**Decision for this RFC:** adopt the same shape rather than Matovidlo's "couple both into one
tool call, atomic-ish" alternative — the UI's own answer to that exact tradeoff was reported
partial failure, not forced all-or-nothing, and diverging from it here would leave kbc-ui, Kai,
and this server disagreeing on what "share" means for the same data.

**Blocking gap:** MCP has no primitive for widening a semantic object's scope. The existing
semantic tool group (`tools/semantic/`, `tools/semantic/service.py`) is read/validation-only
(`search_semantic_context`, `validate_semantic_query_with_used_objects`, …) — there is no
equivalent of the UI's `promoteToOrganizationScope`/`replaceTargetProjects` Metastore calls. A
`share_semantic_model`/`unshare_semantic_model` tool pair therefore cannot be built until:
1. a Metastore scoping endpoint exists and is confirmed callable from this server's auth context
   (same "unshippable without backend" state the UI PR is in), and
2. this RFC (or a follow-up) specifies the new client methods, the bucket-resolution step (walk a
   model's datasets → table ids → bucket ids, as the UI's `bucketsForModel` does), and the
   `ShareSemanticModelResult` shape (per-bucket outcomes + `degraded` reason, mirroring
   `ShareModelResult`/`SharedBucketOutcome` above).

Until that backend primitive exists, this RFC's three bucket-level tools ship as the explicit
raw-bucket path, and the system prompt (see below) must say plainly that they do **not** carry
semantic definitions with them — closing the silent-gap risk AI-3737 flagged, without blocking on
the semantic-layer work.

**System-prompt addendum** (extends the drafted text in Resolution Strategy step 4 above):

> **Semantic layer note:** these tools share/unshare the bucket only. If the bucket has semantic
> models, datasets, or metrics defined on top of it, those definitions are **not** shared and the
> consumer will see tables without any of that meaning attached. Tell the user this explicitly
> before sharing a bucket that has a semantic model on it. Model-level sharing (bucket +
> definitions together) is not yet available via MCP tools.

## Scope

**In scope:** `share_bucket_to_organization`, `share_bucket_to_project`, `unshare_bucket`;
`wait_for_storage_job`; the three new `AsyncStorageClient` methods; the system-prompt extension
above, including the semantic-layer caveat.

**Out of scope:**
- `share_semantic_model` / `unshare_semantic_model` (model-level sharing composing Storage +
  Metastore scope, mirroring [ui#7885](https://github.com/keboola/ui/pull/7885)) — blocked on a
  Metastore scoping endpoint that doesn't exist yet; tracked as a follow-up RFC once that backend
  primitive ships. See "Semantic layer interaction" above.
- `get_shared_buckets` / `link_shared_bucket` — PR #646, separate track (rebase/merge tracked
  independently of this RFC).
- Table aliases / "cherry-pick specific tables into a new named alias bucket" — the
  `table-aliases` endpoint the ticket originally referenced does not appear in the current
  Storage OpenAPI spec at all (zero occurrences); needs live verification against a test project
  or the `storage-api-php-client` source before it can be specified. Deferred to a follow-up RFC.
- The generic `PUT .../buckets/{bucketId}/share`, `POST .../share-to-projects`, `POST
  .../share-to-users` endpoints — all undocumented stubs in the spec; not used.
- BigQuery Analytics Hub "listing" endpoints (`.../buckets/{bucketId}/listing`) — a different,
  unrelated sharing mechanism that happens to live in the same spec neighborhood.
- Actually authoring a `keboola/ai-kit` data-catalog skill file — that's a separate repository;
  this RFC only adds the in-prompt pointer to it.

## Testing / Verification

1. **Before implementation**: verify `share-organization-project`'s request body against a live
   call or the `keboola/storage-api-php-client` source (the same diligence PR #646 applied to
   `link_shared_bucket`, whose ticket-suggested endpoint turned out to be wrong) — see Open
   Questions.
2. Unit tests for `wait_for_storage_job`: immediate success, pending→success after N polls,
   pending→error, and timeout-without-terminal-status.
3. Unit tests per tool: happy path (job succeeds), job error surfaced as a clear tool error,
   `project_id` ambiguity/out-of-scope behavior reusing the existing `MultiProjectMiddleware`
   write-dispatch tests (`tests/test_multiproject.py`) as a pattern.
4. `tox` — pytest, black, isort, flake8, `check-tools-docs` all exit 0; `TOOLS.md` regenerated.
5. Manual smoke test via local `.mcp.json` against two real projects in the same organization:
   share a bucket to the org, confirm it appears in the other project's `get_shared_buckets`,
   link it, unshare it, confirm it drops out of `get_shared_buckets` for a project that hadn't
   already linked it.

## Open Questions

1. **`share_bucket_to_project`'s target-project parameter is unverified.** The Storage OpenAPI
   spec documents no request body for `share-organization-project` at all. Before implementing,
   confirm the actual field name/shape (likely something like `{"projects": [<id>, ...]}` or
   `{"targetProjectId": <id>}`, but this must be confirmed, not assumed) against a live call or
   the PHP client source, exactly as PR #646 did for `link_shared_bucket`.
2. **Job-timeout UX.** If a share/unshare job doesn't finish within the poll timeout, should the
   tool return a "still processing, check back" result, or raise an error? Leaning toward the
   former (report last-known status rather than fail a probably-succeeding-eventually operation),
   but not decided.
3. **Where the three tools live** — folded into PR #646's `shared_buckets.py` vs. a new
   `sharing.py` — depends on that file's size once #646 merges; decide at implementation time.
4. **Semantic-model sharing timeline (AI-3737).** `share_semantic_model`/`unshare_semantic_model`
   are blocked on a Metastore scoping endpoint that doesn't exist yet — the same blocker the UI's
   prototype ([ui#7885](https://github.com/keboola/ui/pull/7885)) is in. Until it ships, do the
   three bucket-level tools in this RFC need to actively *warn* the agent (via tool response, not
   just the system prompt) when the target bucket has semantic objects on it, or is the
   system-prompt caveat sufficient? Leaning toward system-prompt-only for this RFC to avoid adding
   a Metastore read dependency to a Storage-only tool, revisit if AI-3737 needs stronger
   guarantees.
