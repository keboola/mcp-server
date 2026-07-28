# RFC: `get_shared_buckets` + `link_shared_bucket` — discover and link Data Catalog shares

Linear: [AI-3243](https://linear.app/keboola/issue/AI-3243/support-16377-add-keboola-mcp-tool-to-enumerate-shared-with-me-data)

## Problem

`get_buckets` (`src/keboola_mcp_server/tools/storage/tools.py:556-604`) calls
`GET /v2/storage/branch/{branch}/buckets`, which is scoped to buckets that already exist in
the caller's project — including ones already linked from another project (surfaced via
`BucketDetail.source_project`, added in AI-1882). It has no surface for the Data Catalog
**"Shared with me"** view: buckets other projects have shared (`organization`,
`organization-project`, or `specific-projects` scope) that are visible to this project but
**not yet linked**. An agent asked "what could I link?" has no tool to answer that today.

The customer (SUPPORT-16377 / Groupon) worked around this with a per-project custom Python
component that polls `GET /v2/storage/shared-buckets` server-side — up to 24h stale, and
requiring bootstrap in every target project.

**Prior internal objection (AI-3243 comments) that this RFC must resolve:** when this ticket
was triaged, the objection to shipping it as-is was that "response on top of a bigger project
will be non-pageable" — i.e. a naive full-list wrapper risks dumping an unbounded number of
shared buckets into the agent's context for orgs with many shares, the same failure mode
`get_buckets`/`get_tables` already accept for a *bounded* inventory (a project's own buckets)
but which does not hold for an org-wide catalog of shares. This RFC adds pagination
specifically to close that objection.

## Required Behavior

A new read-only tool, `get_shared_buckets`, wrapping
`GET /v2/storage/branch/{branch_id}/shared-buckets` (the same branch-aware path the Keboola
UI already calls for this view).

| Param | Type | Default | Behavior |
|---|---|---|---|
| `limit` | `int` | `50` | Max items returned. Invalid (`<= 0` or `> MAX_SHARED_BUCKETS_LIMIT`) silently resets to default, matching `search`'s convention (`search.py:479-485`). |
| `offset` | `int` | `0` | Clamped to `>= 0`. |

`MAX_SHARED_BUCKETS_LIMIT = 100` (same ceiling as `search`'s `MAX_GLOBAL_SEARCH_LIMIT`).

Response fields per item (the exact set the customer specified, already a lean projection —
no nested `metadata`/`columns` blobs like `BucketDetail`):

| Field | Source | Notes |
|---|---|---|
| `id` | `id` | Shared bucket ID (in the *source* project). |
| `display_name` | `displayName` | |
| `stage` | `stage` | |
| `project_id` | `project.id` | Source project ID. |
| `project_name` | `project.name` | Source project name. |
| `sharing` | `sharing` | Scope: `organization` \| `organization-project` \| `specific-projects`. |
| `linked_by` | `linkedBy` | Projects that have already linked this bucket, if the API returns it — needed for the `specific-projects` case the customer called out as invisible to telemetry. |
| `tables_count` | `tablesCount` | |
| `rows_count` | `rowsCount` | |
| `data_size_bytes` | `dataSizeBytes` | |
| `description` | `description` | |

Output shape mirrors `GetBucketsOutput`'s truncation-note convention (`workspace.py:128`
precedent) rather than a boolean flag:

```python
class GetSharedBucketsOutput(BaseModel):
    shared_buckets: list[SharedBucketDetail]
    total_count: int  # total available at the source, independent of limit/offset
    message: str | None  # e.g. "Returning 50 of 214 shared buckets. Use offset=50 to see more."
```

## Resolution Strategy

1. **Client method** — `AsyncStorageClient.shared_bucket_list(branch_id=None)` in
   `clients/storage.py`, modeled directly on `bucket_list` (`storage.py:442-454`):
   ```python
   async def shared_bucket_list(self, branch_id: str | None = None) -> list[JsonDict]:
       bid = branch_id or self._branch_id
       return cast(list[JsonDict], await self.get(endpoint=f'branch/{bid}/shared-buckets'))
   ```
   The endpoint itself has no server-side pagination (confirmed against the PHP client /
   `SharedBucketsListAction` — it returns the full list), so `limit`/`offset` are applied
   **tool-side**, same as `search.py:591`'s `all_hits[offset : offset + limit]`.

2. **Model** — `SharedBucketDetail(BaseModel)` in a new
   `tools/storage/shared_buckets.py` module (kept separate from `tools/storage/tools.py`,
   already 1000+ lines), with a `model_validator(mode='before')` to flatten
   `project.id`/`project.name` into `project_id`/`project_name` (same flattening style as
   `BucketDetail.set_source_project`, `tools.py:242-245`).

3. **Tool** — `get_shared_buckets(limit: int = 50, offset: int = 0, ctx) -> GetSharedBucketsOutput`
   in `tools/storage/shared_buckets.py`, registered via its own `add_shared_bucket_tools(mcp)`
   under the same `Storage Tools` category as `get_buckets`/`get_tables`. Docstring explicitly
   states the pagination contract and default limit, following the `query_data` precedent of
   documenting hard limits directly in the tool description (`sql.py:282-284`) so the agent
   knows to page rather than assume completeness.

4. **Sort order** — request the endpoint's natural order (no server-side sort param exists);
   apply `offset`/`limit` after a stable sort by `id` so pagination is deterministic across
   calls within a session.

5. **Link action** — `link_shared_bucket(source_project_id: str, source_bucket_id: str,
   target_bucket_name: str, target_stage: Literal['in', 'out'] | None = None,
   display_name: str | None = None, ctx) -> BucketDetail`.

   **Verified against the reference client** (`keboola/storage-api-php-client`,
   `Client::linkBucket()` / `BranchAwareClient::request()`) rather than assumed from the
   ticket, since the ticket's suggested `POST /v2/storage/buckets/{id}/link` shape turned out
   to be wrong: linking a shared bucket is actually
   `POST /v2/storage/branch/{branch_id}/buckets` (branch-prefixed the same way
   `bucket_list`/`shared_bucket_list` are — `BranchAwareClient` rewrites any URL not in its
   `START_ENDPOINTS_WITHOUT_BRANCH` exclusion list to `branch/{id}/...`, and `buckets` is not
   on that list) with body:
   ```json
   {"name": "...", "stage": "in|out", "sourceProjectId": "...", "sourceBucketId": "...", "displayName": "..."}
   ```
   `stage` is a required API field (the endpoint creates a new local bucket, it does not
   infer stage from the source). To keep the tool's signature as close to the customer's
   3-argument ask as possible, `target_stage` defaults to the stage encoded in
   `source_bucket_id`'s own `in.`/`out.` prefix (the universal Keboola bucket-ID convention
   already relied on elsewhere in this codebase, e.g. `tests/tools/test_sql.py`'s
   `in.c-foo.bar` fixtures) rather than requiring the agent to pass it explicitly, while still
   allowing an explicit override for the rare re-stage-on-link case.

   Add `AsyncStorageClient.bucket_link(name, stage, source_project_id, source_bucket_id,
   display_name=None, branch_id=None)` in `clients/storage.py`, mirroring `bucket_list`'s
   optional-`branch_id`-override pattern. The tool returns the newly linked bucket as a
   `BucketDetail` (reusing the existing model — the link response has the same shape as
   `bucket_detail`) so the agent immediately sees the linked bucket's local `id`/`name`
   without a follow-up `get_buckets` call.

## Scope

In scope:

- `get_shared_buckets` tool (read-only), `SharedBucketDetail` model, `GetSharedBucketsOutput`.
- `link_shared_bucket` tool (write), wrapping the link endpoint, returning the resulting
  `BucketDetail`.
- `AsyncStorageClient.shared_bucket_list` and `AsyncStorageClient.bucket_link`.
- `limit`/`offset` pagination per above — this is the change that resolves the prior
  "non-pageable" objection.
- Unit tests (client methods, tool pagination/clamping, model field mapping, link
  success/error paths) and an integration test asserting `get_shared_buckets` runs against a
  real project and returns a shape-valid response (shared buckets may be empty in the test
  project — assert response shape, not a nonzero count). `link_shared_bucket` integration
  coverage is best-effort: only run the live-link assertion if the integtest project fixture
  actually has a real pending share available; otherwise cover it with a unit test against a
  mocked 4xx/409 (already-linked) response.
- `TOOLS.md` regeneration.
- Version bump: this PR lands after #645 (`1.73.5`); bump to `1.74.0` (new tools = minor).

Out of scope:

- Adding a `shared-bucket` item type to `search()` — composes naturally later, but duplicating
  discovery across two tools before either has real usage is premature.
- Surfacing this as metadata on `get_buckets` instead of a separate tool — considered (see
  AI-3243 comment thread) and rejected: unlinked shared buckets are not project resources and
  don't fit `BucketDetail`'s identity model (no local `id` in this project, no
  `branch_id`/`prod_id` shading concept); a distinct read-only tool keeps `get_buckets`'s
  existing (unpaginated, full-inventory) contract unchanged for existing callers.
- Un-linking / `POST /v2/storage/buckets/{id}/unlink` — not requested by the customer, and
  SUPPORT-16370 (recurring "can't stop sharing" pain point) suggests unlink semantics need
  their own investigation before exposing a tool.

## Testing / Verification

1. Unit tests in `tests/tools/storage/test_shared_buckets.py` (parametrized): default
   limit/offset, clamping of invalid `limit` (`0`, negative, `> 100`), `offset` beyond total
   count (empty result + accurate `message`), field mapping from a realistic raw payload
   fixture, and `link_shared_bucket` success + stage-derivation/error paths.
2. Unit tests for `AsyncStorageClient.shared_bucket_list` and `.bucket_link` in
   `tests/clients/test_storage.py` asserting correct endpoint/branch resolution and payload.
3. Integration test in `integtests/tools/storage/` — **deferred**: no test project fixture
   with a real pending share is available yet, so this PR ships without one; unit tests cover
   the client/tool contract against realistic mocked payloads instead. Add this once a
   suitable fixture project exists.
4. `tox` — pytest, black, isort, flake8, check-tools-docs all exit 0.
5. Manual smoke test via local `.mcp.json` against a project with at least one real
   organization-scope share, to confirm `sharing`/`linked_by` populate correctly and that
   `link_shared_bucket` successfully links a real pending share.
