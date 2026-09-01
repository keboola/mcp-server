# RFC: Resolve where a semantic-dataset's physical data actually lives

Linear: [AI-3790](https://linear.app/keboola/issue/AI-3790/verify-that-the-new-semantic-models-and-datasets)

## Problem

A `semantic-dataset` object carries a `tableId` (e.g. `out.c-RGP-Global.checkins`) and, once the
scope-surfacing change in this same Linear issue lands, a `scope`/`sourceProjectId`/
`targetProjectIds` on its parent model. None of that proves the table is actually queryable from
the project asking about it:

- An `organization` or `targeted` scope object only says the *metastore record* is visible
  elsewhere — it says nothing about whether the underlying Keboola Storage bucket behind
  `tableId` was ever shared into the consuming project.
- `get_shared_buckets` (`tools/storage/shared_buckets.py`) already lists buckets shared with this
  project but not yet linked, and `get_buckets`/`bucket_detail`'s `BucketDetail.source_project`
  already says where an already-linked bucket came from — but nothing joins either of those to a
  semantic-dataset's `tableId`.

Visible symptom: a consumer (human or Kai) sees a semantic-dataset via `get_semantic_context` or
`search_semantic_context`, is told it is `organization`/`targeted` scope, and has no way to find
out — short of manually cross-referencing `get_buckets` and `get_shared_buckets` by hand — whether
running a query against its `tableId` will actually work, or whether the bucket needs linking
first, or whether the metastore object was shared without the underlying data ever being shared
(a real misconfiguration, not a hypothetical one, since scope and bucket sharing are two
independent, unenforced actions today).

## Required Behavior

| Scenario | Required behavior |
| --- | --- |
| Dataset's `tableId` resolves to a bucket already present in the caller's own project (`get_buckets`/`bucket_detail`) | Report `status: "local"` if that bucket has no `source_project` (owned outright), or `status: "linked"` with the source project id if it does. |
| Dataset's `tableId` resolves to a bucket not present locally, but listed by `get_shared_buckets` (matched by bucket id and the model's `sourceProjectId`) | Report `status: "shared_not_linked"`, plus the `source_project_id`/`source_bucket_id` a caller would pass to `link_shared_bucket` to fix it. |
| Dataset's `tableId` resolves to neither | Report `status: "unreachable"` — the metastore object's scope says one thing, the actual data says another. This is the case worth surfacing loudly: it means the semantic object was shared (or promoted) without the backing bucket ever being shared, or the bucket link was later removed. |
| Caller does not ask for location resolution | No behavior change — see Scope on why this is opt-in. |

## Resolution Strategy

New module `tools/semantic/data_location.py`, using the `MetastoreClient` and `StorageClient`
already available on the same request-scoped `KeboolaClient` (see `tools/storage/shared_buckets.py`
for the existing pattern of using both side by side):

```python
async def resolve_dataset_location(client: KeboolaClient, dataset: SemanticDatasetData) -> DatasetLocation:
    bucket_id = dataset.table_id.rsplit('.', 1)[0]
    # 1. local/linked: bucket_id present in client.storage_client.bucket_list()
    # 2. shared-not-linked: bucket_id present in client.storage_client.shared_bucket_list(),
    #    matched against the dataset's parent model's sourceProjectId
    # 3. neither -> "unreachable"
```

Two call sites, both **opt-in** (see Scope):

- `get_semantic_context(..., resolve_data_location: bool = False)` — attaches `data_location` to
  each returned `semantic-dataset`.
- `validate_semantic_query` — for each *used* dataset, add a pre-execution finding when its
  resolved status is `shared_not_linked` or `unreachable`, alongside the existing constraint
  violations/post-execution checks it already reports.

## Scope

**In scope:**

- Read-only resolution as described above, for `semantic-dataset` only (the only semantic object
  type that carries a `tableId`).
- Opt-in via an explicit parameter — a routine `get_semantic_context` call does not pay for the
  extra `bucket_list`/`shared_bucket_list` round trips unless a caller asks for them.

**Out of scope:**

- No new "share a bucket to project X" write tool — sharing is a Storage/console action, not a
  consumption concern.
- No reverse lookup ("which projects have I shared this bucket out to") from the owning project's
  side — `get_shared_buckets`/`BucketDetail.source_project` only cover the receiving direction;
  the reverse would need a new Storage API capability and is a separate ask.
- No change to how scope itself is set, promoted, or granted (`create_object`/`patch_object`
  remain unused by any tool in this repo, per the read-only framing of the whole semantic-tools
  surface).

## Testing / Verification

- Unit tests for `resolve_dataset_location` covering all three statuses, plus the case where
  `table_id` has no bucket separator (malformed data — should not raise).
- Unit tests for `get_semantic_context`'s `resolve_data_location` flag: on (attaches
  `data_location`) vs. off (field absent, no extra client calls made — assert the storage client
  mock was not called).
- Unit test for `validate_semantic_query` surfacing a `shared_not_linked`/`unreachable` finding for
  a used dataset.
