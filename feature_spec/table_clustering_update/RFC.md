# RFC: Update the clustering of a BigQuery table

Linear: [DMD-3043](https://linear.app/keboola/issue/DMD-3043)

## Problem

Storage API can set or remove the clustering of a typed BigQuery table through the table definition update
(`PUT /v2/storage/branch/{bid}/tables/{tableId}/definition`, key `clustering`). The MCP server cannot:

- `AsyncStorageClient.table_definition_update()` sends only descriptions.
- The only caller, `update_descriptions`, is a per-item batch tool that turns every failure into
  `UpdateItemResult(success=False)`, so a Storage 422 never reaches the client as a tool error.
- The table detail of `get_tables` drops everything in `definition` except column types and descriptions, so the model
  cannot see the current clustering or partitioning of a table.

## Required Behavior

| Case | Result |
| --- | --- |
| `update_table_clustering(table_id, clustering_fields=[...])` on a typed BigQuery table | Storage job runs, the tool waits for it and returns the job id/status and `clustering_fields` as stored (read back from the table detail). |
| `clustering_fields=null` | Sends `clustering: null`; the clustering is removed; the output has `clustering_fields: null`. |
| `clustering_fields=[]` | Tool error, Storage is not called. |
| Other backend, alias, external bucket, unknown or repeated column, more than 4 columns | Storage answers 422; the tool fails with Storage's message. |
| Table not found | Tool error `Table "..." not found.` |
| `X-Read-Only-Mode` | The tool is not listed (write tool, `readOnlyHint=False`). |
| `get_tables` with `table_ids` | The table detail carries `clustering_fields` (joined with `|`), `time_partitioning`, `range_partitioning` and `require_partition_filter` when Storage returns them. `definition.partitions` is never copied. |
| Governed project (column policy) | The clustering keeps only allowed columns; a partitioning on a hidden column is dropped. |

BigQuery applies a new clustering only to data written after the change; existing rows keep their order until they
are rewritten. The tool description says so.

## Resolution Strategy

- `clients/storage.py` `table_definition_update()`: new arguments `clustering_fields` and `remove_clustering`
  (mutually exclusive). They map to `clustering: {"fields": [...]}` and `clustering: null` in the same PUT. A request
  with `clustering` always creates a job in Storage, so the description no-op check (`_is_unchanged_rejection`) is
  skipped for it and a 400 is raised.
- `tools/storage/tools.py`: new tool `update_table_clustering(table_id, clustering_fields, project_id)` with
  annotations `destructive, idempotent`, tag `storage`. It reuses `_resolve_write_target()` for the branch, calls the
  client, then reads `definition.clustering.fields` back from the table detail, because the job result
  (`TableCreateResponse`) has no clustering. Errors propagate and `@tool_errors()` reports them as tool errors.
- `TableDetail` gets the four read-only layout fields, filled in `_get_table()` from `definition`.
  `_limit_to_policy()` limits them to the column policy.
- Alternatives considered: an optional `clustering` on `update_descriptions` (the 422 would be hidden in per-item
  results, and the tool name stops describing what it does); a general `update_table_definition` tool (repeats the
  description write and needs a deprecation plan for `update_descriptions`).

## Scope

In scope: `clustering` in the client, the new tool, the read-only layout fields in the table detail, tool-list
snapshots, `TOOLS.md`, version bump.

Out of scope: any write of the partitioning (BigQuery cannot change it on an existing table); `definition.partitions`;
changes to `update_descriptions`; adding the tool to `DIRECTORY_TOOLS`; clustering at table create time; MCP-side
checks of `isTyped`, backend, alias, column count or duplicates (Storage decides; only the empty list is rejected
locally); rewriting existing rows.

Unverified: how Storage handles clustering on an untyped BigQuery table (the processor has no `isTyped` check).

## Testing / Verification

- Unit: `tests/clients/test_storage.py::TestTableDefinitionUpdate` (payloads, invalid arguments, a 400 on a clustering
  request is raised), `tests/tools/storage/test_tools.py` (`test_update_table_clustering`,
  `test_update_table_clustering_errors`, `test_get_table` with clustered and partitioned BigQuery tables,
  `test_limit_to_policy_clustering_fields`), `tests/test_server.py` snapshots.
- Integration: `integtests/tools/test_storage.py::test_update_table_clustering_bigquery` creates a typed table, sets
  and removes the clustering, reads it back from the table detail and `get_tables`, and checks the unknown-column
  error; `test_update_table_clustering_rejected_on_snowflake` checks the backend error.
