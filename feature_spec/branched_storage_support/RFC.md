# RFC: Branched Storage Support (`storage-branches`)

## Problem

The Keboola platform introduced a new project feature `storage-branches` that changes how
dev branch storage objects are served by the API. The MCP server must support both the new
behavior and the legacy behavior for projects that haven't migrated.

## Background

### Legacy branches (without `storage-branches`)

- The default API endpoint returns **all** buckets/tables — both production and branched
- Branched objects have the branch ID embedded in their ID: `in.c-BRANCH_ID-bucketname`
- The FQN schema in the warehouse matches the ID: `"DB"."in.c-BRANCH_ID-bucketname"`
- The branch-scoped endpoint (`branch/{id}/buckets`) returns **empty**

### New `storage-branches` feature

- The default endpoint returns **only production** objects
- The branch-scoped endpoint returns **only branched** objects
- Branched objects have **production-like IDs** (no branch ID embedded): `in.c-bucketname`
- The FQN schema uses a prefix pattern: `"DB"."BRANCH_ID_in.c-bucketname"`
- The bucket's `backendPath` field contains the correct warehouse schema for all patterns

### What the UI does (reference)

In short: the UI makes two
parallel API calls (branch + production), merges them with "branch wins on ID collision",
and presents a unified view.

## Required Behavior

### Deference mechanism (presentation to the LLM)

The MCP server presents storage objects to the LLM as if they were all production objects:

- **Bucket/table IDs**: Always production-like (branch ID stripped or never present)
- **Branch metadata**: Hidden (`branch_id` excluded from serialization)
- **Data merging**: Production + current branch, branch wins on overlap. Other branches filtered out.
- **Links**: Point to the branch UI when on a dev branch

This applies identically for both legacy and new branching — the LLM sees the same
unified view regardless of which system the project uses.

### FQN for queries (`query_data` tool)

The fully qualified name must reference the actual warehouse schema:

| Context | FQN Schema |
|---------|-----------|
| Production table | `in.c-bucketname` |
| Legacy branch table | `in.c-BRANCH_ID-bucketname` |
| `storage-branches` table | `BRANCH_ID_in.c-bucketname` |

The bucket's `backendPath` API field provides the correct schema for all cases.

### Data fetching strategy

| Context | Fetch strategy |
|---------|---------------|
| On production branch | Single fetch from default endpoint |
| On dev branch (legacy) | Single fetch from default endpoint (returns everything), filter other branches |
| On dev branch (`storage-branches`) | Parallel fetch from default + branch endpoints, merge by ID |

### Scenarios (all must work for both legacy and new)

1. **Branched table from existing production table**: Listed under production bucket with
   production-like ID. FQN points to branched warehouse schema.

2. **New table in existing bucket (branch only)**: Appears as if it exists in production.
   Listed in the production bucket alongside other tables.

3. **New bucket with table (branch only)**: Bucket and table appear with production-like
   IDs as if they exist in production.

## API Response Observations

Verified via direct API calls against test projects:

- **`branch/default/buckets`**: Returns production-only on `storage-branches` projects;
  returns everything (including branched) on legacy projects
- **`branch/{id}/buckets`**: Returns branched-only on `storage-branches` projects;
  returns empty on legacy projects
- Branched buckets on `storage-branches` have `idBranch` field and
  `KBC.createdBy.branch.id` metadata (same as legacy)
- `backendPath` on buckets gives `[db_name, schema_name]` with the correct warehouse
  schema for any pattern

## Scope and Constraints

- `update_descriptions` on branched tables is out of scope (descriptions don't persist on
  merge) — tracked separately
- The `storage-branches` feature will eventually be rolled out to all projects and the
  feature flag removed. When that happens, the legacy branch handling must be removed
  (tracked as AI-3044)
- The feature is currently Snowflake-only

## Testing

- **Unit tests**: Both legacy and `storage-branches` scenarios parametrized
- **Integration tests**: Two dedicated projects outside the pool — one with feature, one
  without. Each test session creates branches, runs Python transformations to produce
  branched data, validates the deference mechanism, then cleans up branches. Production
  data is idempotent so concurrent sessions are safe.