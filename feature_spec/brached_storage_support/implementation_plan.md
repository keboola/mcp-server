# Implementation Plan: `storage-branches` Feature Support

## Context

The Keboola MCP server already supports dev branches via a "deference" mechanism where branched buckets/tables (with IDs like `in.c-BRANCH_ID-bucket`) are merged with production versions and presented with production IDs to the LLM. The FQN for queries uses the actual branched ID (`"DB"."in.c-BRANCH_ID-bucket"."table"`).

A new project feature `storage-branches` changes how the Storage API works:
1. **Default endpoint** returns ONLY production objects (branched ones are invisible)
2. **Branch-scoped endpoint** (`branch/{id}/buckets`, etc.) returns ONLY branched objects
3. **New FQN pattern**: schema changes from `in.c-BRANCH_ID-bucket` to `BRANCH_ID_in.c-bucket`

We need to support both old and new behavior based on the project feature flag.

---

## API Response Format (Verified via curl)

### New `storage-branches` project (token: STORAGE_TOKEN_BRANCHED, branch 35403)

**Production endpoint** (`branch/default/buckets`):
- Returns only production buckets: `id: "in.c-shopify-data"`, `backendPath: ["KBC_USE4_3047", "in.c-shopify-data"]`

**Branch endpoint** (`branch/35403/buckets`):
- Bucket IDs are **production-like** (no branch ID in ID): `id: "out.c-random-data"`
- `backendPath` has branch prefix: `["KBC_USE4_3047", "35403_out.c-random-data"]`
- `idBranch: 35403` field present
- `KBC.createdBy.branch.id: "35403"` metadata IS present
- Table IDs are also prod-like: `id: "out.c-random-data.customers"`
- Table detail's nested `bucket` object includes `backendPath` with correct schema

### Old branches project (token: STORAGE_TOKEN_OLD_BRANCHES, branch 35402)

**Default endpoint** returns BOTH production and branched buckets:
- Branched: `id: "out.c-35402-python"`, `backendPath: ["KBC_USE4_361", "out.c-35402-python"]`
- Branch endpoint for 35402 returns **empty** (branched buckets live in default endpoint)

### Key implications
- Existing `set_branch_id` validator already works for both: `replace(f'c-{branch_id}-', 'c-')` is a no-op when ID is prod-like, and strips correctly for old pattern
- `shade_by` validation passes for both (prod_id matches in both cases)
- `backendPath` on the bucket gives the correct warehouse schema for ALL cases — no manual prefix construction needed

---

## Implementation Steps

### Step 1: Add `storage-branches` to ProjectFeature type

**File**: `src/keboola_mcp_server/clients/storage.py` (line 17)

```python
ProjectFeature = Literal['global-search', 'storage-branches']
```

### Step 2: Add feature detection helper with caching

**File**: `src/keboola_mcp_server/clients/client.py`

Add a cached `has_feature(feature)` method to `KeboolaClient`:

```python
async def has_feature(self, feature: str) -> bool:
    if self._features_cache is None:
        token_info = await self.storage_client.verify_token()
        owner = token_info.get('owner', {})
        self._features_cache = set(owner.get('features', []))
    return feature in self._features_cache
```

### Step 3: Update existing AsyncStorageClient bucket/table methods

**File**: `src/keboola_mcp_server/clients/storage.py`

Add optional `branch_id` override parameter to bucket/table methods. The API always supports `branch/{id}/` prefix (`default` for main branch):

```python
async def bucket_list(self, include=None, branch_id=None):
    bid = branch_id or self._branch_id
    params = {}
    if include:
        params['include'] = ','.join(include)
    return cast(list[JsonDict], await self.get(endpoint=f'branch/{bid}/buckets', params=params))

async def bucket_detail(self, bucket_id, branch_id=None):
    bid = branch_id or self._branch_id
    return cast(JsonDict, await self.get(endpoint=f'branch/{bid}/buckets/{bucket_id}'))

async def bucket_table_list(self, bucket_id, include=None, branch_id=None):
    bid = branch_id or self._branch_id
    # ...same pattern

async def table_detail(self, table_id, branch_id=None):
    bid = branch_id or self._branch_id
    return cast(JsonDict, await self.get(endpoint=f'branch/{bid}/tables/{table_id}'))
```

Other methods (metadata, components, workspaces) remain unchanged since they already use `branch/{self._branch_id}/`.

### Step 4: Update model validators (minimal change)

**File**: `src/keboola_mcp_server/tools/storage/tools.py`

The existing `set_branch_id` validators already handle both patterns correctly because:
- For storage-branches: metadata has `KBC.createdBy.branch.id: "35403"`, ID is `out.c-random-data`, `replace('c-35403-', 'c-')` → no-op → `prod_id = "out.c-random-data"` ✓
- For old branches: metadata has `KBC.createdBy.branch.id: "35402"`, ID is `out.c-35402-python`, `replace('c-35402-', 'c-')` → `"out.c-python"` ✓

Add `_forced_branch_id` support as a **safety fallback** (in case some edge case doesn't have the metadata):

```python
@model_validator(mode='before')
@classmethod
def set_branch_id(cls, values: dict[str, Any]) -> dict[str, Any]:
    forced_branch_id = values.pop('_forced_branch_id', None)
    branch_id = get_metadata_property(values.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
    if not branch_id and forced_branch_id:
        branch_id = forced_branch_id
    # ... rest unchanged
```

### Step 5: Update `_list_buckets`

**File**: `src/keboola_mcp_server/tools/storage/tools.py` (line 574)

When `storage-branches` is enabled AND we're on a branch:
1. Fetch production buckets via `bucket_list(branch_id='default')`
2. Fetch branch buckets via `bucket_list(branch_id=client.branch_id)`
3. Tag branch results with `_forced_branch_id` (safety fallback)
4. Combine into `raw_bucket_data` — existing grouping-by-`prod_id` logic handles the merge

Use `asyncio.gather()` for parallel fetching of both endpoints. Without `storage-branches` feature, single call as before (existing behavior).

### Step 6: Update `_find_buckets`

**File**: `src/keboola_mcp_server/tools/storage/tools.py` (line 466)

When `storage-branches` is enabled:
- Fetch prod version via `bucket_detail(bucket_id, branch_id='default')`
- Fetch dev version via `bucket_detail(bucket_id, branch_id=client.branch_id)` (same prod-like ID)
- Tag dev result with `_forced_branch_id`
- Skip the old `c-` → `c-BRANCH_ID-` ID manipulation

When feature is NOT enabled: existing logic unchanged.

### Step 7: Update `_get_table`

**File**: `src/keboola_mcp_server/tools/storage/tools.py` (line 721)

When `storage-branches` is enabled:
- Fetch prod table via `table_detail(table_id, branch_id='default')`
- Fetch dev table via `table_detail(table_id, branch_id=client.branch_id)` (same prod-like ID)
- Tag dev_table with `_forced_branch_id`
- Skip the old ID manipulation (`c-` → `c-BRANCH_ID-`)

The raw_table dict will naturally carry `bucket.backendPath` from the API response.

### Step 8: Update `_list_tables`

**File**: `src/keboola_mcp_server/tools/storage/tools.py` (line 817)

`_find_buckets` (called at line 827) already returns correct prod/dev buckets. For the dev_bucket path (line 844), pass `branch_id=client.branch_id` to `bucket_table_list()` and tag results with `_forced_branch_id`.

### Step 9: Update FQN building to use `backendPath`

**File**: `src/keboola_mcp_server/workspace.py`

The table detail API response includes `bucket.backendPath` which gives `[db_name, schema_name]` with the correct warehouse schema for any branching pattern:
- Production: `['KBC_USE4_3047', 'out.c-random-data']`
- Old branch: `['KBC_USE4_361', 'out.c-35402-python']`  
- New storage-branches: `['KBC_USE4_3047', '35403_out.c-random-data']`

#### Snowflake (`_SnowflakeWorkspace.get_table_info`, line 204):

Use `bucket.backendPath` when available, fallback to existing rsplit logic:

```python
bucket_backend_path = table.get('bucket', {}).get('backendPath')
if bucket_backend_path and len(bucket_backend_path) >= 2:
    # Use backendPath directly — handles all branch patterns
    db_name = bucket_backend_path[0]  # or keep CURRENT_DATABASE() result
    schema_name = bucket_backend_path[1]
    table_name = table['name']
elif '.' in table_id:
    schema_name, table_name = table_id.rsplit(sep='.', maxsplit=1)
```

Note: For Snowflake, we may still need `CURRENT_DATABASE()` for `db_name` since `backendPath[0]` might differ from the actual connected database. Verify with testing.

#### BigQuery (`_BigQueryWorkspace.get_table_info`, line 477):

Same pattern. `backendPath[1]` already contains the correctly formatted schema, but verify whether BigQuery needs `.`/`-` → `_` replacement on it (likely already done server-side since it's the actual backend path).

### Step 10: Set up local testing config

**File**: `local_testing/branched_storage/.mcp.json`

Create MCP configs for both test scenarios pointing to local server.

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/keboola_mcp_server/clients/storage.py` | Add `storage-branches` to `ProjectFeature`; add `branch_id` param to `bucket_list`, `bucket_detail`, `bucket_table_list`, `table_detail` + use `branch/{id}/` prefix |
| `src/keboola_mcp_server/clients/client.py` | Add `has_feature()` with caching |
| `src/keboola_mcp_server/tools/storage/tools.py` | Minor `set_branch_id` update (x2); update `_list_buckets`, `_find_buckets`, `_get_table`, `_list_tables` for dual-endpoint fetching |
| `src/keboola_mcp_server/workspace.py` | Update `get_table_info` in both Snowflake/BigQuery classes to use `bucket.backendPath` |
| `tests/tools/storage/test_tools.py` | Add parametrized test cases for `storage-branches` scenarios |
| `tests/tools/test_sql.py` | Add FQN test cases for `backendPath` usage |
| `local_testing/branched_storage/.mcp.json` | MCP config for manual testing |

---

## Test Strategy

### Unit Tests
- **Model validators**: Verify `prod_id` derivation works for both ID patterns
- **`_list_buckets`**: Mock both endpoint calls, verify merge (branch wins), verify backward compat
- **`_find_buckets`**: Test `branch_id` override used with storage-branches
- **`_get_table`**: Verify dev table fetched from branch endpoint, FQN uses `backendPath`
- **`_list_tables`**: Verify tables from both endpoints merged correctly
- **FQN building**: Verify `backendPath` is used when available; fallback to old logic otherwise

### Integration/Manual Tests
Use tokens from `local_testing/branched_storage/.env`:
1. **With `storage-branches`** (STORAGE_TOKEN_BRANCHED, branch 35403): `get_buckets`, `get_tables`, `query_data`
2. **Without feature** (STORAGE_TOKEN_OLD_BRANCHES, branch 35402): verify old behavior unchanged
3. Test all 3 original scenarios (branched from existing, new table in existing bucket, new bucket+table)

---

## Risks

1. **`backendPath[0]` vs connected DB**: Snowflake's `CURRENT_DATABASE()` may differ from `backendPath[0]`; need to verify and possibly only use `backendPath[1]` (schema) while keeping existing db_name resolution
2. **BigQuery schema format**: `backendPath[1]` already has `_` replacements on BigQuery. However, avoid changing existing FQN logic if it would mean refactoring current working code — use `backendPath` only for the new storage-branches path and keep existing rsplit logic as fallback
3. **`shade_by` with storage-branches**: When both prod and dev have same ID, `data_size_bytes` should NOT be summed — use the branched version's size if it exists, otherwise production. Update `shade_by()` or the merge logic to handle this when storage-branches is enabled
4. **`update_descriptions` tool**: Needs `branch_id` for branched tables, but descriptions don't persist on merge — **out of scope**, separate task planned
5. **Performance**: Double API calls when on branch; mitigated with `asyncio.gather()` for parallel fetching
