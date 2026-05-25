# RFC: Per-Project SQLite FTS5 Search Index

Linear: [AI-3236](https://linear.app/keboola/issue/AI-3236/search-is-very-slow)

## Problem

The `search` tool (`src/keboola_mcp_server/tools/search.py:554`) and `search_semantic_context` (`src/keboola_mcp_server/tools/semantic/service.py:602`) hit the Keboola Storage API and Metastore API on every invocation. Each call fetches the full bucket list, table list, configuration list, flow list, and semantic objects, then performs in-memory regex matching across the merged result set.

Visible symptom: slow, unreliable search — surfaced as user complaints in the issue. Latency scales with project size (number of buckets, tables, configs) and there is no caching between calls.

This is correct architecturally for an always-live view but does not match the actual access pattern: search is a read-heavy lookup that runs many times per session against data that changes slowly. We need a local index that pre-stages the same data and serves queries in milliseconds.

## Required Behavior

| Aspect | Required |
| --- | --- |
| Backend | SQLite FTS5, local file per `(project_id, token_hash)` |
| Build trigger | First `verify_token()` call in a session — kicks off a background build task; never blocks the caller |
| Freshness target | Index serves queries that reflect project state at most ~30 minutes old |
| Rebuild trigger | Search call against an index whose `mtime ≥ 30 min` schedules a background rebuild; current (stale) DB continues to serve until rebuild completes |
| Cold-start behavior | If a search arrives before the first build finishes, the call awaits the in-flight build task |
| Concurrency | At most one rebuild task per `(project_id, token_hash)` at any time |
| Cross-tenant isolation | A request authenticated with token T may **only** read the index built with token T. No tool parameter accepts `project_id` or `branch_id` from the LLM |
| Branch scope | Index covers the **default branch only**. Calls with an explicit dev-branch context fall through to the existing live-API path |
| Failure handling | Token verify failure → reject query, do not fall back to a stale index. Rebuild failure → keep current DB, log, retry on next stale search |
| Indexed object types (phase 2) | Buckets, tables (incl. column metadata) |
| Indexed object types (phase 3) | Configurations, flows, transformations, data apps, semantic objects |
| Persistence | DB files survive process restart. In-memory build-task bookkeeping does not — recovers from mtime on next request |

### Threat model

The adversary is the LLM acting on prompt-injected or user-supplied instructions. The only attack of concern is **cross-tenant data leak**: tricking the server into reading another project's index.

| Vector | Mitigation |
| --- | --- |
| LLM passes `project_id` argument | Tool signatures do not accept `project_id` or `branch_id` parameters. FastMCP schema validation rejects unknown args |
| Path traversal via session config | `project_id` and `token_hash` are validated against `^[A-Za-z0-9_-]+$` before being used as path components |
| Index file shared between users with different permissions on the same project | Per-token isolation: each token gets its own DB file under `<project_id>/<sha256(token)[:16]>/default.db` |
| Stamped row escapes file segregation | Every row carries `project_id`; every `SELECT` filters `WHERE project_id = ?` (defense in depth) |
| Stale `VerifiedSession` from old token | TTL on `VerifiedSession`; re-verify on expiry; reject query if verify currently fails |

## Resolution Strategy

### Module layout

New module `src/keboola_mcp_server/search_index/`:

| File | Responsibility |
| --- | --- |
| `types.py` | `VerifiedSession` (frozen dataclass: `project_id`, `token_hash`, `verified_at`); `IndexBuildState` (in-memory bookkeeping) |
| `verify.py` | `verify_and_cache(client, ctx) -> VerifiedSession` — wraps `AsyncStorageClient.verify_token()`; caches result on `ctx.session.state` with TTL |
| `storage.py` | Path resolution (`path_for(session) -> Path`), input sanitization, file locking (`fcntl.flock`), atomic write helpers, 0600/0700 permissions, schema bootstrap |
| `builder.py` | Fetches data via `KeboolaClient`, populates FTS5 in a `.db.tmp` file, atomic rename. Per-`kind` builders (buckets, tables, configs, flows, semantic) for phased rollout |
| `query.py` | Read-only FTS5 query with mandatory `WHERE project_id = ?` clause; result mapping to existing search result models |
| `lifecycle.py` | `ensure_index_for_session()` (called from verify hook) and `query_or_wait()` (called from search tool); manages `_builds: dict[(project_id, token_hash), IndexBuildState]` with `asyncio.Lock` |

### Hook into session lifecycle

`src/keboola_mcp_server/mcp.py:237` (`SessionStateMiddleware.create_session_state`): after the `KeboolaClient` is constructed and stored in session state (line 268), call `verify_and_cache()` to populate `state['verified_session']`. On success, schedule `asyncio.create_task(lifecycle.ensure_index_for_session(session, client))` — non-blocking. Verify failure (invalid/revoked token) propagates as before.

Default-branch detection: `config.branch_id is None` after normalization (`src/keboola_mcp_server/config.py:66-67`). When a branch is explicitly set, the verify hook still runs (project_id binding is useful elsewhere) but `ensure_index_for_session` is a no-op for that session — the search tool falls through to live API.

### Index schema

```sql
CREATE VIRTUAL TABLE search USING fts5(
    project_id UNINDEXED,
    kind UNINDEXED,            -- 'bucket' | 'table' | 'config' | 'flow' | 'semantic_*'
    obj_id UNINDEXED,
    name,
    description,
    content,                   -- concatenated searchable text (column names, config keys, tags…)
    metadata UNINDEXED,        -- JSON blob with the original record for result rehydration
    tokenize='porter unicode61'
);
CREATE INDEX IF NOT EXISTS idx_kind ON search(kind);

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
-- meta rows: 'project_id', 'token_hash', 'built_at_iso', 'schema_version'
```

### Search tool integration

`src/keboola_mcp_server/tools/search.py:554` (`search`): before the existing live-API path, check `verified_session` and (default branch) call `lifecycle.query_or_wait(session, query, kinds, limit)`. On a hit, return the indexed result. On miss (build task failed, branch is not default, etc.), fall through to the existing live implementation — feature is additive, never regresses.

`src/keboola_mcp_server/tools/semantic/service.py:602`: same pattern, narrower scope (semantic kinds only).

### Failure modes & escape hatch

- Disk write failure during rebuild → log, keep old DB. If no DB exists yet, search falls back to live API.
- Repeated rebuild failures → circuit breaker per `(project_id, token_hash)` (defer to phase 4): after N consecutive failures, skip background scheduling for M minutes.
- Process restart mid-rebuild → leftover `.db.tmp` cleaned up on first `ensure_index_for_session()` for that key.
- `verify_token()` returns a different `owner.id` than before for the same token (rotation) → invalidate index file for the old project_id, treat as cold start.

### Non-obvious trade-offs

1. **Build at verify, not at first search.** Verify is the earliest moment we have a confirmed `project_id`. Triggering at verify gives the user a head start: while they read output of the first non-search tool call (often `list_buckets` or similar), the index is being built in the background. Triggering at first search means a synchronous wait on the only call that needs the index. Cost of the verify-time approach: build runs even if the session never searches. Accepted — build is cheap, disk is cheap, and the cost falls on the server, not on the user.

2. **Per-token isolation instead of shared per-project.** A shared per-project index would be smaller and rebuilt less often, but two tokens with different permissions on the same project would see each other's data (existence info leak — token A learns about buckets it has no read permission for via search hits). Per-token isolation removes this leak at the cost of duplicated index files. For typical projects (1–10 active tokens) this is a few hundred MB at most.

3. **Index lives on local disk, not in a shared store.** A shared store (Redis, S3) would let multiple MCP server replicas share build cost. The streamable HTTP MCP server is typically deployed as a single process per region today, and replica fan-out can be added later by swapping the `storage.py` backend without changing the surface API.

4. **No active eviction in MVP.** DB files accumulate. Disk cleanup deferred to a later phase (LRU eviction by access time, 30-day window). Operators can clear `KBC_SEARCH_INDEX_DIR` safely at any time — the next request rebuilds.

### Phased rollout

| Phase | Deliverable | Status |
| --- | --- | --- |
| 1 | `search_index/` scaffolding: `types`, `verify`, `storage`, sanitization + path-traversal tests | done |
| 2 | `builder` + `query` + `lifecycle`. Index `bucket` and `table` kinds. Wire `search` tool to use index for those kinds when the branch is default | done |
| 3 | Extend builder to index `flow`, `transformation`, `configuration`, `configuration-row`, `data-app`, `workspace`. After Phase 3, `search` with textual + literal mode is fully index-served for all indexed kinds | done |
| 4 | Remove live-API fallback for indexed object types; add circuit breaker + observability metrics. Add semantic-object indexing for `search_semantic_context` (separate work item) | future |

### Phase 3 schema additions

Component-derived rows reuse the same FTS5 table; the kind discriminator separates them. Each row's ``obj_id`` is a synthetic, unique-within-index key, with the original Keboola IDs preserved in the ``metadata`` JSON for hydration into ``SearchHit``.

| Kind | `obj_id` format | `metadata` JSON fields |
| --- | --- | --- |
| `configuration`, `transformation`, `flow`, `data-app`, `workspace` | `<component_id>:<configuration_id>` | `component_id`, `configuration_id`, `name`, `display_name`, `description`, `updated` |
| `configuration-row` | `<component_id>:<configuration_id>:<row_id>` | `component_id`, `configuration_id`, `configuration_row_id`, `name`, `description`, `updated` |

The kind classifier in ``builder._derive_component_kind`` matches ``tools/search.py::_fetch_configs`` so indexed and live results are interchangeable: same component → same kind in both paths.

## Scope

### In scope

- New `search_index/` module and integration hook in `SessionStateMiddleware`.
- Index covers default-branch project state. Per-token isolation.
- `search` tool uses index for indexed object types on default branch.
- Background rebuild on stale-on-access (30-min TTL).

### Out of scope (this RFC)

- Dev-branch indexing. Searches with `KBC_BRANCH_ID` set continue to hit live API.
- Cross-replica shared index store.
- Active LRU eviction of cache directory.
- Semantic embeddings / vector search. The FTS5 index is keyword/lexical only. Antfly, Qdrant, or similar can be slotted in as a `storage.py` backend in a future RFC.
- Indexing data outside the listed Keboola object types (e.g., job logs, telemetry).

## Testing / Verification

### Unit tests (`tests/search_index/`)

- **Path sanitization**: `project_id` values `../other`, `foo\x00bar`, `foo/bar`, `..`, `` are rejected with a clear error and never reach the filesystem.
- **Token hash determinism**: `token_hash(token)` is stable across calls, 16 hex chars, never contains the raw token.
- **File permissions**: created DB files are mode `0600`, parent dirs mode `0700`. Verified via `Path.stat().st_mode`.
- **Atomic write**: writing to `.db.tmp` then rename leaves the readable DB pointing at either the old or the new content, never a partial file. Simulate crash by interrupting the build coroutine; the old DB must remain valid.
- **Concurrent rebuild dedup**: two `asyncio.gather(ensure_index_for_session(...), ensure_index_for_session(...))` calls produce exactly one `_build()` invocation.
- **Defense-in-depth query filter**: a row inserted with `project_id='X'` is invisible to a query whose `WHERE project_id = 'Y'`, even within the same DB file (simulating future shared-file scenarios).
- **Stale detection**: `_is_stale(path, ttl=30*60)` returns `True` for `mtime` older than TTL, `False` for fresh files.

### Integration tests (`integtests/test_search_index_e2e.py`)

- Fresh project, no DB present → first session triggers build → second search returns indexed results matching what live API would return.
- Index built, project state changes externally (new bucket created) → search within 30 min still returns stale view; after 30 min next search triggers rebuild and surfaces the new bucket.
- Two parallel sessions for the same `(project, token)` → only one rebuild observed (log assertion).
- Session with `KBC_BRANCH_ID` set → search falls through to live API; no index file created.
- Token rotated (different `owner.id` returned) → old index ignored; new index built under new path.

### Manual verification

- Profile a real project with ≥100 buckets and ≥1000 tables: search latency before vs after, on warm index.
- Measure background build time end-to-end; confirm it does not exceed reasonable bounds (~30s upper bound for very large projects).
- Inspect cache directory after a session: confirm path layout `<project_id>/<token_hash>/default.db` and permissions.
