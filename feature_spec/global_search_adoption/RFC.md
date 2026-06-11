# RFC: Adopt server-side Storage search endpoints in the `search` tool

Linear: [AI-3393](https://linear.app/keboola/issue/AI-3393/adopt-server-side-storage-search-endpoints-in-the-mcp-search-tool)

## Problem

The `search` tool (`src/keboola_mcp_server/tools/search.py`) implements **client-side
search by full enumeration**. On every textual search call it:

1. Lists all buckets — `merged_bucket_list()` (1–2 API calls, prod + branch).
2. Lists tables **per bucket** with `include=columns,columnMetadata` — N API calls for
   N buckets (`_fetch_tables`).
3. Lists all components with `include=configuration,rows` per component type —
   up to 4 API calls (`_fetch_configs`).
4. Regex/literal-matches everything in Python, sorts, and paginates client-side.

Visible symptoms:

- **Latency** scales with project size (a project with hundreds of buckets fires
  hundreds of sequential table-list calls per search).
- **SAPI load**: every agent search downloads the entire project inventory, including
  full configuration JSON bodies even for textual searches that never look at them.
- Pagination (`limit`/`offset`) is cosmetic — the full dataset is fetched regardless.

Storage API now provides server-side search endpoints that eliminate the enumeration:

| Endpoint | Capability |
| --- | --- |
| `GET /v2/storage/global-search` | Full-text **name** search across item types, filterable by `projectIds`, `types`, `branchTypes`, `branchIds`; native `limit`/`offset` and total count |
| `GET /v2/storage/search/tables` | Exact-match table search by `metadataKey` / `metadataValue` / `metadataProvider` |
| `GET /v2/storage/branch/{branchId}/search/component-configurations` | Configuration search by `componentId`, `configurationId`, `metadataKeys` |

Two of the three are already partially wired in `clients/storage.py`:
`global_search()` (`storage.py:842`, currently used only by integtests) and
`component_configurations_search()` (`storage.py:614`, used only for data-app folder
lookups). `search/tables` is not wired at all.

### Prior art (why this is a round trip)

The `search` tool was originally global-search based (AI-1172) and was then rewritten
to client-side enumeration in AI-1838. The driver was capability coverage: global
search indexes **names only**, is gated by the `global-search` project feature, and
cannot serve config-content search. This RFC re-adopts global search for what it is
good at (textual discovery) while explicitly keeping what it cannot do (config-content
search) on the existing client-side path, and defines a feature-flag fallback so we do
not regress projects where global search is unavailable.

## Required Behavior

### 1. Textual search goes server-side

`search(search_type='textual')` must call `GET /v2/storage/global-search` instead of
enumerating buckets/tables/configurations. Scope is always the **current project**:
`projectIds=[<project id from tokens/verify>]`.

Tool signature stays compatible:

| Parameter | Behavior after change |
| --- | --- |
| `patterns: list[str]` | Kept. One global-search request per pattern, issued concurrently; results OR-merged and deduplicated by `(type, id)`. |
| `item_types` | Kept. Mapped to the API `types[]` parameter (mapping below). |
| `search_type` | Kept. `textual` → global-search; `config-based` → existing client-side path (unchanged). |
| `scopes` | Kept, config-based only (unchanged). |
| `mode` | **Deprecated for textual search.** Global search is a full-text index; `regex` cannot be honored server-side. `mode='regex'` + `search_type='textual'` returns a clear tool error instructing to use a plain query (or config-based search). `mode` remains honored for config-based search. |
| `limit` / `offset` | Kept, passed natively to the API. With multiple patterns, each request uses the caller's `limit` and merged results are re-sorted and truncated to `limit` (documented approximation). |

`item_types` → API `types[]` mapping (`SearchItemType` → `ItemType` in
`clients/storage.py:25`):

| Tool `item_types` value | API `types[]` | Post-filter |
| --- | --- | --- |
| `bucket`, `table`, `transformation`, `configuration`, `configuration-row`, `workspace`, `flow` | same value | — |
| `data-app` | `configuration` | keep only `componentId == DATA_APP_COMPONENT_ID` |
| `component` | `configuration`, `configuration-row` | — (mirrors current `_validate_item_types` expansion) |
| empty | omitted (all types) | — |

### 2. Branch-aware search with project-wide fallback

The search must prefer the branch the MCP session is operating on, then widen:

1. **Primary query — current branch context:**
   - On the default branch (`client.branch_id is None` / storage client
     `_branch_id == 'default'`): `branchTypes[]=production`, no `branchIds`.
   - On a dev branch: `branchTypes[]=development`, `branchIds[]=<current branch id>`.

   (This is exactly what `global_search()` does today — the logic moves to an explicit
   parameter so the tool controls it; see Resolution Strategy.)

2. **Fallback query — whole project, any branch:** executed only when the primary
   query returns **zero hits across all patterns**. Same `query`/`types`/`projectIds`,
   but **no `branchTypes` and no `branchIds`** — so items living in other dev branches
   (or in production, when searching from a dev branch) are found.

3. Hits returned from the fallback must be **clearly attributed to their branch** so
   the agent does not mistake another branch's item for one in its own context. The
   API's `fullPath.branch` provides id/name; surface it on the hit (new fields, see
   below).

### 3. Result shape

`SearchHit` is kept as the output model, populated from `GlobalSearchResponse.Item`:

| `SearchHit` field | Source |
| --- | --- |
| `bucket_id` / `table_id` / `component_id` / `configuration_id` / `configuration_row_id` | `item.id`, `item.component_id`, and `item.full_path` (bucket/configuration parents), keyed by `item.type` |
| `item_type` | `item.type` (+ `data-app` re-typing by component id, mirroring `_fetch_configs`) |
| `name` | `item.name` |
| `updated` | `item.created` (the index does not expose update time; rename or document) |
| `display_name`, `description` | **No longer populated** for textual search — not in the index. Fields stay on the model (config-based path and compatibility). |
| `matches` | Empty for textual search (no per-field match attribution from the API). |
| `links` | Unchanged (`ProjectLinksManager`). |

New fields on `SearchHit`:

- `branch_id: str | None`, `branch_name: str | None` — populated from
  `fullPath.branch` when present; lets the agent see fallback hits live elsewhere.

New top-level information: the API returns `all` (total count) and `byType`. Return
hits wrapped in a small output model (`SearchOutput(hits=..., total=..., by_type=...)`)
— this resolves the existing `TODO: Should we report the total number of hits?` at
`search.py:805`. (Tool output shape change → minor version bump, TOOLS.md regen.)

### 4. Feature flag handling

`global-search` is a project feature (`ProjectFeature` literal, checked via
`AsyncStorageClient.is_enabled()`).

- If enabled → server-side path.
- If **not** enabled → fall back to the **legacy client-side textual search**
  (current code, kept in a clearly named module/function), and log a warning.
- The legacy path is removed in a follow-up PR once the feature is confirmed GA on all
  production stacks (tracked as a separate Linear issue). The RFC explicitly does
  **not** gamble on availability — that is what caused the AI-1838 reversal.

### 5. Config-based search is unchanged

`search_type='config-based'` keeps the existing client-side implementation
(`fetch_configurations` / `SearchSpec.match_configuration_scopes`). Global search
indexes names only — there is no server-side substitute for searching configuration
JSON content. Importantly, the config-based path **never enumerates buckets/tables**,
so the expensive part of today's tool is fully covered by the textual migration.

### 6. Adopt the two metadata search endpoints in the client layer

- `search/tables`: new `AsyncStorageClient.tables_search(metadata_key=None,
  metadata_value=None, metadata_provider=None, include=None)` client method. First
  consumer: none in the `search` tool (it is exact-match metadata lookup, not
  free-text); it is groundwork for metadata-driven discovery (e.g. find tables
  carrying a given `KBC.*` key) and for replacing ad-hoc enumeration elsewhere.
- `search/component-configurations`: extend the existing
  `component_configurations_search()` to pass `componentId` (and optionally
  `configurationId`, `include`) **server-side** instead of the current client-side
  post-filter (`storage.py:634-636`).

### Accepted capability regressions (textual search)

These must be stated in the tool docstring so agents adapt:

1. **Descriptions are not searched** (item descriptions, bucket/table descriptions).
2. **Table column names / column descriptions are not searched** (today's
   `_check_column_match`). Mitigation: agents use `get_tables` for column-level
   detail, or config-based search for mappings.
3. **Regex mode is not supported** for textual search.
4. **ID-substring matching is not guaranteed** — the index matches names; searching
   for `in.c-prod.customers` may not hit by ID. Mitigation: docstring directs exact-ID
   lookups to `get_*` tools (it already does).

In exchange: single-digit API calls per search, server-side pagination, true total
counts, and results independent of project size.

## Resolution Strategy

### `clients/storage.py`

1. Refactor `global_search()` (`storage.py:842`): extract the hard-coded branch logic
   into an explicit parameter, e.g.
   `branch_scope: Literal['current', 'all'] = 'current'`:
   - `'current'` → today's behavior (`branchTypes` / `branchIds` from `_branch_id`),
   - `'all'` → no branch filters.
   Keep `projectIds=[await self.project_id()]` inside the method (single-project
   invariant of the MCP server).
2. Add `tables_search()` calling `GET search/tables` (note: **not** branch-prefixed).
3. Extend `component_configurations_search()` with server-side `componentId` /
   `configurationId` / `include` params; keep the signature backward-compatible for
   the existing caller (`tools/components/utils.py:433`).
4. Extend `GlobalSearchResponse.Item` with typed access to `fullPath` branch info
   (`branch_id` / `branch_name` properties or a small `FullPath` model).

### `tools/search.py`

1. Split `search()` by `search_type`:
   - `config-based` → existing flow, untouched.
   - `textual` → new `_global_search_textual(client, spec, limit, offset)`:
     a. `await client.storage_client.is_enabled('global-search')`; if false →
        `_legacy_textual_search(...)` (today's `_fetch_buckets` / `_fetch_tables` /
        `fetch_configurations` textual flow, moved as-is).
     b. Map `item_types` → API `types` (table above).
     c. `asyncio.gather` one `global_search(query=pattern, types=..., limit=...,
        offset=..., branch_scope='current')` per pattern.
     d. If all responses have `all == 0` → repeat with `branch_scope='all'`.
     e. Merge, dedupe by `(type, id)`, map to `SearchHit` (+ branch fields), sort by
        `created` desc, truncate to `limit`, attach links.
2. Reject `mode='regex'` + `search_type='textual'` with a `tool_errors` message.
3. Update the tool docstring: new semantics, regressions list, fallback-to-all-branches
   behavior, total-count field.
4. Keep `SearchSpec` for the config-based path; the textual path no longer needs it
   (no client-side matching).

### Non-obvious trade-offs

- **Per-pattern requests vs. one joined query:** global search treats a multi-word
  query as one full-text query; joining patterns with spaces would change semantics
  (AND-ish vs. the tool's documented OR). One request per pattern preserves OR
  semantics at the cost of ≤ len(patterns) requests — still orders of magnitude
  cheaper than enumeration.
- **Fallback only on zero hits** (not always-merge): merging both scopes on every call
  would double request count and bury current-branch hits among other branches'
  items. Zero-hit fallback matches the agent intent: "prefer my branch, widen only if
  nothing found".
- **Keeping the legacy path temporarily** doubles textual-search code for one release
  cycle. Accepted: it is the safety net the AI-1838 reversal proved necessary, and it
  is deleted by a scheduled follow-up.
- **`updated` semantics change** (index exposes `created` only). Kept under the same
  field name to avoid breaking consumers; documented in the field description.

## Scope

In scope:

- `clients/storage.py`: `global_search()` refactor (+ branch scope param),
  `tables_search()`, server-side params for `component_configurations_search()`,
  `GlobalSearchResponse` branch-info typing.
- `tools/search.py`: textual path switched to global search with feature-flag fallback
  and zero-hit branch widening; `SearchHit.branch_id`/`branch_name`; `SearchOutput`
  with total/by-type counts; docstring rewrite.
- Tests (see below), TOOLS.md regen (`tox -e check-tools-docs`), minor version bump in
  `pyproject.toml` + `uv lock`.

Out of scope:

- Removing the legacy client-side textual search (follow-up issue, after GA
  verification on all stacks).
- Any change to `search_type='config-based'` behavior.
- A new tool exposing `tables_search` to agents (client-layer groundwork only).
- `find_component_id` (AI service based, unrelated).
- Cross-project search (`projectIds` stays pinned to the current project).

## Testing / Verification

Unit tests (`tests/tools/test_search.py`, `tests/clients/`):

- Parametrize the existing textual-search tests on the new axis
  `global_search_enabled` (True/False) — False must exercise the legacy path
  unchanged; True mocks `global_search()` responses.
- Branch widening: primary returns 0 hits → assert second call with
  `branch_scope='all'`; primary returns hits → assert no second call; fallback hits
  carry `branch_id`/`branch_name`.
- `item_types` mapping incl. `data-app` post-filter and `component` expansion.
- Multi-pattern OR-merge + dedupe by `(type, id)`.
- `mode='regex'` + textual → tool error.
- Client: `tables_search` and `component_configurations_search` build correct query
  params (`metadataKeys[i]`, `componentId`, includes).

Integration tests (`integtests/`):

- Extend `integtests/clients/test_client.py` global-search tests for
  `branch_scope='all'` and `tables_search`.
- `integtests/tools/test_search.py`: textual search via global search finds a table
  created by the test fixture (skip when `global-search` feature is off, mirroring
  `test_global_search_with_results`).

Manual verification (local MCP via `.mcp.json`):

1. Default branch: search a known table name → found via 1 API call path; verify hit
   links and total count.
2. Dev branch (`KBC_BRANCH_ID` set): search an item that exists only in production →
   primary query empty, fallback finds it, hit shows production branch attribution.
3. Project without `global-search` feature: tool behaves exactly as today (legacy
   path), warning logged.
4. `tox` — pytest, black, flake8, check-tools-docs all exit 0.

## Open Questions (verify before/while implementing)

1. **Index coverage:** does global search match `displayName` and ID substrings, or
   names only? Determines the final wording of the "accepted regressions" docstring
   section. (Verify empirically against a real stack.)
2. **Match style:** prefix vs. substring vs. tokenized full-text — affects guidance on
   multi-word patterns in the docstring.
3. **`types` support:** confirm `rows`/`state`/`shared-code` behave as expected when
   requested explicitly (they are in `ItemType` but not in the tool's
   `SearchItemType` mapping today).
4. **`search/tables` matching semantics:** exact-match only or partial on
   `metadataValue`? Determines whether it could later serve description search
   (`KBC.description`) as a follow-up.
5. **Multi-pattern + offset:** with >1 patterns, offset-based pagination over a merged
   list is approximate. Acceptable, or should multi-pattern searches cap at one page?
