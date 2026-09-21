# RLS v2 — Implementation Plan

Companion to `RFC.md` in this directory. That RFC supersedes PR #697 (`feat/rls-query-tool`,
@padak) — read it first for the *why*; this file is the task breakdown for the *how*, split by
repo since this now spans three.

## Sequencing

Tasks 1-2 (this repo) can be built and unit-tested against a mocked `MetastoreClient` in parallel
with the other two repos — the existing `tests/conftest.py` pattern
(`client.metastore_client = mocker.AsyncMock(MetastoreClient)`) already supports this. Tasks 3-6
(this repo) need real end-to-end testing against an actual project, which needs the go-monorepo
schema merged first (Task 7). The kbagent work (Task 8) needs the same schema and can otherwise
proceed independently.

---

## This repo (`keboola-mcp-server`)

### Task 1 — `rls.py`: primitive compiler + metastore-backed loader

- `_compile_primitive(condition: dict, dialect: str) -> exp.Condition`: recursively turns the
  `condition` JSON shape from the RFC into a `sqlglot.exp.Condition` tree using sqlglot's builders
  (`exp.column(...).eq(...)`, `.neq()`, `.gt()`, `.gte()`, `.lt()`, `.lte()`, `exp.In(...)`,
  `exp.And`/`exp.Or`, `exp.true()`) — never `sqlglot.parse_one` on a formatted string. One
  `RlsError` per invalid shape (unknown op, empty `and`/`or`, etc.), same style as the pilot's
  existing validation errors.
- `RlsRules.from_metastore(objects: list[MetastoreObject], *, dialect: str, project_id: int) ->
  RlsRules`: new constructor. For each object: skip it unless it applies to `project_id` (its
  `meta.source_project_id == project_id`, or `project_id in meta.target_project_ids` for a
  `targeted` object) — a policy authored for a different project must never match here, even if
  its `table` key happens to collide with a bucket/table name in this project. For every object
  that does apply: validate shape defensively (the backend schema already validates on write, this
  is defense in depth), compile every rule's `condition` via `_compile_primitive`, expand
  `principal`/`principals` into individual per-user entries, and build the same
  `tables: Mapping[str, Mapping[str, str]]` structure by storing `condition.sql(dialect=dialect)`
  — so `rewrite_query()`/`_check_output()` need zero changes.
- Delete `RlsRules.load()` (the YAML loader) and the `pyyaml` dependency it needed — the pilot
  never shipped, so there's no backward compatibility to preserve.
- Tests: mirror the existing `TestLoad` class structure in `tests/test_rls.py`, but against fixture
  `MetastoreObject`s instead of temp YAML files, including cases where `source_project_id`/
  `target_project_ids` do and don't match the calling project. New `TestCompilePrimitive` covering
  every operator, nested boolean composition, and the `true` sentinel. The ~700 lines of
  `TestRewriteQuery`/`TestOutputInvariant`/`TestDisclosure` need no changes (confirmed: they
  already construct `RlsRules(tables={...}, dialect=...)` directly).

### Task 2 — `oauth.py`: populate `user_email` (DONE — implemented this session)

- `exchange_authorization_code()` already calls `introspect_token()` once (for scope
  auto-confirm); it now reuses that same call's `Introspection.user_email` — a confirmed field
  (`user.email` in the introspection response, already read/tested elsewhere in this repo) —
  instead of the originally-planned, unverified `tokens/verify`-based approach. No second network
  call added. `_auto_confirm_project_scope` takes the already-fetched `Introspection` as a
  parameter rather than re-fetching it.
- `ProxyAccessToken` gained a `user_email: str | None` field, populated in `load_access_token()`
  from the session row — mirrors exactly how `scope_project_ids` etc. are already carried there so
  `mcp.py` doesn't pay a second DB round-trip.
- Tests: `exchange_authorization_code()` persists a non-`None` `user_email` given a mocked
  `introspect_token()` response; a missing/ambiguous identity in the response is handled explicitly
  (decide: `None` + RLS-disabled-for-this-session, vs. a hard failure — recommend the former,
  since a token that can't be identified simply can't use RLS-gated tables, it shouldn't break
  everything else the session does).

### Task 3 — `config.py` / `storage.py`: retire the old switches, add the feature flag

- Remove `Config.rls_rules_path`, `Config.rls_principal`, `Config.rls_principal_source` and their
  env/CLI/header wiring.
- Add `'row-level-security'` to `ProjectFeature` (`clients/storage.py`) and a
  `RLS_FEATURE = 'row-level-security'` constant next to `GLOBAL_SEARCH_FEATURE`
  (`tools/search_models.py` or a new `tools/sql_models.py`, match existing convention).
- Tests: `tests/test_config.py` cases for the removed fields go away; no new config surface is
  needed since the flag is a project property read via `is_enabled()`, not a `Config` field.

### Task 4 — `tools/sql.py`: one tool, per-table opt-in

- Delete `query_data_rls`, `RlsQueryDataOutput`, `_resolve_rls_principal`, `RLS_MAX_QUERY_CHARS`'s
  RLS-only framing (fold the size cap into `query_data` itself, gated the same way).
- `query_data` gains the RLS path, gated by `await client.storage_client.is_enabled(RLS_FEATURE)`:
  if off, behave exactly as today (no metastore call at all). If on: resolve the principal (Task
  2's identity), resolve the current project id, fetch/refresh `rls-policy` objects applicable to
  this project (per-call or short TTL cache — no startup load, no readiness gate, see RFC), and run
  `rewrite_query()` exactly as the pilot did, except a table with no applicable policy is left
  unfiltered rather than refused. Keep `applied_rules` in the output unconditionally (empty when
  nothing was filtered) rather than a separate output type.
- Tests: extend `tests/tools/test_sql.py` — flag off ⇒ old behavior; flag on + no applicable policy
  for a table ⇒ unfiltered; flag on + a policy that exists but doesn't apply to this project
  (wrong `source_project_id`, not in `target_project_ids`) ⇒ unfiltered, not refused; flag on +
  applicable policy + no rule for principal ⇒ refused; flag on + applicable policy + rule ⇒
  filtered + `applied_rules` populated; a query joining a protected and unprotected table filters
  only the protected one.

### Task 5 — `authorization.py`: re-trigger read-only mode (DEFERRED, not in this PR)

Current main's `authorization.py` never had the pilot's RLS-forces-read-only concept at all (it
was never merged) — `_get_authorization_config()` is purely header-based today, with no per-call
client/network calls. Adding "force read-only when the project has RLS active" means calling it
from `on_call_tool`/`on_list_tools`, which fire on **every single tool call in every project** —
unlike `query_data`'s own RLS check (paid once per `query_data` call, and only actually reaches the
metastore when the query touches a governed table), this would add a `client.has_feature(...)`
network round-trip to every tool call everywhere, including projects that never use RLS. That's a
real, broad latency cost this task's own description didn't originally account for.

Deferred rather than shipped as an unconditional per-call cost. `query_data` itself already
enforces the actual security property (a governed table's rows are always filtered or refused,
regardless of what other tools are authorized) independent of this — the read-only trigger is
secondary defense-in-depth (stopping some *other* write tool from being used as a bypass), not the
enforcement itself. Revisit with a caching strategy (e.g. resolved once per session/request context
and cached, not re-checked on every tool call) before adding this back.

### Task 6 — Docs, version, PR (DONE — this session)

- `TOOLS.md` regenerated (`tox -e check-tools-docs`) — `query_data`'s docstring gained the
  ROW-LEVEL SECURITY paragraph.
- README gained a new "Row-Level Security" section next to "Tool Authorization and Access
  Control", describing the feature flag + org-authored metastore policy model.
- `examples/rls-demo/` was never part of this repo to begin with (this PR was built fresh on top
  of current `main`, not on the pilot branch, precisely because `main` already had unrelated
  changes the pilot predates — see RFC "Relationship to the pilot") — nothing to update or remove.
- Version bumped 1.82.0 → 1.83.0 (minor — new capability, backward compatible) + `uv lock`.

---

## `go-monorepo` (metastore backend) — see RFC "Dependencies" #1

1. `services/metastore/migrations/schema/rls-policy_schema_1.0.0.json` — the schema from the RFC,
   plus `x-metastore.scope.supported: ["organization", "targeted"]` (**no `"project"`** — policy
   authorship is org-level only, never a project's own admin, even for that project's own tables;
   this is deliberate, not an oversight) and `x-metastore.acl` with `create`/`update`/`delete`
   restricted to `organization-admin` only (precedent: `tag_schema_1.2.0.json`), plus
   `x-metastore.acl.manageGrants` set (required once `targeted` scope is used, or grant management
   403s unconditionally — see `internal/authz/policy.go`).
2. `migrations/<timestamp>_add_rls_policy_schema.go` — ~22 lines, mirrors
   `migrations/20260603120000_add_reference_data_schema.go` (real precedent: commit `c078aa57`,
   adding `semantic-reference-data`, zero Go production code).
3. Fixtures under `test/repository/rls-policy-.../NNN-step/` — create/read lifecycle,
   list/filter, validation error, 403-for-non-admin, **and a case asserting `project` scope is
   rejected outright** (schema-level, not just ACL-level) — plus one line in
   `test/schema/list-all/.../expected-response.json`.
4. Optional: `services/metastore/rfc/rls-policy.md`, per this repo's own convention for new types.

**Flags before merging**:
- `organization-admin` is derived from the Storage API token's `Admin.IsOrganizationMember &&
  Admin.Role == "admin"` (`internal/authz/mapper.go`) — not a dedicated claim. Confirm with the
  metastore/platform team this is the intended gate for a genuine security-relevant object type
  before relying on it.
- Dropping `"project"` from `scope.supported` sidesteps the previously-flagged risk that `project`
  and `targeted` scope alias to the same ACL hint in `DefaultScopeEvaluator.Matches` — that risk
  only mattered if `project` scope were offered, so this design choice removes it rather than
  merely working around it.

Estimated effort: small, ~1-2 days including fixtures — no Go production code, no new tables.

---

## `kbagent` (`~/keboola/cli-new/`) — see RFC "Dependencies" #2

1. New command group, copying the `semantic-layer` group's shape (also a metastore-CRUD flow):
   `commands/rls.py` (+ `_rls_crud.py`, `_rls_guided.py`), `services/rls_service.py`,
   `server/routers/rls.py`, `permissions.py` `OPERATION_REGISTRY` entries (`rls.create`/`update`/
   `delete` → `"admin"`), `cli.py` registration. Every write path always creates at `organization`
   or `targeted` scope — the command surface should not even offer a `--scope project` option for
   this type, matching the backend's own restriction.
2. Add `rls-policy` as a recognized item type in the existing `metastore_client.py` (already
   implements `list_items`/`get_item`/`post_item`/`put_item`/`delete_item`/`get_schema` — this is
   close to a one-line addition to its item-type `Literal`).
3. Guided skill in `plugins/kbagent/skills/kbagent/` (a new `references/rls-workflow.md` +
   generated `SKILL.md` via `make skill-gen`): confirm the invoking admin is acting at org level;
   table picker via the existing `commands/_checkbox_select.py` + `storage_service.list_tables`,
   **scoped to a chosen project** so the flow always records which project(s) the policy is
   actually for (`source_project_id` / `target_project_ids`), even though the write itself lands
   at org scope; per-table principal(s) + primitive builder (never a free-text predicate field);
   dry-run preview using the **same** `_compile_primitive` logic as this repo's `rls.py` (decide:
   import as a dependency, or keep a deliberately-identical small reimplementation — either way,
   preview and enforcement must never be two implementations that can drift); then create-or-patch
   via `list_items`/`put_item`.
4. Docs tax per CONTRIBUTING.md's "Checklist: Adding a New CLI Command": `commands/context.py`
   `AGENT_CONTEXT`, `CLAUDE.md` row, `commands-reference.md`, `gotchas.md` entry, `keboola-expert.md`
   row, changelog entry — CI-gated by `scripts/check_command_sync.py` and friends.
5. Tests: `tests/test_rls_cli.py` (Typer `CliRunner` + mocked service factory, per
   `test_semantic_layer_cli.py`), `tests/test_rls_service.py` (mocked client, including a case
   asserting a non-org-admin caller is rejected before any network call), plus a
   `tests/test_e2e.py` case.

Estimated effort: medium (large only if RLS policy compilation grows cascade/cross-reference
semantics). Rough shape: ~300-600 lines service, ~400-700 lines commands, ~150 lines router,
~600-900 lines tests, plus the doc surfaces above.

---

# v3 — CLS, broader identity, schema-stability policy

Companion to `RFC.md`'s "v3 Amendment" section — read it first. Tasks 1-4 above (v2, RLS) are
DONE and unaffected; these are additive. Phase numbers below match the RFC's "Roadmap" phases.

## This repo (`keboola-mcp-server`) — Phase 1

### Task 9 — `rls.py`: `ClsRules` + composed wrapper (DONE — implemented this session)

- `ClsRules.from_metastore(objects, *, dialect, project_id) -> ClsRules`: same shape/applicability
  logic as `RlsRules.from_metastore` (Task 1), reading `cls-policy` objects instead —
  `tables: Mapping[str, Mapping[str, tuple[str, ...]]]` (rules key -> principal -> allowed columns).
  A sibling dataclass, not a field bolted onto `RlsRules`, and its own self-contained
  `from_metastore` (deliberate duplication, not shared with `RlsRules.from_metastore` — touching
  the already-shipped, heavily-tested original for a DRY saving was judged not worth the risk; see
  RFC "Schema stability" and the class docstring).
  Also gained `table_ids`/`referenced_columns()`, mirroring `RlsRules`'s Phase-1.5 additions, so the
  schema-drift check covers CLS-referenced columns too (see Task 10.5).
- `rewrite_query()` gained an optional `cls_rules: ClsRules | None = None` parameter (every existing
  RLS-only call site needs no change). `_transform` now computes `rls_governed`/`cls_governed`
  independently per table and, when either is true, builds one wrapper:
  `SELECT <cols> FROM <table> WHERE <predicate>` — `<cols>` is `*` when no CLS rule applies, else
  the allowlist (in the order `visible_columns` declared it); `<predicate>` is the RLS predicate
  when one applies, else `TRUE` (`exp.true()`, not a parsed string). A CLS-governed table with no
  rule for the resolved principal fails closed exactly like RLS's own "no rule for user" case
  (`ClsRules.columns_for`, symmetric with `RlsRules.predicate_for`).
  An explicit column reference outside the allowlist is **not** separately detected in this
  rewrite — it doesn't need to be: the wrapper subquery only ever exposes the allowlisted columns,
  so an outer reference to a hidden one is an ordinary "invalid identifier" error from the
  warehouse itself once the query runs, the same way any other nonexistent-column reference would
  be. No extra validation code was needed for that RFC line item.
- `_check_output` extended with a `columns: Mapping[str, tuple[str, ...] | None] | None = None`
  parameter (default `None` keeps every existing direct test call unchanged): re-parses the
  wrapper's SELECT list and compares it (as generated text per column, same discipline the
  WHERE-clause check already uses) against the expected `visible_columns` for the matched
  principal, or expects a plain `SELECT *` when no CLS rule matched that key.
- `references_governed_table()` gained the same optional `cls_rules` parameter, so the cheap
  pre-check in `tools/sql.py` also short-circuits correctly when only CLS (not RLS) governs a
  touched table.
- Tests: `tests/test_rls.py` gained `TestClsFromMetastore`, `TestColumnsFor`, and
  `TestComposedRewrite` (RLS-only unaffected, CLS-only wraps with `WHERE TRUE`, both compose in one
  wrapper, CLS-governed-no-rule-for-principal refuses, a join filters/restricts only the governed
  table, CLS/RLS dialect-mismatch refuses) — 215 tests total in that file, all passing, zero
  changes needed to the ~700 lines of pre-existing RLS-only tests.
- `tools/sql.py`'s `_apply_rls` wired in too (no separate task number in the original plan; folded
  in here since shipping CLS meant nothing without it): fetches `rls-policy` and `cls-policy`
  concurrently (`asyncio.gather`, not two sequential `list_objects` calls), builds both `RlsRules`
  and `ClsRules`, and passes `cls_rules=` through to `references_governed_table()`/`rewrite_query()`.
  Reuses the existing `RLS_FEATURE` flag for CLS too — no second feature flag, per the RFC's
  "two-level opt-in stays table-level" reasoning. `_log_schema_drift` extended to merge
  `rules.referenced_columns()` and `cls_rules.referenced_columns()` per matched key.
  `tests/tools/test_sql.py` gained `TestQueryDataColumnLevelSecurity` (CLS-only restricts columns,
  no-rule-for-principal refuses, RLS+CLS compose for the same table) and a `_stub_metastore_objects`
  test helper — needed because a single `list_objects.return_value` would otherwise hand the same
  (RLS-shaped) objects back for the new `cls-policy` call too; existing RLS-only tests updated to
  use it.

### Task 10 — `tools/sql.py`: principal-resolution chain step 2 (`verify_token()`, not introspection)

- `_apply_rls` (or a renamed/extracted `_resolve_principal` helper) gains: when no OAuth session
  identity is found, call `client.storage_client.verify_token()` on the already project-scoped
  client `_apply_rls` already holds (it already calls `client.storage_client.project_id()` and
  `client.has_feature()` on this same client — no new client, no new header wiring, no
  `introspect_token()` call). Extract whichever field the real response uses for the token owner's
  identity, once confirmed (RFC "Open question") — do not guess a field name into the implementation
  before that's confirmed.
- **Efficiency note, resolve before merging**: `get_token_info()`/`get_project_features()`/
  `get_token_role()` (`mcp.py:957`-`973`) already call `verify_token()` once per tool call for
  feature/role gating in `on_call_tool`. Check whether that result is reachable from `_apply_rls`
  (e.g. threaded through `ctx.session.state` for the duration of one tool call) before adding a
  second `verify_token()` call in the same request — a double call per `query_data` invocation is a
  real, avoidable cost once RLS is on for a project.
- Tests: session with a programmatic token and no OAuth state resolves via `verify_token()`; a
  response with no identity field present falls through to "no principal" unchanged (this is the
  expected, common case for a Data App's service token — step 3, not step 2, is how those resolve).

### Task 10.5 — `rls.py`: policy-vs-schema drift check (Phase 1.5) (DONE — implemented this session)

- Implemented narrower than first scoped, and against `RlsRules` only (`ClsRules`/Task 9 isn't
  built yet): `RlsRules` gained a `table_ids` field (rules key -> original `<bucket>.<table>` id,
  needed because the rules key itself is dialect-normalised and not always a valid
  `table_detail()` argument on BigQuery) and a pure `referenced_columns()` method (re-parses each
  already-compiled predicate string, no network access). `tools/sql.py` gained
  `_log_schema_drift()`: called once per `query_data` call, after a successful rewrite, scoped to
  only the tables `rewrite_query()` actually matched (`rewritten.applied_rules`) rather than every
  governed table in the project — bounds the added `table_detail()` cost by what one query touches.
  Wrapped in `contextlib.suppress(Exception)` at the call site (on top of its own internal
  per-table try/except) so a Storage failure here can never turn a successful, correctly-filtered
  query into an error.
- Tests (`tests/tools/test_sql.py::TestQueryDataSchemaDrift`): a policy naming a column absent from
  `table_detail()`'s response logs a warning and still enforces exactly as before; a policy naming
  a present column logs nothing; a `table_detail()` failure doesn't break the query. Existing
  `TestQueryDataRowLevelSecurity` positive-match tests updated to give `table_detail` a realistic
  mocked return value (previously unconfigured, which leaked an unawaited-coroutine warning once
  this code path started calling it).

## Metastore backend (`go-monorepo`) — Phase 1 + Phase 2

1. **Phase 1**: `cls-policy` schema + migration, identical shape/ACL/scope treatment to
   `rls-policy` (PLAN.md Task 7 above) — `organization`/`targeted` scope only, `organization-admin`
   ACL, versioned filename per the RFC's "Schema stability" policy.
2. **Phase 2**: `rls-token-principal` schema + migration, same ACL/scope treatment. Blocked on
   confirming a stable per-token identifier field (RFC "Open question") — do not schema this until
   that's confirmed against the real API response, not assumed.

## `kbagent` — Phase 1 + Phase 2

1. **Phase 1**: extend the `rls` command group (PLAN.md's kbagent section above) with a `cls`
   sibling — same guided-flow shape (table picker, principal picker, but a column *multi-select*
   from the table's known schema instead of a condition builder), same dry-run-preview-before-write
   discipline.
2. **Phase 2**: not this repo's work — the Data-App token-provisioning UI (owned by the Data Apps
   team) writes `rls-token-principal` objects directly via the metastore API, the same way `kbagent`
   writes `rls-policy`/`cls-policy`. `kbagent` may still want a read-only `rls token-principal list`
   command for admins auditing bindings, but issuing the binding is the provisioning UI's job, not
   a CLI flow.

## Phase 3-6

No task breakdown yet — each is a separate RFC amendment (or its own RFC, for Phase 6) once its
turn comes, per the roadmap ordering in `RFC.md`.
