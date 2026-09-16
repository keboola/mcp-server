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

### Task 5 — `authorization.py`: re-trigger read-only mode

- Replace `ToolAuthorizationMiddleware.is_rls_mode()`'s check (currently
  `server_state.rls_rules is not None`, a deployment-wide flag) with a per-session check: the
  project has the feature flag on *and* at least one applicable `rls-policy` object is resolvable.
  This likely needs to become async (a Storage/metastore call) where it's currently synchronous —
  check call sites before assuming this is a drop-in change.
- Tests: extend `tests/test_authorization.py` for the new trigger condition.

### Task 6 — Docs, version, PR

- `TOOLS.md` regeneration (`tox -e check-tools-docs`) — `query_data`'s docstring absorbs the RLS
  guidance the pilot wrote for `query_data_rls`; `query_data_rls` disappears from the docs.
- README: replace the pilot's "Row-Level Security (pilot)" section describing the YAML file with
  one describing the feature flag + org-authored metastore policy model (no local file, no env
  var, no project-level authoring).
- `examples/rls-demo/`: decide whether to update (point it at a real/mocked metastore policy
  instead of `rls.yaml`) or remove — it was built around the file-based model and its "wrapper
  app asserts `X-RLS-Principal`" premise no longer matches the login-derived-principal design.
- Version bump (minor — new capability, backward compatible) + `uv lock`.

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
