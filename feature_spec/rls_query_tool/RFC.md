# RFC: Row-Level Security, metastore-backed (v2)

Linear: _TBD — pilot; no Linear issue assigned yet_

Status: **Draft** — supersedes and redesigns the pilot in PR #697 (`feat/rls-query-tool`, @padak,
"pilot, do not merge"). That pilot's SQL-rewrite engine (`src/keboola_mcp_server/rls.py`) is kept
almost entirely as-is; what changes is where rules live, how they're authored, which tool exposes
them, and how the whole thing turns on. See "Relationship to the pilot" below for exactly what
carries over.

## Problem

The pilot already states the core problem well and it hasn't changed: a model answering on a
user's behalf must receive already-filtered data, because it cannot be trusted to add the filter
itself. What the pilot got right — an admin-defined predicate substituted for a table on every
read, fail-closed enforcement, a disclosure of which tables were filtered — this RFC keeps. What
it got operationally wrong, per production feedback:

1. **Rules lived in a YAML file** (`KBC_RLS_RULES_PATH`), baked into or mounted onto a specific
   deployment. No admin UI, no audit trail, no cross-project sharing, and any rule change meant a
   server restart.
2. **A rule's predicate was raw SQL text** an admin hand-wrote. A real SQL-injection-shaped trust
   requirement on whoever edits the file — acceptable for an engineer, not for "a customer defines
   their own policy" without engineering involvement.
3. **RLS was a whole-deployment mode switch** (rules-path set ⇒ swap `query_data` for a separate
   `query_data_rls` tool, principal from a header or a model-supplied argument). That means a
   restart-fragile, all-or-nothing posture: every table in that deployment was implicitly subject
   to RLS's fail-closed "no rule ⇒ refuse," whether or not the table was meant to be protected.
4. **No guided way to populate rules.** A human edited YAML by hand; nothing walked an admin
   through it.
5. **Any project admin could define policies for their own project.** There was no way to make RLS
   policy authorship a centrally-governed, org-level responsibility.

This RFC keeps the SQL-rewrite mechanism, and replaces (1)-(5) with: rules stored as Keboola
**Metastore** objects, authored via primitives instead of free-hand SQL, gated per-project by a
feature flag and per-table by whether a policy exists at all, with identity resolved from the
caller's own verified login rather than a header or argument, and policy authorship centralized at
the organization level.

## Relationship to the pilot — what carries over unchanged

- `rewrite_query()`, `_check_output()`, `_check_from_sources()`, `_check_functions()`,
  `_is_cte_reference()` and the rest of the AST-rewrite/fail-closed engine in `rls.py`. Every
  adversarial-review finding baked into that code (CTE shadowing, table-modifier stripping, the
  FROM-source allowlist, the output re-parse safety net) still applies verbatim.
- The `RlsRules` dataclass shape (`tables: Mapping[str, Mapping[str, str]]`, `dialect: str`) that
  `rewrite_query()` consumes. This is the seam: only how it gets *built* changes.
- Query size cap, `outcome=` audit logging, dialect-pinning (predicates are still never
  transpiled between Snowflake and BigQuery).

**What does not carry over**: `query_data_rls` as a separate tool, the `principal` tool argument,
`X-RLS-Principal` header, `Config.rls_principal_source`/`Config.rls_rules_path`, and
`ToolAuthorizationMiddleware`'s whole-deployment RLS-mode-forces-read-only switch. All four existed
to solve problems (identity binding, opt-in) that a verified login and a project-level flag solve
better — see below.

## Required Behavior

### Rule storage: a new Metastore object type, `rls-policy` — org-authored, never project-scoped

One object per protected table (not one blob per project), read via the existing
`MetastoreClient` (`src/keboola_mcp_server/clients/metastore.py`) exactly like the semantic-layer
object types already are (`src/keboola_mcp_server/tools/semantic/`). `name` = the table key
(`in.c-crm.invoices`) for human-readable listings; `id` is the metastore UUID.

```json
{
  "type": "object",
  "required": ["table", "dialect", "rules"],
  "properties": {
    "table": { "type": "string", "pattern": "^[A-Za-z0-9_.\\-]+\\.[A-Za-z0-9_\\-]+$",
               "description": "<bucket>.<table>, same key format the pilot used" },
    "dialect": { "type": "string", "enum": ["snowflake", "bigquery"] },
    "rules": {
      "type": "array", "minItems": 1,
      "items": {
        "type": "object",
        "required": ["condition"],
        "properties": {
          "principal": { "type": "string", "minLength": 1 },
          "principals": { "type": "array", "items": { "type": "string", "minLength": 1 }, "minItems": 1 },
          "condition": { "$ref": "#/$defs/condition" }
        },
        "oneOf": [{ "required": ["principal"] }, { "required": ["principals"] }]
      }
    }
  },
  "$defs": {
    "condition": {
      "oneOf": [
        { "type": "object", "required": ["true"], "properties": { "true": { "const": true } } },
        { "type": "object", "required": ["and"], "properties": {
            "and": { "type": "array", "items": { "$ref": "#/$defs/condition" }, "minItems": 2 } } },
        { "type": "object", "required": ["or"], "properties": {
            "or": { "type": "array", "items": { "$ref": "#/$defs/condition" }, "minItems": 2 } } },
        { "type": "object", "required": ["column", "op", "value"], "properties": {
            "column": { "type": "string", "minLength": 1 },
            "op": { "enum": ["eq", "ne", "gt", "gte", "lt", "lte"] }, "value": {} } },
        { "type": "object", "required": ["column", "op", "values"], "properties": {
            "column": { "type": "string", "minLength": 1 },
            "op": { "enum": ["in", "not_in"] },
            "values": { "type": "array", "minItems": 1 } } },
        { "type": "object", "required": ["column", "op"], "properties": {
            "column": { "type": "string", "minLength": 1 },
            "op": { "enum": ["is_null", "is_not_null"] } } }
      ]
    }
  }
}
```

`principal`/`principals` is one identity string or a literal list of them (a self-contained
"group," no external directory lookup — see "Extensibility"). A predicate is never hand-written
SQL: every `condition` compiles deterministically to a `sqlglot.exp.Condition` via sqlglot's own
expression builders (`exp.column(...).eq(...)`, `exp.And`/`exp.Or`, `exp.true()`), never by
string-formatting — closing the injection surface even for a rule author who isn't an engineer.

**Scope is `organization` or `targeted` — never plain `project`, by design, not merely by
convention.** A policy's `table` field ties it to one specific project's bucket, but *authorship*
is deliberately centralized: only an org admin creates, updates or deletes an `rls-policy` object,
never a project's own project-admin, even for that project's own tables. This is enforced two
ways, redundantly:

- **ACL** (`x-metastore.acl`): `create`/`update`/`delete` restricted to `organization-admin` only.
- **Scope itself**: `x-metastore.scope.supported: ["organization", "targeted"]` — `project` is not
  in the supported list at all. This matters beyond the ACL: the metastore's generic `project`-scope
  path treats that project's own project-admin as an owner-equivalent for many object types: not
  offering `project` scope for `rls-policy` removes that path structurally rather than relying on
  the ACL alone to close it. It also sidesteps a real metastore limitation (see `PLAN.md`) where
  `project` and `targeted` scope currently alias to the same ACL hint — moot here since `project`
  scope is never used for this type.

Because `organization` scope makes an object visible to *every* project in the org
(`project_id IS NULL` in the metastore's own read-enforcement predicate), and a `<bucket>.<table>`
key is not guaranteed unique across an org's projects, the MCP server must not match a fetched
policy on table-key text alone. Every policy also carries `source_project_id`
(`MetaObjectMeta.source_project_id`, already part of the metastore's object metadata) recording
which project it was actually written for. The MCP server applies a fetched policy to the current
project only when `source_project_id` matches it, or the current project is listed in
`target_project_ids` for a `targeted` policy — never merely because the table name matches.
`targeted` is the mechanism for deliberately sharing one org-authored policy across specific
sibling customer projects; plain `organization` scope means "applies only where its
`source_project_id` says," not "applies everywhere."

Branch-awareness is inherited automatically from the metastore (every object type gets it), so a
policy change can be drafted/tested in a dev branch first.

**This object type does not exist yet** — registering it is backend work in a different repo
(Keboola's metastore service). See "Dependencies" below; this RFC's MCP-server-side behavior
cannot ship end-to-end until that lands, though it can be developed and tested against a mocked
`MetastoreClient` in the meantime, the same way semantic-layer tools' tests already do.

### Two-level opt-in: a project feature flag, then per-table policy existence

RLS must never be an accidental default. It is off unless *both* of these are true:

1. **The project has a feature flag enabled.** Reuse the existing, already-proven mechanism for
   "a capability only activates for projects that opted in" — `ProjectFeature`
   (`src/keboola_mcp_server/clients/storage.py`, currently `Literal['global-search',
   'storage-branches']`) checked via `StorageClient.is_enabled()`, the same way
   `tools/search.py` gates its textual-search path on `GLOBAL_SEARCH_FEATURE`. Add
   `'row-level-security'` to that set. Without it, `query_data` never even looks up
   `rls-policy` objects — behavior is byte-for-byte identical to today.
2. **The specific table has an applicable `rls-policy` object** — one whose `source_project_id`
   matches the current project (or whose `target_project_ids` includes it), naming this table.
   Within an RLS-enabled project, a table with no such policy is unfiltered, exactly as today; a
   table with one is fail-closed exactly as the pilot's engine already enforces (no matching rule
   for the resolved principal ⇒ refuse).

Enforcement is therefore `(project has the feature) × (an applicable policy names this table)`,
not a single deployment-wide switch, and never merely "some project in the org happened to create
a same-named policy" — a stray policy authored for a different project has no effect here.

### Identity: the caller's own verified login, not a header or an argument

The pilot's `header`/`argument` trust modes existed because it had no other way to know who was
asking. This repo already does, unused until now — and better grounded than this RFC first
assumed: an earlier draft proposed reading identity from `tokens/verify`'s `admin` object, but
that field was never confirmed to carry an email, and this repo already has a *confirmed* source
for exactly this purpose:

- `introspect_token()` (`src/keboola_mcp_server/auth_login.py`) hits `/v1/auth/token/introspect`
  and returns an `Introspection` dataclass whose `user_email: str | None` is populated from the
  response's `user.email` — a field this codebase already reads and already tests
  (`tests/test_oauth.py`), not a guess.
- `SimpleOAuthProvider.exchange_authorization_code()` (`oauth.py`) already calls
  `introspect_token()` once per new session, today only to auto-confirm project scope
  (`_auto_confirm_project_scope`). The same call's `user_email` is now also persisted onto the
  `OAuthSession` row (`user_email: str | None`, previously hardcoded to `None`) and carried onto
  `ProxyAccessToken` for `mcp.py` to read without a second DB round-trip — mirroring exactly how
  `scope_project_ids` etc. are already carried there.
- **This only resolves identity for OAuth-authenticated sessions.** A directly-supplied Storage
  API token (`KBC_STORAGE_TOKEN`, a project or programmatic token) has no equivalent "who logged
  in" concept — it may not represent one human at all. Such a session simply has no principal to
  resolve, which (per the two-level opt-in above) means it cannot use any RLS-gated table; it is
  not a special case to handle, just the normal "no identity ⇒ no rule can match" outcome.

`query_data` (the only data-query tool now — see below) resolves this identity on every call as
the principal for that call. No tool argument, no header, no deployment-level mode.

### One tool, not two

`query_data_rls` is retired; `query_data` absorbs RLS entirely, gated as above. This is possible
specifically because protection is now a property of *(project feature) × (applicable table
policy)*, not a caller-chosen mode — there is nothing left for a second tool or an optional
argument to express.

`ToolAuthorizationMiddleware`'s pilot-era RLS-forces-read-only behavior (a write tool must never
become a side channel around a read-only filter) is **deferred, not carried over as-is** — see
`PLAN.md` Task 5. It never existed on current `main` (the pilot that had it was never merged), and
re-adding it means an async feature-flag check on every tool call in every project, not just
RLS-enabled ones. `query_data` itself already enforces the real security property (a governed
table's rows are always filtered or refused) independent of this; the read-only trigger was always
secondary defense-in-depth, not the enforcement itself.

### No frozen state — resolve live, every call

Directly answers "a server restart must not lose or misapply the rules mid-conversation": there is
no load-once, conversation-scoped rule set to go stale, because none is kept.

- Identity resolves from the access token via `verify_token()`/`OAuthSession` on every call. A
  restart costs nothing — the next call re-resolves from Postgres/Storage API exactly as before,
  because the client's token, not server memory, is the durable handle.
- `rls-policy` objects are fetched from the metastore per call, or behind a short (tens-of-seconds)
  TTL cache for latency only — never minutes, never "until restart." A cold cache after a restart
  is a few seconds slower, never wrong.
- Consequence: **no startup-time rule loading or readiness gate is needed at all**, unlike the
  pilot's "refuse to start on a bad file." A bad or unreachable policy object affects only queries
  against the table it names, at the moment they're evaluated.

### Writes: MCP server stays read-only; org-admin-only enforced by the backend

The MCP server only ever reads `rls-policy` objects (`list_objects`/`get_object`) — never creates
or patches them, mirroring how it already treats every other metastore object type. This is not
merely a convention this repo follows: the metastore backend's own access-control rules for this
object type must reject a write from anyone but an org admin, regardless of which client attempts
it, and must not offer `project` scope as an alternate path around that. That is out of this
repo's control — see "Dependencies."

## Extensibility

- **New comparison operators** (`like`, a date/range helper, etc.): one more `condition` variant
  in the schema, one more branch in the compiler. Additive, backward compatible, no migration.
- **Multi-clause conditions** (`country = 'CZ' AND status != 'draft'`): already expressible via the
  `and`/`or` nesting above — this was designed in from the start, not a later add-on.
- **Groups of principals**: `principals: [...]` (a literal list, in the schema above) covers "a
  named set of specific people" today with no new infrastructure. Resolving a group against
  Keboola's own project/org user directory (e.g. "everyone with the billing-admin role") is a
  larger, separate enhancement — this repo's Storage API client has no existing call for listing
  project users/admins, so that path means new client work and a dependency on Connection's
  user/role model, not a schema change. Deferred until literal lists prove insufficient.

## Dependencies (outside this repo, both prerequisites for full enforcement)

1. **Metastore backend** (`go-monorepo`, `services/metastore/`): register the `rls-policy` object
   type — a checked-in JSON Schema + a migration inserting it (this platform's object types are
   schema-only; no Go code is required per existing precedent). Must ship with
   `x-metastore.scope.supported: ["organization", "targeted"]` (no `"project"`), `x-metastore.acl`
   restricting `create`/`update`/`delete` to `organization-admin`, and (since `targeted` scope is
   used) a `manageGrants` block, or `PUT .../target-projects` 403s unconditionally. See `PLAN.md`
   in this directory for the exact schema/migration shape and known risks (the `organization-admin`
   role is inferred from token fields, not a dedicated claim).
2. **`kbagent` CLI** (`~/keboola/cli-new/`): a new `rls` command group (mirroring the existing
   `semantic-layer` group, which is also a metastore-backed CRUD flow) plus a guided setup skill in
   `plugins/kbagent/skills/kbagent/` that picks tables (reusing the existing table-listing
   commands), builds primitives interactively (never a free-text predicate field), previews the
   compiled condition before writing, and creates/patches the `rls-policy` object — invoked by an
   org admin, against org scope, naming which project(s) the policy is for. See `PLAN.md`
   in this directory for the concrete file layout.

Both are drafted as task breakdowns in `PLAN.md`, but land as separate PRs in their own repos —
not owned or gated by this RFC.

## Scope and Constraints

- Only `SELECT` statements; no DDL/DML through `query_data`, unchanged from the pilot.
- Predicates are still written in, and only evaluated against, one workspace dialect per table
  policy; no transpilation.
- No native warehouse-level row security (Snowflake `ROW ACCESS POLICY` / BigQuery row-level
  access policies). This MCP-server-side rewrite is a **proxy-level** control: it protects queries
  that go through `query_data` and nothing else (a different MCP session's write path, a BI tool,
  a scheduled job, or direct SQL-client access to the same workspace credentials are all outside
  its reach). Native, engine-enforced RLS would need a per-end-user database session, which does
  not exist today (workspace queries run under one shared service credential) — that is a separate,
  much larger platform initiative (workspace/session provisioning), explicitly out of scope here,
  not silently forgotten.
- Directory-backed group resolution, column masking, a full audit UI: out of scope, per
  "Extensibility" above.
- MCP server never writes `rls-policy` objects itself; only an org admin (via kbagent) does.
- No project-scoped `rls-policy` objects, ever — see above.

## Testing

- `tests/test_rls.py`'s existing `TestRewriteQuery`/`TestOutputInvariant`/`TestDisclosure` suites
  construct `RlsRules(tables=..., dialect=...)` directly already (not via the loader) — they need
  no change. Only the loader tests (currently `TestLoad`, file-based) get a parallel
  `RlsRules.from_metastore` suite against fixture `MetastoreObject`s.
- New unit tests for the primitive compiler: one per operator, nested `and`/`or`, the `true`
  sentinel, `principal` vs `principals`.
- New tests: project-feature-flag off ⇒ unfiltered even with an applicable policy present; table
  with no applicable policy ⇒ unfiltered; a policy that exists but whose `source_project_id`/
  `target_project_ids` don't include the current project ⇒ ignored, not applied; table with an
  applicable policy but no rule for the resolved principal ⇒ refused; a join across a protected and
  unprotected table filters only the protected one.
- New tests for identity resolution: `exchange_authorization_code()` persists a real `user_email`
  from `verify_token()`; a directly-supplied token resolves a principal through the same call.
- `tox` (ruff + pytest + check-tools-docs) clean, per repo convention.
- Manual: `examples/rls-demo/` (the pilot's Node harness) updated to point at a real
  metastore-backed policy instead of a local YAML file, once the backend object type exists.
