# RFC: Row-Level Security, metastore-backed (v2)

Linear: _TBD — pilot; no Linear issue assigned yet_

Status: **Draft** — supersedes and redesigns the pilot in PR #697 (`feat/rls-query-tool`, @padak,
"pilot, do not merge"). That pilot's SQL-rewrite engine (`src/keboola_mcp_server/rls.py`) is kept
almost entirely as-is; what changes is where rules live, how they're authored, which tool exposes
them, and how the whole thing turns on. See "Relationship to the pilot" below for exactly what
carries over.

**v3 amendment** (new sections starting at "## v3 Amendment" below): the RLS design above (v2) is
implemented — `rls.py`'s primitive compiler/metastore loader and `tools/sql.py`'s per-table opt-in
are on `main` of this branch, PLAN.md Tasks 1-4 DONE. v3 does not change any of it; it adds
Column-Level Security as a sibling mechanism in the same code paths, widens identity resolution
past OAuth-login-only, and states explicitly, as policy rather than precedent, the schema-stability
discipline the metastore object types here must keep. Two pieces of "v3 Amendment" are now implemented: the policy-vs-schema drift check (Roadmap
Phase 1.5 — see `rls.py`'s `RlsRules.referenced_columns()`/`table_ids` and `tools/sql.py`'s
`_log_schema_drift()`, PLAN.md Task 10.5, DONE) and Column-Level Security itself (Phase 1 — see
`rls.py`'s `ClsRules` and `rewrite_query()`'s `cls_rules` parameter, `tools/sql.py`'s `_apply_rls`
fetching `cls-policy` alongside `rls-policy`, PLAN.md Task 9, DONE). Both are consumer-side only:
the MCP server can compile and enforce `cls-policy` objects the moment they exist, but the
`cls-policy` schema itself isn't registered in the metastore backend yet (go-monorepo dependency,
unchanged from `rls-policy`'s existing one) — see "Dependencies" and PLAN.md's go-monorepo section.

Still Draft, not implemented: the principal-resolution chain (chain step 2's `verify_token()` call,
chain step 3's `rls-token-principal` binding) and value-masking CLS (Phase 3). Both remain blocked
on confirming real API field names, per the "Open question" below — nothing here was guessed into
code.

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

---

## v3 Amendment: Column-Level Security, a principal-resolution chain, and schema stability as a contract

### Problem, extended

v2 answers "who is asking" with exactly one source: an OAuth-authenticated MCP session's login
identity. Three gaps that leaves open, all raised in review of v2:

1. **A user's own programmatic token** (`kbc_pat_`/`kbc_at_`, set directly as `KBC_STORAGE_TOKEN` —
   the common shape for a local, non-interactive, or CI-driven MCP session) currently resolves to
   *no* principal at all, so it can never read an RLS/CLS-governed table, even though the person who
   minted that token is a real, identifiable human. v2's RFC treated this as a structural
   limitation ("no equivalent 'who logged in' concept"); it isn't one — see "A principal-resolution
   chain" below.
2. **A token that has no natural human owner** — one minted for a Data App deployment, a scheduled
   job, or another internal service acting on a user's behalf — has no login and no personal
   introspection identity to fall back on either. v2's fail-closed default ("no identity ⇒ no
   governed table") is correct for it *by default*, but there needs to be a deliberate, governed way
   to say "this specific token should be evaluated as principal X" — because the alternative is that
   RLS/CLS-governed tables are simply unreachable from any Data App, which isn't a security property,
   it's a feature gap someone works around by turning RLS off for the whole project.
3. **Column-level restriction** (hide/mask specific columns, independent of which rows are visible)
   was explicitly deferred in v2's "Scope and Constraints" ("column masking: out of scope"). This
   amendment builds it now, not later — as the same kind of metastore-object-plus-compiler mechanism
   as RLS, sharing the same rewrite pass, rather than a second, separately-evolving system.

None of this changes v2's core security property or its proxy-level scope (see v2's "Scope and
Constraints" — still true here: this remains a `query_data`-only control, not native warehouse RLS).

### A principal-resolution chain, not a single source

`_apply_rls` (renamed conceptually to "resolve access control", though the function can stay where
it is) tries, in order, stopping at the first match — **never merges identities from more than one
step into a single query**, to avoid a query being evaluated under an ambiguous or accidentally
widened identity:

1. **OAuth session login** (v2, unchanged) — `ctx.session.state[OAUTH_USER_EMAIL_KEY]`, populated
   from `introspect_token()` at `exchange_authorization_code()` time (`oauth.py`). Introspection is
   right here specifically because at that point in the flow no project has been chosen yet (that's
   the whole reason `introspect_token()` exists: "enumerate the projects this token can reach" —
   `auth_login.py:158`) — there is no `X-KBC-ProjectId` to call `verify_token()` with.
2. **Revised from the first draft of this amendment — `verify_token()`, not `introspect_token()`,
   for a directly-supplied programmatic token** (this is the path Data Apps actually use: no OAuth
   session, a `kbc_at_`/`kbc_pat_` bearer token set directly). By the time `_apply_rls` runs inside
   `query_data`, a project is *always* already resolved — `query_data` cannot run without one — so
   `client.storage_client.verify_token()` (`clients/storage.py:1140`, `GET tokens/verify`) is safe to
   call: it 401s only pre-scope, which this call site never is (`mcp.py:995` documents exactly that
   401 for the *unscoped* case, which is why `on_list_tools` — a pre-scope call site — skips it, but
   `_apply_rls` is not that call site). `verify_token()` is also the *existing* call
   `get_token_info()`/`get_project_features()`/`get_token_role()` (`mcp.py:957`-`973`) already makes
   per tool call for feature/role gating — reuse it rather than adding a second network round trip:
   see the caching note in PLAN.md Task 10. Use whichever field in the response actually carries a
   human identity for the token's owner — this repo's own tests only ever populate `admin.role`
   (`tests/test_mcp.py:493` etc.), never an email/name on `admin`, so **do not assume `admin.email`
   exists until confirmed against the real API response** — same caution v2 already applied when it
   rejected this exact assumption the first time (RFC "Identity" section, v2). If no such field
   exists, step 2 degrades gracefully to "no natural identity" and step 3 is the only way such a
   token ever resolves — which is expected and correct for a token that generally *shouldn't* have
   one (a Data App's own service credential).
3. **An explicit, metastore-authored token-principal binding**, for a token `verify_token()` cannot
   attribute to one human at all (a Data App's own service token, a scheduled job's token) — the
   normal case for a Data App, not a fallback for a broken step 2. A new object type,
   `rls-token-principal`:
   ```json
   { "type": "object", "required": ["token_key", "principal"],
     "properties": {
       "token_key": { "type": "string", "minLength": 1,
         "description": "Stable identifier of the bound token — see 'Open question' below" },
       "principal": { "type": "string", "minLength": 1 } } }
   ```
   Governance mirrors `rls-policy` exactly, for the same reason: **`organization`/`targeted` scope
   only, `create`/`update`/`delete` restricted to `organization-admin`.** This is deliberate, not
   just consistent-for-consistency's-sake — if a project admin could bind an arbitrary principal to
   a token they themselves mint, that's a privilege-escalation path around every RLS/CLS rule in the
   project (mint a token, bind it to the most-visible principal, done). Centralizing authorship is
   what makes the binding trustworthy at all.

   **Open question, narrower now but still unresolved — must be confirmed, not assumed, before this
   is buildable:** `token_key` should almost certainly be `verify_token()`'s own top-level token
   `id` (a token's stable identity, distinct from its bearer secret and from the `admin`/`owner`
   sub-objects) — steps 2 and 3 can then share the *same* `verify_token()` call: try an identity
   field on the response first, and if none is there, look up whether the response's token `id` has
   a bound `rls-token-principal` object. But this repo has never exercised that field in code or
   tests (`tests/test_mcp.py`/`tests/test_workspace.py`'s fixtures only ever populate `owner`/
   `admin`, never a top-level `id`) — confirm its name and stability (does it survive a token
   description edit? a scope change on the token?) against the real API response before treating it
   as fact.

   This is the mechanism the "UI which creates the token for a Data App" should write to: at
   provisioning time, whoever creates that token (through that UI, which itself must be
   admin-gated the same way `kbagent rls` is — see below) picks which principal the app's queries
   should be evaluated as, and the UI writes one `rls-token-principal` object keyed by the newly
   minted token's own `id`. Nothing in this repo issues or mints that token; it only ever reads the
   binding, exactly as it only ever reads `rls-policy`.
4. No match at any step ⇒ no principal ⇒ unchanged v2 behavior: refuse a governed table, leave an
   ungoverned one untouched. For a Data App token specifically, this means: no `rls-token-principal`
   binding ⇒ that app simply cannot read an RLS/CLS-governed table, which is the correct, safe
   default until an org admin deliberately provisions one — not a gap to route around by disabling
   RLS for the whole project.

### Column-Level Security: `cls-policy`, composed into the same rewrite

New metastore object type, same authorship/scope/ACL model as `rls-policy` (org-admin-only,
`organization`/`targeted` scope, `source_project_id`/`target_project_ids` applicability — every
paragraph of v2's "Rule storage" section applies verbatim, substituting `cls-policy`):

```json
{
  "type": "object",
  "required": ["table", "dialect", "rules"],
  "properties": {
    "table": { "type": "string", "pattern": "^[A-Za-z0-9_.\\-]+\\.[A-Za-z0-9_\\-]+$" },
    "dialect": { "type": "string", "enum": ["snowflake", "bigquery"] },
    "rules": {
      "type": "array", "minItems": 1,
      "items": {
        "type": "object",
        "required": ["visible_columns"],
        "properties": {
          "principal": { "type": "string", "minLength": 1 },
          "principals": { "type": "array", "items": { "type": "string", "minLength": 1 }, "minItems": 1 },
          "visible_columns": { "type": "array", "items": { "type": "string", "minLength": 1 }, "minItems": 1 }
        },
        "oneOf": [{ "required": ["principal"] }, { "required": ["principals"] }]
      }
    }
  }
}
```

**Deliberately an allowlist (`visible_columns`), not a denylist of hidden ones** — the same
allowlist-not-denylist posture `rls.py` already uses for FROM sources (`_ALLOWED_FROM_SOURCES`) and
comparison operators (`_COMPARISON_OPS`): a column added to a protected table later defaults to
*hidden* until a rule names it, never silently exposed. This is also why v1 of CLS is
allowlist-only projection, not value masking (`NULL`-ing a column while keeping it in the
result shape): projection is a strict subset of what the rewrite already does for RLS (rebuilding
the wrapper's SELECT list is smaller than RLS's WHERE-predicate compiler), while masking needs a
per-column, per-type "what does redaction mean for this column" decision that's better designed once
CLS v1 has real usage to look at — see Roadmap, Phase 3.

**Enforcement point**: extended `rls.py`, not a parallel module — a governed table already gets
wrapped by `_transform` for RLS; CLS composes into the *same* wrapper rather than adding a second
rewrite pass (which would create ordering/interaction bugs a second pass over the first pass's
output could introduce):

```sql
-- today (RLS only):
(SELECT * FROM "in.c-crm"."invoices" WHERE customer_region = 'EU') AS invoices
-- with a CLS rule additionally restricting columns for the same principal:
(SELECT id, amount, created_at FROM "in.c-crm"."invoices" WHERE customer_region = 'EU') AS invoices
```

- `RlsRules.is_governed()` widens to "an RLS rule, a CLS rule, or both apply to this table" — a
  table with only a CLS rule and no RLS rule still gets wrapped (with `WHERE TRUE`, effectively),
  and vice versa. One `is_governed()` covers both, since both now live behind the same
  `row-level-security` project feature flag (see below — deliberately not a second flag).
- The caller writing an explicit column list that names a column *not* in the principal's
  `visible_columns` is a **refusal**, not a silent narrowing — same fail-closed posture as an
  unmatched RLS principal (v2: "no rule for user ⇒ refuse"). Silently dropping a column the caller
  explicitly asked for would look like a bug (an "eventually consistent" data app), not a security
  boundary; refusing tells the caller (the model, relaying to the user) plainly that the column
  isn't available to them.
- `SELECT *` against a CLS-governed table rewrites to the explicit `visible_columns` list, same as
  RLS's `SELECT *`-in-the-wrapper shape already does structurally.
- `_check_output`'s re-parse safety net (`rls.py:610`) gains the symmetric check it already does
  for the WHERE clause: the wrapper's SELECT list must be exactly `visible_columns` for the matched
  principal, compared as generated column-identifier text, not a superset/subset fuzz match.
- **Gate**: reuses the existing `row-level-security` feature flag and the existing two-level opt-in
  (v2's "Two-level opt-in" section) — not a new `column-level-security` flag. A project that hasn't
  opted into RLS at all has no reason to be paying the metastore lookup for CLS either, and a
  project that has opted in but has zero `rls-policy`/`cls-policy` objects for a table already
  short-circuits via `references_governed_table` before either kind of policy is even relevant.
  Table-level opt-in (does *this* table have any applicable policy — RLS, CLS, or both) is the real
  granularity; the flag is only ever the one-time "does this project use this mechanism at all"
  gate.

### Live evaluation: unchanged guarantee, now covering more surface

Directly answering "make sure evaluation happens on every `query_data` call, for any user" — this
is already how v2 works today (RFC "No frozen state — resolve live, every call") and nothing in
this amendment weakens it:

- Every principal-resolution step above (OAuth session, token introspection, the new
  `rls-token-principal` binding) is resolved from the caller's live token/session state on each
  call needing it, cached at most for the lifetime of that session/request context — never written
  to a file, never surviving a restart, never assumed stale-but-good-enough.
- `cls-policy` objects are fetched through the exact same `MetastoreClient.list_objects()` call
  site as `rls-policy` (`tools/sql.py:295`), same per-call-or-short-TTL discipline, same "a cold
  cache after a restart costs a few seconds, never correctness."
- A restart, a token rotation, or an org admin editing a policy mid-conversation all take effect on
  the *next* `query_data` call, not "the next login" — there is nothing here that would make one
  user's access reflect a policy from before their most recent edit.

### Data-contract maturity: what RLS/CLS actually give you, and what they don't

Answering directly: **RLS/CLS plus the schema-stability policy below give real data-contract
coverage over exactly one thing — the `rls-policy`/`cls-policy`/`rls-token-principal` metastore
objects themselves. They give zero data-contract coverage over the Storage tables RLS/CLS
protect.** Those are different claims and this RFC should not be read as making the second one.
Concretely, by dimension:

| Dimension | State on the *policy objects* (`rls-policy` etc.) | State on the *protected tables* (`in.c-crm.invoices` etc.) |
|---|---|---|
| Schema definition | JSON Schema per type, checked in | None — a table's column list/types are whatever the last write left them |
| Versioning discipline | Additive-only, versioned filename (this section, below) | None |
| Breaking-change governance | N/A yet (no version 2 has shipped) but the policy is written down | None — a producer can rename/drop a column with no gate |
| Enforcement point | Read time, by this repo's loader (fails closed on an unrecognized shape) | None |
| Consumer notification | N/A — this repo is the only consumer today | None |
| SLA / freshness | N/A (not applicable to a policy object) | None |
| Ownership | Org-admin authorship, enforced by ACL | Whoever last ran a pipeline into the table |

The sharpest concrete gap: `cls-policy`'s `visible_columns` and an `rls-policy` predicate's
`column` both *name* real columns on a real table, but neither this repo nor the metastore
validates that those names still exist on that table. If a producer renames `country` to
`country_code` upstream, every existing rule referencing `country` goes from "matches, filters
correctly" to "matches nothing recognizable" with **no signal to anyone** — RLS's own fail-closed
design (v2: "no rule for user ⇒ refuse") accidentally makes this *safe* (a stale RLS predicate that
no longer parses/matches ends in a refusal, not a leak; a stale CLS allowlist ends in "no columns
visible," not "everything visible") but not *correct*: an admin's policy silently stopped doing
what they wrote, and nothing tells them.

**What "full" data contracts would need**, for contrast (not proposed here — see Roadmap Phase 6):
a producer-side schema declaration for the table itself, a CI/pipeline gate that validates a write
against it before it lands, a versioned/breaking-change process consumers can subscribe to, and
SLA/freshness tracking. None of that exists in this repo today, and building it is a materially
larger, separate initiative from RLS/CLS.

**One small, additive step that *is* in scope here and closes the sharpest gap above cheaply**:
a read-only **policy-vs-schema drift check**. This repo already has `StorageClient.table_detail()`
(`clients/storage.py:945`) returning a table's live column list. When `RlsRules`/`ClsRules` load
policies for a project (the same `from_metastore` pass, `rls.py:279`-`367`), cross-check each rule's
referenced column names against `table_detail()`'s live schema for that table and `LOG.warning()`
(never raise — this is a hygiene signal, not a new fail-closed gate; the existing "no match ⇒
refuse" behavior already handles the *safety* side) when a rule names a column the table no longer
has. This doesn't validate the table's schema against any contract (there isn't one), only that the
*policy* and the *table* still agree — a narrow, cheap, genuinely useful bridge, not a data-contract
feature. Proposed as Phase 1.5 in the Roadmap below, since it shares Phase 1's code paths.

### Schema stability as the data contract for this feature

This repo has no general "data contract" (producer/consumer schema-and-SLA agreement) feature, and
building one is out of scope here (see Roadmap, Phase 6) — but every metastore object type this RFC
introduces or touches is itself exactly that shape: a schema `rls-policy`/`cls-policy`/
`rls-token-principal` producers (org admins via `kbagent`, eventually a Data-App token UI) write
against, and this repo's loaders (`RlsRules.from_metastore`, a new `ClsRules.from_metastore`) read
against, without either side coordinating a deploy. Stated as explicit policy, not merely precedent:

- **Every schema version is additive-only.** A new version may add an optional field with a
  well-defined default/absence behavior, add a new enum value, or add a new `condition`/operator
  variant (v2's "Extensibility" section already did this once, for comparison operators — this
  generalizes it to every field in every type here). A new version must never: change an existing
  field's type, rename a field in place, narrow an existing enum, or turn a previously-optional
  field required.
- **The consumer (this repo) must tolerate every schema version it's ever shipped for, forever** —
  an object written under an older version parses with old-version defaults, never a hard error
  merely because a newer optional field is absent. Conversely, an object shape this repo genuinely
  doesn't recognize at all (not "old version," but "not a shape any version of this schema
  describes") still fails closed — refused, not silently skipped — exactly as `RlsRules.from_metastore`
  already does today for a malformed `rls-policy` object (`rls.py:295`-`367`).
  Filenames stay versioned (`<type>_schema_<semver>.json`, PLAN.md Task 7's existing convention) so
  "which shape is this" is never inferred from content.
- This is why `rls-token-principal` above is its own small object type rather than a new field
  bolted onto `rls-policy`: it has a different authorship trigger (a token-provisioning UI, not the
  `kbagent rls` guided flow) and a different lifecycle (created once per token, not per protected
  table) — folding it into `rls-policy` would make that object's own schema harder to keep additive
  later, for a saving that's purely cosmetic now.

### Roadmap

Ordered by what unblocks what, not by size:

1. **Phase 1 (this amendment, ship together with v2's remaining Tasks 5/6 status quo):** CLS v1
   (allowlist projection, no masking) — **DONE, consumer-side** (`ClsRules`, `rewrite_query()`'s
   `cls_rules` param, `tools/sql.py`'s `_apply_rls` fetching `cls-policy`; PLAN.md Task 9). No new
   backend dependency beyond what v2 already needs for `rls-policy`, since `cls-policy` is the same
   object-type mechanism — but the `cls-policy` schema itself still needs registering in the
   metastore backend before this is reachable end-to-end (same go-monorepo dependency `rls-policy`
   already has). Principal-resolution chain steps 1-2 (OAuth session, `verify_token()` for a
   directly-supplied programmatic token) remain **Draft**, not implemented — step 2 is blocked on
   confirming which field actually carries identity in `verify_token()`'s response (see "Open
   question" above); nothing was guessed into code for it.
1.5. **Phase 1.5:** the policy-vs-schema drift check above — logs only, no behavior change, shares
   Phase 1's `from_metastore` code path. **DONE** (PLAN.md Task 10.5).
2. **Phase 2:** the `rls-token-principal` binding (chain step 3) — blocked on resolving the "Open
   question" above (a stable per-token identifier) and on the Data-App token-provisioning UI
   actually writing it; that UI is owned outside this repo.
3. **Phase 3:** value masking for CLS (`{"mask": "null"}` / a hash / a partial-redaction shape) as
   an alternative to pure column omission — deferred until Phase 1's allowlist-only shape has real
   usage to design against.
4. **Phase 4:** directory-backed principal groups (already flagged as future work in v2's
   "Extensibility" — `principals: [...]` today is a literal list; resolving a *role* like
   "everyone with billing-admin" needs new Storage-API client work this repo doesn't have yet).
5. **Phase 5:** native warehouse-level RLS/CLS (Snowflake `ROW ACCESS POLICY`/masking policy,
   BigQuery policy tags) — needs per-end-user workspace sessions, which don't exist today (workspace
   queries run under one shared service credential); a separate, much larger platform initiative,
   already flagged out-of-scope in v2's "Scope and Constraints."
6. **Phase 6, separate and much larger:** an actual `data-contract` metastore object type — schema
   and SLA guarantees a table's *producer* commits to, enforced at write time, independent of who's
   allowed to read what. Reuses the org-authored, additive-only-schema governance model this
   amendment states as policy, but is not RLS/CLS and should not be designed as an extension of
   either — it answers "can I trust this table's shape," not "who may see which rows/columns of it."

### v3 Testing

- New unit tests for `ClsRules.from_metastore`/the column-allowlist compiler, mirroring
  `TestCompilePrimitive`/`TestLoad` structure: valid `visible_columns`, `principal` vs `principals`,
  an object applicable/inapplicable to the current project (same `source_project_id`/
  `target_project_ids` matrix as `TestLoad` already covers for `rls-policy`).
- `tests/test_rls.py` `TestRewriteQuery`-style cases for the composed wrapper: RLS-only table
  (unchanged), CLS-only table (WHERE TRUE, explicit column list), both on the same table, `SELECT *`
  rewritten to the allowlist, an explicit disallowed column named ⇒ refused, a table with neither
  ⇒ untouched.
- New tests for principal-resolution step 2: a session with no OAuth identity but a programmatic
  bearer token resolves via `introspect_token()`; introspection returning no `user_email` ⇒ falls
  through to "no principal" exactly like today.
- New tests for the `rls-token-principal` binding once its shape is confirmed: a bound token
  resolves to its principal; an unbound token falls through; a binding whose `source_project_id`/
  `target_project_ids` don't cover the current project is ignored, mirroring `rls-policy`.
- `tox` clean, per repo convention.
