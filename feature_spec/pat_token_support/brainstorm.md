---
slug: pat_token_support
status: ready-for-rfc
author: Martin Vaško
created: 2026-06-24
linear: PSGO-261
---

# Brainstorm: PAT / Access Token support and PKCE login for MCP Server

> This document is the discovery artifact that precedes the formal RFC at `./RFC.md`. Anything in the RFC must be traceable to a decision recorded here.

## 1. Problem framing

### Trigger
Three reinforcing drivers landed together:
1. **Platform PAT rollout (PAT-1838).** Connection is shipping programmatic tokens (`kbc_at_*`, `kbc_pat_*`) and every service must accept them; MCP server is explicitly on the list as PSGO-261.
2. **Kill storage-token login.** We want to stop requiring a hand-pasted `KBC_STORAGE_TOKEN` for local/stdio use and replace it with a browser PKCE login that leases and refreshes tokens.
3. **Multi-project / Kai.** PSGO-261 sits under Kai Multi-Project Support; PATs scoped per project/session are the enabler for multi-project agent workflows.

### Pain
- **Kai multi-project agents** can't act across projects today: one Storage token is bound to one project.
- **Platform / security team** carry the standing risk of long-lived Storage tokens living in user and agent configs with no refresh or revocation.
- **Local / CLI users** must manually create and paste a long-lived Storage token to run MCP over stdio.

### Cost of inaction (6-month horizon)
- MCP server becomes the lone holdout requiring legacy Storage tokens, **blocking deprecation of Storage-token auth** platform-wide.
- **Kai multi-project workflows stall** because per-project Storage tokens are a hard ceiling.
- Long-lived Storage tokens keep accumulating in user/agent configs with no rotation or session revocation — **ongoing leak exposure**.

## 2. Constraints

### Hard constraints (can't change)
- **Resolver contract is fixed.** Exchange must go through Connection `POST /manage/internal/auth-bridge/resolve-storage-token`, authenticated with the server's projected SA JWT (`X-Kubernetes-Authorization`). No new exchange API.
- **One image, both deployments.** `mcp-server` (public, OAuth) and `mcp-server-agent` (direct Storage token) share the image and the `KeboolaClient` auth path; the change must cover both.
- **Legacy token keeps working unchanged** (PSGO-261 AC). Only the `kbc_at_` / `kbc_pat_` prefixes trigger an exchange; `X-StorageAPI-Token` traffic is untouched.
- **No token material in logs or exceptions** (PSGO-261 AC): subject token, resolved Storage token, SA JWT.

### Login / scoping model (user-stated, verbatim)
> "The OAuth does typically exchange to JWT token that is connected to PAT token. For regular MCP locally, we should provide only stack, it should login us and provide us whole stack access, then in the chat we will be narrowing down the scope."

Implications:
- **Local/stdio:** the user supplies only the **stack URL**. Login (PKCE) grants **whole-stack access** (a broad session/PAT). The **scope is then narrowed in-chat** per project/session via the middleware mapping (the future token-scoping tooling).
- **OAuth (public mcp-server):** the OAuth session yields a JWT bound to an underlying PAT; same downstream exchange path.

### Deadlines and dependencies
- **No hard external date.** Approach: land as a **draft PR**, verify end-to-end against a **dev stack** where the resolver + PKCE already work, then roll out to production "any day we need to."
- Soft dependencies that gate the production rollout (not local dev): kbc-stacks must map `mcp-server`'s SA subject to `internal:auth-bridge:resolve-storage-token` and mount the projected token (PSGO-261 Part 2); the Connection resolver and `/admin/auth/pkce/authorize` must be enabled on the target production stacks.

### Prior art
- **`auth-demo-cli/pkce.ts` ([ui#6061](https://github.com/keboola/ui/pull/6061))** — the reference PKCE client to mirror for the new local `login` command.
- **`SimpleOAuthProvider` (`oauth.py`)** — existing PKCE + SAPI-mint patterns to follow for token handling/storage.
- **k8s SA step-up header (`b971146f`)** — the projected-SA-token mechanism; it is the mechanism the resolver's `X-Kubernetes-Authorization` exchange must reuse on the **deployed** mcp-server (in-k8s only).
- **[platform-libraries#507](https://github.com/keboola/platform-libraries/pull/507)** — PHP decentralized-exchange reference for exchange + error mapping.

### Scope split (user-stated, verbatim)
> "I think we cannot get rid of OAuth because it's MCP protocol build on top of it. We can have login separate when not having oauth — instead of passing headers just stack is enough. The OAuth exchange will be done separately as separate PR."

Therefore:
- **OAuth is NOT removed.** It is part of the MCP protocol for the public/HTTP transport.
- **This work = the non-OAuth local path:** supply only the stack URL → PKCE login → whole-stack access → narrow scope in-chat. No header passing required locally.
- **OAuth → PAT exchange is a separate PR** and out of scope here.
- Open design point (carry to Phase 4): the deployed mcp-server has a projected SA token and can call the resolver; **local stdio has no SA token**, so locally MCP likely forwards the `kbc_at_*` bearer downstream and lets the services exchange (per go-monorepo#540). The two paths differ — nail this in Alternatives.

## 3. Stakeholders

| Bucket | Person / team | Concern |
|---|---|---|
| Approver | PSGO tech lead (Martin Zajic, issue creator) | MCP-side design sign-off |
| Approver | Connection / auth team (PAT-1838, connection#7403) | Resolver + `/v1/auth` PKCE contract; SA scope grant |
| Approver | Security `?` | Token handling, no-logging, scope-narrowing model |
| Approver | kbc-stacks owner `?` | Per-stack SA-subject → `internal:auth-bridge:resolve-storage-token` mapping (Part 2) |
| Implementer | Martin Vaško (PSGO) | MCP server Python change |
| Affected (not in room) | mcp-server-agent operators | Same image/auth path; must not regress direct-Storage-token use |
| Affected (not in room) | Local MCP users / Kai agents | New login UX; in-chat scope narrowing |

## 4. Alternatives considered

Core question: how does MCP turn an inbound `kbc_at_*` / `kbc_pat_*` into authenticated downstream calls, given local (no SA token) and deployed (has SA token) differ?

### Option A: Forward bearer downstream (no MCP-side exchange)
- **Solves:** Local works with no SA token; trivial — attach `Authorization: Bearer kbc_*` + `X-KBC-ProjectId` to service clients and let each service exchange (go-monorepo#540).
- **Costs:** Only Query/Metastore accept programmatic tokens today; Storage/Queue/AI/sync-actions/data-science/scheduler must all add support first.
- **Risks:** Broken tools until every downstream service accepts PATs; does not meet PSGO-261 AC ("the service authenticates to the resolver with its own SA JWT").

### Option B: MCP-side resolver exchange (PSGO-261 core)
- **Solves:** Meets the AC exactly; one exchange → legacy Storage token works with all services unchanged.
- **Costs:** Requires the projected SA token → only the **deployed/in-k8s** mcp-server can do it.
- **Risks:** No local-stdio story; doesn't address "kill storage-token login" on its own.

### Option C: Hybrid (recommended)
- **Solves:** Deployed mcp-server does Option B (resolver exchange via SA JWT); local stdio does the PKCE login + Option A (forward bearer, services exchange). Each path is the only viable one for its environment; covers both deployments from one image.
- **Costs:** Two auth code paths to maintain and test; local path inherits Option A's "services must accept PATs" dependency.
- **Risks:** Local tools that hit a service not yet PAT-aware fail until that service ships support — must be surfaced clearly (see stress-test).

### Recommendation
**Option C.** It is the only option that satisfies the PSGO-261 AC on the deployed service *and* delivers the stack-only local login the user wants, without removing OAuth. The accepted tradeoff: two distinct token paths (resolver-exchange in k8s, forward-bearer locally), and the local path depends on downstream services accepting programmatic tokens. OAuth→PAT exchange remains a separate PR.

## 5. Impact analysis

> Adapted to `keboola-mcp-server` (Python). The skill's connection/platform-wiki checklist (Doctrine, Zend, storage-driver protobuf) does not apply here; the cross-cutting table below is the mcp-server equivalent. File:line refs verified against current `main`.

### Files / symbols touched
| File / symbol | Change |
|---|---|
| `config.py` `Config` (17-138) | add `project_id` field (same `KBC_*`/`X-*` resolution); add `is_programmatic_token()` helper |
| `clients/auth_bridge.py` (new) | `StorageTokenResolver.resolve(subject_token, project_id)` → resolver call, per-request SA read, error mapping, token redaction |
| `mcp.py` `apply_request_config` (229-245) / `create_session_state` (248-294) | branch on programmatic token: deployed → resolve to Storage token; local → forward bearer |
| `clients/client.py` `__init__` (125-216) | no contract change — `bearer_or_sapi_token` (169) already routes Bearer vs SAPI |
| `clients/base.py` `RawKeboolaClient.__init__` (38-42) | no change — already picks `Authorization: Bearer` vs `X-StorageAPI-Token` by prefix |
| `auth_login.py` (new) | PKCE crypto + loopback callback + `/admin/auth/pkce/authorize` + `POST /v1/auth/pkce/token` |
| `credentials.py` (new) | mode-600 token store + auto-refresh via `POST /v1/auth/token/refresh` + dead-token → relogin |
| `cli.py` `parse_args`/`run_server` (28-207) | `login` subcommand; stack-only startup that loads stored creds |
| `workspace.py` (b971146f) | share one SA-token-file read helper with the resolver client |

### Services / APIs touched (all outbound from MCP)
| Surface | Impact |
|---|---|
| Connection `POST /manage/internal/auth-bridge/resolve-storage-token` | **New** outbound call (deployed only); needs SA JWT + scope grant |
| Connection `/admin/auth/pkce/authorize`, `POST /v1/auth/pkce/token` | **New** outbound (local login) |
| Connection `POST /v1/auth/token/refresh` | **New** outbound (token refresh during usage) |
| Storage / Queue / AI / data-science / scheduler / sync-actions / metastore clients | Contract unchanged; **token source** changes (resolved Storage token deployed, forwarded bearer local) |

### Cross-cutting checklist (mcp-server)
| Dimension | Touched? | Detail |
|---|---|---|
| Transports (stdio / streamable-http) | Yes | stdio gets login path; http/OAuth path unchanged this PR |
| OAuth provider (`SimpleOAuthProvider`) | No | left intact; OAuth→PAT exchange is a separate PR |
| Both deployments (mcp-server + mcp-server-agent) | Yes | one image; agent's direct-Storage-token path must not regress |
| Legacy `X-StorageAPI-Token` path | No (must stay) | only `kbc_at_`/`kbc_pat_` prefixes trigger new behavior |
| k8s projected SA token | Yes | reuse b971146f mechanism for resolver auth |
| `TOOLS.md` / tool signatures | No | no tool signature change expected |
| Config / env vars | Yes | `project_id`, `KBC_PKCE_CLIENT_ID`, SA-token path var |
| Unit + integration tests | Yes | new resolver, login, refresh, regression for legacy token |
| Version bump + `uv.lock` | Yes | minor (new capability) |

## 6. Security pass

| Dimension | Assessment |
|---|---|
| Authentication boundary | **Yes.** Adds a programmatic-token (`kbc_at_`/`kbc_pat_`) acceptance path, a resolver exchange, and a PKCE login. Does not weaken auth — all paths still require Connection-issued credentials; legacy token path unchanged. |
| Authorization / role check | **Yes.** The PAT/AT carries its own scope; the resolver validates the subject token's access to the requested `projectId`. MCP must pass the correct `project_id` and must not elevate scope. In-chat narrowing can only *reduce* scope, never widen it. |
| Tenant isolation (cross-project / cross-org) | **Critical — Yes.** A token scoped to project A must never resolve a project-B Storage token. Resolver enforces; MCP must never reuse/cache a resolved token across projects or sessions. v1 has **no caching**, which preserves isolation by construction. |
| Data exposure (responses / logs / events / exports) | **Yes.** Token material (subject token, resolved Storage token, SA JWT, refresh token) must never appear in logs or exception messages (AC). Tested by asserting redaction in captured logs. |
| Encryption at rest / in transit | **Partial.** In transit: TLS to Connection. At rest: **local creds file holds access + refresh tokens in plaintext** (mode 600). No KMS locally — accepted for a single-user dev machine; flagged. |
| External egress (3rd-party APIs, service accounts) | **No new third party.** New egress is to Connection only. Deployed path uses the projected SA service account (scope `internal:auth-bridge:resolve-storage-token`). |
| Audit trail (event emitted, visible to support) | **Connection-side.** Login, refresh, and exchange are audited by Connection; sessions are listable/revocable via `/v1/auth/sessions`. MCP emits no new audit events. |
| Abuse / DoS / rate limiting | **Yes — design concern.** Refresh-during-usage + no caching = one resolver round-trip per request → load on the resolver. A dead token must trigger relogin, **not** an infinite refresh loop. Needs bounded retry / backoff. |
| Multi-tenant blast radius | **Yes.** The deployed SA token can resolve Storage tokens for valid subject tokens; if it leaks, blast radius is broad. Mitigated by narrow resolver scope + per-request file read (no long-lived in-memory copy) + never logging it. |
| SOX / compliance (HIPAA, GDPR, SOC2) | **No direct change.** No change to SOX-protected branch workflows. GDPR: see PII row. |
| Secrets handling (KMS, encryption-service, logs) | **Yes.** SA token read from projected file **per request** (no cache, handles rotation). `KBC_PKCE_CLIENT_ID` may be injected as a secret (blank-tolerant). Creds file mode 600. |
| PII (collection, retention, deletion) | **Minor.** PKCE token response includes `user{id,email,name}`; persisted in the local creds file. Deleting the file removes it. No server-side PII added. |

## 6.5. Stress-test findings

- `[failure-mode]` **Rotating refresh + concurrent clients = logout storm.** `/v1/auth/token/refresh` rotates the refresh token, so the old one dies. An in-process `asyncio.Lock` stops one process double-refreshing, but **two MCP processes sharing the same creds file** (user runs two clients, or agent + CLI) will rotate each other out → random mid-session logouts. Ask the author: is the creds file single-writer? Do we need file locking or a per-client token?
- `[second-order]` **"Narrow scope in chat" is advisory, not enforced on the stored credential.** Login grants whole-stack access and that whole-stack token sits on disk; in-chat narrowing is a runtime construct. A stolen creds file = whole-stack access. **Resolved (D1):** accepted by design — no minting; mitigate with mode-600 + rotation + no-logging, not credential scoping.
- `[foot-gun]` **`project_id` ambiguity resolves the *wrong* project silently.** The resolver needs one `projectId`; a stale per-call source returns a valid token for the wrong project with no error. **Resolved (D2):** `project_id` is explicit session state (CLI/env default + select-project tool), never silently derived; storage-touching tools require a selected project when none is set.
- `[adoption]` **The hybrid split gives identical-looking failures.** Locally (forward-bearer), a tool can fail because (a) that service doesn't accept PATs yet, (b) wrong `project_id`, or (c) expired token — all surface as a raw downstream 401. First adopters can't self-diagnose. Need an explicit error taxonomy / preflight check that says which of the three it is.
- `[user-mentioned]` **Interim divergence while OAuth exchange is a separate PR.** Until the deferred OAuth→PAT PR lands, the public mcp-server keeps minting a SAPI token from OAuth while local uses the new PAT path — the same image runs two different auth models. State this interim state explicitly so reviewers don't assume parity.

## Decisions from review (2026-06-24)

**D1 — No minting; the on-disk credential stays whole-stack.** We will not mint project-scoped PATs at login. We don't know upfront what the user will access (complex/multi-project work is expected), so login leases the whole-stack session token (access + refresh) and stores it. Scope narrowing is **runtime-only session state**, never a property of the stored credential. Accepted tradeoff: a stolen creds file grants whole-stack access — mitigated by mode-600 storage + refresh rotation + no logging, not by credential scoping.

**D2 — `project_id` is explicit session state, not derived from the token.** With a whole-stack PAT there is no implicit project (today `StorageClient.project_id()` reads it from `tokens/verify`, storage.py:1079 — that only works because the legacy token is project-bound). So:
- `project_id` becomes optional session state, defaulting from CLI/env (`KBC_PROJECT_ID`) and overridable in HTTP via `X-KBC-ProjectId`. We do **not** know project ids upfront.
- A new chat can **start full-scope** (no project selected). Storage-touching tools then either use the default or require a project to be selected first (clear error, see error-taxonomy finding).
- Narrowing happens via a **tool** that sets the active project into session state (mirrors the `get_project_info` discovery pattern: a stack-level "list accessible projects" step, then a "select project" step). Once set, the resolver uses it and `project_id()` returns it directly without `tokens/verify`.
- Restrictions can also be pre-seeded from CLI/env (a project allow-list on the session) when known.

Open implementation detail (non-blocking for the RFC's shape): the exact stack-level endpoint to enumerate PAT-accessible projects, and whether selection is a dedicated tool vs. a parameter on existing tools.

## 7. Open questions
- [ ] **Concurrent-client creds-file handling.** Single-writer assumption, file locking, or per-client token to avoid rotating-refresh logout storms? Owner: Martin Vaško. Needed by: before implementation.
- [ ] **Hybrid-failure error taxonomy.** How does a local user distinguish "service not PAT-aware" vs "wrong project_id" vs "expired token"? Owner: Martin Vaško. Needed by: before implementation.
- [ ] **PKCE `clientId` allocation + `/admin/auth/pkce/authorize` prod GA.** Owner: Connection auth. Needed by: before production rollout (not local dev).
- [ ] **Exact SA-token path env var** — align with b971146f and the Go `*_KUBERNETES_TOKEN_PATH` convention. Owner: Martin Vaško. Needed by: before implementation.
- [ ] **Approver names** for Security and kbc-stacks buckets. Owner: PSGO lead. Needed by: before RFC sign-off.

## 8. Next step

`ready-for-rfc`. Recommended approach (Option C hybrid) is locked, security pass is complete, the two design-blocking questions are resolved (D1 no-minting / whole-stack credential; D2 explicit-session-state `project_id` + select-project tool), and there are 5 stress-test findings. Remaining open questions are implementation details that do not change the RFC's shape.

Next: update `RFC.md` to the hybrid (Option C) model — add the local PKCE-login path, the explicit `project_id` session state + select-project tool, and fold in the five stress-test findings. Then proceed via the `rfc-write` flow / review.
