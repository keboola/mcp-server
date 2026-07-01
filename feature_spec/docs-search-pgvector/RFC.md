# RFC: Replace the AI docs-service with the pgvector docs-search SDK

Linear: PSGO-268 (TypeScript rewrite follow-up) · depends on
[`keboola/ui#6672`](https://github.com/keboola/ui/pull/6672) —
`@keboola/docs-search` (pgvector index builder + retrieval SDK).

## Problem

Three MCP tools currently reach the **AI service** (`ai.<stack>` / the legacy Python
LanceDB-Qdrant docs index) for documentation intelligence:

| Tool | Today (AI service) | What it needs |
| --- | --- | --- |
| `docs_query` | `rawAi` `POST docs/question` | Q&A over Keboola docs with source URLs |
| `find_component_id` | `ai.suggestComponent` (`POST suggest/component`) | component recommendation for a task |
| `get_config_examples` / `fetchComponent` docs | `rawAi` `GET docs/components/{id}` | a component's documentation page |

Problems with the AI-service dependency:
- **Reliability**: live runs show intermittent `422 Request contents is not valid` from
  `docs/question` and read-timeouts — the docs path is the flakiest surface in the suite.
- **Opaque, external, single-region**: an out-of-band service the MCP can't reason about,
  version, or run locally; couples MCP availability to it.
- **Duplication**: `keboola/ui#6672` introduces `@keboola/docs-search`, a first-class SDK
  that builds the docs index into **pgvector** and serves the exact same three retrieval
  shapes. Continuing to call the AI service means maintaining two doc backends.

The rewrite already reuses `@keboola/api-client` for everything else; docs should likewise
move to the published SDK, backed by a Postgres/pgvector index that is **built out-of-band
and only read at runtime**.

## Required Behavior

| # | Requirement |
| --- | --- |
| 1 | `docs_query`, `find_component_id`, and component-docs retrieval are served by `@keboola/docs-search` (`answerQuestion` / `recommendComponents` / `getComponentDoc`) against a pgvector index — **same tool names, params, and output shapes** (`DocsAnswer{text, source_urls}`, suggested-component list, component doc). |
| 2 | The **AI-service integration is removed entirely**: `rawAi`, `ai` (typed client), the `ai`/`docs/*`/`suggest/*` endpoints and their URL derivation are deleted from the MCP. |
| 3 | **Postgres is added to `docker-compose`** (dev) and consumed via a single `DATABASE_URL` (pgvector-enabled instance) at runtime. |
| 4 | The index is **filled by a separate build job** (the `@keboola/docs-search` `runIndexBuild` + the source connectors), not by the MCP. A **cron job** rebuilds it periodically (incremental: only changed docs are re-embedded). |
| 5 | The docs tools are available **only when the MCP has access to the docs index** (a reachable, migrated, non-empty pgvector DB). When it isn't, the three docs tools degrade gracefully (filtered out / clear "docs index unavailable" error) — the rest of the server is unaffected. |
| 6 | **Auth/gate** (open question, see below): the docs index is **global, non-tenant data**; the MCP reads it with its own server-side `DATABASE_URL`. No per-user docs token is introduced; the tools follow normal read-only tool visibility. |
| 7 | The MCP has **no build-time dependency on index creation** — it starts and serves even if the index is stale/missing; index building lives entirely in the aside job. |

### Retrieval mapping (SDK → tools)

```
docs_query(query)            -> createDocsSearch({pool,embedder,llm}).answerQuestion(query) -> {text, source_urls}
find_component_id(query)     -> .recommendComponents(query)  -> [{componentId, score}]
get_config_examples(id) /
  component docs (fetchComponent) -> .getComponentDoc(id)    -> ParentDoc | null
```

The SDK is **dependency-injected** (`pool: pg.Pool`, `embedder: Embedder`, optional `llm: Llm`)
and owns no globals — it fits the MCP's per-request client-factory model.

### Open question — the access gate (point 5)

The docs index is **global** (Keboola help/dev docs + the public component catalog), not
project data, so reading it does not require a Storage-scoped grant. Candidate gates, with a
recommendation:

| Option | Meaning | Assessment |
| --- | --- | --- |
| **No auth on the index; tool gated by a valid MCP session** (recommended) | MCP holds `DATABASE_URL` (infra secret); the three docs tools are offered to any authenticated session, like other read-only tools | Simplest; matches that docs are non-sensitive/global; the DB is never exposed to the client |
| Storage token required | Reuse the per-request Storage token as the gate | Adds no real security (docs aren't project-scoped) but keeps "must be a Keboola user" |
| Management token | Gate on a management-scoped token | Overkill; docs are not org-admin data |
| Application/session token to the embedder | Needed only for the **embedder** (query embedding) if it calls a hosted embedding API | Orthogonal to index access — see below |

**Recommendation:** treat index access as **infrastructure** (server-side `DATABASE_URL`), keep
the tools visible to any authenticated session, and make the **embedder** credential a
deployment secret (the same way the AI service key is a deployment concern today), not a user
token. Final call to be confirmed with platform security before implementation.

## Resolution Strategy

Runtime (MCP) — thin, read-only:
- Add a `docsSearch` to the client factory: build a shared `pg.Pool` from `DATABASE_URL` and an
  `Embedder` from deployment config, then `createDocsSearch({ pool, embedder, llm })`. The pool
  is process-scoped (not per-request) and injected where the three tools need it.
- Rewrite `src/tools/doc.ts` (`docs_query` → `answerQuestion`), the `find_component_id` handler
  in `src/tools/search/` (→ `recommendComponents`), and the component-docs reads in
  `src/tools/components/` + `get_config_examples` (→ `getComponentDoc`). Preserve tool
  names/descriptions/output shapes exactly.
- **Delete** the AI surface: `rawAi`, the typed `ai` client, `urls.ai`, and related config.
- Availability gate: at server build, probe the index (`index_meta.last_success_at` present +
  reachable). If absent, filter the three docs tools out of `tools/list` and deny calls with a
  clear message (parity with the existing feature-gating in `src/mcp/filtering.ts`).
- Config: add `DATABASE_URL` (+ embedder endpoint/key/model) to `src/env.ts` as **optional**
  deployment env; the server boots without them (docs tools just gate off).

Index build (aside — NOT in the MCP request path):
- A separate job (own package/app or a small `scripts/` entry) runs the connectors (git clone
  of help/dev docs + component catalog + frontmatter parse), then `runIndexBuild(pool, {sources,
  embedder, gates})`. Transactional + incremental: COMMIT iff gates pass, else ROLLBACK (the
  prior index stays intact). This is scheduled by **cron** (see the architecture doc).
- The MCP and the build job share only the Postgres database (the index), never code paths.

Local dev:
- `docker-compose` gains a `pgvector/pgvector` service; `migrate(pool)` (from the SDK) applies
  `migrations/*.sql` (creates the `vector` extension + `doc`/`doc_chunk`/`index_manifest`/
  `index_meta` tables + HNSW index). A `npm run docs:build` convenience wires the connectors +
  `runIndexBuild` against the local DB so a developer can populate and query locally.

### Non-obvious trade-offs
- **Embedder at query time**: `answerQuestion`/`recommendComponents` embed the query, so the MCP
  needs an `Embedder` (a hosted embedding endpoint or a local model). This is the one remaining
  external call; it replaces the AI service with a much narrower dependency (embeddings only,
  no bespoke retrieval service). `getComponentDoc` needs **no** embedder (direct lookup).
- **`llm` for `answerQuestion`**: Q&A needs an LLM to synthesize the answer from retrieved docs.
  Provided via deployment config; if absent, `docs_query` can fall back to returning top
  retrieved snippets (`search`) rather than a synthesized answer.
- **Stale index tolerance**: because the MCP only reads, a failed/late build serves the last
  good index; the MCP never blocks on building.

## Scope

In scope: swapping the three docs tools onto `@keboola/docs-search`; removing the AI-service
client/URLs/config; adding Postgres to docker-compose + optional `DATABASE_URL`/embedder env;
the availability gate; a local `docs:build` path; unit tests (mock the SDK) + an integ test
(against a seeded local pgvector).

Out of scope (tracked in the architecture doc): the production index-build **app + cron**
deployment, the **source connectors** (git clone/frontmatter — owned by the build side per
#6672), Terraform for the provisioned Postgres + pgvector extension, and embedder-provider
selection/procurement.

## Testing / Verification
- **Unit**: `docs_query` / `find_component_id` / `get_config_examples` with an injected fake
  `DocsSearch` (stub `search`/`answerQuestion`/`getComponentDoc`/`recommendComponents`) — assert
  the tools map inputs/outputs unchanged. Availability-gate tests (index present/absent → tool
  visible/denied). No network.
- **Integration**: a local `pgvector` (docker-compose) seeded via `runIndexBuild` with a small
  deterministic `StubEmbedder`; run `docs_query`/`find_component_id`/component-docs through the
  MCP client and assert real retrieval. Mirrors the SDK's own testcontainers integ tier.
- **Parity check**: compare outputs against the current AI-service tools for a fixed query set
  before deleting the AI path.
