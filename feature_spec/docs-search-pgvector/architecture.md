# Architecture & Rollout: Docs-search on Postgres/pgvector

Companion to [`RFC.md`](./RFC.md). This document covers the **infrastructure and rollout** for
serving MCP documentation intelligence from a pgvector index built out-of-band — the pieces the
platform doesn't have yet (a provisioned Postgres with `pgvector`, the extension install path, a
scheduled index-build job) and how they fit together.

Guiding principle: **the MCP server reads a prebuilt index; it never builds one.** Index
creation is a separate, scheduled concern. If the builder is down or mid-run, the MCP keeps
serving the last committed index.

## 1. Components & topology

```
                 ┌──────────────────────────────────────────────────────────┐
   docs sources  │  Index Builder (aside job / cron)                         │
   (help repo,   │   connectors: git clone + frontmatter parse + URL derive  │
    dev-portal,  │   @keboola/docs-search: chunk → hash-diff → embed(changed)│
    component    │   → runIndexBuild(): txn COMMIT iff gates pass else ROLLBACK│
    catalog)     └───────────────┬──────────────────────────────────────────┘
                                  │ writes (transactional)
                                  ▼
                       ┌────────────────────────┐        embeddings
                       │  Postgres + pgvector    │◀───────  Embedder (hosted embedding API
                       │  doc / doc_chunk /      │           or self-hosted model)
                       │  index_manifest /       │
                       │  index_meta (HNSW idx)  │
                       └───────────┬─────────────┘
                                   │ reads only (SELECT + vector search)
                                   ▼
                       ┌────────────────────────┐
                       │  MCP server (per pod)   │  createDocsSearch({pool, embedder, llm})
                       │  docs_query /           │  → answerQuestion / recommendComponents
                       │  find_component_id /    │  → getComponentDoc
                       │  get_config_examples    │
                       └────────────────────────┘
```

Two independent deployables share **only the database**:
- **Index Builder** — writes the index. Scheduled (cron). Owns the connectors + `runIndexBuild`.
- **MCP server** — reads the index. Stateless w.r.t. the index; one shared `pg.Pool` per pod.

## 2. Postgres + pgvector provisioning (new dependency)

The platform has no Postgres for the MCP today. Rollout:

- **Instance**: a managed Postgres (per stack/region) reachable by both the MCP pods and the
  builder job. Sized for the index (docs are small; the embeddings dominate — `halfvec(3072)`
  ≈ 6 KB/chunk; a few 10k chunks ⇒ low hundreds of MB + the HNSW index). Start small; it is
  read-mostly with a periodic write burst.
- **Extension `pgvector` (≥ 0.7 for `halfvec`)**: must be installed/allow-listed on the
  instance. Managed-Postgres offerings differ:
  - Cloud SQL / RDS / Azure Flexible: enable `vector` from the supported-extensions list, then
    `CREATE EXTENSION vector;` (the SDK's idempotent migration does this — but the extension
    must be *permitted* first).
  - Self-managed: install the `pgvector` package into the image/host.
- **Terraform** (per the platform's `kbc-stacks` conventions):
  - a `postgresql` instance/database + a least-privilege role for the MCP (read: `SELECT` on
    `doc`/`doc_chunk`/`index_meta`) and a separate role for the builder (read/write + DDL for
    migrations).
  - enable the `vector` extension flag on the instance (provider-specific: e.g. Cloud SQL
    `database_flags`/enabled-extensions, or an `apt`/image step for self-managed).
  - output `DATABASE_URL` (or split host/port/db/user/password) into the MCP + builder secrets.
  - network/SG rules so both workloads can reach the DB; TLS required.
- **Migrations**: `@keboola/docs-search` ships idempotent `migrations/*.sql` (`migrate(pool)`),
  applied by the **builder** at build start (it owns DDL). The MCP role needs no DDL.

## 3. The index build job (cron)

- **What it runs**: the source connectors (git clone of the help + dev-portal repos, fetch the
  component catalog, parse frontmatter, derive canonical URLs) → `runIndexBuild(pool, {sources,
  embedder, gates})`.
- **Incremental**: content-hash diff via `index_manifest` — only changed docs are re-embedded
  (embedding is the cost/latency driver). Unchanged docs are untouched.
- **Transactional + gated**: the build COMMITs only if validation gates pass
  (`minDocs`/`minComponents`/`maxDocDropPct`) — otherwise ROLLBACK, leaving the previous index
  intact. A bad docs push or a connector regression can never publish an empty/broken index.
- **Schedule**: cron (e.g. hourly/daily depending on docs churn) as a k8s `CronJob` (or the
  platform's scheduler). Single-flight: overlapping runs must not race — use a build advisory
  lock or `concurrencyPolicy: Forbid`.
- **Embedder**: the builder needs the embedding provider credential (deployment secret). Same
  model/dim as the MCP's query-time embedder (must match — `index_meta.embedding_model`/`dim`
  record it; the MCP can assert compatibility on startup).
- **Observability**: emit build duration, docs added/changed/removed, embed calls, gate
  outcome, and `index_meta.last_success_at`. Alert if `last_success_at` age exceeds an SLO.

## 4. Index lifecycle & the MCP's read contract

- **Freshness**: MCP reads whatever is committed; staleness bounded by the cron cadence. No
  runtime coupling to the builder.
- **Availability probe**: on server build the MCP checks the index is reachable + populated
  (`index_meta.last_success_at IS NOT NULL`, `doc_count > 0`). Result drives tool visibility
  (RFC point 5): index healthy → docs tools offered; else filtered out + calls denied with a
  clear message. The rest of the MCP is unaffected.
- **Model-compatibility guard**: if the MCP's query embedder model/dim ≠ `index_meta`'s, the
  docs tools gate off (mismatched vectors would return garbage) and log a loud warning.
- **Connection management**: one `pg.Pool` per MCP pod (small max; the docs tools are
  low-QPS), created at startup, closed on shutdown.

## 5. Failure modes

| Failure | Effect | Mitigation |
| --- | --- | --- |
| Builder fails / gates fail | No new index published | ROLLBACK keeps last good index; MCP unaffected; alert on `last_success_at` age |
| Postgres unreachable from MCP | Docs tools unavailable | Availability probe gates the 3 tools off; other tools keep working |
| Embedder (query) down | `docs_query`/`find_component_id` error | Tool-level error; `getComponentDoc` still works (no embedder); optional `search`-only fallback for `docs_query` |
| Extension not enabled | Migration/build fails | Terraform enables `vector`; builder migration is the canary |
| Model/dim drift builder↔MCP | Wrong results | Startup compatibility guard gates docs tools off |
| Index empty on first rollout | Docs tools gated off until first successful build | Ship builder + run once before enabling docs tools in prod |

## 6. Rollout sequence

1. **Land `@keboola/docs-search`** (#6672) + the source connectors (its follow-up).
2. **Provision Postgres + `pgvector`** per stack via Terraform; wire `DATABASE_URL` secrets to
   both the MCP and the builder.
3. **Deploy the builder + cron**; run once; verify `index_meta` populated and gates pass.
4. **Ship the MCP docs-search integration** behind the availability probe (RFC) — with the
   AI-service path still present, feature-flagged, so we can A/B the outputs.
5. **Parity-verify** the three tools against the AI service on a fixed query set.
6. **Cut over**: enable docs-search, **remove the AI-service integration** (RFC point 2).
7. **Decommission** the legacy AI docs service once no MCP/stack references it.

## 7. Security & cost notes

- **Index access is infrastructure**, not a user grant: the MCP holds `DATABASE_URL`; clients
  never touch Postgres. Docs are global/non-tenant, so no per-project authorization is needed
  (see RFC §"the access gate"). Confirm with platform security before cutover.
- **Least privilege**: distinct DB roles for MCP (read) vs builder (read/write/DDL).
- **Embedder credential** is a deployment secret (MCP query-time + builder), rotated like any
  other; it is the only external call left on the docs path.
- **Cost**: embeddings are computed only for changed docs (incremental) at build time, and once
  per query at read time — far cheaper than a bespoke hosted retrieval service; Postgres is
  read-mostly and small.

## 8. Embedding model & dimensions

pgvector (like LanceDB) is only the vector **store** — it does not generate embeddings. A
model must turn text → vector; that model is the one external piece. The MCP + the seeder
select it via `DOCS_EMBEDDER_MODEL`, and **build-time and query-time must use the same model
and dimension** (mismatched vectors return garbage):

| `DOCS_EMBEDDER_MODEL` | Embedder | Infra to provision | Dim |
| --- | --- | --- | --- |
| `stub` | Deterministic offline hash (CI/tests only — **not semantic**) | none | 3072 (default) |
| `local` | In-process HuggingFace model via transformers.js (ONNX, CPU) — no service, no key | **none** (optional `@huggingface/transformers` dep; model weights cached on first use) | model-native (e.g. 384 `all-MiniLM-L6-v2`, 1024 `bge-large`) |
| *(remote model name)* | OpenAI/Azure-compatible endpoint (`DOCS_EMBEDDER_ENDPOINT`/`API_KEY`) — e.g. reuse kai-bot's `embeddings` deployment | reuse existing / provision one | 3072 `text-embedding-3-large` (or 1536 small) |

- **Dimension is configurable** end-to-end: `DOCS_EMBEDDER_DIM` drives the embedder and the
  `halfvec(N)` column (`halfvec` indexes up to 4000 dims, so 384/768/1024/1536/3072 all work;
  under 2000 a plain `vector(N)` is also possible). The local seeder recreates the tables if
  the dim changes.
- **3072 vs 1024**: more dims = marginally better retrieval + ~3× storage/search cost. On a
  bounded docs corpus 1024 is within a couple % of 3072 — a good default for local models.
- **To avoid provisioning any embedding infra**, use `local` (only Postgres is provisioned) or
  reuse kai-bot's already-provisioned Azure `embeddings` deployment. Provisioning a *new*
  dedicated deployment is the only option that adds infra.
