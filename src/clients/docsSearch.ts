/**
 * Docs-search retrieval client (pgvector-backed).
 *
 * Serves the `docs_query` and `find_component_id` tools from a prebuilt pgvector
 * documentation index (RFC: feature_spec/docs-search-pgvector/RFC.md). The MCP only
 * ever *reads* the index — it is built out-of-band by a cron job (see the architecture
 * doc); a missing/stale index degrades the two docs tools gracefully, nothing else.
 *
 * VENDORED, TEMPORARILY: the retrieval tier below is a verbatim copy of
 * `@keboola/docs-search` (keboola/ui#6672). That package is a private workspace package
 * not yet on the registry `@keboola/api-client` comes from, so it cannot be installed
 * here. Once it publishes, delete the vendored functions and replace the body of
 * `createDocsSearchFromEnv` with:
 *
 *     import { createDocsSearch, OpenAIEmbedder } from '@keboola/docs-search';
 *     const sdk = createDocsSearch({ pool, embedder, llm });
 *
 * keeping the exported {@link DocsSearch} interface identical so the tools don't change.
 *
 * `recommendComponents` exposes each result's `sourceKey` so `find_component_id` can
 * recover the component id (it lives in `source_key = 'component:<id>'`). This was added
 * to the SDK on keboola/ui#6672, so the vendored SELECT here matches the published shape
 * and the swap will be a clean drop-in.
 */
import { Pool } from 'pg';

import { type Env, parseEnv } from '@/env';
import { logger } from '@/logger';

// ---------------------------------------------------------------------------
// Contract (mirrors @keboola/docs-search public types)
// ---------------------------------------------------------------------------

export type RetrievedDoc = {
  id: string;
  /** Stable natural key, e.g. `component:keboola.ex-salesforce`. */
  sourceKey: string;
  sourceUrl: string;
  title: string | null;
  content: string;
  componentType: string | null;
  /** Cosine similarity in [0, 1]. */
  score: number;
};

export type DocsAnswer = { text: string; sourceUrls: string[] };

export type SearchOptions = {
  k?: number;
  minSimilarity?: number;
  componentType?: string | null;
  componentOnly?: boolean;
};

export type Embedder = {
  readonly model: string;
  readonly dim: number;
  /** Returns one unit-normalized vector per input text. */
  embed(texts: string[]): Promise<number[][]>;
};

export type Llm = {
  answer(input: { question: string; context: string }): Promise<{ answer: string }>;
};

export type DocsSearch = {
  search(query: string, opts?: SearchOptions): Promise<RetrievedDoc[]>;
  answerQuestion(question: string, opts?: SearchOptions): Promise<DocsAnswer>;
  recommendComponents(query: string, opts?: SearchOptions): Promise<RetrievedDoc[]>;
  /** True when the index is reachable and has at least one successfully-built doc. */
  isReady(): Promise<boolean>;
  close(): Promise<void>;
};

const DEFAULT_K = 15;
const DEFAULT_MIN_SIMILARITY = 0.25;

// ---------------------------------------------------------------------------
// Retrieval (vendored from @keboola/docs-search)
// ---------------------------------------------------------------------------

const toVectorLiteral = (vec: number[]): string => `[${vec.join(',')}]`;

type DocResultRow = {
  id: string;
  source_key: string;
  source_url: string;
  title: string | null;
  content: string;
  component_type: string | null;
  sim: number;
};

/**
 * ANN retrieval: rank child chunks by cosine distance (HNSW), collapse to best-scoring
 * parent doc, apply the similarity threshold, return top-k parents in score order.
 */
const search = async (
  pool: Pool,
  embedder: Embedder,
  query: string,
  opts: SearchOptions = {},
): Promise<RetrievedDoc[]> => {
  const k = opts.k ?? DEFAULT_K;
  const minSim = opts.minSimilarity ?? DEFAULT_MIN_SIMILARITY;
  const [vec] = await embedder.embed([query]);
  if (!vec) return [];
  const literal = toVectorLiteral(vec);
  const candidateLimit = k * 4; // over-fetch chunks to survive parent dedup

  const { rows } = await pool.query<DocResultRow>(
    `WITH cand AS (
       SELECT c.doc_id, (c.embedding <=> $1::halfvec) AS dist
       FROM doc_chunk c
       JOIN doc d ON d.id = c.doc_id
       WHERE ($3::text IS NULL OR d.component_type = $3)
         AND ($4::bool IS NOT TRUE OR d.component_type IS NOT NULL)
       ORDER BY c.embedding <=> $1::halfvec
       LIMIT $5
     ), best AS (
       SELECT DISTINCT ON (doc_id) doc_id, dist FROM cand ORDER BY doc_id, dist
     )
     SELECT d.id, d.source_key, d.source_url, d.title, d.content, d.component_type,
            (1 - b.dist) AS sim
     FROM best b JOIN doc d ON d.id = b.doc_id
     WHERE (1 - b.dist) >= $2
     ORDER BY sim DESC
     LIMIT $6`,
    [literal, minSim, opts.componentType ?? null, opts.componentOnly ?? false, candidateLimit, k],
  );

  return rows.map((r) => ({
    id: r.id,
    sourceKey: r.source_key,
    sourceUrl: r.source_url,
    title: r.title,
    content: r.content,
    componentType: r.component_type,
    score: Number(r.sim),
  }));
};

const formatContext = (docs: { content: string }[]): string =>
  docs.map((d) => `<source_doc>\n${d.content}\n</source_doc>`).join('\n\n');

// ---------------------------------------------------------------------------
// Embedder / LLM (OpenAI-compatible in prod; deterministic stub for local/CI)
// ---------------------------------------------------------------------------

/** Sentinel `DOCS_EMBEDDER_MODEL` value selecting the offline {@link StubEmbedder}. */
export const STUB_EMBEDDER_MODEL = 'stub';
/** Sentinel `DOCS_EMBEDDER_MODEL` value selecting the in-process {@link LocalEmbedder}. */
export const LOCAL_EMBEDDER_MODEL = 'local';

/** Default HuggingFace model + dim for the local embedder (small, fast, CPU-friendly). */
export const DEFAULT_LOCAL_MODEL = 'Xenova/all-MiniLM-L6-v2';
export const DEFAULT_LOCAL_DIM = 384;
/** Default dim for the stub / remote embedders (text-embedding-3-large native size). */
export const DEFAULT_EMBEDDER_DIM = 3072;

/**
 * Deterministic, offline embedder (vendored from @keboola/docs-search): the same text
 * always yields the same unit vector, with no network calls. NOT for production retrieval
 * quality — it exists so `docs:build` + the integ test can seed and query a real pgvector
 * index reproducibly (build and query embed identically). Selected via
 * `DOCS_EMBEDDER_MODEL=stub`.
 */
export class StubEmbedder implements Embedder {
  readonly model = STUB_EMBEDDER_MODEL;
  readonly dim: number;

  constructor(dim = 3072) {
    this.dim = dim;
  }

  embed(texts: string[]): Promise<number[][]> {
    return Promise.resolve(texts.map((t) => this.vector(t)));
  }

  private vector(text: string): number[] {
    // Seed an LCG from a rolling hash of the text, fill dim floats, L2-normalize.
    let seed = 2166136261;
    for (let i = 0; i < text.length; i++) {
      seed ^= text.charCodeAt(i);
      seed = Math.imul(seed, 16777619);
    }
    let state = seed >>> 0;
    const next = () => {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      return state / 0xffffffff - 0.5;
    };
    const v = Array.from({ length: this.dim }, next);
    const norm = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
    return v.map((x) => x / norm);
  }
}

/**
 * In-process embedder running a HuggingFace model as ONNX on CPU via transformers.js —
 * no external service, no API key (selected via `DOCS_EMBEDDER_MODEL=local`). The model
 * id (`DOCS_EMBEDDER_LOCAL_MODEL`) and dim (`DOCS_EMBEDDER_DIM`) are configurable; the dim
 * MUST match the model's output size and the pgvector column. `@huggingface/transformers`
 * is an optional dependency, dynamically imported so stub/remote users don't need it
 * installed; a clear error is thrown if it is missing.
 */
export class LocalEmbedder implements Embedder {
  readonly model: string;
  readonly dim: number;
  // Lazily-loaded feature-extraction pipeline (model weights are fetched/cached on first use).
  private extractor: Promise<
    (texts: string[], opts: object) => Promise<{ tolist(): number[][] }>
  > | null = null;

  constructor(model: string, dim: number) {
    this.model = model;
    this.dim = dim;
  }

  private pipeline() {
    if (!this.extractor) {
      this.extractor = import('@huggingface/transformers')
        .then(({ pipeline }) => pipeline('feature-extraction', this.model) as never)
        .catch((err: unknown) => {
          this.extractor = null;
          const cause = err instanceof Error ? err.message : String(err);
          throw new Error(
            "The local embedder needs the optional '@huggingface/transformers' package. " +
              'Install it (`npm i @huggingface/transformers`) or use a remote embedder ' +
              `(DOCS_EMBEDDER_ENDPOINT/API_KEY/MODEL). Cause: ${cause}`,
          );
        });
    }
    return this.extractor;
  }

  async embed(texts: string[]): Promise<number[][]> {
    const extractor = await this.pipeline();
    // Mean-pool + L2-normalize → one unit vector per text (cosine-ready).
    const output = await extractor(texts, { pooling: 'mean', normalize: true });
    return output.tolist();
  }
}

/** OpenAI/Azure-compatible embedder. Thin fetch wrapper — no SDK dependency. */
class OpenAIEmbedder implements Embedder {
  readonly model: string;
  readonly dim: number;
  private readonly endpoint: string;
  private readonly apiKey: string;

  constructor(opts: { endpoint: string; apiKey: string; model: string; dim: number }) {
    this.endpoint = opts.endpoint;
    this.apiKey = opts.apiKey;
    this.model = opts.model;
    this.dim = opts.dim;
  }

  async embed(texts: string[]): Promise<number[][]> {
    const res = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'api-key': this.apiKey },
      body: JSON.stringify({ input: texts, model: this.model }),
    });
    if (!res.ok) {
      throw new Error(`embedding request failed: ${res.status} ${await res.text()}`);
    }
    const json = (await res.json()) as { data: { embedding: number[] }[] };
    return json.data.map((d) => d.embedding);
  }
}

/** OpenAI-compatible chat LLM for answerQuestion synthesis. */
class OpenAILlm implements Llm {
  private readonly endpoint: string;
  private readonly apiKey: string;
  private readonly model: string;

  constructor(opts: { endpoint: string; apiKey: string; model: string }) {
    this.endpoint = opts.endpoint;
    this.apiKey = opts.apiKey;
    this.model = opts.model;
  }

  async answer(input: { question: string; context: string }): Promise<{ answer: string }> {
    const res = await fetch(this.endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'api-key': this.apiKey },
      body: JSON.stringify({
        model: this.model,
        messages: [
          {
            role: 'system',
            content:
              'You answer questions using ONLY the provided Keboola documentation excerpts. ' +
              'Be concise. If the excerpts do not contain the answer, say so.',
          },
          { role: 'user', content: `Question: ${input.question}\n\nDocs:\n${input.context}` },
        ],
      }),
    });
    if (!res.ok) {
      throw new Error(`llm request failed: ${res.status} ${await res.text()}`);
    }
    const json = (await res.json()) as { choices: { message: { content: string } }[] };
    return { answer: json.choices[0]?.message.content ?? '' };
  }
}

// ---------------------------------------------------------------------------
// Provider construction + assembly
// ---------------------------------------------------------------------------

const buildDocsSearch = (pool: Pool, embedder: Embedder, llm: Llm | null): DocsSearch => ({
  search: (query, opts) => search(pool, embedder, query, opts),
  recommendComponents: (query, opts) =>
    search(pool, embedder, query, { ...opts, componentOnly: true }),
  answerQuestion: async (question, opts) => {
    const docs = await search(pool, embedder, question, opts);
    const sourceUrls = [...new Set(docs.map((d) => d.sourceUrl))];
    if (!llm) {
      // No LLM configured: fall back to returning the retrieved snippets (RFC trade-off).
      const text = docs.length
        ? docs.map((d) => d.content).join('\n\n---\n\n')
        : 'No relevant documentation was found.';
      return { text, sourceUrls };
    }
    const { answer } = await llm.answer({ question, context: formatContext(docs) });
    return { text: answer, sourceUrls };
  },
  isReady: async () => {
    try {
      const { rows } = await pool.query<{ last_success_at: Date | null; doc_count: number | null }>(
        'SELECT last_success_at, doc_count FROM index_meta LIMIT 1',
      );
      const meta = rows[0];
      return Boolean(meta?.last_success_at) && (meta?.doc_count ?? 0) > 0;
    } catch (err) {
      logger.warn({ err }, 'docs-search index probe failed');
      return false;
    }
  },
  close: () => pool.end(),
});

/**
 * Builds the query-time embedder from env, by `DOCS_EMBEDDER_MODEL`:
 *   - `stub`  → deterministic offline {@link StubEmbedder} (CI/tests), dim = DOCS_EMBEDDER_DIM ?? 3072
 *   - `local` → in-process {@link LocalEmbedder} (HuggingFace/ONNX), model = DOCS_EMBEDDER_LOCAL_MODEL
 *               ?? all-MiniLM-L6-v2, dim = DOCS_EMBEDDER_DIM ?? 384
 *   - else (endpoint+key+model) → remote {@link OpenAIEmbedder}, dim = DOCS_EMBEDDER_DIM ?? 3072
 * Returns `null` when the config is incomplete (docs tools then gate off). Shared with the
 * `docs:build` seeder so the index is built and queried with the same model + dim.
 */
export const createEmbedderFromEnv = (env: Env): Embedder | null => {
  const kind = env.DOCS_EMBEDDER_MODEL;
  if (kind === STUB_EMBEDDER_MODEL) {
    return new StubEmbedder(env.DOCS_EMBEDDER_DIM ?? DEFAULT_EMBEDDER_DIM);
  }
  if (kind === LOCAL_EMBEDDER_MODEL) {
    return new LocalEmbedder(
      env.DOCS_EMBEDDER_LOCAL_MODEL ?? DEFAULT_LOCAL_MODEL,
      env.DOCS_EMBEDDER_DIM ?? DEFAULT_LOCAL_DIM,
    );
  }
  if (!env.DOCS_EMBEDDER_ENDPOINT || !env.DOCS_EMBEDDER_API_KEY || !kind) {
    return null;
  }
  return new OpenAIEmbedder({
    endpoint: env.DOCS_EMBEDDER_ENDPOINT,
    apiKey: env.DOCS_EMBEDDER_API_KEY,
    model: kind,
    dim: env.DOCS_EMBEDDER_DIM ?? DEFAULT_EMBEDDER_DIM,
  });
};

/**
 * Builds the docs-search provider from deployment env, or returns `null` when the index
 * is not configured (no `DATABASE_URL`, or no embedder credentials). A `null` provider
 * gates the docs tools off — the rest of the server is unaffected.
 */
export const createDocsSearchFromEnv = (env: Env): DocsSearch | null => {
  if (!env.DATABASE_URL) return null;
  const embedder = createEmbedderFromEnv(env);
  if (!embedder) {
    logger.warn('DATABASE_URL is set but DOCS_EMBEDDER_* is not; docs tools disabled.');
    return null;
  }
  const pool = new Pool({ connectionString: env.DATABASE_URL, max: 4 });
  const llm =
    env.DOCS_LLM_ENDPOINT && env.DOCS_LLM_API_KEY && env.DOCS_LLM_MODEL
      ? new OpenAILlm({
          endpoint: env.DOCS_LLM_ENDPOINT,
          apiKey: env.DOCS_LLM_API_KEY,
          model: env.DOCS_LLM_MODEL,
        })
      : null;
  return buildDocsSearch(pool, embedder, llm);
};

// ---------------------------------------------------------------------------
// Process-scoped accessor (the pool must outlive a single request)
// ---------------------------------------------------------------------------

let cached: DocsSearch | null | undefined;
let override: DocsSearch | null | undefined;

/** Returns the process-scoped docs-search provider (memoized), or `null` if unconfigured. */
export const getDocsSearch = (env?: Env): DocsSearch | null => {
  if (override !== undefined) return override;
  if (cached === undefined) {
    cached = createDocsSearchFromEnv(env ?? parseEnv());
  }
  return cached;
};

/** Test seam: force the provider (or `null`) and bypass env. Pass `undefined` to reset. */
export const setDocsSearchForTests = (provider: DocsSearch | null | undefined): void => {
  override = provider;
};
