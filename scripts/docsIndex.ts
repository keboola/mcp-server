/**
 * Local/CI docs-index provisioning: schema + a small fixture corpus + a minimal seeder.
 *
 * This is the *local* mirror of the production index build. In production the index is
 * built out-of-band by the `@keboola/docs-search` indexer (`runIndexBuild` + the source
 * connectors that clone the help/dev repos and the component catalog) — see
 * feature_spec/docs-search-pgvector/architecture.md. That machinery is out of scope for
 * this repo; here we only need *some* reproducible content so a developer (and CI) can run
 * `docs_query` / `find_component_id` end-to-end against a real pgvector index.
 *
 * The seeder is deliberately minimal (one chunk per doc, no incremental diffing/gates) —
 * enough for retrieval to work. Paired with the deterministic StubEmbedder, build and
 * query embed identically, so retrieval is reproducible offline.
 */
import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';

import type { Embedder } from '@/clients/docsSearch';

/** A source document to index (subset of @keboola/docs-search `SourceDoc`). */
export type SourceDoc = {
  sourceKey: string;
  sourceType: 'help' | 'dev' | 'component';
  title: string;
  content: string;
  sourceUrl: string;
  componentType: string | null;
};

/** Idempotent schema (mirrors @keboola/docs-search migrations/001_init.sql). */
/** Default embedding dimension (text-embedding-3-large / stub). The local embedder is 384. */
export const DEFAULT_DIM = 3072;

/**
 * Idempotent schema at a given embedding dimension (mirrors @keboola/docs-search
 * migrations/001_init.sql, but the `halfvec(N)` size is parametrized). `halfvec` covers
 * up to 4000 dims, so the same type works for 384 / 768 / 1024 / 1536 / 3072.
 */
export const migrationSql = (dim: number): string => `
create extension if not exists vector;

create table if not exists doc (
  id             uuid primary key,
  source_key     text not null unique,
  source_type    text not null,
  source_url     text not null,
  title          text,
  content        text not null,
  component_type text,
  content_hash   text not null
);

create table if not exists doc_chunk (
  id          uuid primary key,
  doc_id      uuid not null references doc(id) on delete cascade,
  ordinal     int  not null,
  embed_input text not null,
  embedding   halfvec(${dim}) not null
);

create table if not exists index_manifest (
  source_key   text primary key,
  content_hash text not null,
  doc_id       uuid not null,
  indexed_at   timestamptz not null default now()
);

create table if not exists index_meta (
  id               boolean primary key default true check (id),
  embedding_model  text not null,
  embedding_dim    int  not null,
  last_success_at  timestamptz,
  doc_count        int,
  chunk_count      int
);

create index if not exists doc_chunk_embedding_hnsw
  on doc_chunk using hnsw (embedding halfvec_cosine_ops);
create index if not exists doc_component_type_idx on doc (component_type);
create index if not exists doc_source_type_idx on doc (source_type);
`;

/** A tiny but representative corpus: a couple of help/dev pages + a few component docs. */
export const FIXTURE_SOURCES: SourceDoc[] = [
  {
    sourceKey: 'connection-docs:overview',
    sourceType: 'help',
    title: 'What is Keboola Connection?',
    content:
      'Keboola Connection is a data operations platform that lets you extract, store, ' +
      'transform, and write data across many services. Storage holds your data in buckets ' +
      'and tables; components run extractions, transformations, and writers.',
    sourceUrl: 'https://help.keboola.com/overview/',
    componentType: null,
  },
  {
    sourceKey: 'developers-docs:api',
    sourceType: 'dev',
    title: 'Storage API basics',
    content:
      'The Storage API manages buckets, tables, and file uploads. Authenticate with a ' +
      'Storage API token via the X-StorageApi-Token header.',
    sourceUrl: 'https://developers.keboola.com/integrate/storage/api/',
    componentType: null,
  },
  {
    sourceKey: 'component:keboola.ex-db-mysql',
    sourceType: 'component',
    title: 'MySQL extractor',
    content:
      'The MySQL extractor loads data from a MySQL database into Keboola Storage. Configure ' +
      'host, port, database, user, password, and the tables or queries to extract.',
    sourceUrl: 'https://components.keboola.com/components/keboola.ex-db-mysql',
    componentType: 'extractor',
  },
  {
    sourceKey: 'component:keboola.wr-db-snowflake',
    sourceType: 'component',
    title: 'Snowflake writer',
    content:
      'The Snowflake writer loads tables from Keboola Storage into a Snowflake database. ' +
      'Configure the connection and the input mapping of tables to write.',
    sourceUrl: 'https://components.keboola.com/components/keboola.wr-db-snowflake',
    componentType: 'writer',
  },
  {
    sourceKey: 'component:keboola.ex-google-analytics-v4',
    sourceType: 'component',
    title: 'Google Analytics extractor',
    content:
      'The Google Analytics extractor pulls reports and metrics from Google Analytics 4 ' +
      'into Keboola Storage using OAuth authorization.',
    sourceUrl: 'https://components.keboola.com/components/keboola.ex-google-analytics-v4',
    componentType: 'extractor',
  },
];

const vectorLiteral = (vec: number[]): string => `[${vec.join(',')}]`;

/**
 * Applies the idempotent schema at `dim`. If the index already exists at a *different*
 * embedding dimension, the tables are dropped and recreated — switching embedder/dim
 * requires a full reindex anyway, and the seeder rebuilds from scratch. (In production the
 * dim is stable; a planned dim change is an intentional reindex.)
 */
export const migrateDocsIndex = async (pool: Pool, dim: number = DEFAULT_DIM): Promise<void> => {
  const { rows } = await pool.query<{ type: string }>(
    `SELECT format_type(a.atttypid, a.atttypmod) AS type
     FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
     WHERE c.relname = 'doc_chunk' AND a.attname = 'embedding' AND NOT a.attisdropped`,
  );
  const current = rows[0]?.type; // e.g. "halfvec(3072)"
  if (current && current !== `halfvec(${dim})`) {
    await pool.query('DROP TABLE IF EXISTS doc_chunk, doc, index_manifest, index_meta CASCADE');
  }
  await pool.query(migrationSql(dim));
};

/**
 * Splits `text` into ~`size`-char windows with `overlap` chars of carry-over, breaking on
 * whitespace so words aren't cut. Short text yields a single chunk. Keeps long real docs
 * retrievable (each chunk is embedded + searched independently, then collapsed to its parent).
 */
export const chunkText = (text: string, size = 1000, overlap = 100): string[] => {
  const clean = text.replace(/\s+/g, ' ').trim();
  if (clean.length <= size) return clean ? [clean] : [];
  const chunks: string[] = [];
  let start = 0;
  while (start < clean.length) {
    let end = Math.min(start + size, clean.length);
    if (end < clean.length) {
      const nextSpace = clean.lastIndexOf(' ', end);
      if (nextSpace > start) end = nextSpace;
    }
    chunks.push(clean.slice(start, end).trim());
    if (end >= clean.length) break;
    start = Math.max(end - overlap, start + 1);
  }
  return chunks;
};

/**
 * Wipes and re-seeds the index from `sources` using `embedder`: each doc is chunked
 * (see {@link chunkText}) into one-or-more `doc_chunk` rows, all pointing at the parent
 * `doc`. Stamps `index_meta` so the availability probe reports ready. Returns counts.
 */
export const seedDocsIndex = async (
  pool: Pool,
  embedder: Embedder,
  sources: SourceDoc[] = FIXTURE_SOURCES,
): Promise<{ docCount: number; chunkCount: number }> => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('TRUNCATE doc, doc_chunk, index_manifest, index_meta RESTART IDENTITY');

    let chunkCount = 0;
    for (let i = 0; i < sources.length; i++) {
      const doc = sources[i]!;
      const chunks = chunkText(doc.content);
      if (chunks.length === 0) continue;
      // Prefix each chunk with the title (parity with the SDK's embed_input) and embed the batch.
      const embedInputs = chunks.map((c) => `${doc.title}\n${c}`);
      const vectors = await embedder.embed(embedInputs);

      const docId = randomUUID();
      await client.query(
        `INSERT INTO doc (id, source_key, source_type, source_url, title, content, component_type, content_hash)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [docId, doc.sourceKey, doc.sourceType, doc.sourceUrl, doc.title, doc.content, doc.componentType, String(i)],
      );
      for (let j = 0; j < chunks.length; j++) {
        await client.query(
          `INSERT INTO doc_chunk (id, doc_id, ordinal, embed_input, embedding)
           VALUES ($1, $2, $3, $4, $5::halfvec)`,
          [randomUUID(), docId, j, embedInputs[j]!, vectorLiteral(vectors[j]!)],
        );
        chunkCount++;
      }
      await client.query(
        `INSERT INTO index_manifest (source_key, content_hash, doc_id) VALUES ($1, $2, $3)`,
        [doc.sourceKey, String(i), docId],
      );
    }

    await client.query(
      `INSERT INTO index_meta (id, embedding_model, embedding_dim, last_success_at, doc_count, chunk_count)
       VALUES (true, $1, $2, now(), $3, $4)`,
      [embedder.model, embedder.dim, sources.length, chunkCount],
    );
    await client.query('COMMIT');
    return { docCount: sources.length, chunkCount };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
};

/** Convenience: the exact text used as a chunk's embed input (deterministic-query helper). */
export const embedInputFor = (doc: SourceDoc): string => `${doc.title}\n${doc.content}`;
