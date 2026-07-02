/**
 * Local docs-index builder — `npm run docs:build`.
 *
 * Populates the pgvector docs index at DATABASE_URL with a small fixture corpus so a
 * developer can run `docs_query` / `find_component_id` against a real local index. This is
 * the dev mirror of the production out-of-band build (see
 * feature_spec/docs-search-pgvector/architecture.md); it does NOT fetch real docs.
 *
 * Local quickstart:
 *   docker compose up -d pgvector
 *   DATABASE_URL=postgres://mcp:mcp@localhost:5432/docs DOCS_EMBEDDER_MODEL=stub \
 *     npm run docs:build
 *
 * With DOCS_EMBEDDER_MODEL=stub the deterministic offline embedder is used (no API key).
 * Point DOCS_EMBEDDER_* at a real embedding endpoint for semantically meaningful vectors.
 */
import { Pool } from 'pg';

import { createEmbedderFromEnv } from '@/clients/docsSearch';
import { parseEnv } from '@/env';
import { logger } from '@/logger';
import { FIXTURE_SOURCES, migrateDocsIndex, seedDocsIndex } from './docsIndex';

const main = async (): Promise<void> => {
  const env = parseEnv();
  if (!env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required (e.g. postgres://mcp:mcp@localhost:5432/docs).');
  }
  const embedder = createEmbedderFromEnv(env);
  if (!embedder) {
    throw new Error(
      'No embedder configured. Set DOCS_EMBEDDER_MODEL=stub for offline dev, or DOCS_EMBEDDER_ENDPOINT/API_KEY/MODEL.',
    );
  }

  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    logger.info(`Building docs index with embedder "${embedder.model}" (dim ${embedder.dim})…`);
    await migrateDocsIndex(pool);
    const { docCount, chunkCount } = await seedDocsIndex(pool, embedder, FIXTURE_SOURCES);
    logger.info(`Docs index built: ${docCount} docs, ${chunkCount} chunks.`);
  } finally {
    await pool.end();
  }
};

main().catch((err) => {
  logger.error({ err }, 'docs:build failed');
  process.exitCode = 1;
});
