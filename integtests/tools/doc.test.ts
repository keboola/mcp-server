import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { embedInputFor, FIXTURE_SOURCES, migrateDocsIndex, seedDocsIndex } from '../../scripts/docsIndex';
import { callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest } from '../testproject/fixture';

import { StubEmbedder } from '@/clients/docsSearch';

// Ported from integtests/tools/test_doc.py, rebuilt on the pgvector docs-search index
// (RFC: feature_spec/docs-search-pgvector/). Drives docs_query + find_component_id through
// the MCP against a real Postgres provided by docker-compose (see the `integration_tests`
// CI job). The suite seeds the fixture corpus with the deterministic StubEmbedder — the
// same embedder the server uses at query time (DOCS_EMBEDDER_MODEL=stub) — so retrieval is
// reproducible offline.
//
// Requires DATABASE_URL + DOCS_EMBEDDER_MODEL=stub; skips otherwise (parity with
// storage_branches). Locally:
//   docker compose up -d pgvector
//   DATABASE_URL=postgres://mcp:mcp@localhost:5432/docs DOCS_EMBEDDER_MODEL=stub npm run test:integ
//
// StubEmbedder is deterministic but not semantic, so queries use a fixture's exact embed
// input (title\ncontent) to guarantee a top hit — mirroring the SDK's own integ tier.

const DATABASE_URL = (process.env.DATABASE_URL ?? '').trim();
const STUB = process.env.DOCS_EMBEDDER_MODEL === 'stub';
const describeDocs = DATABASE_URL && STUB ? describe : describe.skip;
if (!(DATABASE_URL && STUB)) {
  console.warn('SKIP: docs tools integration — set DATABASE_URL + DOCS_EMBEDDER_MODEL=stub.');
}

const OVERVIEW = FIXTURE_SOURCES.find((d) => d.sourceKey === 'connection-docs:overview')!;
const MYSQL = FIXTURE_SOURCES.find((d) => d.sourceKey === 'component:keboola.ex-db-mysql')!;

describeDocs('docs tools (integration, pgvector)', () => {
  let pool: Pool | undefined;

  beforeAll(async () => {
    pool = new Pool({ connectionString: DATABASE_URL });
    const embedder = new StubEmbedder(3072);
    await migrateDocsIndex(pool, embedder.dim);
    await seedDocsIndex(pool, embedder, FIXTURE_SOURCES);
  }, 60_000);

  afterAll(async () => {
    await pool?.end();
  });

  it('docs_query returns an answer with text and source URLs', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'docs_query', {
        query: embedInputFor(OVERVIEW),
      });
      expect(text.length).toBeGreaterThan(0);
      expect(text).toContain('Keboola Connection');
      expect(text).toContain(OVERVIEW.sourceUrl);
    } finally {
      await session.close();
    }
  });

  it('find_component_id recommends a component id from the index', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'find_component_id', {
        query: embedInputFor(MYSQL),
      });
      // The component id is recovered from the doc's `component:<id>` source key.
      expect(text).toContain('keboola.ex-db-mysql');
    } finally {
      await session.close();
    }
  });
});
