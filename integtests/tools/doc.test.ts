import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  embedInputFor,
  FIXTURE_SOURCES,
  migrateDocsIndex,
  seedDocsIndex,
} from '../../scripts/docsIndex';
import { callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest } from '../testproject/fixture';

import {
  createDocsSearchFromEnv,
  type DocsSearch,
  setDocsSearchForTests,
  StubEmbedder,
} from '@/clients/docsSearch';
import { parseEnv } from '@/env';

// Ported from integtests/tools/test_doc.py, rebuilt on the pgvector docs-search index
// (RFC: feature_spec/docs-search-pgvector/). Fully self-contained: it provisions a real
// pgvector via testcontainers, seeds the fixture corpus with the deterministic
// StubEmbedder, and drives docs_query / find_component_id through the MCP — no live docs
// service and no Keboola project needed (the docs index is global). Requires Docker.
//
// StubEmbedder makes retrieval reproducible but not semantic, so queries use a fixture's
// exact embed input (title\ncontent) to guarantee a top hit — mirroring the SDK's own
// deterministic integ tier.

const OVERVIEW = FIXTURE_SOURCES.find((d) => d.sourceKey === 'connection-docs:overview')!;
const MYSQL = FIXTURE_SOURCES.find((d) => d.sourceKey === 'component:keboola.ex-db-mysql')!;

describe('docs tools (integration, pgvector)', () => {
  let container: StartedPostgreSqlContainer | undefined;
  let pool: Pool | undefined;
  let provider: DocsSearch | undefined;
  let skipReason = '';

  beforeAll(async () => {
    try {
      container = await new PostgreSqlContainer('pgvector/pgvector:pg16').start();
    } catch (err) {
      skipReason = `no Docker / pgvector container: ${(err as Error).message}`;
      console.warn(`SKIP: docs tools integration — ${skipReason}`);
      return;
    }
    const connectionString = container.getConnectionUri();
    pool = new Pool({ connectionString });
    await migrateDocsIndex(pool);
    await seedDocsIndex(pool, new StubEmbedder(3072), FIXTURE_SOURCES);

    // Build the real provider from env, pointed at the container, with the stub embedder.
    provider = createDocsSearchFromEnv({
      ...parseEnv(),
      DATABASE_URL: connectionString,
      DOCS_EMBEDDER_MODEL: 'stub',
      DOCS_EMBEDDER_DIM: 3072,
    })!;
    setDocsSearchForTests(provider);
  }, 120_000);

  afterAll(async () => {
    setDocsSearchForTests(undefined);
    await provider?.close();
    await pool?.end();
    await container?.stop();
  });

  it('docs_query returns an answer with text and source URLs', async () => {
    if (skipReason) return;
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
    if (skipReason) return;
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
