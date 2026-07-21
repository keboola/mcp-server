/**
 * Build a REAL local docs index from the public Keboola documentation — `npm run docs:crawl`.
 *
 * A simplified, local stand-in for the production out-of-band index builder (whose full
 * connectors live on the @keboola/docs-search side, keboola/ui#6672). It crawls the public
 * help + developer docs sitemaps, extracts each page's main text, chunks it, embeds with the
 * configured embedder, and writes the pgvector index the MCP reads. No Keboola stack / token
 * needed — only public HTTP + your local Postgres.
 *
 * Quickstart:
 *   docker compose up -d --wait pgvector
 *   DATABASE_URL=postgres://mcp:mcp@localhost:5432/docs DOCS_EMBEDDER_MODEL=local DOCS_EMBEDDER_DIM=384 \
 *     npm run docs:crawl -- --limit 50        # omit --limit for the full crawl
 *
 * Then point the MCP at the same DATABASE_URL + embedder and query docs_query / find_component_id.
 * Flags: --limit N (cap pages, for a quick run), --source help|dev|all (default all).
 */
import type * as CheerioNS from 'cheerio'; // type-only (erased) — cheerio is a runtime-optional dep
import { parseArgs } from 'node:util';
import { Pool } from 'pg';

import { createEmbedderFromEnv } from '@/clients/docsSearch';
import { parseEnv } from '@/env';
import { logger } from '@/logger';
import { migrateDocsIndex, seedDocsIndex, type SourceDoc } from './docsIndex';

type CheerioLoad = typeof CheerioNS.load;

type Source = { type: 'help' | 'dev'; sitemap: string };

const SOURCES: Record<string, Source> = {
  help: { type: 'help', sitemap: 'https://help.keboola.com/sitemap-index.xml' },
  dev: { type: 'dev', sitemap: 'https://developers.keboola.com/sitemap.xml' },
};

const CONCURRENCY = 6;

const fetchText = async (url: string): Promise<string> => {
  const res = await fetch(url, { headers: { 'user-agent': 'keboola-mcp-docs-crawl' } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.text();
};

/** Recursively resolve a sitemap or sitemap-index into a flat list of page URLs. */
const sitemapUrls = async (sitemapUrl: string): Promise<string[]> => {
  const xml = await fetchText(sitemapUrl);
  const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]!.trim());
  const isIndex = /<sitemapindex/i.test(xml);
  if (!isIndex) return locs;
  const nested = await Promise.all(locs.map((u) => sitemapUrls(u).catch(() => [])));
  return nested.flat();
};

/** Extract a page's title + main content text, dropping nav/script/style/etc. */
const extractPage = (html: string, load: CheerioLoad): { title: string; content: string } => {
  const $ = load(html);
  $('script, style, nav, header, footer, aside, noscript, svg').remove();
  const main = $('main, article, .sl-markdown-content, [role="main"]').first();
  const root = main.length ? main : $('body');
  const content = root.text().replace(/\s+/g, ' ').trim();
  const title = ($('h1').first().text() || $('title').text() || '').replace(/\s+/g, ' ').trim();
  return { title, content };
};

/** Run `worker` over `items` with bounded concurrency, collecting non-null results. */
const mapPool = async <T, R>(
  items: T[],
  limit: number,
  worker: (item: T, i: number) => Promise<R | null>,
): Promise<R[]> => {
  const out: R[] = [];
  let cursor = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (let i = cursor++; i < items.length; i = cursor++) {
      const r = await worker(items[i]!, i);
      if (r !== null) out.push(r);
    }
  });
  await Promise.all(runners);
  return out;
};

const main = async (): Promise<void> => {
  const { values } = parseArgs({
    options: { limit: { type: 'string' }, source: { type: 'string', default: 'all' } },
    allowPositionals: false,
  });
  const env = parseEnv();
  if (!env.DATABASE_URL) throw new Error('DATABASE_URL is required.');
  const embedder = createEmbedderFromEnv(env);
  if (!embedder) {
    throw new Error('No embedder configured (set DOCS_EMBEDDER_MODEL=local for a no-key local build).');
  }
  const limit = values.limit ? Number(values.limit) : Infinity;
  const chosen: Source[] =
    values.source === 'all'
      ? Object.values(SOURCES)
      : [SOURCES[values.source ?? 'all']].filter((s): s is Source => s !== undefined);
  if (chosen.length === 0) throw new Error(`Unknown --source "${values.source}" (help|dev|all).`);

  const { load } = await import('cheerio').catch(() => {
    throw new Error("docs:crawl needs the optional 'cheerio' package (`npm i cheerio`).");
  });

  // 1) Collect page URLs from the sitemaps.
  logger.info(`Resolving sitemaps for: ${chosen.map((s) => s.type).join(', ')}…`);
  const urlsBySource = await Promise.all(
    chosen.map(async (s) => ({ type: s.type, urls: await sitemapUrls(s.sitemap) })),
  );
  let pages = urlsBySource.flatMap((s) => s.urls.map((url) => ({ type: s.type, url })));
  if (Number.isFinite(limit)) pages = pages.slice(0, limit);
  logger.info(`Fetching ${pages.length} page(s) with concurrency ${CONCURRENCY}…`);

  // 2) Fetch + extract each page into a SourceDoc.
  let done = 0;
  const sources = await mapPool<{ type: 'help' | 'dev'; url: string }, SourceDoc>(
    pages,
    CONCURRENCY,
    async ({ type, url }) => {
      try {
        const { title, content } = extractPage(await fetchText(url), load);
        if (content.length < 200) return null; // skip near-empty pages (nav-only, redirects)
        if (++done % 25 === 0) logger.info(`  …extracted ${done}/${pages.length}`);
        return {
          sourceKey: `${type}:${new URL(url).pathname}`,
          sourceType: type,
          title: title || url,
          content,
          sourceUrl: url,
          componentType: null,
        };
      } catch (err) {
        logger.warn(`  skip ${url}: ${(err as Error).message}`);
        return null;
      }
    },
  );
  logger.info(`Extracted ${sources.length} docs. Embedding with "${embedder.model}" + indexing…`);

  // 3) Migrate + seed (chunk + embed + store). Full rebuild.
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    await migrateDocsIndex(pool, embedder.dim);
    const { docCount, chunkCount } = await seedDocsIndex(pool, embedder, sources);
    logger.info(`Docs index built from live docs: ${docCount} docs, ${chunkCount} chunks.`);
  } finally {
    await pool.end();
  }
};

main().catch((err) => {
  logger.error({ err }, 'docs:crawl failed');
  process.exitCode = 1;
});
