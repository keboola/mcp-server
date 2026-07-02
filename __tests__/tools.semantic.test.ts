import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { registerSemanticTools } from '@/tools/semantic';

const mswServer = setupServer();
beforeAll(() => mswServer.listen({ onUnhandledRequest: 'error' }));
afterEach(() => mswServer.resetHandlers());
afterAll(() => mswServer.close());

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

const connect = async () => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  const server = new McpServer({ name: 'test', version: '0.0.0' });
  registerSemanticTools(server, config);
  await server.connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client;
};

const callText = async (
  client: Awaited<ReturnType<typeof connect>>,
  name: string,
  args: Record<string, unknown>,
) => {
  const result = await client.callTool({ name, arguments: args });
  return result;
};

const text = (result: Awaited<ReturnType<typeof callText>>): string =>
  (result.content as { text: string }[])[0]!.text;

// JSON:API list envelope; each item has top-level type/id/attributes/meta.
const listResponse = (
  items: {
    type: string;
    id: string;
    attributes?: Record<string, unknown>;
    meta?: Record<string, unknown>;
  }[],
) => HttpResponse.json({ data: items });

const objectResponse = (item: {
  type: string;
  id: string;
  attributes?: Record<string, unknown>;
  meta?: Record<string, unknown>;
}) => HttpResponse.json({ data: item });

// Helper: route metastore repository/schema requests on metastore.test.
// Paged list endpoints page until a short page is returned; to make finite,
// only serve list items on offset=0 and return an empty page otherwise.
const metastore = (handlers: (url: URL, objectType: string) => Response | undefined) =>
  http.get('https://metastore.test/*', ({ request }) => {
    const url = new URL(request.url);
    const m = url.pathname.match(/\/api\/v1\/(?:repository|schema)\/([^/]+)/);
    const objectType = m ? m[1]! : '';
    const isList =
      url.pathname.includes('/repository/') && !/\/repository\/[^/]+\/.+/.test(url.pathname);
    const offset = Number(url.searchParams.get('offset') ?? '0');
    if (isList && offset > 0) {
      return HttpResponse.json({ data: [] });
    }
    return handlers(url, objectType) ?? new HttpResponse(null, { status: 500 });
  });

describe('get_semantic_schema', () => {
  it('returns JSON schema per requested semantic type', async () => {
    mswServer.use(
      metastore((url, objectType) => {
        if (url.pathname.includes('/schema/')) {
          return HttpResponse.json({ objectType, schema: { type: 'object', title: objectType } });
        }
        return undefined;
      }),
    );
    const result = await callText(await connect(), 'get_semantic_schema', {
      semantic_types: ['semantic-dataset'],
    });
    expect(result.isError).toBeFalsy();
    expect(text(result)).toContain('semantic-dataset');
  });

  it('errors on empty semantic_types', async () => {
    const result = await callText(await connect(), 'get_semantic_schema', { semantic_types: [] });
    expect(result.isError).toBe(true);
    expect(text(result)).toContain('At least one semantic type');
  });
});

describe('get_semantic_context', () => {
  it('lists compact objects when ids are empty', async () => {
    mswServer.use(
      metastore((url, objectType) => {
        if (objectType === 'semantic-model' && url.pathname.endsWith('/semantic-model')) {
          // First page full (limit=20 -> return < limit to stop).
          return listResponse([
            {
              type: 'semantic-model',
              id: 'm1',
              attributes: { name: 'Sales Model', sql_dialect: 'snowflake' },
            },
          ]);
        }
        return undefined;
      }),
    );
    const result = await callText(await connect(), 'get_semantic_context', {
      semantic_objects: [{ object_type: 'semantic-model' }],
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    expect(out).toContain('Sales Model');
    expect(out).toContain('snowflake');
  });

  it('returns full attributes when ids are provided', async () => {
    mswServer.use(
      metastore((url, objectType) => {
        if (objectType === 'semantic-dataset' && url.pathname.endsWith('/d1')) {
          return objectResponse({
            type: 'semantic-dataset',
            id: 'd1',
            attributes: { name: 'Orders', tableId: 'in.c-x.orders', secretKey: 'keepme' },
          });
        }
        return undefined;
      }),
    );
    const result = await callText(await connect(), 'get_semantic_context', {
      semantic_objects: [{ object_type: 'semantic-dataset', ids: ['d1'] }],
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    // Full attributes view includes the raw attributes map.
    expect(out).toContain('keepme');
    expect(out).toContain('attributes');
  });

  it('errors on empty semantic_objects', async () => {
    const result = await callText(await connect(), 'get_semantic_context', {
      semantic_objects: [],
    });
    expect(result.isError).toBe(true);
  });
});

describe('search_semantic_context', () => {
  it('matches by attribute value and groups by model, with matched paths', async () => {
    mswServer.use(
      metastore((_url, objectType) => {
        if (objectType === 'semantic-dataset') {
          return listResponse([
            {
              type: 'semantic-dataset',
              id: 'd1',
              attributes: { name: 'Revenue Facts', tableId: 'in.c-x.rev', modelUUID: 'm1' },
            },
          ]);
        }
        // All other types empty.
        return listResponse([]);
      }),
    );
    const result = await callText(await connect(), 'search_semantic_context', {
      patterns: ['revenue'],
      semantic_types: ['semantic-dataset'],
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    expect(out).toContain('m1'); // grouped by semantic_model_id
    expect(out).toContain('Revenue Facts');
    expect(out).toContain('meta.name'); // matched on display name
  });

  it('errors when no usable patterns are provided', async () => {
    const result = await callText(await connect(), 'search_semantic_context', {
      patterns: ['   '],
    });
    expect(result.isError).toBe(true);
    expect(text(result)).toContain('At least one regex pattern');
  });

  it('errors on invalid regex', async () => {
    mswServer.use(metastore(() => listResponse([])));
    const result = await callText(await connect(), 'search_semantic_context', { patterns: ['('] });
    expect(result.isError).toBe(true);
    expect(text(result)).toContain('Invalid regex pattern');
  });
});

describe('validate_semantic_query', () => {
  // A model with a dataset (in.table), a metric SUM("AMOUNT") on that dataset, and a
  // post-query 'range' constraint scoped to the metric.
  const buildModelHandlers = () =>
    metastore((url, objectType) => {
      if (url.pathname.includes('/schema/')) return undefined;
      // get model by id
      if (objectType === 'semantic-model' && url.pathname.endsWith('/m1')) {
        return objectResponse({
          type: 'semantic-model',
          id: 'm1',
          attributes: { name: 'M1', sql_dialect: 'snowflake' },
        });
      }
      if (objectType === 'semantic-model') {
        return listResponse([
          {
            type: 'semantic-model',
            id: 'm1',
            attributes: { name: 'M1', sql_dialect: 'snowflake' },
          },
        ]);
      }
      if (objectType === 'semantic-dataset' && url.pathname.endsWith('/d1')) {
        return objectResponse({
          type: 'semantic-dataset',
          id: 'd1',
          attributes: {
            name: 'Orders',
            tableId: 'orders_tbl',
            fqn: 'DB.SCHEMA.ORDERS',
            modelUUID: 'm1',
          },
        });
      }
      if (objectType === 'semantic-dataset') {
        return listResponse([
          {
            type: 'semantic-dataset',
            id: 'd1',
            attributes: {
              name: 'Orders',
              tableId: 'orders_tbl',
              fqn: 'DB.SCHEMA.ORDERS',
              modelUUID: 'm1',
            },
          },
        ]);
      }
      if (objectType === 'semantic-metric') {
        return listResponse([
          {
            type: 'semantic-metric',
            id: 'mt1',
            attributes: {
              name: 'Total Amount',
              sql: 'SUM("AMOUNT")',
              dataset: 'orders_tbl',
              modelUUID: 'm1',
            },
          },
        ]);
      }
      if (objectType === 'semantic-relationship') return listResponse([]);
      if (objectType === 'semantic-constraint') {
        return listResponse([
          {
            type: 'semantic-constraint',
            id: 'c1',
            attributes: {
              name: 'Amount range check',
              constraintType: 'range',
              severity: 'warning',
              metrics: ['Total Amount'],
              modelUUID: 'm1',
            },
          },
        ]);
      }
      return undefined;
    });

  it('auto-detects dataset + metric and surfaces a post-execution check', async () => {
    mswServer.use(buildModelHandlers());
    const result = await callText(await connect(), 'validate_semantic_query', {
      sql_query: 'SELECT SUM("AMOUNT") FROM DB.SCHEMA.ORDERS',
      semantic_model_ids: ['m1'],
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    expect(out).toContain('validation_auto_detected');
    expect(out).toContain('orders_tbl'); // used dataset tableId
    expect(out).toContain('Total Amount'); // used metric
    expect(out).toContain('post_execution_checks');
    expect(out).toContain('Amount range check');
    expect(out).toContain('valid'); // valid true (warning severity)
  });

  it('compares expected objects and reports missing ones', async () => {
    mswServer.use(buildModelHandlers());
    const result = await callText(await connect(), 'validate_semantic_query', {
      sql_query: 'SELECT 1',
      semantic_model_ids: ['m1'],
      expected_semantic_objects: [{ object_type: 'semantic-dataset', ids: ['d1'] }],
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    // d1 was expected but not detected in `SELECT 1`.
    expect(out).toContain('missing_expected_objects');
    expect(out).toContain('validation_detected_from_expected');
  });

  it('errors on empty sql', async () => {
    const result = await callText(await connect(), 'validate_semantic_query', {
      sql_query: '   ',
      semantic_model_ids: ['m1'],
    });
    expect(result.isError).toBe(true);
    expect(text(result)).toContain('sql_query must not be empty');
  });

  it('errors when no model ids', async () => {
    const result = await callText(await connect(), 'validate_semantic_query', {
      sql_query: 'SELECT 1',
      semantic_model_ids: [],
    });
    expect(result.isError).toBe(true);
  });
});
