import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { createServer } from '@/server';
import {
  cleanBucketName,
  createTransformationConfiguration,
  joinSqlStatements,
  splitSqlStatements,
  updateParams,
  updateTransformationParameters,
} from '@/tools/components';
import {
  __testing,
  validateRootParametersConfiguration,
  validateRootStorageConfiguration,
} from '@/tools/validation';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

const callTool = async (name: string, args: Record<string, unknown>) => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverT);
  const client = new Client({ name: 't', version: '0' });
  await client.connect(clientT);
  const result = await client.callTool({ name, arguments: args });
  const text = (result.content as { text: string }[])[0]!.text;
  await client.close();
  return { text, isError: result.isError };
};

describe('get_config_examples', () => {
  it('renders root and row configuration examples as markdown', async () => {
    server.use(
      http.get('https://ai.test/*', ({ request }) => {
        expect(new URL(request.url).pathname).toBe('/docs/components/keboola.ex-aws-s3');
        return HttpResponse.json({
          rootConfigurationExamples: [{ foo: 'bar' }],
          rowConfigurationExamples: [{ baz: 1 }],
        });
      }),
    );

    const { text } = await callTool('get_config_examples', { component_id: 'keboola.ex-aws-s3' });
    expect(text).toContain('# Configuration Examples for `keboola.ex-aws-s3`');
    expect(text).toContain('## Root Configuration Examples');
    expect(text).toContain('"foo": "bar"');
    expect(text).toContain('## Row Configuration Examples');
  });

  it('returns an empty string when the component lookup fails', async () => {
    server.use(http.get('https://ai.test/*', () => new HttpResponse(null, { status: 404 })));
    const { text, isError } = await callTool('get_config_examples', { component_id: 'nope' });
    expect(isError).toBeFalsy();
    expect(text).toBe('');
  });
});

const verify = () =>
  http.get('https://connection.test/*', ({ request }) => {
    const { pathname } = new URL(request.url);
    if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
    return undefined;
  });

describe('get_components', () => {
  it('merges AI catalog metadata with Storage data and derives capabilities', async () => {
    server.use(
      verify(),
      http.get('https://ai.test/*', () =>
        HttpResponse.json({
          id: 'keboola.ex-aws-s3',
          name: 'AWS S3',
          type: 'extractor',
          flags: ['genericDockerUI-rows', 'genericDockerUI-tableOutput'],
          documentation: 'docs here',
        }),
      ),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.includes('/components/')) {
          return HttpResponse.json({ data: { synchronous_actions: ['testConnection'] } });
        }
        return undefined;
      }),
    );

    const { text } = await callTool('get_components', { component_ids: ['keboola.ex-aws-s3'] });
    expect(text).toContain('AWS S3');
    expect(text).toContain('is_row_based: true');
    expect(text).toContain('testConnection');
    expect(text).toContain('docs here');
  });

  it('falls back to the Storage API when the AI catalog returns 404', async () => {
    server.use(
      verify(),
      http.get('https://ai.test/*', () => new HttpResponse(null, { status: 404 })),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        return HttpResponse.json({ id: 'priv.comp', name: 'Private', type: 'writer', flags: [] });
      }),
    );

    const { text } = await callTool('get_components', { component_ids: ['priv.comp'] });
    expect(text).toContain('Private');
  });
});

describe('get_configs', () => {
  it('lists configs by component id grouped under the component', async () => {
    server.use(
      verify(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.endsWith('/components/keboola.ex-db-mysql/configs')) {
          return HttpResponse.json([{ id: '100', name: 'My DB', isDisabled: false }]);
        }
        if (pathname.endsWith('/components/keboola.ex-db-mysql')) {
          return HttpResponse.json({
            id: 'keboola.ex-db-mysql',
            name: 'MySQL',
            type: 'extractor',
            flags: [],
          });
        }
        return undefined;
      }),
    );

    const { text } = await callTool('get_configs', { component_ids: ['keboola.ex-db-mysql'] });
    expect(text).toContain('My DB');
    expect(text).toContain('MySQL');
  });

  it('returns full details with redacted secrets for specific configs', async () => {
    server.use(
      verify(),
      http.get('https://ai.test/*', () => new HttpResponse(null, { status: 404 })),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.endsWith('/configs/100')) {
          return HttpResponse.json({
            id: '100',
            name: 'My DB',
            version: 3,
            configuration: { parameters: { host: 'db', '#password': 'plaintext' } },
          });
        }
        if (pathname.endsWith('/components/keboola.ex-db-mysql')) {
          return HttpResponse.json({
            id: 'keboola.ex-db-mysql',
            name: 'MySQL',
            type: 'extractor',
            flags: [],
          });
        }
        return undefined;
      }),
    );

    const { text } = await callTool('get_configs', {
      configs: [{ component_id: 'keboola.ex-db-mysql', configuration_id: '100' }],
    });
    expect(text).toContain('[REDACTED]');
    expect(text).not.toContain('plaintext');
    expect(text).toContain('host');
  });
});

describe('run_sync_action', () => {
  it('merges row config over root and posts to the sync-actions endpoint', async () => {
    let actionBody: { configData?: Record<string, unknown>; action?: string } | undefined;
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/configs/cfg/rows/r1')) {
          return HttpResponse.json({
            configuration: { parameters: { row: 1 }, storage: { input: 'r' } },
          });
        }
        if (pathname.endsWith('/configs/cfg')) {
          return HttpResponse.json({
            configuration: {
              parameters: { base: 1 },
              storage: {},
              authorization: { oauth_api: { id: 'a' } },
            },
          });
        }
        return undefined;
      }),
      http.post('https://sync-actions.test/*', async ({ request }) => {
        expect(new URL(request.url).pathname).toBe('/actions');
        actionBody = (await request.json()) as {
          configData?: Record<string, unknown>;
          action?: string;
        };
        return HttpResponse.json({ status: 'success', tables: [] });
      }),
    );

    const { text } = await callTool('run_sync_action', {
      action_name: 'getTables',
      component_id: 'keboola.ex-db-mysql',
      configuration_id: 'cfg',
      configuration_row_id: 'r1',
    });

    expect(actionBody?.action).toBe('getTables');
    // row parameters merged on top of root; authorization carried from root.
    expect(actionBody?.configData).toMatchObject({
      parameters: { base: 1, row: 1 },
      authorization: { oauth_api: { id: 'a' } },
    });
    expect(text).toContain('success');
  });
});

// ---------------------------------------------------------------------------
// WRITE TOOLS
// ---------------------------------------------------------------------------

/** AI catalog 404 so fetchComponent falls back to Storage component_detail. */
const aiNotFound = () =>
  http.get('https://ai.test/*', () => new HttpResponse(null, { status: 404 }));

describe('create_config', () => {
  it('validates, creates the config, and stamps creation metadata', async () => {
    let createBody: Record<string, unknown> | undefined;
    let metadataBody: Record<string, unknown> | undefined;
    server.use(
      verify(),
      aiNotFound(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.endsWith('/components/keboola.ex-generic')) {
          return HttpResponse.json({
            id: 'keboola.ex-generic',
            name: 'Generic',
            type: 'extractor',
            flags: [],
            configurationSchema: {
              type: 'object',
              required: ['host'],
              properties: { host: { type: 'string' } },
            },
          });
        }
        return undefined;
      }),
      http.post('https://connection.test/*', async ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/configs')) {
          createBody = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({ id: '555', version: 1 });
        }
        if (pathname.endsWith('/metadata')) {
          metadataBody = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json([]);
        }
        return undefined;
      }),
    );

    const { text, isError } = await callTool('create_config', {
      name: 'My config',
      description: 'desc',
      component_id: 'keboola.ex-generic',
      parameters: { host: 'db.example.com' },
    });

    expect(isError).toBeFalsy();
    expect(createBody?.name).toBe('My config');
    expect((createBody?.configuration as Record<string, unknown>).parameters).toMatchObject({
      host: 'db.example.com',
    });
    // creation metadata is KBC.MCP.createdBy
    expect(JSON.stringify(metadataBody)).toContain('KBC.MCP.createdBy');
    expect(text).toContain('555');
    expect(text).toContain('version: 1');
  });

  it('fails a schema-required violation with a recoverable hint', async () => {
    server.use(
      verify(),
      aiNotFound(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        return HttpResponse.json({
          id: 'keboola.ex-generic',
          name: 'Generic',
          type: 'extractor',
          flags: [],
          configurationSchema: { type: 'object', required: ['host'], properties: {} },
        });
      }),
    );

    const { text, isError } = await callTool('create_config', {
      name: 'X',
      description: 'd',
      component_id: 'keboola.ex-generic',
      parameters: {},
    });

    expect(isError).toBe(true);
    expect(text).toContain('required property');
    expect(text).toContain('HINT: Ensure ALL of the following required fields');
  });

  it('refuses suitable-only components (SQL transformation)', async () => {
    const { text, isError } = await callTool('create_config', {
      name: 'X',
      description: 'd',
      component_id: 'keboola.snowflake-transformation',
      parameters: {},
    });
    expect(isError).toBe(true);
    expect(text).toContain('cannot be used with keboola.snowflake-transformation');
  });
});

describe('update_config', () => {
  it('applies a parameter diff over the existing config and bumps metadata', async () => {
    let putBody: Record<string, unknown> | undefined;
    server.use(
      verify(),
      aiNotFound(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.endsWith('/configs/100')) {
          return HttpResponse.json({
            id: '100',
            name: 'Cfg',
            version: 3,
            configuration: { parameters: { host: 'old', port: 5432 }, storage: {} },
          });
        }
        if (pathname.endsWith('/components/keboola.ex-db-mysql')) {
          return HttpResponse.json({
            id: 'keboola.ex-db-mysql',
            name: 'MySQL',
            type: 'extractor',
            flags: [],
          });
        }
        return undefined;
      }),
      http.put('https://connection.test/*', async ({ request }) => {
        putBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({ id: '100', name: 'Cfg', version: 4, description: 'd' });
      }),
      http.post('https://connection.test/*', () => HttpResponse.json([])),
    );

    const { text, isError } = await callTool('update_config', {
      change_description: 'switch host',
      component_id: 'keboola.ex-db-mysql',
      configuration_id: '100',
      parameter_updates: [{ op: 'set', path: 'host', value: 'new' }],
    });

    expect(isError).toBeFalsy();
    const cfg = putBody?.configuration as Record<string, unknown>;
    expect(cfg.parameters).toMatchObject({ host: 'new', port: 5432 }); // diff preserved port
    expect(putBody?.changeDescription).toBe('switch host');
    expect(text).toContain('version: 4');
  });
});

describe('update_config_row', () => {
  it('updates a row with str_replace diff and is_disabled', async () => {
    let putBody: Record<string, unknown> | undefined;
    server.use(
      verify(),
      aiNotFound(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.endsWith('/rows/r1')) {
          return HttpResponse.json({
            id: 'r1',
            configuration: { parameters: { table: 'old_name' } },
          });
        }
        if (pathname.endsWith('/components/keboola.ex-db-mysql')) {
          return HttpResponse.json({
            id: 'keboola.ex-db-mysql',
            name: 'M',
            type: 'extractor',
            flags: [],
          });
        }
        return undefined;
      }),
      http.put('https://connection.test/*', async ({ request }) => {
        putBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({ id: 'r1', version: 2 });
      }),
      http.post('https://connection.test/*', () => HttpResponse.json([])),
    );

    const { isError } = await callTool('update_config_row', {
      change_description: 'rename table',
      component_id: 'keboola.ex-db-mysql',
      configuration_id: '100',
      configuration_row_id: 'r1',
      parameter_updates: [
        { op: 'str_replace', path: 'table', search_for: 'old', replace_with: 'new' },
      ],
      is_disabled: true,
    });

    expect(isError).toBeFalsy();
    const cfg = putBody?.configuration as Record<string, unknown>;
    expect(cfg.parameters).toMatchObject({ table: 'new_name' });
    expect(putBody?.isDisabled).toBe(true);
  });
});

describe('create_sql_transformation', () => {
  it('resolves dialect from token defaultBackend and builds the transformation payload', async () => {
    let createBody: Record<string, unknown> | undefined;
    let createPath = '';
    server.use(
      aiNotFound(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) {
          return HttpResponse.json({ owner: { id: '42', defaultBackend: 'snowflake' } });
        }
        if (pathname.endsWith('/workspaces')) return HttpResponse.json([]);
        // search/component-configurations folder lookup
        if (pathname.includes('/search/component-configurations')) return HttpResponse.json([]);
        if (pathname.endsWith('/configs')) return HttpResponse.json([]); // configuration_list for folder count
        if (pathname.includes('/components/keboola.snowflake-transformation')) {
          return HttpResponse.json({
            id: 'keboola.snowflake-transformation',
            name: 'Snowflake',
            type: 'transformation',
            flags: [],
          });
        }
        return undefined;
      }),
      http.post('https://connection.test/*', async ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/configs')) {
          createPath = pathname;
          createBody = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({ id: 'tf1', version: 1 });
        }
        if (pathname.endsWith('/metadata')) return HttpResponse.json([]);
        return undefined;
      }),
    );

    const { text, isError } = await callTool('create_sql_transformation', {
      name: 'My TF',
      description: 'transform stuff',
      sql_code_blocks: [{ name: 'step', script: 'CREATE TABLE out AS SELECT 1;' }],
      created_table_names: ['out'],
    });

    expect(isError).toBeFalsy();
    expect(createPath).toContain('keboola.snowflake-transformation');
    const cfg = createBody?.configuration as Record<string, unknown>;
    const storage = cfg.storage as { output: { tables: { destination: string }[] } };
    expect(storage.output.tables[0]!.destination).toContain('out.c-');
    expect(text).toContain('tf1');
  });
});

describe('update_sql_transformation', () => {
  it('returns a Python/R hint when the config is missing (404)', async () => {
    server.use(
      aiNotFound(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) {
          return HttpResponse.json({ owner: { id: '42', defaultBackend: 'snowflake' } });
        }
        if (pathname.endsWith('/workspaces')) return HttpResponse.json([]);
        if (pathname.includes('/configs/nope')) return new HttpResponse(null, { status: 404 });
        return undefined;
      }),
    );

    const { text, isError } = await callTool('update_sql_transformation', {
      change_description: 'x',
      configuration_id: 'nope',
    });

    expect(isError).toBe(true);
    expect(text).toContain("use 'update_config'");
    expect(text).toContain('keboola.python-transformation-v2');
  });
});

// ---------------------------------------------------------------------------
// VALIDATION + MODEL UNIT TESTS
// ---------------------------------------------------------------------------

describe('validation: sanitizeSchema', () => {
  it.each([
    [{ type: 'object', required: true }, { type: 'object' }],
    [{ type: 'object', required: false }, { type: 'object' }],
    [
      { type: 'object', required: ['foo', 'bar'] },
      { type: 'object', required: ['foo', 'bar'] },
    ],
    [{ type: 'string', enum: [] }, { type: 'string' }],
  ])('normalizes required/enum %#', (input, expected) => {
    expect(__testing.sanitizeSchema(input as Record<string, unknown>)).toEqual(expected);
  });

  it('propagates a boolean child-required flag up to the parent', () => {
    const out = __testing.sanitizeSchema({
      type: 'object',
      properties: { foo: { type: 'string', required: true } },
    });
    expect(out).toEqual({
      type: 'object',
      required: ['foo'],
      properties: { foo: { type: 'string' } },
    });
  });

  it('converts an empty-list properties to an empty dict', () => {
    const out = __testing.sanitizeSchema({ type: 'object', properties: [] });
    expect(out.properties).toEqual({});
  });
});

describe('validation: parameters & storage', () => {
  const component = {
    component_id: 'keboola.ex-generic',
    component_type: 'extractor',
    capabilities: { is_row_based: false },
    configuration_schema: {
      type: 'object',
      required: ['host'],
      properties: { host: { type: 'string' }, port: { type: 'integer' } },
    },
  };

  it('accepts valid parameters', () => {
    expect(() =>
      validateRootParametersConfiguration({ host: 'h', port: 1 }, component),
    ).not.toThrow();
  });

  it('rejects wrong type', () => {
    expect(() =>
      validateRootParametersConfiguration({ host: 'h', port: 'nope' }, component),
    ).toThrow(/not of type/);
  });

  it('skips validation when the component has no schema', () => {
    const noSchema = { ...component, configuration_schema: null };
    expect(() => validateRootParametersConfiguration({ anything: true }, noSchema)).not.toThrow();
  });

  it('requires writer root storage to contain input mappings', () => {
    const writer = {
      component_id: 'keboola.wr-x',
      component_type: 'writer',
      capabilities: { is_row_based: false },
      configuration_schema: null,
    };
    expect(() => validateRootStorageConfiguration({}, writer)).toThrow(/must contain "input"/);
  });
});

describe('model: SQL + param utils', () => {
  it('splits and joins SQL statements', () => {
    const stmts = splitSqlStatements('SELECT 1; SELECT 2;');
    expect(stmts).toHaveLength(2);
    expect(joinSqlStatements(stmts)).toContain('SELECT 1');
  });

  it('updateParams applies a diff without mutating the input', () => {
    const original = { a: 1, b: { c: 2 } };
    const out = updateParams(original, [{ op: 'set', path: 'b.c', value: 9 }]);
    expect(out).toMatchObject({ a: 1, b: { c: 9 } });
    expect(original.b.c).toBe(2);
  });

  it('updateParams str_replace and remove', () => {
    const out = updateParams({ name: 'old_table', drop: 1 }, [
      { op: 'str_replace', path: 'name', search_for: 'old', replace_with: 'new' },
      { op: 'remove', path: 'drop' },
    ]);
    expect(out).toEqual({ name: 'new_table' });
  });

  it('updateTransformationParameters renames a code and summarizes structure on structural change', () => {
    const [updated, summary] = updateTransformationParameters(
      { blocks: [{ name: 'B', codes: [{ name: 'c', script: 'SELECT 1' }] }] },
      [
        {
          op: 'add_code',
          block_id: 'b0',
          code: { name: 'c2', script: 'SELECT 2' },
          position: 'end',
        },
      ],
    );
    expect(updated.blocks[0]!.codes).toHaveLength(2);
    expect(summary).toContain('Updated Transformation Structure');
  });

  it('cleanBucketName folds diacritics and strips invalid chars', () => {
    expect(cleanBucketName('Český Bucket!')).toBe('Cesky-Bucket');
  });

  it('createTransformationConfiguration builds output table destinations', () => {
    const cfg = createTransformationConfiguration(
      [{ name: 's', script: 'CREATE TABLE t AS SELECT 1;' }],
      'My TF',
      ['t'],
    ) as { storage: { output: { tables: { source: string; destination: string }[] } } };
    expect(cfg.storage.output.tables[0]).toMatchObject({
      source: 't',
      destination: 'out.c-My-TF.t',
    });
  });
});
