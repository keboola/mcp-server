import { type HttpBindings, serve } from '@hono/node-server';
import { RESPONSE_ALREADY_SENT } from '@hono/node-server/utils/response';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { Hono } from 'hono';

import type { Config } from '@/config';
import { logger } from '@/logger';
import { createServer, SERVER_NAME, SERVER_VERSION } from '@/server';

type Bindings = HttpBindings;

/**
 * Builds the per-request Config: the base (env/CLI) config with HTTP headers
 * layered on top. `X-*` headers map onto Config fields by name; the OAuth bearer
 * token is taken from the standard `Authorization: Bearer <token>` header.
 */
const configFromHeaders = (base: Config, headers: Headers): Config => {
  const map: Record<string, string | undefined> = {};
  headers.forEach((value, key) => {
    map[key] = value;
  });

  const auth = headers.get('authorization');
  const bearer = auth?.match(/^Bearer\s+(.+)$/i)?.[1];
  if (bearer) {
    map['bearerToken'] = bearer;
  }

  return base.replaceBy(map);
};

export const createHttpApp = (baseConfig: Config): Hono<{ Bindings: Bindings }> => {
  const app = new Hono<{ Bindings: Bindings }>();

  app.get('/health-check', (c) => c.json({ status: 'ok' }));
  app.get('/', (c) => c.json({ name: SERVER_NAME, version: SERVER_VERSION }));

  // Stateless streamable-HTTP: each POST gets a fresh server + transport, mirroring
  // the Python `stateless_http=True` setup. No session is retained between requests.
  app.post('/mcp', async (c) => {
    const config = configFromHeaders(baseConfig, c.req.raw.headers);
    const body: unknown = await c.req.json().catch(() => undefined);

    const server = createServer(config);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    await server.connect(transport);

    const { incoming, outgoing } = c.env;
    outgoing.on('close', () => {
      void transport.close();
      void server.close();
    });

    await transport.handleRequest(incoming, outgoing, body);
    return RESPONSE_ALREADY_SENT;
  });

  // Stateless mode has no sessions, so GET (SSE stream) and DELETE (session end)
  // are not supported — match the SDK's stateless contract with 405.
  const notAllowed = {
    jsonrpc: '2.0',
    error: { code: -32000, message: 'Method not allowed.' },
    id: null,
  };
  app.get('/mcp', (c) => c.json(notAllowed, 405));
  app.delete('/mcp', (c) => c.json(notAllowed, 405));

  return app;
};

export const startHttp = (config: Config, host: string, port: number): ReturnType<typeof serve> => {
  const app = createHttpApp(config);
  const server = serve({ fetch: app.fetch, hostname: host, port });
  logger.info(`Starting MCP server with Streamable-HTTP transport on http://${host}:${port}/`);
  return server;
};
