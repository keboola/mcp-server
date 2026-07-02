import { serve } from '@hono/node-server';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { createHttpApp } from '@/transports/http';

let httpServer: ReturnType<typeof serve>;
let port: number;

const connect = async (headers?: Record<string, string>) => {
  const url = new URL(`http://localhost:${port}/mcp`);
  const client = new Client({ name: 'test-client', version: '0.0.0' });
  await client.connect(new StreamableHTTPClientTransport(url, { requestInit: { headers } }));
  return client;
};

// Returns the TOON-encoded tool output text.
const callServerInfo = async (client: Awaited<ReturnType<typeof connect>>) => {
  const result = await client.callTool({ name: 'get_server_info', arguments: {} });
  const content = result.content as { type: string; text: string }[];
  return content[0]!.text;
};

describe('streamable-http transport', () => {
  beforeAll(async () => {
    const app = createHttpApp(new Config({ branchId: '999' }));
    port = await new Promise<number>((resolve) => {
      httpServer = serve({ fetch: app.fetch, port: 0 }, (info: AddressInfo) => resolve(info.port));
    });
  });

  afterAll(() => {
    httpServer.close();
  });

  it('serves a health check', async () => {
    const res = await fetch(`http://localhost:${port}/health-check`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: 'ok' });
  });

  it('lists tools over the MCP endpoint', async () => {
    const client = await connect();
    const { tools } = await client.listTools();
    expect(tools.map((tool) => tool.name)).toContain('get_server_info');
    await client.close();
  });

  it('uses the base config when no headers override it', async () => {
    const client = await connect();
    expect(await callServerInfo(client)).toContain('branchId: "999"');
    await client.close();
  });

  it('layers X- headers over the base config per request', async () => {
    const client = await connect({ 'X-Branch-Id': '123', 'X-StorageApiToken': 'tok' });
    expect(await callServerInfo(client)).toContain('branchId: "123"');
    await client.close();
  });
});
