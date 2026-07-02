import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { createServer, SERVER_NAME } from '@/server';

const connect = async (config: Config) => {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const server = createServer(config);
  await server.connect(serverTransport);

  const client = new Client({ name: 'test-client', version: '0.0.0' });
  await client.connect(clientTransport);
  return client;
};

describe('MCP server', () => {
  it('lists registered tools over an in-memory transport', async () => {
    const client = await connect(new Config());
    const { tools } = await client.listTools();

    const names = tools.map((tool) => tool.name);
    expect(names).toContain('get_server_info');
    // Every tool must expose an input schema (drives the tool-filtering / docs).
    expect(tools.every((tool) => tool.inputSchema?.type === 'object')).toBe(true);
  });

  it('runs a tool and returns the server name', async () => {
    const client = await connect(new Config({ branchId: '123' }));
    const result = await client.callTool({ name: 'get_server_info', arguments: {} });

    // Output is TOON, not JSON: a flat object renders as `key: value` lines.
    const content = result.content as { type: string; text: string }[];
    const text = content[0]!.text;
    expect(text).toContain(`name: ${SERVER_NAME}`);
    expect(text).toContain('branchId: "123"');
  });
});
