import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';

import type { Config } from '@/config';
import { createServer } from '@/server';

// Shared integration-test harness: connect a real MCP server (built from a leased project's
// Config) over an in-memory transport and call its tools — the same shape as the unit tests
// (__tests__/*), but hitting the real Keboola stack instead of msw mocks.

export type McpSession = {
  client: Client;
  close: () => Promise<void>;
};

export const connectMcp = async (config: Config): Promise<McpSession> => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  const server = createServer(config);
  await server.connect(serverT);
  const client = new Client({ name: 'kbc-mcp-integtests', version: '0.0.0' });
  await client.connect(clientT);
  return {
    client,
    close: async () => {
      await client.close();
      await server.close();
    },
  };
};

/** Calls a tool and returns its text content, asserting the call did not error. */
export const callToolText = async (
  client: Client,
  name: string,
  args: Record<string, unknown> = {},
): Promise<string> => {
  const result = await client.callTool({ name, arguments: args });
  if (result.isError) {
    const text = (result.content as { text?: string }[])[0]?.text ?? '';
    throw new Error(`Tool "${name}" returned an error: ${text}`);
  }
  return (result.content as { text: string }[])[0]!.text;
};

/** Calls a tool and returns the raw CallToolResult (for negative-path / isError assertions). */
export const callToolRaw = (client: Client, name: string, args: Record<string, unknown> = {}) =>
  client.callTool({ name, arguments: args });
