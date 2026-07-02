import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { createServer } from '@/server';

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

const connect = async () => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client;
};

describe('prompts', () => {
  it('registers the six one-click Keboola prompts', async () => {
    const client = await connect();
    const { prompts } = await client.listPrompts();
    const names = prompts.map((p) => p.name).sort();
    expect(names).toEqual(
      [
        'analyze_project_structure',
        'component_usage_summary',
        'create_project_documentation',
        'data_quality_assessment',
        'error_analysis_report',
        'project_health_check',
      ].sort(),
    );
    await client.close();
  });

  it('returns a single user message for a prompt', async () => {
    const client = await connect();
    const result = await client.getPrompt({ name: 'project_health_check' });
    expect(result.messages).toHaveLength(1);
    expect(result.messages[0]!.role).toBe('user');
    expect((result.messages[0]!.content as { text: string }).text).toContain('health check');
    await client.close();
  });
});
