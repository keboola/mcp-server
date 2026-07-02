import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { formatValidationErrors, prettifyValidationError } from '@/mcp/errors';
import { registerTool } from '@/mcp/tool';

/**
 * Port of `tests/test_errors.py`.
 *
 * The Python `errors` module bundled three concerns: (1) Pydantic validation-error
 * prettifying, (2) the `tool_errors` decorator (recovery hints + logging + SAPI event
 * telemetry), and (3) a FastMCP `ValidationErrorMiddleware`.
 *
 * In the TypeScript port those concerns are split: the recovery-hint / error-result /
 * logging behavior lives in `@/mcp/tool` (`registerTool`), so it is tested here against
 * that surface; the SAPI-event telemetry and FastMCP middleware have no TS equivalent
 * (different runtime) and are not ported. The validation-error formatting helpers were
 * ported to `@/mcp/errors` (adapted from Pydantic's `ValidationError` to Zod's
 * `ZodError`) and are tested directly.
 */

const zodError = (shape: z.ZodRawShape, input: unknown): z.ZodError => {
  const result = z.object(shape).safeParse(input);
  if (result.success) throw new Error('expected a validation failure');
  return result.error;
};

describe('formatValidationErrors', () => {
  it('extracts field, message and extra (code) from each issue', () => {
    const err = zodError({ sql_query: z.string(), query_name: z.string() }, { foo: 'bar' });
    const formatted = formatValidationErrors(err.issues);

    expect(formatted.errors).toHaveLength(2);
    const fields = formatted.errors.map((e) => e.field).sort();
    expect(fields).toEqual(['query_name', 'sql_query']);
    for (const e of formatted.errors) {
      expect(e.message).toBeTruthy();
      // The Zod issue `code` is carried through under `extra`, mirroring Pydantic's `type`.
      expect(e.extra.code).toBe('invalid_type');
    }
  });

  it('joins nested paths with dots', () => {
    const err = zodError({ outer: z.object({ inner: z.string() }) }, { outer: { inner: 1 } });
    const formatted = formatValidationErrors(err.issues);
    expect(formatted.errors[0]!.field).toBe('outer.inner');
  });
});

describe('prettifyValidationError', () => {
  it('renders the count header with the model name', () => {
    const err = zodError({ a: z.string(), b: z.number() }, {});
    const text = prettifyValidationError(err, 'MyTool');
    const lines = text.split('\n');
    expect(lines[0]).toBe('Found 2 validation error(s) for MyTool');
    // Field locations are surfaced explicitly in the body.
    expect(text).toContain('field: a');
    expect(text).toContain('field: b');
  });

  it('defaults the model name to "unknown"', () => {
    const err = zodError({ a: z.string() }, {});
    expect(prettifyValidationError(err).split('\n')[0]).toBe(
      'Found 1 validation error(s) for unknown',
    );
  });
});

// --- registerTool error path (the TS home of Python's `tool_errors` recovery/logging) ---

const callTool = async (
  def: Parameters<typeof registerTool>[1],
  args: Record<string, unknown> = {},
) => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  const mcp = new McpServer({ name: 'test', version: '0.0.0' });
  registerTool(mcp, def);
  await mcp.connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client.callTool({ name: def.name, arguments: args });
};

describe('registerTool error handling', () => {
  it('returns an error result carrying the exception message', async () => {
    const result = await callTool({
      name: 'boom',
      description: 'd',
      handler: () => {
        throw new Error('Simulated failure');
      },
    });
    expect(result.isError).toBe(true);
    expect((result.content as { text: string }[])[0]!.text).toBe('Simulated failure');
  });

  it('appends the recovery hint when one is configured', async () => {
    const result = await callTool({
      name: 'boom_recover',
      description: 'd',
      recovery: 'Check that data has valid types.',
      handler: () => {
        throw new Error('Simulated failure');
      },
    });
    expect(result.isError).toBe(true);
    const text = (result.content as { text: string }[])[0]!.text;
    expect(text).toContain('Simulated failure');
    expect(text).toContain('Recovery: Check that data has valid types.');
  });

  it('omits the Recovery line when no hint is configured', async () => {
    const result = await callTool({
      name: 'boom_plain',
      description: 'd',
      handler: () => {
        throw new Error('Simulated failure');
      },
    });
    expect((result.content as { text: string }[])[0]!.text).not.toContain('Recovery:');
  });
});
