import { describe, expect, it } from 'vitest';

import { callToolRaw, connectMcp } from './helpers/mcp';
import { getTestProjectForTest } from './testproject/fixture';

// Ported from integtests/test_errors.py.
//
// The Python tests call the in-process tool functions directly and assert on raised
// exceptions / typed outputs. Here we exercise the same error paths end-to-end through
// the MCP client: tools that swallow "not found" into a structured output return text
// (no isError), while tools that propagate an upstream HTTP/SQL error surface it as a
// CallToolResult with `isError: true` and the formatted message in the text content.

const errorText = (result: unknown): string =>
  (result as { content: { text: string }[] }).content[0]!.text;

describe('error handling (integration)', () => {
  // test_storage_api_404_error_maintains_standard_behavior: a non-existent bucket is
  // reported via the structured `buckets_not_found` field, not as a tool error.
  it('get_buckets reports a missing bucket without erroring', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const result = await callToolRaw(session.client, 'get_buckets', {
        bucket_ids: ['non.existent.bucket'],
      });
      expect(result.isError).toBeFalsy();
      expect(errorText(result)).toContain('non.existent.bucket');
    } finally {
      await session.close();
    }
  });

  // test_concurrent_error_handling: many concurrent not-found lookups are each handled
  // consistently (no error, each reported in buckets_not_found).
  it('handles concurrent missing-bucket lookups consistently', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const results = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          callToolRaw(session.client, 'get_buckets', {
            bucket_ids: [`non.existent.bucket.${i}`],
          }),
        ),
      );
      for (let i = 0; i < results.length; i++) {
        const result = results[i]!;
        expect(result.isError).toBeFalsy();
        expect(errorText(result)).toContain(`non.existent.bucket.${i}`);
      }
    } finally {
      await session.close();
    }
  });

  // test_jobs_api_404_error_: requesting a non-existent job id propagates the upstream
  // 404 as a tool error mentioning the job id.
  it('get_jobs surfaces a 404 for a non-existent job id as a tool error', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const result = await callToolRaw(session.client, 'get_jobs', { job_ids: ['999999999'] });
      expect(result.isError).toBeTruthy();
      // The TS queue client surfaces the upstream 404 as a generic "Not Found" message
      // (it does not echo the job id / URL the Python HTTPStatusError carried), so we
      // assert only that the not-found error reached the tool layer.
      expect(errorText(result)).toMatch(/404|not found/i);
    } finally {
      await session.close();
    }
  });

  // NOTE: the Python test_docs_api_empty_query_error (a 422 from the AI docs service on an
  // empty query) was dropped: docs_query is now served by the pgvector docs-search index
  // (RFC: feature_spec/docs-search-pgvector/), where an empty query returns no results
  // rather than erroring. The docs happy-path + index gating are covered by
  // integtests/tools/doc.test.ts.

  // test_sql_api_invalid_query_error_(snowflake|bigquery): an invalid SQL query is
  // surfaced as a tool error with the "Failed to run SQL query" prefix, regardless of
  // the backend dialect.
  it('query_data surfaces an invalid SQL query as a tool error', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const result = await callToolRaw(session.client, 'query_data', {
        sql_query: 'INVALID SQL SYNTAX HERE',
        query_name: 'Invalid SQL query.',
      });
      expect(result.isError).toBeTruthy();
      expect(errorText(result)).toMatch(/Failed to run SQL query/i);
    } finally {
      await session.close();
    }
  });

  // Bad tool input (schema violation) is rejected by the server before the handler runs
  // and is reported as a tool error — the structured-error end-to-end contract.
  it('rejects a tool call with invalid input as a structured error', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      // get_jobs.limit is an int in [1, 500]; an out-of-range value must be rejected.
      const result = await callToolRaw(session.client, 'get_jobs', { limit: 99999 });
      expect(result.isError).toBeTruthy();
    } finally {
      await session.close();
    }
  });

  // test_event_emitted / TestStorageEvents: the Python suite verifies that a SAPI Storage
  // event is emitted (and its mcpServerContext payload) for every tool call. That is an
  // internal telemetry concern asserted by polling the SAPI events endpoint, not a
  // tool-output contract reachable through the MCP client, so it is not ported here.
  it.skip('emits a SAPI storage event per tool call (telemetry; not a tool-layer concern)', () => {
    // Intentionally skipped — see comment above. Would require polling client.storage
    // events and is out of scope for the in-memory server/middleware-level suite.
  });
});
