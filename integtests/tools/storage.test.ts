import { describe, expect, it } from 'vitest';

import { callToolRaw, callToolText, connectMcp } from '../helpers/mcp';
import { seedProject } from '../helpers/seed';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_storage.py. Each test leases a fresh project, resets it,
// seeds the standard fixtures (2 input buckets, 1 CSV table with columns id/name/item_count,
// 2 component configs), then exercises the storage tools.
//
// The MCP server returns tool output as TOON text (token-oriented notation), so the assertions
// match substrings / regex / the TOON tabular header rather than reconstructing pydantic models
// the way the Python tests did against the in-process tool functions.
const SEEDED_BUCKET_IDS = ['in.c-test_bucket_01', 'in.c-test_bucket_02'];
const SEEDED_TABLE_ID = 'in.c-test_bucket_01.test_table_01';
const SEEDED_COLUMNS = ['id', 'name', 'item_count'];

describe('storage tools (integration)', () => {
  // --- get_buckets ---------------------------------------------------------

  it('get_buckets lists the seeded buckets', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_buckets');
      for (const id of SEEDED_BUCKET_IDS) expect(text).toContain(id);
    } finally {
      await session.close();
    }
  });

  it('get_buckets reports bucket counts by stage (port of test_get_buckets)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_buckets');
      // bucket_counts: both seeded buckets are stage 'in' → total=2, input=2, output=0.
      // TOON renders the nested object as indented `key: value` lines.
      expect(text).toMatch(/total_buckets:\s*2/);
      expect(text).toMatch(/input_buckets:\s*2/);
      expect(text).toMatch(/output_buckets:\s*0/);
      // Every listed bucket carries an explicit stage column.
      expect(text).toContain('stage');
    } finally {
      await session.close();
    }
  });

  it('get_buckets returns full detail for specific bucket ids (port of test_get_bucket)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      for (const bucketId of SEEDED_BUCKET_IDS) {
        const text = await callToolText(session.client, 'get_buckets', { bucket_ids: [bucketId] });
        expect(text).toContain(bucketId);
      }
    } finally {
      await session.close();
    }
  });

  it('get_buckets emits TOON tabular output (port of test_get_buckets_output_format)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_buckets');
      // Two buckets are presented in TOON's list-of-objects tabular form:
      //   buckets[2]{id,name,displayName,stage,...}:
      expect(text).toMatch(/^buckets\[2\]\{[^}]*\bid\b[^}]*\}:/m);
    } finally {
      await session.close();
    }
  });

  // --- get_tables ----------------------------------------------------------

  it('get_tables lists a bucket and returns table detail with an FQN', async () => {
    // FQN + warehouse-native types are resolved via a Snowflake workspace; pin the backend
    // (BigQuery leases expose no fully_qualified_name).
    const project = await getTestProjectForTest({ backend: 'snowflake' });
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const listed = await callToolText(session.client, 'get_tables', {
        bucket_ids: ['in.c-test_bucket_01'],
      });
      expect(listed).toContain(SEEDED_TABLE_ID);

      const detail = await callToolText(session.client, 'get_tables', {
        table_ids: [SEEDED_TABLE_ID],
      });
      expect(detail).toMatch(/fully_qualified_name|fullyQualifiedName/);
      expect(detail).toContain('item_count');
    } finally {
      await session.close();
    }
  });

  it('get_tables detail returns the seeded columns with types (port of test_get_table)', async () => {
    // database_native_type is resolved via the warehouse — pin Snowflake.
    const project = await getTestProjectForTest({ backend: 'snowflake' });
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const detail = await callToolText(session.client, 'get_tables', {
        table_ids: [SEEDED_TABLE_ID],
      });
      expect(detail).toContain(SEEDED_TABLE_ID);
      expect(detail).toContain('test_table_01');
      // Every CSV column must appear in the detail's column listing.
      for (const col of SEEDED_COLUMNS) expect(detail).toContain(col);
      // Detail carries database-native type info per column.
      expect(detail).toContain('database_native_type');
    } finally {
      await session.close();
    }
  });

  it('get_tables listing returns summaries without FQN/columns (port of test_get_tables)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      // Bucket with the seeded table: exactly one table, summary shape.
      const withTable = await callToolText(session.client, 'get_tables', {
        bucket_ids: ['in.c-test_bucket_01'],
      });
      expect(withTable).toContain(SEEDED_TABLE_ID);
      // Summaries never resolve the warehouse FQN nor expand columns — those fields must be
      // absent from the output entirely (not emitted as a misleading null).
      expect(withTable).not.toMatch(/fullyQualifiedName|fully_qualified_name/);
      expect(withTable).not.toContain('database_native_type');

      // The second seeded bucket has no tables → empty table list.
      const emptyBucket = await callToolText(session.client, 'get_tables', {
        bucket_ids: ['in.c-test_bucket_02'],
      });
      expect(emptyBucket).not.toContain(SEEDED_TABLE_ID);
    } finally {
      await session.close();
    }
  });

  it('get_tables emits TOON tabular output (port of test_get_tables_output_format)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_tables', {
        bucket_ids: ['in.c-test_bucket_01'],
      });
      // One table in the bucket → TOON list-of-objects tabular header `tables[1]{...}:`.
      expect(text).toMatch(/^tables\[1\]\{[^}]*\bid\b[^}]*\}:/m);
    } finally {
      await session.close();
    }
  });

  // --- update_descriptions -------------------------------------------------

  it('update_descriptions updates a bucket description (port of test_update_descriptions_bucket)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const bucketId = SEEDED_BUCKET_IDS[0]!;
      const text = await callToolText(session.client, 'update_descriptions', {
        updates: [{ item_id: bucketId, description: 'New Description' }],
      });
      expect(text).toMatch(/total_processed:\s*1/);
      expect(text).toMatch(/successful:\s*1/);
      expect(text).toMatch(/failed:\s*0/);
      expect(text).toContain(bucketId);

      // Verify the description actually landed: get_buckets detail surfaces it.
      const detail = await callToolText(session.client, 'get_buckets', { bucket_ids: [bucketId] });
      expect(detail).toContain('New Description');
    } finally {
      await session.close();
    }
  });

  it('update_descriptions updates a table description (port of test_update_descriptions_table)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'update_descriptions', {
        updates: [{ item_id: SEEDED_TABLE_ID, description: 'New Table Description' }],
      });
      expect(text).toMatch(/total_processed:\s*1/);
      expect(text).toMatch(/successful:\s*1/);
      expect(text).toMatch(/failed:\s*0/);
      expect(text).toContain(SEEDED_TABLE_ID);

      const detail = await callToolText(session.client, 'get_tables', {
        table_ids: [SEEDED_TABLE_ID],
      });
      expect(detail).toContain('New Table Description');
    } finally {
      await session.close();
    }
  });

  it('update_descriptions updates a column description (port of test_update_descriptions_table_column)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const columnName = SEEDED_COLUMNS[0]!;
      const columnId = `${SEEDED_TABLE_ID}.${columnName}`;
      const text = await callToolText(session.client, 'update_descriptions', {
        updates: [{ item_id: columnId, description: 'New Table Column Description' }],
      });
      expect(text).toMatch(/total_processed:\s*1/);
      expect(text).toMatch(/successful:\s*1/);
      expect(text).toMatch(/failed:\s*0/);
      expect(text).toContain(columnId);

      // The column description must surface in the table detail's column listing.
      const detail = await callToolText(session.client, 'get_tables', {
        table_ids: [SEEDED_TABLE_ID],
      });
      expect(detail).toContain('New Table Column Description');
    } finally {
      await session.close();
    }
  });

  it('update_descriptions handles mixed item types in one call (port of test_update_descriptions_mixed_types)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const bucketId = SEEDED_BUCKET_IDS[0]!;
      const columnName = SEEDED_COLUMNS[0]!;
      const text = await callToolText(session.client, 'update_descriptions', {
        updates: [
          { item_id: bucketId, description: 'Mixed Bucket Description' },
          { item_id: SEEDED_TABLE_ID, description: 'Mixed Table Description' },
          { item_id: `${SEEDED_TABLE_ID}.${columnName}`, description: 'Mixed Column Description' },
        ],
      });
      expect(text).toMatch(/total_processed:\s*3/);
      expect(text).toMatch(/successful:\s*3/);
      expect(text).toMatch(/failed:\s*0/);

      const bucketDetail = await callToolText(session.client, 'get_buckets', {
        bucket_ids: [bucketId],
      });
      expect(bucketDetail).toContain('Mixed Bucket Description');

      const tableDetail = await callToolText(session.client, 'get_tables', {
        table_ids: [SEEDED_TABLE_ID],
      });
      expect(tableDetail).toContain('Mixed Table Description');
      expect(tableDetail).toContain('Mixed Column Description');
    } finally {
      await session.close();
    }
  });

  it('update_descriptions reports failure for an invalid item id (port of test_update_descriptions_invalid_path)', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      // The tool itself succeeds (no isError); the per-item result records the failure.
      const text = await callToolText(session.client, 'update_descriptions', {
        updates: [{ item_id: 'invalid-path', description: 'This should fail' }],
      });
      expect(text).toMatch(/total_processed:\s*1/);
      expect(text).toMatch(/successful:\s*0/);
      expect(text).toMatch(/failed:\s*1/);
      expect(text).toContain('invalid-path');
      expect(text).toContain('Invalid item_id format');
    } finally {
      await session.close();
    }
  });

  it('get_tables flags missing tables rather than erroring', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      // A non-existent table id is reported via tables_not_found, not a tool error.
      const result = await callToolRaw(session.client, 'get_tables', {
        table_ids: ['in.c-test_bucket_01.does_not_exist'],
      });
      expect(result.isError).toBeFalsy();
      const text = (result.content as { text: string }[])[0]!.text;
      expect(text).toContain('does_not_exist');
    } finally {
      await session.close();
    }
  });
});
