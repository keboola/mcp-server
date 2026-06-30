import { describe, expect, it } from 'vitest';

import { __testing } from '@/tools/validation';
import { getTestProjectForTest } from './testproject/fixture';

// Ported from integtests/test_validate.py.
//
// The Python test fetches the storage stack index (which lists every component with its
// root/row configuration schema) and runs each schema through KeboolaParametersValidator
// to confirm none of them are *structurally* invalid (jsonschema.SchemaError) — a sanity
// check that the MCP server can sanitize+validate every real component schema. Validation
// errors against dummy parameters are irrelevant and ignored; only schema errors fail.
//
// The TS validator's schema sanitization (sanitizeSchema, the equivalent of
// KeboolaParametersValidator.sanitize_schema) is the structural check: it throws on a
// structurally invalid schema and returns normally otherwise. We run it over every
// component's root and row schema fetched from the live stack index.

type RawComponent = {
  id: string;
  type?: string;
  name?: string;
  configurationSchema?: Record<string, unknown> | null;
  configurationRowSchema?: Record<string, unknown> | null;
};

describe('component schema validation (integration)', () => {
  it('sanitizes every root and row schema on the stack without a structural error', async () => {
    const project = await getTestProjectForTest({ clean: false });

    // Fetch the storage stack index directly — it carries the full component list with
    // their configuration schemas (the same source the Python test reads).
    const res = await fetch(`${project.storageApiUrl}/v2/storage`, {
      headers: { 'X-StorageApi-Token': project.storageApiToken },
    });
    expect(res.ok, `Storage index fetch failed: ${res.status}`).toBeTruthy();
    const data = (await res.json()) as { components?: RawComponent[] };
    const components = data.components ?? [];
    expect(components.length).toBeGreaterThan(0);

    let rootCount = 0;
    let rowCount = 0;
    const invalidRoot: string[] = [];
    const invalidRow: string[] = [];

    for (const component of components) {
      if (component.configurationSchema) {
        rootCount++;
        try {
          __testing.sanitizeSchema(component.configurationSchema);
        } catch {
          invalidRoot.push(component.id);
        }
      }
      if (component.configurationRowSchema) {
        rowCount++;
        try {
          __testing.sanitizeSchema(component.configurationRowSchema);
        } catch {
          invalidRow.push(component.id);
        }
      }
    }

    expect(invalidRoot, `Invalid root schemas (${invalidRoot.length}): ${invalidRoot}`).toEqual([]);
    expect(invalidRow, `Invalid row schemas (${invalidRow.length}): ${invalidRow}`).toEqual([]);
    // Sanity: the stack exposed at least some schemas to validate.
    expect(rootCount + rowCount).toBeGreaterThan(0);
  });
});
