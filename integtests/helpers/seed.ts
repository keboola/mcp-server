import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { TestProject } from '../testproject/fixture';

// Seeds a leased project with the same fixtures the Python integ suite creates
// (integtests/conftest.py: _create_buckets/_create_tables/_create_configs): two input
// buckets, one CSV table, and two component configurations. Uses the raw Storage API
// (form-encoded, matching the kbcstorage SDK the Python tests used) so it does not depend
// on api-client method shapes.

const DATA_DIR = join(dirname(fileURLToPath(import.meta.url)), '..', 'data', 'proj');

export type SeedBucket = { id: string; displayName: string };
export type SeedTable = { id: string; bucketId: string; name: string };
export type SeedConfig = { componentId: string; configurationId: string; internalId: string };
export type SeededProject = {
  buckets: SeedBucket[];
  tables: SeedTable[];
  configs: SeedConfig[];
};

const BUCKETS: { displayName: string; id: string }[] = [
  { displayName: 'test_bucket_01', id: 'in.c-test_bucket_01' },
  { displayName: 'test_bucket_02', id: 'in.c-test_bucket_02' },
];
const TABLES: { bucketId: string; name: string; id: string }[] = [
  { bucketId: 'in.c-test_bucket_01', name: 'test_table_01', id: 'in.c-test_bucket_01.test_table_01' },
];
const CONFIGS: { componentId: string; internalId: string; file: string }[] = [
  { componentId: 'ex-generic-v2', internalId: 'test_config1', file: 'ex-generic-v2/test_config1.json' },
  {
    componentId: 'keboola.snowflake-transformation',
    internalId: 'test_config2',
    file: 'keboola.snowflake-transformation/test_config2.json',
  },
];

/** Form-encoded POST to the Storage API (the shape kbcstorage write endpoints expect). */
const form = async (
  base: string,
  token: string,
  path: string,
  fields: Record<string, string>,
): Promise<Record<string, unknown>> => {
  const body = new URLSearchParams(fields);
  const res = await fetch(`${base}/v2/storage/${path}`, {
    method: 'POST',
    headers: {
      'X-StorageApi-Token': token,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body,
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`Seed POST ${path} failed: ${res.status} ${text}`);
  return text ? (JSON.parse(text) as Record<string, unknown>) : {};
};

export const seedProject = async (project: TestProject): Promise<SeededProject> => {
  const base = project.storageApiUrl;
  const token = project.storageApiToken;

  const buckets: SeedBucket[] = [];
  for (const b of BUCKETS) {
    const created = await form(base, token, 'buckets', { name: b.displayName, stage: 'in' });
    buckets.push({ id: String(created.id ?? b.id), displayName: b.displayName });
  }

  const tables: SeedTable[] = [];
  for (const t of TABLES) {
    const csv = readFileSync(join(DATA_DIR, 'buckets', t.bucketId, `${t.name}.csv`), 'utf-8');
    // Synchronous create-from-string: the simplest way to seed a small table without the
    // file-upload + async-import dance.
    const created = await form(base, token, `buckets/${t.bucketId}/tables`, {
      name: t.name,
      dataString: csv,
    });
    tables.push({ id: String(created.id ?? t.id), bucketId: t.bucketId, name: t.name });
  }

  const configs: SeedConfig[] = [];
  for (const c of CONFIGS) {
    const configuration = readFileSync(join(DATA_DIR, 'configs', c.file), 'utf-8');
    const created = await form(base, token, `branch/default/components/${c.componentId}/configs`, {
      name: c.internalId,
      configuration,
    });
    configs.push({
      componentId: c.componentId,
      configurationId: String(created.id ?? ''),
      internalId: c.internalId,
    });
  }

  return { buckets, tables, configs };
};
