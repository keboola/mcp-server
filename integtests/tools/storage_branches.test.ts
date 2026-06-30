import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { callToolText, connectMcp, type McpSession } from '../helpers/mcp';

import { Config } from '@/config';

// Ported from integtests/tools/test_storage_branches.py — validates the storage-branches
// deference mechanism (a dev-branch MCP context sees production buckets/tables deferred into the
// branch, plus the branch's own new objects, and gets a branch-scoped workspace for SQL).
//
// These tests require a project WITH the `storage-branches` feature, which the regular local pool
// projects do NOT have (CI uses a dedicated project — 3055 — supplied out-of-band). The project is
// addressed directly by env vars, mirroring the Python fixtures:
//   - INTEGTEST_POOL_STORAGE_API_URL          → storage API base URL
//   - INTEGTEST_STORAGE_TOKEN_STORAGE_BRANCHES → token for the storage-branches project
//
// When either is absent the whole suite is skipped (see SKIP_REASON). The setup faithfully mirrors
// the Python harness: it provisions production data idempotently, creates two dev branches, and
// runs Python transformations in each branch so they run unchanged once such a project is wired up.

const STORAGE_BRANCHES_TOKEN_ENV_VAR = 'INTEGTEST_STORAGE_TOKEN_STORAGE_BRANCHES';
const POOL_STORAGE_API_URL_ENV_VAR = 'INTEGTEST_POOL_STORAGE_API_URL';
const PYTHON_TRANSFORMATION_COMPONENT = 'keboola.python-transformation-v2';

const token = (process.env[STORAGE_BRANCHES_TOKEN_ENV_VAR] ?? '').trim();
const storageApiUrl = (process.env[POOL_STORAGE_API_URL_ENV_VAR] ?? '').trim();

// Gate the suite: without the dedicated project's token + URL there is nothing to run against.
// The dedicated storage-branches project is 3055 in CI, not necessarily in the local pool.
const SKIP_REASON = !token
  ? `${STORAGE_BRANCHES_TOKEN_ENV_VAR} not set (storage-branches project unavailable in this pool)`
  : !storageApiUrl
    ? `${POOL_STORAGE_API_URL_ENV_VAR} not set (storage-branches project URL unavailable)`
    : null;

// --- HTTP helpers (ports of the Python _api_request / job-wait helpers) ----------------------

const apiRequest = async (
  method: string,
  url: string,
  init: { json?: unknown; form?: Record<string, string> } = {},
): Promise<Record<string, unknown>> => {
  const headers: Record<string, string> = { 'X-StorageApi-Token': token };
  let body: string | URLSearchParams | undefined;
  if (init.json !== undefined) {
    headers['Content-Type'] = 'application/json';
    body = JSON.stringify(init.json);
  } else if (init.form !== undefined) {
    body = new URLSearchParams(init.form);
  }
  const resp = await fetch(url, { method, headers, body });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`${method} ${url} failed: ${resp.status} ${text}`);
  return text ? (JSON.parse(text) as Record<string, unknown>) : {};
};

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const waitForStorageJob = async (jobId: string, timeoutMs = 120_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await apiRequest('GET', `${storageApiUrl}/v2/storage/jobs/${jobId}`);
    const status = job.status;
    if (status === 'success') return;
    if (status === 'error' || status === 'cancelled') {
      throw new Error(`Storage job ${jobId} failed: ${JSON.stringify(job)}`);
    }
    await sleep(2_000);
  }
  throw new Error(`Storage job ${jobId} did not complete within ${timeoutMs}ms`);
};

const waitForQueueJob = async (jobId: string, timeoutMs = 300_000): Promise<void> => {
  const queueUrl = storageApiUrl.replace('connection.', 'queue.');
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await apiRequest('GET', `${queueUrl}/jobs/${jobId}`);
    const status = job.status;
    if (status === 'success') return;
    if (status === 'error' || status === 'cancelled' || status === 'terminated') {
      throw new Error(`Queue job ${jobId} failed with status=${String(status)}`);
    }
    await sleep(5_000);
  }
  throw new Error(`Queue job ${jobId} did not complete within ${timeoutMs}ms`);
};

const createBranch = async (name: string): Promise<string> => {
  const job = await apiRequest('POST', `${storageApiUrl}/v2/storage/dev-branches`, { json: { name } });
  const jobId = String(job.id);
  await waitForStorageJob(jobId);
  const done = await apiRequest('GET', `${storageApiUrl}/v2/storage/jobs/${jobId}`);
  return String((done.results as Record<string, unknown>).id);
};

const deleteBranch = async (branchId: string): Promise<void> => {
  try {
    const job = await apiRequest('DELETE', `${storageApiUrl}/v2/storage/dev-branches/${branchId}`);
    await waitForStorageJob(String(job.id));
  } catch {
    // Best-effort cleanup, matching the Python teardown.
  }
};

const ensureBucket = async (name: string, stage = 'in'): Promise<string> => {
  const bucketId = `${stage}.c-${name}`;
  const resp = await fetch(`${storageApiUrl}/v2/storage/buckets/${bucketId}`, {
    headers: { 'X-StorageApi-Token': token },
  });
  if (resp.ok) return bucketId;
  if (resp.status !== 404) throw new Error(`GET bucket ${bucketId} failed: ${resp.status}`);
  const created = await apiRequest('POST', `${storageApiUrl}/v2/storage/buckets`, {
    json: { name, stage, description: 'Integration test bucket' },
  });
  return String(created.id);
};

const ensureTable = async (bucketId: string, tableName: string, csvData: string): Promise<string> => {
  const tableId = `${bucketId}.${tableName}`;
  const resp = await fetch(`${storageApiUrl}/v2/storage/tables/${tableId}`, {
    headers: { 'X-StorageApi-Token': token },
  });
  if (resp.ok) return tableId;
  if (resp.status !== 404) throw new Error(`GET table ${tableId} failed: ${resp.status}`);
  const created = await apiRequest('POST', `${storageApiUrl}/v2/storage/buckets/${bucketId}/tables`, {
    form: { name: tableName, delimiter: ',', dataString: csvData },
  });
  return String(created.id);
};

const pythonTransformConfig = (
  destination: string,
  csvFilename: string,
  fieldnames: string[],
  row: Record<string, string>,
): Record<string, unknown> => {
  const fieldsStr = JSON.stringify(fieldnames).replace(/"/g, "'");
  const rowStr = JSON.stringify(row).replace(/"/g, "'");
  const script = [
    'import csv',
    'import os',
    "os.makedirs('out/tables', exist_ok=True)",
    `with open('out/tables/${csvFilename}', mode='wt', encoding='utf-8') as f:`,
    `    writer = csv.DictWriter(f, fieldnames=${fieldsStr}, dialect='kbc')`,
    '    writer.writeheader()',
    `    writer.writerow(${rowStr})`,
  ].join('\n');
  return {
    storage: {
      output: {
        tables: [{ source: csvFilename, destination, primary_key: ['id'] }],
      },
    },
    parameters: {
      blocks: [{ name: 'Generate data', codes: [{ name: 'script', script: [script] }] }],
      packages: [],
    },
  };
};

const createConfigInBranch = async (
  branchId: string,
  componentId: string,
  name: string,
  config: Record<string, unknown>,
): Promise<string> => {
  const created = await apiRequest(
    'POST',
    `${storageApiUrl}/v2/storage/branch/${branchId}/components/${componentId}/configs`,
    { json: { name, description: `Integration test config: ${name}`, configuration: JSON.stringify(config) } },
  );
  return String(created.id);
};

const runJobInBranch = async (branchId: string, componentId: string, configId: string): Promise<void> => {
  const queueUrl = storageApiUrl.replace('connection.', 'queue.');
  const job = await apiRequest('POST', `${queueUrl}/jobs`, {
    json: { component: componentId, config: configId, mode: 'run', branchId },
  });
  await waitForQueueJob(String(job.id));
};

// --- Suite -----------------------------------------------------------------------------------

type BranchProject = { branchAId: string; branchBId: string };

const describeBranches = SKIP_REASON ? describe.skip : describe;

describeBranches('storage-branches tools (integration)', () => {
  let project: BranchProject;
  // Per-branch Config: production-branch alias when branchId is undefined.
  const configFor = (branchId?: string): Config =>
    new Config({ storageApiUrl, storageToken: token, branchId });

  beforeAll(async () => {
    // Confirm the token's project actually carries the storage-branches feature before paying for
    // the (slow) branch + transformation-job setup.
    const verify = await apiRequest('GET', `${storageApiUrl}/v2/storage/tokens/verify`);
    const owner = (verify.owner ?? {}) as Record<string, unknown>;
    const features = Array.isArray(owner.features) ? (owner.features as string[]) : [];
    if (!features.includes('storage-branches')) {
      throw new Error(
        `project ${String(owner.name)} must have the storage-branches feature enabled to run these tests`,
      );
    }

    // Idempotent production data (shared safely across concurrent sessions).
    await ensureBucket('test_bucket_01');
    await ensureTable(
      'in.c-test_bucket_01',
      'test_table_01',
      '"id","name","item_count"\n1,"item1",10\n2,"item2",20',
    );

    const uid = Math.random().toString(36).slice(2, 10);
    const branchAId = await createBranch(`integtest-branch-A-${uid}`);
    const branchBId = await createBranch(`integtest-branch-B-${uid}`);

    // Branch A: update the existing production table (creates a branched version).
    let cid = await createConfigInBranch(
      branchAId,
      PYTHON_TRANSFORMATION_COMPONENT,
      'update-tbl',
      pythonTransformConfig('in.c-test_bucket_01.test_table_01', 'test_table_01.csv', ['id', 'name', 'item_count'], {
        id: '99',
        name: 'branched_item',
        item_count: '999',
      }),
    );
    await runJobInBranch(branchAId, PYTHON_TRANSFORMATION_COMPONENT, cid);

    // Branch A: create a new bucket + table that exists only in the branch.
    cid = await createConfigInBranch(
      branchAId,
      PYTHON_TRANSFORMATION_COMPONENT,
      'create-tbl',
      pythonTransformConfig('in.c-test_branch.test_table_branch', 'test_table_branch.csv', ['id', 'name', 'value'], {
        id: '1',
        name: 'branch_a_data',
        value: '100',
      }),
    );
    await runJobInBranch(branchAId, PYTHON_TRANSFORMATION_COMPONENT, cid);

    // Branch B: create a different branch-only bucket + table.
    cid = await createConfigInBranch(
      branchBId,
      PYTHON_TRANSFORMATION_COMPONENT,
      'create-b-tbl',
      pythonTransformConfig('in.c-test_branch_2.test_table_branch', 'test_table_branch.csv', ['id', 'name', 'value'], {
        id: '1',
        name: 'branch_b_data',
        value: '200',
      }),
    );
    await runJobInBranch(branchBId, PYTHON_TRANSFORMATION_COMPONENT, cid);

    project = { branchAId, branchBId };
  }, 600_000);

  afterAll(async () => {
    if (project) {
      await deleteBranch(project.branchAId);
      await deleteBranch(project.branchBId);
    }
  });

  const withSession = async (config: Config, fn: (session: McpSession) => Promise<void>): Promise<void> => {
    const session = await connectMcp(config);
    try {
      await fn(session);
    } finally {
      await session.close();
    }
  };

  it('get_buckets from Branch A includes production + Branch A buckets, not Branch B (test_list_buckets_includes_branch_a_bucket)', async () => {
    await withSession(configFor(project.branchAId), async (session) => {
      const text = await callToolText(session.client, 'get_buckets');
      expect(text).toContain('in.c-test_bucket_01');
      expect(text).toContain('in.c-test_branch');
      expect(text).not.toContain('in.c-test_branch_2');
    });
  });

  it('get_tables lists a branch-only bucket with a production-like id (test_list_tables_in_branched_bucket)', async () => {
    await withSession(configFor(project.branchAId), async (session) => {
      const text = await callToolText(session.client, 'get_tables', { bucket_ids: ['in.c-test_branch'] });
      expect(text).toContain('test_table_branch');
      expect(text).toContain('in.c-test_branch.test_table_branch');
      // branch_id is internal-only and must not leak into the deferred (production-like) output.
      expect(text).not.toMatch(/branch_id/);
    });
  });

  it('get_tables for the production bucket sees the branched table (test_deference_branched_table)', async () => {
    await withSession(configFor(project.branchAId), async (session) => {
      const text = await callToolText(session.client, 'get_tables', { bucket_ids: ['in.c-test_bucket_01'] });
      expect(text).toContain('in.c-test_bucket_01.test_table_01');
      expect(text).not.toMatch(/branch_id/);
    });
  });

  it('get_project_info from a dev branch reports is_development_branch=true (test_get_project_info_reports_dev_branch)', async () => {
    await withSession(configFor(project.branchAId), async (session) => {
      const text = await callToolText(session.client, 'get_project_info');
      expect(text).toContain(String(project.branchAId));
      expect(text).toMatch(/is_development_branch:\s*true/);
      expect(text).toMatch(/branch_name:/);
    });
  });

  it('get_project_info from the default branch reports is_development_branch=false (test_get_project_info_reports_default_branch)', async () => {
    await withSession(configFor(undefined), async (session) => {
      const text = await callToolText(session.client, 'get_project_info');
      expect(text).toMatch(/is_development_branch:\s*false/);
      expect(text).toMatch(/branch_name:/);
      // The default branch must not be either dev branch created for this session.
      expect(text).not.toContain(String(project.branchAId));
      expect(text).not.toContain(String(project.branchBId));
    });
  });

  it.each([
    ['in.c-test_branch.test_table_branch', 'branch-only table created via transformation'],
    ['in.c-test_bucket_01.test_table_01', 'production table (also branched in Branch A)'],
  ])(
    'query_data from a dev branch reaches %s (test_query_data_from_dev_branch_reaches_both_kinds_of_tables)',
    async (tableId, description) => {
      await withSession(configFor(project.branchAId), async (session) => {
        const tablesListing = await callToolText(session.client, 'get_tables', { table_ids: [tableId] });
        // Pull the FQN out of the table detail; without it the table is not queryable.
        const fqnMatch = tablesListing.match(/(?:fully_qualified_name|fullyQualifiedName):\s*(\S+)/);
        expect(fqnMatch, `${description}: table ${tableId} has no FQN, cannot query`).not.toBeNull();
        const fqn = fqnMatch![1]!;

        const text = await callToolText(session.client, 'query_data', {
          sql_query: `SELECT COUNT(*) AS row_count FROM ${fqn}`,
          query_name: `Row count for ${tableId}`,
        });
        // csv_data holds a header row + a single COUNT(*) data row (a positive integer).
        expect(text).toContain('csv_data');
        const countMatch = text.match(/row_count[^\d]*(\d+)/i);
        expect(countMatch, `${description}: expected a numeric row count`).not.toBeNull();
        expect(Number(countMatch![1])).toBeGreaterThanOrEqual(1);
      });
    },
  );

  it('dev-branch and default-branch contexts get different workspaces (test_workspace_id_is_branch_aware)', async () => {
    let devWorkspace = '';
    let defaultWorkspace = '';
    await withSession(configFor(project.branchAId), async (session) => {
      const text = await callToolText(session.client, 'get_project_info');
      devWorkspace = text.match(/workspace_id:\s*(\d+)/)?.[1] ?? '';
    });
    await withSession(configFor(undefined), async (session) => {
      const text = await callToolText(session.client, 'get_project_info');
      defaultWorkspace = text.match(/workspace_id:\s*(\d+)/)?.[1] ?? '';
    });
    expect(Number(devWorkspace)).toBeGreaterThan(0);
    expect(Number(defaultWorkspace)).toBeGreaterThan(0);
    expect(devWorkspace).not.toBe(defaultWorkspace);
  });
});
