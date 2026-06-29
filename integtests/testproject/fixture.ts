import { onTestFinished } from 'vitest';

import { Config } from '@/config';
import { cleanProject } from './clean';
import { getPool } from './pool';
import type { AcquireOptions, Backend } from './types';

// Per-test-case project acquisition — port of go-utils GetTestProjectForTest. The lease is
// acquired now and released automatically when the current test finishes (vitest
// onTestFinished), so each case holds a project only for its own duration.

export type TestProject = {
  config: Config;
  storageApiUrl: string;
  storageApiToken: string;
  backend: Backend;
  projectId: number;
};

/**
 * Leases a project for the calling test, resets it to a clean state, and returns a ready
 * `Config`. The lease is released on test completion. If the whole pool is busy this blocks
 * (and retries) until a project frees up rather than failing.
 *
 * Must be called from within a running test (it registers onTestFinished).
 */
export const getTestProjectForTest = async (opts: AcquireOptions = {}): Promise<TestProject> => {
  const pool = getPool();
  const leased = await pool.getTestProject(opts);
  onTestFinished(async () => {
    await leased.release();
  });

  await cleanProject(leased.definition);

  return {
    config: new Config({
      storageApiUrl: leased.storageApiUrl,
      storageToken: leased.storageApiToken,
    }),
    storageApiUrl: leased.storageApiUrl,
    storageApiToken: leased.storageApiToken,
    backend: leased.backend,
    projectId: leased.definition.project,
  };
};
