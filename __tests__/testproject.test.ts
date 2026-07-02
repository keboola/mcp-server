import { describe, expect, it } from 'vitest';

import type { Locker } from '../integtests/testproject/locker';
import { createPool } from '../integtests/testproject/pool';
import { parseProjects } from '../integtests/testproject/projects';
import { isCompatible, lockKey, type ProjectDefinition } from '../integtests/testproject/types';

const def = (over: Partial<ProjectDefinition> = {}): ProjectDefinition => ({
  host: 'connection.keboola.com',
  project: 1,
  token: 'tok',
  backend: 'snowflake',
  stagingStorage: 's3',
  legacyTransformation: false,
  isGuest: false,
  ...over,
});

/** In-memory locker for deterministic pool tests (no redis/fs). */
const fakeLocker = (): { locker: Locker; held: Set<string> } => {
  const held = new Set<string>();
  return {
    held,
    locker: {
      forProject: (d) => ({
        tryLock: async () => {
          const k = lockKey(d);
          if (held.has(k)) return null;
          held.add(k);
          return async () => {
            held.delete(k);
          };
        },
      }),
      close: async () => {},
    },
  };
};

describe('parseProjects', () => {
  it('parses the array form and applies defaults', () => {
    const defs = parseProjects(
      JSON.stringify([
        {
          host: 'connection.keboola.com',
          project: 5684,
          token: 't',
          backend: 'bigquery',
          stagingStorage: 'gcs',
        },
      ]),
    );
    expect(defs).toHaveLength(1);
    expect(defs[0]!.project).toBe(5684);
    expect(defs[0]!.legacyTransformation).toBe(false);
    expect(defs[0]!.isGuest).toBe(false);
  });

  it('rejects an empty array', () => {
    expect(() => parseProjects('[]')).toThrow(/non-empty array/);
  });

  it('rejects an entry missing a required field', () => {
    expect(() =>
      parseProjects(
        JSON.stringify([{ host: 'h', project: 1, backend: 'snowflake', stagingStorage: 's3' }]),
      ),
    ).toThrow(/\[0\] is invalid/);
  });
});

describe('isCompatible', () => {
  it('matches any backend when none requested, else exact', () => {
    expect(isCompatible(def({ backend: 'snowflake' }), {})).toBe(true);
    expect(isCompatible(def({ backend: 'snowflake' }), { backend: 'snowflake' })).toBe(true);
    expect(isCompatible(def({ backend: 'snowflake' }), { backend: 'bigquery' })).toBe(false);
  });
});

describe('createPool.getTestProject', () => {
  it('leases a free project and exposes its URL/token', async () => {
    const { locker, held } = fakeLocker();
    const pool = createPool([def({ project: 1 })], locker);
    const p = await pool.getTestProject();
    expect(p.storageApiUrl).toBe('https://connection.keboola.com');
    expect(p.storageApiToken).toBe('tok');
    expect(held.size).toBe(1);
    await p.release();
    expect(held.size).toBe(0);
  });

  it('honors the backend selector', async () => {
    const { locker } = fakeLocker();
    const pool = createPool(
      [def({ project: 1, backend: 'snowflake' }), def({ project: 2, backend: 'bigquery' })],
      locker,
    );
    const p = await pool.getTestProject({ backend: 'bigquery' });
    expect(p.definition.project).toBe(2);
  });

  it('throws when no compatible project exists', async () => {
    const { locker } = fakeLocker();
    const pool = createPool([def({ backend: 'snowflake' })], locker);
    await expect(pool.getTestProject({ backend: 'bigquery' })).rejects.toThrow(/No compatible/);
  });

  it('retries (does not error) until a busy project frees up', async () => {
    const { locker, held } = fakeLocker();
    const d = def({ project: 7 });
    held.add(lockKey(d)); // pre-hold the only project
    const pool = createPool([d], locker);

    const acquired = pool.getTestProject();
    // Free it shortly after; the pool should keep retrying and then resolve.
    setTimeout(() => held.delete(lockKey(d)), 250);

    const p = await acquired;
    expect(p.definition.project).toBe(7);
  });
});
