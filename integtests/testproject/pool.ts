import { createFsLocker } from './fsLocker';
import { LOCK_DIR_ENV, LOCK_HOST_ENV, LOCK_PASSWORD_ENV,type Locker } from './locker';
import { loadProjects } from './projects';
import { createRedisLocker } from './redisLocker';
import {
  type AcquireOptions,
  isCompatible,
  type LockedProject,
  type ProjectDefinition,
  storageApiUrl,
} from './types';

// Port of go-utils ProjectsPool.GetTestProject: try each compatible project once; if all are
// busy, sleep briefly and retry the WHOLE pool forever. The only hard error is "no compatible
// project exists in the pool at all".

const RETRY_DELAY_MS = 100;
const WAIT_LOG_EVERY_MS = 5000;

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Chooses the redis locker when LOCK_HOST/PASSWORD are set, else the host-local fs fallback. */
export const newLocker = (): Locker => {
  const host = process.env[LOCK_HOST_ENV];
  const password = process.env[LOCK_PASSWORD_ENV];
  if (host) {
    if (!password) throw new Error(`${LOCK_PASSWORD_ENV} is required when ${LOCK_HOST_ENV} is set.`);
    return createRedisLocker(host, password);
  }
  return createFsLocker(process.env[LOCK_DIR_ENV]);
};

export type Pool = {
  getTestProject: (opts?: AcquireOptions) => Promise<LockedProject>;
  close: () => Promise<void>;
};

export const createPool = (defs: ProjectDefinition[], locker: Locker): Pool => {
  const lockers = new Map(defs.map((d) => [d, locker.forProject(d)]));

  const getTestProject = async (opts: AcquireOptions = {}): Promise<LockedProject> => {
    const compatible = defs.filter((d) => isCompatible(d, opts));
    if (compatible.length === 0) {
      throw new Error(`No compatible test project in the pool (backend=${opts.backend ?? 'any'}).`);
    }

    let lastLog = 0;
    // Randomize the start so parallel workers don't all stampede the first project.
    const start = Math.floor(Math.random() * compatible.length);
    for (let attempt = 0; ; attempt++) {
      for (let i = 0; i < compatible.length; i++) {
        const def = compatible[(start + i) % compatible.length]!;
        const release = await lockers.get(def)!.tryLock();
        if (release) {
          return {
            definition: def,
            storageApiUrl: storageApiUrl(def),
            storageApiToken: def.token,
            backend: def.backend,
            release,
          };
        }
      }
      const now = Date.now();
      if (now - lastLog >= WAIT_LOG_EVERY_MS) {
        // eslint-disable-next-line no-console
        console.info(`[testproject] all ${compatible.length} project(s) busy; waiting…`);
        lastLog = now;
      }
      await sleep(RETRY_DELAY_MS);
    }
  };

  return { getTestProject, close: () => locker.close() };
};

let singleton: Pool | undefined;

/** Process-singleton pool built from TEST_KBC_PROJECTS_FILE + the env-selected locker. */
export const getPool = (): Pool => {
  if (!singleton) singleton = createPool(loadProjects(), newLocker());
  return singleton;
};
