import type { ProjectDefinition } from './types';

/**
 * Locker abstraction (port of the go-utils `locker` / `projectLocker` interfaces).
 *
 * `tryLock` performs a single, non-blocking attempt to lease the project. It returns a
 * `release` function on success, or `null` if the project is currently leased elsewhere.
 * The pool (pool.ts) is responsible for the retry loop.
 */
export type ReleaseFn = () => Promise<void>;

export type ProjectLocker = {
  tryLock: () => Promise<ReleaseFn | null>;
};

export type Locker = {
  forProject: (def: ProjectDefinition) => ProjectLocker;
  /** Releases any locker-wide resources (e.g. the redis connection). */
  close: () => Promise<void>;
};

export const LOCK_HOST_ENV = 'TEST_KBC_PROJECTS_LOCK_HOST';
export const LOCK_PASSWORD_ENV = 'TEST_KBC_PROJECTS_LOCK_PASSWORD';
export const LOCK_DIR_ENV = 'TEST_KBC_PROJECTS_LOCK_DIR_NAME';
