import { closeSync, mkdirSync, openSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import type { Locker, ProjectLocker, ReleaseFn } from './locker';
import { lockKey, type ProjectDefinition } from './types';

/**
 * Host-local file lock — port of go-utils pkg/testproject/fslocker.go. Used only as a
 * fallback for local single-host runs when no redis is configured; it provides NO
 * cross-runner safety (CI always uses the redis locker).
 *
 * A project is leased by exclusively creating `<dir>/<host>-<projectId>.lock`
 * (O_CREAT|O_EXCL); a concurrent creator gets EEXIST and treats the project as busy.
 * Release unlinks the file.
 */
export const createFsLocker = (dirName?: string): Locker => {
  const dir = dirName ?? join(tmpdir(), 'kbc-mcp-testproject-locks');
  mkdirSync(dir, { recursive: true });

  const forProject = (def: ProjectDefinition): ProjectLocker => {
    const file = join(dir, `${lockKey(def)}.lock`);
    return {
      tryLock: async (): Promise<ReleaseFn | null> => {
        let fd: number;
        try {
          fd = openSync(file, 'wx'); // exclusive create; throws EEXIST if held
        } catch {
          return null;
        }
        closeSync(fd);
        let released = false;
        return async (): Promise<void> => {
          if (released) return;
          released = true;
          rmSync(file, { force: true });
        };
      },
    };
  };

  return {
    forProject,
    close: async (): Promise<void> => {
      /* nothing to close */
    },
  };
};
