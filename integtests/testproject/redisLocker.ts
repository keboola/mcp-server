import Redis from 'ioredis';
import { randomUUID } from 'node:crypto';

import type { Locker, ProjectLocker, ReleaseFn } from './locker';
import { lockKey, type ProjectDefinition } from './types';

/**
 * Redis-backed cross-runner project lease — port of go-utils
 * pkg/testproject/redislocker.go (which wraps bsm/redislock).
 *
 * We use raw ioredis + small Lua scripts instead of a lock library so the
 * compare-and-swap semantics match the go implementation exactly:
 *   obtain : SET key token NX PX ttl
 *   refresh: if GET key == token then PEXPIRE key ttl   (every ttl/4)
 *   release: if GET key == token then DEL key
 * A crashed worker's lease therefore self-expires after at most TTL.
 */
const TTL_MS = 2 * 60 * 1000;
const REFRESH_MS = TTL_MS / 4;

const REFRESH_LUA =
  "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";
const RELEASE_LUA =
  "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

/** Parses `redis://host:port` / `rediss://...` / a `+tls` suffix into ioredis options. */
const parseRedisUrl = (url: string): { host: string; port: number; tls: boolean } => {
  const sep = url.indexOf('://');
  if (sep === -1) throw new Error(`${url}: no protocol specified (expected redis://...)`);
  const scheme = url.slice(0, sep);
  const hostPort = url.slice(sep + 3);
  const [host, port] = hostPort.split(':');
  return {
    host: host || '127.0.0.1',
    port: port ? Number(port) : 6379,
    tls: scheme.includes('+tls') || scheme === 'rediss',
  };
};

export const createRedisLocker = (redisUrl: string, password: string): Locker => {
  const { host, port, tls } = parseRedisUrl(redisUrl);
  const client = new Redis({
    host,
    port,
    password,
    ...(tls ? { tls: { minVersion: 'TLSv1.2' as const } } : {}),
    maxRetriesPerRequest: 1,
    lazyConnect: false,
  });

  const forProject = (def: ProjectDefinition): ProjectLocker => {
    const key = lockKey(def);
    return {
      tryLock: async (): Promise<ReleaseFn | null> => {
        const token = randomUUID();
        const ok = await client.set(key, token, 'PX', TTL_MS, 'NX');
        if (ok !== 'OK') return null;

        // Auto-extend the lease for the (unknown) lifetime of the test.
        const timer = setInterval(() => {
          client.eval(REFRESH_LUA, 1, key, token, String(TTL_MS)).catch(() => {
            /* best-effort; lease will expire on its own if refresh fails */
          });
        }, REFRESH_MS);
        timer.unref?.();

        let released = false;
        return async (): Promise<void> => {
          if (released) return;
          released = true;
          clearInterval(timer);
          await client.eval(RELEASE_LUA, 1, key, token).catch(() => {
            /* lease will expire via TTL even if the explicit release fails */
          });
        };
      },
    };
  };

  return {
    forProject,
    close: async (): Promise<void> => {
      await client.quit().catch(() => client.disconnect());
    },
  };
};
