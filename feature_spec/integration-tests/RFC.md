# RFC: TypeScript Integration Tests — per-case redis-leased project pool

Linear: PSGO-268 (TypeScript rewrite) · branch
`martinvasko-psgo-268-rewrite-keboola-mcp-server-from-python-to-typescript-11`

## Problem

The Python integration suite acquires **one** shared test project for an entire CI run
(`integtests/conftest.py` → `ProjectPool`, session-scoped) and serializes concurrent runners
with a **Storage-API branch-metadata** lock (`integtests/project_lock.py`). Two problems:

1. **Coarse acquisition.** A whole CI run holds a single project for its full duration, even
   though most test cases are independent. With a pool of N projects, at most N runs can
   proceed; a run that only needs a project for one case still blocks one for minutes.
2. **Inconsistent locking.** The branch-metadata lock works but is intricate
   (write-and-verify window, oldest-timestamp-wins, stale-TTL cleanup, anti-collision sleeps)
   and the team already has a battle-tested redis lease (`keboola/go-utils`
   `pkg/testproject`) used across the go monorepo that "works perfectly". Reimplementing a
   second bespoke protocol in TS is wasted risk.

The TS rewrite has **no integration tests yet**. Rather than port the branch-metadata lock,
this RFC adopts the go-utils model: a **redis-leased pool**, acquired **per test case**, with
the same `projects.json` layout and CI export mechanism the go monorepo uses.

## Required Behavior

| Concern | Behavior |
| --- | --- |
| Pool source | A `projects.json` array of project definitions (host, project id, token, backend, stagingStorage), the same schema as [`go-monorepo/build/ci/projects.json`](https://github.com/keboola/go-monorepo/blob/main/build/ci/projects.json). Path from `TEST_KBC_PROJECTS_FILE` (absolute). |
| CI secret injection | A composite action mirroring [`export-kbc-projects`](https://github.com/keboola/go-monorepo/blob/main/.github/actions/export-kbc-projects/action.yml): `envsubst` the `$TEST_KBC_PROJECT_<id>_TOKEN` placeholders in `build/ci/projects.json` from `TEST_KBC_PROJECT_*` secrets into a runtime `projects.json`. |
| Acquisition granularity | **Per test case**, not per run. A test calls `getTestProject(...)`; the lease is released automatically at the end of that test. |
| Mutual exclusion | A **redis lease** per `(host, projectId)` key (port of go-utils `redislocker.go`): `SET key token NX PX ttl`; auto-extend at `ttl/4`; release via compare-and-delete. TTL 2 min. |
| Local fallback | When no redis is configured, fall back to a **host-local file lock** (port of `fslocker.go`) so the suite runs on a developer machine without redis. |
| Exhaustion policy | When every compatible project is currently leased, **do not error** — sleep briefly and retry the whole pool **forever** (matching `go-utils` `GetTestProject`). The only failure is "no *compatible* project exists at all" (e.g. asked for BigQuery, pool has none). |
| Compatibility filter | Optional `{ backend }` selector so a test can require `snowflake` / `bigquery`; only matching definitions are considered. |
| Project hygiene | On acquisition, the project is reset to a known-clean state (`cleanProject`, port of the Python `_purge_project` guard + wipe) before the test runs; a dedicated-project guard refuses to wipe a project holding non-`*.c-test*` buckets. |
| Parallelism | Vitest runs files in parallel workers; each worker leases independently. Two cases on different workers may hold two different projects at once; two cases never share one project. |

### Environment contract

| Variable | Meaning | Required |
| --- | --- | --- |
| `TEST_KBC_PROJECTS_FILE` | Absolute path to the generated `projects.json` | yes |
| `TEST_KBC_PROJECTS_LOCK_HOST` | redis URL (`redis://host:port`, `+tls` suffix for TLS) | CI only |
| `TEST_KBC_PROJECTS_LOCK_PASSWORD` | redis password | CI only |
| `TEST_KBC_PROJECTS_LOCK_DIR_NAME` | dir for the fs-locker fallback | local optional |

No per-token env arrays (`INTEGTEST_STORAGE_TOKENS`) anymore — the pool is the JSON file.

## Resolution Strategy

New harness under `integtests/testproject/` (a faithful TS port of the go-utils package +
the go-monorepo `internal/utils/testproject/project.go` wrapper):

| File | Responsibility | Ported from |
| --- | --- | --- |
| `types.ts` | `ProjectDefinition` (zod-validated), `LockedProject`, `Backend`/`StagingStorage` enums | `testproject.go` `Definition` |
| `projects.ts` | Load + parse + validate `projects.json` from `TEST_KBC_PROJECTS_FILE`; process-singleton pool | `getProjects`/`GetProjectsFrom` |
| `redisLocker.ts` | ioredis lease: `SET NX PX` obtain, Lua compare-and-`pexpire` refresh, Lua compare-and-`del` release; background auto-extend timer at `TTL/4` | `redislocker.go` |
| `fsLocker.ts` | `proper-lockfile`/`O_CREAT|O_EXCL` host-local lock | `fslocker.go` |
| `pool.ts` | `getTestProject({backend?})`: loop compatible defs, `tryLock` each; if none free sleep 100 ms and retry forever; throw only when no compatible def exists | `ProjectsPool.GetTestProject` |
| `clean.ts` | `cleanProject(endpoint)`: dedicated-project guard + wipe buckets / configs / workspaces / MCP branch metadata via `@keboola/api-client` | Python `_purge_project` |
| `fixture.ts` | `getTestProjectForTest()` — acquires, registers `onTestFinished(release)`, returns `{ config, storageApiUrl, storageApiToken, backend, cleanup }` | `GetTestProjectForTest` |

Test harness:

- `vitest.integ.config.ts` — separate config (`include: integtests/**/*.test.ts`, long
  `testTimeout`, `fileParallelism: true`, `globalSetup` validates the pool loads once).
- npm scripts: `test:integ` (run), `test:integ:watch`.
- Each ported test calls `getTestProjectForTest()` in the body (or a small per-test fixture),
  builds the in-memory MCP client against that project's `Config`, exercises tools, asserts.
- Reuse the existing unit-test harness shape (`InMemoryTransport` + MCP `Client`) — the only
  difference is the `Config` comes from a leased project and the calls hit the real stack.

Redis semantics (port of `redislocker.go`, no extra lock library — raw ioredis + Lua so the
compare-and-swap matches `bsm/redislock` exactly):

```
obtain : SET <host>-<projectId> <token> NX PX <ttl>        -> ok ? leased : busy
refresh: if GET key == token then PEXPIRE key <ttl>        (Lua, every ttl/4)
release: if GET key == token then DEL key                  (Lua)
```

The auto-extend timer holds the lease for the (unknown, possibly long) duration of a single
test and is cleared on release, so a crashed worker's lease expires after at most `TTL`.

CI: a dedicated `integration_tests` job (separate from `ci.yml`) runs
`export-kbc-projects` → `npm run test:integ` with redis service + `TEST_KBC_PROJECT_*`
secrets. Sketch lives in this folder's `ci-job.yml` and is wired into the workflow at
implementation time.

### Non-obvious trade-offs

- **Per-case vs per-file acquisition.** Per *case* maximizes pool utilization but multiplies
  lease churn and `cleanProject` cost (a wipe per case). We default to per-case (as
  requested) but expose a per-file helper for suites whose cases share expensive fixtures
  (e.g. the storage suite that seeds buckets/tables once). The lease helper is the same; only
  the scope of `onTestFinished` vs `beforeAll/afterAll` differs.
- **Infinite retry.** Matches go-utils and is correct for a bounded CI pool, but a
  genuinely deadlocked pool would hang until the job timeout rather than failing fast. We
  rely on the job-level timeout + the `TTL`-bounded lease expiry as the backstop, and log the
  wait every few seconds so a stuck pool is visible.
- **Redis as the single point of coordination.** If redis is down, CI integtests can't
  coordinate. The fs-locker fallback is host-local only (no cross-runner safety), so CI
  always uses redis; the fallback exists purely for local single-host runs.

## Scope

In scope:
- The `integtests/testproject/` harness, `projects.json` layout, `export-kbc-projects` action,
  `vitest.integ.config.ts`, npm scripts, `integtests/README.md`, and the CI job.
- Porting the Python integration test *cases* (`integtests/**`) to vitest on top of the new
  harness — done incrementally per module after this RFC is agreed.

Out of scope:
- Changing production server behavior. Integtests exercise the shipped tools as-is.
- Cross-host fs-locking (redis is the cross-runner mechanism).
- Removing the Python integtests until the TS ports reach parity (kept as the reference).
- The unit-test suite (already complete: 383 tests).

## Testing / Verification

- **Harness unit tests** (run in the normal `vitest` suite, no redis/projects needed):
  `projects.ts` parsing/validation, `pool.ts` selection + infinite-retry (with a fake
  in-memory locker), `redisLocker.ts` against `ioredis-mock`, the dedicated-project guard in
  `clean.ts`. These give us confidence without a live stack.
- **Live integration run** (CI `integration_tests` job): redis service + `TEST_KBC_PROJECT_*`
  secrets → `export-kbc-projects` → `npm run test:integ`. Verifies real leasing, parallel
  workers never collide on a project, and the ported tool tests pass against a real stack.
- **Parity check:** each ported module is cross-checked against its Python counterpart in
  `integtests/` (same scenarios, same assertions) before the Python file is removed.
- **Manual local run:** developer sets `TEST_KBC_PROJECTS_FILE` to a one-project file (no
  redis) and runs `npm run test:integ` — exercises the fs-locker path.
