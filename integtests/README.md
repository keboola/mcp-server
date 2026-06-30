# Integration Tests

Integration tests run the shipped MCP tools against **real** Keboola projects. Each test case
leases a project from a shared pool, runs against it, and releases it — so many CI runners
(and many test cases) can proceed in parallel without corrupting each other's data.

Design rationale and the full protocol are in
[`feature_spec/integration-tests/RFC.md`](../feature_spec/integration-tests/RFC.md). This file
is the operational guide.

> **Status:** the harness (`integtests/testproject/`) and pool/CI wiring are in place. The
> individual test *cases* are being ported from the Python suite (`integtests/*.py`, kept as
> the parity reference) module by module.

---

## 1. The project pool (`projects.json`)

The pool is a JSON **array** of project definitions — the same schema the go monorepo uses
([`go-monorepo/build/ci/projects.json`](https://github.com/keboola/go-monorepo/blob/main/build/ci/projects.json)):

```json
[
  { "host": "connection.keboola.com", "project": 1234, "token": "<sapi-token>", "backend": "snowflake", "stagingStorage": "s3" },
  { "host": "connection.keboola.com", "project": 1235, "token": "<sapi-token>", "backend": "bigquery", "stagingStorage": "gcs" }
]
```

The committed `.github/ci/projects.json` is a **template** whose tokens are `$TEST_KBC_PROJECT_<id>_TOKEN`
placeholders. In CI the `export-kbc-projects` action substitutes them from `TEST_KBC_PROJECT_*`
secrets into a runtime `projects.json`.

The harness loads the pool once per process from the absolute path in **`TEST_KBC_PROJECTS_FILE`**.

### Environment contract

| Variable | Meaning | Where |
| --- | --- | --- |
| `TEST_KBC_PROJECTS_FILE` | Absolute path to the generated `projects.json` | always |
| `TEST_MCP_PROJECTS_LOCK_HOST` | redis URL (`redis://host:port`, `rediss://` or `+tls` for TLS) | CI |
| `TEST_MCP_PROJECTS_LOCK_PASSWORD` | redis password | CI |
| `TEST_MCP_PROJECTS_LOCK_DIR_NAME` | dir for the local fs-lock fallback | local (optional) |

When `TEST_MCP_PROJECTS_LOCK_HOST` is set, projects are leased via **redis** (cross-runner
safe). Otherwise the harness falls back to a **host-local file lock** — fine for a single
developer machine, but it provides no cross-runner safety, so CI always uses redis.

---

## 2. How leasing works

A test acquires a project with the per-case helper:

```ts
import { getTestProjectForTest } from '../testproject/fixture';

it('lists buckets', async () => {
  const { config, backend } = await getTestProjectForTest(); // or { backend: 'snowflake' }
  // build an in-memory MCP client from `config`, call tools, assert…
  // lease is released automatically when this test finishes
});
```

- **Per case, not per run.** The lease is held only for the duration of the calling test
  (released via vitest `onTestFinished`).
- **Redis lease** keyed by `<host>-<projectId>`: `SET NX PX` to obtain, auto-extended at
  `TTL/4`, compare-and-delete to release. A crashed worker's lease self-expires after `TTL`
  (2 min). Port of go-utils [`redislocker.go`](https://github.com/keboola/go-utils/blob/main/pkg/testproject/redislocker.go).
- **Exhaustion → wait, never error.** If every compatible project is busy, the pool sleeps
  briefly and retries the whole pool **forever** (matching go-utils `GetTestProject`). The
  only hard error is asking for a backend the pool has none of.
- **Clean on acquire.** Before handing the project over, `cleanProject` resets it (deletes
  buckets/configs/workspaces + MCP branch metadata). A guard refuses to wipe a project that
  holds any non-`*.c-test*` bucket — protection against a misconfigured pool pointing at a
  real project.

---

## 3. Running locally

Create a one- or two-project `projects.json` for **your own dedicated** test projects (never
the CI pool — you would interfere with CI), then:

```bash
export TEST_KBC_PROJECTS_FILE="$(pwd)/projects.local.json"
# no redis → host-local fs lock is used automatically
npm run test:integ
```

Add `TEST_MCP_PROJECTS_LOCK_HOST` / `_PASSWORD` only if you want to exercise the redis path
locally (e.g. `docker run -p 6379:6379 redis`).

Harness logic itself (pool selection, parsing, retry) is covered by ordinary unit tests in
`__tests__/testproject.test.ts` and runs in the normal `npm test` — no projects or redis
needed.

---

## 4. Security: don't leak tokens in test output

Vitest prints values in failure output. **Never** put a raw token where a failed assertion or
thrown error would render it. Read the token from the leased `config`/`TestProject` object and
pass it into clients — do not interpolate it into assertion messages or `console.log`. Tokens
in `projects.json` come from CI secrets; keep them out of logs and snapshots.

---

## 5. CI

The `integration_tests` job (see the RFC's `ci-job.yml` sketch) runs:

1. `export-kbc-projects` → generates `projects.json` from `TEST_KBC_PROJECT_*` secrets and
   exports its absolute path as `TEST_KBC_PROJECTS_FILE`.
2. Starts a redis service and sets `TEST_MCP_PROJECTS_LOCK_HOST` / `_PASSWORD`.
3. `npm run test:integ`.

Pool size = max concurrent runners. Add projects to `.github/ci/projects.json` (and a matching
`TEST_KBC_PROJECT_<id>_TOKEN` secret) to raise the ceiling. Integration tests are skipped for
fork PRs (no access to secrets).

### The committed pool (`.github/ci/projects.json`)

All on the `connection.europe-west3.gcp.keboola.com` stack (GCP → `gcs` staging):

| Project ID | Backend | Role |
|---|---|---|
| 2728, 2729 | Snowflake | pool |
| 2731, 2732 | BigQuery | pool |
| 2908 | Snowflake | has the `storage-branches` feature (used by the branch-storage tests) |

Each needs a `TEST_KBC_PROJECT_<id>_TOKEN` GitHub secret (a Storage API master token). The
redis lease comes from `vars.TEST_MCP_PROJECTS_LOCK_HOST` + `secrets.TEST_MCP_PROJECTS_LOCK_PASSWORD`,
and the pool file path from `vars.TEST_KBC_PROJECTS_FILE` — same convention as keboola/go-monorepo.
