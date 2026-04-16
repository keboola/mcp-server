# Integration Tests

Each test session creates and deletes real objects (buckets, tables, configurations) in a
Keboola project. The `project_lock` fixture ensures that only one session owns a given
project at a time, so concurrent CI jobs or overlapping local runs do not corrupt each
other's data.

> **Important**: The integration tests are **not** tied to specific projects. Any clean
> Keboola project with a workspace can be used. For local development you only need **two**
> projects in the pool (one Snowflake, one BigQuery) plus one old-branches project. The CI
> pool uses four projects for concurrency, but this is not a requirement for local runs.

---

## 1. Setting Up Your Own Test Environment

### Do NOT reuse the CI projects

The CI pool projects are shared across all CI runners. If you point your local `.env` at
them, you will interfere with CI runs: the pool lock mechanism will block or — worse — your
local run may acquire a CI project, crash mid-way, and leave it in a dirty state that
causes random CI failures.

**Always create your own dedicated test projects for local development.**

### Creating test projects

You need:

1. **Two pool projects** — one on Snowflake backend, one on BigQuery (or two of the same
   backend if you only need one). These must be **completely empty** (no buckets, no tables,
   no configurations) when a test session starts. The lock mechanism cleans up after each
   session, but if a session crashes, leftovers may remain and cause the next run to fail.

2. **A workspace in each pool project** — each project needs a read-only Snowflake or
   BigQuery workspace. This is required for `query_data` and FQN resolution tests.

3. **One old-branches project** — a project **without** the `storage-branches` feature,
   used by `test_storage_branches.py`. Does not need a workspace.

### Creating a workspace and finding the schema

Each pool project must have a workspace with read-only storage access. To create one:

1. Open the project in the Keboola UI
2. Go to **Transformations** and create a new workspace (Snowflake or BigQuery)
3. Enable **Read-only Storage Access** on the workspace

To find the workspace schema name (`WORKSPACE_XXXXXXXX`):

- **From the UI**: Click **Connect** on the workspace — the schema is shown in the
  connection details
- **From SQL**: Open the SQL editor in the workspace and run `SHOW SCHEMAS;` — look for the
  schema starting with `WORKSPACE_`
- **From the API**:
  ```bash
  curl -s -H "X-StorageApi-Token: YOUR_TOKEN" \
    "https://connection.YOUR-STACK.keboola.com/v2/storage/branch/default/workspaces" \
    | python3 -c "import json,sys; [print(f'{w[\"id\"]}: {w[\"connection\"][\"schema\"]}') for w in json.load(sys.stdin)]"
  ```

### `.env` file

Create a `.env` file in the project root:

```dotenv
# Required — pool projects
INTEGTEST_POOL_STORAGE_API_URL=https://connection.europe-west3.gcp.keboola.com
INTEGTEST_STORAGE_TOKENS=<token-project-A> <token-project-B>
INTEGTEST_WORKSPACE_SCHEMAS=<WORKSPACE_for_A> <WORKSPACE_for_B>

# Required — old-branches project for branch storage tests
INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES=<token-for-project-WITHOUT-storage-branches>

# Optional — second project for multi-client tests
INTEGTEST_STORAGE_TOKEN_PRJ2=<token>
INTEGTEST_WORKSPACE_SCHEMA_PRJ2=<WORKSPACE_XXX>
```

**Critical**: The order of `INTEGTEST_STORAGE_TOKENS` and `INTEGTEST_WORKSPACE_SCHEMAS`
must match — the first token is paired with the first schema, the second with the second,
etc. A mismatch causes the lock cleanup to delete the workspace (see
[Workspace deletion pitfall](#workspace-deletion-pitfall) below).

### Running the tests

```bash
source <your.venv>/bin/activate
pytest integtests/ -v --log-cli-level=INFO
```

`--log-cli-level=INFO` shows lock acquisition and release messages in real time, which is
useful when diagnosing a stall.

### Avoiding token leaks in test output

**Important for anyone writing new integration tests or fixtures.**

Pytest prints fixture parameter values in error tracebacks. If a fixture takes
`storage_api_token: str` as a parameter and the test fails, the raw token appears in
the CI log in plain text. This is a security risk.

**Bad** — token leaks on failure:
```python
@pytest.fixture
def my_fixture(storage_api_token: str, storage_api_url: str):
    # If this fixture or any test using it fails, pytest prints:
    #   storage_api_token = '2728-6118086-cHM8a1s6c1AR...'
    ...
```

**Good** — read tokens from env vars inside the function body:
```python
@pytest.fixture
def my_fixture(keboola_project, storage_api_url: str):
    # keboola_project dependency ensures env_init has run and set KBC_STORAGE_TOKEN
    token = os.environ['KBC_STORAGE_TOKEN']
    # On failure, pytest only shows: keboola_project=ProjectDef(...), storage_api_url='https://...'
    ...
```

The `env_init` fixture (triggered via `keboola_project`) copies the pool token into
`KBC_STORAGE_TOKEN` and the URL into `STORAGE_API_URL`. Reading from these env vars
inside the function body keeps the token out of pytest's traceback display.

For tokens not in the pool (e.g. `INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES`), use
`os.getenv(...)` / `os.environ[...]` directly — never accept them as fixture parameters.

---

## 2. How the Pool and Locking Work

### Overview

The pool manages a set of test projects. Each test session acquires **one** project from the
pool, runs all tests against it, then releases it. The lock ensures no two sessions use the
same project simultaneously.

**Key behavior**:
- A session acquires one project and holds it for the entire run
- The project must be **completely clean** (no buckets, configs, tables) at the start — the
  `keboola_project` fixture explicitly checks this and fails if anything exists
- After all tests complete, `keboola_project` deletes everything it created
- If a session crashes without cleaning up, the next session that acquires the same project
  will detect the stale lock and run `clean_project` to wipe everything before proceeding

### Lock storage: Keboola branch metadata

Lock state is stored in the metadata of the default branch of the test project, accessed
via the Storage API. No external service is required.

Each runner writes up to two metadata keys per acquisition attempt:

| Key | Value |
|---|---|
| `KBC.integtest.lock.<uuid>` | JSON: `lock_id`, `acquired_at` (ISO 8601 UTC), `runner_info` |
| `KBC.integtest.lock.<uuid>.released` | ISO 8601 timestamp written on release |

An entry without a corresponding `.released` key is considered active.

### Acquisition protocol

1. **Write a candidate entry** with the current UTC timestamp.
2. **Anti-collision window** — sleep 3 seconds to allow any concurrent writers to finish.
3. **Read all active entries** (those without a `.released` counterpart).
4. **Oldest timestamp wins** — the runner whose `acquired_at` is earliest holds the lock.
   Ties are broken by `lock_id` (lexicographic). If this runner's entry wins, it proceeds.
   Otherwise it writes its own `.released` key and retries after
   `INTEGTEST_LOCK_POLL_INTERVAL_SECONDS`.

### Stale lock detection and cleanup

If the winning entry is older than `INTEGTEST_LOCK_TTL_MINUTES` it is considered abandoned
(the previous runner crashed without releasing). The detecting runner:

1. Writes `.released` keys for all stale entries.
2. Calls `clean_project`: deletes all buckets (with their tables) and all component
   configurations from the project, restoring the clean state the tests require.
3. Re-enters the acquisition protocol from step 1.

If the integration tests ever take close to
60 minutes to complete, raise `INTEGTEST_LOCK_TTL_MINUTES` to roughly 2x the expected
duration — otherwise a slow-but-healthy runner may have its lock stolen mid-run.

### Workspace deletion pitfall

During `clean_project`, the lock mechanism tries to **preserve** the workspace by matching
the configured `WORKSPACE_SCHEMA` against the project's `keboola.sandboxes` configurations.
If the schema matches, that sandbox config is kept; everything else is deleted.

**If the schema does NOT match** (because the workspace was recreated, the `.env` is stale,
or the token/schema order is wrong), `clean_project` **deletes ALL sandbox configs including
the workspace itself**. This silently destroys the workspace, and subsequent tests that need
it will fail with:

```
ValueError: No Keboola workspace found or the workspace has no read-only storage access
```

**Prevention**: Always verify your token/schema mapping is correct before running tests:

```bash
# Quick check — all should show ✓
python -c "
import os, httpx
from dotenv import load_dotenv
load_dotenv()
tokens = os.getenv('INTEGTEST_STORAGE_TOKENS', '').split()
schemas = os.getenv('INTEGTEST_WORKSPACE_SCHEMAS', '').split()
url = os.getenv('INTEGTEST_POOL_STORAGE_API_URL', '')
for t, s in zip(tokens, schemas):
    info = httpx.get(f'{url}/v2/storage/tokens/verify', headers={'X-StorageApi-Token': t}).json()
    name = f'{info[\"owner\"][\"name\"]} ({info[\"owner\"][\"id\"]})'
    ws = httpx.get(f'{url}/v2/storage/branch/default/workspaces', headers={'X-StorageApi-Token': t}).json()
    ok = '✓' if any(w['connection'].get('schema') == s for w in ws) else '✗ MISMATCH'
    print(f'...{t[-4:]} | {name:>40} | {s:>22} | {ok}')
"
```

### Pool of projects

`ProjectPool` holds a list of `ProjectEndpoint` objects. During each acquisition pass it
tries them in randomized order and returns the first one it can lock. If all are busy it
sleeps for `INTEGTEST_LOCK_POLL_INTERVAL_SECONDS` and retries the full list. Raising the
pool size allows multiple CI jobs to run concurrently, each against a different project.

Before the pool is created, `verify_project_endpoint` calls `GET /v2/storage/tokens/verify`
for every configured token. A revoked or misspelled token therefore causes an immediate
`pytest.fail` rather than a confusing mid-run error.

### Fixture dependency chain

`project_lock` is a session-scoped fixture. `storage_api_token` and `workspace_schema`
derive their values from the acquired endpoint, so every fixture that touches the project
is automatically blocked until the lock is held:

```
env_file_loaded
  └── storage_api_url             <-- reads INTEGTEST_POOL_STORAGE_API_URL
        └── project_lock         <-- lock acquired at session start
              ├── storage_api_token
              └── workspace_schema
                    └── env_init
                          └── keboola_project   (creates test buckets, tables, configs)
```

The lock is released in `project_lock`'s teardown, after every session-scoped fixture that
depends on it has been torn down and the project has been cleaned up by `keboola_project`.

### Lock tuning

The defaults work for both local and CI use. Override only if you have a reason to.

| Variable | Default | Meaning |
|---|---|---|
| `INTEGTEST_LOCK_TTL_MINUTES` | `60` | A lock older than this is considered abandoned and cleaned up by the next runner that detects it |
| `INTEGTEST_LOCK_POLL_INTERVAL_SECONDS` | `30` | How long to wait between retries when all projects in the pool are busy |
| `INTEGTEST_LOCK_MAX_WAIT_MINUTES` | `90` | Raise `TimeoutError` after this many minutes of waiting |

---

## 3. Test-Specific Projects

### Old-branches project (branch storage tests)

The branch storage tests (`test_storage_branches.py`) validate the deference mechanism on
both `storage-branches` and old-style branch projects. They run automatically on whatever
pool project is acquired, and additionally on a dedicated old-branches project:

```dotenv
INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES=<master-token-of-a-project-WITHOUT-storage-branches-feature>
```

The test fails if this variable is not set. The project must **not** have the
`storage-branches` feature enabled (the pool projects are expected to have it).

Production data (`in.c-test_bucket_01` with `test_table_01`) is created idempotently
in this project and left in place between runs. Only branches are created and cleaned up
per session, so multiple concurrent sessions can safely share the project.

No workspace schema is needed — these tests only exercise bucket/table listing, not
`query_data`.

### Second project (two-project tests)

A small number of tests exercise simultaneous access to two separate projects. They need:

```dotenv
INTEGTEST_STORAGE_TOKEN_PRJ2=<master-token-of-second-test-project>
INTEGTEST_WORKSPACE_SCHEMA_PRJ2=<workspace-schema-of-second-project>
```

These are read directly without locking and can be omitted if you are not running those
tests. No lock is needed because the only test that uses PRJ2
(`test_http_multiple_clients_with_different_headers`) is strictly read-only against it —
it calls `list_tools`, `list_resources`, and `get_project_info`, and never creates,
modifies, or deletes any object. Any future test that writes to PRJ2 must acquire a lock
for it first.

### Metastore tests

A subset of tests exercises the Metastore API. The Metastore URL is derived automatically
from `INTEGTEST_POOL_STORAGE_API_URL` by replacing the `connection.` prefix with `metastore.`
(e.g. `https://connection.north-europe.azure.keboola.com` -> `https://metastore.north-europe.azure.keboola.com`).
Authentication reuses the storage API token — no additional environment variables are needed.

---

## 4. Migrating from the Old Setup

The old system used two single-value variables. Rename them in your `.env`:

| Old variable | New variable | Value |
|---|---|---|
| `INTEGTEST_STORAGE_TOKEN` | `INTEGTEST_STORAGE_TOKENS` | same token, no change |
| `INTEGTEST_WORKSPACE_SCHEMA` | `INTEGTEST_WORKSPACE_SCHEMAS` | same schema, no change |

The old names are no longer read. The session fails immediately at startup if either new
variable is missing or empty.

`INTEGTEST_STORAGE_TOKEN_PRJ2` and `INTEGTEST_WORKSPACE_SCHEMA_PRJ2` are unchanged.

---

## 5. GitHub CI Setup

### Pool projects

The CI pool consists of four Keboola projects, all on
`https://connection.europe-west3.gcp.keboola.com`:

| Project ID | Dashboard URL                                                                 | Backend   | Notes        |
|------------|-------------------------------------------------------------------------------|-----------|--------------|
| 2728       | https://connection.europe-west3.gcp.keboola.com/admin/projects/2728/dashboard | Snowflake |              |
| 2729       | https://connection.europe-west3.gcp.keboola.com/admin/projects/2729/dashboard | Snowflake |              |
| 2731       | https://connection.europe-west3.gcp.keboola.com/admin/projects/2731/dashboard | BigQuery  |              |
| 2732       | https://connection.europe-west3.gcp.keboola.com/admin/projects/2732/dashboard | BigQuery  |              |

Having four slots means up to four CI jobs can run concurrently — each acquires a
different project from the pool and they do not block each other.

### Old-branches project (not in pool)

| Project ID | Dashboard URL                                                                 | Backend   | Notes                              |
|------------|-------------------------------------------------------------------------------|-----------|------------------------------------|
| 2906       | https://connection.europe-west3.gcp.keboola.com/admin/projects/2906/dashboard | Snowflake | No `storage-branches` feature      |

This project is used by `test_storage_branches.py` via `INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES`.
It is **not** part of the pool and has no lock mechanism — concurrent access is safe because
production data is created idempotently and each session only manages its own branches.

### Secrets and variables

The `integration_tests` job in `.github/workflows/ci.yml` reads the following from the
repository's GitHub Secrets/Variables:

| Name | Kind | Purpose |
|---|---|---|
| `INTEGTEST_STORAGE_TOKENS` | Secret | Space-separated master tokens for all four pool projects (order must match `INTEGTEST_WORKSPACE_SCHEMAS`) |
| `INTEGTEST_POOL_STORAGE_API_URL` | Variable | `https://connection.europe-west3.gcp.keboola.com` |
| `INTEGTEST_WORKSPACE_SCHEMAS` | Variable | Space-separated Snowflake workspace schemas, one per project in the same order as the tokens |
| `INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES` | Secret | Master token for a project **without** the `storage-branches` feature (used by `test_storage_branches.py`) |

### Concurrency

- **Within a single CI run** — the matrix covers Python 3.10, 3.11, and 3.12 with
  `max-parallel: 1`, so the three versions run sequentially. This is intentional: a
  single run only needs one project slot, not three.
- **Across concurrent CI runs** — the project-pool locking protocol (described in
  section 2) handles collisions. Each run acquires a different project, so up to four
  runs can proceed in parallel without interfering.
- **Duplicate-run prevention** — a workflow-level concurrency key
  (`ci-${{ github.ref }}`) cancels any in-progress run on the same branch when a new
  push arrives.

### Fork behaviour

Integration tests are skipped for pull requests from forks
(`github.repository != github.event.repository.full_name`) because forks do not have
access to the repository secrets. The dependencies are still installed so the
environment setup can be validated.