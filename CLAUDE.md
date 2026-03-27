# Keboola MCP Server - Project Guide

## Git Workflow
- **Always create a branch first** before committing changes
- Branch names must start with the Linear issue ID and be short (e.g., `AI-2480-whitelist-n8n-domains`)
- Commit messages must **start** with the Linear issue ID (e.g., `AI-2480: description`)
- When working on a Linear task, **check the current branch first** (`git branch`). If not already on the correct task branch, create one before making any changes: `git checkout -b AI-XXXX-short-description`
- When creating PRs, use the template at `.github/pull_request_template.md`
- **Never use `git push --force`** or rebase commits that have already been pushed - use merge commits instead to avoid rewriting history for others

## Testing
- **All tox checks must pass before pushing** — CI runs the same checks (pytest, black, flake8, check-tools-docs) and will fail the build if any of them fail
- **Use tox** for final testing - it runs pytest, black (formatting), flake8 (linter), and check-tools-docs (verifies TOOLS.md is up-to-date)
- It's OK to use pytest directly for running individual tests during development
- Activate the virtual environment first (e.g., `source <venv>/bin/activate`)
- Run specific tests: `tox -e py310 -- tests/test_file.py -v`
- Run all checks: `tox`
- **Write parameterized tests** (`@pytest.mark.parametrize`) to reduce boilerplate; declare parameter names as a tuple of strings, not a single comma-separated string (e.g. `('a', 'b')` not `'a, b'`)
- **Be careful with mocking** - don't mock too much or tests will just test the mocks, not the real code
- **Extend existing tests instead of adding new ones** - when adding new scenarios (e.g. OAuth bearer token cases), add parameters to an existing parametrized test rather than writing a separate test function; this avoids test bloat and keeps related cases together
- **Only test what's necessary** - add test cases that cover genuinely new behavior, not duplicates of cases already covered by existing parametrize entries

## Virtual Environments
- Look for a venv folder in the project root (e.g., `3.10.venv/`, `.venv/`) that contains an editable install of the project, or ask the user which venv to use
- Activate the venv before running tox or uv commands
- After version bump in `pyproject.toml`, sync lock file: `uv lock` (no `--active` flag — unlike `uv sync`, `uv lock` does not accept it)

## Setting Up a Fresh Clone
Run these steps once after cloning the repository:
```bash
# 1. Create virtual environment (requires Python 3.10)
python3.10 -m venv 3.10.venv

# 2. Activate and install uv
source 3.10.venv/bin/activate
pip install --upgrade pip uv

# 3. Sync all dependencies from the lock file
#    --active is required so uv installs into the already-activated venv
uv sync --active --extra dev --extra tests

# 4. Verify everything works
tox
```
All four tox environments (pytest, black, flake8, check-tools-docs) should exit 0.

## Integration Tests — Adding a New Client

- **Derive service URLs from existing stack env vars** — do NOT introduce new `INTEGTEST_<SERVICE>_URL`
  env vars. All services on a stack share the same base domain; derive the URL by replacing the
  `connection.` prefix in `INTEGTEST_POOL_STORAGE_API_URL`.
  Example: `storage_api_url.replace('connection.', 'metastore.', 1)`
- **Reuse the storage API token** — services on the same stack accept the same token. Only add a
  separate token env var if the service uses a different auth scheme that cannot accept the storage token.
- This keeps CI configuration minimal (no new secrets per service) and avoids drift between
  integration test fixtures and the actual stack topology.

## Versioning

- **Every PR must bump `pyproject.toml` version** before merging.
- Use semantic versioning:
  - **Patch** (`1.x.y` → `1.x.y+1`): bug fixes, refactoring, docs, tests, chores
  - **Minor** (`1.x.y` → `1.x+1.0`): new features, new tools, new capabilities
  - **Major**: breaking API/protocol changes (rare)
- After bumping, always sync the lock file: `uv lock`
- Commit the version bump and `uv.lock` change together (can be a separate commit or bundled with
  the main feature commit).

## Security Considerations
- When whitelisting domains in OAuth, prefer **explicit domain lists over regex patterns**
- Regex could unintentionally allow future domains that weren't reviewed (principle of least privilege)
