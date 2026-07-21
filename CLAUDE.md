# Keboola MCP Server - Project Guide

## Contributing Guidelines

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the authoritative contributor guide. It covers:

- **RFC requirement** — when a design doc in `feature_spec/<feature-name>/RFC.md` is required before implementation (new tools, new architectural concepts, new end-to-end behavior), and the required RFC structure
- **Testing requirements** — regression tests for bug fixes, unit + E2E tests for features, and parametrize conventions
- **PR checklist** — the full set of items every PR must address (branch naming, version bump, `uv.lock`, `tox`, RFC link, etc.)

The rules in this file (git workflow, versioning, venv setup) complement `CONTRIBUTING.md` — when the two overlap, `CONTRIBUTING.md` is the source of truth for contributor-facing process. Read it before opening a PR.

## Git Workflow
- **Always create a branch first** before committing changes
- Branch names must start with the Linear issue ID and be short (e.g., `AI-2480-whitelist-n8n-domains`)
  - **Exception:** release branches use `release/vX.Y.Z` (e.g., `release/v1.55.0`) — these have no Linear issue
- Commit messages must **start** with the Linear issue ID (e.g., `AI-2480: description`)
- When working on a Linear task, **check the current branch first** (`git branch`). If not already on the correct task branch, create one before making any changes: `git checkout -b AI-XXXX-short-description`
- When creating PRs, use the template at `.github/pull_request_template.md`
- **Every PR must include a `pyproject.toml` version bump** — bump before merging; see [Versioning](#versioning) for the rules
- **Prefer rebasing onto `main`** to keep a linear history. Rebasing your own feature/PR branch and force-pushing the result is allowed and expected — always use `git push --force-with-lease` (never a bare `git push --force`) so you never clobber commits someone else pushed. Do not rebase a branch that others are actively committing to.

## Mapping a Docker Image Tag to a Version

Images on Docker Hub (`keboola/mcp-server`) are tagged `production-<full-git-sha>` (or `canary-orion-<sha>`, `dev-<sha>`, etc.) depending on which git tag triggered the build — see [Releasing](#releasing) for the tag → stack mapping. To resolve a tag to a release version and check whether it's the latest deployed image, don't guess — run:

```bash
# 1. Which commit + version is in the image? (sha = part after "production-")
git fetch origin main
git log --oneline -1 <sha>
git show <sha>:pyproject.toml | grep -m1 '^version'

# 2. Is anything newer already merged to main but not in the image?
git log --oneline <sha>..origin/main

# 3. Is it the latest production image on Docker Hub? (tags sorted newest-first)
curl -s "https://hub.docker.com/v2/repositories/keboola/mcp-server/tags/?page_size=20&name=production" |
  python3 -c "import sys,json; [print(t['name'], t['last_updated']) for t in json.load(sys.stdin)['results']]"
```

Notes:
- The remote default branch is **`main`** (a local clone may have a stale `master` ref — always fetch and compare against `origin/main`).
- The image version is whatever `pyproject.toml` said **at that commit**; the latest production tag can lag behind `main` HEAD (merged but not yet deployed).

## Testing
- **All tox checks must pass before pushing** — CI runs the same checks (pytest, black, isort, flake8, check-tools-docs) and will fail the build if any of them fail
- **Use tox** for final testing - it runs pytest, black (formatting), isort (import ordering), flake8 (linter), and check-tools-docs (verifies TOOLS.md is up-to-date)
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
All five tox environments (pytest, black, isort, flake8, check-tools-docs) should exit 0.

## Integration Tests

See `integtests/README.md` for setup and conventions.

## Local End-to-End Testing with MCP

For manual end-to-end testing, you can set up a local MCP server in a `.mcp.json` file
in the project root (it is already in `.gitignore`). Point it to the venv's Python
interpreter which has the package installed in editable mode — this ensures the server
runs your local source code. Placing the `.mcp.json` in the project root allows you to
test from the same Claude Code (or other MCP client) session where you develop, with the
server always reflecting your latest code changes:

```json
{
  "mcpServers": {
    "keboola-local": {
      "command": "<absolute-path-to-project>/.venv/bin/python",
      "args": ["-m", "keboola_mcp_server"],
      "env": {
        "KBC_STORAGE_API_URL": "https://connection.<stack>.keboola.com",
        "KBC_STORAGE_TOKEN": "<your-token>",
        "KBC_BRANCH_ID": "<optional-branch-id>"
      }
    }
  }
}
```

- Use the **absolute path** to the venv Python — relative paths or bare `python` may pick up
  a different interpreter that doesn't have your local edits.
- After making code changes, the MCP server must be **reloaded** to pick them up.
- If the editable install is stale (e.g. after `git pull` with dependency changes), run
  `uv sync --active --extra dev --extra tests` to update it.
- Do not commit tokens.

## Versioning

- **Every PR must bump `pyproject.toml` version** before merging.
- Use semantic versioning:
  - **Patch** (`1.x.y` → `1.x.y+1`): bug fixes, refactoring, docs, tests, chores
  - **Minor** (`1.x.y` → `1.x+1.0`): new features, new tools, new capabilities
  - **Major**: breaking API/protocol changes (rare)
- After bumping, always sync the lock file: `uv lock`
- Commit the version bump and `uv.lock` change together (can be a separate commit or bundled with
  the main feature commit).

## Releasing

- We **do not release every version**. Changes land on the trunk (`main`) continuously; we
  release periodically once the accumulated changes have been re-tested together, so we don't
  break working setups for users.
- A release is one or two git tags pushed to `origin`:
  - `vX.Y.Z` — MCP server release (always)
  - `agent-vX.Y.Z` — In Platform Agent release (only when releasing the agent as well)
- Either tag triggers `release.yml` CI (builds/publishes the Docker image). KaiBench runs only on
  production `vX.Y.Z` tags — not `agent-vX.Y.Z`, and not the canary/dev tags below.
- `release.yml` maps git tags to image tags and deployment stacks as follows:

  | Git tag pushed | Image tag built | Helm chart | Deployed to |
  |---|---|---|---|
  | `vX.Y.Z` | `production-<sha>` (+ `latest`) | `mcp-server` | production stacks |
  | `agent-vX.Y.Z` | `production-<sha>` | `mcp-server-agent` | production stacks |
  | `canary-orion-vX.Y.Z-dev.N` | `canary-orion-<sha>` | `mcp-server` | canary-orion stacks |
  | `canary-orion-agent-vX.Y.Z-dev.N` | `canary-orion-<sha>` | `mcp-server-agent` | canary-orion stacks |
  | `dev-vX.Y.Z-dev.N` | `dev-<sha>` | `mcp-server` | testing stacks |
  | `dev-agent-vX.Y.Z-dev.N` | `dev-<sha>` | `mcp-server-agent` | testing stacks |

  The stack routing (which physical stacks a `canary-orion-`/`dev-`/`production-` image tag lands on)
  is configured on the `keboola/kbc-stacks` side; this repo only builds the image and triggers the
  tag update.
- Use the **`release-notes` skill** to prepare a release — it generates the release notes, opens
  the draft `release/vX.Y.Z` PR, and walks through tagging both `vX.Y.Z` and `agent-vX.Y.Z`.

## Security Considerations
- When whitelisting domains in OAuth, prefer **explicit domain lists over regex patterns**
- Regex could unintentionally allow future domains that weren't reviewed (principle of least privilege)
