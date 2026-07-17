---
name: recreate-dependabot-pr
description: Recreate stale Dependabot PR(s) for keboola/mcp-server as a fresh branch from main, producing a clean, conflict-free PR. This repo manages Python deps via uv (pyproject.toml + uv.lock) and GitHub Actions, so updates are applied with `uv lock --upgrade-package` and workflow edits — not composer. Use when Dependabot PRs are stale, conflict, or you want to batch several bumps + security alerts into one PR. Dependabot auto-closes its original PRs once the bump lands on main, so this skill does not close them.
---

# Recreate Dependabot PR (keboola/mcp-server)

Recreate stale/conflicting Dependabot PRs as one clean branch from `main`. This is a
Python/uv repo: dependencies live in `pyproject.toml` (`dependencies` / optional groups)
and are locked in `uv.lock`; CI is GitHub Actions. There is **no composer** here.

Two modes:
- **Single** — recreate one PR (mirrors the connection skill; re-invoke for the next).
- **Batch** (preferred here) — every bump touches `uv.lock` *and* requires a
  `pyproject.toml` version bump, so N separate PRs mutually conflict. Fold all open
  Dependabot PRs + open security alerts into **one** PR.

> **We do NOT close the original Dependabot PRs.** Once the equivalent bump is merged
> to `main`, Dependabot detects it and closes its own PRs automatically. Do not
> `gh pr close` them.

## Workflow

### Step 1: List the work

```bash
# Open Dependabot PRs
gh pr list --repo keboola/mcp-server --author "app/dependabot" \
  --json number,title,headRefName,url --limit 50
# Open security alerts (these may have NO corresponding PR — often transitive deps)
gh api repos/keboola/mcp-server/dependabot/alerts \
  --jq '.[] | select(.state=="open") | {number, pkg:.dependency.package.name,
        severity:.security_advisory.severity, scope:.dependency.scope,
        first_patched:.security_vulnerability.first_patched_version.identifier}'
```

If nothing is open, tell the user and stop.

### Step 2: Classify each package

For each PR title `"bump <package> from <old> to <new>"` and each security alert,
determine how it must be updated:

- **Direct dep pinned in `pyproject.toml` with `==`** (e.g. `fastmcp == 3.4.2`) →
  the pin must be **edited** to the new version, then re-locked.
- **Direct dep with a compatible-release range** (`~= X.Y`) → if `<new>` is already
  inside the range, no pyproject edit is needed — just re-lock. If `<new>` exceeds the
  range (e.g. `cryptography ~= 48.0` → `49.0.0`), **edit** the range (`~= 49.0`).
- **Transitive dep** (appears only in `uv.lock`, not in `pyproject.toml`; typical for
  security alerts like `urllib3`, `pydantic-settings`) → no pyproject edit; bump via
  `uv lock --upgrade-package <pkg>`.
- **GitHub Action** (ecosystem `github-actions`, e.g. `actions/checkout 6 → 7`) →
  edit every `uses:` line under `.github/workflows/`. No lock file involved.

### Step 3: Fresh branch from main

```bash
git fetch origin main
git checkout -b chore/deps-security-batch origin/main   # or chore/bump-<pkg> for single
```

If the branch already exists locally or remote, tell the user and **stop** (don't force-delete).

### Step 4: Apply pyproject + workflow edits

Edit the `==` pins and out-of-range `~=` ranges identified in Step 2, and any workflow
`uses:` lines. **Also bump the `pyproject.toml` `version`** (required on every PR;
dependency updates are a *patch* bump per the versioning rules).

### Step 5: Re-lock

```bash
uv lock --upgrade-package <pkgA> --upgrade-package <pkgB> ...
```

List every changed package (direct + transitive security deps). Note the repo sets
`exclude-newer = "7 days"` in `[tool.uv]` — if a required security version was published
within the last 7 days, `uv lock` will refuse it. Only then, relax it for that run:

```bash
uv lock --exclude-newer $(date -u +%Y-%m-%dT%H:%M:%SZ) --upgrade-package <pkg>
```

Confirm the resolved versions satisfy the security alerts' `first_patched` (a `~=` range
or the resolver may land on a version **newer** than the Dependabot target — that's fine,
mention it in the PR body).

### Step 6: Verify with tox

CI runs the same checks, so they must pass before pushing. If no venv exists, create one
(see project CLAUDE.md — Python 3.10):

```bash
python3.10 -m venv 3.10.venv && source 3.10.venv/bin/activate
pip install -q --upgrade pip uv && uv sync --active --extra dev --extra tests
tox    # pytest, black, flake8, check-tools-docs — all must exit 0
```

If tests fail because a new major version changed behavior, that package needs manual
review — split it out of the batch and tell the user.

### Step 7: Commit

```bash
git add pyproject.toml uv.lock .github/workflows/
git commit -m "chore(deps): bump dependencies and resolve security alerts"
```

No `Co-Authored-By` trailer — mechanical dependency update, not authored code.

### Step 8: Push + create PR

```bash
git push -u origin chore/deps-security-batch
```

Fill `.github/pull_request_template.md` for a dependency update (Linear: `-`; routine
update, no API/behavioral changes; standard deployment; rollback = deploy previous
version). List each bump and the security alerts (with `Fixes`/`Supersedes #<n>` for the
Dependabot PRs) in the body, then:

```bash
gh pr create --repo keboola/mcp-server --draft \
  --title "chore(deps): dependency bumps + security alerts" \
  --label dependencies --body "<filled-template>"
```

If `gh pr create` fails, show the error + branch URL for a manual PR.

### Step 9: Do NOT close the originals

Skip closing the Dependabot PRs — merging the batch to `main` makes Dependabot close them
automatically. Just note in the PR body which PR numbers it supersedes.

## Error Handling

| Situation | Action |
|---|---|
| Nothing open | Tell user, stop |
| Branch already exists | Tell user, stop (don't force-delete) |
| `uv lock` refuses a security version (cooldown) | Re-run with `--exclude-newer <now>` for that package |
| Resolved newer than Dependabot target | Fine — use resolved version, note it in PR body |
| Major-version bump breaks tests | Split that package out, flag for manual review |
| `tox` fails | Fix or split the offending bump; never push red |
| `gh pr create` fails | Show error + branch URL for manual PR |
