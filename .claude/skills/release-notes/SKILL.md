---
name: release-notes
description: Prepares release notes and tags for a new Keboola MCP Server version. Use when the user asks to prepare a release, create release notes, or tag a new version.
disable-model-invocation: true
---

**Before doing anything**, ask the user three questions in a single prompt:

1. **What version are we releasing?** (e.g. `1.55.0`)
2. **MCP only or also agent?** — MCP only (`v1.55.0`) or both MCP and agent (`v1.55.0` + `agent-v1.55.0`)
3. **Release mode** — tag directly from `main` or create a release PR first?

Then follow the matching path below.

---

## Path A — Tag directly from `main`

**1. Find the base tag**
```bash
BASE_TAG=$(git tag --list 'v*' | grep -v '\-dev\.' | grep -v 'agent-v' | sort -V | tail -1)
```

**2. Collect merged PRs since that tag**
```bash
BASE_TIMESTAMP=$(git log -1 --format=%cI $BASE_TAG)
gh api --paginate "repos/keboola/mcp-server/pulls?state=closed&sort=updated&direction=desc&per_page=100" \
  --jq ".[] | select(.merged_at != null and .merged_at > \"${BASE_TIMESTAMP}\") | {number, title, user: .user.login, merged_at}"
```

**3. Categorise** — include: breaking changes, new features, enhancements, bug fixes. Skip: CI/internal, docs-only, dependency bumps, integtest changes.

**4. Present the draft release notes** to the user in this format — **plain prose only, no PR numbers or links**:

```
Release vX.Y.Z — changes since vA.B.C:

#### ⚠️ Breaking Changes
- **Title**: What changed and what users must do to migrate.

#### New Features
- **Title**: What it does and what problem it solves.

#### Enhancements
- **Title**: What improved and the observable effect.

#### Bug Fixes
- **Title**: What was broken and what is now correct.
```

Omit any section that has no entries.

**5. Ask the user:** "Ready to push tags?" — wait for explicit confirmation before proceeding.

**6. Create and push tags** (only after confirmation):
```bash
PREV_BRANCH=$(git branch --show-current)
git checkout main && git pull
git tag vX.Y.Z
# if agent release confirmed in step 0:
git tag agent-vX.Y.Z
git push origin vX.Y.Z
# if agent release:
git push origin agent-vX.Y.Z
git checkout "$PREV_BRANCH"
```

---

## Path B — Release PR first, then tag

**1–3.** Same as Path A steps 1–3 (find base tag, collect PRs, categorise).

**4. Present the draft release notes** (same format as Path A step 4).

**5. Create the release branch and draft PR** from `main`:

> Release branches use `release/vX.Y.Z` naming — explicit exception to the Linear-ID-prefix
> convention in `CLAUDE.md`.

```bash
git checkout main && git pull
git checkout -b release/vX.Y.Z
git push -u origin release/vX.Y.Z
gh pr create --draft --title "chore: release vX.Y.Z" --body "..."
```

The PR body uses the project PR template. Fill the Summary section with the release notes:

```markdown
## Description

**Linear**: N/A

### Change Type
- [ ] Major / [x] Minor / [ ] Patch

### Summary

Release vX.Y.Z. Changes since vA.B.C:

#### ⚠️ Breaking Changes
- **Title**: What changed and what users must do to migrate.

#### New Features
- **Title**: What it does and what problem it solves.

#### Enhancements
- **Title**: What improved and the observable effect.

#### Bug Fixes
- **Title**: What was broken and what is now correct.

## Testing
- [ ] Tested with Cursor AI desktop (`Streamable-HTTP` transports)
- [ ] Tested with claude.ai web and `canary-orion` MCP (`Streamable-HTTP`)
- [ ] Tested with In Platform Agent on `canary-orion`
- [ ] Tested with RO chat on `canary-orion`
- [ ] KaiBench run locally against the canary image

## Checklist
- [ ] Self-review completed
- [x] Project version bumped according to the change type (if applicable)
```

**6. After PR is approved and merged**, ask the user: "Ready to push tags?" — wait for explicit confirmation before proceeding.

**7. Create and push tags** (only after confirmation):
```bash
PREV_BRANCH=$(git branch --show-current)
git checkout main && git pull
git tag vX.Y.Z
# if agent release confirmed in step 0:
git tag agent-vX.Y.Z
git push origin vX.Y.Z
# if agent release:
git push origin agent-vX.Y.Z
git checkout "$PREV_BRANCH"
```

---

Pushed tags trigger `release.yml` CI which builds and publishes the Docker image. KaiBench runs automatically — only on production tags, not dev tags.
