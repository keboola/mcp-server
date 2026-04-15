---
name: release-notes
description: Prepares release notes and a draft PR for a new Keboola MCP Server version. Use when the user asks to prepare a release, create release notes, or tag a new version.
disable-model-invocation: true
---

**Before doing anything**, ask the user two questions in a single prompt:

1. **What version are we releasing?** (e.g. `1.55.0`)
2. **MCP only or also agent?** — MCP only (`v1.55.0`) or both MCP and agent (`v1.55.0` + `agent-v1.55.0`)

Then follow these steps:

**1. Find the base tag**
```bash
BASE_TAG=$(git tag --list 'v*' | grep -v '\-dev\.' | grep -v 'agent-v' | sort -V | tail -1)
```

**2. Collect merged PRs since that tag**
```bash
BASE_DATE=$(git log -1 --format=%ci $BASE_TAG | cut -d' ' -f1)
gh api "repos/keboola/mcp-server/pulls?state=closed&sort=updated&direction=desc&per_page=100" \
  --jq ".[] | select(.merged_at != null and .merged_at > \"${BASE_DATE}\") | {number, title, user: .user.login, merged_at}"
```

**3. Categorise** — include: breaking changes, new features, enhancements, bug fixes. Skip: CI/internal, docs-only, dependency bumps, integtest changes.

**4. Create branch and draft PR** from `main`:
```bash
git checkout -b release/vX.Y.Z
git push -u origin release/vX.Y.Z
gh pr create --draft --title "chore: release vX.Y.Z" --body "..."
```

The PR body uses the project PR template. Fill the Summary section with the release notes — **plain prose only, no PR numbers or links**:

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

**5. After PR is approved and tested**, merge via GitHub UI then tag:
```bash
git checkout main && git pull
git tag vX.Y.Z && git push origin vX.Y.Z
# if agent release confirmed in step 0:
git tag agent-vX.Y.Z && git push origin agent-vX.Y.Z
```

Pushed tags trigger `release.yml` CI which builds and publishes the Docker image. KaiBench runs automatically — only on production tags, not dev tags.
