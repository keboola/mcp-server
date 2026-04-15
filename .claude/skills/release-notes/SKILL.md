# Release Notes Skill

Prepare release notes for a new Keboola MCP Server version as a draft PR.

## Overview

1. **Ask the user two questions before doing anything else** (see Step 0)
2. Collect all merged PRs since the last production tag via GitHub API
3. Categorise PRs (breaking changes, new features, bug fixes, internal)
4. Create a draft PR using the project's PR template, with release notes in the Summary section
5. Testing (manual + KaiBench locally) is done before the PR is merged and the release tag is pushed

Releases are **not** created via `gh release create` — the release tag is pushed after the PR is reviewed, tested, and merged.

---

## Step 0: Ask the user — REQUIRED before any other action

Use `AskUserQuestion` with **both questions in a single prompt**:

> 1. **What version are we releasing?** (e.g. `1.55.0`)
> 2. **Is this a single MCP release or also an agent release?**
>    - MCP only (`v1.55.0`)
>    - Both MCP and agent (`v1.55.0` + `agent-v1.55.0`)

Do not proceed until the user answers. Use their answer as `NEW_VERSION` and `RELEASE_TYPE` throughout the rest of the steps.

---

## Step 1: Find base tag

```bash
# Latest production release (no -dev., no agent-v)
BASE_TAG=$(git tag --list 'v*' | grep -v '\-dev\.' | grep -v 'agent-v' | sort -V | tail -1)
echo "Base: $BASE_TAG"
echo "Releasing: v$NEW_VERSION  (type: $RELEASE_TYPE)"
```

## Step 2: Collect merged PRs since base tag

```bash
# Via git log (quick overview)
git log ${BASE_TAG}..HEAD --oneline --merges | grep "Merge pull request"

# Via GitHub API (titles + authors — use to write the release notes body)
BASE_DATE=$(git log -1 --format=%ci $BASE_TAG | cut -d' ' -f1)
gh api "repos/keboola/mcp-server/pulls?state=closed&sort=updated&direction=desc&per_page=100" \
  --jq ".[] | select(.merged_at != null and .merged_at > \"${BASE_DATE}\") | {number, title, user: .user.login, merged_at}"
```

## Step 3: Categorise

| Category | Include in release notes? | Examples |
|---|---|---|
| **Breaking changes** | Yes, prominently | Transport removal, API shape changes |
| **New features** | Yes | New tools, new parameters |
| **Bug fixes / improvements** | Yes | Correctness, UX, perf |
| **CI / internal** | No | KaiBench, integtest, dev tooling |
| **Docs only** | No | REVIEW.md, CLAUDE.md |
| **Chores** | No | Dependency bumps, lint |

## Step 4: Create the draft PR

Create the branch `release/vX.Y.Z` from main and open a draft PR. Write the release notes **directly in the PR body** using the project PR template — no separate files needed.

```bash
git checkout -b release/vX.Y.Z
git push -u origin release/vX.Y.Z

gh pr create --draft \
  --title "chore: release vX.Y.Z" \
  --body "..."
```

### PR body format

Fill in the project PR template:

```markdown
## Description

**Linear**: N/A

### Change Type

- [ ] Major (breaking changes, significant new features)
- [x] Minor (new features, enhancements, backward compatible)
- [ ] Patch (bug fixes, small improvements, no new features)

### Summary

Release vX.Y.Z. Changes since vA.B.C:

---

#### ⚠️ Breaking Changes

- **<Title>**: <What changed and what users must do to migrate>.

#### New Features

- **<Title>**: <What the feature does and what problem it solves>.

#### Enhancements

- **<Title>**: <What improved and the observable effect>.

#### Bug Fixes

- **<Title>**: <What was broken and what is now correct>.

---

## Testing

- [ ] Tested with Cursor AI desktop (`Streamable-HTTP` transports)

### Optional testing
- [ ] Tested with Cursor AI desktop (all transports)
- [ ] Tested with claude.ai web and `canary-orion` MCP (`Streamable-HTTP`)
- [ ] Tested with In Platform Agent on `canary-orion`
- [ ] Tested with RO chat on `canary-orion`
- [ ] KaiBench run locally against the canary image

## Checklist

- [ ] Self-review completed
- [ ] Unit tests added/updated (if applicable)
- [ ] Integration tests added/updated (if applicable)
- [x] Project version bumped according to the change type (if applicable)
- [ ] Documentation updated (if applicable)
```

Write bullet points as **plain prose — no PR numbers, no PR links**. Describe what the change does and why it matters to users.

## Step 5: Testing before merge

Before marking the PR ready and merging, check off all applicable items in the Testing section of the PR. KaiBench in CI runs **only** on proper release tags (e.g. `v1.55.0`), not on dev/canary tags.

## Step 6: Merge and tag

After PR is approved and all tests pass:

```bash
# Merge PR via GitHub UI, then:
git checkout main && git pull

# Always tag the MCP release
git tag vX.Y.Z && git push origin vX.Y.Z

# If the user confirmed an agent release in Step 0, also push:
git tag agent-vX.Y.Z && git push origin agent-vX.Y.Z
```

Both tags trigger the same `release.yml` CI — each builds and publishes the Docker image.
KaiBench evaluation runs automatically as part of release CI (production tags only, not dev tags).

---

## Tips

- PRs touching only `.github/`, `tests/`, `integtests/`, `CLAUDE.md`, or bumping dev deps → internal, skip in release notes.
- If a feature was built across multiple PRs, group them as a single bullet describing the end result.
- Always call out breaking changes with `⚠️` both in the PR body and at the top of the Summary.
