# Contributing to Keboola MCP Server

## Before You Start

Check the [Linear board](https://linear.app/keboola) for an existing issue, or create one.
Branch names must start with the Linear issue ID (e.g. `AI-1234-short-description`).
See [CLAUDE.md](CLAUDE.md) for the full git workflow, versioning rules, and testing setup.

---

## RFC Requirement

**Any PR that adds or changes more than ~200 lines of non-test code requires an RFC to be approved before implementation begins.**
PRs without an RFC will not be accepted when they exceed this threshold.

### When an RFC is required

| Change type | RFC required? |
|---|---|
| New feature ≥ 200 lines | **Yes** |
| Significant refactor ≥ 200 lines | **Yes** |
| Bug fix (any size) | No |
| Small enhancement / existing code | No |
| Docs, tests, chores | No |

### Writing an RFC

Create `feature_spec/<feature-name>/RFC.md` before writing implementation code.
Look at existing RFCs for structure and depth:

- [`feature_spec/branched_storage_support/RFC.md`](feature_spec/branched_storage_support/RFC.md)
- [`feature_spec/project_info_branch_context/RFC.md`](feature_spec/project_info_branch_context/RFC.md)

An RFC must cover at minimum:

```
# RFC: <Title>

Linear: [AI-XXXX](https://linear.app/keboola/issue/AI-XXXX/...)

## Problem
What is broken or missing, and what is the visible symptom?

## Required Behavior
Precise description of what the system must do after the change.
Use tables / bullet points for clarity.

## Resolution Strategy
How you plan to implement it. Reference specific files and functions.
Call out any non-obvious trade-offs.

## Scope
What is in scope and what is explicitly out of scope.

## Testing / Verification
How to verify correctness — unit tests, integration tests, manual steps.
```

Get the RFC reviewed and agreed on before writing implementation code.

---

## Testing Requirements

### Bug fixes

Every bug fix must include:

- **A regression test** that fails on the unfixed code and passes after the fix.
- **Ideally an E2E test** (integration test or manual scenario in the PR description) that
  demonstrates both the incorrect behavior and the corrected behavior.

Without a regression test the bug is likely to resurface silently.

### Features

- **Unit tests are required.** Keep mocks minimal — mocking too much means you are testing
  the mocks, not the real code. Prefer testing real interactions.
- **E2E / integration tests are required** when the feature exposes new external behavior
  (new tool, new tool parameter, changed tool response shape).
- **Extend existing parametrized tests** rather than adding new test functions for related
  scenarios. Use `@pytest.mark.parametrize` with a new parameter axis.
- Only add test cases that cover genuinely new behavior — do not duplicate cases already
  handled by existing parametrize entries.

### General testing guidelines

These apply to all change types:

- Run `tox` before pushing. CI runs the same checks (pytest, black, flake8,
  check-tools-docs) and will fail if any of them fail.
- Declare `@pytest.mark.parametrize` parameter names as a tuple of strings, not a
  comma-separated string (e.g. `('a', 'b')` not `'a, b'`).
- See [CLAUDE.md § Testing](CLAUDE.md) for venv setup and `tox` usage.

---

## PR Checklist

Use the [PR template](.github/pull_request_template.md). Make sure every item below is
addressed before requesting review.

### All PRs

- [ ] Branch name starts with the Linear issue ID
- [ ] Commit messages start with the Linear issue ID
- [ ] `pyproject.toml` version bumped (patch / minor / major per CLAUDE.md)
- [ ] `uv.lock` synced (`uv lock`)
- [ ] `tox` passes locally (pytest + black + flake8 + check-tools-docs)
- [ ] Self-review completed

### Bug fixes

- [ ] Regression test added that reproduces the bug
- [ ] E2E scenario described in PR description (wrong behavior → correct behavior)

### Large features (≥ 200 lines)

- [ ] RFC in `feature_spec/<feature-name>/RFC.md` reviewed and agreed on **before** this PR
- [ ] RFC linked in the PR description
- [ ] Integration tests cover new external behavior
- [ ] `TOOLS.md` regenerated if tool signatures changed (`tox -e check-tools-docs`)
