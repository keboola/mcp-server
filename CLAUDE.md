# Keboola MCP Server - Project Guide

## Git Workflow
- **Always create a branch first** before committing changes
- Branch names must start with the Linear issue ID and be short (e.g., `AI-2480-whitelist-n8n-domains`)
- Commit messages should reference the Linear issue ID (e.g., `AI-2480: description`)
- When creating PRs, use the template at `.github/pull_request_template.md`

## Testing
- **Use tox** for final testing - it runs pytest, black (formatting), flake8 (linter), and check-tools-docs (verifies TOOLS.md is up-to-date)
- It's OK to use pytest directly for running individual tests during development
- Activate the virtual environment first (e.g., `source <venv>/bin/activate`)
- Run specific tests: `tox -e py310 -- tests/test_file.py -v`
- Run all checks: `tox`
- **Write parameterized tests** (`@pytest.mark.parametrize`) to reduce boilerplate
- **Be careful with mocking** - don't mock too much or tests will just test the mocks, not the real code

## Virtual Environments
- Look for a venv folder in the project root (e.g., `3.10.venv/`, `.venv/`) that contains an editable install of the project, or ask the user which venv to use
- Activate the venv before running tox or uv commands
- After version bump in `pyproject.toml`, sync lock file: `uv lock`

## Security Considerations
- When whitelisting domains in OAuth, prefer **explicit domain lists over regex patterns**
- Regex could unintentionally allow future domains that weren't reviewed (principle of least privilege)
