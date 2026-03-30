# Code Review Guidelines

This document codifies the review standards for the keboola-mcp-server project, derived from established team review practices. Follow these guidelines when authoring or reviewing pull requests.

---

## 1. Testing

### Use parameterized tests
Write `@pytest.mark.parametrize` tests instead of multiple near-identical test functions. When adding new scenarios (e.g. a new error case, a new role), add a parameter tuple to the existing parameterized test rather than writing a separate test function.

```python
# Good
@pytest.mark.parametrize(("input_val", "expected"), [
    ("valid", True),
    ("invalid", False),
    ("edge_case", False),
])
def test_validation(input_val, expected):
    assert validate(input_val) == expected

# Bad -- separate functions for each case
def test_validation_valid():
    assert validate("valid") is True

def test_validation_invalid():
    assert validate("invalid") is False
```

### Tests must reflect reality
Mocked data should be consistent and realistic. If a test mocks a job with `runId: '456'` but requests `job_ids: ['123']`, or mocked API responses don't match the actual API contract, the test is meaningless. Don't test scenarios that can never happen (e.g. SAPI returning 200 OK with missing required fields).

### Test the right thing
Don't write tests that only exercise mocks. If validation is handled by SAPI on the server side, don't duplicate that validation locally just to have something to test. A weak test that passes trivially provides false confidence.

### Keep test files organized
- Test file names follow `test_*.py` convention (not `*_test.py` -- pytest may not discover them)
- Place tests adjacent to what they test: tests for `clients/storage.py` go in `tests/clients/test_client.py`
- Don't create new test files when existing ones already cover the module

### Set proper preconditions in integration tests
Integration tests should verify their starting state. Check that the test environment is clean before running (e.g. no leftover schedules, no stale configs) and clean up afterward. Use `mocker` fixtures when their automatic cleanup is sufficient.

---

## 2. API Client Design

### Keep API clients thin
API clients should be thin wrappers around HTTP endpoints. They should not implement high-level business logic, orchestration, or decision-making. That belongs in the tool/service layer.

```python
# Good -- thin client
async def create_workspace(self, config_id: str, name: str) -> dict:
    return await self._post(f"branch/{self.branch_id}/configs/{config_id}/workspaces", json={"name": name})

# Bad -- client implements business logic
async def create_workspace_if_not_exists(self, config_id: str, name: str) -> dict:
    existing = await self.list_workspaces(config_id)
    for ws in existing:
        if ws["name"] == name:
            return ws
    return await self._post(...)
```

### Follow established patterns for branch handling
All Keboola API clients accept `branch_id` in their constructor/factory method (`create()`), not as a per-call parameter. This is a project-wide convention.

### Declare optional fields properly
Use `Field(default=None, ...)` for optional API response fields. If an API field may be absent, the Pydantic model must reflect that with `| None = Field(default=None, ...)`.

### Don't hardwire defaults in function signatures
For API functions, declare parameters as optional with `None` defaults and move real default values into the function body. This makes the API contract clearer and avoids coupling callers to specific defaults.

```python
# Good
async def list_events(self, job_id: str, limit: int | None = None) -> list:
    params = {"limit": limit or 500}

# Bad
async def list_events(self, job_id: str, limit: int = 500) -> list:
    params = {"limit": limit}
```

### Use `bearer_of_sapi_token` consistently
When a new API client is introduced, check whether it supports OAuth bearer tokens. If not, name the auth method appropriately and don't reuse `bearer_of_sapi_token` for APIs that only accept SAPI tokens.

---

## 3. Pydantic Models and Data

### Only fetch what you need
If an API response contains a large `data` section and you only use one field from it, extract just that field instead of storing the entire blob.

### Model fields should match semantics
- Use `| None` for fields that are genuinely optional in the API response
- Use descriptive field names that match the domain (e.g. `Schedule` not `ScheduleConfiguration`, `TargetRun` not `LastExecution`)
- Docstrings that just repeat the class/field name add no value -- describe what the thing *is for*

### Don't skip validation then construct unvalidated
If you use `model_construct()` (which skips Pydantic validation), don't separately write manual validation logic. Put the validators in the model itself and use normal construction.

---

## 4. Error Handling and Logging

### Use appropriate log levels
- `info`: operational events that should always be logged (headers received, filters applied)
- `warning`: genuinely unusual situations that may need attention
- `debug`: verbose details useful during development
- Don't log normal operations as warnings

### Use `LOG.exception` when swallowing exceptions
When catching and recovering from an exception, use `LOG.exception()` to capture the full traceback. Just logging `error_type` and `error` fields loses the stack trace, which is critical for debugging in Datadog.

### Don't validate what the server validates
If a value is eventually sent to SAPI and SAPI validates it, don't duplicate that validation locally. Superfluous validation is code to maintain with no benefit. Trust the API to enforce its own constraints.

### Communicate ignored inputs to the agent
When a tool silently ignores input from the agent (e.g. dropping secrets it sent), add a message to the tool output explaining what happened and why. The agent needs feedback to avoid repeating the same action.

---

## 5. Tool Descriptions and Prompts

### Be accurate in tool docstrings
Tool descriptions are read by LLMs. Misleading descriptions cause incorrect tool usage. If the tool uses diff-based updates, don't say "send the full configuration including unmodified fields."

### Structure tool descriptions consistently
Use a consistent format across tools:
- Brief description of what the tool does
- `WHEN TO USE:` section with bullet points
- `RETURNS:` section describing output format and modes
- Don't duplicate what the system prompt already says

### Keep examples accurate
If a tool example shows `job_ids=[], config_id="12345"`, make sure it also includes `component_id` if that's required. Inaccurate examples mislead the agent.

### Markdown formatting matters
- Add empty lines before headings
- Use consistent heading styles within the system prompt
- Precise wording matters -- LLMs interpret instructions literally

---

## 6. Code Organization

### Don't over-engineer module exports
Maintaining `__init__.py` re-exports for purely internal code is high-cost, low-value. It's fine to import directly from the actual module:
```python
# Fine for internal code
from keboola_mcp_server.tools.components.tools import add_config_row
```

### Avoid `utils.py` accumulation
`utils.py` files tend to collect unrelated functions over time. When adding utility functions, consider whether they belong in a more specific module. If a utility is only used by one module, it probably belongs in that module.

### Don't access protected members
Use public properties or read-only accessors instead of accessing `_protected` attributes from outside the class.

### Remove dead code
If you declare classes, types, or functions that aren't used anywhere, remove them. Unused `SearchConfigurationScope` declarations or unreferenced parameters waste reviewer attention and mislead future developers.

### Private functions called externally must be renamed
If a function prefixed with `_` is used from outside its module, drop the underscore. The naming should reflect the actual visibility.

---

## 7. Performance and Efficiency

### Stop searching after the first match when appropriate
If you only need the first match, break out of the search loop as soon as you find it. Don't collect all matches and then discard all but one.

### Use efficient string operations
Prefer single-character indexing over substring creation. Prefer tuples over sets for small, fixed collections. These are micro-optimizations but they signal awareness of performance.

### De-duplicate before fetching
When fetching resources by IDs, de-duplicate the ID list first. Cache results of repeated lookups (e.g. `fetch_component()` calls for the same component).

### Rate limiting belongs in the client layer
Throttling (semaphores, rate limiters) should live in the API client or HTTP layer, not scattered across tool implementations. This ensures consistent enforcement.

---

## 8. Consistency and Conventions

### Use `ensure_ascii=False`
When serializing JSON for human or agent consumption, use `ensure_ascii=False` to preserve Unicode characters.

### Branch ID semantics
`KeboolaClient.branch_id is None` means the default/main branch. Any other value means a dev branch. If a branch ID header points to the main branch, normalize it to `None` during client creation rather than checking `is_default` everywhere.

### Translate branch-specific IDs for cross-branch operations
When performing operations that span branches (e.g. finding table usage), translate branch-specific IDs to their production/main equivalents using `table.prod_id`.

### Version bumping
- Bump the project version in `pyproject.toml` for every PR that changes behavior
- Ensure the version doesn't collide with what's already on `main` (check after rebasing)
- Keep the PR description in sync with the actual version

### Don't list tool names in README
Tool names change. Reference `TOOLS.md` (auto-generated) instead of maintaining a manual list.

---

## 9. Documentation in Code

### Don't duplicate between docstring and system prompt
If information is already in the system prompt (e.g. how sync actions work), the tool docstring should not repeat it. Keep tool descriptions focused on *when* and *how* to use the tool.

### Be precise about mutability in naming
Use `set_matches()` for methods that mutate in place. Reserve `with_*` prefix for immutable operations that return a new copy. The naming should communicate whether the object is modified.

### Document mutual exclusivity
When function parameters are mutually exclusive (e.g. pass `bucket_id` OR `table_id`, not both), document this explicitly.

---

## 10. Review Process Expectations

### Reply with fix commit hashes
When addressing review comments, reply with the commit hash that contains the fix (e.g. "Fixed in abc1234"). This helps reviewers verify changes without re-reading the entire diff.

### Apply the same standard to AI-generated code
Code produced by Devin, Copilot, or other AI tools gets the same review scrutiny. Common issues with AI-generated PRs:
- Excessive boilerplate (should be parameterized tests)
- Superfluous validation (trusting mock data over real API contracts)
- Separate workspaces per token when a shared one suffices
- Business logic in the wrong layer (client vs. tool/service)

### Test before claiming something works
If you change OAuth handling, test the full OAuth flow. If you change tool descriptions, test with an actual agent (Cursor, Claude, KAI). "It compiles" is not "it works."

### Don't leave debug artifacts
Remove `print()` statements, debug logs, and leftover development code before requesting review.
