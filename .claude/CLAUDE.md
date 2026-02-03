# Keboola MCP Server - Project Guidelines

This document contains project-specific coding standards and best practices for the Keboola MCP Server. These guidelines complement the global CLAUDE.md standards and address patterns specific to this codebase.

## Code Simplicity Standards

**Avoid Unnecessary Wrapper Functions:**
- Do NOT create helper methods for simple conditional checks
- Direct inline checks are preferred for readability when the condition is simple
- Example: Use `if token_role in ['guest', 'readonly']:` instead of `requires_read_only_access()`
- Only create helper functions when they prevent significant duplication or encapsulate complex logic

**When to Extract Helpers:**
- When the same logic is duplicated across multiple modules
- When the logic is complex enough that a descriptive name improves readability
- When the helper can be reused in multiple contexts

**Example - Bad (Over-abstraction):**
```python
@staticmethod
def is_guest_role(token_info: JsonDict) -> bool:
    """Check if the token belongs to a guest user."""
    role = ToolsFilteringMiddleware.get_token_role(token_info).lower()
    return role == 'guest'

@staticmethod
def requires_read_only_access(token_info: JsonDict) -> bool:
    """Check if the token requires read-only tool access."""
    return (
        ToolsFilteringMiddleware.is_guest_role(token_info)
        or ToolsFilteringMiddleware.is_read_only_role(token_info)
    )

# Usage
if self.requires_read_only_access(token_info):
    # filter tools
```

**Example - Good (Direct and Clear):**
```python
# Usage - direct inline check
if token_role in ['guest', 'readonly']:
    # filter tools
```

## Test Structure Standards

**Test Files Must Mirror Source Structure:**
- `src/keboola_mcp_server/mcp.py` → `tests/test_mcp.py`
- `src/keboola_mcp_server/authorization.py` → `tests/test_authorization.py`
- `src/keboola_mcp_server/clients/client.py` → `tests/test_clients/test_client.py`

**NO Separate Test Files for Features:**
- Do NOT create `test_role_based_authorization.py` for role authorization in mcp.py
- Do NOT create `test_feature_x.py` for a feature implemented in an existing module
- The test file structure should mirror the source code structure exactly

**Exception:**
- Only create separate test files if the feature has its own separate source module

## Test Quality Standards

**Prefer Integration Tests Over Heavy Mocking:**
- Use real server instances with mocked external dependencies (like test_server.py does)
- Mock only at system boundaries (API responses, token verification, external services)
- Avoid mocking internal implementation details

**Good Integration Test Pattern (from test_server.py:220-254):**
```python
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('admin_info', 'expected_included', 'expected_excluded'),
    [
        ({'role': 'admin'}, 'modify_flow', 'update_flow'),
        ({'role': 'guest'}, 'get_buckets', 'create_config'),
        ({'role': 'readOnly'}, 'query_data', 'update_descriptions'),
    ],
)
async def test_role_based_access(mocker, admin_info, expected_included, expected_excluded):
    # Mock only external boundary (token verification)
    mocker.patch(
        'keboola_mcp_server.clients.client.AsyncStorageClient.verify_token',
        return_value={'admin': admin_info, 'owner': {'features': []}},
    )

    # Use real server instance
    mcp = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))

    # Test real behavior
    async with Client(mcp) as client:
        tools = await client.list_tools()
        tool_names = {tool.name for tool in tools}
        assert expected_included in tool_names
        assert expected_excluded not in tool_names
```

**Bad Test Pattern (Heavy Mocking):**
```python
# DON'T mock everything - this tests almost nothing
async def test_feature(middleware, mock_context):
    call_next = AsyncMock(return_value=mock_tools)
    with patch.object(middleware, 'method1', AsyncMock(return_value=...)):
        with patch.object(middleware, 'method2', return_value=...):
            # This test validates mocks, not real behavior
            result = await middleware.on_list_tools(mock_context, call_next)
```

**What to Mock:**
- External API responses (Storage API, Scheduler API)
- Token verification calls
- File system operations
- Network requests
- Time-dependent behavior

**What NOT to Mock:**
- Internal method calls within the module being tested
- Simple utility functions
- The middleware chain itself (use real FastMCP server)
- Tool filtering logic (test the actual behavior)

## Logging Standards

**Log Level Guidelines:**
- **DEBUG**: Verbose/frequent operations that would be noisy in production
  - Example: Every tools listing call
  - Example: Detailed parameter values during processing
- **INFO**: Significant operational decisions that should always be visible
  - Example: Authorization decisions (access granted/denied)
  - Example: Configuration changes
  - Example: Client initialization
- **WARNING**: Unexpected situations needing attention but not stopping execution
  - NOT for normal operations
- **ERROR**: Errors preventing operation completion

**Example:**
```python
# Good - DEBUG for frequent operations
if token_role in ['guest', 'readonly']:
    tools = [t for t in tools if is_read_only_tool(t)]
    LOG.debug(f'Read-only access: filtered to {len(tools)} read-only tools for role={token_role}')

# Bad - INFO would be too noisy (called on every tools listing)
LOG.info(f'Read-only access: filtered to {len(tools)} read-only tools for role={token_role}')
```

## Code Deduplication

**Shared Utility Module:**
- Extract helpers duplicated across modules to `src/keboola_mcp_server/utils.py`
- Document where shared utilities live in module docstrings
- Import shared utilities instead of duplicating code

**Example:**
```python
# utils.py
def is_read_only_tool(tool: Tool) -> bool:
    """Check if a tool has readOnlyHint=True annotation.

    This is used by both ToolsFilteringMiddleware and ToolAuthorizationMiddleware
    to determine which tools are read-only.
    """
    if tool.annotations is None:
        return False
    return tool.annotations.readOnlyHint is True

# mcp.py
from keboola_mcp_server.utils import is_read_only_tool

# authorization.py
from keboola_mcp_server.utils import is_read_only_tool
```

## Documentation Standards

**Docstrings Must Match Implementation:**
- Review docstrings after implementation changes
- Role matrices, feature lists, and behavior descriptions must be accurate
- Prefer clear descriptions over complex tables

**Example - Bad (Inaccurate):**
```python
"""
Role-based access control:
- Admin: Full access to all tools
- Guest: Read-only access
- Read: Read-only access
- Other roles: Standard access
"""
# Reality: Admin has some tools blocked, other roles have specific restrictions
```

**Example - Good (Accurate):**
```python
"""
Role-based access control:
- Guest: Read-only access (only tools with readOnlyHint=True)
- Read: Read-only access (only tools with readOnlyHint=True)
- Other non-admin roles: Write tools available, with specific tools (e.g., `modify_flow`) explicitly restricted
- Admin: Broad access to tools, with specific write tools (e.g., `update_flow`) explicitly restricted
"""
```

## README Standards

**Role Descriptions:**
- List roles in logical progression (most restrictive first)
- Use consistent terminology ("read-only tools" not "query-only operations")
- Be specific about what each role can/cannot do

**Example Structure:**
```markdown
#### Role-Based Access Control

- **Guest**: Read-only access limited to tools marked as read-only (no modifying operations)
- **Read**: Similar to guest, users with read role can only access tools marked as read-only
- **Other non-admin roles**: Standard write access with some administrative tools restricted
- **Admin**: Broad tool access for administrative operations, with specific tools restricted
```

## Version Management

**ALWAYS Bump Version in pyproject.toml:**
- Every PR must increment the version number
- Use semantic versioning:
  - Patch (1.42.1 → 1.42.2): Bug fixes, small improvements
  - Minor (1.42.1 → 1.43.0): New features, non-breaking changes
  - Major (1.42.1 → 2.0.0): Breaking changes
- Update version BEFORE creating the PR

## Dependency Management

**Use `uv` instead of `pip`:**
- This project uses `uv` for dependency management, NOT `pip`
- To run commands: `uv run <command>`
- To run tests: `uv run --extra tests pytest tests/`
- To install dependencies: `uv sync`
- The virtual environment is managed by `uv` and located in `.venv/`
- Never use `pip install` - always use `uv` commands

**Examples:**
```bash
# Run tests
uv run --extra tests pytest tests/test_mcp.py -v

# Run specific test group
uv run --extra tests pytest tests/test_server.py -k "role"

# Run the server
uv run keboola-mcp-server

# Sync dependencies
uv sync
```

## Testing & Quality Checks

**Run Tox After Python Changes:**
- ALWAYS run `uv run tox` after modifying Python source files, tests, or integration tests
- Tox runs: pytest (932 tests), black (formatting), flake8 (linting), and check-tools-docs
- All checks must pass before committing or creating a PR
- If flake8 fails, fix the issues immediately - don't commit with linting errors
- If black reformats files, those changes will be applied automatically

**When to Run Tox:**
- After editing any `.py` files in `src/`, `tests/`, or `integtests/`
- Before creating a git commit with Python changes
- Before creating or updating a pull request
- When requested by the user to "run tests" or "check code quality"

**Tox Commands:**
```bash
# Run all checks (recommended)
uv run tox

# Run specific environment only
uv run tox -e python      # Run tests only
uv run tox -e black       # Run black formatting only
uv run tox -e flake8      # Run linting only
uv run tox -e check-tools-docs  # Verify TOOLS.md is up to date
```

**Common Issues:**
- **Flake8 F841**: Unused variable - remove the variable or prefix with `_` if needed for compatibility
- **Flake8 PT004**: Fixture without return - prefix fixture name with `_` per pytest convention
- **Black reformatting**: Commit the reformatted files - black's changes are always correct
- **Test failures**: Fix the underlying issue before proceeding

## Review Checklist

Before submitting a PR, verify:

- [ ] Tests: All tox checks pass (pytest, black, flake8, check-tools-docs)
- [ ] Code simplicity: No unnecessary wrapper functions for simple conditions
- [ ] Test structure: Tests mirror source file structure (no separate feature test files)
- [ ] Test quality: Integration tests with minimal mocking (only external boundaries)
- [ ] Logging: Appropriate log levels (DEBUG for frequent operations)
- [ ] Deduplication: Shared logic extracted to utils.py
- [ ] Docstrings: Accurate and match actual implementation
- [ ] README: Clear, accurate role descriptions in logical order
- [ ] Version: Incremented in pyproject.toml

## References

These guidelines were established based on feedback from:
- PR #350 (Tool Authorization Middleware)
- PR #381 (Role-Based Authorization)
- Code review patterns from the Keboola MCP Server team
