# Keboola MCP Server - Development Guide

## Project Setup and Commands

### Dependencies and Environment
- This project uses `uv` as the package manager
- Python dependencies are defined in `pyproject.toml`
- Optional dependency groups: `tests`, `codestyle`, `integtests`, `dev`

### Installing Dependencies
```bash
# Install test dependencies
uv sync --extra tests

# Install all development dependencies
uv sync --all-extras
```

### Running Tests
```bash
# Run specific test file
uv run --extra tests pytest tests/tools/test_oauth.py -v

# Run all unit tests (excluding integration tests)
uv run --extra tests pytest tests/ -k "not integtest" --maxfail=5 -q

# Run specific test function
uv run --extra tests pytest tests/test_server.py::TestServer::test_list_tools -v
```

### Code Quality
```bash
# Check syntax (if available)
python3 -m py_compile src/keboola_mcp_server/tools/oauth.py

# The project uses black, isort, flake8 for code formatting (defined in pyproject.toml)
```

### Project Structure
- Tools are in `src/keboola_mcp_server/tools/`
- Tests are in `tests/` with corresponding structure
- Each tool module should have:
  - Implementation file (e.g., `oauth.py`)
  - Test file (e.g., `test_oauth.py`)
  - Registration in `server.py`

### Adding New Tools
1. Create tool implementation in `src/keboola_mcp_server/tools/[name].py`
2. Add import and registration in `src/keboola_mcp_server/server.py`
3. Create tests in `tests/tools/test_[name].py`
4. Update tool list in `tests/test_server.py::TestServer::test_list_tools`

### Testing Patterns
- Use `@pytest.mark.asyncio` for async test functions
- Use `mcp_context_client` fixture for mocked MCP context with KeboolaClient
- Use `pytest.mark.parametrize` for testing multiple scenarios
- Mock external API calls using `mocker.MagicMock()`

### Key Fixtures
- `keboola_client`: Mocked KeboolaClient with storage/jobs/ai clients
- `mcp_context_client`: MCP Context with mocked clients in session state
- `empty_context`: Basic mocked MCP Context with empty state