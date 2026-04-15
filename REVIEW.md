# Code Review Guidelines

Review standards for the keboola-mcp-server project, derived from patterns in team code reviews. For general project conventions (git workflow, testing commands, virtual environments), see `CLAUDE.md`.

## Always check

### Testing
- New test scenarios are added as parameters to existing `@pytest.mark.parametrize` tests, not as separate test functions
- Mocked data is consistent and realistic (e.g. mocked job IDs match the IDs being requested)
- Tests don't exercise impossible scenarios (e.g. SAPI returning 200 OK with missing required fields)
- Integration tests verify clean starting state as a precondition and clean up only resources they created
- Edge cases for `is not None` vs truthy checks are covered (e.g. empty string `""` treated differently from `None`)
- Tests that spawn server subprocesses use random ports, not hardcoded ones

### API Client Design
- API clients are thin wrappers around HTTP endpoints -- no business logic, orchestration, or decision-making in the client layer
- `branch_id` is accepted in the client constructor/factory (`create()`), not as a per-call parameter
- Optional API response fields use `| None = Field(default=None, ...)`
- API function parameters use `None` defaults with real defaults in the function body, not hardwired in the signature
- New API clients correctly identify whether they support OAuth bearer tokens vs SAPI tokens only
- Established libraries are used for common patterns (e.g. `httpx-retries` for retry logic) instead of hand-rolled solutions
- Error handling in try/except blocks correctly identifies which operation failed when wrapping multiple calls

### Pydantic Models
- Only the needed fields are extracted from API responses, not entire blobs
- Field/class names match domain semantics (e.g. `Schedule` not `ScheduleConfiguration`)
- Constants are named for what they hold (`*_COMPONENT_ID` not `*_COMPONENT_NAME`)
- `model_construct()` is not used alongside separate manual validation -- put validators in the model itself
- Docstrings describe purpose, not just repeat the class/field name

### Error Handling and Logging
- `LOG.exception()` or `LOG.warning(..., exc_info=True)` is used when swallowing exceptions, to preserve tracebacks for Datadog
- Log levels are appropriate: `info` for operational events, `warning` for genuinely unusual situations, `debug` for development verbosity
- Validation is not duplicated locally when SAPI already validates the same thing server-side
- Tools that silently ignore agent input include a message in the output explaining what was ignored and why
- Error responses in non-debug mode don't leak sensitive information (tokens, internal URLs)
- No unreachable code after calls that always raise (e.g. `return` after `_raise_for_status()`)
- Removing keys from mapping dicts (e.g. client-name-to-component-id) doesn't break existing downstream clients

### Tool Descriptions and Prompts
- Tool descriptions are accurate -- if the tool uses diff-based updates, it doesn't say "send the full config"
- Tool descriptions follow a consistent format: brief description, `WHEN TO USE:`, `RETURNS:`
- Tool descriptions don't duplicate what the system prompt already says
- Tool examples include all required parameters (e.g. `component_id` when needed)
- Markdown formatting is correct (empty lines before headings, consistent styles)
- Global guidance lives in the system prompt once, not repeated per-tool (context window cost)
- `Field(description=...)` includes constraints the agent needs (e.g. max character lengths)
- User-provided string inputs are stripped of whitespace at tool boundaries

### Code Organization
- No `__init__.py` re-export maintenance for purely internal code
- `utils.py` files don't accumulate unrelated functions -- single-use utilities belong in their consuming module
- Protected members (`_foo`) are not accessed from outside the class -- expose via public properties
- Unused classes, types, functions, and parameters are removed
- Functions used outside their module are not prefixed with `_`

### Performance
- Search loops break early when only the first match is needed
- ID lists are de-duplicated before fetching; repeated lookups are cached
- Throttling/rate limiting lives in the API client or HTTP layer, not in individual tool implementations

## Style

### Conventions
- `ensure_ascii=False` when serializing JSON for human or agent consumption
- `KeboolaClient.branch_id is None` means default/main branch; normalize main branch IDs to `None` at client creation
- Branch-specific IDs are translated to production equivalents via `table.prod_id` for cross-branch operations
- `set_*` prefix for methods that mutate in place; `with_*` prefix for immutable operations returning a copy
- Mutually exclusive function parameters are documented explicitly
- Don't list tool names in README -- reference auto-generated `TOOLS.md` instead

### PR Process
- Review comment fixes are replied to with the commit hash (e.g. "Fixed in abc1234")
- AI-generated code (Devin, Copilot) gets the same review scrutiny -- watch for excessive boilerplate, superfluous validation, and business logic in the wrong layer
- OAuth changes are tested with the full OAuth flow; tool description changes are tested with an actual agent (Cursor, Claude, KAI)
- No `print()` statements, debug logs, or leftover development artifacts in the final diff

## Skip
- Formatting-only changes already enforced by `black` and `flake8` (run via tox)
- Changes to `uv.lock` that are only lockfile resolution updates
- Auto-generated content in `TOOLS.md`
