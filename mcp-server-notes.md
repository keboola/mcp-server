## MCP Server Notes

### Setup
- The server config is centralized around `keboola_mcp_server.config.Config` class.
- The effecive config is read from multiple sources and their precedence is: CLI < environment variables < HTTP headers.
  Header values can override deployment defaults per request.
- The names of the environment variables and HTTP headers are derived from the Config class field names prefixed with `KBC_` for env vars and `X-` for HTTP headers.
  The matching is quite loose and ignore upper/lower case chars and dash versus underscore chars.
- Values of certain fields are normalized, for example:
  - `branch_id` values like `""`, `none`, `null`, `default`, `production` become `None` all mean the main branch
  - URLs are cleaned to hostname + scheme only; path/query parts are discarded.
- `http-compat` is not a separate transport. It is only a backward-compatible alias for `streamable-http`.

### MiddleWare Chain
- `LoggingMiddleware`: request/response logging only.
- `SessionStateMiddleware`: reconstructs per-request runtime state; it builds `KeboolaClient`, `WorkspaceManager` from the server config and HTTP headers
- `ToolAuthorizationMiddleware`: filters tools based the HTTP headers `X-Allowed-Tools`, `X-Disallowed-Tools`, and `X-Read-Only-Mode`
- `ToolsFilteringMiddleware`: filters tools based on the actual project features and user role.
- `ValidationErrorMiddleware`: catches Pydantic ValidationError and formats it with explicit field locations.

### Sessions and SessionState
- Streamable-HTTP transport runs in stateless mode. There are no persistent MCP sessions and a new "session" is created for every request.
- The stateless / sessionless mode means that certain protocol features can't be used (e.g. LLM sampling, elicitation and others).
- Session state:
  - constructed per request, lives in `ctx.session.state` and contains: `KeboolaClient`, `WorkspaceManager` and `conversation_id`
  - created by SessionStateMiddleware
  - passed to all operations (tool calls, promts listing, etc)
  - Cleanup is intentionally disabled due to a client-side bug in Claude. Not a problem, because each session lives only within a request.
- Mocking the session state for tests rely on a special shortcut: if `ctx.session` is a `MagicMock`, session initialization is skipped in SessionStateMiddleware.

### OAuth
- OAuth is optional and available only for Streamable-HTTP transport. It's use is controlled by `KBC_OAUTH_CLIENT_ID` and `KBC_OAUTH_CLIENT_SECRET` variables.
- The auth codes and access/refresh tokens are JWT-based and contain encrypted data. There is no DB/Redis-backed store for auth codes or tokens.
- The implemented Oauth provider supports Dynamic Client Registration (DCR), but does not persist the clients info. It's a fake implemnetation.
- We use whitelisted redirect URIs to accept connections only from the "trusted" OAuth clients.
- We ignore the OAuth scopes requested/registered by the OAuth clients.
- The extra SAPI token is created, because some Keboola APIs still require `X-StorageAPI-Token` and don't accepts `Authorization: Bearer` header.
- SessionStateMiddleware passes this extra SAPI token to KeboolaClient and its specialized Keboola API clients that need it.

### Tools
- Tool input schemas are derived automatically from Python signatures and Pydantic models. There is no hand-written JSON schema assembly for normal tool registration.
- Tools are decorated by `@tool_errors()`, whichtriggers SAPI event for the telemetry and formats errors for easier LLM consumption.
- `TOOLS.md` is generated from tool metadata. CI checks it through tox, so any tool change can fail CI until docs are regenerated.
- Tool outputs usually go through 3 serializers, depending on the tool.
  - `_exclude_none_serializer`: Default JSON serializer. Converts data to JSON and drops None values. If it hits an unsupported type, it falls back to str(), which can hide serialization mistakes.
  - `toon_serializer`: Formats output into a more LLM-friendly text/table style instead of raw JSON.
  - `toon_serializer_compact`: Same as toon_serializer, but also removes fields that are None in all rows, so repeated outputs are smaller and cleaner.

#### Tools Filtering
- Tool filtering happens both on `tools/list` and `tools/call`, so hidden tools are also blocked if called directly.
- Tool annotations such as `readOnlyHint` are used for tools filtering (e.g. user with readonly role will only see the read-only tools).
- Two types of filtering:
  - filtering by HTTP headers - mainly for proxy/infrastructure control, not the primary security boundary. Real access control still depends on the Keboola token and project role.
  - filtering by project/user - real access control based on the project features and the user's role
    - project feature flags, for example semantic tooling or conditional flow support
    - token role, for example readonly vs admin/share vs guest
    - token type, especially SAPI vs OAuth behavior differences
    - branch context, for example Data App tools are blocked outside the main branch
- Important edge case: `*/list` requests force `branch_id=None`, so discovery behaves as if it is on the main branch.
  A tool may appear in `tools/list` but still be blocked later when called in a non-main branch.

### Prompts
- The MCP server exposes several prompts in addition to its tools.
- Registered prompts are meant as one-click LLM task templates.

### System Prompt
- This is the main system/project prompt and it describes the main principles that apply across all tools.
- The system prompt is available through `get_project_info` to the agent and should usually be the agent's first call.
- While the protocol (MCP) allows for "server instructions", they don't seem to be accepted/used by many agents.
  This is why we opted for linking the main system/project prompt to `get_project_info` tool.

### SQL queries
- The MCP server creates a read-only workspace in each project that it is connected to. The same workspace is reused by all requests/users connecting to the same project.
- This shared workspace is used for runnig SQL queries (either built-in SQLs or those from `query_data` tool).
- The MCP server support both the Snowflake and BigQuery backends. For Snowflake backends the SQLs are executed via the Query Service API and for BigQuery backends they
  are executed via the Storage API.
- Users can choose to supply their own workspace instead of using the shared one by providing the workspace schema either via the env variable or the HTTP header.

### Preview Endpoint

- `/preview/configuration` is a custom HTTP endpoint, not an MCP tool.
- It exists to simulate configuration-changing tools without applying writes, so UI/integration layers can show a diff or preview.
- It validates input against the tool input schema, builds a read-only client, and calls internal preview-style functions.
- It only exists in HTTP mode because it depends on the surrounding Starlette app and startup-populated tool schemas.

### Telemetry Storage Events

- Telemetry is triggered from the `@tool_errors()` wrapper in a `finally` block, so it runs for both successful and failed tool calls.
- Events are written as best effort; telemetry failures must not break the tool result.
- Logged payloads can include tool arguments, with truncation. This matters for security/privacy review.
- Some user-agent values are remapped to special component IDs for analytics purposes.

### Validations
- The project uses Pydantic validation heavily for config, tool input, and API response models.
- Component/config schema validation uses JSON Schema when agent creates a new configuration.
  - The validator is intentionally tolerant of known real-world schema inconsistencies from upstream services and fixes them.
  - Schemas are fetched along with component detail either from AI service or SAPI service.
  - The schema validation against agent's provided configuration is skipped when:
    - If the component does not have schema
    - If the schema document itself is invalid, the system log the problem and continue instead of hard-failing. This is deliberate, because upstream schemas are not always clean and blocking on them would break tool calls.
