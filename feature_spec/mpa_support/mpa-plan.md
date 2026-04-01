# Multi-Project Architecture (MPA) - Implementation Plan

## Context

Users of the Keboola MCP server typically have access to multiple projects within an organization. Currently, the server supports only a single project per session, requiring users to re-login to switch projects. This plan adds multi-project support so that all tools can operate across projects via a `project_id` parameter, with per-project branch management and a CLI init flow that creates Storage tokens from a Manage token.

## Key Design Decisions

### Middleware-Based Project + Branch Resolution

Rather than modifying all 31 tool function signatures to add `project_id` and `branch_id`, we use a **middleware approach**:

1. A `ProjectResolutionMiddleware` dynamically injects optional `project_id` and `branch_id` parameters into every tool's JSON schema during `on_list_tools`
2. On `on_call_tool`, the middleware extracts `project_id` and `branch_id` from arguments, resolves the correct `KeboolaClient` + `WorkspaceManager`, and places them in `ctx.session.state` under the same legacy keys
3. **Zero changes to existing tool functions** — they still call `KeboolaClient.from_state(ctx.session.state)` as before
4. Full backward compatibility: single-project mode works identically (no extra params injected)

### Stateless Branch Handling

The server is stateless (no persistent DB). Branch selection is per-tool-call:
- `branch_id` is an optional middleware-injected parameter on every tool call
- Default branch comes from the per-project config in `mcp.json` (or main if not set)
- The `create_branch` and `list_branches` tools let users discover/create branches
- No session state or "switch branch" concept needed

### Param Visibility Rules (Backward Compatible)

Parameters are only injected when they provide value — if a value is fixed in config, the param is hidden:

| Config scenario | project_id visible | branch_id visible |
|---|---|---|
| Legacy (env vars, no config file) | No | No |
| 1 project, branch_id set in config | No | No |
| 1 project, no branch_id in config | No | Yes |
| 2+ projects, all with branch_id | Yes | No |
| 2+ projects, some/all without branch_id | Yes | Yes |

This means:
- **Legacy mode is 100% unchanged** — no config file = no extra params
- **Single-project with fixed branch** = behaves exactly like legacy
- **Agent can only change what the config allows** — if branch_id is in config, it's locked

### OAuth Compatibility

OAuth mode currently provides a single project token. MPA is not supported with OAuth yet — this is documented as a known limitation. No code changes for OAuth+MPA in this iteration.

---

## Phase 1: Foundation — Config & Multi-Project State

### 1.1 Extend Config (`src/keboola_mcp_server/config.py`)

Add `ProjectConfig` dataclass:
```python
@dataclass(frozen=True)
class ProjectConfig:
    project_id: str
    storage_api_url: str
    storage_token: str
    branch_id: Optional[str] = None
    workspace_schema: Optional[str] = None
    alias: Optional[str] = None
    forbid_main_branch_writes: bool = False
```

Add to existing `Config`:
- `projects: tuple[ProjectConfig, ...] = ()`
- `default_project_id: Optional[str] = None`
- `forbid_main_branch_writes: bool = False` (global default)
- `is_mpa_mode` property: `return len(self.projects) > 0`
- `from_config_file(path: Path) -> Config` classmethod to load `mcp.json`

### 1.2 Create ProjectRegistry (`src/keboola_mcp_server/project_registry.py` — new file)

```python
@dataclass
class ProjectContext:
    project_id: str
    client: KeboolaClient
    workspace_manager: WorkspaceManager
    alias: str | None
    forbid_main_branch_writes: bool

class ProjectRegistry:
    STATE_KEY = 'project_registry'
    projects: dict[str, ProjectContext]  # keyed by project_id
    default_project_id: str | None

    def get_project(self, project_id: str | None) -> ProjectContext
    def list_projects(self) -> list[ProjectContext]
    def inject_into_state(self, state: dict, project_id: str | None) -> None
    @classmethod
    def from_state(cls, state) -> ProjectRegistry
```

### 1.3 Update SessionStateMiddleware (`src/keboola_mcp_server/mcp.py`)

In `create_session_state`, when `config.is_mpa_mode`:
- Create `KeboolaClient` + `WorkspaceManager` for each project (concurrently via `asyncio.gather`)
- Build `ProjectRegistry`, store in state
- Also inject default project's client/workspace under legacy keys (for middleware that runs before project resolution, e.g. `ToolsFilteringMiddleware`)

---

## Phase 2: Project Resolution Middleware

### 2.1 Create ProjectResolutionMiddleware (`src/keboola_mcp_server/mcp.py`)

`on_list_tools`:
- If 2+ projects in config: inject optional `project_id` parameter into every tool's JSON schema (description lists available project IDs/aliases)
- If any project has no `branch_id` fixed in config: inject optional `branch_id` parameter into every tool's JSON schema
- Skip injection for project-agnostic tools (`docs_query`)
- In legacy mode (no config file): no-op

`on_call_tool`:
- Extract and pop `project_id` from `context.message.arguments` (if present)
- Extract and pop `branch_id` from `context.message.arguments` (if present)
- Resolve `ProjectContext` from registry (use default/only project if `project_id` not specified; error if ambiguous with 2+ projects)
- Determine effective branch: explicit `branch_id` arg > project's config `branch_id` > main (None)
- If effective branch differs from project's default, call `client.with_branch_id(branch_id)` to get a branch-specific client
- Inject resolved client + workspace_manager into `ctx.session.state` under legacy keys
- Check `forbid_main_branch_writes`: if tool is a write op, effective branch is main, and setting is True → raise `ToolError`
- In legacy mode (no config file): no-op (session state already set by SessionStateMiddleware as today)

### 2.2 Register Middleware (`src/keboola_mcp_server/server.py`)

Middleware chain order:
```python
middleware=[
    SessionStateMiddleware(),
    ProjectResolutionMiddleware(),  # NEW — after session, before auth
    ToolAuthorizationMiddleware(),
    ToolsFilteringMiddleware(),
    ValidationErrorMiddleware(),
]
```

---

## Phase 3: Branch Tools & Write Protection

### 3.1 Add `dev_branch_create` to Storage Client (`src/keboola_mcp_server/clients/storage.py`)

```python
async def dev_branch_create(self, name: str, description: str = '') -> JsonDict:
    return await self.post(endpoint='dev-branches', data={'name': name, 'description': description})
```

### 3.2 Create Branch Tools (`src/keboola_mcp_server/tools/branches.py` — new file)

**`list_branches`** (readOnlyHint=True):
- Calls `client.storage_client.branches_list()`
- Returns list of branches with id, name, isDefault, created, description

**`create_branch`** (destructiveHint=False):
- Parameters: `name: str`, `description: str = ''`
- Calls `client.storage_client.dev_branch_create(name, description)`
- Returns created branch info

Register via `add_branch_tools(mcp)` in `server.py`.

### 3.3 Main Branch Write Protection

In `ProjectResolutionMiddleware.on_call_tool`:
- After project resolution, if the tool is not read-only AND the client is on main branch (branch_id is None) AND `forbid_main_branch_writes` is True for this project (or globally):
  - Raise `ToolError`: "Write operations on the main branch are forbidden. Create a development branch first using `create_branch`, then specify the branch when calling tools."

---

## Phase 4: get_project_info Changes

### 4.1 Update `get_project_info` (`src/keboola_mcp_server/tools/project.py`)

In MPA mode when no specific `project_id` is given (or a special "all" mode), return a new `MultiProjectInfo` model:

```python
class MultiProjectInfo(BaseModel):
    projects: list[ProjectInfo]  # per-project info (without llm_instruction)
    llm_instruction: str         # shared, returned once
```

Each project entry includes: `project_id`, `project_name`, `project_description`, `organization_id`, `sql_dialect`, `conditional_flows`, `links`, `user_role`, `toolset_restrictions`.

In single-project mode, behavior is unchanged (returns `ProjectInfo` as today).

---

## Phase 5: CLI Init Command

### 5.1 Add ManageClient (`src/keboola_mcp_server/clients/manage.py` — new file)

Async HTTP client for Manage API (based on reference CLI pattern):
- Auth header: `X-KBC-ManageApiToken: <token>`
- `verify_token()` → `GET /manage/tokens/verify`
- `get_project(project_id)` → `GET /manage/projects/{project_id}`
- `list_organization_projects(org_id)` → `GET /manage/organizations/{org_id}/projects`
- `create_project_token(project_id, description, ...)` → `POST /manage/projects/{project_id}/tokens`

Token creation payload (matching reference CLI):
```json
{
    "description": "keboola-mcp-server",
    "canManageBuckets": true,
    "canReadAllFileUploads": true,
    "canReadAllProjectEvents": true,
    "canManageDevBranches": true,
    "canManageTokens": true
}
```

### 5.2 Add `init` CLI Command (`src/keboola_mcp_server/cli.py`)

New `init` subcommand added to argparse:
```
python -m keboola_mcp_server init \
    --manage-token <TOKEN> \
    --api-url https://connection.north-europe.azure.keboola.com \
    [--project-ids 12345,67890] \
    [--all] \
    --output mcp.json \
    [--forbid-main-branch-writes]
```

Flow:
1. Verify manage token → `GET /manage/tokens/verify`
2. Get org from token info, list all projects in org
3. Project selection (three modes):
   - `--project-ids 12345,67890`: Use specific projects (non-interactive)
   - `--all`: Add all projects in the organization
   - Neither flag: Interactive prompt listing available projects for user selection
3. For each selected project, create Storage API token via manage API
4. Write `mcp.json` with format:
```json
{
    "version": 1,
    "default_project_id": "12345",
    "forbid_main_branch_writes": false,
    "projects": [
        {
            "project_id": "12345",
            "alias": "my-project",
            "storage_api_url": "https://connection.north-europe.azure.keboola.com",
            "token": "<created-storage-token>"
        }
    ]
}
```
5. **Manage token is NOT stored** in the config file

### 5.3 Add `--config-file` to `run_server` (`src/keboola_mcp_server/cli.py`)

```
python -m keboola_mcp_server --transport stdio --config-file mcp.json
```

When `--config-file` is provided, load `Config.from_config_file(path)` instead of using CLI args for token/URL. OAuth and other server settings can still come from env vars.

---

## Phase 6: ToolsFilteringMiddleware Updates

### 6.1 Adapt for MPA (`src/keboola_mcp_server/mcp.py`)

`ToolsFilteringMiddleware` currently calls `verify_token()` to get project features and token role. In MPA mode:
- `on_list_tools`: Use the default project's client (already injected by SessionStateMiddleware under legacy keys)
- `on_call_tool`: By this point, `ProjectResolutionMiddleware` has already injected the correct project's client, so `ToolsFilteringMiddleware` works without changes

Consider caching `verify_token()` results in `ProjectContext` during session creation to avoid repeated API calls.

---

## Files Summary

### New Files
| File | Purpose |
|------|---------|
| `src/keboola_mcp_server/project_registry.py` | ProjectContext, ProjectRegistry |
| `src/keboola_mcp_server/clients/manage.py` | Async ManageClient for Manage API |
| `src/keboola_mcp_server/tools/branches.py` | list_branches, create_branch tools |

### Modified Files
| File | Changes |
|------|---------|
| `src/keboola_mcp_server/config.py` | ProjectConfig dataclass, MPA fields, config file loading |
| `src/keboola_mcp_server/mcp.py` | ProjectResolutionMiddleware, SessionStateMiddleware MPA support, write protection |
| `src/keboola_mcp_server/server.py` | Register new middleware + branch tools |
| `src/keboola_mcp_server/cli.py` | `init` subcommand, `--config-file` flag |
| `src/keboola_mcp_server/clients/storage.py` | `dev_branch_create` method |
| `src/keboola_mcp_server/tools/project.py` | MultiProjectInfo response for MPA mode |

### Test Files
| File | Tests |
|------|-------|
| `tests/test_config.py` | ProjectConfig, is_mpa_mode, from_config_file |
| `tests/test_project_registry.py` (new) | Registry creation, project resolution, defaults, errors |
| `tests/test_mcp.py` | MPA session state, ProjectResolutionMiddleware, write protection |
| `tests/tools/test_branches.py` (new) | list_branches, create_branch |
| `tests/tools/test_project.py` | Multi-project info response |

---

## Verification Plan

1. **Unit tests**: Run `tox` — all existing tests must pass (backward compatibility), plus new MPA tests
2. **Single-project mode**: Start server with existing env vars / CLI args → verify all tools work exactly as before (no `project_id` param visible)
3. **MPA mode**: Start server with `--config-file mcp.json` containing 2+ projects → verify:
   - `get_project_info` returns all projects
   - Tools accept `project_id` parameter
   - Default project used when `project_id` omitted
   - Error when `project_id` missing and no default
4. **Init command**: Run `init` with a manage token → verify `mcp.json` created with correct tokens, manage token not stored
5. **Branch tools**: Create branch, list branches, verify per-project branch management
6. **Write protection**: Enable `forbid_main_branch_writes`, attempt a write tool on main → verify rejection, create branch and retry → verify success

---

## Important Considerations

- **OAuth + MPA**: OAuth provides a single bearer token for one project. MPA in OAuth mode is not supported initially — only SAPI token mode. Document this clearly.
- **Workspace creation**: Each project needs its own workspace (async). Use `asyncio.gather` for concurrent creation during session init.
- **Token info caching**: Cache `verify_token()` results in `ProjectContext` to avoid redundant API calls per tool invocation.
- **Schema injection**: Modifying Tool JSON schema dynamically requires working with the raw dict returned by `tool.parameters`. Add `project_id` as an optional string property.
- **Cross-stack projects**: While the initial version supports only same-organization projects, `ProjectConfig.storage_api_url` is per-project, so cross-stack support is architecturally possible.