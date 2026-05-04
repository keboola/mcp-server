# Multi-Project Architecture (MPA)

**Linear**: [AI-3027](https://linear.app/keboola/issue/AI-3027/draw-new-architecture-of-mpa-ui-platform-mcp-kai)
**Status**: RFC / Design spec — implementation not yet started

---

## Motivation

The current MCP server accepts a single credential at startup (a Storage API token or an OAuth
bearer token), which locks the entire session to one Keboola project. This is sufficient for
the Claude.ai / direct-user flow, but becomes a blocker for:

- **Kai** — the Keboola AI platform needs a single agent session to reason across multiple
  projects within an organisation.
- **Enterprise users** managing many projects who want one agent session, not one server per
  project.
- **Programmatic / headless** scenarios where a long-running session must add new projects on
  demand without restarting.

---

## Deployment Contexts

Understanding where MPA applies is critical. The hosted `mcp.keboola.com` is unaffected —
OAuth users continue exactly as today.

| Deployment | Users | Auth method | Multi-project? |
|---|---|---|---|
| **mcp.keboola.com** (public hosted) | Regular users (Claude.ai, Claude Code) | OAuth → bearer token | ❌ No — OAuth always resolves to 1 project. No changes needed. |
| **Local / self-hosted** | Power users, developers | Programmatic token + refresh token | ✅ Yes |
| **Kai** (internal AI platform) | Keboola AI service | Programmatic token + refresh token | ✅ Yes |
| **Kai / headless single-project** | Internal pipelines | PAT token | Single project only |

---

## Token Taxonomy

| Token | Scope | Lifetime | How to obtain |
|---|---|---|---|
| **Storage Token** *(existing)* | Single project | Until revoked | Keboola UI |
| **OAuth bearer** *(existing)* | Single project | Session-scoped | OAuth flow on mcp.keboola.com |
| **PAT** (Personal Access Token) | Single project | Never expires | Keboola UI or generated from programmatic token |
| **Programmatic Token** | All projects the user can access | Short-lived, refreshable | `POST /login` (username + password + MFA) → `token` + `refresh_token` |

### Programmatic token — startup modes

**Mode A (target for initial RFC): user logs in upfront, passes both tokens as arguments:**
```bash
python -m keboola_mcp_server \
  --programmatic-token <token> \
  --refresh-token <refresh_token> \
  --api-url https://connection.eu-central-1.keboola.com
```

> **Security note:** Passing tokens as CLI arguments exposes them in shell history
> (`~/.bash_history`) and process listings (`ps aux`). For production and headless
> deployments (Kai), prefer the environment variable equivalents `KBC_PROGRAMMATIC_TOKEN`
> and `KBC_REFRESH_TOKEN` instead of CLI flags.

**Mode B (deferred): server performs login interactively.**
Deferred because MFA handling in a headless process is non-trivial. For Kai, pre-obtained
tokens are always available.

---

## Authentication Flows

### Flow A — PAT token (single project, local / Kai)

```
User / Kai
  │  --pat-token <PAT>
  ▼
MCP Server ──► Keboola Storage API: verify token → project_id
  │              auto-register project in session registry
  ▼
Server ready (1 project active)
  │
  │  get_project_info(project_id="123")
  │  get_tables(project_id="123")
  ▼
Storage API (using PAT)
```

### Flow B — Programmatic token (multi-project, local / Kai)

```
User / Kai
  │  --programmatic-token <tok> --refresh-token <rtok>
  ▼
MCP Server ──► Login API: validate token
             ──► Management API: GET /projects → [proj_A, proj_B, …]
             ──► Start background token refresh task
  │
  ▼
Server ready (0 projects active)

  │  list_accessible_projects()
  ◄──────────────────────────────────── [proj_A, proj_B, …]

  │  add_project_to_session(project_id="A")
  ▼
MCP Server: validate A is in accessible list (in-memory)
            Session registry = { A }  (append-only, no external call)

  │  add_project_to_session(project_id="B")
  ▼
MCP Server: validate B is in accessible list (in-memory)
            Session registry = { A, B }

  │  get_tables(project_id="A")   →  Storage API (programmatic token, scoped to A)
  │  get_tables(project_id="B")   →  Storage API (programmatic token, scoped to B)
```

### Flow C — OAuth bearer (existing, mcp.keboola.com, unchanged)

No changes. OAuth resolves to exactly one project and behaves like Flow A once authenticated.

---

## Session State Design

### Current (single project)

```python
ctx.session.state = {
    "sapi_client": KeboolaClient(token, url, branch_id),
    "workspace_manager": WorkspaceManager(…),
    "conversation_id": "…",
}
```

### Proposed (multi-project)

```python
@dataclasses.dataclass
class ProjectEntry:
    project_id: str
    project_name: str
    region: str
    storage_api_url: str
    sapi_client: KeboolaClient      # credential depends on auth_mode (programmatic token / PAT / OAuth)
    workspace_manager: WorkspaceManager
    added_at: datetime

@dataclasses.dataclass
class ProgrammaticTokenContext:
    token: str
    refresh_token: str
    expires_at: datetime
    last_used_at: datetime        # refresh only while session is active
    refresh_task: asyncio.Task    # background coroutine

ctx.session.state = {
    "auth_mode": "pat" | "programmatic" | "oauth" | "storage_token",
    "programmatic_ctx": ProgrammaticTokenContext | None,

    # Append-only — keyed by project_id string
    "project_registry": dict[str, ProjectEntry],

    # Legacy keys kept for backward compat in single-project modes
    "sapi_client": KeboolaClient,
    "workspace_manager": WorkspaceManager,
    "conversation_id": str,
}
```

**Append-only invariant:** `project_registry` grows only during a session. No remove
operation is exposed. Removing a project mid-conversation would invalidate prior reasoning
and confuse the agent.

> **Implementation note — stateful sessions:** The `ctx.session.state` pattern above requires
> the MCP session to be long-lived. The current Streamable-HTTP transport creates a new
> `ServerSession` per request (stateless). MPA therefore requires either (a) switching to
> stateful SSE/WebSocket transport for multi-project clients, or (b) a process-level
> `ServerState` store keyed by a stable `session_id` (supplied by the client or assigned at
> connection time) with an explicit cleanup / TTL policy. The choice is an implementation
> decision deferred to the coding phase; the logical design above is transport-agnostic.

---

## Token Refresh Strategy

```
On session start (programmatic token mode):
  → Start background asyncio.Task: token_refresh_loop()

token_refresh_loop():
  while session alive:
    sleep until (expires_at − 60 s)
    if (now − last_used_at) > 24 h:
      mark session expired, stop loop
    else:
      POST /token/refresh → new_token, new_refresh_token
      update ProgrammaticTokenContext in state

On any tool call (programmatic mode):
  → update last_used_at = now

On session close:
  → cancel refresh_task
```

**24-hour idle rule:** If no tool call is made for 24 hours the refresh loop stops and the
session is marked expired. The next tool call raises a `ToolError`:
> "Session expired due to inactivity. Restart the server with fresh credentials."

---

## project_id: Middleware vs. Tool Argument

Two options were evaluated:

**Option 1 — HTTP header / middleware injection**
Client sends `X-Project-Id` header; `SessionStateMiddleware` resolves the right client.
Tool signatures stay unchanged.
*Problem:* The LLM does not see which project it is operating on. Switching projects
mid-conversation requires the client to change a hidden header — impractical in an MCP
conversation, and invisible in the conversation history.

**Option 2 — Explicit `project_id` tool argument** ✅ chosen
Each project-scoped tool declares `project_id` as a required parameter.
*Benefits:* Fully transparent to the LLM; the agent explicitly states which project each
call targets; multi-project calls within one conversation are unambiguous; reviewable in
conversation history; natural for cross-project reasoning ("compare tables in A vs B").
*Cost:* All project-scoped tool signatures change. Centralised via a new helper
`KeboolaClient.from_project(state, project_id)`.

**Backward compatibility:** In single-project modes (PAT / OAuth / Storage Token) and in
multi-project mode with exactly one project registered, `project_id` may be made optional
— the helper defaults to the only active project. When two or more projects are active and
`project_id` is omitted, the call raises a `ToolError` explaining the ambiguity. This
preserves existing single-project prompts without modification. See the `from_project`
helper in the Tool Signature Changes section for the full implementation.

---

## Tool Signature Changes

### New helper

```python
# In KeboolaClient
@classmethod
def from_project(cls, state: dict, project_id: str | None = None) -> 'KeboolaClient':
    registry = state.get("project_registry", {})
    if project_id is None:
        if len(registry) == 1:
            project_id = next(iter(registry))
        elif len(registry) == 0:
            raise ToolError("No project is active. Call add_project_to_session first.")
        else:
            raise ToolError(
                "Multiple projects are active. Specify project_id explicitly."
            )
    if project_id not in registry:
        raise ToolError(
            f"Project '{project_id}' is not active in this session. "
            "Call add_project_to_session(project_id=…) first."
        )
    return registry[project_id].sapi_client
```

### Before / after

```python
# BEFORE
async def get_tables(ctx: Context) -> list[Table]:
    client = KeboolaClient.from_state(ctx.session.state)
    …

# AFTER
async def get_tables(
    ctx: Context,
    project_id: Annotated[str, Field(description="ID of the project to query.")],
) -> list[Table]:
    client = KeboolaClient.from_project(ctx.session.state, project_id)
    …
```

### Tools that do NOT receive `project_id`

| Tool | Reason |
|---|---|
| `list_accessible_projects` | New; org-level, no project scope |
| `add_project_to_session` | New; org-level, no project scope |
| `find_component_id` | Component catalogue is global |
| `docs_query` | Documentation is global |
| `get_flow_schema` | Schema is global |

---

## New Tools

### `list_accessible_projects`

```
Auth:     programmatic token required
Readonly: true
Tags:     project, multi-project

Returns:  list of { project_id, project_name, region, storage_api_url }

Error if auth_mode != "programmatic":
  "list_accessible_projects requires a programmatic token.
   In PAT / OAuth / Storage Token mode the project is already active —
   call get_project_info instead."
```

### `add_project_to_session`

```
Auth:     programmatic token required
Readonly: true  (purely in-memory; no external API call)
Tags:     project, multi-project

Parameters:
  project_id: str

Behaviour:
  1. Verify project_id is in the accessible projects list (in-memory check).
  2. If already in registry → return { status: "already_active" }  (idempotent).
  3. Create KeboolaClient (using the programmatic token) + WorkspaceManager for this project.
  4. Append ProjectEntry to project_registry (append-only).
  5. Return { project_id, project_name, region, status: "added" }

Error: project not accessible → ToolError "Project '<id>' is not accessible with the
       current programmatic token."
```

---

## Access Control Matrix

| Credential | `list_accessible_projects` | `add_project_to_session` | Project-scoped tools |
|---|---|---|---|
| PAT | ❌ not applicable | ❌ not applicable | ✅ PAT's project only |
| Programmatic, 0 projects added | ✅ | ✅ | ❌ ToolError "add a project first" |
| Programmatic, ≥1 project added | ✅ | ✅ | ✅ any registered project |
| OAuth / Storage Token | ❌ not applicable | ❌ not applicable | ✅ token's project only |

---

## Kai Integration

### Current

```
Kai → Storage Token (single project) → MCP Server → 1 project
```

### Proposed

```
Kai
 │  --programmatic-token <tok> --refresh-token <rtok>
 ▼
MCP Server
 ├─► Login API: validate
 ├─► Management API: list all projects
 └─► Background token refresh

 │  list_accessible_projects()   →  [A, B, C, …]
 │  add_project_to_session(A)    →  in-memory register only
 │  add_project_to_session(B)    →  in-memory register only

 │  get_tables(project_id=A)     →  Storage API (programmatic token, project A)
 │  run_job(project_id=B)        →  Jobs API   (programmatic token, project B)
```

---

## New CLI Arguments

```
--programmatic-token STR    Programmatic token obtained via external login (POST /login).
                            Enables multi-project mode. Requires --refresh-token.
--refresh-token STR         Refresh token paired with --programmatic-token.
--pat-token STR             Personal Access Token (single-project, never expires).
                            Equivalent to --storage-token but signals PAT semantics.
```

**Environment variable equivalents:**
```
KBC_PROGRAMMATIC_TOKEN
KBC_REFRESH_TOKEN
KBC_PAT_TOKEN
```

**Mode selection (mutually exclusive):**
```
if programmatic_token + refresh_token  → multi-project mode
elif pat_token OR storage_token        → single-project mode
elif OAuth (via HTTP headers)          → single-project mode (existing)
else                                   → error: no credentials
```

---

## Session Lifecycle

```
Server start
     │
     ├─ OAuth / PAT / Storage Token
     │       └─ Single-project mode
     │               registry = { project_X: auto-registered }
     │               All project tools immediately available
     │
     └─ Programmatic token + refresh token
             └─ Multi-project mode
                     registry = {}  (empty)
                     Background refresh task started

                     ┌───────────────────────────────────┐
                     │ list_accessible_projects   ✅     │
                     │ add_project_to_session     ✅     │
                     │ All project-scoped tools   ❌ ToolError │
                     └───────────────────────────────────┘
                                     │
                       add_project_to_session(A)
                                     ▼
                             registry = { A }
                             Project A tools ✅
                                     │
                       add_project_to_session(B)
                                     ▼
                             registry = { A, B }
                             Project A + B tools ✅

                     [idle > 24 h without any tool call]
                                     ▼
                             Token refresh stops
                             Next tool call → ToolError "Session expired"
```

---

## Prompting Changes

When `auth_mode == "programmatic"` the system prompt must include:

> This MCP server is connected via a programmatic token and can access multiple projects.
> Before using any project-scoped tool you **must**:
> 1. Call `list_accessible_projects` to see available projects.
> 2. Call `add_project_to_session(project_id=<id>)` for each project you need to work with.
> 3. Call `get_project_info(project_id=<id>)` to load each project's context and instructions.
>
> Always specify `project_id` in every subsequent tool call. If a call fails with
> "project not active", add the project first using `add_project_to_session`.
> You cannot remove a project from the session once added — this is intentional.

---

## Open Questions

1. **Management API base URL and caching** — At startup (programmatic mode) the server calls
   the Management API once to enumerate accessible projects and caches the result in session
   state. `list_accessible_projects` returns from that cache — it does **not** re-call the
   API on each invocation. Staleness is acceptable for the session lifetime (a restart picks
   up any new projects). Confirm whether the endpoint is on `manage.keboola.com` or on the
   same `connection.<region>.keboola.com` host; may need a new `--management-api-url` CLI arg.

2. **Multiple PATs at startup** — Should the server accept multiple `--pat-token` args (one
   per project, all registered at startup)? Deferred — programmatic token is the primary
   multi-project path.

3. **Branch support** — In multi-project mode, does each project get an independent branch
   setting, or is `branch_id` also a per-tool parameter alongside `project_id`?

4. **`get_project_info` backward compat** — Currently takes no `project_id`. Proposed:
   optional parameter that defaults to the only active project; error if ambiguous.

5. **MFA in headless Mode A** — For Kai the pre-obtained `token + refresh_token` pair is
   provisioned externally. The server never drives the MFA challenge. This must be documented
   for operators.
