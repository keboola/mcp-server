FLOW_TOOLS_TAG = 'flows'
UPDATE_FLOW_TOOL_NAME = 'update_flow'
MODIFY_FLOW_TOOL_NAME = 'modify_flow'

# Tools allowed before the user has confirmed a project scope. Everything else is blocked with a
# message telling the assistant to ask the user which projects to work on first (ask-first UX).
# Shared by mcp.py's ToolsFilteringMiddleware and multiproject.py's MultiProjectMiddleware.
BOOTSTRAP_TOOLS = {'get_accessible_projects', 'set_project_scope', 'create_project'}

# The only tools of a session with `X-Workspace-Scope: strict` (see `Config.workspace_scope`). Each of them reads
# either the pinned workspace or Storage metadata the session's token can see; `search` is left out because it also
# lists configurations, flows and data apps.
STRICT_WORKSPACE_SCOPE_TOOLS = frozenset({'query_data', 'get_buckets', 'get_tables', 'get_project_info', 'docs_query'})

# Tag for tools supporting config diff preview feature
CONFIG_DIFF_PREVIEW_TAG = 'config-diff-preview'

# Tag for semantic layer tools
SEMANTIC_TOOLS_TAG = 'semantic'
