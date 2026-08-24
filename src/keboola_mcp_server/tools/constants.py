FLOW_TOOLS_TAG = 'flows'
UPDATE_FLOW_TOOL_NAME = 'update_flow'
MODIFY_FLOW_TOOL_NAME = 'modify_flow'

# Tools allowed before the user has confirmed a project scope. Everything else is blocked with a
# message telling the assistant to ask the user which projects to work on first (ask-first UX).
# Shared by mcp.py's ToolsFilteringMiddleware and multiproject.py's MultiProjectMiddleware.
BOOTSTRAP_TOOLS = {'get_accessible_projects', 'set_project_scope'}

# Tag for tools supporting config diff preview feature
CONFIG_DIFF_PREVIEW_TAG = 'config-diff-preview'

# Tag for semantic layer tools
SEMANTIC_TOOLS_TAG = 'semantic'
