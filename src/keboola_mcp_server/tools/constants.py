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

# Merge-request (Branches 2.0, non-SOX) tools. The tag drives TOOLS.md categorization and list-time
# filtering; the name sets are the call-time gating rules in mcp.py's `authorize_tool_call` (which
# receives a tool *name*, not a Tool). Keep them in sync with `tools/merge_requests/tools.py`.
MERGE_REQUEST_TOOLS_TAG = 'merge-request'
MERGE_REQUESTS_FEATURE = 'branches-merge-requests'
MERGE_REQUEST_TOOL_NAMES = {
    'get_merge_requests',
    'create_merge_request',
    'update_merge_request',
    'request_merge_request_review',
    'approve_merge_request',
    'request_merge_request_changes',
    'merge_merge_request',
    'get_merge_request_conflicts',
    'resolve_merge_request_conflict',
}
# Tools that touch or promote branch content: usable only from a development-branch session. They stay
# visible on production (their descriptions state the constraint) and are denied at call time only.
MERGE_REQUEST_BRANCH_ONLY_TOOLS = {
    'create_merge_request',
    'request_merge_request_review',
    'merge_merge_request',
    'get_merge_request_conflicts',
    'resolve_merge_request_conflict',
}
# The call-time denial for MERGE_REQUEST_BRANCH_ONLY_TOOLS on a production session. Deliberately
# branch-agnostic: `authorize_tool_call` has no branch name in scope; the named handoff ("open a session on
# branch 'reporting'") comes from the read tools' `next_step`, where `branch_from_name` is known.
MERGE_REQUEST_BRANCH_ONLY_MESSAGE = (
    'This tool runs only from a development-branch session. Tell the user to open a session on the merge '
    "request's source branch and ask again there."
)
