FLOW_TOOLS_TAG = 'flows'
UPDATE_FLOW_TOOL_NAME = 'update_flow'
MODIFY_FLOW_TOOL_NAME = 'modify_flow'

# Tools allowed before the user has confirmed a project scope. Everything else is blocked with a
# message telling the assistant to ask the user which projects to work on first (ask-first UX).
# Shared by mcp.py's ToolsFilteringMiddleware and multiproject.py's MultiProjectMiddleware.
BOOTSTRAP_TOOLS = {'get_accessible_projects', 'set_project_scope', 'create_project'}

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
# Tools bound to the session's branch context: they declare no `project_id` (the session branch is the implicit
# argument when `merge_request_id` is omitted), so the multi-project middleware always targets the active project
# with them instead of asking which project (see `MultiProjectMiddleware._dispatch_single_target`).
MERGE_REQUEST_SESSION_BOUND_TOOLS = {
    'create_merge_request',
    'request_merge_request_review',
    'merge_merge_request',
    'get_merge_request_conflicts',
    'resolve_merge_request_conflict',
}
# Tools whose implicit argument is the session's own development branch: usable only from a development-branch
# session. Only `create_merge_request` is left here. Merge / request-review / conflicts / resolve act on an explicit
# `merge_request_id` from a production session too, because kbc-ui blocks Kai in a branch whose merge request is
# `in_review` or `approved` (K1) -- exactly when those steps are needed -- and the kbagent CLI allows the same. The
# tool stays visible on production (its description states the constraint) and is denied at call time only.
MERGE_REQUEST_BRANCH_ONLY_TOOLS = {'create_merge_request'}
# The call-time denial for MERGE_REQUEST_BRANCH_ONLY_TOOLS on a production session. Deliberately branch-agnostic:
# `authorize_tool_call` has no branch in scope.
MERGE_REQUEST_BRANCH_ONLY_MESSAGE = (
    'create_merge_request runs only from a development-branch chat: the merge request is created for the branch '
    'the chat is on, and this chat is on production. Tell the user to open a chat on the development branch and ask '
    'again there.'
)
