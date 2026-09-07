"""Merge-request tools: the Keboola Branches 2.0 (non-SOX) promotion flow for a development branch.

Read and review-state tools work from any session. Tools that touch or promote branch content (create,
request review, merge, conflicts, resolve) run only from a development-branch session; the middleware
denies them on production (see `ToolsFilteringMiddleware.authorize_tool_call`), and their descriptions
say so up front so the model can hand the user off.
"""

import asyncio
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated, Any

import httpx
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import Field, ValidationError

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import (
    TOKEN_INFO_STATE_KEY,
    KeboolaMcpServer,
    process_concurrently,
    toon_serializer_compact,
    unwrap_results,
)
from keboola_mcp_server.scope import ProjectIdArg
from keboola_mcp_server.tools.constants import MERGE_REQUEST_BRANCH_ONLY_MESSAGE, MERGE_REQUEST_TOOLS_TAG
from keboola_mcp_server.tools.merge_requests.conflicts import (
    DIFF_CONTENT_KEYS,
    build_config_conflict,
    diff_warnings,
    envelope_holes,
)
from keboola_mcp_server.tools.merge_requests.models import (
    ActivityEvent,
    AutoMergeStrategy,
    ChangedConfig,
    ConfigConflict,
    ConflictRef,
    MergeRequest,
    MergeRequestConflictsOutput,
    MergeRequestDetail,
    MergeRequestsDetailOutput,
    MergeRequestsListOutput,
    MergeRequestStateFilter,
    MergeResult,
    ResolveConflictResult,
    ResolvedConfiguration,
    TakeMode,
)
from keboola_mcp_server.tools.merge_requests.status import (
    LastRefusal,
    SessionContext,
    build_next_step,
    build_status,
    same_id,
)

LOG = logging.getLogger(__name__)

MERGE_CONFLICT_CODE = 'storage.mergeRequests.validation'
MERGE_NOT_READY_CODE = 'storage.mergeRequests.notReadyToMerge'
# Storage job statuses after which the job never changes again (same set as workspace.py's poller).
STORAGE_JOB_TERMINAL_STATUSES = frozenset({'success', 'error', 'warning', 'terminated', 'cancelled', 'canceled'})
MERGE_JOB_TIMEOUT_SEC = 600.0
MERGE_JOB_POLL_INTERVAL_SEC = 1.0

ROLE_DENIED_MESSAGE = (
    'Your project role may not permit this merge-request action (a project admin with role "admin" or "share" can '
    'do it), or the merge request is no longer open.'
)
BRANCH_ONLY_SENTENCE = (
    "Available only from a development-branch session; on production, tell the user to open a session on the "
    "merge request's source branch and ask again there."
)


def add_merge_request_tools(mcp: KeboolaMcpServer) -> None:
    """Add merge-request tools to the MCP server."""
    read_only = ToolAnnotations(readOnlyHint=True)
    destructive = ToolAnnotations(destructiveHint=True)
    write = ToolAnnotations(destructiveHint=False)
    for fn, annotations in (
        (get_merge_requests, read_only),
        (create_merge_request, destructive),
        (update_merge_request, destructive),
        (request_merge_request_review, write),
        (approve_merge_request, write),
        (request_merge_request_changes, write),
        (merge_merge_request, destructive),
        (get_merge_request_conflicts, read_only),
        (resolve_merge_request_conflict, write),
    ):
        mcp.add_tool(
            FunctionTool.from_function(
                fn, annotations=annotations, serializer=toon_serializer_compact, tags={MERGE_REQUEST_TOOLS_TAG}
            )
        )
    LOG.info('Merge-request tools added to the MCP server.')


# ---- shared plumbing --------------------------------------------------------------------------------------


@dataclass
class _MrContext:
    """Everything a merge-request tool needs besides the MR itself, loaded once per call."""

    client: KeboolaClient
    token_info: Mapping[str, Any]
    branches: list[JsonDict]
    links: ProjectLinksManager

    @property
    def admin_id(self) -> Any:
        admin = self.token_info.get('admin')
        return admin.get('id') if isinstance(admin, Mapping) else None

    @property
    def can_write(self) -> bool:
        admin = self.token_info.get('admin')
        role = str(admin.get('role') or '').lower() if isinstance(admin, Mapping) else ''
        if role == 'readonly' or getattr(self.client, 'readonly', False) is True:
            return False
        return role in ('admin', 'share') or bool(self.client.bearer_token)

    @property
    def branch_names(self) -> dict[str, str]:
        return {str(b['id']): str(b.get('name') or b['id']) for b in self.branches if 'id' in b}

    @property
    def default_branch(self) -> JsonDict:
        for branch in self.branches:
            if branch.get('isDefault'):
                return branch
        raise ToolError('The project has no default branch; cannot resolve the merge target.')

    @property
    def session_branch(self) -> JsonDict | None:
        """The session's development branch, or None on the production branch."""
        if self.client.branch_id is None:
            return None
        for branch in self.branches:
            if same_id(branch.get('id'), self.client.branch_id):
                return branch
        raise ToolError(f'Branch "{self.client.branch_id}" not found in the project; it may have been deleted.')

    def session(self, mr: Mapping[str, Any]) -> SessionContext:
        branch_from_id = (mr.get('branches') or {}).get('branchFromId')
        on_mr_branch = self.client.branch_id is not None and same_id(branch_from_id, self.client.branch_id)
        return SessionContext(on_mr_branch=on_mr_branch, can_write=self.can_write)

    def branch_from_name(self, mr: Mapping[str, Any]) -> str | None:
        branch_from_id = (mr.get('branches') or {}).get('branchFromId')
        return self.branch_names.get(str(branch_from_id)) if branch_from_id is not None else None

    def mr_links(self, mr: Mapping[str, Any]) -> list[Link]:
        branch_from_id = (mr.get('branches') or {}).get('branchFromId')
        if branch_from_id is None:
            return []
        return [self.links.get_merge_request_link(branch_from_id, str(mr.get('title') or ''))]


async def resolve_branch_pair(client: KeboolaClient) -> tuple[JsonDict | None, JsonDict]:
    """
    One `branches_list` call resolving both the session branch (None on production) and the default branch.
    Unlike `tools.project._resolve_branch_context`, it returns the default branch even while on a dev branch.
    """
    ctx = _MrContext(client=client, token_info={}, branches=await client.storage_client.branches_list(), links=None)  # type: ignore[arg-type]
    return ctx.session_branch, ctx.default_branch


async def _load(ctx: Context) -> _MrContext:
    client = KeboolaClient.from_state(ctx.session.state)
    token_info = ctx.session.state.get(TOKEN_INFO_STATE_KEY) if hasattr(ctx.session.state, 'get') else None
    if not isinstance(token_info, Mapping):
        # Not called through ToolsFilteringMiddleware (which caches the verification for this call).
        token_info = await client.storage_client.verify_token()
    branches = await client.storage_client.branches_list()
    owner = token_info.get('owner')
    project_id = str(owner.get('id')) if isinstance(owner, Mapping) else await client.storage_client.project_id()
    # Project-level links (no session-branch prefix): a merge request lives on its own source branch.
    links = ProjectLinksManager(base_url=client.storage_api_url, project_id=project_id, branch_id=None)
    return _MrContext(client=client, token_info=token_info, branches=branches, links=links)


def _error_body(exc: httpx.HTTPStatusError) -> dict[str, Any]:
    try:
        body = exc.response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _map_write_error(exc: httpx.HTTPStatusError) -> ToolError | None:
    """Every MR write maps the backend's 403 onto one clear message; other statuses are left to the caller."""
    if exc.response.status_code == 403:
        body = _error_body(exc)
        error = body.get('error') or body.get('message')
        detail = f'The backend refused it: {error} ' if error else ''
        return ToolError(f'{detail}{ROLE_DENIED_MESSAGE}')
    return None


def _parse_conflicts(raw: Any) -> list[ConflictRef]:
    if not isinstance(raw, list):
        return []
    return [ConflictRef.from_api(item) for item in raw if isinstance(item, Mapping)]


def _build_detail(
    c: _MrContext,
    mr: Mapping[str, Any],
    *,
    conflicts: Sequence[ConflictRef] | None,
    with_activity_log: bool,
    last_refusal: LastRefusal | None = None,
) -> MergeRequestDetail:
    summary = MergeRequest.from_api(mr, branch_names=c.branch_names, links=c.mr_links(mr))
    activity_log = mr.get('activityLog')
    return MergeRequestDetail(
        **summary.model_dump(),
        status=build_status(
            mr,
            conflicts=conflicts,
            admin_id=c.admin_id,
            session=c.session(mr),
            branch_from_name=c.branch_from_name(mr),
            last_refusal=last_refusal,
        ),
        changed_configurations=ChangedConfig.list_from_change_log(mr.get('changeLog')),
        activity_log=(
            [ActivityEvent.from_api(e) for e in activity_log if isinstance(e, Mapping)]
            if with_activity_log and isinstance(activity_log, list)
            else None
        ),
    )


async def _detail_with_conflicts(
    c: _MrContext, mr: Mapping[str, Any], *, with_activity_log: bool
) -> MergeRequestDetail:
    conflicts = _parse_conflicts(await c.client.storage_client.merge_request_conflicts(mr['id']))
    return _build_detail(c, mr, conflicts=conflicts, with_activity_log=with_activity_log)


async def _resolve_branch_mr(
    c: _MrContext, merge_request_id: int | None, *, include_activity_log: bool = False
) -> JsonDict:
    """
    The MR-id convention for branch-only tools: with the id omitted, the session branch's single MR; with the
    id given, the MR is validated to belong to the session branch — never act on another branch's MR.
    """
    if c.client.branch_id is None:
        raise ToolError(MERGE_REQUEST_BRANCH_ONLY_MESSAGE)
    if merge_request_id is None:
        rows = await c.client.storage_client.merge_requests_list()
        matches = [r for r in rows if same_id((r.get('branches') or {}).get('branchFromId'), c.client.branch_id)]
        if not matches:
            raise ToolError(
                'The current development branch has no merge request yet. Create one with create_merge_request, '
                'or pass merge_request_id if you meant another branch (open a session on that branch first).'
            )
        mr = matches[0]
        if include_activity_log:
            mr = await c.client.storage_client.merge_request_detail(mr['id'], include_activity_log=True)
        return mr
    mr = await c.client.storage_client.merge_request_detail(merge_request_id, include_activity_log=include_activity_log)
    branch_from_id = (mr.get('branches') or {}).get('branchFromId')
    if not same_id(branch_from_id, c.client.branch_id):
        name = c.branch_from_name(mr)
        where = f"branch '{name}' (id {branch_from_id})" if name else f'branch id {branch_from_id}'
        raise ToolError(
            f'Merge request {merge_request_id} belongs to {where}, not to the current session branch. '
            'Tell the user to open a session on that branch and ask again there.'
        )
    return mr


def _validate_auto_merge(strategy: str | None, at: str | None, *, require_pairing: bool) -> None:
    if strategy == 'scheduled' and not at:
        raise ToolError("auto_merge='scheduled' requires auto_merge_at (ISO-8601 date-time).")
    if require_pairing and at is not None and strategy != 'scheduled':
        raise ToolError("auto_merge_at is only meaningful with auto_merge='scheduled'.")


async def _await_storage_job(client: KeboolaClient, job_id: str) -> JsonDict | None:
    """Polls a Storage job until `success`/`error`; returns None on timeout (the job keeps running)."""
    deadline = time.monotonic() + MERGE_JOB_TIMEOUT_SEC
    while True:
        job = await client.storage_client.job_detail(job_id)
        if str(job.get('status') or '') in STORAGE_JOB_TERMINAL_STATUSES:
            return job
        if time.monotonic() >= deadline:
            return None
        await asyncio.sleep(MERGE_JOB_POLL_INTERVAL_SEC)


def _validate_resolved(resolved: Mapping[str, Any]) -> ResolvedConfiguration:
    """Strict validation of a caller-authored body: all five keys, non-empty name, a real boolean is_disabled."""
    try:
        return ResolvedConfiguration.model_validate(dict(resolved))
    except ValidationError as exc:
        problems = '; '.join(f"{'.'.join(str(p) for p in e['loc']) or 'body'}: {e['msg']}" for e in exc.errors())
        raise ToolError(
            'The resolved configuration must spell out the full replaced content (a rebase REPLACES the version): '
            f'{problems}. Required keys: name, description (may be null), is_disabled (boolean), configuration, rows.'
        ) from exc


def _job_error_message(job: Mapping[str, Any]) -> str:
    error = job.get('error')
    if isinstance(error, Mapping) and error.get('message'):
        return str(error['message'])
    if error:
        return str(error)
    return f"The merge job ended with status '{job.get('status')}'."


# ---- Read / diagnosis — any session, any role ------------------------------------------------------------


@tool_errors()
async def get_merge_requests(
    ctx: Context,
    merge_request_ids: Annotated[
        Sequence[int],
        Field(description='Merge request ids to get the full detail of. Leave empty to list all merge requests.'),
    ] = (),
    state: Annotated[
        MergeRequestStateFilter | None,
        Field(
            description=(
                'List mode only: keep merge requests whose state or derived state equals this value '
                "(e.g. 'in_development', 'approved', 'rejected', 'merged'). Ignored when ids are given."
            )
        ),
    ] = None,
    project_id: ProjectIdArg = None,
) -> MergeRequestsListOutput | MergeRequestsDetailOutput:
    """
    Lists the project's merge requests, or returns the full detail of the given ones.

    A merge request promotes a development branch's configuration changes into production. Works from any
    session (production or branch).

    Without ids: summaries of all merge requests (title, state, who created it, reviewers, source branch).
    With ids: per merge request the status block (`derived_state`, `merge_blockers`, `mergeable`,
    `allowed_actions`, `viewer`, live `conflicts`, and `next_step` — the single recommended next action),
    the changed configurations and the review history. Follow `next_step` verbatim when guiding the user.

    Usage:
    - "Is there anything to merge?" → list (optionally with `state`), then get the candidates' detail and read
      `merge_blockers` / `next_step`.
    - "What is in merge request 42?" → detail of [42]; narrate `changed_configurations` and `activity_log`.
    """
    c = await _load(ctx)
    if merge_request_ids:

        async def _one(mr_id: int) -> MergeRequestDetail:
            mr, conflicts_raw = await asyncio.gather(
                c.client.storage_client.merge_request_detail(mr_id, include_activity_log=True),
                c.client.storage_client.merge_request_conflicts(mr_id),
            )
            return _build_detail(c, mr, conflicts=_parse_conflicts(conflicts_raw), with_activity_log=True)

        results = await process_concurrently(list(dict.fromkeys(merge_request_ids)), _one)
        return MergeRequestsDetailOutput(merge_requests=unwrap_results(results, 'Failed to get merge requests'))

    rows = await c.client.storage_client.merge_requests_list()
    merge_requests = [MergeRequest.from_api(r, branch_names=c.branch_names, links=c.mr_links(r)) for r in rows]
    if state is not None:
        wanted = state.lower()
        merge_requests = [m for m in merge_requests if wanted in (m.state.lower(), m.derived_state.lower())]
    return MergeRequestsListOutput(merge_requests=merge_requests, links=[c.links.get_project_detail_link()])


# ---- Review-state actions — any session, admin/share/OAuth -----------------------------------------------


@tool_errors()
async def approve_merge_request(
    ctx: Context,
    merge_request_id: Annotated[int, Field(description='The merge request id.')],
    project_id: ProjectIdArg = None,
) -> MergeRequestDetail:
    """
    Approves a merge request as a reviewer.

    Valid only while the merge request is `in_review` (the backend rejects it otherwise) and never for its
    creator. Projects with the default of 0 required approvals never enter `in_review`, so this is needed only
    when the project requires approvals. Works from any session. Returns the merge request with its status;
    follow `next_step`.
    """
    c = await _load(ctx)
    try:
        mr = await c.client.storage_client.merge_request_approve(merge_request_id)
    except httpx.HTTPStatusError as exc:
        if mapped := _map_write_error(exc):
            raise mapped from exc
        raise
    return await _detail_with_conflicts(c, mr, with_activity_log=False)


@tool_errors()
async def request_merge_request_changes(
    ctx: Context,
    merge_request_id: Annotated[int, Field(description='The merge request id.')],
    reason: Annotated[
        str | None, Field(description='Why changes are needed; recorded in the review history for the author.')
    ] = None,
    project_id: ProjectIdArg = None,
) -> MergeRequestDetail:
    """
    Sends a merge request back to its author for changes (a reviewer's "reject").

    The merge request returns to `development` and loses its approvals; it is NOT closed — the author fixes the
    branch and merges (or requests a review) again. Works from any session. Returns the merge request with its
    status; follow `next_step`.
    """
    c = await _load(ctx)
    try:
        mr = await c.client.storage_client.merge_request_request_changes(merge_request_id, reason=reason)
    except httpx.HTTPStatusError as exc:
        if mapped := _map_write_error(exc):
            raise mapped from exc
        raise
    return await _detail_with_conflicts(c, mr, with_activity_log=False)


@tool_errors()
async def update_merge_request(
    ctx: Context,
    merge_request_id: Annotated[int, Field(description='The merge request id.')],
    title: Annotated[str | None, Field(description='New title. Omit to keep the current one.')] = None,
    description: Annotated[
        str | None, Field(description='New description. Omit to keep the current one; an empty string clears it.')
    ] = None,
    reviewer_ids: Annotated[
        Sequence[int] | None, Field(description='New complete list of reviewer user ids. Omit to keep the current one.')
    ] = None,
    auto_merge: Annotated[
        AutoMergeStrategy | None,
        Field(
            description=(
                "'none' turns auto-merge OFF (the only way to disarm it). 'immediately' and 'scheduled' ARM it: "
                'the backend merges on its own once the merge request is approved (at auto_merge_at for '
                "'scheduled'). Omit to keep the current setting."
            )
        ),
    ] = None,
    auto_merge_at: Annotated[
        str | None,
        Field(description="ISO-8601 date-time of a 'scheduled' auto-merge. Required with auto_merge='scheduled'."),
    ] = None,
    project_id: ProjectIdArg = None,
) -> MergeRequestDetail:
    """
    Updates a merge request's title, description, reviewers or auto-merge setting. Omitted fields are kept.

    Use it to disarm auto-merge (`auto_merge='none'`) or to change reviewers. Arming auto-merge
    (`'immediately'` / `'scheduled'`) makes the backend merge without a further confirmation — confirm with
    the user first. Not allowed once the merge request is merged or canceled. Works from any session.
    Returns the merge request with its status; follow `next_step`.
    """
    _validate_auto_merge(auto_merge, auto_merge_at, require_pairing=False)
    payload: JsonDict = {}
    if title is not None:
        payload['title'] = title
    if description is not None:
        payload['description'] = description
    if reviewer_ids is not None:
        payload['reviewerIds'] = list(reviewer_ids)
    if auto_merge is not None:
        payload['autoMergeStrategy'] = auto_merge
    if auto_merge_at is not None:
        payload['autoMergeAt'] = auto_merge_at
    if not payload:
        raise ToolError(
            'Nothing to update: pass at least one of title, description, reviewer_ids, auto_merge, auto_merge_at.'
        )

    c = await _load(ctx)
    try:
        mr = await c.client.storage_client.merge_request_update(merge_request_id, payload)
    except httpx.HTTPStatusError as exc:
        if mapped := _map_write_error(exc):
            raise mapped from exc
        raise
    return await _detail_with_conflicts(c, mr, with_activity_log=False)


# ---- Author / promotion — development-branch session only ------------------------------------------------


@tool_errors()
async def create_merge_request(
    ctx: Context,
    title: Annotated[str, Field(description='A short title describing the change.')],
    description: Annotated[str | None, Field(description='An optional longer description for the reviewers.')] = None,
    reviewer_ids: Annotated[Sequence[int], Field(description='User ids of the reviewers to request. Optional.')] = (),
    auto_merge: Annotated[
        AutoMergeStrategy,
        Field(
            description=(
                "'none' (default) = the user merges explicitly. 'immediately' / 'scheduled' ARM auto-merge: the "
                'backend merges on its own once approvals suffice (at auto_merge_at for scheduled). Confirm with the '
                'user before arming.'
            )
        ),
    ] = 'none',
    auto_merge_at: Annotated[
        str | None, Field(description="ISO-8601 date-time; required if and only if auto_merge='scheduled'.")
    ] = None,
    project_id: ProjectIdArg = None,
) -> MergeRequestDetail:
    """
    Creates a merge request for the CURRENT development branch into production.

    Available only from a development-branch session; on production, tell the user to open a session on the
    merge request's source branch and ask again there. The source branch is the session branch and the target
    is production — there are no branch parameters. A branch can have only one merge request.

    On a project with the default of 0 required approvals the happy path is two calls:
    create_merge_request → merge_merge_request (no review step). Returns the merge request with its status;
    follow `next_step`.
    """
    _validate_auto_merge(auto_merge, auto_merge_at, require_pairing=True)
    c = await _load(ctx)
    session_branch = c.session_branch
    if session_branch is None:
        raise ToolError(MERGE_REQUEST_BRANCH_ONLY_MESSAGE)
    default_branch = c.default_branch
    if same_id(session_branch.get('id'), default_branch.get('id')):
        raise ToolError(MERGE_REQUEST_BRANCH_ONLY_MESSAGE)
    try:
        mr = await c.client.storage_client.merge_request_create(
            branch_from_id=session_branch['id'],
            branch_into_id=default_branch['id'],
            title=title,
            description=description,
            reviewer_ids=reviewer_ids,
            auto_merge_strategy=auto_merge,
            auto_merge_at=auto_merge_at,
        )
    except httpx.HTTPStatusError as exc:
        if mapped := _map_write_error(exc):
            raise mapped from exc
        if 400 <= exc.response.status_code < 500:
            rows = await c.client.storage_client.merge_requests_list()
            existing = [r for r in rows if same_id((r.get('branches') or {}).get('branchFromId'), session_branch['id'])]
            if existing:
                detail = await _detail_with_conflicts(c, existing[0], with_activity_log=False)
                raise ToolError(
                    f"Branch '{session_branch.get('name')}' already has merge request {detail.id} "
                    f"('{detail.title}', state {detail.derived_state}); a branch can have only one. "
                    f'Next step: {detail.status.next_step}'
                ) from exc
        raise
    return await _detail_with_conflicts(c, mr, with_activity_log=False)


@tool_errors()
async def request_merge_request_review(
    ctx: Context,
    merge_request_id: Annotated[
        int | None,
        Field(description="The merge request id. Omit to use the current branch's merge request."),
    ] = None,
    project_id: ProjectIdArg = None,
) -> MergeRequestDetail:
    """
    Sends the current branch's merge request for review (author action).

    Available only from a development-branch session; on production, tell the user to open a session on the
    merge request's source branch and ask again there. Rarely needed: merge_merge_request already skips the
    review when the project requires no approvals, so call this only when a merge was refused as "not ready"
    or when the project requires approvals. The merge request moves to `in_review` (or straight to `approved`
    when 0 approvals are required). Returns the merge request with its status; follow `next_step`.
    """
    c = await _load(ctx)
    mr = await _resolve_branch_mr(c, merge_request_id)
    try:
        updated = await c.client.storage_client.merge_request_request_review(mr['id'])
    except httpx.HTTPStatusError as exc:
        if mapped := _map_write_error(exc):
            raise mapped from exc
        raise
    return await _detail_with_conflicts(c, updated, with_activity_log=False)


@tool_errors()
async def merge_merge_request(
    ctx: Context,
    merge_request_id: Annotated[
        int | None,
        Field(description="The merge request id. Omit to use the current branch's merge request."),
    ] = None,
    project_id: ProjectIdArg = None,
) -> MergeResult:
    """
    Merges the current branch's merge request into production and waits for the merge to finish.

    Available only from a development-branch session; on production, tell the user to open a session on the
    merge request's source branch and ask again there. IRREVERSIBLE: on success the changes are in production
    and the source branch (with everything else on it: buckets, tables, workspaces) is deleted by a background
    job — confirm with the user first. Works directly from `development` when the project requires no
    approvals.

    When the backend refuses, nothing changes and the result explains why: `refusal='conflicts'` lists the
    conflicting configurations (resolve them with get_merge_request_conflicts / resolve_merge_request_conflict,
    then merge again); `refusal='not_ready'` means missing approvals, a merge lock or a wrong state. After a
    success the session's branch is gone: follow `next_step` and tell the user to open a production session.
    """
    c = await _load(ctx)
    mr = await _resolve_branch_mr(c, merge_request_id)  # `branchFromId` captured before the merge (nulled later)
    mr_id = int(mr['id'])
    session = c.session(mr)
    branch_name = c.branch_from_name(mr)

    try:
        job = await c.client.storage_client.merge_request_merge(mr_id)
    except httpx.HTTPStatusError as exc:
        if mapped := _map_write_error(exc):
            raise mapped from exc
        if exc.response.status_code != 409:
            raise
        body = _error_body(exc)
        code = body.get('code')
        message = str(body.get('error') or body.get('message') or exc)
        conflicts: list[ConflictRef] | None
        refusal: LastRefusal
        if code == MERGE_NOT_READY_CODE:
            refusal, conflicts = 'not_ready', None
        elif code == MERGE_CONFLICT_CODE or code is None:
            refusal = 'conflicts'
            params = body.get('params')
            conflicts = _parse_conflicts(params.get('errors') if isinstance(params, Mapping) else None)
            if not conflicts:
                # A conflict 409 without a usable `params.errors` (older stack, proxy body): fetch the live list so
                # the status never claims "mergeable" right after the backend refused the merge.
                conflicts = _parse_conflicts(await c.client.storage_client.merge_request_conflicts(mr_id))
        else:
            raise
        status = build_status(
            mr,
            conflicts=conflicts,
            admin_id=c.admin_id,
            session=session,
            branch_from_name=branch_name,
            last_refusal=refusal,
            refusal_message=message,
        )
        return MergeResult(
            merge_request_id=mr_id,
            merged=False,
            state=status.state,
            job_id=None,
            refusal=refusal,
            refusal_message=message,
            conflicts=conflicts,
            status=status,
            source_branch_deleting=False,
            warnings=[],
            next_step=status.next_step,
        )

    job_id = str(job['id'])
    final = await _await_storage_job(c.client, job_id)
    if final is None:
        in_merge = {**mr, 'state': 'in_merge'}
        status = build_status(
            in_merge, conflicts=None, admin_id=c.admin_id, session=session, branch_from_name=branch_name
        )
        return MergeResult(
            merge_request_id=mr_id,
            merged=False,
            state='in_merge',
            job_id=job_id,
            refusal=None,
            refusal_message=None,
            conflicts=None,
            status=status,
            source_branch_deleting=False,
            warnings=[f'The merge job {job_id} is still running after {int(MERGE_JOB_TIMEOUT_SEC)} s.'],
            next_step=(
                f'The merge is still running (Storage job {job_id}); check again with get_merge_requests in a moment '
                'and do not edit the branch meanwhile.'
            ),
        )
    if str(final.get('status')) == 'success':
        return MergeResult(
            merge_request_id=mr_id,
            merged=True,
            state='published',
            job_id=job_id,
            refusal=None,
            refusal_message=None,
            conflicts=None,
            status=None,
            source_branch_deleting=True,
            warnings=[],
            next_step=build_next_step(
                state='published',
                derived_state='merged',
                merge_blockers=['state'],
                conflicts=None,
                allowed_actions=[],
                viewer=build_status(
                    mr, conflicts=None, admin_id=c.admin_id, session=session, branch_from_name=branch_name
                ).viewer,
                pending=[],
                session=session,
                branch_from_name=branch_name,
            ),
        )
    rolled_back = {**mr, 'state': 'approved'}  # the backend rolls a failed merge back to `approved`
    status = build_status(
        rolled_back, conflicts=None, admin_id=c.admin_id, session=session, branch_from_name=branch_name
    )
    return MergeResult(
        merge_request_id=mr_id,
        merged=False,
        state='approved',
        job_id=job_id,
        refusal=None,
        refusal_message=_job_error_message(final),
        conflicts=None,
        status=status,
        source_branch_deleting=False,
        warnings=[],
        next_step=f'The merge job failed ({_job_error_message(final)}); the merge request is back in `approved`. {status.next_step}',
    )


# ---- Conflict resolution — development-branch session only -----------------------------------------------


@tool_errors()
async def get_merge_request_conflicts(
    ctx: Context,
    merge_request_id: Annotated[
        int | None,
        Field(description="The merge request id. Omit to use the current branch's merge request."),
    ] = None,
    project_id: ProjectIdArg = None,
) -> MergeRequestConflictsOutput:
    """
    Shows what blocks the current branch's merge request from merging: each conflicting configuration with its
    three-way diff and a per-path classification.

    Available only from a development-branch session; on production, tell the user to open a session on the
    merge request's source branch and ask again there. A configuration conflicts when it changed both on the
    branch and in production since the branch was created. For each one you get `base` / `ours` (branch) /
    `theirs` (production), `changes` (each path tagged `changed_by` ours|theirs|both), `conflicting_paths`
    (both sides changed differently — the actual conflict) and `suggested_take` when one side is a safe pick.
    Walk the user through them one by one and resolve each with resolve_merge_request_conflict. Follow `next_step`.
    """
    c = await _load(ctx)
    mr = await _resolve_branch_mr(c, merge_request_id)
    mr_id = int(mr['id'])
    refs = _parse_conflicts(await c.client.storage_client.merge_request_conflicts(mr_id))

    async def _one(ref: ConflictRef) -> ConfigConflict:
        diff = await c.client.storage_client.configuration_diff(ref.component_id, ref.configuration_id)
        return build_config_conflict(ref, diff)

    conflicts = unwrap_results(await process_concurrently(refs, _one), 'Failed to load configuration diffs')
    status = build_status(
        mr, conflicts=refs, admin_id=c.admin_id, session=c.session(mr), branch_from_name=c.branch_from_name(mr)
    )
    return MergeRequestConflictsOutput(
        merge_request_id=mr_id, conflicts=conflicts, status=status, next_step=status.next_step
    )


@tool_errors()
async def resolve_merge_request_conflict(
    ctx: Context,
    component_id: Annotated[str, Field(description='The component id of the conflicting configuration.')],
    configuration_id: Annotated[str, Field(description='The configuration id.')],
    take: Annotated[
        TakeMode | None,
        Field(
            description=(
                "'ours' keeps the branch version, 'theirs' takes the production version, 'delete' deletes the "
                "configuration. Taking one side DISCARDS the other side's changes. Pass exactly one of take / resolved."
            )
        ),
    ] = None,
    resolved: Annotated[
        dict[str, Any] | None,
        Field(
            description=(
                'The manually merged content when neither side alone is right, as an object with ALL of these keys: '
                '"name" (non-empty string), "description" (string or null), "is_disabled" (boolean), '
                '"configuration" (object) and "rows" (array of row objects, complete) — the rebase replaces the whole '
                'version, so nothing may be omitted.'
            )
        ),
    ] = None,
    change_description: Annotated[
        str | None, Field(description="The new version's change description (any mode; ignored for 'delete').")
    ] = None,
    merge_request_id: Annotated[
        int | None,
        Field(description="The merge request id. Omit to use the current branch's merge request."),
    ] = None,
    project_id: ProjectIdArg = None,
) -> ResolveConflictResult:
    """
    Resolves ONE conflicting configuration of the current branch's merge request by re-anchoring the branch
    configuration onto the current production version with the chosen content.

    Available only from a development-branch session; on production, tell the user to open a session on the
    merge request's source branch and ask again there. Call get_merge_request_conflicts first, then for each
    conflict have the user choose: `take='ours'` / `'theirs'` / `'delete'`, or pass `resolved` with the
    hand-merged content. Taking one side discards the other side's changes — say so. The configuration must
    be in the merge request's live conflict set. `remaining_conflicts` tells you whether to continue the loop;
    approvals survive the resolution. Follow `next_step`.
    """
    if (take is None) == (resolved is None):
        raise ToolError("Pass exactly one of take='ours'|'theirs'|'delete' or a resolved configuration.")
    custom = _validate_resolved(resolved) if resolved is not None else None  # strict, before any network call

    c = await _load(ctx)
    mr = await _resolve_branch_mr(c, merge_request_id)
    mr_id = int(mr['id'])

    live = _parse_conflicts(await c.client.storage_client.merge_request_conflicts(mr_id))
    if not any(r.component_id == component_id and r.configuration_id == str(configuration_id) for r in live):
        raise ToolError(
            f'{component_id}/{configuration_id} is not in merge request {mr_id}\'s conflict set; nothing to resolve. '
            'Call get_merge_request_conflicts for the current set.'
        )

    diff = await c.client.storage_client.configuration_diff(component_id, configuration_id)
    theirs = diff.get('theirs')
    onto_version = theirs.get('version') if isinstance(theirs, Mapping) else None
    if onto_version is None:
        raise ToolError(
            f'The diff of {component_id}/{configuration_id} has no production (theirs) side; cannot determine the '
            'version to rebase onto.'
        )

    warnings = list(diff_warnings(diff))
    body: dict[str, Any]
    mode: str
    if custom is not None:
        body = {
            'name': custom.name,
            'description': custom.description,
            'isDisabled': custom.is_disabled,
            'configuration': custom.configuration,
            'rows': custom.rows,
        }
        mode = 'custom'
    elif take == 'delete':
        body, mode = {}, 'delete'
    else:
        side = diff.get('ours') if take == 'ours' else theirs
        if not isinstance(side, Mapping):
            # An absent side is not a deletion: never turn it into a tombstone behind the user's back.
            raise ToolError(
                f"The diff has no {take} side: the configuration does not exist on that branch. "
                "Use take='delete' to delete it explicitly, or pass the content as `resolved`."
            )
        if side.get('isDeleted'):
            body, mode = {}, 'delete'  # taking a deleted side IS the delete resolution
        else:
            holes = envelope_holes(side)
            envelope = side.get('diff') or {}
            if holes or not str(envelope.get('name') or '').strip():
                raise ToolError(
                    f"The diff's {take} side carries no {', '.join(holes) or 'name'}; cannot compose the content from it "
                    '(backend envelope hole). Pass the resolution as `resolved` instead.'
                )
            if not isinstance(envelope.get('isDisabled'), bool):
                raise ToolError(
                    f"The diff's {take} side carries a non-boolean isDisabled ({envelope.get('isDisabled')!r}). "
                    'Pass the resolution as `resolved` instead.'
                )
            body = {key: envelope[key] for key in DIFF_CONTENT_KEYS if key in envelope}
            mode = take  # type: ignore[assignment]

    if body:
        if change_description is not None:
            body['changeDescription'] = change_description
    elif change_description is not None:
        warnings.append(
            'change_description ignored: the delete resolution cannot carry one; the backend records its default message.'
        )

    try:
        await c.client.storage_client.configuration_rebase(
            component_id, configuration_id, version=int(onto_version), diff=body
        )
    except httpx.HTTPStatusError as exc:
        if mapped := _map_write_error(exc):
            raise mapped from exc
        raise

    remaining = _parse_conflicts(await c.client.storage_client.merge_request_conflicts(mr_id))
    status = build_status(
        mr,
        conflicts=remaining,
        admin_id=c.admin_id,
        session=c.session(mr),
        branch_from_name=c.branch_from_name(mr),
        remaining_conflicts=remaining,
    )
    return ResolveConflictResult(
        component_id=component_id,
        configuration_id=str(configuration_id),
        resolved=True,
        mode=mode,  # type: ignore[arg-type]
        rebased_onto_version=int(onto_version),
        remaining_conflicts=remaining,
        status=status,
        warnings=warnings,
        next_step=status.next_step,
    )
