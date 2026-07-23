import asyncio
import logging
from typing import Annotated, Optional, cast

import httpx
from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.auth_login import exchange_scoped_token, get_access_token, introspect_token
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import SCOPE_KEY, MultiProjectMiddleware, ServerState, SessionScope, process_concurrently
from keboola_mcp_server.resources.prompts import get_project_system_prompt
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

PROJECT_TOOLS_TAG = 'project'


def add_project_tools(mcp: FastMCP) -> None:
    """Add project tools to the MCP server."""

    LOG.info(f'Adding tool {get_project_info.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            get_project_info,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={PROJECT_TOOLS_TAG},
        )
    )

    LOG.info(f'Adding tool {update_project_description.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            update_project_description,
            annotations=ToolAnnotations(destructiveHint=True),
            tags={PROJECT_TOOLS_TAG},
        )
    )

    LOG.info(f'Adding tool {get_accessible_projects.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            get_accessible_projects,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={PROJECT_TOOLS_TAG},
        )
    )

    LOG.info(f'Adding tool {set_project_scope.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            set_project_scope,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={PROJECT_TOOLS_TAG},
        )
    )

    LOG.info('Project tools initialized.')


async def _parent_subject_token(client: KeboolaClient) -> str:
    """
    Resolves the whole-stack (parent) programmatic token used to introspect/scope.

    Prefers the refreshable token from the local PKCE credential store (so re-scoping always starts
    from the parent, never from an already-narrowed scoped token); falls back to whatever bearer the
    client currently carries (a directly-supplied PAT, or an HTTP bearer).
    """
    if client.bearer_token is None:
        raise ValueError(
            'Project scoping requires a Keboola programmatic token (kbc_at_/kbc_pat_). '
            'Run "keboola-mcp-server login --api-url <url>" first, or supply such a token.'
        )
    try:
        return await get_access_token(client.storage_api_url)
    except RuntimeError:
        return client.bearer_token


async def _resolve_branch_context(client: KeboolaClient) -> tuple[str | int, str, bool]:
    """
    Resolves the current branch's id, name, and dev-branch flag from the storage API.

    `client.branch_id` is None on the default/production branch (normalized by
    `KeboolaClient.with_branch_id`), so we look up the branches list and pick either
    the entry matching the client's branch_id or the one with `isDefault=True`.
    """
    target_branch_id = client.branch_id
    branches = await client.storage_client.branches_list()

    selected: JsonDict | None = None
    for branch in branches:
        if target_branch_id is None:
            if branch.get('isDefault') is True:
                selected = branch
                break
        else:
            if str(branch.get('id')) == str(target_branch_id):
                selected = branch
                break

    if selected is None:
        # Should not happen in a healthy project, but stay defensive.
        fallback_id: str | int = target_branch_id if target_branch_id is not None else 'default'
        return fallback_id, 'unknown', target_branch_id is not None

    branch_id = cast(
        str | int,
        selected.get('id', target_branch_id if target_branch_id is not None else 'default'),
    )
    branch_name = cast(str, selected.get('name', 'unknown'))
    is_development_branch = selected.get('isDefault') is not True
    return branch_id, branch_name, is_development_branch


def _get_toolset_restrictions(role: str) -> str | None:
    """
    Returns a human-readable description of toolset restrictions for the given user role,
    or None if no special restrictions apply.
    """
    role = role.lower()
    if role == 'readonly':
        return (
            f'Your Keboola user role is "{role}". '
            'Only read-only tools are available. '
            'All write operations (creating, updating, or deleting resources) are disabled.'
        )
    if not role or role == 'unknown':
        return 'Your Keboola user role is unknown. You can manage flows but cannot set their schedules.'
    if role not in ('admin', 'share'):
        return f'Your Keboola user role is "{role}". You can manage flows but cannot set their schedules.'
    return None


class ProjectInfo(BaseModel):
    project_id: str | int = Field(description='The id of the project.')
    project_name: str = Field(description='The name of the project.')
    project_description: str = Field(
        description='The description of the project.',
    )
    organization_id: str | int = Field(description='The ID of the organization this project belongs to.')
    sql_dialect: str = Field(description='The sql dialect used in the project.')
    workspace_id: int = Field(
        description=(
            'The ID of the read-only Keboola workspace the MCP server uses to run SQL queries '
            '(via `query_data`). It exposes all production tables, plus the current development '
            "branch's tables when operating on a branch. On legacy projects (without the "
            '`storage-branches` feature) the workspace always lives in the production branch. '
            'Reusable by other RO tooling (e.g. data-app testing) without provisioning a '
            'private workspace.'
        )
    )
    conditional_flows: bool = Field(description='Whether the project supports conditional flows.')
    links: list[Link] = Field(description='The links relevant to the project.')
    branch_id: str | int = Field(
        description='The ID of the branch this call is operating on (default/production or a development branch).'
    )
    branch_name: str = Field(description='The name of the branch this call is operating on.')
    is_development_branch: bool = Field(
        description=(
            'True if this call is operating on a development branch, False if on the default/production branch. '
            'Use this to apply branch-specific guidance (e.g., FQN handling in transformations, '
            'unsupported tools in development branches).'
        )
    )
    user_role: str = Field(
        description='The Keboola role of the current user (e.g. "admin", "developer", "guest", "readonly").',
    )
    toolset_restrictions: str | None = Field(
        default=None,
        description=(
            'Describes any restrictions on the available toolset implied by the user role. '
            'None if no special restrictions apply.'
        ),
    )
    llm_instruction: str = Field(
        description=(
            'These are the base instructions for working on the project. '
            'Use them as the basis for all further instructions. '
            'Do not change them. Remember to include them in all subsequent instructions.'
        )
    )


@tool_errors()
async def update_project_description(
    ctx: Context,
    description: Annotated[
        str,
        Field(description='The new project description text.'),
    ],
) -> None:
    """Updates the description of the current Keboola project."""
    client = KeboolaClient.from_state(ctx.session.state)
    storage = client.storage_client

    await storage.branch_metadata_update({MetadataField.PROJECT_DESCRIPTION: description})

    LOG.info('Project description updated successfully.')


@tool_errors()
async def get_project_info(
    ctx: Context,
) -> ProjectInfo:
    """
    Retrieves structured information about the current project,
    including essential context and base instructions for working with it
    (e.g., transformations, components, workflows, and dependencies).

    Always call this tool at least once at the start of a conversation
    to establish the project context before using other tools.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    storage = client.storage_client

    token_data = await storage.verify_token()
    project_data = cast(JsonDict, token_data.get('owner', {}))
    project_id = cast(str, project_data.get('id', ''))
    project_name = cast(str, project_data.get('name', ''))

    organization_data = cast(JsonDict, token_data.get('organization', {}))
    organization_id = cast(str, organization_data.get('id', ''))

    user_role = token_data.get('admin', {}).get('role') or 'unknown'

    metadata = await storage.branch_metadata_get()
    description = cast(
        str, next((item['value'] for item in metadata if item.get('key') == MetadataField.PROJECT_DESCRIPTION), '')
    )

    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    sql_dialect = await workspace_manager.get_sql_dialect()
    workspace_id = await workspace_manager.get_workspace_id()
    project_features = cast(JsonDict, project_data.get('features', {}))
    conditional_flows = 'hide-conditional-flows' not in project_features
    links = links_manager.get_project_links()

    branch_id, branch_name, is_development_branch = await _resolve_branch_context(client)

    project_info = ProjectInfo(
        project_id=project_id,
        project_name=project_name,
        project_description=description,
        organization_id=organization_id,
        sql_dialect=sql_dialect,
        workspace_id=workspace_id,
        conditional_flows=conditional_flows,
        links=links,
        branch_id=branch_id,
        branch_name=branch_name,
        is_development_branch=is_development_branch,
        user_role=user_role,
        toolset_restrictions=_get_toolset_restrictions(user_role),
        llm_instruction=get_project_system_prompt(sql_dialect),
    )

    LOG.info('Returning unified project info.')
    return project_info


def _sql_dialect_from_token(token_data: JsonDict) -> str | None:
    """Derives the project's SQL dialect from the token's owner.defaultBackend, without a workspace."""
    backend = cast(JsonDict, token_data.get('owner', {})).get('defaultBackend')
    if backend == 'snowflake':
        return 'Snowflake'
    if backend == 'bigquery':
        return 'BigQuery'
    return None


class AccessibleProject(BaseModel):
    id: int = Field(description='The project id.')
    name: str | None = Field(default=None, description='The project name.')
    role: str | None = Field(default=None, description='The user role in this project (e.g. "admin").')
    in_scope: bool = Field(default=False, description='Whether the session is currently scoped to this project.')
    sql_dialect: str | None = Field(
        default=None, description='The SQL dialect of the project ("Snowflake" or "BigQuery").'
    )


class BaseInstructionGroup(BaseModel):
    """Base working instructions shared by all projects of one SQL dialect (dialect-specific system prompt)."""

    project_ids: list[int] = Field(description='The scoped projects these instructions apply to.')
    sql_dialect: str | None = Field(default=None, description='The SQL dialect these projects share.')
    instructions: str = Field(description='The base working instructions for projects of this dialect.')


class AccessibleProjects(BaseModel):
    user_email: str | None = Field(default=None, description='The email of the authenticated user.')
    projects: list[AccessibleProject] = Field(description='The projects the current token can reach across the stack.')
    scoped_project_ids: list[int] | None = Field(
        default=None,
        description='The projects the session is currently scoped to, or null if no scope has been confirmed yet.',
    )
    read_only: bool | None = Field(default=None, description='Whether the current scoped token is read-only.')
    base_instructions: list[BaseInstructionGroup] | None = Field(
        default=None,
        description=(
            'The base working instructions, grouped by SQL dialect (deduplicated across projects). '
            'Only present when the tool is called with with_llm_instruction=true; request this once at the '
            'start of a conversation.'
        ),
    )
    llm_instruction: str = Field(
        description='Guidance for the assistant on how to use this result (distinct from base_instructions).',
    )


class ProjectScope(BaseModel):
    project_ids: list[int] = Field(description='The projects the session is now scoped to.')
    read_only: bool = Field(description='Whether the scoped token is read-only.')
    llm_instruction: str = Field(description='Guidance for the assistant on the new scope.')


async def _project_sql_dialect(
    server_state: ServerState, storage_api_url: str, subject_token: str, project_id: int
) -> tuple[int, str | None]:
    """Fetches one project's SQL dialect by verifying the parent token narrowed with X-KBC-ProjectId.

    No workspace is provisioned — the dialect comes from the token's owner.defaultBackend, so this is
    a single cheap Storage API call per project.
    """
    per_client = await MultiProjectMiddleware.client_for_project(
        server_state, storage_api_url, subject_token, project_id, read_only=True
    )
    token_data = await per_client.storage_client.verify_token()
    return project_id, _sql_dialect_from_token(token_data)


@tool_errors()
async def get_accessible_projects(
    ctx: Context,
    with_llm_instruction: Annotated[
        bool,
        Field(
            description=(
                'If true, include the base working instructions (base_instructions), grouped by SQL dialect. '
                'Request this once at the very start of a conversation; omit it on later calls.'
            )
        ),
    ] = False,
) -> AccessibleProjects:
    """
    Lists the Keboola projects the current login can access across the stack, each with its SQL dialect.

    Call this early in a conversation when the user logs in with a stack-wide token (PKCE login),
    present the projects, and ask whether they want to work across all of them or a subset. Then call
    `set_project_scope` with their choice. This tool compacts several API calls (token introspection
    plus a per-project token verify for the SQL dialect) into one result, so the assistant does not
    need a separate get_project_info call per project. Pass with_llm_instruction=true on the first
    call to also receive the base working instructions grouped by dialect.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    subject_token = await _parent_subject_token(client)
    introspection = await introspect_token(client.storage_api_url, subject_token=subject_token)

    scope = ctx.session.state.get(SCOPE_KEY)
    scoped_ids = scope.project_ids if isinstance(scope, SessionScope) and scope.confirmed else None

    # Enrich each project with its SQL dialect (concurrently). Best-effort: a project whose verify
    # fails simply keeps sql_dialect=None rather than failing the whole listing.
    server_state = ServerState.from_context(ctx)
    dialects: dict[int, str | None] = {}
    results = await process_concurrently(
        [p.id for p in introspection.projects],
        lambda pid: _project_sql_dialect(server_state, client.storage_api_url, subject_token, pid),
    )
    for result in results:
        if isinstance(result, asyncio.CancelledError):
            raise result  # never swallow cancellation — let it propagate
        if isinstance(result, BaseException):
            LOG.warning(f'Could not resolve SQL dialect for a project: {result}', exc_info=result)
            continue
        pid, dialect = result
        dialects[pid] = dialect

    projects = [
        AccessibleProject(
            id=p.id,
            name=p.name,
            role=p.role,
            in_scope=scoped_ids is not None and p.id in scoped_ids,
            sql_dialect=dialects.get(p.id),
        )
        for p in introspection.projects
    ]

    # Optionally attach the base working instructions, grouped by dialect so the (large) prompt is
    # sent once per distinct dialect rather than duplicated per project.
    base_instructions: list[BaseInstructionGroup] | None = None
    if with_llm_instruction:
        by_dialect: dict[str | None, list[int]] = {}
        for p in projects:
            by_dialect.setdefault(p.sql_dialect, []).append(p.id)
        base_instructions = [
            BaseInstructionGroup(
                project_ids=ids,
                sql_dialect=dialect,
                # No/unknown dialect -> pass '' so the prompt omits dialect-specific guidance rather
                # than defaulting to Snowflake (which would mislead a BigQuery/unknown project).
                instructions=get_project_system_prompt(dialect or ''),
            )
            for dialect, ids in by_dialect.items()
        ]

    if scoped_ids is None:
        instruction = (
            'No project scope has been confirmed yet. Ask the user whether to operate across all these '
            'projects or a subset, then call "set_project_scope" with the chosen project ids. Never write '
            'to more than one project without explicit user confirmation.'
        )
    else:
        instruction = (
            f'Session is currently scoped to {len(scoped_ids)} project(s). '
            'Call "set_project_scope" to change the scope.'
        )
    return AccessibleProjects(
        user_email=introspection.user_email,
        projects=projects,
        scoped_project_ids=scoped_ids,
        read_only=scope.read_only if scoped_ids is not None else None,
        base_instructions=base_instructions,
        llm_instruction=instruction,
    )


@tool_errors()
async def set_project_scope(
    ctx: Context,
    project_ids: Annotated[
        Optional[list[int]],
        Field(
            description='The project ids to scope the session to. '
            'Omit or pass null to scope to ALL accessible projects.'
        ),
    ] = None,
    read_only: Annotated[
        bool,
        Field(description='If true, mint a read-only scoped token (no write operations in any scoped project).'),
    ] = False,
) -> ProjectScope:
    """
    Scopes the current session to a set of Keboola projects.

    Mints a scoped access token (narrowed to `project_ids`, optionally read-only) that is used for the
    rest of the conversation. Read-only tools then run against every scoped project in a single call;
    write operations target the active (first) project only. Call this when the user states which
    projects to work on; it can be called again any time to re-scope.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    parent_token = await _parent_subject_token(client)

    # Distinguish "omit/null" (scope to all) from an explicit empty list, which is almost certainly a
    # caller mistake and must not silently broaden the scope to every project.
    if project_ids is not None and len(project_ids) == 0:
        raise ValueError('project_ids must be a non-empty list of project ids, or omitted/null to scope to all.')

    ids = list(project_ids or [])
    if not ids:
        introspection = await introspect_token(client.storage_api_url, subject_token=parent_token)
        ids = [p.id for p in introspection.projects]
    if not ids:
        raise ValueError('No accessible projects to scope to.')

    # Mint a token narrowed to the chosen projects. If the exchange endpoint is unavailable on the
    # stack, fall back to the whole-stack parent token (still narrowed per request by X-KBC-ProjectId)
    # so scoping/fan-out keeps working without the security narrowing.
    try:
        minted = await exchange_scoped_token(
            client.storage_api_url, subject_token=parent_token, project_ids=ids, read_only=read_only
        )
        scope = SessionScope(
            project_ids=ids,
            read_only=minted.read_only,
            scoped_token=minted.access_token,
            scoped_expires_at=minted.expires_at,
            confirmed=True,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (400, 401, 403):
            # Client error (bad project_ids, invalid/insufficient token): the input or auth is wrong,
            # not the exchange endpoint — surface it instead of silently downgrading to an unscoped
            # whole-stack token, which would mislead the caller about what was actually scoped.
            raise
        LOG.warning('Scoped-token exchange failed; scoping with the whole-stack token instead.', exc_info=True)
        scope = SessionScope(project_ids=ids, read_only=read_only, confirmed=True)
    except Exception:
        # Network/timeout/unavailable exchange endpoint: fall back so scoping still works, narrowed
        # per request by X-KBC-ProjectId, without the extra token-scoping security narrowing.
        LOG.warning('Scoped-token exchange failed; scoping with the whole-stack token instead.', exc_info=True)
        scope = SessionScope(project_ids=ids, read_only=read_only, confirmed=True)
    ctx.session.state[SCOPE_KEY] = scope

    # Scope-first UX: the tool list is filtered to scoping-only until a scope is confirmed. Now that
    # it is, tell the client to re-fetch so the full tool set appears. Best-effort — clients that
    # don't act on list_changed still work (the data tools are no longer gated once scope is set).
    try:
        await ctx.session.send_tool_list_changed()
    except Exception as e:
        LOG.debug(f'Could not send tools/list_changed after scoping: {e}')

    multi = len(ids) > 1
    return ProjectScope(
        project_ids=ids,
        read_only=scope.read_only,
        llm_instruction=(
            (
                f'Session scoped to {len(ids)} projects. Read-only tools return results per project. '
                'Write operations are not fanned out — they target the first scoped project; to write '
                'elsewhere, re-scope to that project first (confirm with the user).'
            )
            if multi
            else f'Session scoped to project {ids[0]}.'
        ),
    )
