"""Multi-project read fan-out (PSGO-261): ``MultiProjectMiddleware`` runs a read-only tool call
once per project in the active multi-project scope and merges the results.

Split out of ``mcp.py`` to keep that module focused on the core middleware/server wiring.
"""

import logging
from typing import Any

from fastmcp.exceptions import ToolError
from fastmcp.exceptions import ValidationError as FastMCPValidationError
from fastmcp.server import middleware as fmw
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.tools import Tool
from fastmcp.tools.tool import ToolResult
from mcp import types as mt
from pydantic import ValidationError as PydanticValidationError

from keboola_mcp_server.clients.auth_bridge import strip_bearer
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import build_tracing_headers, deployed_sa_token_path
from keboola_mcp_server.mcp import ServerState, is_read_only_tool
from keboola_mcp_server.scope import PROJECT_ID_ARG, SCOPE_KEY, SessionScope
from keboola_mcp_server.tools.constants import BOOTSTRAP_TOOLS
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

# Scope/auth tools that operate on the whole-stack token, not a single project -- never fanned out,
# never given a project_id (they don't have one to target).
_NO_FANOUT_TOOLS = {'get_accessible_projects', 'set_project_scope'}

# Read tools that report on exactly one project (not a list to fan out over) and take an explicit
# project_id argument to say which -- same single-target resolution/swap as a write tool, just
# without the write semantics. get_project_info resolves through the active project's
# WorkspaceManager (workspace id / sql dialect), so it can only ever report one project at a time.
_SINGLE_TARGET_READ_TOOLS = {'get_project_info'}

# Optional per-call argument injected on fan-out-eligible read tools to restrict a single call to a
# subset of the scoped projects (consumed and stripped by MultiProjectMiddleware.on_call_tool).
_PROJECT_FILTER_ARG = 'project_ids'


def _active_client_honors_scope(state: dict[str, Any], scope: SessionScope) -> bool:
    """True when the base session client already matches ``scope.read_only`` -- the active-
    project shortcuts below skip the per-project client swap only in that case (defense in
    depth: `SessionStateMiddleware.create_session_state` already builds the base client
    read-only whenever the scope requests it, so this is normally true and the shortcut's cost
    stays zero; see the "Security hardening" RFC increment).
    """
    if not scope.read_only:
        return True
    client = state.get(KeboolaClient.STATE_KEY)
    return isinstance(client, KeboolaClient) and client.readonly is True


class MultiProjectMiddleware(fmw.Middleware):
    """Fans a read-only tool call out across every project in the active multi-project scope.

    Single-project (or no) scope is an unchanged passthrough. With >1 project selected, a read-only
    tool runs once per project — the active ``KeboolaClient`` in session state is swapped to each
    project's client and the per-project results are labelled with a per-project text envelope. Their
    structured content is deep-merged (lists concatenated across projects, counters summed) into one
    schema-valid object, degrading to count-first with a truncated sample past ``_FANOUT_MAX_ITEMS``.
    Write tools never fan out: a write always targets exactly one project, named by its own
    ``project_id`` argument (required once 2+ projects are scoped) -- see
    ``_dispatch_single_target``. ``get_project_info`` uses the same single-target resolution
    (it reports on the active project's WorkspaceManager, so it can't fan out either).
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        ctx = context.fastmcp_context
        state = ctx.session.state
        scope = state.get(SCOPE_KEY) if isinstance(state, dict) else None
        name = context.message.name

        # Ask-first gate: until the user confirms a scope via set_project_scope, block data tools and
        # tell the assistant to ask the user which projects to work on. Only applies when a scope has
        # been auto-leased (local programmatic session); deployed/legacy sessions have no scope.
        if isinstance(scope, SessionScope) and not scope.confirmed and name not in BOOTSTRAP_TOOLS:
            raise ToolError(
                f'This session can access {len(scope.project_ids)} Keboola project(s), but no scope has '
                'been confirmed yet. Call "get_accessible_projects", show the user their projects, and ask '
                'whether to work across ALL of them or a subset. Then call "set_project_scope" '
                '(no arguments = all projects, or pass the chosen project ids, optionally read_only=true). '
                'This confirmation is required once per session.'
            )

        # No auto-leased scope (deployed / legacy) or a bootstrap/scope tool: pass through untouched.
        # Bootstrap tools own a real `project_ids` argument, so we must not strip it.
        if not isinstance(scope, SessionScope) or name in BOOTSTRAP_TOOLS:
            return await call_next(context)
        # Whole-stack scope/auth tools: always the active project's client, no project_id to target.
        if name in _NO_FANOUT_TOOLS:
            return await call_next(context)
        # Single-project-at-a-time tools (get_project_info) and all write tools resolve their own
        # explicit project_id the same way -- one target, swap the client, no fan-out.
        if name in _SINGLE_TARGET_READ_TOOLS:
            return await self._dispatch_single_target(context, call_next, ctx, state, scope)
        tool = await ctx.fastmcp.get_tool(name)
        if not is_read_only_tool(tool):
            return await self._dispatch_single_target(context, call_next, ctx, state, scope)

        # Read tool: consume the optional per-call project filter (advertised via on_list_tools) so the
        # tool never receives it, then narrow this call's target projects to the requested subset.
        requested = None
        args = getattr(context.message, 'arguments', None)
        if isinstance(args, dict):
            requested = args.pop(_PROJECT_FILTER_ARG, None)

        targets = list(scope.project_ids)
        if requested is not None:
            # Omit the filter to run across the full scope; an explicit empty list is a caller mistake
            # (it must not silently fall through to the whole scope).
            if not requested:
                raise ToolError(
                    f'"{_PROJECT_FILTER_ARG}" must be a non-empty list of project ids, '
                    'or omitted to run across the full scope.'
                )
            outside = [p for p in requested if p not in scope.project_ids]
            if outside:
                raise ToolError(
                    f'Project(s) {outside} are outside the current scope {scope.project_ids}. '
                    'Call "set_project_scope" to change the scope first.'
                )
            targets = [p for p in scope.project_ids if p in requested]
        if not targets:
            return await call_next(context)

        server_state = ServerState.from_context(ctx)
        original_client = state.get(KeboolaClient.STATE_KEY)
        original_workspace = state.get(WorkspaceManager.STATE_KEY)
        is_real_client = isinstance(original_client, KeboolaClient)
        # Default (auto-leased) scope carries no minted token; fall back to the active client's token.
        base_token = scope.scoped_token or (original_client.token if is_real_client else '')
        # The active client's own URL — the current request/session's, not the startup config's
        # (which can differ or be unset for streamable-HTTP setups that supply it per request).
        storage_api_url = original_client.storage_api_url if is_real_client else server_state.config.storage_api_url

        # A single target (scope of one, or narrowed to one via the filter) runs once against that
        # project only — one call, that project's X-KBC-ProjectId, no per-project envelope.
        if len(targets) == 1:
            target = targets[0]
            if target == scope.active_project_id and _active_client_honors_scope(state, scope):
                return await call_next(context)
            try:
                await self._swap_project(state, server_state, storage_api_url, base_token, target, scope.read_only)
                return await call_next(context)
            finally:
                state[KeboolaClient.STATE_KEY] = original_client
                state[WorkspaceManager.STATE_KEY] = original_workspace

        results: list[tuple[int, ToolResult]] = []
        errors: list[tuple[int, str]] = []
        try:
            for project_id in targets:
                await self._swap_project(state, server_state, storage_api_url, base_token, project_id, scope.read_only)
                # Isolate per-project failures: one project's error (e.g. Queue 401, a transient 5xx)
                # must not discard the other projects' good results. Collect it and keep going, so the
                # agent gets a partial response plus a retry hint. CancelledError is BaseException, so
                # `except Exception` lets client cancellation propagate.
                try:
                    results.append((project_id, await call_next(context)))
                except (FastMCPValidationError, PydanticValidationError):
                    # Argument-level validation error: the same bad arguments fail identically in
                    # every project, so fanning out would emit N identical copies plus a confusing
                    # "failed for all N projects" aggregate. Abort and surface the single clean error.
                    raise
                except Exception as e:
                    LOG.warning(f'Fan-out call failed for project {project_id}: {e}', exc_info=True)
                    errors.append((project_id, str(e)))
        finally:
            state[KeboolaClient.STATE_KEY] = original_client
            state[WorkspaceManager.STATE_KEY] = original_workspace

        # Every project failed → nothing partial to return; surface a single aggregate error.
        if not results and errors:
            detail = '; '.join(f'project {pid}: {msg}' for pid, msg in errors)
            raise ToolError(f'The tool failed for all {len(errors)} scoped project(s): {detail}')

        return self._merge(results, errors)

    @staticmethod
    def _resolve_single_target(scope: SessionScope, project_id: Any) -> int | None:
        """Picks the single project a write call or a single-target read targets, or raises if
        that's ambiguous/invalid.

        ``project_id`` is required once 2+ projects are scoped (no more implicit "first project"
        default); with exactly one scoped project it's optional and defaults to that project.
        """
        if project_id is None:
            if len(scope.project_ids) >= 2:
                raise ToolError(
                    f'{len(scope.project_ids)} projects are scoped ({scope.project_ids}). '
                    'Pass project_id=<id> (one of the scoped projects) -- this tool targets exactly one project.'
                )
            return scope.active_project_id
        try:
            target = int(project_id)
        except (TypeError, ValueError):
            raise ToolError(f'project_id must be an integer project id, got: {project_id!r}')
        if target not in scope.project_ids:
            raise ToolError(
                f'Project {target} is outside the current scope {scope.project_ids}. '
                'Call "set_project_scope" to change the scope first.'
            )
        return target

    async def _dispatch_single_target(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
        ctx: Any,
        state: dict[str, Any],
        scope: SessionScope,
    ) -> mt.CallToolResult:
        """Targets a write/modify/delete tool, or a single-target read tool (get_project_info), at
        the project named by its ``project_id`` argument (peeked, not popped -- it's a real
        declared tool parameter, not middleware-only).
        """
        args = getattr(context.message, 'arguments', None)
        project_id = args.get(PROJECT_ID_ARG) if isinstance(args, dict) else None
        target = self._resolve_single_target(scope, project_id)

        if target is None or (target == scope.active_project_id and _active_client_honors_scope(state, scope)):
            return await call_next(context)

        server_state = ServerState.from_context(ctx)
        original_client = state.get(KeboolaClient.STATE_KEY)
        original_workspace = state.get(WorkspaceManager.STATE_KEY)
        is_real_client = isinstance(original_client, KeboolaClient)
        base_token = scope.scoped_token or (original_client.token if is_real_client else '')
        storage_api_url = original_client.storage_api_url if is_real_client else server_state.config.storage_api_url
        try:
            await self._swap_project(state, server_state, storage_api_url, base_token, target, scope.read_only)
            return await call_next(context)
        finally:
            state[KeboolaClient.STATE_KEY] = original_client
            state[WorkspaceManager.STATE_KEY] = original_workspace

    async def on_list_tools(
        self,
        context: MiddlewareContext[mt.ListToolsRequest],
        call_next: CallNext[mt.ListToolsRequest, list[Tool]],
    ) -> list[Tool]:
        # Advertise the optional per-call `project_ids` filter on fan-out-eligible read tools while a
        # multi-project scope is active, so the assistant can target a subset (e.g. a single project)
        # without changing the session scope. The value is consumed and stripped in on_call_tool.
        tools = await call_next(context)
        ctx = context.fastmcp_context
        state = getattr(ctx.session, 'state', None)
        scope = state.get(SCOPE_KEY) if isinstance(state, dict) else None

        # NOTE: we intentionally do NOT hide data tools before a scope is confirmed. Hiding relied on
        # the client re-fetching the tool list after notifications/tools/list_changed, which Claude Code
        # (and others) don't do mid-session — that left the newly-unlocked tools invisible until a
        # reconnect. Instead every tool stays listed and the call-time ask-first gate (on_call_tool)
        # steers the user to set_project_scope first; once scoped, the already-listed tools just work.
        if not (isinstance(scope, SessionScope) and scope.confirmed and len(scope.project_ids) > 1):
            return tools

        patched: list[Tool] = []
        for tool in tools:
            if (
                tool.name in BOOTSTRAP_TOOLS
                or tool.name in _NO_FANOUT_TOOLS
                or tool.name in _SINGLE_TARGET_READ_TOOLS
                or not is_read_only_tool(tool)
            ):
                patched.append(tool)
                continue
            params = dict(tool.parameters or {})
            props = dict(params.get('properties') or {})
            if _PROJECT_FILTER_ARG in props:
                patched.append(tool)
                continue
            props[_PROJECT_FILTER_ARG] = {
                'type': 'array',
                'items': {'type': 'integer'},
                'description': (
                    'Optional. Restrict this call to these project ids (a subset of the confirmed '
                    'multi-project scope). Omit to run across all scoped projects.'
                ),
            }
            params['properties'] = props
            patched.append(tool.model_copy(update={'parameters': params}))
        return patched

    @classmethod
    async def _swap_project(
        cls,
        state: dict[str, Any],
        server_state: ServerState,
        storage_api_url: str,
        base_token: str,
        project_id: int,
        read_only: bool,
    ) -> None:
        """Points the session state at `project_id` for the duration of one fanned-out tool call.

        Swaps in a per-project `KeboolaClient` AND a `WorkspaceManager` built on it, so
        workspace-bound reads (query_data) run against *this* project's workspace rather than the
        active project's. The workspace is provisioned lazily on first use per project.
        Note: rebuilt per call; caching across calls would need a store that survives the
        per-request state rebuild — add if provisioning latency shows up in practice.
        """
        client = await cls.client_for_project(server_state, storage_api_url, base_token, project_id, read_only)
        state[KeboolaClient.STATE_KEY] = client
        state[WorkspaceManager.STATE_KEY] = await WorkspaceManager.create(
            client, server_state.config.workspace_schema, kubernetes_token_path=deployed_sa_token_path()
        )

    @staticmethod
    async def client_for_project(
        server_state: ServerState, storage_api_url: str, token: str, project_id: int, read_only: bool
    ) -> KeboolaClient:
        # `storage_api_url` is the current request/session URL (e.g. the active `KeboolaClient`'s),
        # not `server_state.config.storage_api_url` — that's the startup/lifespan config, which can
        # differ (or be unset) for streamable-HTTP setups that supply the URL per request.
        # Normalize any inbound `Bearer ` scheme; KeboolaClient adds it back for bearer tokens,
        # so a pre-prefixed value would otherwise become `Authorization: Bearer Bearer …`.
        token = strip_bearer(token)
        return await KeboolaClient(
            storage_api_url=storage_api_url,
            storage_api_token=token,
            bearer_token=token,
            headers={
                **build_tracing_headers(server_state.runtime_info),
                'X-KBC-ProjectId': str(project_id),
            },
            readonly=read_only or None,
        ).with_branch_id(None)

    @staticmethod
    def _deep_merge(a: Any, b: Any) -> Any:
        """Merges two per-project structured outputs so the result still validates the tool's schema.

        Lists are concatenated (the combined slice across projects), nested objects merged key by key,
        and numeric counters summed; any other scalar keeps the first project's value. This keeps every
        required field present with its declared type, so the merged object validates against the
        single-project output schema.
        """
        if isinstance(a, list) and isinstance(b, list):
            return a + b
        if isinstance(a, dict) and isinstance(b, dict):
            merged = dict(a)
            for key, value in b.items():
                merged[key] = MultiProjectMiddleware._deep_merge(a[key], value) if key in a else value
            return merged
        if isinstance(a, bool) or isinstance(b, bool):
            return a
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return a + b  # counters like search "total"
        return a

    # Total list items across projects before a fanned-out result degrades to count-first: instead of
    # dumping every project's full listing (which, on big projects, overflows the context window in a
    # single tool result), return per-project counts + a truncated sample + guidance to narrow. Small
    # multi-project results stay fully detailed. Class attribute so tests can lower it.
    _FANOUT_MAX_ITEMS = 200

    @staticmethod
    def _largest_list_len(sc: Any) -> int:
        """Item count of a structured payload = the length of its largest top-level list (buckets/tables/hits)."""
        if isinstance(sc, dict):
            return max((len(v) for v in sc.values() if isinstance(v, list)), default=0)
        if isinstance(sc, list):
            return len(sc)
        return 0

    @staticmethod
    def _truncate_lists(sc: Any, limit: int) -> Any:
        """Truncate every top-level list to `limit` (schema-safe: a shorter list still validates)."""
        if isinstance(sc, dict):
            return {k: (v[:limit] if isinstance(v, list) else v) for k, v in sc.items()}
        if isinstance(sc, list):
            return sc[:limit]
        return sc

    # Key stamped onto every dict item in a merged multi-project structured_content, so a client
    # reading only structured_content (not the `=== project N ===` text envelope) can still tell
    # which project an item came from once results are concatenated. Leading underscore + a name
    # unlikely to collide with any real Keboola field (see PSGO-261 RFC addendum: merged-result
    # project attribution). No output schema in this codebase sets extra='forbid'/additionalProperties:
    # false, so an extra key here doesn't break schema validation for any existing tool.
    _PROJECT_ATTRIBUTION_KEY = '_scope_project_id'

    @staticmethod
    def _tag_items_with_project(sc: Any, project_id: int) -> Any:
        """Stamps ``project_id`` onto every dict item in ``sc``'s top-level lists (non-dict items --
        e.g. a list of plain strings/ids -- are left alone; nothing to attribute).
        """
        if not isinstance(sc, dict):
            return sc
        tagged = dict(sc)
        for key, value in sc.items():
            if isinstance(value, list):
                tagged[key] = [
                    (
                        {**item, MultiProjectMiddleware._PROJECT_ATTRIBUTION_KEY: project_id}
                        if isinstance(item, dict)
                        else item
                    )
                    for item in value
                ]
        return tagged

    @staticmethod
    def _merge(results: list[tuple[int, 'ToolResult']], errors: 'list[tuple[int, str]] | None' = None) -> 'ToolResult':
        # Deep-merge the per-project structured payloads into one schema-valid object (lists concatenated
        # across projects). Counters (e.g. bucket_counts, search total) are summed by _deep_merge, so they
        # keep reflecting the true totals even if the item lists get truncated below.
        # Per-project failures (partial success) are surfaced as retry-hint notes in the text content,
        # so the model can re-run just the failed project(s) via the project_ids filter.
        error_notes = [
            mt.TextContent(
                type='text',
                text=f'project {pid} failed (retry with project_ids=[{pid}]): {msg}',
            )
            for pid, msg in (errors or [])
        ]
        merged_structured: Any = None
        per_project_counts: list[tuple[int, int]] = []
        total_items = 0
        for project_id, result in results:
            sc = MultiProjectMiddleware._tag_items_with_project(result.structured_content, project_id)
            item_count = MultiProjectMiddleware._largest_list_len(sc)
            per_project_counts.append((project_id, item_count))
            total_items += item_count
            if sc is not None:
                merged_structured = (
                    sc if merged_structured is None else MultiProjectMiddleware._deep_merge(merged_structured, sc)
                )

        # Small enough: full detail, with per-project text envelopes AND a `_scope_project_id` on every
        # merged structured_content item -- attribution survives whichever half of the result a caller
        # actually reads.
        if total_items <= MultiProjectMiddleware._FANOUT_MAX_ITEMS:
            content: list[Any] = list(error_notes)
            for project_id, result in results:
                content.append(mt.TextContent(type='text', text=f'=== project {project_id} ==='))
                content.extend(result.content or [])
            return ToolResult(content=content, structured_content=merged_structured)

        # Count-first: the combined listing is too large for one result. Return per-project counts, a
        # truncated sample (first _FANOUT_MAX_ITEMS), and guidance — instead of every project's full dump.
        summary = ', '.join(f'project {pid}: {n}' for pid, n in per_project_counts)
        note = (
            f'Multi-project result is large — {total_items} items across {len(results)} project(s) '
            f'({summary}). Showing the first {MultiProjectMiddleware._FANOUT_MAX_ITEMS} in structured_content; '
            f'counters reflect the true totals. Narrow with project_ids=[...] on this tool, or use the '
            f'search tool to find specific items.'
        )
        truncated = MultiProjectMiddleware._truncate_lists(merged_structured, MultiProjectMiddleware._FANOUT_MAX_ITEMS)
        return ToolResult(content=error_notes + [mt.TextContent(type='text', text=note)], structured_content=truncated)
