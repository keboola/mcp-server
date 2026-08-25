import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from mcp import types as mt
from pydantic import ValidationError as PydanticValidationError

from keboola_mcp_server.clients.auth_bridge import StorageTokenResolver
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.multiproject import MultiProjectMiddleware
from keboola_mcp_server.scope import SCOPE_KEY, SessionScope
from keboola_mcp_server.workspace import WorkspaceManager


def _tool(name: str, read_only: bool = False, tags: set[str] | None = None) -> MagicMock:
    tool = MagicMock()
    tool.name = name
    tool.tags = tags or set()
    if read_only:
        tool.annotations.readOnlyHint = True
    else:
        tool.annotations = None
    return tool


class TestMultiProjectMiddleware:
    """Read tools fan out across the scoped projects; writes and single-project scope do not."""

    @staticmethod
    def _ctx(scope: SessionScope | None, tool_name: str, read_only: bool, arguments: dict | None = None):
        state: dict = {KeboolaClient.STATE_KEY: 'orig-client'}
        if scope is not None:
            state[SCOPE_KEY] = scope
        ctx = MagicMock(spec=Context)
        ctx.session = SimpleNamespace(state=state)
        ctx.request_context.lifespan_context = ServerState(
            config=Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x'),
            runtime_info=ServerRuntimeInfo(transport='stdio'),
        )
        tool = MagicMock()
        tool.name = tool_name
        if read_only:
            tool.annotations.readOnlyHint = True
        else:
            tool.annotations = None
        ctx.fastmcp.get_tool = AsyncMock(return_value=tool)
        message = SimpleNamespace(name=tool_name, arguments=arguments if arguments is not None else {})
        context = SimpleNamespace(message=message, fastmcp_context=ctx)
        return context, state

    @staticmethod
    def _result(text: str) -> ToolResult:
        return ToolResult(
            content=[mt.TextContent(type='text', text=text)],
            structured_content={'rows': [text]},
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('scope', 'tool_name', 'read_only'),
        [
            (None, 'get_tables', True),
            (SessionScope(project_ids=[11], confirmed=True), 'get_tables', True),
            # write, single scoped project: project_id is optional, defaults to the active project.
            (SessionScope(project_ids=[11], confirmed=True), 'update_config', False),
            (SessionScope(project_ids=[11, 22], confirmed=True), 'get_accessible_projects', True),  # excluded tool
        ],
        ids=['no_scope', 'single_project', 'write_tool_single_project', 'excluded_tool'],
    )
    async def test_passthrough_calls_once(self, scope, tool_name, read_only) -> None:
        context, _ = self._ctx(scope, tool_name, read_only)
        calls = []

        async def call_next(_):
            calls.append(1)
            return self._result('single')

        result = await MultiProjectMiddleware().on_call_tool(context, call_next)
        assert len(calls) == 1
        assert result.content[0].text == 'single'

    @pytest.mark.asyncio
    async def test_unconfirmed_scope_blocks_data_tools(self) -> None:
        # Default (auto-leased, unconfirmed) scope: data tools are gated with an ask-first message.
        scope = SessionScope(project_ids=[11, 22], confirmed=False)
        context, _ = self._ctx(scope, 'get_tables', read_only=True)

        async def call_next(_):
            raise AssertionError('call_next must not run for a gated tool')

        with pytest.raises(ToolError, match='no scope has been confirmed'):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

    @pytest.mark.asyncio
    async def test_unconfirmed_scope_allows_bootstrap_tools(self) -> None:
        scope = SessionScope(project_ids=[11, 22], confirmed=False)
        context, _ = self._ctx(scope, 'get_accessible_projects', read_only=True)
        calls = []

        async def call_next(_):
            calls.append(1)
            return self._result('projects')

        result = await MultiProjectMiddleware().on_call_tool(context, call_next)
        assert len(calls) == 1
        assert result.content[0].text == 'projects'

    @pytest.mark.asyncio
    async def test_write_tool_targets_named_project(self) -> None:
        # 2+ scoped projects, project_id names a non-active one: the client (and workspace) are
        # swapped to that project for the single call, then restored.
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'update_config', read_only=False, arguments={'project_id': '22'})
        active_clients: list = []

        async def call_next(_):
            active_clients.append(state[KeboolaClient.STATE_KEY])
            return self._result('updated')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(WorkspaceManager, 'create', AsyncMock(return_value='wsm')),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert active_clients == ['client-22']
        assert state[KeboolaClient.STATE_KEY] == 'orig-client'  # restored
        assert result.content[0].text == 'updated'

    @pytest.mark.asyncio
    async def test_write_tool_no_swap_for_active_project(self) -> None:
        # project_id names the already-active (first) project: no client swap needed.
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        context, _state = self._ctx(scope, 'update_config', read_only=False, arguments={'project_id': '11'})
        calls = []

        async def call_next(_):
            calls.append(1)
            return self._result('updated')

        with patch.object(MultiProjectMiddleware, 'client_for_project', AsyncMock()) as client_for_project:
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        client_for_project.assert_not_called()
        assert len(calls) == 1
        assert result.content[0].text == 'updated'

    @pytest.mark.asyncio
    async def test_write_tool_ambiguous_without_project_id_raises(self) -> None:
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        context, _ = self._ctx(scope, 'update_config', read_only=False, arguments={})

        async def call_next(_):
            raise AssertionError('call_next must not run for an ambiguous write')

        with pytest.raises(ToolError, match='2 projects are scoped'):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

    @pytest.mark.asyncio
    async def test_write_tool_project_id_outside_scope_raises(self) -> None:
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        context, _ = self._ctx(scope, 'update_config', read_only=False, arguments={'project_id': '33'})

        async def call_next(_):
            raise AssertionError('call_next must not run for an out-of-scope project_id')

        with pytest.raises(ToolError, match='outside the current scope'):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

    @pytest.mark.asyncio
    async def test_get_project_info_targets_named_project(self) -> None:
        # Same single-target resolution as a write tool: 2+ scoped projects, project_id names a
        # non-active one -- swap to it for the call, then restore.
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'get_project_info', read_only=True, arguments={'project_id': '22'})
        active_clients: list = []

        async def call_next(_):
            active_clients.append(state[KeboolaClient.STATE_KEY])
            return self._result('info')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(WorkspaceManager, 'create', AsyncMock(return_value='wsm')),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert active_clients == ['client-22']
        assert state[KeboolaClient.STATE_KEY] == 'orig-client'  # restored
        assert result.content[0].text == 'info'

    @pytest.mark.asyncio
    async def test_get_project_info_defaults_for_single_scoped_project(self) -> None:
        # Single scoped project, no project_id given: defaults to it, no swap.
        scope = SessionScope(project_ids=[11], confirmed=True)
        context, _ = self._ctx(scope, 'get_project_info', read_only=True, arguments={})
        calls = []

        async def call_next(_):
            calls.append(1)
            return self._result('info')

        with patch.object(MultiProjectMiddleware, 'client_for_project', AsyncMock()) as client_for_project:
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        client_for_project.assert_not_called()
        assert len(calls) == 1
        assert result.content[0].text == 'info'

    @pytest.mark.asyncio
    async def test_get_project_info_ambiguous_without_project_id_raises(self) -> None:
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        context, _ = self._ctx(scope, 'get_project_info', read_only=True, arguments={})

        async def call_next(_):
            raise AssertionError('call_next must not run for an ambiguous get_project_info')

        with pytest.raises(ToolError, match='2 projects are scoped'):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

    @pytest.mark.asyncio
    async def test_read_tool_fans_out_per_project(self) -> None:
        scope = SessionScope(
            project_ids=[11, 22], scoped_token='kbc_at_s', scoped_expires_at=time.time() + 3600, confirmed=True
        )
        context, state = self._ctx(scope, 'get_tables', read_only=True)
        active_clients: list = []
        active_workspaces: list = []

        async def call_next(_):
            active_clients.append(state[KeboolaClient.STATE_KEY])
            active_workspaces.append(state[WorkspaceManager.STATE_KEY])
            return self._result('rows')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(
                WorkspaceManager,
                'create',
                AsyncMock(side_effect=lambda client, _schema, kubernetes_token_path=None: f'wsm-{client}'),
            ),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        # Ran once per project, each against that project's client AND workspace.
        assert active_clients == ['client-11', 'client-22']
        assert active_workspaces == ['wsm-client-11', 'wsm-client-22']
        # Active client and workspace restored afterwards.
        assert state[KeboolaClient.STATE_KEY] == 'orig-client'
        assert state.get(WorkspaceManager.STATE_KEY) is None
        # Per-project results are labelled in the text content.
        texts = [c.text for c in result.content]
        assert texts == ['=== project 11 ===', 'rows', '=== project 22 ===', 'rows']
        # Structured output is deep-merged (list fields concatenated) so it still validates the schema.
        assert result.structured_content == {'rows': ['rows', 'rows']}

    @pytest.mark.asyncio
    async def test_swap_project_uses_active_client_url_and_sa_token_path(self, monkeypatch) -> None:
        # _swap_project must use the CURRENT request's Storage API URL (the active client's), not
        # server_state.config's startup/lifespan URL, and must pass the deployed SA token path
        # through to WorkspaceManager.create exactly like create_session_state does.
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'get_tables', read_only=True)
        # server_state.config carries a different (stale/absent) URL than the active request client.
        state[KeboolaClient.STATE_KEY] = KeboolaClient(
            storage_api_url='https://connection.request.keboola.com', legacy_storage_token='kbc_at_s'
        )
        seen_calls: list = []

        async def fake_client_for_project(_ss, storage_api_url, _token, pid, _ro):
            seen_calls.append((storage_api_url, pid))
            return f'client-{pid}'

        async def call_next(_):
            return self._result('rows')

        with (
            patch.object(MultiProjectMiddleware, 'client_for_project', AsyncMock(side_effect=fake_client_for_project)),
            patch.object(WorkspaceManager, 'create', AsyncMock(return_value='wsm')) as ws_create,
        ):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert seen_calls == [
            ('https://connection.request.keboola.com', 11),
            ('https://connection.request.keboola.com', 22),
        ]
        for call in ws_create.await_args_list:
            assert call.kwargs.get('kubernetes_token_path') == '/var/run/secrets/token'

    @pytest.mark.asyncio
    async def test_fan_out_uses_bearer_token_not_active_project_resolved_token(self) -> None:
        # No scoped_token (auto-leased scope, or set_project_scope's exchange-failure fallback) with
        # a real active client whose .legacy_storage_token has been narrowed to the active project's
        # resolved legacy Storage token by KeboolaClient.create, while .bearer_token stays the
        # whole-stack subject token. Fanning out to a DIFFERENT project must use .bearer_token,
        # never that narrowed .legacy_storage_token -- reusing it would run the fanned-out call
        # against the WRONG project (PSGO-280).
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        context, state = self._ctx(scope, 'get_tables', read_only=True)
        state[KeboolaClient.STATE_KEY] = KeboolaClient(
            storage_api_url='https://connection.keboola.com',
            legacy_storage_token='legacy-project-11-token',
            bearer_token='kbc_at_whole_stack',
        )
        seen_tokens: list = []

        async def fake_client_for_project(_ss, _url, token, pid, _ro):
            seen_tokens.append(token)
            return f'client-{pid}'

        async def call_next(_):
            return self._result('rows')

        with (
            patch.object(MultiProjectMiddleware, 'client_for_project', AsyncMock(side_effect=fake_client_for_project)),
            patch.object(WorkspaceManager, 'create', AsyncMock(return_value='wsm')),
        ):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert seen_tokens == ['kbc_at_whole_stack', 'kbc_at_whole_stack']

    @pytest.mark.asyncio
    async def test_single_target_dispatch_uses_bearer_token_not_active_project_resolved_token(self) -> None:
        # Same hazard as above, for the write/get_project_info single-target dispatch path.
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        context, state = self._ctx(scope, 'update_config', read_only=False, arguments={'project_id': '22'})
        state[KeboolaClient.STATE_KEY] = KeboolaClient(
            storage_api_url='https://connection.keboola.com',
            legacy_storage_token='legacy-project-11-token',
            bearer_token='kbc_at_whole_stack',
        )
        seen_tokens: list = []

        async def call_next(_):
            return self._result('updated')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, token, pid, _ro: seen_tokens.append(token) or f'client-{pid}'),
            ),
            patch.object(WorkspaceManager, 'create', AsyncMock(return_value='wsm')),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert seen_tokens == ['kbc_at_whole_stack']
        assert result.content[0].text == 'updated'

    @pytest.mark.asyncio
    async def test_query_data_targets_single_project_workspace(self) -> None:
        # query_data is no longer excluded: with the project_ids filter it runs once against that
        # project's own workspace, so the user can query any scoped project without re-scoping.
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'query_data', read_only=True, arguments={'project_ids': [22]})
        seen_workspaces: list = []

        async def call_next(_):
            seen_workspaces.append(state[WorkspaceManager.STATE_KEY])
            return self._result('csv')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(
                WorkspaceManager,
                'create',
                AsyncMock(side_effect=lambda client, _schema, kubernetes_token_path=None: f'wsm-{client}'),
            ),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert seen_workspaces == ['wsm-client-22']  # ran against project 22's workspace
        assert result.content[0].text == 'csv'

    @pytest.mark.asyncio
    async def test_project_filter_single_target_runs_once(self) -> None:
        # project_ids filter narrows a multi-project scope to one project: one call, that project's
        # client, and the filter is stripped from the arguments the tool receives.
        scope = SessionScope(project_ids=[11, 22, 33], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'get_tables', read_only=True, arguments={'project_ids': [22]})
        seen_clients: list = []
        seen_workspaces: list = []

        async def call_next(_):
            seen_clients.append(state[KeboolaClient.STATE_KEY])
            seen_workspaces.append(state[WorkspaceManager.STATE_KEY])
            return self._result('t')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(
                WorkspaceManager,
                'create',
                AsyncMock(side_effect=lambda client, _schema, kubernetes_token_path=None: f'wsm-{client}'),
            ),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert seen_clients == ['client-22']  # ran once, against project 22 only
        assert seen_workspaces == ['wsm-client-22']  # its own workspace
        assert state[KeboolaClient.STATE_KEY] == 'orig-client'  # restored
        assert 'project_ids' not in context.message.arguments  # stripped before the tool
        assert result.content[0].text == 't'  # raw single-project result, not an envelope

    @pytest.mark.asyncio
    async def test_project_filter_subset_fans_out(self) -> None:
        scope = SessionScope(project_ids=[11, 22, 33], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'get_tables', read_only=True, arguments={'project_ids': [11, 33]})
        seen_clients: list = []

        async def call_next(_):
            seen_clients.append(state[KeboolaClient.STATE_KEY])
            return self._result('t')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(
                WorkspaceManager,
                'create',
                AsyncMock(side_effect=lambda client, _schema, kubernetes_token_path=None: f'wsm-{client}'),
            ),
        ):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert seen_clients == ['client-11', 'client-33']  # only the requested subset, in scope order

    @pytest.mark.asyncio
    async def test_project_filter_outside_scope_raises(self) -> None:
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, _ = self._ctx(scope, 'get_tables', read_only=True, arguments={'project_ids': [99]})

        async def call_next(_):
            raise AssertionError('must not run for an out-of-scope filter')

        with pytest.raises(ToolError, match='outside the current scope'):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

    @pytest.mark.asyncio
    async def test_on_list_tools_injects_project_filter(self) -> None:
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        # a read fan-out tool, an excluded tool, and a write tool
        read_tool = _tool('get_tables', read_only=True)
        read_tool.parameters = {'type': 'object', 'properties': {'bucket_ids': {'type': 'array'}}}
        excluded = _tool('get_project_info', read_only=True)
        excluded.parameters = {'type': 'object', 'properties': {}}
        write_tool = _tool('update_config', read_only=False)
        write_tool.parameters = {'type': 'object', 'properties': {}}
        for t in (read_tool, excluded, write_tool):
            t.model_copy = lambda update, _t=t: SimpleNamespace(name=_t.name, parameters=update['parameters'])

        context, _ = self._ctx(scope, 'x', read_only=True)

        async def call_next(_):
            return [read_tool, excluded, write_tool]

        tools = await MultiProjectMiddleware().on_list_tools(context, call_next)
        by_name = {t.name: t for t in tools}
        assert 'project_ids' in by_name['get_tables'].parameters['properties']
        assert 'project_ids' not in by_name['get_project_info'].parameters['properties']
        assert 'project_ids' not in by_name['update_config'].parameters['properties']

    @pytest.mark.asyncio
    async def test_on_list_tools_unconfirmed_scope_lists_all_tools(self) -> None:
        # Data tools are NOT hidden before scope is confirmed: hiding relied on the client re-fetching
        # after tools/list_changed, which Claude Code doesn't do mid-session. All tools stay listed;
        # the call-time ask-first gate steers to set_project_scope instead.
        scope = SessionScope(project_ids=[11, 22], confirmed=False)
        context, _ = self._ctx(scope, 'x', read_only=True)

        async def call_next(_):
            return [
                _tool('get_accessible_projects', read_only=True),
                _tool('set_project_scope', read_only=True),
                _tool('get_tables', read_only=True),
                _tool('update_config', read_only=False),
            ]

        tools = await MultiProjectMiddleware().on_list_tools(context, call_next)
        assert {t.name for t in tools} == {
            'get_accessible_projects',
            'set_project_scope',
            'get_tables',
            'update_config',
        }

    @pytest.mark.asyncio
    async def test_on_list_tools_no_scope_is_passthrough(self) -> None:
        # Legacy Storage-token session (no SessionScope): every tool stays advertised, unchanged.
        context, _ = self._ctx(None, 'x', read_only=True)

        async def call_next(_):
            return [_tool('get_tables', read_only=True), _tool('update_config', read_only=False)]

        tools = await MultiProjectMiddleware().on_list_tools(context, call_next)
        assert {t.name for t in tools} == {'get_tables', 'update_config'}

    @staticmethod
    def _items_result(n: int) -> ToolResult:
        return ToolResult(
            content=[mt.TextContent(type='text', text=f'{n} items')],
            structured_content={'buckets': list(range(n)), 'total': n},
        )

    def test_merge_small_keeps_full_detail(self) -> None:
        merged = MultiProjectMiddleware._merge([(11, self._items_result(2)), (22, self._items_result(3))])
        # Under the cap: per-project text envelopes + fully merged lists; counters summed.
        # Non-dict list items (plain ints here) are left alone -- nothing to attribute.
        assert merged.structured_content == {'buckets': [0, 1, 0, 1, 2], 'total': 5}
        assert [c.text for c in merged.content] == ['=== project 11 ===', '2 items', '=== project 22 ===', '3 items']

    @staticmethod
    def _dict_items_result(project_id: int, n: int) -> ToolResult:
        return ToolResult(
            content=[mt.TextContent(type='text', text=f'{n} items')],
            structured_content={'tables': [{'id': f'p{project_id}-t{i}'} for i in range(n)], 'total': n},
        )

    def test_tag_items_with_project_stamps_dict_items_only(self) -> None:
        tagged = MultiProjectMiddleware._tag_items_with_project(
            {'tables': [{'id': 't1'}, {'id': 't2'}], 'ids': [1, 2], 'total': 2}, project_id=42
        )
        assert tagged == {
            'tables': [{'id': 't1', '_scope_project_id': 42}, {'id': 't2', '_scope_project_id': 42}],
            'ids': [1, 2],  # non-dict items untouched
            'total': 2,
        }

    def test_tag_items_with_project_passes_through_non_dict_and_none(self) -> None:
        assert MultiProjectMiddleware._tag_items_with_project(None, project_id=42) is None
        assert MultiProjectMiddleware._tag_items_with_project([1, 2, 3], project_id=42) == [1, 2, 3]

    def test_merge_small_stamps_project_id_on_dict_items_in_structured_content(self) -> None:
        # Attribution must survive a client that reads only structured_content, not the text envelope.
        merged = MultiProjectMiddleware._merge(
            [(11, self._dict_items_result(11, 2)), (22, self._dict_items_result(22, 1))]
        )
        assert merged.structured_content == {
            'tables': [
                {'id': 'p11-t0', '_scope_project_id': 11},
                {'id': 'p11-t1', '_scope_project_id': 11},
                {'id': 'p22-t0', '_scope_project_id': 22},
            ],
            'total': 3,
        }

    @pytest.mark.asyncio
    async def test_fan_out_partial_failure_returns_successes_with_retry_hint(self) -> None:
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'get_tables', read_only=True)

        async def call_next(_):
            if state[KeboolaClient.STATE_KEY] == 'client-22':
                raise RuntimeError('boom-22')
            return self._result('rows')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(
                WorkspaceManager,
                'create',
                AsyncMock(side_effect=lambda client, _schema, kubernetes_token_path=None: f'wsm-{client}'),
            ),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        # Project 11 succeeded; project 22's failure is a retry hint, not a total failure.
        assert result.structured_content == {'rows': ['rows']}
        texts = [c.text for c in result.content]
        assert any('project 22 failed' in t and 'project_ids=[22]' in t for t in texts)

    @pytest.mark.asyncio
    async def test_fan_out_all_failed_raises_aggregate(self) -> None:
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, _ = self._ctx(scope, 'get_tables', read_only=True)

        async def call_next(_):
            raise RuntimeError('down')

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(
                WorkspaceManager,
                'create',
                AsyncMock(side_effect=lambda client, _schema, kubernetes_token_path=None: f'wsm-{client}'),
            ),
            pytest.raises(ToolError, match='failed for all 2 scoped'),
        ):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

    @pytest.mark.asyncio
    async def test_fan_out_validation_error_raised_once_not_per_project(self) -> None:
        # A bad argument (e.g. get_components with no component_ids) fails identically in every
        # project, so it must surface as ONE clean validation error, not N copies + an aggregate.
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'get_tables', read_only=True)
        calls = []

        async def call_next(_):
            calls.append(state[KeboolaClient.STATE_KEY])
            raise PydanticValidationError.from_exception_data('get_tables', [])

        with (
            patch.object(
                MultiProjectMiddleware,
                'client_for_project',
                AsyncMock(side_effect=lambda _ss, _url, _token, pid, _ro: f'client-{pid}'),
            ),
            patch.object(
                WorkspaceManager,
                'create',
                AsyncMock(side_effect=lambda client, _schema, kubernetes_token_path=None: f'wsm-{client}'),
            ),
            pytest.raises(PydanticValidationError),
        ):
            await MultiProjectMiddleware().on_call_tool(context, call_next)

        # Aborted after the first project; not retried across the rest.
        assert calls == ['client-11']

    def test_merge_large_degrades_to_count_first(self, monkeypatch) -> None:
        # Lower the cap so a modest result trips the count-first path.
        monkeypatch.setattr(MultiProjectMiddleware, '_FANOUT_MAX_ITEMS', 3)
        merged = MultiProjectMiddleware._merge([(11, self._items_result(2)), (22, self._items_result(3))])
        # Single guidance note (no per-project full dump), lists truncated, counters preserved.
        assert len(merged.content) == 1
        note = merged.content[0].text
        assert 'project 11: 2' in note
        assert 'project 22: 3' in note
        assert 'search tool' in note
        assert 'project_ids' in note
        assert len(merged.structured_content['buckets']) == 3  # truncated to the cap
        assert merged.structured_content['total'] == 5  # true total preserved


class TestActiveProjectReadOnlyGuard:
    """The active-project shortcut only skips the per-project client swap when the base client
    already honors the scope's read_only -- defense in depth for the fail-open case where
    SessionStateMiddleware couldn't build the base client read-only (Security hardening RFC
    increment)."""

    @staticmethod
    def _ctx_with_client(scope: SessionScope, tool_name: str, read_only_tool: bool, client_readonly) -> tuple:
        client = MagicMock(spec=KeboolaClient)
        client.readonly = client_readonly
        client.legacy_storage_token = 'kbc_at_x'
        client.storage_api_url = 'https://connection.keboola.com'
        state: dict = {KeboolaClient.STATE_KEY: client, SCOPE_KEY: scope}
        ctx = MagicMock(spec=Context)
        ctx.session = SimpleNamespace(state=state)
        ctx.request_context.lifespan_context = ServerState(
            config=Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x'),
            runtime_info=ServerRuntimeInfo(transport='stdio'),
        )
        tool = MagicMock()
        tool.name = tool_name
        tool.annotations.readOnlyHint = read_only_tool if read_only_tool else None
        ctx.fastmcp.get_tool = AsyncMock(return_value=tool)
        message = SimpleNamespace(name=tool_name, arguments={})
        context = SimpleNamespace(message=message, fastmcp_context=ctx)
        return context, state, client

    @pytest.mark.asyncio
    async def test_skips_swap_when_base_client_already_readonly(self, mocker) -> None:
        scope = SessionScope(project_ids=[11], read_only=True, confirmed=True)
        context, _, _ = self._ctx_with_client(scope, 'get_tables', read_only_tool=True, client_readonly=True)
        swap = mocker.patch.object(MultiProjectMiddleware, '_swap_project', new=AsyncMock())

        async def call_next(_):
            return 'ok'

        await MultiProjectMiddleware().on_call_tool(context, call_next)
        swap.assert_not_called()

    @pytest.mark.asyncio
    async def test_swaps_when_base_client_is_not_readonly_despite_readonly_scope(self, mocker) -> None:
        # The fail-open case: the base client couldn't be built read-only (e.g. an older session
        # predating the fix), so the active-project shortcut must not trust it -- fall through to
        # a real per-project swap, which enforces read_only itself.
        scope = SessionScope(project_ids=[11], read_only=True, confirmed=True)
        context, state, _ = self._ctx_with_client(scope, 'get_tables', read_only_tool=True, client_readonly=None)

        async def fake_swap(state_, server_state, storage_api_url, base_token, project_id, read_only):
            new_client = MagicMock(spec=KeboolaClient)
            new_client.readonly = read_only or None
            state_[KeboolaClient.STATE_KEY] = new_client

        mocker.patch.object(MultiProjectMiddleware, '_swap_project', new=AsyncMock(side_effect=fake_swap))
        mocker.patch.object(WorkspaceManager, 'create', new=AsyncMock(return_value='wsm'))

        captured_readonly = []

        async def call_next(_):
            captured_readonly.append(state[KeboolaClient.STATE_KEY].readonly)
            return 'ok'

        await MultiProjectMiddleware().on_call_tool(context, call_next)
        assert captured_readonly == [True]


class TestClientForProject:
    """`client_for_project` just asks `ServerState.storage_token_resolver` for the (possibly-None)
    auth-bridge resolver and hands it to `KeboolaClient.create` -- the resolve-success/failure/
    no-resolver behavior itself is covered once, in
    tests/clients/test_client.py::TestKeboolaClientCreate."""

    @pytest.mark.asyncio
    async def test_uses_the_resolver_server_state_provides(self, mocker) -> None:
        server_state = ServerState(
            config=Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x'),
            runtime_info=ServerRuntimeInfo(transport='http'),
        )
        resolver = AsyncMock(spec=StorageTokenResolver)
        resolver.resolve = AsyncMock(return_value='legacy-storage-token-789')
        get_resolver = mocker.patch.object(ServerState, 'storage_token_resolver', return_value=resolver)

        client = await MultiProjectMiddleware.client_for_project(
            server_state, 'https://connection.keboola.com', 'kbc_at_abc', 11, False
        )

        assert client.bearer_token == 'kbc_at_abc'
        assert client.legacy_storage_token == 'legacy-storage-token-789'
        resolver.resolve.assert_awaited_once_with(subject_token='kbc_at_abc', project_id=11)
        get_resolver.assert_called_once_with('https://connection.keboola.com')

    @pytest.mark.asyncio
    async def test_no_resolver_when_not_deployed(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        server_state = ServerState(
            config=Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x'),
            runtime_info=ServerRuntimeInfo(transport='stdio'),
        )

        client = await MultiProjectMiddleware.client_for_project(
            server_state, 'https://connection.keboola.com', 'kbc_at_abc', 11, False
        )

        assert client.legacy_storage_token == 'kbc_at_abc'
