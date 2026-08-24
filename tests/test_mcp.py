import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field
from starlette.requests import Request

from keboola_mcp_server.clients.auth_bridge import is_programmatic_token
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import (
    AggregateError,
    ServerState,
    SessionStateMiddleware,
    ToolsFilteringMiddleware,
    _exclude_none_serializer,
    _filter_toon_nulls,
    process_concurrently,
    toon_serializer,
    unwrap_results,
)


class SimpleModel(BaseModel):
    field1: str | None = None
    field2: int | None = Field(default=None, serialization_alias='field2_alias')
    field3: datetime | None = None


class NestedModel(BaseModel):
    field1: str | None = None
    field2: list[str] | None = None


def _tool(name: str, read_only: bool = False, tags: set[str] | None = None) -> MagicMock:
    tool = MagicMock()
    tool.name = name
    tool.tags = tags or set()
    if read_only:
        tool.annotations.readOnlyHint = True
    else:
        tool.annotations = None
    return tool


async def _async_square(n: int) -> int:
    """Simple async function that squares a number after a short delay."""
    await asyncio.sleep(0.01)  # Simulate some async work
    return n * n


async def _async_fail(n: int) -> None:
    """Simple async function that always raises an exception."""
    await asyncio.sleep(0.01)
    raise ValueError(f'Failed for {n}')


async def _async_square_or_fail(n: int) -> int:
    """Async function that squares even numbers and fails for odd numbers."""
    await asyncio.sleep(0.01)
    if n % 2 == 0:
        return n * n
    else:
        raise ValueError(f'Failed for odd number {n}')


@pytest.mark.parametrize(
    ('data', 'expected'),
    [
        (None, ''),
        # Exclude none values from a single model
        (SimpleModel(field1='value1'), '{"field1":"value1"}'),
        # Exclude none values from a list of models
        (
            [SimpleModel(field1='value1', field2=None), SimpleModel(field2=123)],
            '[{"field1":"value1"},{"field2":123}]',
        ),
        # Exclude none values from a dictionary with models
        (
            {'key1': SimpleModel(field1='value1'), 'key2': None, 'key3': SimpleModel(field2=456)},
            '{"key1":{"field1":"value1"},"key3":{"field2":456}}',
        ),
        # Exclude none values from primitives
        ({'key1': 123, 'key2': None, 'key3': 'value'}, '{"key1":123,"key3":"value"}'),
        # Exclude none values with nested structures
        (
            {'key1': [SimpleModel(field1='value1'), None], 'key2': {'nested_key': SimpleModel(field2=789)}},
            '{"key1":[{"field1":"value1"}],"key2":{"nested_key":{"field2":789}}}',
        ),
        (
            {
                'key1': [
                    SimpleModel(field3=datetime(2025, 2, 3, 10, 11, 12, tzinfo=timezone(timedelta(hours=2)))),
                    None,
                ],
                'key2': {'nested_key': SimpleModel(field2=789)},
                'key3': datetime(2025, 1, 1, 1, 2, 3),
            },
            (
                '{"key1":[{"field3":"2025-02-03T10:11:12+02:00"}],'
                '"key2":{"nested_key":{"field2":789}},'
                '"key3":"2025-01-01T01:02:03"}'
            ),
        ),
    ],
)
def test_exclude_none_serializer(data, expected):
    result = _exclude_none_serializer(data)
    assert result == expected


@pytest.mark.parametrize(
    ('data', 'expected'),
    [
        # Top-level None
        (None, 'null'),
        # Empty dict
        ({}, ''),
        # Empty list
        ([], '[0]:'),
        # Empty tuple
        ((), '[0]:'),
        # Empty set
        (set(), '[0]:'),
        # Datetime
        (
            datetime(2025, 1, 1),
            '"2025-01-01T00:00:00"',
        ),
        # Simple dictionary
        (
            {'key': 'value', 'none_key': None},
            'key: value\nnone_key: null',
        ),
        # List
        (
            ['item1', 'item2'],
            '[2]: item1,item2',
        ),
        # Mixed types in a list
        (
            ['a', 1, True, None],
            '[4]: a,1,true,null',
        ),
        # Tuple
        (
            (1, 2, 3),
            '[3]: 1,2,3',
        ),
        # Nested dictionary
        (
            {'a': {'b': 1}},
            'a:\n  b: 1',
        ),
        # Deeply nested None
        (
            {'a': {'b': None}},
            'a:\n  b: null',
        ),
        # Model with some None values - toon_serializer includes None and does NOT use aliases
        (
            SimpleModel(field1='value1', field2=123),
            'field1: value1\nfield2: 123\nfield3: null',
        ),
        # Simple model (only has primitive fields) in a list
        (
            [SimpleModel(field1='value1', field2=123), SimpleModel(field1='value2', field2=456)],
            '[2]{field1,field2,field3}:\n  value1,123,null\n  value2,456,null',
        ),
        # Nested model (has a list field) in a list - this disables the tabular view
        (
            [
                NestedModel(field1='value1', field2=['item1', 'item2']),
                NestedModel(field1='value2', field2=['item3', 'item4']),
            ],
            ('[2]:\n  - field1: value1\n    field2[2]: item1,item2\n  - field1: value2\n    field2[2]: item3,item4'),
        ),
        # Complex structure with models, lists, dicts, and None
        (
            {
                'users': [
                    {'name': 'Alice', 'active': True},
                    {'name': 'Bob', 'active': None},
                ],
                'meta': SimpleModel(field1='test'),
            },
            ('users[2]{name,active}:\n  Alice,true\n  Bob,null\nmeta:\n  field1: test\n  field2: null\n  field3: null'),
        ),
    ],
)
def test_toon_serializer(data, expected):
    result = toon_serializer(data)
    assert result == expected


def test_filter_toon_nulls_single_item_list() -> None:
    data = [{'a': 1, 'b': None, 'c': {'d': None, 'e': 2}}]
    assert _filter_toon_nulls(data) == [{'a': 1, 'c': {'e': 2}}]


def test_filter_toon_nulls_multi_item_list_preserves_alignment() -> None:
    data = [{'a': 1, 'b': None}, {'a': None, 'b': 2}]
    assert _filter_toon_nulls(data) == [{'a': 1, 'b': None}, {'a': None, 'b': 2}]


def test_filter_toon_nulls_multi_item_list_preserves_key_order() -> None:
    data = [
        {
            'b': 1,
            'd': None,
            'a': None,
        },
        {'a': 2, 'b': None, 'c': 3, 'd': None, 'e': None},
    ]
    result = _filter_toon_nulls(data)
    assert result == [{'b': 1, 'a': None, 'c': None}, {'b': None, 'a': 2, 'c': 3}]
    assert list(result[0].keys()) == ['b', 'a', 'c']


@pytest.mark.parametrize(
    ('data', 'expected'),
    [
        ({}, {}),
        ([], []),
        (['a', None, 1], ['a', None, 1]),
        ({'a': None, 'b': 2}, {'b': 2}),
        ({'a': {'b': None, 'c': 3}}, {'a': {'c': 3}}),
        ([{'a': None, 'b': None}, {'a': 1, 'b': None}], [{'a': None}, {'a': 1}]),
        ([{'a': None}, {'b': None}], [{}, {}]),
        ([{'a': {'b': None}, 'c': 1}], [{'a': {}, 'c': 1}]),
        # Test that _filter_toon_nulls applies recursively to lists nested inside dicts
        (
            [
                {'a': 1, 'b': [None, 2, 3]},
                {'a': None, 'b': [4, None]},
            ],
            [
                {'a': 1, 'b': [None, 2, 3]},
                {'a': None, 'b': [4, None]},
            ],
        ),
        # Test with deeper nesting for key 'b'
        (
            [
                {'a': 1, 'b': [{'c': None, 'd': 2}, {'c': None, 'd': None}]},
                {'a': 2, 'b': [{'c': None, 'd': None}, {'c': None, 'd': None}]},
            ],
            [
                {'a': 1, 'b': [{'d': 2}, {'d': None}]},
                {'a': 2, 'b': [{}, {}]},
            ],
        ),
    ],
)
def test_filter_toon_nulls_edge_cases(data, expected) -> None:
    assert _filter_toon_nulls(data) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('items', 'afunc', 'max_concurrency', 'expected_successes', 'expected_exceptions'),
    [
        # All succeed
        (list(range(5)), _async_square, 2, [0, 1, 4, 9, 16], []),
        # Mixed success and failure (odd numbers fail)
        (list(range(5)), _async_square_or_fail, 3, [0, 4, 16], ['Failed for odd number 1', 'Failed for odd number 3']),
        # All fail
        (list(range(3)), _async_fail, 2, [], ['Failed for 0', 'Failed for 1', 'Failed for 2']),
        # Empty input
        ([], _async_square, 5, [], []),
    ],
    ids=['all_succeed', 'mixed_success_failure', 'all_fail', 'empty_input'],
)
async def test_process_concurrently(items, afunc, max_concurrency, expected_successes, expected_exceptions):
    """Test process_concurrently with various scenarios."""
    results = await process_concurrently(items, afunc, max_concurrency=max_concurrency)

    assert len(results) == len(items)

    successes = sorted([r for r in results if not isinstance(r, BaseException)])
    exceptions = [str(e) for e in results if isinstance(e, BaseException)]

    assert successes == expected_successes
    assert exceptions == expected_exceptions


@pytest.mark.asyncio
async def test_process_concurrently_respects_max_concurrency():
    """Test that max_concurrency limits simultaneous executions."""
    max_concurrency = 3
    current_running = 0
    peak_running = 0
    lock = asyncio.Lock()

    async def track_concurrency(n: int) -> int:
        nonlocal current_running, peak_running
        async with lock:
            current_running += 1
            peak_running = max(peak_running, current_running)
        try:
            await asyncio.sleep(0.01)
            return n * n
        finally:
            async with lock:
                current_running -= 1

    results = await process_concurrently(list(range(10)), track_concurrency, max_concurrency=max_concurrency)

    assert sorted(results) == [i * i for i in range(10)]
    assert peak_running <= max_concurrency


@pytest.mark.asyncio
@pytest.mark.parametrize('max_concurrency', [0, -1, -10])
async def test_process_concurrently_invalid_max_concurrency(max_concurrency):
    """Test that process_concurrently raises ValueError for invalid max_concurrency."""
    with pytest.raises(ValueError, match='max_concurrency must be a positive integer'):
        await process_concurrently([1, 2, 3], _async_square, max_concurrency=max_concurrency)


@pytest.mark.parametrize(
    ('results', 'expected'),
    [
        # All successes
        ([1, 2, 3], [1, 2, 3]),
        # Empty list
        ([], []),
        # Single success
        (['value'], ['value']),
    ],
    ids=['all_successes', 'empty', 'single_success'],
)
def test_unwrap_results_success(results, expected):
    """Test unwrap_results returns successes when no exceptions present."""
    assert unwrap_results(results) == expected


def test_unwrap_results_raises_aggregate_error():
    """Test unwrap_results raises AggregateError when exceptions are present."""
    exc1 = ValueError('error 1')
    exc2 = RuntimeError('error 2')
    results: list[int | BaseException] = [1, exc1, 2, exc2, 3]

    with pytest.raises(AggregateError) as exc_info:
        unwrap_results(results, 'Test errors')

    err = exc_info.value
    assert err.message == 'Test errors'
    assert err.exceptions == [exc1, exc2]
    assert str(err) == 'Test errors (2 errors): ValueError: error 1; RuntimeError: error 2'


def test_unwrap_results_all_exceptions():
    """Test unwrap_results when all results are exceptions."""
    exc1 = ValueError('error 1')
    exc2 = ValueError('error 2')
    results: list[int | BaseException] = [exc1, exc2]

    with pytest.raises(AggregateError) as exc_info:
        unwrap_results(results)

    err = exc_info.value
    assert err.exceptions == [exc1, exc2]
    assert str(err) == 'Multiple errors occurred (2 errors): ValueError: error 1; ValueError: error 2'


class TestToolsFilteringMiddleware:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('branch_id', 'expect_filtered'),
        [
            ('1234', True),
            (None, False),
        ],
    )
    async def test_list_tools_filters_data_apps_by_branch(
        self,
        mcp_context_client,
        branch_id: str | None,
        expect_filtered: bool,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.branch_id = branch_id
        keboola_client.storage_client.verify_token = AsyncMock(return_value={'owner': {'features': []}, 'admin': {}})

        data_app_tools = [
            'modify_streamlit_data_app',
            'get_data_apps',
            'deploy_data_app',
            'delete_python_js_data_app_draft',
        ]
        tools = [_tool(name) for name in data_app_tools] + [_tool('other_tool')]

        async def call_next(_):
            return tools

        middleware = ToolsFilteringMiddleware()
        context = SimpleNamespace(fastmcp_context=mcp_context_client)
        result = await middleware.on_list_tools(context, call_next)

        result_names = {t.name for t in result}
        for name in data_app_tools:
            if expect_filtered:
                assert name not in result_names
            else:
                assert name in result_names
        assert 'other_tool' in result_names

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('token_role', 'bearer_token', 'hidden_tools', 'visible_tools'),
        [
            ('admin', None, {'update_flow'}, {'modify_flow', 'read_only_tool'}),
            ('share', None, {'update_flow'}, {'modify_flow', 'read_only_tool'}),
            ('', None, {'modify_flow'}, {'update_flow', 'read_only_tool'}),
            ('readOnly', None, {'modify_flow', 'update_flow'}, {'read_only_tool'}),
            ('guest', None, {'modify_flow'}, {'update_flow', 'read_only_tool'}),
            # OAuth users: regular/guest users get modify_flow access (different from SAPI)
            ('', 'oauth_token', {'update_flow'}, {'modify_flow', 'read_only_tool'}),
            # Empty bearer token behaves the same as no bearer token (SAPI regular)
            ('', '', {'modify_flow'}, {'update_flow', 'read_only_tool'}),
        ],
    )
    async def test_list_tools_filters_flow_tools_by_role(
        self,
        mcp_context_client,
        keboola_client,
        token_role: str,
        bearer_token: str | None,
        hidden_tools: set[str],
        visible_tools: set[str],
    ) -> None:
        keboola_client.bearer_token = bearer_token
        keboola_client.storage_client.verify_token = AsyncMock(
            return_value={'owner': {'features': []}, 'admin': {'role': token_role}}
        )

        tools = [
            _tool('modify_flow'),
            _tool('update_flow'),
            _tool('other_tool'),
            _tool('read_only_tool', read_only=True),
        ]

        async def call_next(_):
            return tools

        middleware = ToolsFilteringMiddleware()
        context = SimpleNamespace(fastmcp_context=mcp_context_client)
        result = await middleware.on_list_tools(context, call_next)

        result_names = {t.name for t in result}
        for tool_name in hidden_tools:
            assert tool_name not in result_names
        for tool_name in visible_tools:
            assert tool_name in result_names

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('token_role', 'bearer_token', 'called_tool', 'tool_read_only', 'expect_error'),
        [
            ('admin', None, 'modify_flow', False, False),
            ('admin', None, 'update_flow', False, True),
            ('share', None, 'modify_flow', False, False),
            ('share', None, 'update_flow', False, True),
            ('', None, 'modify_flow', False, True),
            ('', None, 'update_flow', False, False),
            ('guest', None, 'write_tool', False, False),
            ('guest', None, 'read_only_tool', True, False),
            ('readOnly', None, 'write_tool', False, True),
            ('readOnly', None, 'read_only_tool', True, False),
            # OAuth users: regular users can call modify_flow (different from SAPI regular)
            ('', 'oauth_token', 'modify_flow', False, False),
            ('', 'oauth_token', 'update_flow', False, True),
            # Empty bearer token behaves the same as no bearer token (SAPI regular)
            ('', '', 'modify_flow', False, True),
        ],
    )
    async def test_call_tool_blocks_flow_tools_by_role(
        self,
        mcp_context_client,
        keboola_client,
        token_role: str,
        bearer_token: str | None,
        called_tool: str,
        tool_read_only: bool,
        expect_error: bool,
    ) -> None:
        keboola_client.bearer_token = bearer_token
        keboola_client.storage_client.verify_token = AsyncMock(
            return_value={'owner': {'features': []}, 'admin': {'role': token_role}}
        )

        tool = _tool(called_tool, read_only=tool_read_only)
        mcp_context_client.fastmcp = SimpleNamespace(get_tool=AsyncMock(return_value=tool))
        context = SimpleNamespace(fastmcp_context=mcp_context_client, message=SimpleNamespace(name=called_tool))

        expected = MagicMock()

        async def call_next(_):
            return expected

        middleware = ToolsFilteringMiddleware()
        if expect_error:
            with pytest.raises(ToolError):
                await middleware.on_call_tool(context, call_next)
        else:
            result = await middleware.on_call_tool(context, call_next)
            assert result is expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'tool_name',
        [
            'modify_streamlit_data_app',
            'delete_python_js_data_app_draft',
        ],
    )
    @pytest.mark.parametrize(
        ('branch_id', 'expect_error'),
        [
            ('5678', True),
            (None, False),
        ],
    )
    async def test_call_tool_blocks_data_apps_by_branch(
        self,
        mcp_context_client,
        branch_id: str | None,
        expect_error: bool,
        tool_name: str,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.branch_id = branch_id
        keboola_client.storage_client.verify_token = AsyncMock(return_value={'owner': {'features': []}, 'admin': {}})

        tool = _tool(tool_name)
        mcp_context_client.fastmcp = SimpleNamespace(get_tool=AsyncMock(return_value=tool))
        context = SimpleNamespace(fastmcp_context=mcp_context_client, message=SimpleNamespace(name=tool_name))

        expected = MagicMock()

        async def call_next(_):
            return expected

        middleware = ToolsFilteringMiddleware()
        if expect_error:
            with pytest.raises(ToolError, match='Data apps are supported only in the main production branch'):
                await middleware.on_call_tool(context, call_next)
        else:
            result = await middleware.on_call_tool(context, call_next)
            assert result is expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('has_semantic_models', 'tool_name', 'expect_filtered'),
        [
            (False, 'search_semantic_context', True),
            (False, 'get_semantic_schema', True),
            (True, 'search_semantic_context', False),
            (True, 'get_semantic_schema', False),
        ],
        ids=[
            'no_models_search',
            'no_models_schema',
            'with_models_search',
            'with_models_schema',
        ],
    )
    async def test_list_tools_filters_semantic_tools_by_models(
        self,
        mcp_context_client,
        has_semantic_models: bool,
        tool_name: str,
        expect_filtered: bool,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.verify_token = AsyncMock(return_value={'owner': {'features': []}, 'admin': {}})
        keboola_client.metastore_client.list_objects = AsyncMock(
            return_value=[MagicMock()] if has_semantic_models else []
        )

        tools = [
            _tool('search_semantic_context', tags={'semantic'}),
            _tool('get_semantic_context', tags={'semantic'}),
            _tool('get_semantic_schema', tags={'semantic'}),
            _tool('validate_semantic_query', tags={'semantic'}),
            _tool('other_tool'),
        ]

        async def call_next(_):
            return tools

        middleware = ToolsFilteringMiddleware()
        context = SimpleNamespace(fastmcp_context=mcp_context_client)
        result = await middleware.on_list_tools(context, call_next)

        result_names = {t.name for t in result}
        if expect_filtered:
            assert tool_name not in result_names
        else:
            assert tool_name in result_names
        assert 'other_tool' in result_names

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('has_semantic_models', 'tool_name', 'tool_tags', 'expect_error'),
        [
            (False, 'search_semantic_context', {'semantic'}, True),
            (False, 'get_semantic_schema', {'semantic'}, True),
            (True, 'search_semantic_context', {'semantic'}, False),
            (True, 'get_semantic_schema', {'semantic'}, False),
            (False, 'other_tool', set(), False),
        ],
        ids=[
            'no_models_search_tool',
            'no_models_schema_tool',
            'with_models_search_tool',
            'with_models_schema_tool',
            'no_models_non_semantic_tool',
        ],
    )
    async def test_call_tool_blocks_semantic_tools_by_models(
        self,
        mcp_context_client,
        has_semantic_models: bool,
        tool_name: str,
        tool_tags: set[str],
        expect_error: bool,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.verify_token = AsyncMock(return_value={'owner': {'features': []}, 'admin': {}})
        keboola_client.metastore_client.list_objects = AsyncMock(
            return_value=[MagicMock()] if has_semantic_models else []
        )

        tool = _tool(tool_name, tags=tool_tags)
        mcp_context_client.fastmcp = SimpleNamespace(get_tool=AsyncMock(return_value=tool))
        context = SimpleNamespace(fastmcp_context=mcp_context_client, message=SimpleNamespace(name=tool_name))

        expected = MagicMock()

        async def call_next(_):
            return expected

        middleware = ToolsFilteringMiddleware()
        if expect_error:
            with pytest.raises(ToolError, match='no semantic models'):
                await middleware.on_call_tool(context, call_next)
        else:
            result = await middleware.on_call_tool(context, call_next)
            assert result is expected


class TestSessionStateMiddleware:
    def test_apply_request_config_never_logs_header_values(self, mocker) -> None:
        # Authorization (a live programmatic bearer token) and X-Storage-Api-Token must never
        # appear in the debug log this method emits -- only header names, never values.
        log_debug = mocker.patch('keboola_mcp_server.mcp.LOG.debug')
        config = Config(storage_api_url='https://connection.keboola.com')
        http_rq = MagicMock(spec=Request)
        http_rq.headers = {'Authorization': 'Bearer kbc_at_super_secret', 'X-Storage-Api-Token': 'legacy-secret'}
        http_rq.scope = {}

        SessionStateMiddleware.apply_request_config(http_rq, config, own_stack_storage_api_url=None)

        logged = ' '.join(str(call) for call in log_debug.call_args_list)
        assert 'kbc_at_super_secret' not in logged
        assert 'legacy-secret' not in logged

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('method', 'expected_branch_id', 'expected_skip_token_exchange'),
        [
            ('tools/list', None, True),
            ('resources/list', None, True),
            ('prompts/list', None, True),
            ('tools/call', '999', False),
            ('resources/read', '999', False),
        ],
        ids=['tools_list', 'resources_list', 'prompts_list', 'tools_call', 'resources_read'],
    )
    async def test_on_request_branch_handling(
        self, method: str, expected_branch_id: str | None, expected_skip_token_exchange: bool
    ):
        config = Config(
            storage_api_url='https://connection.test.keboola.com',
            storage_token='test-token',
            branch_id='999',
        )
        runtime_info = ServerRuntimeInfo(transport='stdio')
        server_state = ServerState(config=config, runtime_info=runtime_info)

        # Use a non-MagicMock session so the middleware enters the branch-handling code path
        session = SimpleNamespace(state={})

        # ctx must pass isinstance(ctx, Context) check, so we use MagicMock(spec=Context).
        # However ctx.session must NOT be a MagicMock (line 146 guard), so we override it.
        ctx = MagicMock(spec=Context)
        ctx.session = session
        ctx.request_context.lifespan_context = server_state

        context = SimpleNamespace(method=method, fastmcp_context=ctx)
        expected_result = object()

        async def call_next(_):
            return expected_result

        captured_configs: list[Config] = []
        captured_own_stack_urls: list[str | None] = []
        captured_skip_token_exchange: list[bool] = []

        async def fake_create_session_state(
            cfg, _runtime_info, readonly=None, *, own_stack_storage_api_url, skip_token_exchange=False
        ):
            captured_configs.append(cfg)
            captured_own_stack_urls.append(own_stack_storage_api_url)
            captured_skip_token_exchange.append(skip_token_exchange)
            return {'fake': 'state'}

        middleware = SessionStateMiddleware()

        with (
            patch.object(middleware, 'create_session_state', side_effect=fake_create_session_state),
            patch('keboola_mcp_server.mcp.get_http_request_or_none', return_value=None),
        ):
            result = await middleware.on_request(context, call_next)

        assert result is expected_result
        assert len(captured_configs) == 1
        assert captured_configs[0].branch_id == expected_branch_id
        # The session's client is told which stack is the server's own one, so that it can decide
        # whether the Kubernetes step-up header may be sent.
        assert captured_own_stack_urls == ['https://connection.test.keboola.com']
        # /list requests skip the programmatic-token exchange (up to a ~35s resolver round trip) --
        # a client's initial tools/list fetch must be fast; see create_session_state's docstring.
        assert captured_skip_token_exchange == [expected_skip_token_exchange]

    @pytest.mark.parametrize(
        ('server_storage_api_url', 'headers', 'expected_storage_api_url'),
        [
            # No Storage API URL in the headers: the server's own stack is used.
            (
                'https://connection.keboola.com',
                {'X-Storage-Api-Token': 'header-token'},
                'https://connection.keboola.com',
            ),
            # The expected case: the caller asks for the very stack this server runs on.
            (
                'https://connection.keboola.com',
                {'X-Storage-Api-Url': 'https://connection.keboola.com', 'X-Branch-Id': '123'},
                'https://connection.keboola.com',
            ),
            # The same stack spelled with the scheme's default port is honoured as our own.
            (
                'https://connection.keboola.com:443',
                {'X-Storage-Api-Url': 'https://connection.keboola.com', 'X-Branch-Id': '123'},
                'https://connection.keboola.com',
            ),
            # Another Keboola stack is not honoured ...
            (
                'https://connection.keboola.com',
                {'X-Storage-Api-Url': 'https://connection.north-europe.azure.keboola.com', 'X-Branch-Id': '123'},
                'https://connection.keboola.com',
            ),
            # ... and neither are hosts that only look like this server's stack.
            (
                'https://connection.keboola.com',
                {'X-Storage-Api-Url': 'https://connection.keboola.com.attacker.example'},
                'https://connection.keboola.com',
            ),
            (
                'https://connection.keboola.com',
                {'X-Storage-Api-Url': 'https://connection.attacker.example'},
                'https://connection.keboola.com',
            ),
            (
                'https://connection.keboola.com',
                {'X-Storage-Api-Url': 'https://connection.keboola.com@attacker.example'},
                'https://connection.keboola.com',
            ),
            # A server with no stack of its own (locally run, stdio) keeps taking the URL
            # from the request, which is the only source it has.
            (
                None,
                {'X-Storage-Api-Url': 'https://connection.north-europe.azure.keboola.com'},
                'https://connection.north-europe.azure.keboola.com',
            ),
        ],
        ids=[
            'no_url_in_headers',
            'own_stack',
            'own_stack_default_port',
            'other_stack',
            'lookalike_suffix',
            'foreign_domain',
            'user_info',
            'no_own_stack',
        ],
    )
    def test_apply_request_config_pins_storage_api_url(
        self,
        server_storage_api_url: str | None,
        headers: dict[str, str],
        expected_storage_api_url: str,
    ):
        """A request may not steer the server to a Keboola stack other than its own."""
        config = Config(storage_api_url=server_storage_api_url, storage_token='server-token')
        http_rq = MagicMock(spec=Request)
        http_rq.headers = headers
        http_rq.scope = {}

        applied = SessionStateMiddleware.apply_request_config(
            http_rq, config, own_stack_storage_api_url=server_storage_api_url
        )

        assert applied.storage_api_url == expected_storage_api_url
        # Only the Storage API URL is pinned; the other per-request headers keep working.
        assert applied.storage_token == headers.get('X-Storage-Api-Token', 'server-token')
        assert applied.branch_id == headers.get('X-Branch-Id')

    @pytest.mark.parametrize(
        ('headers', 'expected_storage_token'),
        [
            # A programmatic bearer token arrives only via Authorization -- no Storage-token
            # header/alias reaches it, so apply_request_config must read it directly.
            ({'Authorization': 'Bearer kbc_at_abc'}, 'kbc_at_abc'),
            ({'Authorization': 'Bearer kbc_pat_abc'}, 'kbc_pat_abc'),
            # Case-insensitive "bearer" scheme, matching is_programmatic_token/strip_bearer.
            ({'Authorization': 'bearer kbc_at_abc'}, 'kbc_at_abc'),
            # An explicit Storage-token header always wins; Authorization is never consulted.
            ({'Authorization': 'Bearer kbc_at_abc', 'X-Storage-Api-Token': 'legacy-token'}, 'legacy-token'),
            # A non-programmatic Authorization value must never be forwarded as a Storage token.
            ({'Authorization': 'Bearer some-other-oauth-token'}, None),
            ({}, None),
        ],
        ids=[
            'bearer_access_token',
            'bearer_pat',
            'lowercase_bearer_scheme',
            'explicit_storage_token_wins',
            'non_programmatic_bearer_ignored',
            'no_authorization_header',
        ],
    )
    def test_apply_request_config_reads_programmatic_token_from_authorization_header(
        self, headers: dict[str, str], expected_storage_token: str | None
    ) -> None:
        config = Config(storage_api_url='https://connection.keboola.com')
        http_rq = MagicMock(spec=Request)
        http_rq.headers = headers
        http_rq.scope = {}

        applied = SessionStateMiddleware.apply_request_config(http_rq, config, own_stack_storage_api_url=None)

        assert applied.storage_token == expected_storage_token

    @pytest.mark.parametrize(
        ('server_kwargs', 'headers', 'expected_workspace_id', 'expected_workspace_schema', 'expect_warning'),
        [
            # server pinned by id: header asking for a different id/schema is ignored outright.
            ({'workspace_id': '111'}, {'X-Workspace-Id': '222'}, '111', None, True),
            ({'workspace_id': '111'}, {'X-Workspace-Schema': 'OTHER'}, '111', None, True),
            ({'workspace_id': '111'}, {'X-Workspace-Id': '111'}, '111', None, False),
            ({'workspace_id': '111'}, {}, '111', None, False),
            # server pinned by schema: same treatment, checked/restored together with `workspace_id`
            # rather than per-field -- a schema-pinned server is a single-project deployment that
            # doesn't receive per-request X-Workspace-Id headers in the first place, so an id header
            # here is also dropped (the one previously-uncovered cell from the mcp.py:320 thread).
            ({'workspace_schema': 'SERVER_SCHEMA'}, {'X-Workspace-Schema': 'OTHER'}, None, 'SERVER_SCHEMA', True),
            ({'workspace_schema': 'SERVER_SCHEMA'}, {'X-Workspace-Id': '222'}, None, 'SERVER_SCHEMA', True),
            # an empty schema header is the multi-user opt-out (README), not an override -- it
            # must not restore the server's schema pin.
            ({'workspace_schema': 'SERVER_SCHEMA'}, {'X-Workspace-Schema': ''}, None, '', False),
            # no server-side pin (the shared multi-tenant / AJDA-3052 Data App flow): the header
            # is the only source, so it keeps working.
            ({}, {'X-Workspace-Id': '222'}, '222', None, False),
        ],
        ids=[
            'id_overridden',
            'id_overridden_by_schema_header',
            'id_unchanged',
            'id_no_header',
            'schema_overridden',
            'schema_pin_also_drops_id_header',
            'empty_schema_header_is_opt_out_not_override',
            'no_server_pin',
        ],
    )
    def test_apply_request_config_pins_workspace(
        self,
        caplog: pytest.LogCaptureFixture,
        server_kwargs: dict[str, str],
        headers: dict[str, str],
        expected_workspace_id: str | None,
        expected_workspace_schema: str | None,
        expect_warning: bool,
    ):
        """A workspace pin configured on the server must be authoritative over a request header
        -- mirroring the Storage API URL check above -- but a server with no pin of its own must
        keep taking it from the request (AI-3669 review, workspace.py:836 thread). `workspace_id`
        and `workspace_schema` are checked/restored together, not per-field: the same
        silent-override risk applies to either kind of server pin (AI-3669 review, mcp.py:320
        thread)."""
        config = Config(storage_token='server-token', **server_kwargs)
        http_rq = MagicMock(spec=Request)
        http_rq.headers = headers
        http_rq.scope = {}

        with caplog.at_level('WARNING'):
            applied = SessionStateMiddleware.apply_request_config(http_rq, config, own_stack_storage_api_url=None)

        assert applied.workspace_id == expected_workspace_id
        assert applied.workspace_schema == expected_workspace_schema
        warned = any('is pinned' in r.message for r in caplog.records)
        assert warned is expect_warning


class TestProgrammaticTokenExchange:
    """SessionStateMiddleware exchanges programmatic tokens via the auth-bridge resolver (PSGO-261)."""

    @pytest.mark.asyncio
    async def test_missing_kubernetes_token_path_raises(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_pat_abc', project_id='1')
        with pytest.raises(ValueError, match='KBC_KUBERNETES_TOKEN_PATH'):
            await SessionStateMiddleware._exchange_programmatic_token(config)

    @pytest.mark.asyncio
    async def test_missing_project_id_raises(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_pat_abc')
        with pytest.raises(ValueError, match='project id is required'):
            await SessionStateMiddleware._exchange_programmatic_token(config)

    @pytest.mark.asyncio
    async def test_invalid_project_id_raises(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(
            storage_api_url='https://connection.keboola.com', storage_token='kbc_pat_abc', project_id='not-an-int'
        )
        with pytest.raises(ValueError, match='Invalid project id'):
            await SessionStateMiddleware._exchange_programmatic_token(config)

    @pytest.mark.asyncio
    async def test_happy_path_calls_resolver(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_abc', project_id='42')

        resolver = MagicMock()
        resolver.resolve = AsyncMock(return_value='legacy-storage-token')
        with patch('keboola_mcp_server.mcp.StorageTokenResolver', return_value=resolver) as resolver_cls:
            token = await SessionStateMiddleware._exchange_programmatic_token(config)

        assert token == 'legacy-storage-token'
        resolver_cls.assert_called_once_with(
            storage_api_url='https://connection.keboola.com', kubernetes_token_path='/var/run/secrets/token'
        )
        resolver.resolve.assert_awaited_once_with(subject_token='kbc_at_abc', project_id=42)

    @pytest.mark.asyncio
    async def test_end_to_end_from_authorization_header_and_x_kbc_project_id(self, monkeypatch) -> None:
        # Regression for the actual inbound shape this feature exists for: Kai's bearer sessions
        # send the token only as Authorization, never as a Storage-token header (that's the sapi
        # branch). apply_request_config must route it into storage_token before
        # _exchange_programmatic_token ever runs, or the exchange never triggers at all.
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com')
        http_rq = MagicMock(spec=Request)
        http_rq.headers = {'Authorization': 'Bearer kbc_at_abc', 'X-KBC-ProjectId': '42'}
        http_rq.scope = {}

        applied = SessionStateMiddleware.apply_request_config(http_rq, config, own_stack_storage_api_url=None)
        assert is_programmatic_token(applied.storage_token)

        resolver = MagicMock()
        resolver.resolve = AsyncMock(return_value='legacy-storage-token')
        with patch('keboola_mcp_server.mcp.StorageTokenResolver', return_value=resolver):
            token = await SessionStateMiddleware._exchange_programmatic_token(applied)

        assert token == 'legacy-storage-token'
        resolver.resolve.assert_awaited_once_with(subject_token='kbc_at_abc', project_id=42)

    @pytest.mark.asyncio
    async def test_create_session_state_skips_exchange_for_list_requests(self, monkeypatch) -> None:
        # The resolver call has up to a ~35s timeout; tools/list must stay fast, so
        # create_session_state(skip_token_exchange=True) must never reach it -- a raw, unexchanged
        # programmatic token is used instead (storage/metastore calls with it fail fast, not slow).
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_abc', project_id='42')
        runtime_info = ServerRuntimeInfo(transport='http-compat/streamable-http')

        with patch('keboola_mcp_server.mcp.StorageTokenResolver') as resolver_cls:
            state = await SessionStateMiddleware.create_session_state(
                config, runtime_info, own_stack_storage_api_url=None, skip_token_exchange=True
            )

        resolver_cls.assert_not_called()
        client = state[KeboolaClient.STATE_KEY]
        assert client.token == 'kbc_at_abc'
