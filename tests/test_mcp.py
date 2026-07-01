import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.tools.tool import ToolResult
from mcp import types as mt
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import (
    SCOPE_KEY,
    AggregateError,
    MultiProjectMiddleware,
    ServerState,
    SessionScope,
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
            '{"key1":[{"field3":"2025-02-03T10:11:12+02:00"}],'
            '"key2":{"nested_key":{"field2":789}},'
            '"key3":"2025-01-01T01:02:03"}',
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
            '[2]:\n'
            '  - field1: value1\n'
            '    field2[2]: item1,item2\n'
            '  - field1: value2\n'
            '    field2[2]: item3,item4',
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
            'users[2]{name,active}:\n'
            '  Alice,true\n'
            '  Bob,null\n'
            'meta:\n'
            '  field1: test\n'
            '  field2: null\n'
            '  field3: null',
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
        ('features', 'tool_name', 'expect_filtered'),
        [
            ([], 'search_semantic_context', True),
            ([], 'get_semantic_schema', True),
            (['mcp-semantic-tooling'], 'search_semantic_context', False),
            (['mcp-semantic-tooling'], 'get_semantic_schema', False),
            (['other-feature'], 'search_semantic_context', True),
            (['other-feature'], 'get_semantic_schema', True),
        ],
        ids=[
            'no_feature_search',
            'no_feature_schema',
            'with_feature_search',
            'with_feature_schema',
            'unrelated_feature_search',
            'unrelated_feature_schema',
        ],
    )
    async def test_list_tools_filters_semantic_tools_by_feature(
        self,
        mcp_context_client,
        features: list[str],
        tool_name: str,
        expect_filtered: bool,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.verify_token = AsyncMock(
            return_value={'owner': {'features': features}, 'admin': {}}
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
        ('features', 'tool_name', 'tool_tags', 'expect_error'),
        [
            ([], 'search_semantic_context', {'semantic'}, True),
            ([], 'get_semantic_schema', {'semantic'}, True),
            (['mcp-semantic-tooling'], 'search_semantic_context', {'semantic'}, False),
            (['mcp-semantic-tooling'], 'get_semantic_schema', {'semantic'}, False),
            ([], 'other_tool', set(), False),
        ],
        ids=[
            'no_feature_search_tool',
            'no_feature_schema_tool',
            'with_feature_search_tool',
            'with_feature_schema_tool',
            'no_feature_non_semantic_tool',
        ],
    )
    async def test_call_tool_blocks_semantic_tools_by_feature(
        self,
        mcp_context_client,
        features: list[str],
        tool_name: str,
        tool_tags: set[str],
        expect_error: bool,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.verify_token = AsyncMock(
            return_value={'owner': {'features': features}, 'admin': {}}
        )

        tool = _tool(tool_name, tags=tool_tags)
        mcp_context_client.fastmcp = SimpleNamespace(get_tool=AsyncMock(return_value=tool))
        context = SimpleNamespace(fastmcp_context=mcp_context_client, message=SimpleNamespace(name=tool_name))

        expected = MagicMock()

        async def call_next(_):
            return expected

        middleware = ToolsFilteringMiddleware()
        if expect_error:
            with pytest.raises(ToolError, match='Semantic Layer Tooling'):
                await middleware.on_call_tool(context, call_next)
        else:
            result = await middleware.on_call_tool(context, call_next)
            assert result is expected


class TestSessionStateMiddleware:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('method', 'expected_branch_id'),
        [
            ('tools/list', None),
            ('resources/list', None),
            ('prompts/list', None),
            ('tools/call', '999'),
            ('resources/read', '999'),
        ],
        ids=['tools_list', 'resources_list', 'prompts_list', 'tools_call', 'resources_read'],
    )
    async def test_on_request_branch_handling(self, method: str, expected_branch_id: str | None):
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

        async def fake_create_session_state(cfg, _runtime_info, readonly=None):
            captured_configs.append(cfg)
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


class TestResolveLocalTokens:
    """SessionStateMiddleware keeps local tokens fresh and re-mints the scoped token (PSGO-261)."""

    @pytest.mark.asyncio
    async def test_deployed_is_noop(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x')
        out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, None)
        assert out_config is config
        assert out_scope is None

    @pytest.mark.asyncio
    async def test_legacy_token_is_noop(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='legacy-sapi-token')
        out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, None)
        assert out_config is config
        assert out_scope is None

    @pytest.mark.asyncio
    async def test_programmatic_no_scope_refreshes_parent(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_old')
        with patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='kbc_at_fresh')):
            out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, None)
        assert out_config.storage_token == 'kbc_at_fresh'
        assert out_scope is None

    @pytest.mark.asyncio
    async def test_default_scope_uses_parent_token_without_minting(self, monkeypatch) -> None:
        # The default (auto-leased) multi-project scope carries no minted token: it uses the parent
        # token and just sets the active project — no exchange call.
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_old')
        scope = SessionScope(project_ids=[11, 22], read_only=False)
        with (
            patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='kbc_at_parent')),
            patch('keboola_mcp_server.mcp.exchange_scoped_token', AsyncMock()) as exch,
        ):
            out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        exch.assert_not_awaited()
        assert out_config.storage_token == 'kbc_at_parent'
        assert out_config.project_id == '11'  # active project = first in scope
        assert out_scope.scoped_token is None

    @pytest.mark.asyncio
    async def test_fresh_scoped_token_is_not_reminted(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_old')
        scope = SessionScope(project_ids=[11], scoped_token='kbc_at_live', scoped_expires_at=time.time() + 3600)
        with (
            patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='kbc_at_parent')),
            patch('keboola_mcp_server.mcp.exchange_scoped_token', AsyncMock()) as exch,
        ):
            out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        exch.assert_not_awaited()
        assert out_config.storage_token == 'kbc_at_live'

    @pytest.mark.asyncio
    async def test_near_expiry_scoped_token_is_reminted(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_old')
        scope = SessionScope(project_ids=[11, 22], scoped_token='kbc_at_stale', scoped_expires_at=time.time() - 1)
        minted = SimpleNamespace(access_token='kbc_at_fresh_scoped', expires_at=time.time() + 900)
        with (
            patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='kbc_at_parent')),
            patch('keboola_mcp_server.mcp.exchange_scoped_token', AsyncMock(return_value=minted)) as exch,
        ):
            out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        exch.assert_awaited_once()
        assert out_scope.scoped_token == 'kbc_at_fresh_scoped'
        assert out_config.storage_token == 'kbc_at_fresh_scoped'

    @pytest.mark.asyncio
    async def test_autolease_scopes_all_accessible_projects(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x')
        introspection = SimpleNamespace(
            projects=[SimpleNamespace(id=11), SimpleNamespace(id=22), SimpleNamespace(id=33)]
        )
        with (
            patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='kbc_at_parent')),
            patch('keboola_mcp_server.mcp.introspect_token', AsyncMock(return_value=introspection)),
        ):
            scope = await SessionStateMiddleware._autolease_default_scope(config)
        assert scope.project_ids == [11, 22, 33]
        assert scope.scoped_token is None  # default scope uses the parent token

    @pytest.mark.asyncio
    async def test_autolease_noop_when_deployed(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x')
        assert await SessionStateMiddleware._autolease_default_scope(config) is None


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
            (SessionScope(project_ids=[11, 22], confirmed=True), 'update_config', False),  # write: no fan-out
            (SessionScope(project_ids=[11, 22], confirmed=True), 'query_data', True),  # excluded tool
        ],
        ids=['no_scope', 'single_project', 'write_tool', 'excluded_tool'],
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
    async def test_read_tool_fans_out_per_project(self) -> None:
        scope = SessionScope(
            project_ids=[11, 22], scoped_token='kbc_at_s', scoped_expires_at=time.time() + 3600, confirmed=True
        )
        context, state = self._ctx(scope, 'get_tables', read_only=True)
        active_clients: list = []

        async def call_next(_):
            active_clients.append(state[KeboolaClient.STATE_KEY])
            return self._result('rows')

        with patch.object(
            MultiProjectMiddleware,
            '_client_for_project',
            AsyncMock(side_effect=lambda _ss, _token, pid, _ro: f'client-{pid}'),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        # Ran once per project, each against that project's client.
        assert active_clients == ['client-11', 'client-22']
        # Active client restored afterwards.
        assert state[KeboolaClient.STATE_KEY] == 'orig-client'
        # Per-project results are labelled in the text content.
        texts = [c.text for c in result.content]
        assert texts == ['=== project 11 ===', 'rows', '=== project 22 ===', 'rows']
        # Structured output is deep-merged (list fields concatenated) so it still validates the schema.
        assert result.structured_content == {'rows': ['rows', 'rows']}

    @pytest.mark.asyncio
    async def test_project_filter_single_target_runs_once(self) -> None:
        # project_ids filter narrows a multi-project scope to one project: one call, that project's
        # client, and the filter is stripped from the arguments the tool receives.
        scope = SessionScope(project_ids=[11, 22, 33], scoped_token='kbc_at_s', confirmed=True)
        context, state = self._ctx(scope, 'get_tables', read_only=True, arguments={'project_ids': [22]})
        seen_clients: list = []

        async def call_next(_):
            seen_clients.append(state[KeboolaClient.STATE_KEY])
            return self._result('t')

        with patch.object(
            MultiProjectMiddleware,
            '_client_for_project',
            AsyncMock(side_effect=lambda _ss, _token, pid, _ro: f'client-{pid}'),
        ):
            result = await MultiProjectMiddleware().on_call_tool(context, call_next)

        assert seen_clients == ['client-22']  # ran once, against project 22 only
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

        with patch.object(
            MultiProjectMiddleware,
            '_client_for_project',
            AsyncMock(side_effect=lambda _ss, _token, pid, _ro: f'client-{pid}'),
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
        excluded = _tool('query_data', read_only=True)
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
        assert 'project_ids' not in by_name['query_data'].parameters['properties']
        assert 'project_ids' not in by_name['update_config'].parameters['properties']
