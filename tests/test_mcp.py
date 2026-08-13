import asyncio
import base64
import dataclasses
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from pydantic import BaseModel, Field
from starlette.requests import Request

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
from keboola_mcp_server.scope import SCOPE_KEY, SCOPE_TOKEN_ARG, SessionScope, resolve_scope_key
from keboola_mcp_server.workspace import WorkspaceManager


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
    async def test_list_tools_programmatic_pre_scope_skips_verify(self, mcp_context_client) -> None:
        # Programmatic session with no confirmed scope: tools/list must not call verify_token (it would
        # block connecting on a slow stack). Advertise the superset; on_call_tool still enforces.
        client = KeboolaClient.from_state(mcp_context_client.session.state)
        client.storage_client.verify_token = AsyncMock(side_effect=AssertionError('verify must not run pre-scope'))

        tools = [_tool('get_tables', read_only=True), _tool('create_flow'), _tool('get_semantic_context')]

        async def call_next(_):
            return tools

        context = SimpleNamespace(fastmcp_context=mcp_context_client)
        # programmatic token + no confirmed scope → filtering skipped, verify_token not called
        with patch('keboola_mcp_server.mcp.is_programmatic_token', return_value=True):
            result = await ToolsFilteringMiddleware().on_list_tools(context, call_next)
        assert {t.name for t in result} == {'get_tables', 'create_flow', 'get_semantic_context'}

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
    @pytest.mark.parametrize('tool_name', ['get_accessible_projects', 'set_project_scope'])
    async def test_call_tool_bootstrap_tools_skip_verify(
        self, mcp_context_client, keboola_client, tool_name: str
    ) -> None:
        # Bootstrap tools must work before any project is chosen (that's their purpose): calling
        # verify_token() here -- which needs a single-project context (X-KBC-ProjectId) -- would 401
        # before the tool's own body (which establishes that context) ever runs.
        keboola_client.storage_client.verify_token = AsyncMock(side_effect=AssertionError('verify must not run'))

        tool = _tool(tool_name)
        mcp_context_client.fastmcp = SimpleNamespace(get_tool=AsyncMock(return_value=tool))
        context = SimpleNamespace(fastmcp_context=mcp_context_client, message=SimpleNamespace(name=tool_name))

        expected = MagicMock()

        async def call_next(_):
            return expected

        result = await ToolsFilteringMiddleware().on_call_tool(context, call_next)
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

        context = SimpleNamespace(message=SimpleNamespace(), method=method, fastmcp_context=ctx)
        expected_result = object()

        async def call_next(_):
            return expected_result

        captured_configs: list[Config] = []
        captured_own_stack_urls: list[str | None] = []

        async def fake_create_session_state(cfg, _runtime_info, readonly=None, *, own_stack_storage_api_url):
            captured_configs.append(cfg)
            captured_own_stack_urls.append(own_stack_storage_api_url)
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('scope', 'expected_readonly'),
        [
            (None, None),
            (SessionScope(project_ids=[18], read_only=False, confirmed=True), None),
            (SessionScope(project_ids=[18], read_only=True, confirmed=True), True),
        ],
        ids=['no_scope', 'writable_scope', 'readonly_scope'],
    )
    async def test_on_request_threads_scope_read_only_into_session_state(self, scope, expected_readonly) -> None:
        # Security hardening RFC increment: a read-only confirmed scope must be enforced on the
        # base session client too, not just relied on via the (possibly-absent) scoped_token.
        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_x')
        server_state = ServerState(config=config, runtime_info=ServerRuntimeInfo(transport='stdio'))
        session = SimpleNamespace(state={})
        ctx = MagicMock(spec=Context)
        ctx.session = session
        ctx.request_context.lifespan_context = server_state

        args = {}
        if scope is not None:
            args[SCOPE_TOKEN_ARG] = scope.to_token(resolve_scope_key(config))
        context = SimpleNamespace(message=SimpleNamespace(arguments=args), method='tools/call', fastmcp_context=ctx)

        captured_readonly = []

        async def fake_create_session_state(cfg, _runtime_info, readonly=None, *, own_stack_storage_api_url):
            captured_readonly.append(readonly)
            return {}

        async def call_next(_):
            return 'ok'

        middleware = SessionStateMiddleware()
        with (
            patch.object(middleware, 'create_session_state', side_effect=fake_create_session_state),
            patch('keboola_mcp_server.mcp.get_http_request_or_none', return_value=None),
        ):
            await middleware.on_request(context, call_next)

        assert captured_readonly == [expected_readonly]

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

    def test_apply_request_config_injects_exchanged_session_token(self):
        from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
        from starlette.requests import Request

        from keboola_mcp_server.clients.auth_bridge import is_programmatic_token
        from keboola_mcp_server.oauth import ProxyAccessToken

        access_token = ProxyAccessToken(
            token='mcp_proxy',
            client_id='claude.ai',
            scopes=['claudai', 'projectless'],
            expires_at=int(time.time() + 3600),
            kbc_access_token='kbc_at_exchanged',
            session_id='session-1',
        )
        http_rq = Request({'type': 'http', 'headers': [], 'user': AuthenticatedUser(access_token)})
        config = Config(storage_api_url='https://connection.test.keboola.com')

        out_config = SessionStateMiddleware.apply_request_config(http_rq, config, own_stack_storage_api_url=None)

        assert out_config.storage_token == 'kbc_at_exchanged'
        assert is_programmatic_token(out_config.storage_token)

    @pytest.mark.asyncio
    async def test_on_request_persists_remint_of_expiring_oauth_scoped_token(self, monkeypatch) -> None:
        # End-to-end regression for the bug this fixes: a deployed OAuth session's scoped_token
        # expiring mid-conversation silently 401ed every fanned-out call thereafter, since nothing
        # ever refreshed it. on_request must re-mint it (via _resolve_local_tokens) and persist the
        # refresh to the OAuth session row so it's fixed for the rest of the session, not just once.
        from datetime import datetime, timezone

        from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
        from starlette.requests import Request

        from keboola_mcp_server.oauth import ProxyAccessToken

        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        access_token = ProxyAccessToken(
            token='mcp_proxy',
            client_id='claude.ai',
            scopes=['claudai', 'projectless'],
            expires_at=int(time.time() + 3600),
            kbc_access_token='kbc_at_fresh_oauth',
            session_id='session-1',
            scope_project_ids=[18, 83],
            scope_confirmed=True,
            scope_scoped_token='kbc_at_stale',
            scope_scoped_expires_at=datetime.fromtimestamp(time.time() - 1, tz=timezone.utc),
        )
        http_rq = Request({'type': 'http', 'headers': [], 'user': AuthenticatedUser(access_token)})

        session_store = AsyncMock()
        config = Config(storage_api_url='https://connection.test.keboola.com')
        server_state = ServerState(
            config=config,
            runtime_info=ServerRuntimeInfo(transport='http-compat/streamable-http'),
            session_store=session_store,
        )
        session = SimpleNamespace(state={})
        ctx = MagicMock(spec=Context)
        ctx.session = session
        ctx.request_context.lifespan_context = server_state
        context = SimpleNamespace(message=SimpleNamespace(arguments={}), method='tools/call', fastmcp_context=ctx)

        minted = SimpleNamespace(access_token='kbc_at_reminted', expires_at=time.time() + 3600)

        async def call_next(_):
            return 'ok'

        with (
            patch('keboola_mcp_server.mcp.get_http_request_or_none', return_value=http_rq),
            patch('keboola_mcp_server.mcp.exchange_scoped_token', AsyncMock(return_value=minted)),
            patch.object(SessionStateMiddleware, 'create_session_state', AsyncMock(return_value={})),
        ):
            result = await SessionStateMiddleware().on_request(context, call_next)

        assert result == 'ok'
        session_store.update_scope.assert_awaited_once()
        call = session_store.update_scope.await_args
        assert call.args == ('session-1',)
        assert call.kwargs['project_ids'] == [18, 83]
        assert call.kwargs['scoped_token'] == 'kbc_at_reminted'
        assert call.kwargs['scoped_expires_at'] == datetime.fromtimestamp(minted.expires_at, tz=timezone.utc)

    @pytest.mark.asyncio
    async def test_on_request_applies_persisted_kai_scope(self, monkeypatch) -> None:
        # A deployed, non-OAuth, programmatic-token session (Kai) with no scope_token argument and
        # no session_state_persists must fall back to the kai_scope_store, not auto-lease default.
        from starlette.requests import Request

        from keboola_mcp_server.auth_login import Introspection, ProjectAccess
        from keboola_mcp_server.session_store.kai_scope import KaiScope

        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        http_rq = Request({'type': 'http', 'headers': [(b'x-conversation-id', b'conv-1')], 'user': None})

        kai_scope_store = AsyncMock()
        kai_scope_store.get.return_value = KaiScope(project_ids=[18], read_only=False, confirmed=True)
        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_kai')
        server_state = ServerState(
            config=config,
            runtime_info=ServerRuntimeInfo(transport='http-compat/streamable-http'),
            kai_scope_store=kai_scope_store,
        )
        session = SimpleNamespace(state={})
        ctx = MagicMock(spec=Context)
        ctx.session = session
        ctx.request_context.lifespan_context = server_state
        context = SimpleNamespace(message=SimpleNamespace(arguments={}), method='tools/call', fastmcp_context=ctx)

        captured_scopes = []

        async def fake_create_session_state(cfg, _runtime_info, readonly=None, *, own_stack_storage_api_url):
            return {}

        async def call_next(_):
            captured_scopes.append(ctx.session.state.get(SCOPE_KEY))
            return 'ok'

        with (
            patch('keboola_mcp_server.mcp.get_http_request_or_none', return_value=http_rq),
            patch.object(SessionStateMiddleware, 'create_session_state', side_effect=fake_create_session_state),
            patch(
                'keboola_mcp_server.mcp.introspect_token',
                AsyncMock(
                    return_value=Introspection(
                        user_id=42, user_email=None, user_name=None, projects=[ProjectAccess(id=18)]
                    )
                ),
            ),
        ):
            result = await SessionStateMiddleware().on_request(context, call_next)

        assert result == 'ok'
        kai_scope_store.get.assert_awaited_once_with('conv-1', 42)
        assert captured_scopes == [SessionScope(project_ids=[18], read_only=False, confirmed=True)]


class TestReadPersistedKaiScope:
    """Kai session-scope persistence (pat_token_support/RFC.md, increment 6):
    SessionStateMiddleware._read_persisted_kai_scope."""

    @staticmethod
    def _introspection(project_ids: list[int], user_id: int | None = 42):
        from keboola_mcp_server.auth_login import Introspection, ProjectAccess

        return Introspection(
            user_id=user_id,
            user_email='kai@keboola.com',
            user_name='Kai',
            projects=[ProjectAccess(id=pid) for pid in project_ids],
        )

    @pytest.mark.asyncio
    async def test_returns_stored_scope_when_still_reachable(self) -> None:
        from keboola_mcp_server.session_store.kai_scope import KaiScope

        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_x')
        config = dataclasses.replace(config, conversation_id='conv-1')
        store = AsyncMock()
        store.get.return_value = KaiScope(project_ids=[18], read_only=False, confirmed=True)

        with patch(
            'keboola_mcp_server.mcp.introspect_token',
            AsyncMock(return_value=self._introspection([18, 83])),
        ):
            scope = await SessionStateMiddleware._read_persisted_kai_scope(config, store)

        assert scope is not None
        assert scope.project_ids == [18]
        assert scope.confirmed is True
        store.get.assert_awaited_once_with('conv-1', 42)
        store.drop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_drops_scope_when_a_project_is_no_longer_reachable(self) -> None:
        from keboola_mcp_server.session_store.kai_scope import KaiScope

        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_x')
        config = dataclasses.replace(config, conversation_id='conv-1')
        store = AsyncMock()
        store.get.return_value = KaiScope(project_ids=[18, 83], read_only=False, confirmed=True)

        with patch(
            'keboola_mcp_server.mcp.introspect_token',
            AsyncMock(return_value=self._introspection([18])),  # 83 dropped out
        ):
            scope = await SessionStateMiddleware._read_persisted_kai_scope(config, store)

        assert scope is None
        store.drop.assert_awaited_once_with('conv-1', 42)

    @pytest.mark.asyncio
    async def test_added_projects_do_not_invalidate_the_stored_scope(self) -> None:
        from keboola_mcp_server.session_store.kai_scope import KaiScope

        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_x')
        config = dataclasses.replace(config, conversation_id='conv-1')
        store = AsyncMock()
        store.get.return_value = KaiScope(project_ids=[18], read_only=False, confirmed=True)

        with patch(
            'keboola_mcp_server.mcp.introspect_token',
            AsyncMock(return_value=self._introspection([18, 999])),  # gained access to 999
        ):
            scope = await SessionStateMiddleware._read_persisted_kai_scope(config, store)

        assert scope is not None
        assert scope.project_ids == [18]
        store.drop.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_stored_row_returns_none(self) -> None:
        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_x')
        config = dataclasses.replace(config, conversation_id='conv-1')
        store = AsyncMock()
        store.get.return_value = None

        with patch(
            'keboola_mcp_server.mcp.introspect_token',
            AsyncMock(return_value=self._introspection([18])),
        ):
            scope = await SessionStateMiddleware._read_persisted_kai_scope(config, store)

        assert scope is None

    @pytest.mark.asyncio
    async def test_unresolvable_user_id_returns_none_without_lookup(self) -> None:
        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_x')
        config = dataclasses.replace(config, conversation_id='conv-1')
        store = AsyncMock()

        with patch(
            'keboola_mcp_server.mcp.introspect_token',
            AsyncMock(return_value=self._introspection([18], user_id=None)),
        ):
            scope = await SessionStateMiddleware._read_persisted_kai_scope(config, store)

        assert scope is None
        store.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_introspection_failure_returns_none(self) -> None:
        config = Config(storage_api_url='https://connection.test.keboola.com', storage_token='kbc_at_x')
        config = dataclasses.replace(config, conversation_id='conv-1')
        store = AsyncMock()

        with patch(
            'keboola_mcp_server.mcp.introspect_token',
            AsyncMock(side_effect=RuntimeError('network down')),
        ):
            scope = await SessionStateMiddleware._read_persisted_kai_scope(config, store)

        assert scope is None
        store.get.assert_not_awaited()


class TestProgrammaticTokenForwarding:
    """A programmatic token (kbc_at_/kbc_pat_) is always forwarded downstream as a Bearer (PSGO-261).

    KeboolaClient already sends a Bearer token to every service it wraps (Storage, Queue, AI,
    etc.), so no legacy per-project Storage token needs to be minted via the auth-bridge resolver
    -- that resolver call was removed entirely; see git history for the prior
    `_exchange_programmatic_token`/`StorageTokenResolver` code this replaced.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize('kubernetes_token_path', [None, '/var/run/secrets/token'], ids=['local', 'deployed'])
    @pytest.mark.parametrize('project_id', [None, '42'], ids=['no_project_id', 'with_project_id'])
    async def test_forwards_bearer_regardless_of_deployment_or_project_id(
        self, monkeypatch, kubernetes_token_path: str | None, project_id: str | None
    ) -> None:
        if kubernetes_token_path:
            monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', kubernetes_token_path)
        else:
            monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(
            storage_api_url='https://connection.keboola.com', storage_token='kbc_at_abc', project_id=project_id
        )
        runtime_info = ServerRuntimeInfo(transport='http')

        with patch.object(WorkspaceManager, 'create', AsyncMock(return_value='wsm')):
            state = await SessionStateMiddleware.create_session_state(
                config, runtime_info, own_stack_storage_api_url=None
            )

        client = state[KeboolaClient.STATE_KEY]
        assert client.bearer_token == 'kbc_at_abc'
        assert client.token == 'kbc_at_abc'
        assert client.headers.get('X-KBC-ProjectId') == project_id


class TestMaybeUseStoredSession:
    """Local HTTP with no token falls back to the stored PKCE session (PSGO-261)."""

    @pytest.mark.asyncio
    async def test_no_token_loads_stored_session(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com')  # no token
        with patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='kbc_at_stored')):
            out = await SessionStateMiddleware._maybe_use_stored_session(config)
        assert out.storage_token == 'kbc_at_stored'

    @pytest.mark.asyncio
    async def test_existing_token_is_noop(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_hdr')
        with patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(side_effect=AssertionError('must not read'))):
            out = await SessionStateMiddleware._maybe_use_stored_session(config)
        assert out is config

    @pytest.mark.asyncio
    async def test_list_request_uses_valid_stored_token_without_network_refresh(self, monkeypatch) -> None:
        # /list with a still-valid stored token must not do a network refresh: read it as-is.
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com')
        with (
            patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(side_effect=AssertionError('no network'))),
            patch(
                'keboola_mcp_server.mcp.load_tokens',
                return_value=SimpleNamespace(access_token='kbc_at_file', is_near_expiry=False),
            ),
        ):
            out = await SessionStateMiddleware._maybe_use_stored_session(config, refresh=False)
        assert out.storage_token == 'kbc_at_file'

    @pytest.mark.asyncio
    async def test_list_request_refreshes_only_an_expired_stored_token(self, monkeypatch) -> None:
        # /list with an EXPIRED stored token refreshes (once) so session-state Storage calls don't fail.
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com')
        with (
            patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(return_value='kbc_at_fresh')) as gat,
            patch(
                'keboola_mcp_server.mcp.load_tokens',
                return_value=SimpleNamespace(access_token='kbc_at_stale', is_near_expiry=True),
            ),
        ):
            out = await SessionStateMiddleware._maybe_use_stored_session(config, refresh=False)
        gat.assert_awaited_once()
        assert out.storage_token == 'kbc_at_fresh'

    @pytest.mark.asyncio
    async def test_deployed_is_noop(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com')
        out = await SessionStateMiddleware._maybe_use_stored_session(config)
        assert out is config

    @pytest.mark.asyncio
    async def test_no_stored_session_is_noop(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com')
        with patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(side_effect=RuntimeError('no creds'))):
            out = await SessionStateMiddleware._maybe_use_stored_session(config)
        assert out.storage_token is None


class TestReadPersistedLoginScope:
    """Local sessions are scoped at `login` time (Security hardening RFC increment) --
    SessionStateMiddleware._read_persisted_login_scope."""

    @pytest.mark.asyncio
    async def test_returns_confirmed_scope_from_stored_credential(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x')
        stored = SimpleNamespace(project_ids=[18, 83], read_only=True)
        with patch('keboola_mcp_server.mcp.load_tokens', return_value=stored):
            scope = await SessionStateMiddleware._read_persisted_login_scope(config)

        assert scope == SessionScope(project_ids=[18, 83], read_only=True, confirmed=True)

    @pytest.mark.asyncio
    async def test_none_when_credential_predates_the_scoping_choice(self, monkeypatch) -> None:
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x')
        stored = SimpleNamespace(project_ids=None, read_only=False)
        with patch('keboola_mcp_server.mcp.load_tokens', return_value=stored):
            scope = await SessionStateMiddleware._read_persisted_login_scope(config)

        assert scope is None

    @pytest.mark.asyncio
    async def test_none_when_not_local_programmatic(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x')
        with patch('keboola_mcp_server.mcp.load_tokens', side_effect=AssertionError('must not be called')):
            scope = await SessionStateMiddleware._read_persisted_login_scope(config)

        assert scope is None


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
    async def test_deployed_with_confirmed_scope_applies_active_project_id(self, monkeypatch) -> None:
        # Deployed sessions skip token refresh/re-minting (the resolver-exchange path in
        # create_session_state handles that once project_id is known) -- but a confirmed scope's
        # active project id must still be threaded through, or every call after set_project_scope
        # keeps building the active client from the unscoped whole-stack token (PSGO-261 regression:
        # get_accessible_projects worked, every subsequent scoped call 401'd).
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x')
        scope = SessionScope(project_ids=[18], scoped_token='kbc_at_scoped', confirmed=True)
        with patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(side_effect=AssertionError('no PKCE store'))):
            out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        assert out_config.project_id == '18'
        assert out_config.storage_token == 'kbc_at_x'  # untouched; resolver-exchange narrows it
        assert out_scope is scope  # untouched

    @pytest.mark.asyncio
    async def test_deployed_confirmed_scope_overrides_a_header_supplied_project_id(self, monkeypatch) -> None:
        # Regression: project_id is header-eligible (X-KBC-ProjectId), so a caller could set
        # config.project_id before scope resolution runs. A confirmed scope's active project must
        # always win -- otherwise a session scoped to project 18 could be silently redirected to
        # whatever project a request header names, via the base (un-swapped) client.
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_x', project_id='7')
        scope = SessionScope(project_ids=[18], confirmed=True)
        out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        assert out_config.project_id == '18'
        assert out_scope is scope

    @pytest.mark.asyncio
    async def test_deployed_near_expiry_scoped_token_is_reminted(self, monkeypatch) -> None:
        # Regression: a deployed (OAuth) session's scoped_token was never refreshed once minted by
        # set_project_scope, so it silently started 401ing every fanned-out call once it expired,
        # for the rest of the conversation, with no error pointing at the real cause.
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_fresh_oauth')
        scope = SessionScope(
            project_ids=[18, 83], scoped_token='kbc_at_stale', scoped_expires_at=time.time() - 1, confirmed=True
        )
        minted = SimpleNamespace(access_token='kbc_at_reminted', expires_at=time.time() + 3600)
        with patch('keboola_mcp_server.mcp.exchange_scoped_token', AsyncMock(return_value=minted)) as exch:
            out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        exch.assert_awaited_once_with(
            'https://connection.keboola.com', subject_token='kbc_at_fresh_oauth', project_ids=[18, 83], read_only=False
        )
        assert out_scope.scoped_token == 'kbc_at_reminted'
        assert out_scope.scoped_expires_at == minted.expires_at
        assert out_config.project_id == '18'

    @pytest.mark.asyncio
    async def test_deployed_scoped_token_not_near_expiry_is_untouched(self, monkeypatch) -> None:
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_fresh_oauth')
        scope = SessionScope(
            project_ids=[18, 83], scoped_token='kbc_at_live', scoped_expires_at=time.time() + 3600, confirmed=True
        )
        with patch('keboola_mcp_server.mcp.exchange_scoped_token', AsyncMock()) as exch:
            _out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        exch.assert_not_awaited()
        assert out_scope is scope

    @pytest.mark.asyncio
    async def test_deployed_remint_failure_keeps_old_scope(self, monkeypatch) -> None:
        # Same failure mode as before this fix (the caller keeps using the stale token and 401s
        # downstream) rather than crashing the request outright.
        monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/secrets/token')
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='kbc_at_fresh_oauth')
        scope = SessionScope(
            project_ids=[18], scoped_token='kbc_at_stale', scoped_expires_at=time.time() - 1, confirmed=True
        )
        with patch(
            'keboola_mcp_server.mcp.exchange_scoped_token', AsyncMock(side_effect=RuntimeError('exchange down'))
        ):
            _out_config, out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
        assert out_scope is scope
        assert out_scope.scoped_token == 'kbc_at_stale'

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
            out_config, _out_scope = await SessionStateMiddleware._resolve_local_tokens(config, scope)
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
    async def test_bearer_prefixed_token_is_stripped_before_exchange(self, monkeypatch) -> None:
        # A programmatic token supplied with an explicit `Bearer ` scheme (tolerated on input) must be
        # normalized to bare form; the exchange/introspect helpers add the scheme themselves, so a
        # pre-prefixed value would otherwise become `Authorization: Bearer Bearer …` (PSGO-261).
        monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
        config = Config(storage_api_url='https://connection.keboola.com', storage_token='Bearer kbc_pat_x')
        scope = SessionScope(project_ids=[11], scoped_token='kbc_at_stale', scoped_expires_at=time.time() - 1)
        minted = SimpleNamespace(access_token='kbc_at_fresh_scoped', expires_at=time.time() + 900)
        exch = AsyncMock(return_value=minted)
        with (
            # No stored PKCE session → falls back to the directly-supplied config token.
            patch('keboola_mcp_server.mcp.get_access_token', AsyncMock(side_effect=RuntimeError)),
            patch('keboola_mcp_server.mcp.exchange_scoped_token', exch),
        ):
            await SessionStateMiddleware._resolve_local_tokens(config, scope)
        exch.assert_awaited_once()
        assert exch.await_args.kwargs['subject_token'] == 'kbc_pat_x'  # bare, no `Bearer ` prefix

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


def _b64_key(fill: bytes) -> str:
    """A base64-encoded 32-byte KBC_SESSION_ENCRYPTION_KEY built from a repeated fill byte --
    deterministic test keys, distinct fills give distinct keys."""
    return base64.b64encode(fill * 32).decode('ascii')


class TestScopeToken:
    """The multi-project scope is carried by the caller as the `scope_token` tool argument, not read
    back from ctx.session.state -- which is rebuilt empty on every request under this server's
    default stateless-HTTP transport, so nothing survives there between one tool call and the next.
    Encrypted (AES-GCM), not just signed -- it may carry a live `scoped_token` bearer credential.
    """

    KEY_A = base64.b64decode(_b64_key(b'\x01'))
    KEY_B = base64.b64decode(_b64_key(b'\x02'))

    def test_round_trip(self) -> None:
        scope = SessionScope(
            project_ids=[11, 22], read_only=True, scoped_token='kbc_at_s', scoped_expires_at=1234.0, confirmed=True
        )
        token = scope.to_token(self.KEY_A)
        assert SessionScope.from_token(token, self.KEY_A) == scope

    def test_token_does_not_contain_the_scoped_token_in_the_clear(self) -> None:
        # The whole point of encrypting rather than just signing: a live bearer credential must
        # not be recoverable from the client-visible blob without the key.
        scope = SessionScope(project_ids=[11], scoped_token='kbc_at_super_secret_live_token', confirmed=True)
        token = scope.to_token(self.KEY_A)
        assert 'kbc_at_super_secret_live_token' not in token
        # Also not recoverable via a bare base64-decode (no key at all) -- unlike the old JWS.
        padded = token + '=' * (-len(token) % 4)
        assert b'kbc_at_super_secret_live_token' not in base64.urlsafe_b64decode(padded)

    def test_wrong_key_rejected(self) -> None:
        token = SessionScope(project_ids=[11], confirmed=True).to_token(self.KEY_A)
        with pytest.raises(Exception, match='.+'):
            SessionScope.from_token(token, self.KEY_B)

    def test_resolve_scope_key_prefers_configured_session_encryption_key(self) -> None:
        key = _b64_key(b'\x03')
        assert resolve_scope_key(Config(session_encryption_key=key)) == base64.b64decode(key)

    def test_resolve_scope_key_fallback_is_stable_within_process(self) -> None:
        config = Config()
        assert resolve_scope_key(config) == resolve_scope_key(config)

    @staticmethod
    def _call_tool_context(arguments: dict) -> SimpleNamespace:
        message = SimpleNamespace(name='get_tables', arguments=arguments)
        return SimpleNamespace(message=message, method='tools/call')

    def test_read_scope_from_request_decodes_and_pops_token(self) -> None:
        key = _b64_key(b'\x04')
        config = Config(session_encryption_key=key)
        scope = SessionScope(project_ids=[11, 22], confirmed=True)
        arguments = {'scope_token': scope.to_token(base64.b64decode(key)), 'other_arg': 1}

        context = self._call_tool_context(arguments)
        result = SessionStateMiddleware._read_scope_from_request(context, config)

        assert result == scope
        # Popped so the underlying tool function never sees it as an unexpected argument.
        assert 'scope_token' not in arguments
        assert arguments == {'other_arg': 1}

    @pytest.mark.parametrize(
        'arguments',
        [{}, {'scope_token': None}, {'scope_token': ''}, {'scope_token': 'not-a-valid-token'}],
        ids=['missing', 'none', 'empty', 'malformed'],
    )
    def test_read_scope_from_request_returns_none_when_absent_or_invalid(self, arguments: dict) -> None:
        config = Config(session_encryption_key=_b64_key(b'\x05'))
        context = self._call_tool_context(dict(arguments))
        assert SessionStateMiddleware._read_scope_from_request(context, config) is None

    def test_read_scope_from_request_ignores_non_call_tool_requests(self) -> None:
        # tools/list (and other non-call requests) have a .message, just not one with .arguments.
        context = SimpleNamespace(message=SimpleNamespace(), method='tools/list', fastmcp_context=None)
        assert SessionStateMiddleware._read_scope_from_request(context, Config()) is None

    def test_wrong_key_falls_back_to_no_scope_via_read_scope_from_request(self) -> None:
        # A token minted with a different key (e.g. a replica whose fallback key differs) must
        # degrade to "no scope" rather than raise -- the ask-first gate then re-prompts the caller.
        token = SessionScope(project_ids=[11], confirmed=True).to_token(self.KEY_A)
        context = self._call_tool_context({'scope_token': token})
        config = Config(session_encryption_key=base64.b64encode(self.KEY_B).decode())
        assert SessionStateMiddleware._read_scope_from_request(context, config) is None

    @staticmethod
    def _http_rq_with_oauth_user(**access_token_kwargs) -> SimpleNamespace:
        from keboola_mcp_server.oauth import ProxyAccessToken

        access_token = ProxyAccessToken(
            token='opaque-access-token',
            client_id='client-1',
            scopes=[],
            expires_at=None,
            kbc_access_token='kbc_at_x',
            **access_token_kwargs,
        )
        user = AuthenticatedUser(access_token)
        return SimpleNamespace(scope={'user': user})

    def test_read_persisted_oauth_scope_builds_scope_when_confirmed(self) -> None:
        http_rq = self._http_rq_with_oauth_user(
            session_id='session-1',
            scope_project_ids=[11, 22],
            scope_read_only=True,
            scope_confirmed=True,
            scope_scoped_token='kbc_at_scoped',
            scope_scoped_expires_at=datetime.fromtimestamp(1234.0, tz=timezone.utc),
        )
        scope = SessionStateMiddleware._read_persisted_oauth_scope(http_rq)
        assert scope == SessionScope(
            project_ids=[11, 22],
            read_only=True,
            scoped_token='kbc_at_scoped',
            scoped_expires_at=1234.0,
            confirmed=True,
        )

    @pytest.mark.parametrize(
        'access_token_kwargs',
        [
            {'scope_confirmed': False, 'scope_project_ids': [11]},
            {'scope_confirmed': True, 'scope_project_ids': None},
        ],
        ids=['unconfirmed', 'no_project_ids'],
    )
    def test_read_persisted_oauth_scope_returns_none_when_not_confirmed(self, access_token_kwargs: dict) -> None:
        http_rq = self._http_rq_with_oauth_user(**access_token_kwargs)
        assert SessionStateMiddleware._read_persisted_oauth_scope(http_rq) is None

    def test_read_persisted_oauth_scope_returns_none_for_non_oauth_request(self) -> None:
        assert SessionStateMiddleware._read_persisted_oauth_scope(None) is None
        assert SessionStateMiddleware._read_persisted_oauth_scope(SimpleNamespace(scope={})) is None

    def test_read_oauth_session_id_returns_session_id_for_oauth_request(self) -> None:
        http_rq = self._http_rq_with_oauth_user(session_id='session-1')
        assert SessionStateMiddleware._read_oauth_session_id(http_rq) == 'session-1'

    def test_read_oauth_session_id_returns_none_for_non_oauth_request(self) -> None:
        assert SessionStateMiddleware._read_oauth_session_id(None) is None
        assert SessionStateMiddleware._read_oauth_session_id(SimpleNamespace(scope={})) is None

    def test_read_persisted_local_scope_returns_confirmed_scope_from_session_state(self) -> None:
        scope = SessionScope(project_ids=[18, 83], confirmed=True)
        ctx = SimpleNamespace(session=SimpleNamespace(state={SCOPE_KEY: scope}))
        assert SessionStateMiddleware._read_persisted_local_scope(ctx) is scope

    def test_read_persisted_local_scope_returns_none_when_absent_or_invalid(self) -> None:
        assert (
            SessionStateMiddleware._read_persisted_local_scope(SimpleNamespace(session=SimpleNamespace(state={})))
            is None
        )
        assert (
            SessionStateMiddleware._read_persisted_local_scope(
                SimpleNamespace(session=SimpleNamespace(state={SCOPE_KEY: 'not-a-scope'}))
            )
            is None
        )
        assert (
            SessionStateMiddleware._read_persisted_local_scope(SimpleNamespace(session=SimpleNamespace(state=None)))
            is None
        )
        # Regression: a real (non-mocked) session object has no `.state` attribute at all until this
        # middleware sets one on a prior request -- must not raise AttributeError on the very first request.
        assert SessionStateMiddleware._read_persisted_local_scope(SimpleNamespace(session=object())) is None

    @pytest.mark.asyncio
    async def test_on_list_tools_advertises_scope_token_unconditionally(self) -> None:
        # Unlike MultiProjectMiddleware's `project_ids` filter, this must show up even with no scope
        # confirmed yet (indeed, even before get_accessible_projects has ever been called) -- a
        # tools/list request can't itself carry a scope_token, so scope state can't gate this.
        tool = _tool('get_tables', read_only=True)
        tool.parameters = {'type': 'object', 'properties': {}}
        tool.model_copy = lambda update, _t=tool: SimpleNamespace(name=_t.name, parameters=update['parameters'])
        context = SimpleNamespace(method='tools/list')

        async def call_next(_):
            return [tool]

        tools = await SessionStateMiddleware().on_list_tools(context, call_next)

        assert 'scope_token' in tools[0].parameters['properties']

    @pytest.mark.asyncio
    async def test_on_list_tools_skips_scope_token_when_session_state_persists(self) -> None:
        # stdio (and --no-stateless-http streamable-http) keep ctx.session.state across requests --
        # on_request reuses an already-confirmed scope straight from it, so there's nothing for the
        # caller to resend and advertising scope_token would just be clutter.
        tool = _tool('get_tables', read_only=True)
        tool.parameters = {'type': 'object', 'properties': {}}

        ctx = MagicMock(spec=Context)
        ctx.request_context.lifespan_context = ServerState(
            config=Config(), runtime_info=ServerRuntimeInfo(transport='stdio')
        )
        context = SimpleNamespace(method='tools/list', fastmcp_context=ctx)

        async def call_next(_):
            return [tool]

        tools = await SessionStateMiddleware().on_list_tools(context, call_next)

        assert 'scope_token' not in tools[0].parameters['properties']

    @pytest.mark.asyncio
    async def test_on_list_tools_advertises_scope_token_when_session_state_does_not_persist(self) -> None:
        tool = _tool('get_tables', read_only=True)
        tool.parameters = {'type': 'object', 'properties': {}}
        tool.model_copy = lambda update, _t=tool: SimpleNamespace(name=_t.name, parameters=update['parameters'])

        ctx = MagicMock(spec=Context)
        ctx.request_context.lifespan_context = ServerState(
            config=Config(),
            runtime_info=ServerRuntimeInfo(transport='http-compat/streamable-http', stateless_http=True),
        )
        context = SimpleNamespace(method='tools/list', fastmcp_context=ctx)

        async def call_next(_):
            return [tool]

        tools = await SessionStateMiddleware().on_list_tools(context, call_next)

        assert 'scope_token' in tools[0].parameters['properties']
