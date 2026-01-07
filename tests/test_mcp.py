import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import BaseModel, Field

from keboola_mcp_server.mcp import (
    AggregateError,
    _exclude_none_serializer,
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
