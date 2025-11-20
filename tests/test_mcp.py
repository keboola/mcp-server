from datetime import datetime, timedelta, timezone

import pytest
from pydantic import BaseModel, Field

from keboola_mcp_server.mcp import _exclude_none_serializer, toon_serializer


class SimpleModel(BaseModel):
    field1: str | None = None
    field2: int | None = Field(default=None, serialization_alias='field2_alias')
    field3: datetime | None = None


class NestedModel(BaseModel):
    field1: str | None = None
    field2: list[str] | None = None


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
