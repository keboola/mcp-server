import pytest
from pydantic import BaseModel, Field

from keboola_mcp_server.mcp import _exclude_none_serializer


class TestModel(BaseModel):
    field1: str | None = None
    field2: int | None = Field(default=None, serialization_alias='field2_alias')


@pytest.mark.parametrize(
    ('data', 'expected'),
    [
        (None, ''),
        # Case: Exclude none values from a single model
        (TestModel(field1='value1'), '{"field1":"value1"}'),
        # Case: Exclude none values from a list of models
        ([TestModel(field1='value1', field2=None), TestModel(field2=123)], '[{"field1":"value1"},{"field2":123}]'),
        # Case: Exclude none values from a dictionary with models
        (
            {'key1': TestModel(field1='value1'), 'key2': None, 'key3': TestModel(field2=456)},
            '{"key1":{"field1":"value1"},"key3":{"field2":456}}',
        ),
        # Case: Exclude none values from primitives
        ({'key1': 123, 'key2': None, 'key3': 'value'}, '{"key1":123,"key3":"value"}'),
        # Case: Exclude none values with nested structures
        (
            {'key1': [TestModel(field1='value1'), None], 'key2': {'nested_key': TestModel(field2=789)}},
            '{"key1":[{"field1":"value1"}],"key2":{"nested_key":{"field2":789}}}',
        ),
    ],
)
def test_exclude_none_serializer(data, expected):
    result = _exclude_none_serializer(data)
    assert result == expected
