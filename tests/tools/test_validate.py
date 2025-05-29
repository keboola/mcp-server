import json
import logging

import jsonschema
import pytest

from keboola_mcp_server.tools import _validate


@pytest.mark.parametrize(
    ('schema_name', 'expected_keywords'),
    [
        (
            _validate.ConfigurationSchemaResourceName.STORAGE,
            ['type', 'properties', 'storage', 'input', 'output', 'tables', 'files', 'destination', 'source'],
        )
    ],
)
def test_load_schema(schema_name, expected_keywords):
    schema = _validate._load_schema(schema_name)
    assert schema is not None
    for keyword in expected_keywords:
        assert keyword in str(schema)


@pytest.mark.parametrize(
    ('valid_storage_path'),
    [
        # 1. Output table with delete_where using where_filters
        ('tests/resources/storage_valid_1.json'),
        # 2. Minimal valid input and output tables
        ('tests/resources/storage_valid_2.json'),
        # 3. Input and output files
        ('tests/resources/storage_valid_3.json'),
        # 4. Input table with where_column and where_values, output table with schema
        ('tests/resources/storage_valid_4.json'),
    ],
)
def test_validate_storage_valid(valid_storage_path: str):
    with open(valid_storage_path, 'r') as f:
        valid_storage = json.load(f)
    # returns the same valid storage no exception is raised
    assert _validate.validate_storage(valid_storage) == valid_storage


@pytest.mark.parametrize(
    ('invalid_storage_path'),
    [
        # 1. Input table missing required property (source or source_search)
        # Each item in tables must have either source or source_search (enforced by oneOf). This object has neither.
        ('tests/resources/storage_invalid_1.json'),
        # 2. The table_files item missing required destination (output table_files)
        # Each item in table_files must have both source and destination.
        ('tests/resources/storage_invalid_2.json'),
        # 3. Missing source in input, missing destination in output
        ('tests/resources/storage_invalid_3.json'),
        # 4. Schema present with forbidden properties (output tables)
        # If schema is present, columns (and several other properties) must not be present (enforced by allOf).
        ('tests/resources/storage_invalid_4.json'),
        # 5. Output table missing required property (destination or source)
        # Both destination and source are required for each output table.
        ('tests/resources/storage_invalid_5.json'),
        # 6. The where_operator has an invalid value (input tables)
        # where_operator must be either 'eq' or 'ne', not 'gt'.
        ('tests/resources/storage_invalid_6.json'),
        # 7. The files item missing required source (output files)
        # Each item in files must have a source property
        ('tests/resources/storage_invalid_7.json'),
    ],
)
def test_validate_storage_invalid(invalid_storage_path: str):
    """We expect the json will not be validated and raise a RecoverableValidationError"""
    with open(invalid_storage_path, 'r') as f:
        invalid_storage = json.load(f)
    with pytest.raises(_validate.RecoverableValidationError) as exc_info:
        _validate.validate_storage(invalid_storage, initial_message='This is a test message')
    err = exc_info.value
    assert 'This is a test message' in str(err)
    assert f'{json.dumps(invalid_storage, indent=2)}' in str(err)


def test_validate_json_against_schema_invalid_schema(caplog):
    """
    We expect passing when the schema is invalid since it is not an Agent error.
    However, we expect logging the error.
    """
    corrupted_schema = {'type': 'int', 'minimum': 5}
    with caplog.at_level(logging.ERROR):
        _validate._validate_json_against_schema(
            json_data={'foo': 1}, schema=corrupted_schema, initial_message='This is a test message'
        )
    assert f'schema: {corrupted_schema}' in caplog.text


def test_recoverable_validation_error_str():
    err = jsonschema.ValidationError('Validation error', instance={'foo': 1})
    rve = _validate.RecoverableValidationError.create_from_values(
        err, invalid_json={'foo': 1}, initial_message='Initial msg'
    )
    s = str(rve)
    assert 'Validation error' in s
    assert 'Initial msg' in s
    assert 'Recovery instructions:' in s
    assert '"foo": 1' in s


@pytest.mark.parametrize(
    "input_schema,expected_schema",
    [
        # Case 1: required true -> []
        ({"type": "object", "required": True}, {"type": "object", "required": []}),
        # Case 2: required false -> []
        ({"type": "object", "required": False}, {"type": "object", "required": []}),
        # Case 3: required as list (should remain unchanged)
        ({"type": "object", "required": ["foo", "bar"]}, {"type": "object", "required": ["foo", "bar"]}),
        # Case 4: required missing (should not be added)
        ({"type": "object"}, {"type": "object"}),
        # Case 5: nested properties with required true/false
        (
            {
                "type": "object",
                "required": ["foo", "bar"],
                "properties": {
                    "foo": {
                        "type": "string",
                        "required": ['foo2'],
                        "properties": {"foo2": {"type": "string", "required": True}},
                    },
                    "bar": {"type": "number", "required": False},
                    "baz": {"type": "boolean", "required": ["baz"]},
                },
            },
            {
                "type": "object",
                "required": ["foo", "bar"],
                "properties": {
                    "foo": {
                        "type": "string",
                        "required": ['foo2'],
                        "properties": {"foo2": {"type": "string", "required": []}},
                    },
                    "bar": {"type": "number", "required": []},
                    "baz": {"type": "boolean", "required": ["baz"]},
                },
            },
        ),
        # Case 6: input is not a dict (should return as is)
        ("notadict", "notadict"),
        (123, 123),
        (None, None),
        # Case 7: required as string (should become [])
        ({"type": "object", "required": "yes"}, {"type": "object", "required": []}),
        # Case 8: required as int (should remain unchanged, as not str/bool)
        ({"type": "object", "required": 1}, {"type": "object", "required": []}),
    ],
)
def test_normalize_schema(input_schema, expected_schema):
    result = _validate.KeboolaParametersValidator.normalize_schema(input_schema)
    assert result == expected_schema
