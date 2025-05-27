import json

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


def test_validate_json_against_schema_invalid_schema():
    """
    We expect logging the error and raising a SchemaError.
    - only in case when the schema provided is invalid (not agent error)
    """
    corrupted_schema = {'type': 'int', 'minimum': 5}
    with pytest.raises(jsonschema.SchemaError) as exc_info:
        _validate._validate_json_against_schema(
            json_data={'foo': 1}, schema=corrupted_schema, initial_message='This is a test message'
        )
    err = exc_info.value
    assert f'schema: {corrupted_schema}' in err.message


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
