import json
import logging

import jsonschema
import pytest

from keboola_mcp_server.client import JsonDict
from keboola_mcp_server.tools import validation


@pytest.mark.parametrize(
    ('schema_name', 'expected_keywords'),
    [
        (
            validation.ConfigurationSchemaResources.STORAGE,
            ['type', 'properties', 'storage', 'input', 'output', 'tables', 'files', 'destination', 'source'],
        )
    ],
)
def test_load_schema(schema_name, expected_keywords):
    schema = validation._load_schema(schema_name)
    assert schema is not None
    for keyword in expected_keywords:
        assert keyword in str(schema)


@pytest.mark.parametrize(
    ('valid_storage_path'),
    [
        # 1. Output table with delete_where using where_filters
        ('tests/resources/storage/storage_valid_1.json'),
        # 2. Minimal valid input and output tables
        ('tests/resources/storage/storage_valid_2.json'),
        # 3. Input and output files
        ('tests/resources/storage/storage_valid_3.json'),
        # 4. Input table with where_column and where_values, output table with schema
        ('tests/resources/storage/storage_valid_4.json'),
    ],
)
def test_validate_storage_valid(valid_storage_path: str):
    with open(valid_storage_path, 'r') as f:
        valid_storage = json.load(f)
    # returns the same valid storage no exception is raised
    assert validation.validate_storage_configuration_against_schema(valid_storage) == valid_storage


@pytest.mark.parametrize(
    ('invalid_storage_path'),
    [
        # 1. Input table missing required property (source or source_search)
        # Each item in tables must have either source or source_search (enforced by oneOf). This object has neither.
        ('tests/resources/storage/storage_invalid_1.json'),
        # 2. The table_files item missing required destination (output table_files)
        # Each item in table_files must have both source and destination.
        ('tests/resources/storage/storage_invalid_2.json'),
        # 3. Missing source in input, missing destination in output
        ('tests/resources/storage/storage_invalid_3.json'),
        # 4. Schema present with forbidden properties (output tables)
        # If schema is present, columns (and several other properties) must not be present (enforced by allOf).
        ('tests/resources/storage/storage_invalid_4.json'),
        # 5. Output table missing required property (destination or source)
        # Both destination and source are required for each output table.
        ('tests/resources/storage/storage_invalid_5.json'),
        # 6. The where_operator has an invalid value (input tables)
        # where_operator must be either 'eq' or 'ne', not 'gt'.
        ('tests/resources/storage/storage_invalid_6.json'),
        # 7. The files item missing required source (output files)
        # Each item in files must have a source property
        ('tests/resources/storage/storage_invalid_7.json'),
    ],
)
def test_validate_storage_invalid(invalid_storage_path: str):
    """We expect the json will not be validated and raise a RecoverableValidationError"""
    with open(invalid_storage_path, 'r') as f:
        invalid_storage = json.load(f)
    with pytest.raises(validation.RecoverableValidationError) as exc_info:
        validation.validate_storage_configuration_against_schema(
            invalid_storage, initial_message='This is a test message'
        )
    err = exc_info.value
    assert 'This is a test message' in str(err)
    assert f'{json.dumps(invalid_storage, indent=2)}' in str(err)


@pytest.mark.parametrize(
    ('input_storage', 'output_storage'),
    [
        ({'input': {}, 'output': {}}, {'input': {}, 'output': {}}),
        ({'storage': {'input': {}, 'output': {}}}, {'storage': {'input': {}, 'output': {}}}),
    ],
)
def test_validate_storage_output_format(input_storage, output_storage):
    """Test that storage configuration validation preserves the input format - whether the input contains a 'storage'
    key or not, the output will match the input structure exactly."""
    result = validation.validate_storage_configuration_against_schema(input_storage)
    assert result == output_storage


@pytest.mark.parametrize(
    ('input_parameters', 'output_parameters'),
    [
        ({'a': 1}, {'a': 1}),
        ({'parameters': {'a': 1, 'b': 2}}, {'parameters': {'a': 1, 'b': 2}}),
    ],
)
def test_validate_parameters_output_format(input_parameters, output_parameters):
    """Test that parameters configuration validation preserves the input format - whether the input contains a
    'parameters' key or not, the output will match the input structure exactly."""
    accepting_schema = {'type': 'object', 'additionalProperties': True}  # accepts any json object
    result = validation.validate_parameters_configuration_against_schema(input_parameters, accepting_schema)
    assert result == output_parameters


@pytest.mark.parametrize(
    ('valid_flow_path'),
    [
        ('tests/resources/flow/flow_valid_1.json'),
        ('tests/resources/flow/flow_valid_2.json'),
        ('tests/resources/flow/flow_valid_3.json'),
    ],
)
def test_validate_flow_valid(valid_flow_path: str):
    with open(valid_flow_path, 'r') as f:
        valid_flow = json.load(f)
    assert validation.validate_flow_configuration_against_schema(valid_flow) == valid_flow


@pytest.mark.parametrize(
    'invalid_flow_path',
    [
        'tests/resources/flow/flow_invalid_1.json',
        'tests/resources/flow/flow_invalid_2.json',
        'tests/resources/flow/flow_invalid_3.json',
        'tests/resources/flow/flow_invalid_4.json',
        'tests/resources/flow/flow_invalid_5.json',
        'tests/resources/flow/flow_invalid_6.json',
    ],
)
def test_validate_flow_invalid(invalid_flow_path: str):
    with open(invalid_flow_path, 'r') as f:
        invalid_flow = json.load(f)
    with pytest.raises(validation.RecoverableValidationError):
        validation.validate_flow_configuration_against_schema(invalid_flow)


def test_validate_json_against_schema_invalid_schema(caplog):
    """
    We expect passing when the schema is invalid since it is not an Agent error.
    However, we expect logging the error.
    """
    corrupted_schema = {'type': 'int', 'minimum': 5}
    with caplog.at_level(logging.ERROR):
        validation._validate_json_against_schema(
            json_data={'foo': 1}, schema=corrupted_schema, initial_message='This is a test message'
        )
    assert f'schema: {corrupted_schema}' in caplog.text


def test_recoverable_validation_error_str():
    err = jsonschema.ValidationError('Validation error', instance={'foo': 1})
    rve = validation.RecoverableValidationError.create_from_values(
        err, invalid_json={'foo': 1}, initial_message='Initial msg'
    )
    s = str(rve)
    assert 'Validation error' in s
    assert 'Initial msg' in s
    assert 'Recovery instructions:' in s
    assert '"foo": 1' in s


@pytest.mark.parametrize(
    ('input_schema', 'expected_schema'),
    [
        # Case 1: required true -> remove the required field
        ({'type': 'object', 'required': True}, {'type': 'object'}),
        # Case 2: required false -> remove the required field
        ({'type': 'object', 'required': False}, {'type': 'object'}),
        # Case 3: required as list (should remain unchanged)
        ({'type': 'object', 'required': ['foo', 'bar']}, {'type': 'object', 'required': ['foo', 'bar']}),
        # Case 4: required missing (should not be added)
        ({'type': 'object'}, {'type': 'object'}),
        # Case 5: nested properties with required true/false
        (
            {
                'type': 'object',
                'required': ['foo'],
                'properties': {
                    'foo': {
                        'type': 'string',
                        'required': ['foo2'],
                        'properties': {'foo2': {'type': 'string', 'required': True}},
                    },
                    'bar': {'type': 'number', 'required': False},
                    'baz': {'type': 'boolean', 'required': ['baz']},
                },
            },
            {
                'type': 'object',
                'required': ['foo'],
                'properties': {
                    'foo': {
                        'type': 'string',
                        'required': ['foo2'],
                        'properties': {'foo2': {'type': 'string'}},
                    },
                    'bar': {'type': 'number'},
                    'baz': {'type': 'boolean', 'required': ['baz']},
                },
            },
        ),
        # Case 6: nested properties with required true/false - add if required and remove if Not
        (
            {
                'type': 'object',
                'required': ['foo2'],
                'properties': {
                    'foo': {'type': 'string', 'required': True},
                    'foo2': {'type': 'string', 'required': 'False'},
                },
            },
            {
                'type': 'object',
                'required': ['foo'],
                'properties': {
                    'foo': {'type': 'string'},
                    'foo2': {'type': 'string'},
                },
            },
        ),
        # Case 7: properties values are not a dict type (should return as it is)
        ({'properties': {'a': 1}}, {'properties': {'a': 1}}),
        # Case 8: properties are an empty list should convert to an empty dict
        ({'properties': []}, {'properties': {}}),
        # Case 9: required as string -> remove the required field
        ({'type': 'object', 'required': 'yes'}, {'type': 'object'}),
        # Case 10: required as int - remove the required field
        ({'type': 'object', 'required': 1}, {'type': 'object'}),
        # Case 11: empty schema should return an empty schema
        ({}, {}),
    ],
)
def test_normalize_schema(input_schema: JsonDict, expected_schema: JsonDict):
    result = validation.KeboolaParametersValidator.sanitize_schema(input_schema)
    assert result == expected_schema


@pytest.mark.parametrize(
    ('input_schema'),
    [
        # case 1: properties are non-empty list -> fail
        {'type': 'object', 'properties': [{'type': 'string'}]},
        # case 2: properties are not a dict -> fail
        {'type': 'object', 'properties': 1},
    ],
)
def test_normalize_schema_invalid_parameters(input_schema: JsonDict):
    with pytest.raises(jsonschema.SchemaError):
        validation.KeboolaParametersValidator.sanitize_schema(input_schema)


@pytest.mark.parametrize(
    ('schema_path', 'json_data'),
    [
        # we pass the schema and json_data which are expected to be valid
        ('tests/resources/parameters/root_parameters_schema.json', {'embedding_settings': {'provider_type': 'openai'}}),
        (
            'tests/resources/parameters/row_parameters_schema.json',
            {'text_column': 'this is the only required field of this schema'},
        ),
    ],
)
def test_schema_validation(caplog, schema_path: str, json_data: JsonDict):
    """Testing the failure of the jsonschema.validate and the success of the KeboolaParametersValidator.validate"""
    with open(schema_path, 'r') as f:
        schema = json.load(f)

    with caplog.at_level(logging.ERROR):
        # we expect the error logging when schema is invalid but not failure since it is not an Agent error
        validation._validate_json_against_schema(json_data, schema, validate_fn=jsonschema.validate)
    assert f'schema: {schema}' in caplog.text

    try:
        validation._validate_json_against_schema(
            json_data, schema, validate_fn=validation.KeboolaParametersValidator.validate
        )
    except jsonschema.ValidationError:
        pytest.fail('ValidationError was raised when it should not have been')


@pytest.mark.parametrize(
    ('schema_path', 'data_path', 'valid'),
    [
        # text_column is required and correctly set to "notes" (exists in columns).
        # primary_key is required only when load_type = incremental_load, and it's provided ("email").
        # Chunking settings are only present when enable_chunking = true, which is respected.
        (
            'tests/resources/parameters/row_parameters_schema.json',
            'tests/resources/parameters/row_parameters_valid.json',
            True,
        ),
        # Missing required field: text_column
        # Missing required field: primary_key (required when load_type = incremental_load)
        # Invalid batch_size: 0 (minimum allowed: 1)
        # Invalid chunk_size: 9000 (maximum allowed: 8000)
        # Invalid chunk_overlap: -10 (minimum allowed: 0)
        (
            'tests/resources/parameters/row_parameters_schema.json',
            'tests/resources/parameters/row_parameters_invalid.json',
            False,
        ),
    ],
)
def test_validate_row_parameters(schema_path: str, data_path: str, valid: bool):
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    with open(data_path, 'r') as f:
        data = json.load(f)
    if valid:
        try:
            validation.validate_parameters_configuration_against_schema(data, schema)
        except jsonschema.ValidationError:
            pytest.fail('ValidationError was raised when it should not have been')
    else:
        with pytest.raises(jsonschema.ValidationError):
            validation.validate_parameters_configuration_against_schema(data, schema)


@pytest.mark.parametrize(
    ('schema_path', 'data_path', 'valid'),
    [
        # embedding_settings is required.
        # When provider_type is "openai", the openai_settings object must include model and #api_key.
        (
            'tests/resources/parameters/root_parameters_schema.json',
            'tests/resources/parameters/root_parameters_valid.json',
            True,
        ),
        # "embedding_settings" is required at the top level.
        # Even though qdrant_settings has all required fields, it doesn't satisfy the top-level schema.
        (
            'tests/resources/parameters/root_parameters_schema.json',
            'tests/resources/parameters/root_parameters_invalid.json',
            False,
        ),
    ],
)
def test_validate_root_parameters(schema_path: str, data_path: str, valid: bool):
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    with open(data_path, 'r') as f:
        data = json.load(f)
    if valid:
        try:
            validation.validate_parameters_configuration_against_schema(data, schema)
        except jsonschema.ValidationError:
            pytest.fail('ValidationError was raised when it should not have been')
    else:
        with pytest.raises(jsonschema.ValidationError):
            validation.validate_parameters_configuration_against_schema(data, schema)
