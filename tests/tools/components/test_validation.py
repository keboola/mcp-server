import json
import logging
from typing import Optional

import jsonschema
import pytest

from keboola_mcp_server.clients.client import ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.clients.storage import ComponentAPIResponse, JsonDict
from keboola_mcp_server.tools import validation
from keboola_mcp_server.tools.components.model import Component


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
    assert validation._validate_storage_configuration_against_schema(valid_storage) == valid_storage


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
        validation._validate_storage_configuration_against_schema(
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
    result = validation._validate_storage_configuration_against_schema(input_storage)
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
    result = validation._validate_parameters_configuration_against_schema(input_parameters, accepting_schema)
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
    assert (
        validation.validate_flow_configuration_against_schema(valid_flow, flow_type=ORCHESTRATOR_COMPONENT_ID)
        == valid_flow
    )


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
        validation.validate_flow_configuration_against_schema(invalid_flow, flow_type=ORCHESTRATOR_COMPONENT_ID)


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
            validation._validate_parameters_configuration_against_schema(data, schema)
        except jsonschema.ValidationError:
            pytest.fail('ValidationError was raised when it should not have been')
    else:
        with pytest.raises(jsonschema.ValidationError):
            validation._validate_parameters_configuration_against_schema(data, schema)


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
            validation._validate_parameters_configuration_against_schema(data, schema)
        except jsonschema.ValidationError:
            pytest.fail('ValidationError was raised when it should not have been')
    else:
        with pytest.raises(jsonschema.ValidationError):
            validation._validate_parameters_configuration_against_schema(data, schema)


@pytest.mark.parametrize(
    ('input_storage', 'output_storage'),
    [
        ({'input': {}, 'output': {}}, {'input': {}, 'output': {}}),
        ({'storage': {'input': {}, 'output': {}}}, {'input': {}, 'output': {}}),
        ({}, {}),  # we expect passing when no storage is provided
        (None, {}),  # we expect passing when no storage is provided
        ({'storage': None}, {}),  # we expect passing when no storage is provided
    ],
)
def test_validate_storage_configuration_output(
    mock_component: dict, input_storage: Optional[JsonDict], output_storage: Optional[JsonDict]
):
    """testing expected storage output for a given storage input"""
    component_raw = mock_component.copy()
    component_raw['type'] = 'extractor'  # we need extractor to pass the validation for storage necessity
    api_component = ComponentAPIResponse.model_validate(component_raw)
    component = Component.from_api_response(api_component)
    result = validation._validate_storage_configuration(input_storage, component)
    expected = output_storage  # we expect unwrapped structure
    assert result == expected


@pytest.mark.parametrize(
    ('is_writer_row_based', 'storage', 'is_storage_row_based', 'error_message'),
    [
        # Non-row-based writer with input storage for root configuration
        (False, {'storage': {'input': {'files': []}}}, False, None),
        # Non-row-based writer without input storage for root configuration
        (
            False,
            {},
            False,
            'The "storage" must contain "input" mappings for the root configuration of the writer component',
        ),
        # Non-row-based writer with input storage for row configuration
        (False, {'storage': {'input': {'files': []}}}, True, None),  # should not fail, but log warning
        # Row-based writer with input storage
        (True, {'storage': {'input': {'files': []}}}, True, None),
        # Row-based writer without input storage
        (
            True,
            {},
            True,
            'The "storage" must contain "input" mappings for the row configuration of the writer component',
        ),
    ],
)
def test_validate_storage_of_row_based_and_root_based_writers(
    caplog,
    mock_component: dict,
    is_writer_row_based: bool,
    storage: Optional[JsonDict],
    is_storage_row_based: bool,
    error_message: Optional[str],
):
    """testing storage necessity validation"""
    component_raw = mock_component.copy()
    component_raw['type'] = 'writer'
    component_raw['component_flags'] = ['genericDockerUI-rows'] if is_writer_row_based else []

    api_component = ComponentAPIResponse.model_validate(component_raw)
    component = Component.from_api_response(api_component)
    if error_message is None:
        if not is_writer_row_based and is_storage_row_based:
            with caplog.at_level(logging.WARNING):
                validation._validate_storage_configuration(
                    storage=storage, component=component, is_row_storage=is_storage_row_based
                )
                assert 'Validating "storage" for row configuration of non-row-based writer' in caplog.text
        else:
            validation._validate_storage_configuration(
                storage=storage, component=component, is_row_storage=is_storage_row_based
            )
    else:
        with pytest.raises(ValueError, match=error_message) as exception:
            validation._validate_storage_configuration(
                storage=storage, component=component, is_row_storage=is_storage_row_based
            )
        assert component.component_id in str(exception.value)


@pytest.mark.parametrize(
    ('storage', 'is_valid'),
    [
        ({}, False),
        ({'storage': None}, False),
        ({'storage': {}}, False),
        ({'storage': {'input': {}}}, False),
        ({'storage': {'output': {}}}, False),
        ({'storage': {'anything-else': {}}}, False),
        ({'storage': {'input': {}, 'output': {}}}, False),  # empty input or output is not allowed
        ({'storage': {'input': {'tables': []}, 'output': {'tables': []}}}, True),
        ({'input': {'tables': []}, 'output': {'tables': []}}, True),
    ],
)
def test_validate_storage_of_sql_transformation(mock_component: dict, storage: Optional[JsonDict], is_valid: bool):
    """testing storage necessity validation"""
    component_raw = mock_component.copy()
    component_raw['type'] = 'transformation'
    # we test the validation for both SQL transformations
    for transformation_id in [validation.BIGQUERY_TRANSFORMATION_ID, validation.SNOWFLAKE_TRANSFORMATION_ID]:
        component_raw['id'] = transformation_id
        api_component = ComponentAPIResponse.model_validate(component_raw)
        component = Component.from_api_response(api_component)
        if is_valid:
            validation._validate_storage_configuration(storage=storage, component=component)
        else:
            with pytest.raises(
                ValueError,
                match='The "storage" must contain either "input" or "output" mappings in the configuration of the SQL ',
            ) as exception:
                validation._validate_storage_configuration(storage=storage, component=component)
            assert f'{component.component_id}' in str(exception.value)
            assert 'SQL transformation' in str(exception.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('input_parameters', 'output_parameters'),
    [
        ({'a': 1}, {'a': 1}),
        ({'parameters': {'a': 1, 'b': 2}}, {'a': 1, 'b': 2}),
    ],
)
async def test_validate_root_parameters_configuration_output(
    mock_component: dict, input_parameters: JsonDict, output_parameters: JsonDict
):
    """testing returned format structures  {...}"""
    accepting_schema = {'type': 'object', 'additionalProperties': True}  # accepts any json object
    component_raw = mock_component.copy()
    api_component = ComponentAPIResponse.model_validate(component_raw)
    component = Component.from_api_response(api_component)
    component.configuration_schema = accepting_schema
    result = validation.validate_root_parameters_configuration(input_parameters, component)
    expected = output_parameters  # we expect unwrapped structure
    assert result == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('input_parameters', 'output_parameters'),
    [
        ({'a': 1}, {'a': 1}),
        ({'parameters': {'a': 1, 'b': 2}}, {'a': 1, 'b': 2}),
    ],
)
async def test_validate_row_parameters_configuration_output(
    mock_component: dict, input_parameters: JsonDict, output_parameters: JsonDict
):
    """testing normalized and returned structures {parameters: {...}} vs {...}"""
    accepting_schema = {'type': 'object', 'additionalProperties': True}  # accepts any json object
    component_raw = mock_component.copy()
    api_component = ComponentAPIResponse.model_validate(component_raw)
    component = Component.from_api_response(api_component)
    component.configuration_row_schema = accepting_schema
    result = validation.validate_row_parameters_configuration(input_parameters, component)
    expected = output_parameters  # we expect unwrapped structure
    assert result == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('input_schema', [None, {}])
async def test_validate_parameters_configuration_no_schema(mock_component: dict, input_schema: Optional[JsonDict]):
    """We expect passing the validation when no schema is provided"""
    input_parameters: JsonDict = {'a': 1}
    component_raw = mock_component.copy()
    api_component = ComponentAPIResponse.model_validate(component_raw)
    component = Component.from_api_response(api_component)
    component.configuration_row_schema = input_schema
    result = validation.validate_row_parameters_configuration(input_parameters, component)
    expected = input_parameters  # we expect unwrapped structure
    assert result == expected


@pytest.mark.parametrize(
    ('file_path', 'is_parameter_key_present', 'is_valid'),
    [
        ('tests/resources/parameters/root_parameters_invalid.json', True, False),
        ('tests/resources/parameters/root_parameters_invalid.json', False, False),
        ('tests/resources/parameters/root_parameters_valid.json', True, True),
        ('tests/resources/parameters/root_parameters_valid.json', False, True),
    ],
)
def test_validate_parameters_root_real_scenario(
    mock_component: dict, file_path: str, is_parameter_key_present: bool, is_valid: bool
):
    """We test the validation of the root parameters configuration for a real scenario
    regardless of the parameters key presence we expect the same output"""
    with open(file_path, 'r') as f:
        input_parameters = json.load(f)
    assert 'parameters' not in input_parameters  # we do not expect the parameters key in the input
    with open('tests/resources/parameters/root_parameters_schema.json', 'r') as f:
        input_schema = json.load(f)

    component_raw = mock_component.copy()
    api_component = ComponentAPIResponse.model_validate(component_raw)
    component = Component.from_api_response(api_component)
    component.configuration_schema = input_schema
    modified_input_parameters = {'parameters': input_parameters} if is_parameter_key_present else input_parameters
    if is_valid:
        ret_params = validation.validate_root_parameters_configuration(modified_input_parameters, component)
        assert ret_params == input_parameters
    else:
        with pytest.raises(validation.RecoverableValidationError) as exception:
            validation.validate_root_parameters_configuration(modified_input_parameters, component, 'test oops')
        assert 'test oops' in str(exception.value)
