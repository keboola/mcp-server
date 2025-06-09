import json
from typing import Optional

import pytest

from keboola_mcp_server.client import JsonDict
from keboola_mcp_server.tools._validate import RecoverableValidationError
from keboola_mcp_server.tools.components import utils
from keboola_mcp_server.tools.components.model import AllComponentTypes, Component


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
    component = Component.model_validate(component_raw)
    result = utils.validate_storage_configuration(input_storage, component)
    expected = output_storage  # we expect unwrapped structure
    assert result == expected


@pytest.mark.parametrize(
    ('component_type', 'storage', 'is_valid'),
    [
        ('writer', {}, False),
        ('transformation', {}, False),
        ('transformation', {'storage': {}}, False),
        ('transformation', {'storage': None}, False),
        ('extractor', {}, True),
        ('application', {}, True),
        ('transformation', {'input': {'tables': []}, 'output': {'tables': []}}, True),
        ('transformation', {'storage': {'input': {'tables': []}, 'output': {'tables': []}}}, True),
    ],
)
def test_validate_storage_configuration_necessity(
    mock_component: dict, component_type: AllComponentTypes, storage: Optional[JsonDict], is_valid: bool
):
    """testing storage necessity validation"""
    component_raw = mock_component.copy()
    component_raw['type'] = component_type
    component = Component.model_validate(component_raw)
    if is_valid:
        utils.validate_storage_configuration(storage=storage, component=component)
    else:
        with pytest.raises(ValueError, match='Storage configuration cannot be empty') as exception:
            utils.validate_storage_configuration(storage=storage, component=component)
        assert f'{component.component_id}' in str(exception)
        assert f'{component.component_type}' in str(exception)


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
    component = Component.model_validate(mock_component)
    component.configuration_schema = accepting_schema
    result = utils.validate_root_parameters_configuration(input_parameters, component)
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
    component = Component.model_validate(mock_component)
    component.configuration_row_schema = accepting_schema
    result = utils.validate_row_parameters_configuration(input_parameters, component)
    expected = output_parameters  # we expect unwrapped structure
    assert result == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('input_schema', [None, {}])
async def test_validate_parameters_configuration_no_schema(mock_component: dict, input_schema: Optional[JsonDict]):
    """We expect passing the validation when no schema is provided"""
    input_parameters: JsonDict = {'a': 1}
    component = Component.model_validate(mock_component)
    component.configuration_row_schema = input_schema
    result = utils.validate_row_parameters_configuration(input_parameters, component)
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

    component = Component.model_validate(mock_component)
    component.configuration_schema = input_schema
    modified_input_parameters = {'parameters': input_parameters} if is_parameter_key_present else input_parameters
    if is_valid:
        ret_params = utils.validate_root_parameters_configuration(modified_input_parameters, component)
        assert ret_params == input_parameters
    else:
        with pytest.raises(RecoverableValidationError) as exception:
            utils.validate_root_parameters_configuration(modified_input_parameters, component, 'test oops')
        assert 'test oops' in str(exception.value)
