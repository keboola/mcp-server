from typing import Optional

import pytest

from keboola_mcp_server.client import JsonDict, KeboolaClient
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
    """testing normalized and returned structures {storage: {...}} vs {...}"""
    component_raw = mock_component.copy()
    component_raw['type'] = 'extractor'  # set storage to {} to pass the validation for storage necessity
    component = Component.model_validate(component_raw)
    result = utils.validate_storage_configuration(input_storage, component)
    expected = output_storage  # we expect unwrapped structure
    assert result == expected


@pytest.mark.parametrize(
    ('component_type', 'storage', 'is_valid'),
    [
        ('writer', {}, False),
        ('transformation', {}, False),
        ('extractor', {}, True),
        ('application', {}, True),
        ('storage', {'input': {'tables': []}, 'output': {'tables': []}}, True),
    ],
)
def test_validate_storage_necessity(
    mock_component: dict, component_type: AllComponentTypes, storage: Optional[JsonDict], is_valid: bool
):
    """testing storage necessity validation"""
    component_raw = mock_component.copy()
    component_raw['type'] = component_type
    component = Component.model_validate(component_raw)
    if is_valid:
        utils._validate_storage_necessity(storage=storage, component=component)
    else:
        with pytest.raises(ValueError, match='Storage configuration is empty') as exception:
            utils._validate_storage_necessity(storage=storage, component=component)
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
    mock_component: dict, input_parameters: JsonDict, output_parameters: JsonDict, keboola_client: KeboolaClient
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
    mock_component: dict, input_parameters: JsonDict, output_parameters: JsonDict, keboola_client: KeboolaClient
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
async def test_validate_parameters_configuration_no_schema(
    mock_component: dict, input_schema: Optional[JsonDict], keboola_client: KeboolaClient
):
    """We expect passing the validation when no schema is provided"""
    input_parameters: JsonDict = {'a': 1}
    component = Component.model_validate(mock_component)
    component.configuration_row_schema = input_schema
    result = utils.validate_row_parameters_configuration(input_parameters, component)
    expected = input_parameters  # we expect unwrapped structure
    assert result == expected
