from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from keboola_mcp_server.client import JsonDict, KeboolaClient
from keboola_mcp_server.tools._validate import validate_parameters, validate_storage
from keboola_mcp_server.tools.components import utils


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
def test_validate_storage_configuration_output(input_storage: Optional[JsonDict], output_storage: Optional[JsonDict]):
    """testing normalized and returned structures {storage: {...}} vs {...}"""
    if input_storage is not None and input_storage.get('storage') is not None:
        result = validate_storage(input_storage)
        expected = {'storage': output_storage}
        assert result == expected  # we expect wrapped structure (normalized)

    result = utils.validate_storage_configuration(input_storage)
    expected = output_storage  # we expect unwrapped structure
    assert result == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('input_parameters', 'output_parameters'),
    [
        ({'a': 1}, {'a': 1}),
        ({'parameters': {'a': 1, 'b': 2}}, {'a': 1, 'b': 2}),
    ],
)
async def test_validate_root_parameters_configuration_output(
    mocker, input_parameters: JsonDict, output_parameters: JsonDict, keboola_client: KeboolaClient
):
    """testing normalized and returned structures {parameters: {...}} vs {...}"""
    accepting_schema = {'type': 'object', 'additionalProperties': True}  # accepts any json object
    result = validate_parameters(input_parameters, accepting_schema)
    expected = {'parameters': output_parameters}
    assert result == expected  # we expect wrapped structure (normalized)

    component = MagicMock()
    component.configuration_schema = accepting_schema
    mocker.patch('keboola_mcp_server.tools.components.utils._get_component', new=AsyncMock(return_value=component))
    result = await utils.validate_root_parameters_configuration(keboola_client, input_parameters, 'component-id')
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
    mocker, input_parameters: JsonDict, output_parameters: JsonDict, keboola_client: KeboolaClient
):
    """testing normalized and returned structures {parameters: {...}} vs {...}"""
    accepting_schema = {'type': 'object', 'additionalProperties': True}  # accepts any json object
    result = validate_parameters(input_parameters, accepting_schema)
    expected = {'parameters': output_parameters}
    assert result == expected  # we expect wrapped structure (normalized)

    component = MagicMock()
    component.configuration_schema = accepting_schema
    mocker.patch('keboola_mcp_server.tools.components.utils._get_component', new=AsyncMock(return_value=component))
    result = await utils.validate_row_parameters_configuration(keboola_client, input_parameters, 'component-id')
    expected = output_parameters  # we expect unwrapped structure
    assert result == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('input_schema', [None, {}])
async def test_validate_parameters_configuration_no_schema(
    mocker, input_schema: Optional[JsonDict], keboola_client: KeboolaClient
):
    """We expect passing the validation when no schema is provided"""
    input_parameters: JsonDict = {'a': 1}
    component = MagicMock()
    component.configuration_row_schema = input_schema
    mocker.patch('keboola_mcp_server.tools.components.utils._get_component', new=AsyncMock(return_value=component))
    result = await utils.validate_row_parameters_configuration(keboola_client, input_parameters, 'component-id')
    expected = input_parameters  # we expect unwrapped structure
    assert result == expected
