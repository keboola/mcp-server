import re
from typing import Any, Sequence, Union

import pytest

from keboola_mcp_server.tools.components.model import (
    ComponentType,
    ConfigParamRemove,
    ConfigParamReplace,
    ConfigParamSet,
)
from keboola_mcp_server.tools.components.utils import (
    TransformationConfiguration,
    _apply_param_update,
    clean_bucket_name,
    get_transformation_configuration,
    handle_component_types,
    update_params,
)


@pytest.mark.parametrize(
    ('component_type', 'expected'),
    [
        ('application', ['application']),
        (['extractor', 'writer'], ['extractor', 'writer']),
        (None, ['application', 'extractor', 'writer']),
        ([], ['application', 'extractor', 'writer']),
    ],
)
def test_handle_component_types(
    component_type: Union[ComponentType, Sequence[ComponentType], None],
    expected: list[ComponentType],
):
    """Test list_component_configurations tool with core component."""
    assert handle_component_types(component_type) == expected


@pytest.mark.parametrize(
    ('sql_statements', 'created_table_names', 'transformation_name', 'expected_bucket_id'),
    [
        # testing with multiple sql statements and no output table mappings
        # it should not create any output tables
        (['SELECT * FROM test', 'SELECT * FROM test2'], [], 'test name', 'out.c-test-name'),
        # testing with multiple sql statements and output table mappings
        # it should create output tables according to the mappings
        (
            [
                'CREATE OR REPLACE TABLE "test_table_1" AS SELECT * FROM "test";',
                'CREATE OR REPLACE TABLE "test_table_2" AS SELECT * FROM "test";',
            ],
            ['test_table_1', 'test_table_2'],
            'test name two',
            'out.c-test-name-two',
        ),
        # testing with single sql statement and output table mappings
        (
            ['CREATE OR REPLACE TABLE "test_table_1" AS SELECT * FROM "test";'],
            ['test_table_1'],
            'test name',
            'out.c-test-name',
        ),
    ],
)
def test_get_transformation_configuration(
    sql_statements: list[str],
    created_table_names: list[str],
    transformation_name: str,
    expected_bucket_id: str,
):
    """Test get_transformation_configuration tool which should return the correct transformation configuration
    given the sql statement created_table_names and transformation_name."""

    codes = [TransformationConfiguration.Parameters.Block.Code(name='Code 0', sql_statements=sql_statements)]
    configuration = get_transformation_configuration(
        codes=codes,
        transformation_name=transformation_name,
        output_tables=created_table_names,
    )

    assert configuration is not None
    assert isinstance(configuration, TransformationConfiguration)
    # we expect only one block and one code for the given sql statements
    assert configuration.parameters is not None
    assert len(configuration.parameters.blocks) == 1
    assert len(configuration.parameters.blocks[0].codes) == 1
    assert configuration.parameters.blocks[0].codes[0].name == 'Code 0'
    assert configuration.parameters.blocks[0].codes[0].sql_statements == sql_statements
    # given output_table_mappings, assert following tables are created
    assert configuration.storage is not None
    assert configuration.storage.input is not None
    assert configuration.storage.output is not None
    assert configuration.storage.input.tables == []
    if not created_table_names:
        assert configuration.storage.output.tables == []
    else:
        assert len(configuration.storage.output.tables) == len(created_table_names)
        for created_table, expected_table_name in zip(configuration.storage.output.tables, created_table_names):
            assert created_table.source == expected_table_name
            assert created_table.destination == f'{expected_bucket_id}.{expected_table_name}'


@pytest.mark.parametrize(
    ('input_str', 'expected_str'),
    [
        ('!@#$%^&*()+,./;\'[]"\\`', ''),
        ('a_-', 'a_-'),
        ('1234567890', '1234567890'),
        ('test_table_1', 'test_table_1'),
        ('test:-Table-1!', 'test-Table-1'),
        ('test Test', 'test-Test'),
        ('__test_test', 'test_test'),
        ('--test-test', '--test-test'),  # it is allowed
        ('+ěščřžýáíé', 'escrzyaie'),
    ],
)
def test_clean_bucket_name(input_str: str, expected_str: str):
    """Test clean_bucket_name function."""
    assert clean_bucket_name(input_str) == expected_str


@pytest.mark.parametrize(
    'input_sql_statements_name',
    [
        'sql_statements',
        'script',
    ],
)
def test_transformation_configuration_serialization(input_sql_statements_name: str):
    """Test transformation configuration serialization."""
    transformation_params_cfg = {
        'parameters': {
            'blocks': [
                {'name': 'Block 0', 'codes': [{'name': 'Code 0', input_sql_statements_name: ['SELECT * FROM test']}]}
            ]
        },
        'storage': {},
    }
    configuration = TransformationConfiguration.model_validate(transformation_params_cfg)
    assert configuration.parameters.blocks[0].codes[0].name == 'Code 0'
    assert configuration.parameters.blocks[0].codes[0].sql_statements == ['SELECT * FROM test']
    returned_params_cfg = configuration.model_dump(by_alias=True)
    assert returned_params_cfg['parameters']['blocks'][0]['codes'][0]['name'] == 'Code 0'
    # for both sql_statements and script, we expect the same result script for api request

    assert returned_params_cfg['parameters']['blocks'][0]['codes'][0]['script'] == ['SELECT * FROM test']


@pytest.mark.parametrize(
    ('params', 'update', 'expected'),
    [
        # Test 'set' operation on simple key
        (
            {'api_key': 'old_key', 'count': 42},
            ConfigParamSet(op='set', path='api_key', new_val='new_key'),
            {'api_key': 'new_key', 'count': 42},
        ),
        # Test 'set' operation on nested key
        (
            {'database': {'host': 'localhost', 'port': 5432}},
            ConfigParamSet(op='set', path='database.host', new_val='remotehost'),
            {'database': {'host': 'remotehost', 'port': 5432}},
        ),
        # Test 'set' operation on new key
        (
            {'api_key': 'old_key'},
            ConfigParamSet(op='set', path='new_key', new_val='new_value'),
            {'api_key': 'old_key', 'new_key': 'new_value'},
        ),
        # Test 'str_replace' operation on existing string
        (
            {'api_key': 'old_key_value'},
            ConfigParamReplace(op='str_replace', path='api_key', search_for='old', replace_with='new'),
            {'api_key': 'new_key_value'},
        ),
        # Test 'str_replace' operation on nested string
        (
            {'database': {'host': 'old_host_name'}},
            ConfigParamReplace(op='str_replace', path='database.host', search_for='old', replace_with='new'),
            {'database': {'host': 'new_host_name'}},
        ),
        # Test 'remove' operation on simple key
        (
            {'api_key': 'value', 'count': 42},
            ConfigParamRemove(op='remove', path='api_key'),
            {'count': 42},  # api_key should be removed
        ),
        # Test 'remove' operation on nested key
        (
            {'database': {'host': 'localhost', 'port': 5432}},
            ConfigParamRemove(op='remove', path='database.port'),
            {'database': {'host': 'localhost'}},  # port should be removed
        ),
        # Test 'remove' operation on entire object
        (
            {'database': {'host': 'localhost', 'port': 5432}, 'api_key': 'value'},
            ConfigParamRemove(op='remove', path='database'),
            {'api_key': 'value'},  # database should be removed
        ),
    ],
)
def test_apply_param_update(
    params: dict[str, Any],
    update: ConfigParamSet | ConfigParamReplace | ConfigParamRemove,
    expected: dict[str, Any],
):
    """Test _apply_param_update function with various operations."""
    result = _apply_param_update(params, update)
    assert result == expected


def test_update_params():
    """Test update_params function with multiple operations."""
    params = {
        'api_key': 'old_key',
        'database': {'host': 'localhost', 'port': 5432},
        'deprecated_field': 'old_value',
    }

    updates = [
        ConfigParamSet(op='set', path='api_key', new_val='new_key'),
        ConfigParamReplace(op='str_replace', path='database.host', search_for='localhost', replace_with='remotehost'),
        ConfigParamRemove(op='remove', path='deprecated_field'),
    ]

    expected = {
        'api_key': 'new_key',
        'database': {'host': 'remotehost', 'port': 5432},
        # deprecated_field should be removed, not set to None
    }

    result = update_params(params, updates)
    assert result == expected


def test_update_params_empty_updates():
    """Test update_params function with empty updates list."""
    params = {'api_key': 'value'}
    result = update_params(params, [])
    assert result == params


@pytest.mark.parametrize(
    ('params', 'update', 'expected_error'),
    [
        # Test 'str_replace' operation on non-existent path should raise ValueError
        (
            {'api_key': 'value'},
            ConfigParamReplace(op='str_replace', path='nonexistent.key', search_for='old', replace_with='new'),
            'Path "nonexistent.key" does not exist',
        ),
        # Test 'str_replace' operation on non-string value should raise ValueError
        (
            {'count': 42},
            ConfigParamReplace(op='str_replace', path='count', search_for='4', replace_with='5'),
            'Path "count" is not a string',
        ),
        # Test 'remove' operation on non-existent path should raise ValueError
        (
            {'api_key': 'value'},
            ConfigParamRemove(op='remove', path='nonexistent_key'),
            'Path "nonexistent_key" does not exist',
        ),
        # Test 'remove' operation on non-existent nested path should raise ValueError
        (
            {'database': {'host': 'localhost'}},
            ConfigParamRemove(op='remove', path='database.nonexistent_field'),
            'Path "database.nonexistent_field" does not exist',
        ),
        # Test 'remove' operation on completely non-existent nested path should raise ValueError
        (
            {'api_key': 'value'},
            ConfigParamRemove(op='remove', path='nonexistent.nested.path'),
            'Path "nonexistent.nested.path" does not exist',
        ),
    ],
)
def test_apply_param_update_error_cases(
    params: dict[str, Any],
    update: ConfigParamSet | ConfigParamReplace | ConfigParamRemove,
    expected_error: str,
):
    """Test _apply_param_update function with error cases that should raise ValueError."""
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        _apply_param_update(params, update)


def test_update_params_single_update():
    """Test update_params function with single update."""
    params = {'api_key': 'old_key'}
    updates = [ConfigParamSet(op='set', path='api_key', new_val='new_key')]
    expected = {'api_key': 'new_key'}

    result = update_params(params, updates)
    assert result == expected


@pytest.mark.parametrize(
    ('params', 'update', 'expected_error_match'),
    [
        # Test setting nested value through string
        (
            {'api_key': 'string_value'},
            ConfigParamSet(op='set', path='api_key.nested', new_val='new_value'),
            'Cannot set nested value at path "api_key.nested"',
        ),
        # Test setting deeply nested value through string
        (
            {'database': {'config': 'string_value'}},
            ConfigParamSet(op='set', path='database.config.host', new_val='localhost'),
            'Cannot set nested value at path "database.config.host"',
        ),
        # Test setting nested value through number
        (
            {'count': 42},
            ConfigParamSet(op='set', path='count.nested', new_val='new_value'),
            'Cannot set nested value at path "count.nested"',
        ),
        # Test setting nested value through list
        (
            {'items': [1, 2, 3]},
            ConfigParamSet(op='set', path='items.nested', new_val='new_value'),
            'Cannot set nested value at path "items.nested"',
        ),
        # Test setting nested value through boolean
        (
            {'flag': True},
            ConfigParamSet(op='set', path='flag.nested', new_val='new_value'),
            'Cannot set nested value at path "flag.nested"',
        ),
    ],
)
def test_apply_param_update_set_nested_through_non_dict_errors(
    params: dict[str, Any],
    update: ConfigParamSet,
    expected_error_match: str,
):
    """Test _apply_param_update raises error when trying to set nested value through non-dict."""
    with pytest.raises(ValueError, match=expected_error_match):
        _apply_param_update(params, update)


@pytest.mark.parametrize(
    ('data', 'path', 'value', 'expected_error_match', 'expected_type'),
    [
        # Test setting through string
        (
            {'api_key': 'string_value'},
            'api_key.nested',
            'new_value',
            'Cannot set nested value at path "api_key.nested"',
            'str',
        ),
        # Test setting through number
        (
            {'count': 42},
            'count.nested',
            'new_value',
            'Cannot set nested value at path "count.nested"',
            'int',
        ),
        # Test setting through list
        (
            {'items': [1, 2, 3]},
            'items.nested',
            'new_value',
            'Cannot set nested value at path "items.nested"',
            'list',
        ),
        # Test setting through boolean
        (
            {'flag': True},
            'flag.nested',
            'new_value',
            'Cannot set nested value at path "flag.nested"',
            'bool',
        ),
        # Test setting through None
        (
            {'value': None},
            'value.nested',
            'new_value',
            'Cannot set nested value at path "value.nested"',
            'NoneType',
        ),
        # Test deeply nested path with non-dict in middle
        (
            {'database': {'config': 'string_value'}},
            'database.config.host.port',
            5432,
            'Cannot set nested value at path "database.config.host.port"',
            'str',
        ),
    ],
)
def test_set_nested_value_through_non_dict_errors(
    data: dict[str, Any],
    path: str,
    value: Any,
    expected_error_match: str,
    expected_type: str,
):
    """Test _set_nested_value raises error when encountering non-dict in path."""
    from keboola_mcp_server.tools.components.utils import _set_nested_value

    with pytest.raises(ValueError, match=expected_error_match):
        _set_nested_value(data, path, value)

    # Also verify the error message contains the type information
    with pytest.raises(ValueError, match=f'type: {expected_type}'):
        _set_nested_value(data, path, value)
