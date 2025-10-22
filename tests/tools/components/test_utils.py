import re
from typing import Any, Sequence

import pytest

from keboola_mcp_server.tools.components.model import (
    ALL_COMPONENT_TYPES,
    ComponentType,
    ConfigParamListAppend,
    ConfigParamRemove,
    ConfigParamReplace,
    ConfigParamSet,
    ConfigParamUpdate,
    TransformationConfiguration,
)
from keboola_mcp_server.tools.components.utils import (
    _apply_param_update,
    _set_nested_value,
    clean_bucket_name,
    expand_component_types,
    get_transformation_configuration,
    update_params,
)


@pytest.mark.parametrize(
    ('component_type', 'expected'),
    [
        (['extractor', 'writer'], ('extractor', 'writer')),
        (['writer', 'extractor', 'writer', 'extractor'], ('extractor', 'writer')),
        ([], ALL_COMPONENT_TYPES),
        (None, ALL_COMPONENT_TYPES),
    ],
)
def test_expand_component_types(
    component_type: Sequence[ComponentType],
    expected: list[ComponentType],
):
    """Test list_component_configurations tool with core component."""
    assert expand_component_types(component_type) == expected


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
        # Test 'set' operation creating deeply nested path
        (
            {'api_key': 'value'},
            ConfigParamSet(op='set', path='config.database.connection.host', new_val='localhost'),
            {'api_key': 'value', 'config': {'database': {'connection': {'host': 'localhost'}}}},
        ),
        # Test 'set' operation with different value types - list
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.items', new_val=[1, 2, 3]),
            {'config': {'items': [1, 2, 3]}},
        ),
        # Test 'set' operation with different value types - boolean
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.enabled', new_val=True),
            {'config': {'enabled': True}},
        ),
        # Test 'set' operation with different value types - None
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.value', new_val=None),
            {'config': {'value': None}},
        ),
        # Test 'set' operation with different value types - number
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.timeout', new_val=300),
            {'config': {'timeout': 300}},
        ),
        # Test 'set' operation with multiple JSONPath matches
        (
            {'messages': [{'text': 'old1'}, {'text': 'old2 old3'}]},
            ConfigParamSet(op='set', path='messages[*].text', new_val='new'),
            {'messages': [{'text': 'new'}, {'text': 'new'}]},
        ),
        # Test 'set' operation with '$' (root) JSONPath
        (
            {'messages': [{'text': 'old1'}, {'text': 'old2 old3'}]},
            ConfigParamSet(op='set', path='$', new_val={'object': 'new'}),
            {'object': 'new'},
        ),
        # Test 'str_replace' operation on existing string
        (
            {'api_key': 'old_key_value'},
            ConfigParamReplace(op='str_replace', path='api_key', search_for='old', replace_with='new'),
            {'api_key': 'new_key_value'},
        ),
        # Test 'str_replace' operation with empty replace string
        (
            {'api_key': 'old_key_value'},
            ConfigParamReplace(op='str_replace', path='api_key', search_for='old_', replace_with=''),
            {'api_key': 'key_value'},
        ),
        # Test 'str_replace' operation on nested string
        (
            {'database': {'host': 'old_host_name'}},
            ConfigParamReplace(op='str_replace', path='database.host', search_for='old', replace_with='new'),
            {'database': {'host': 'new_host_name'}},
        ),
        # Test 'str_replace' with multiple occurrences
        (
            {'message': 'old old old'},
            ConfigParamReplace(op='str_replace', path='message', search_for='old', replace_with='new'),
            {'message': 'new new new'},
        ),
        # Test 'str_replace' with multiple JSONPath matches
        (
            {'messages': ['old1', 'old2 old3']},
            ConfigParamReplace(op='str_replace', path='messages[*]', search_for='old', replace_with='new'),
            {'messages': ['new1', 'new2 new3']},
        ),
        # Test 'remove' operation on simple key
        (
            {'api_key': 'value', 'count': 42},
            ConfigParamRemove(op='remove', path='api_key'),
            {'count': 42},
        ),
        # Test 'remove' operation on nested key
        (
            {'database': {'host': 'localhost', 'port': 5432}},
            ConfigParamRemove(op='remove', path='database.port'),
            {'database': {'host': 'localhost'}},
        ),
        # Test 'remove' operation on entire object
        (
            {'database': {'host': 'localhost', 'port': 5432}, 'api_key': 'value'},
            ConfigParamRemove(op='remove', path='database'),
            {'api_key': 'value'},
        ),
        # Test 'remove' operation with multiple JSONPath matches
        (
            {'messages': [{'text': 'old1'}, {'text': 'old2 old3', 'metadata': {'id': 1}}]},
            ConfigParamRemove(op='remove', path='messages[*].text'),
            {'messages': [{}, {'metadata': {'id': 1}}]},
        ),
        # Test 'remove' operation with '$' JSONPath - it doesn't do anything
        (
            {'messages': [{'text': 'old1'}, {'text': 'old2 old3'}]},
            ConfigParamRemove(op='remove', path='$'),
            {'messages': [{'text': 'old1'}, {'text': 'old2 old3'}]},
        ),
        # Test 'list_append' operation on simple list
        (
            {'items': [1, 2, 3]},
            ConfigParamListAppend(op='list_append', path='items', value=4),
            {'items': [1, 2, 3, 4]},
        ),
        # Test 'list_append' operation on nested list
        (
            {'config': {'values': ['a', 'b']}},
            ConfigParamListAppend(op='list_append', path='config.values', value='c'),
            {'config': {'values': ['a', 'b', 'c']}},
        ),
        # Test 'list_append' operation on deeply nested list (like SQL transformation structure)
        (
            {'blocks': [{'codes': [{'script': ['SELECT 1']}]}]},
            ConfigParamListAppend(op='list_append', path='blocks[0].codes[0].script', value='SELECT 2'),
            {'blocks': [{'codes': [{'script': ['SELECT 1', 'SELECT 2']}]}]},
        ),
        # Test 'list_append' operation with multiple JSONPath matches
        (
            {'messages': [{'items': [1]}, {'items': [2]}]},
            ConfigParamListAppend(op='list_append', path='messages[*].items', value=99),
            {'messages': [{'items': [1, 99]}, {'items': [2, 99]}]},
        ),
        # Test 'list_append' operation with different value types - dict
        (
            {'config': {'entries': [{'id': 1}]}},
            ConfigParamListAppend(op='list_append', path='config.entries', value={'id': 2}),
            {'config': {'entries': [{'id': 1}, {'id': 2}]}},
        ),
    ],
)
def test_apply_param_update(
    params: dict[str, Any],
    update: ConfigParamUpdate,
    expected: dict[str, Any],
):
    """Test _apply_param_update function with valid operations."""
    result = _apply_param_update(params, update)
    assert result == expected


@pytest.mark.parametrize(
    ('params', 'update', 'expected_error'),
    [
        # Test 'str_replace' operation on non-existent path
        (
            {'api_key': 'value'},
            ConfigParamReplace(op='str_replace', path='nonexistent.key', search_for='old', replace_with='new'),
            'Path "nonexistent.key" does not exist',
        ),
        # Test 'str_replace' operation on non-string value
        (
            {'count': 42},
            ConfigParamReplace(op='str_replace', path='count', search_for='4', replace_with='5'),
            'Path "count" is not a string',
        ),
        # Test 'str_replace' when search string is empty
        (
            {'api_key': 'my_secret_key'},
            ConfigParamReplace(op='str_replace', path='api_key', search_for='', replace_with='a'),
            'Search string is empty',
        ),
        # Test 'str_replace' when search string not found
        (
            {'api_key': 'my_secret_key'},
            ConfigParamReplace(op='str_replace', path='api_key', search_for='notfound', replace_with='new'),
            'Search string "notfound" not found in path "api_key"',
        ),
        # Test 'str_replace' when search string and replace string are the same
        (
            {'api_key': 'my_secret_key'},
            ConfigParamReplace(op='str_replace', path='api_key', search_for='a', replace_with='a'),
            'Search string and replace string are the same: "a"',
        ),
        # Test 'remove' operation on non-existent path
        (
            {'api_key': 'value'},
            ConfigParamRemove(op='remove', path='nonexistent_key'),
            'Path "nonexistent_key" does not exist',
        ),
        # Test 'remove' operation on non-existent nested path
        (
            {'database': {'host': 'localhost'}},
            ConfigParamRemove(op='remove', path='database.nonexistent_field'),
            'Path "database.nonexistent_field" does not exist',
        ),
        # Test 'remove' operation on completely non-existent nested path
        (
            {'api_key': 'value'},
            ConfigParamRemove(op='remove', path='nonexistent.nested.path'),
            'Path "nonexistent.nested.path" does not exist',
        ),
        # Test 'set' operation on nested value through string
        (
            {'api_key': 'string_value'},
            ConfigParamSet(op='set', path='api_key.nested', new_val='new_value'),
            'Cannot set nested value at path "api_key.nested"',
        ),
        # Test 'set' operation on deeply nested value through string
        (
            {'database': {'config': 'string_value'}},
            ConfigParamSet(op='set', path='database.config.host', new_val='localhost'),
            'Cannot set nested value at path "database.config.host"',
        ),
        # Test 'set' operation on nested value through number
        (
            {'count': 42},
            ConfigParamSet(op='set', path='count.nested', new_val='new_value'),
            'Cannot set nested value at path "count.nested"',
        ),
        # Test 'set' operation on nested value through list
        (
            {'items': [1, 2, 3]},
            ConfigParamSet(op='set', path='items.nested', new_val='new_value'),
            'Cannot set nested value at path "items.nested"',
        ),
        # Test 'set' operation on nested value through boolean
        (
            {'flag': True},
            ConfigParamSet(op='set', path='flag.nested', new_val='new_value'),
            'Cannot set nested value at path "flag.nested"',
        ),
        # Test 'list_append' operation on non-existent path
        (
            {'items': [1, 2, 3]},
            ConfigParamListAppend(op='list_append', path='nonexistent_list', value=4),
            'Path "nonexistent_list" does not exist',
        ),
        # Test 'list_append' operation on non-existent nested path
        (
            {'config': {'values': [1, 2]}},
            ConfigParamListAppend(op='list_append', path='config.nonexistent', value=3),
            'Path "config.nonexistent" does not exist',
        ),
        # Test 'list_append' operation on non-list value (string)
        (
            {'api_key': 'my_value'},
            ConfigParamListAppend(op='list_append', path='api_key', value='extra'),
            'Path "api_key" is not a list',
        ),
        # Test 'list_append' operation on non-list value (dict)
        (
            {'config': {'host': 'localhost'}},
            ConfigParamListAppend(op='list_append', path='config', value='item'),
            'Path "config" is not a list',
        ),
        # Test 'list_append' operation on non-list value (number)
        (
            {'count': 42},
            ConfigParamListAppend(op='list_append', path='count', value=1),
            'Path "count" is not a list',
        ),
    ],
)
def test_apply_param_update_errors(
    params: dict[str, Any],
    update: ConfigParamUpdate,
    expected_error: str,
):
    """Test _apply_param_update function with error cases."""
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        _apply_param_update(params, update)


@pytest.mark.parametrize(
    ('params', 'updates', 'expected'),
    [
        # Test with multiple operations
        (
            {
                'api_key': 'old_key',
                'database': {'host': 'localhost', 'port': 5432},
                'deprecated_field': 'old_value',
            },
            [
                ConfigParamSet(op='set', path='api_key', new_val='new_key'),
                ConfigParamReplace(
                    op='str_replace', path='database.host', search_for='localhost', replace_with='remotehost'
                ),
                ConfigParamRemove(op='remove', path='deprecated_field'),
            ],
            {
                'api_key': 'new_key',
                'database': {'host': 'remotehost', 'port': 5432},
            },
        ),
        # Test with single update
        (
            {'api_key': 'old_key'},
            [ConfigParamSet(op='set', path='api_key', new_val='new_key')],
            {'api_key': 'new_key'},
        ),
        # Test with empty updates list
        (
            {'api_key': 'value'},
            [],
            {'api_key': 'value'},
        ),
        # Test sequential dependency - set then modify
        (
            {'config': {}},
            [
                ConfigParamSet(op='set', path='config.url', new_val='http://old.example.com'),
                ConfigParamReplace(op='str_replace', path='config.url', search_for='old', replace_with='new'),
            ],
            {'config': {'url': 'http://new.example.com'}},
        ),
        # Test sequential dependency - set, modify, then set another dependent value
        (
            {},
            [
                ConfigParamSet(op='set', path='database.host', new_val='localhost'),
                ConfigParamSet(op='set', path='database.port', new_val=5432),
                ConfigParamSet(op='set', path='database.ssl', new_val=True),
            ],
            {'database': {'host': 'localhost', 'port': 5432, 'ssl': True}},
        ),
        # Test order matters - set, replace, then set again
        (
            {'value': 'initial'},
            [
                ConfigParamReplace(op='str_replace', path='value', search_for='initial', replace_with='modified'),
                ConfigParamSet(op='set', path='value', new_val='final'),
            ],
            {'value': 'final'},
        ),
    ],
)
def test_update_params(
    params: dict[str, Any],
    updates: Sequence[ConfigParamUpdate],
    expected: dict[str, Any],
):
    """Test update_params function with valid operations."""
    result = update_params(params, updates)
    assert result == expected


def test_update_params_does_not_mutate_original_dict():
    """Test that update_params does NOT mutate the original params dict."""
    params = {'api_key': 'old_key', 'count': 42}
    updates = [
        ConfigParamSet(op='set', path='api_key', new_val='new_key'),
        ConfigParamSet(op='set', path='count', new_val=100),
    ]

    result = update_params(params, updates)

    # The function returns a new dict with updates
    assert result == {'api_key': 'new_key', 'count': 100}
    # The original dict is unchanged
    assert params == {'api_key': 'old_key', 'count': 42}
    # They are different objects
    assert result is not params


def test_update_params_with_error_in_middle():
    """Test that update_params raises error if any update fails, and original dict is unchanged."""
    params = {'api_key': 'value', 'count': 42}
    original_params = params.copy()
    updates = [
        ConfigParamSet(op='set', path='api_key', new_val='new_key'),
        ConfigParamRemove(op='remove', path='nonexistent_field'),  # This will fail
        ConfigParamSet(op='set', path='count', new_val=100),  # This won't be reached
    ]

    with pytest.raises(ValueError, match='Path "nonexistent_field" does not exist'):
        update_params(params, updates)

    # Original dict is completely unchanged (no mutations)
    assert params == original_params
    assert params == {'api_key': 'value', 'count': 42}


@pytest.mark.parametrize(
    ('data', 'path', 'value', 'expected_error'),
    [
        # Test setting through string
        (
            {'api_key': 'string_value'},
            'api_key.nested',
            'new_value',
            'Cannot set nested value at path "api_key.nested": encountered non-dict value at "api_key" (type: str)',
        ),
        # Test setting through number
        (
            {'count': 42},
            'count.nested',
            'new_value',
            'Cannot set nested value at path "count.nested": encountered non-dict value at "count" (type: int)',
        ),
        # Test setting through list
        (
            {'items': [1, 2, 3]},
            'items.nested',
            'new_value',
            'Cannot set nested value at path "items.nested": encountered non-dict value at "items" (type: list)',
        ),
        # Test setting through boolean
        (
            {'flag': True},
            'flag.nested',
            'new_value',
            'Cannot set nested value at path "flag.nested": encountered non-dict value at "flag" (type: bool)',
        ),
        # Test setting through None
        (
            {'value': None},
            'value.nested',
            'new_value',
            'Cannot set nested value at path "value.nested": encountered non-dict value at "value" (type: NoneType)',
        ),
        # Test deeply nested path with non-dict in middle
        (
            {'database': {'config': 'string_value'}},
            'database.config.host.port',
            5432,
            (
                'Cannot set nested value at path "database.config.host.port": '
                'encountered non-dict value at "database.config" (type: str)'
            ),
        ),
    ],
)
def test_set_nested_value_through_non_dict_errors(
    data: dict[str, Any],
    path: str,
    value: Any,
    expected_error: str,
):
    """Test _set_nested_value raises error when encountering non-dict in path."""
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        _set_nested_value(data, path, value)
