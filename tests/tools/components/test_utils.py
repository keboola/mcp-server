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
    SimplifiedTfBlocks,
    TfAddBlock,
    TfAddCode,
    TfParamUpdate,
    TfRemoveCode,
    TfRenameBlock,
    TfRenameCode,
    TfSetCode,
    TfStrReplace,
    TransformationConfiguration,
)
from keboola_mcp_server.tools.components.utils import (
    _apply_param_update,
    clean_bucket_name,
    create_transformation_configuration,
    expand_component_types,
    set_nested_value,
    structure_summary,
    update_params,
    update_transformation_parameters,
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
    ('codes', 'transformation_name', 'output_tables', 'expected'),
    [
        # testing with multiple sql statements and no output table mappings
        # it should not create any output tables
        (
            [SimplifiedTfBlocks.Block.Code(name='Code 0', script='SELECT * FROM test;\nSELECT * FROM test2;')],
            'test name',
            [],
            TransformationConfiguration(
                parameters=TransformationConfiguration.Parameters(
                    blocks=[
                        TransformationConfiguration.Parameters.Block(
                            name='Blocks',
                            codes=[
                                TransformationConfiguration.Parameters.Block.Code(
                                    name='Code 0',
                                    script=['SELECT\n  *\nFROM test;', 'SELECT\n  *\nFROM test2;'],
                                )
                            ],
                        )
                    ]
                ),
                storage=TransformationConfiguration.Storage(
                    input=TransformationConfiguration.Storage.Destination(tables=[]),
                    output=TransformationConfiguration.Storage.Destination(tables=[]),
                ),
            ),
        ),
        # testing with multiple sql statements and output table mappings
        # it should create output tables according to the mappings
        (
            [
                SimplifiedTfBlocks.Block.Code(
                    name='Code 0',
                    script=(
                        'CREATE OR REPLACE TABLE "test_table_1" AS SELECT * FROM "test";\n'
                        '-- comment\n'
                        'CREATE OR REPLACE TABLE "test_table_2" AS SELECT * FROM "test";'
                    ),
                )
            ],
            'test name two',
            ['test_table_1', 'test_table_2'],
            TransformationConfiguration(
                parameters=TransformationConfiguration.Parameters(
                    blocks=[
                        TransformationConfiguration.Parameters.Block(
                            name='Blocks',
                            codes=[
                                TransformationConfiguration.Parameters.Block.Code(
                                    name='Code 0',
                                    script=[
                                        'CREATE OR REPLACE TABLE "test_table_1" AS\nSELECT\n  *\nFROM "test";',
                                        '/* comment */\nCREATE OR REPLACE TABLE "test_table_2" AS\nSELECT\n  *\n'
                                        'FROM "test";',
                                    ],
                                )
                            ],
                        )
                    ]
                ),
                storage=TransformationConfiguration.Storage(
                    input=TransformationConfiguration.Storage.Destination(tables=[]),
                    output=TransformationConfiguration.Storage.Destination(
                        tables=[
                            TransformationConfiguration.Storage.Destination.Table(
                                source='test_table_1',
                                destination='out.c-test-name-two.test_table_1',
                            ),
                            TransformationConfiguration.Storage.Destination.Table(
                                source='test_table_2',
                                destination='out.c-test-name-two.test_table_2',
                            ),
                        ]
                    ),
                ),
            ),
        ),
        # testing with single sql statement and output table mappings
        (
            [
                SimplifiedTfBlocks.Block.Code(
                    name='Code 0',
                    script='CREATE OR REPLACE TABLE "test_table_1" AS SELECT * FROM "test";',
                )
            ],
            'test name',
            ['test_table_1'],
            TransformationConfiguration(
                parameters=TransformationConfiguration.Parameters(
                    blocks=[
                        TransformationConfiguration.Parameters.Block(
                            name='Blocks',
                            codes=[
                                TransformationConfiguration.Parameters.Block.Code(
                                    name='Code 0',
                                    script=['CREATE OR REPLACE TABLE "test_table_1" AS\nSELECT\n  *\nFROM "test";'],
                                )
                            ],
                        )
                    ]
                ),
                storage=TransformationConfiguration.Storage(
                    input=TransformationConfiguration.Storage.Destination(tables=[]),
                    output=TransformationConfiguration.Storage.Destination(
                        tables=[
                            TransformationConfiguration.Storage.Destination.Table(
                                source='test_table_1',
                                destination='out.c-test-name.test_table_1',
                            ),
                        ]
                    ),
                ),
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_create_transformation_configuration(
    codes: list[SimplifiedTfBlocks.Block.Code],
    transformation_name: str,
    output_tables: list[str],
    expected: TransformationConfiguration,
):
    """Test create_transformation_configuration function which should return the correct transformation configuration
    given the codes, transformation_name and output_tables."""

    configuration = await create_transformation_configuration(
        codes=codes,
        transformation_name=transformation_name,
        output_tables=output_tables,
        sql_dialect='snowflake',
    )

    assert configuration == expected


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
    ('params', 'update', 'expected'),
    [
        # Test 'set' operation on simple key
        (
            {'api_key': 'old_key', 'count': 42},
            ConfigParamSet(op='set', path='api_key', value='new_key'),
            {'api_key': 'new_key', 'count': 42},
        ),
        # Test 'set' operation on nested key
        (
            {'database': {'host': 'localhost', 'port': 5432}},
            ConfigParamSet(op='set', path='database.host', value='remotehost'),
            {'database': {'host': 'remotehost', 'port': 5432}},
        ),
        # Test 'set' operation on new key
        (
            {'api_key': 'old_key'},
            ConfigParamSet(op='set', path='new_key', value='new_value'),
            {'api_key': 'old_key', 'new_key': 'new_value'},
        ),
        # Test 'set' operation creating deeply nested path
        (
            {'api_key': 'value'},
            ConfigParamSet(op='set', path='config.database.connection.host', value='localhost'),
            {'api_key': 'value', 'config': {'database': {'connection': {'host': 'localhost'}}}},
        ),
        # Test 'set' operation with different value types - list
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.items', value=[1, 2, 3]),
            {'config': {'items': [1, 2, 3]}},
        ),
        # Test 'set' operation with different value types - boolean
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.enabled', value=True),
            {'config': {'enabled': True}},
        ),
        # Test 'set' operation with different value types - None
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.value', value=None),
            {'config': {'value': None}},
        ),
        # Test 'set' operation with different value types - number
        (
            {'config': {}},
            ConfigParamSet(op='set', path='config.timeout', value=300),
            {'config': {'timeout': 300}},
        ),
        # Test 'set' operation with multiple JSONPath matches
        (
            {'messages': [{'text': 'old1'}, {'text': 'old2 old3'}]},
            ConfigParamSet(op='set', path='messages[*].text', value='new'),
            {'messages': [{'text': 'new'}, {'text': 'new'}]},
        ),
        # Test 'set' operation with '$' (root) JSONPath
        (
            {'messages': [{'text': 'old1'}, {'text': 'old2 old3'}]},
            ConfigParamSet(op='set', path='$', value={'object': 'new'}),
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
            ConfigParamSet(op='set', path='api_key.nested', value='new_value'),
            'Cannot set nested value at path "api_key.nested"',
        ),
        # Test 'set' operation on deeply nested value through string
        (
            {'database': {'config': 'string_value'}},
            ConfigParamSet(op='set', path='database.config.host', value='localhost'),
            'Cannot set nested value at path "database.config.host"',
        ),
        # Test 'set' operation on nested value through number
        (
            {'count': 42},
            ConfigParamSet(op='set', path='count.nested', value='new_value'),
            'Cannot set nested value at path "count.nested"',
        ),
        # Test 'set' operation on nested value through list
        (
            {'items': [1, 2, 3]},
            ConfigParamSet(op='set', path='items.nested', value='new_value'),
            'Cannot set nested value at path "items.nested"',
        ),
        # Test 'set' operation on nested value through boolean
        (
            {'flag': True},
            ConfigParamSet(op='set', path='flag.nested', value='new_value'),
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
                ConfigParamSet(op='set', path='api_key', value='new_key'),
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
            [ConfigParamSet(op='set', path='api_key', value='new_key')],
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
                ConfigParamSet(op='set', path='config.url', value='http://old.example.com'),
                ConfigParamReplace(op='str_replace', path='config.url', search_for='old', replace_with='new'),
            ],
            {'config': {'url': 'http://new.example.com'}},
        ),
        # Test sequential dependency - set, modify, then set another dependent value
        (
            {},
            [
                ConfigParamSet(op='set', path='database.host', value='localhost'),
                ConfigParamSet(op='set', path='database.port', value=5432),
                ConfigParamSet(op='set', path='database.ssl', value=True),
            ],
            {'database': {'host': 'localhost', 'port': 5432, 'ssl': True}},
        ),
        # Test order matters - set, replace, then set again
        (
            {'value': 'initial'},
            [
                ConfigParamReplace(op='str_replace', path='value', search_for='initial', replace_with='modified'),
                ConfigParamSet(op='set', path='value', value='final'),
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
        ConfigParamSet(op='set', path='api_key', value='new_key'),
        ConfigParamSet(op='set', path='count', value=100),
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
        ConfigParamSet(op='set', path='api_key', value='new_key'),
        ConfigParamRemove(op='remove', path='nonexistent_field'),  # This will fail
        ConfigParamSet(op='set', path='count', value=100),  # This won't be reached
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
        set_nested_value(data, path, value)


@pytest.mark.parametrize(
    ('parameters', 'expected_markdown'),
    [
        # Test with single block and single code
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Main Block',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Select Data',
                                'script': "SELECT * FROM customers WHERE status = 'active';",
                            }
                        ],
                    }
                ]
            },
            (
                '## Updated Transformation Structure\n'
                '\n'
                '### Block id: `b0`, name: `Main Block`\n'
                '\n'
                '- **Code id: `b0.c0`, name: `Select Data`** SQL snippet:\n'
                '\n'
                '  ```sql\n'
                "  SELECT * FROM customers WHERE status = 'active';\n"
                '  ```\n'
            ),
        ),
        # Test with multiple blocks and codes
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Data Extraction',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Extract Customers',
                                'script': 'SELECT id, name, email FROM customers;',
                            },
                            {
                                'id': 'b0.c1',
                                'name': 'Extract Orders',
                                'script': 'SELECT order_id, customer_id, amount FROM orders;',
                            },
                        ],
                    },
                    {
                        'id': 'b1',
                        'name': 'Data Transformation',
                        'codes': [
                            {
                                'id': 'b1.c0',
                                'name': 'Aggregate Data',
                                'script': 'SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id;',
                            }
                        ],
                    },
                ]
            },
            (
                '## Updated Transformation Structure\n'
                '\n'
                '### Block id: `b0`, name: `Data Extraction`\n'
                '\n'
                '- **Code id: `b0.c0`, name: `Extract Customers`** SQL snippet:\n'
                '\n'
                '  ```sql\n'
                '  SELECT id, name, email FROM customers;\n'
                '  ```\n'
                '\n'
                '- **Code id: `b0.c1`, name: `Extract Orders`** SQL snippet:\n'
                '\n'
                '  ```sql\n'
                '  SELECT order_id, customer_id, amount FROM orders;\n'
                '  ```\n'
                '\n'
                '### Block id: `b1`, name: `Data Transformation`\n'
                '\n'
                '- **Code id: `b1.c0`, name: `Aggregate Data`** SQL snippet:\n'
                '\n'
                '  ```sql\n'
                '  SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id;\n'
                '  ```\n'
            ),
        ),
        # Test with multiline SQL script
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Complex Query',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Multi-line Select',
                                'script': (
                                    'SELECT\n'
                                    '  customer_id,\n'
                                    '  SUM(amount) as total,\n'
                                    '  COUNT(*) as order_count\n'
                                    'FROM orders\n'
                                    "WHERE status = 'completed'\n"
                                    'GROUP BY customer_id;'
                                ),
                            }
                        ],
                    }
                ]
            },
            (
                '## Updated Transformation Structure\n'
                '\n'
                '### Block id: `b0`, name: `Complex Query`\n'
                '\n'
                '- **Code id: `b0.c0`, name: `Multi-line Select`** SQL snippet:\n'
                '\n'
                '  ```sql\n'
                '  SELECT\n  customer_id,\n  SUM(amount) as total,\n  COUNT(*) as order_count\n'
                "FROM orders\nWHERE status = 'completed'\nGROUP BY customer_id;\n"
                '  ```\n'
            ),
        ),
        # Test with empty script
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Empty Block',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Empty Code',
                                'script': '',
                            }
                        ],
                    }
                ]
            },
            (
                '## Updated Transformation Structure\n'
                '\n'
                '### Block id: `b0`, name: `Empty Block`\n'
                '\n'
                '- **Code id: `b0.c0`, name: `Empty Code`** SQL snippet:\n'
                '\n'
                '  *Empty script*\n'
            ),
        ),
        # Test with block containing no codes
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block Without Codes',
                        'codes': [],
                    }
                ]
            },
            (
                '## Updated Transformation Structure\n'
                '\n'
                '### Block id: `b0`, name: `Block Without Codes`\n'
                '\n'
                '*No code blocks*\n'
            ),
        ),
        # Test with empty blocks list
        (
            {'blocks': []},
            '## Updated Transformation Structure\n\nNo blocks found in transformation.\n',
        ),
        # Test with very long script (truncation)
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Long Script Block',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Very Long Query',
                                'script': (
                                    'SELECT column1, column2, column3, column4, column5, column6, '
                                    'column7, column8, column9, column10, column11, column12, '
                                    'column13, column14, column15, column16 FROM very_large_table '
                                    'WHERE condition1 = true;'
                                ),
                            }
                        ],
                    }
                ]
            },
            (
                '## Updated Transformation Structure\n'
                '\n'
                '### Block id: `b0`, name: `Long Script Block`\n'
                '\n'
                '- **Code id: `b0.c0`, name: `Very Long Query`** SQL snippet:\n'
                '\n'
                '  ```sql\n'
                '  SELECT column1, column2, column3, column4, column5, column6, column7, '
                'column8, column9, column10, column11, column12, column13, column14, column15, '
                'co... (53 chars truncated)\n'
                '  ```\n'
            ),
        ),
    ],
)
def test_structure_summary(parameters: dict[str, Any], expected_markdown: str):
    """Test structure_summary function generates correct markdown output."""
    result = structure_summary(parameters)
    assert result == expected_markdown


@pytest.mark.parametrize(
    ('initial_params', 'updates', 'expected_params', 'expected_msg'),
    [
        # String replacement without structure change - should only report replacement
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                            SimplifiedTfBlocks.Block.Code(name='Code Y', script='SELECT * FROM table2'),
                        ],
                    ),
                ]
            ),
            [
                TfStrReplace(op='str_replace', block_id=None, code_id=None, search_for='FROM', replace_with='IN'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * IN table1'),
                            SimplifiedTfBlocks.Block.Code(name='Code Y', script='SELECT * IN table2'),
                        ],
                    ),
                ]
            ),
            'Replaced 2 occurrences of "FROM" in the transformation',
        ),
        # Structural change without string replacement - should only report structure
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfAddBlock(
                    op='add_block',
                    block=SimplifiedTfBlocks.Block(name='New Block', codes=[]),
                    position='end',
                ),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                    SimplifiedTfBlocks.Block(name='New Block', codes=[]),
                ]
            ),
            'Added block with name "New Block"\n## Updated Transformation Structure',
        ),
        # Non-structural operations - should return empty message
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfRenameBlock(op='rename_block', block_id='b0', block_name='Renamed Block'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Renamed Block',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            '',
        ),
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfRenameCode(op='rename_code', block_id='b0', code_id='b0.c0', code_name='Renamed Code'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Renamed Code', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            '',
        ),
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfSetCode(op='set_code', block_id='b0', code_id='b0.c0', script='SELECT * FROM new_table'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT\n  *\nFROM new_table;'),
                        ],
                    ),
                ]
            ),
            "Changed code with id 'b0.c0' in block 'b0' (code was automatically reformatted)",
        ),
        # Multiple non-structural operations - should return message from set_code
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfRenameBlock(op='rename_block', block_id='b0', block_name='Renamed Block'),
                TfSetCode(op='set_code', block_id='b0', code_id='b0.c0', script='SELECT * FROM new_table'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Renamed Block',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT\n  *\nFROM new_table;'),
                        ],
                    ),
                ]
            ),
            "Changed code with id 'b0.c0' in block 'b0' (code was automatically reformatted)",
        ),
        # Structural change + string replacement - should report both
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfAddBlock(
                    op='add_block',
                    block=SimplifiedTfBlocks.Block(
                        name='New Block',
                        codes=[SimplifiedTfBlocks.Block.Code(name='New Code', script='SELECT * FROM table2')],
                    ),
                    position='end',
                ),
                TfStrReplace(op='str_replace', block_id=None, code_id=None, search_for='FROM', replace_with='IN'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * IN table1'),
                        ],
                    ),
                    SimplifiedTfBlocks.Block(
                        name='New Block',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='New Code', script='SELECT\n  *\nIN table2;'),
                        ],
                    ),
                ]
            ),
            (
                'Added block with name "New Block" (code was automatically reformatted)\n'
                'Replaced 2 occurrences of "FROM" in the transformation\n## Updated Transformation Structure'
            ),
        ),
        # Multiple string replacements - should report all
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                            SimplifiedTfBlocks.Block.Code(name='Code Y', script='SELECT * FROM table2'),
                        ],
                    ),
                ]
            ),
            [
                TfStrReplace(
                    op='str_replace', block_id='b0', code_id='b0.c0', search_for='table1', replace_with='new_table1'
                ),
                TfStrReplace(
                    op='str_replace', block_id='b0', code_id='b0.c1', search_for='table2', replace_with='new_table2'
                ),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM new_table1'),
                            SimplifiedTfBlocks.Block.Code(name='Code Y', script='SELECT * FROM new_table2'),
                        ],
                    ),
                ]
            ),
            (
                'Replaced 1 occurrence of "table1" in code "b0.c0", block "b0"\n'
                'Replaced 1 occurrence of "table2" in code "b0.c1", block "b0"'
            ),
        ),
        # Add code (structural) + string replacement - should report both
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfAddCode(
                    op='add_code',
                    block_id='b0',
                    code=SimplifiedTfBlocks.Block.Code(name='New Code', script='SELECT * FROM table2'),
                    position='end',
                ),
                TfStrReplace(op='str_replace', block_id=None, code_id=None, search_for='FROM', replace_with='IN'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * IN table1'),
                            SimplifiedTfBlocks.Block.Code(name='New Code', script='SELECT\n  *\nIN table2;'),
                        ],
                    ),
                ]
            ),
            'Added code with name "New Code" (code was automatically reformatted)\nReplaced 2 occurrences of "FROM" in '
            'the transformation\n## Updated Transformation Structure',
        ),
        # Remove code (structural) - should report structure
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                            SimplifiedTfBlocks.Block.Code(name='Code Y', script='SELECT * FROM table2'),
                        ],
                    ),
                ]
            ),
            [
                TfRemoveCode(op='remove_code', block_id='b0', code_id='b0.c0'),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code Y', script='SELECT * FROM table2'),
                        ],
                    ),
                ]
            ),
            '## Updated Transformation Structure',
        ),
        # Multiple structural changes - should report structure once
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [
                TfAddBlock(
                    op='add_block',
                    block=SimplifiedTfBlocks.Block(name='New Block', codes=[]),
                    position='end',
                ),
                TfAddCode(
                    op='add_code',
                    block_id='b0',
                    code=SimplifiedTfBlocks.Block.Code(name='New Code', script='SELECT 1'),
                    position='end',
                ),
            ],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                            SimplifiedTfBlocks.Block.Code(name='New Code', script='SELECT\n  1;'),
                        ],
                    ),
                    SimplifiedTfBlocks.Block(name='New Block', codes=[]),
                ]
            ),
            (
                'Added block with name "New Block"\n'
                'Added code with name "New Code" (code was automatically reformatted)\n'
                '## Updated Transformation Structure'
            ),
        ),
        # Empty updates list - should return empty message
        (
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            [],
            SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name='Block A',
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name='Code X', script='SELECT * FROM table1'),
                        ],
                    ),
                ]
            ),
            '',
        ),
    ],
)
def test_update_transformation_parameters(
    initial_params: SimplifiedTfBlocks,
    updates: Sequence[TfParamUpdate],
    expected_params: SimplifiedTfBlocks,
    expected_msg: str,
):
    result_params, result_msg = update_transformation_parameters(initial_params, updates, sql_dialect='snowflake')

    assert result_params == expected_params

    if '##' in expected_msg:
        # For multi-line messages, check message prefix
        assert result_msg.startswith(expected_msg)
    else:
        # For simple patterns, check exact match
        assert result_msg == expected_msg
