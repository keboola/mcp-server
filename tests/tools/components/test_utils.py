from typing import Sequence, Union

import pytest

from keboola_mcp_server.tools.components.model import ComponentType
from keboola_mcp_server.tools.components.utils import (
    TransformationConfiguration,
    _get_transformation_configuration,
    _handle_component_types,
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
    assert _handle_component_types(component_type) == expected


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

    configuration = _get_transformation_configuration(
        statements=sql_statements,
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
    assert configuration.parameters.blocks[0].codes[0].script == sql_statements
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
