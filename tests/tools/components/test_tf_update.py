"""
Tests for transformation parameter update functions.

Tests all operations for modifying SQL transformation parameters including
block and code management, script updates, and string replacements.
"""

import copy

import pytest

from keboola_mcp_server.tools.components.model import (
    SimplifiedTfBlocks,
    TfAddBlock,
    TfAddCode,
    TfAddScript,
    TfRemoveBlock,
    TfRemoveCode,
    TfRenameBlock,
    TfRenameCode,
    TfSetCode,
    TfStrReplace,
)
from keboola_mcp_server.tools.components.tf_update import (
    add_block,
    add_code,
    add_script,
    remove_block,
    remove_code,
    rename_block,
    rename_code,
    set_code,
    str_replace,
)


@pytest.fixture
def sample_params():
    """Sample transformation parameters with blocks and codes."""
    return {
        'blocks': [
            {
                'id': 'b0',
                'name': 'Block A',
                'codes': [
                    {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                    {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                ],
            },
            {
                'id': 'b1',
                'name': 'Block B',
                'codes': [
                    {'id': 'b1.c0', 'name': 'Code Z', 'script': 'SELECT * FROM table3'},
                ],
            },
        ]
    }


@pytest.fixture
def empty_params():
    """Empty transformation parameters."""
    return {'blocks': []}


# ============================================================================
# ADD_BLOCK TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params', 'expected_message'),
    [
        # Add block to end of existing blocks
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Existing Block', 'codes': []},
                ]
            },
            TfAddBlock(
                op='add_block',
                block=SimplifiedTfBlocks.Block(name='New Block', codes=[]),
                position='end',
            ),
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Existing Block', 'codes': []},
                    {'name': 'New Block', 'codes': []},
                ]
            },
            'Added block with name "New Block"',
        ),
        # Add block to start of existing blocks
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Existing Block', 'codes': []},
                ]
            },
            TfAddBlock(
                op='add_block',
                block=SimplifiedTfBlocks.Block(name='New Block', codes=[]),
                position='start',
            ),
            {
                'blocks': [
                    {'name': 'New Block', 'codes': []},
                    {'id': 'b0', 'name': 'Existing Block', 'codes': []},
                ]
            },
            'Added block with name "New Block"',
        ),
        # Add block with multiple codes
        (
            {'blocks': []},
            TfAddBlock(
                op='add_block',
                block=SimplifiedTfBlocks.Block(
                    name='Multi Code Block',
                    codes=[
                        SimplifiedTfBlocks.Block.Code(
                            name='Code 1',
                            script=(
                                'SELECT u.id, u.name, COUNT(o.id) as order_count '
                                'FROM users u LEFT JOIN orders o ON u.id = o.user_id '
                                "WHERE u.created_at > '2024-01-01' "
                                'GROUP BY u.id, u.name HAVING COUNT(o.id) > 5'
                            ),
                        ),
                        SimplifiedTfBlocks.Block.Code(
                            name='Code 2',
                            script=(
                                'SELECT p.product_name, SUM(oi.quantity * oi.price) as revenue '
                                'FROM products p INNER JOIN order_items oi ON p.id = oi.product_id '
                                'GROUP BY p.product_name ORDER BY revenue DESC LIMIT 10'
                            ),
                        ),
                    ],
                ),
            ),
            {
                'blocks': [
                    {
                        'name': 'Multi Code Block',
                        'codes': [
                            {
                                'name': 'Code 1',
                                'script': (
                                    'SELECT\n'
                                    '  u.id,\n'
                                    '  u.name,\n'
                                    '  COUNT(o.id) AS order_count\n'
                                    'FROM users AS u\n'
                                    'LEFT JOIN orders AS o\n'
                                    '  ON u.id = o.user_id\n'
                                    'WHERE\n'
                                    "  u.created_at > '2024-01-01'\n"
                                    'GROUP BY\n'
                                    '  u.id,\n'
                                    '  u.name\n'
                                    'HAVING\n'
                                    '  COUNT(o.id) > 5;'
                                ),
                            },
                            {
                                'name': 'Code 2',
                                'script': (
                                    'SELECT\n'
                                    '  p.product_name,\n'
                                    '  SUM(oi.quantity * oi.price) AS revenue\n'
                                    'FROM products AS p\n'
                                    'INNER JOIN order_items AS oi\n'
                                    '  ON p.id = oi.product_id\n'
                                    'GROUP BY\n'
                                    '  p.product_name\n'
                                    'ORDER BY\n'
                                    '  revenue DESC\n'
                                    'LIMIT 10;'
                                ),
                            },
                        ],
                    },
                ]
            },
            'Added block with name "Multi Code Block" (code was automatically reformatted)',
        ),
    ],
)
def test_add_block(initial_params, operation, expected_params, expected_message):
    """Test adding blocks to transformation parameters."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = add_block(params, operation, 'snowflake')
    assert result_params == expected_params
    assert result_msg == expected_message


@pytest.mark.parametrize(
    ('initial_params', 'block_name', 'error_match'),
    [
        # Params without blocks key
        ({}, 'First Block', "Invalid parameters: must contain 'blocks' key"),
        # Params with other keys but no blocks
        ({'other_key': 'value'}, 'First Block', "Invalid parameters: must contain 'blocks' key"),
        # Empty block name
        ({'blocks': []}, '', 'Invalid operation: block name cannot be empty'),
        # Whitespace-only block names
        ({'blocks': []}, '   ', 'Invalid operation: block name cannot be empty'),
        ({'blocks': []}, '\t', 'Invalid operation: block name cannot be empty'),
        ({'blocks': []}, '\n', 'Invalid operation: block name cannot be empty'),
    ],
)
def test_add_block_error(initial_params, block_name, error_match):
    """Test error cases when adding blocks."""
    params = copy.deepcopy(initial_params)
    operation = TfAddBlock(
        op='add_block',
        block=SimplifiedTfBlocks.Block(
            name=block_name,
            codes=[SimplifiedTfBlocks.Block.Code(name='First Code', script='SELECT 1')],
        ),
        position='end',
    )

    with pytest.raises(ValueError, match=error_match):
        add_block(params, operation, 'snowflake')


# ============================================================================
# REMOVE_BLOCK TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params'),
    [
        # Remove first block
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                    {'id': 'b1', 'name': 'Block B', 'codes': []},
                ]
            },
            TfRemoveBlock(op='remove_block', block_id='b0'),
            {
                'blocks': [
                    {'id': 'b1', 'name': 'Block B', 'codes': []},
                ]
            },
        ),
        # Remove last block
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                    {'id': 'b1', 'name': 'Block B', 'codes': []},
                ]
            },
            TfRemoveBlock(op='remove_block', block_id='b1'),
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                ]
            },
        ),
        # Remove middle block
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                    {'id': 'b1', 'name': 'Block B', 'codes': []},
                    {'id': 'b2', 'name': 'Block C', 'codes': []},
                ]
            },
            TfRemoveBlock(op='remove_block', block_id='b1'),
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                    {'id': 'b2', 'name': 'Block C', 'codes': []},
                ]
            },
        ),
        # Remove only block
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Only Block', 'codes': []},
                ]
            },
            TfRemoveBlock(op='remove_block', block_id='b0'),
            {'blocks': []},
        ),
    ],
)
def test_remove_block_success(initial_params, operation, expected_params):
    """Test successfully removing blocks from transformation parameters."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = remove_block(params, operation, 'snowflake')
    assert result_params == expected_params
    assert result_msg == ''


@pytest.mark.parametrize(
    ('initial_params', 'block_id_to_remove'),
    [
        # Block not found
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                ]
            },
            'nonexistent',
        ),
        # Empty blocks list
        (
            {'blocks': []},
            'b0',
        ),
    ],
)
def test_remove_block_error(initial_params, block_id_to_remove):
    """Test error cases when removing blocks."""
    params = copy.deepcopy(initial_params)
    operation = TfRemoveBlock(op='remove_block', block_id=block_id_to_remove)

    with pytest.raises(ValueError, match=f"Block with id '{block_id_to_remove}' does not exist"):
        remove_block(params, operation, 'snowflake')


# ============================================================================
# RENAME_BLOCK TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params'),
    [
        # Rename first block
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': [{'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT 1'}]},
                    {'id': 'b1', 'name': 'Block B', 'codes': []},
                ]
            },
            TfRenameBlock(op='rename_block', block_id='b0', block_name='Renamed Block A'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Renamed Block A',
                        'codes': [{'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT 1'}],
                    },
                    {'id': 'b1', 'name': 'Block B', 'codes': []},
                ]
            },
        ),
        # Rename second block
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                    {'id': 'b1', 'name': 'Block B', 'codes': []},
                ]
            },
            TfRenameBlock(op='rename_block', block_id='b1', block_name='Renamed Block B'),
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                    {'id': 'b1', 'name': 'Renamed Block B', 'codes': []},
                ]
            },
        ),
        # Rename with special characters
        (
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block A', 'codes': []},
                ]
            },
            TfRenameBlock(op='rename_block', block_id='b0', block_name='Block with Special-Chars_123'),
            {
                'blocks': [
                    {'id': 'b0', 'name': 'Block with Special-Chars_123', 'codes': []},
                ]
            },
        ),
    ],
)
def test_rename_block_success(initial_params, operation, expected_params):
    """Test successfully renaming blocks."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = rename_block(params, operation, 'snowflake')
    assert result_params == expected_params
    assert result_msg == ''


@pytest.mark.parametrize(
    ('block_id', 'block_name', 'error_match'),
    [
        # Non-existent block IDs
        ('nonexistent', 'New Name', "Block with id 'nonexistent' does not exist"),
        ('b999', 'New Name', "Block with id 'b999' does not exist"),
        # Empty block name
        ('b0', '', 'Invalid operation: block name cannot be empty'),
        # Whitespace-only block names
        ('b0', '   ', 'Invalid operation: block name cannot be empty'),
        ('b0', '\t', 'Invalid operation: block name cannot be empty'),
        ('b0', '\n', 'Invalid operation: block name cannot be empty'),
    ],
)
def test_rename_block_error(sample_params, block_id, block_name, error_match):
    """Test error cases when renaming blocks."""
    params = copy.deepcopy(sample_params)
    operation = TfRenameBlock(op='rename_block', block_id=block_id, block_name=block_name)

    with pytest.raises(ValueError, match=error_match):
        rename_block(params, operation, 'snowflake')


# ============================================================================
# ADD_CODE TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params', 'expected_message'),
    [
        # Add code to end
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                ]
            },
            TfAddCode(
                op='add_code',
                block_id='b0',
                code=SimplifiedTfBlocks.Block.Code(
                    name='New Code at End',
                    script=('SELECT col1 FROM table1;'),
                ),
                position='end',
            ),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                            {'name': 'New Code at End', 'script': 'SELECT\n  col1\nFROM table1;'},
                        ],
                    },
                ]
            },
            'Added code with name "New Code at End" (code was automatically reformatted)',
        ),
        # Add code to start
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            TfAddCode(
                op='add_code',
                block_id='b0',
                code=SimplifiedTfBlocks.Block.Code(
                    name='New Code at Start',
                    script=(
                        'SELECT DISTINCT category, AVG(price) OVER (PARTITION BY category) as avg_price '
                        'FROM products WHERE in_stock = true ORDER BY category'
                    ),
                ),
                position='start',
            ),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {
                                'name': 'New Code at Start',
                                'script': (
                                    'SELECT DISTINCT\n'
                                    '  category,\n'
                                    '  AVG(price) OVER (PARTITION BY category) AS avg_price\n'
                                    'FROM products\n'
                                    'WHERE\n'
                                    '  in_stock = TRUE\n'
                                    'ORDER BY\n'
                                    '  category;'
                                ),
                            },
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            'Added code with name "New Code at Start" (code was automatically reformatted)',
        ),
    ],
)
def test_add_code_success(initial_params, operation, expected_params, expected_message):
    """Test successfully adding code to blocks."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = add_code(params, operation, sql_dialect='snowflake')
    assert result_params == expected_params
    assert result_msg == expected_message


@pytest.mark.parametrize(
    ('block_id', 'code_name', 'error_match'),
    [
        # Non-existent block IDs
        ('nonexistent', 'Test Code', "Block with id 'nonexistent' does not exist"),
        ('b999', 'Test Code', "Block with id 'b999' does not exist"),
        # Empty code name
        ('b0', '', 'Invalid operation: code name cannot be empty'),
        # Whitespace-only code names
        ('b0', '   ', 'Invalid operation: code name cannot be empty'),
        ('b0', '\t', 'Invalid operation: code name cannot be empty'),
        ('b0', '\n', 'Invalid operation: code name cannot be empty'),
    ],
)
def test_add_code_error(sample_params, block_id, code_name, error_match):
    """Test error cases when adding code to blocks."""
    params = copy.deepcopy(sample_params)
    operation = TfAddCode(
        op='add_code',
        block_id=block_id,
        code=SimplifiedTfBlocks.Block.Code(name=code_name, script='SELECT 1'),
        position='end',
    )

    with pytest.raises(ValueError, match=error_match):
        add_code(params, operation, sql_dialect='snowflake')


# ============================================================================
# REMOVE_CODE TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params'),
    [
        # Remove first code
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                ]
            },
            TfRemoveCode(op='remove_code', block_id='b0', code_id='b0.c0'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                ]
            },
        ),
        # Remove only code in block
        (
            {
                'blocks': [
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [
                            {'id': 'b1.c0', 'name': 'Code Z', 'script': 'SELECT * FROM table3'},
                        ],
                    },
                ]
            },
            TfRemoveCode(op='remove_code', block_id='b1', code_id='b1.c0'),
            {
                'blocks': [
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [],
                    },
                ]
            },
        ),
    ],
)
def test_remove_code_success(initial_params, operation, expected_params):
    """Test successfully removing code from blocks."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = remove_code(params, operation, sql_dialect='snowflake')
    assert result_params == expected_params
    assert result_msg == ''


@pytest.mark.parametrize(
    ('block_id', 'code_id'),
    [
        ('b0', 'nonexistent'),
        ('nonexistent', 'b0.c0'),
        ('b0', 'b1.c0'),
    ],
)
def test_remove_code_error(sample_params, block_id, code_id):
    """Test error cases when removing code from blocks."""
    params = copy.deepcopy(sample_params)
    operation = TfRemoveCode(op='remove_code', block_id=block_id, code_id=code_id)

    with pytest.raises(ValueError, match=f"Code with id '{code_id}' in block '{block_id}' does not exist"):
        remove_code(params, operation, sql_dialect='snowflake')


# ============================================================================
# RENAME_CODE TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params'),
    [
        # Rename first code
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                ]
            },
            TfRenameCode(op='rename_code', block_id='b0', code_id='b0.c0', code_name='Renamed Code X'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Renamed Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                ]
            },
        ),
        # Rename second code
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [
                            {'id': 'b1.c0', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                            {'id': 'b1.c1', 'name': 'Code Z', 'script': 'SELECT * FROM table3'},
                        ],
                    },
                ]
            },
            TfRenameCode(op='rename_code', block_id='b1', code_id='b1.c1', code_name='Renamed Code Z'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [
                            {'id': 'b1.c0', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                            {'id': 'b1.c1', 'name': 'Renamed Code Z', 'script': 'SELECT * FROM table3'},
                        ],
                    },
                ]
            },
        ),
    ],
)
def test_rename_code_success(initial_params, operation, expected_params):
    """Test successfully renaming code in blocks."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = rename_code(params, operation, sql_dialect='snowflake')
    assert result_params == expected_params
    assert result_msg == ''


@pytest.mark.parametrize(
    ('block_id', 'code_id', 'code_name', 'error_match'),
    [
        # Non-existent code IDs
        ('b0', 'nonexistent', 'New Name', "Code with id 'nonexistent' in block 'b0' does not exist"),
        ('nonexistent', 'b0.c0', 'New Name', "Code with id 'b0.c0' in block 'nonexistent' does not exist"),
        ('b0', 'b1.c0', 'New Name', "Code with id 'b1.c0' in block 'b0' does not exist"),
        # Empty code name
        ('b0', 'b0.c0', '', 'Invalid operation: code name cannot be empty'),
        # Whitespace-only code names
        ('b0', 'b0.c0', '   ', 'Invalid operation: code name cannot be empty'),
        ('b0', 'b0.c0', '\t', 'Invalid operation: code name cannot be empty'),
        ('b0', 'b0.c0', '\n', 'Invalid operation: code name cannot be empty'),
    ],
)
def test_rename_code_error(sample_params, block_id, code_id, code_name, error_match):
    """Test error cases when renaming code in blocks."""
    params = copy.deepcopy(sample_params)
    operation = TfRenameCode(op='rename_code', block_id=block_id, code_id=code_id, code_name=code_name)

    with pytest.raises(ValueError, match=error_match):
        rename_code(params, operation, sql_dialect='snowflake')


# ============================================================================
# SET_CODE TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params', 'expected_message'),
    [
        # Set code script
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            TfSetCode(op='set_code', block_id='b0', code_id='b0.c0', script='SELECT * FROM new_table'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT\n  *\nFROM new_table;'},
                        ],
                    },
                ]
            },
            "Changed code with id 'b0.c0' in block 'b0' (code was automatically reformatted)",
        ),
        # Set multiline script
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            TfSetCode(op='set_code', block_id='b0', code_id='b0.c0', script='SELECT *\nFROM table1\nWHERE col = 1'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT\n  *\nFROM table1\nWHERE\n  col = 1;'},
                        ],
                    },
                ]
            },
            "Changed code with id 'b0.c0' in block 'b0' (code was automatically reformatted)",
        ),
    ],
)
def test_set_code_success(initial_params, operation, expected_params, expected_message):
    """Test successfully setting code script."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = set_code(params, operation, sql_dialect='snowflake')
    assert result_params == expected_params
    assert result_msg == expected_message


@pytest.mark.parametrize(
    ('block_id', 'code_id', 'script', 'error_match'),
    [
        ('b0', 'nonexistent', 'SELECT 1', "Code with id 'nonexistent' in block 'b0' does not exist"),
        ('nonexistent', 'b0.c0', 'SELECT 1', "Code with id 'b0.c0' in block 'nonexistent' does not exist"),
        ('b0', 'b0.c0', '', 'Invalid operation: script cannot be empty'),
        ('b0', 'b0.c0', '   ', 'Invalid operation: script cannot be empty'),
    ],
)
def test_set_code_error(sample_params, block_id, code_id, script, error_match):
    """Test error cases when setting code script."""
    params = copy.deepcopy(sample_params)
    operation = TfSetCode(op='set_code', block_id=block_id, code_id=code_id, script=script)

    with pytest.raises(ValueError, match=error_match):
        set_code(params, operation, sql_dialect='snowflake')


# ============================================================================
# ADD_SCRIPT TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params', 'expected_message'),
    [
        # Append to existing script
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            TfAddScript(op='add_script', block_id='b0', code_id='b0.c0', script='WHERE col = 1'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Code X',
                                'script': 'SELECT\n  *\nFROM table1\nWHERE\n  col = 1;',
                            },
                        ],
                    },
                ]
            },
            "Added script to code with id 'b0.c0' in block 'b0' (code was automatically reformatted)",
        ),
        # Prepend to existing script
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            TfAddScript(
                op='add_script', block_id='b0', code_id='b0.c0', script='SELECT * FROM table0;', position='start'
            ),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Code X',
                                'script': 'SELECT\n  *\nFROM table0;\n\nSELECT\n  *\nFROM table1;',
                            },
                        ],
                    },
                ]
            },
            "Added script to code with id 'b0.c0' in block 'b0' (code was automatically reformatted)",
        ),
        # Prepend to existing script (creates invalid SQL, not reformatted)
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            TfAddScript(
                op='add_script', block_id='b0', code_id='b0.c0', script='SELECT * FROM table0', position='start'
            ),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {
                                'id': 'b0.c0',
                                'name': 'Code X',
                                'script': 'SELECT * FROM table0 SELECT * FROM table1',
                            },
                        ],
                    },
                ]
            },
            "Added script to code with id 'b0.c0' in block 'b0'",
        ),
        # Append to empty script
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': ''},
                        ],
                    },
                ]
            },
            TfAddScript(op='add_script', block_id='b0', code_id='b0.c0', script='SELECT 1', position='end'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT\n  1;'},
                        ],
                    },
                ]
            },
            "Added script to code with id 'b0.c0' in block 'b0' (code was automatically reformatted)",
        ),
    ],
)
def test_add_script_success(initial_params, operation, expected_params, expected_message):
    """Test successfully adding script to code."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = add_script(params, operation, sql_dialect='snowflake')
    assert result_params == expected_params
    assert result_msg == expected_message


@pytest.mark.parametrize(
    ('block_id', 'code_id', 'script', 'error_match'),
    [
        # Non-existent code IDs
        ('b0', 'nonexistent', 'SELECT 1', "Code with id 'nonexistent' in block 'b0' does not exist"),
        ('nonexistent', 'b0.c0', 'SELECT 1', "Code with id 'b0.c0' in block 'nonexistent' does not exist"),
        # Empty script
        ('b0', 'b0.c0', '', 'Invalid operation: script cannot be empty'),
        # Whitespace-only scripts
        ('b0', 'b0.c0', '   ', 'Invalid operation: script cannot be empty'),
        ('b0', 'b0.c0', '\t', 'Invalid operation: script cannot be empty'),
        ('b0', 'b0.c0', '\n', 'Invalid operation: script cannot be empty'),
    ],
)
def test_add_script_error(sample_params, block_id, code_id, script, error_match):
    """Test error cases when adding script to code."""
    params = copy.deepcopy(sample_params)
    operation = TfAddScript(
        op='add_script',
        block_id=block_id,
        code_id=code_id,
        script=script,
        position='end',
    )

    with pytest.raises(ValueError, match=error_match):
        add_script(params, operation, sql_dialect='snowflake')


# ============================================================================
# STR_REPLACE TESTS
# ============================================================================


@pytest.mark.parametrize(
    ('initial_params', 'operation', 'expected_params', 'expected_msg'),
    [
        # Replace in all blocks and codes
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [
                            {'id': 'b1.c0', 'name': 'Code Z', 'script': 'SELECT * FROM table3'},
                        ],
                    },
                ]
            },
            TfStrReplace(op='str_replace', block_id=None, code_id=None, search_for='FROM', replace_with='IN'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * IN table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * IN table2'},
                        ],
                    },
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [
                            {'id': 'b1.c0', 'name': 'Code Z', 'script': 'SELECT * IN table3'},
                        ],
                    },
                ]
            },
            'Replaced 3 occurrences of "FROM" in the transformation',
        ),
        # Replace in specific block
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [
                            {'id': 'b1.c0', 'name': 'Code Z', 'script': 'SELECT * FROM table3'},
                        ],
                    },
                ]
            },
            TfStrReplace(op='str_replace', block_id='b0', code_id=None, search_for='FROM', replace_with='IN'),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * IN table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * IN table2'},
                        ],
                    },
                    {
                        'id': 'b1',
                        'name': 'Block B',
                        'codes': [
                            {'id': 'b1.c0', 'name': 'Code Z', 'script': 'SELECT * FROM table3'},
                        ],
                    },
                ]
            },
            'Replaced 2 occurrences of "FROM" in block "b0"',
        ),
        # Replace in specific code
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                ]
            },
            TfStrReplace(
                op='str_replace', block_id='b0', code_id='b0.c0', search_for='table1', replace_with='new_table1'
            ),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM new_table1'},
                            {'id': 'b0.c1', 'name': 'Code Y', 'script': 'SELECT * FROM table2'},
                        ],
                    },
                ]
            },
            'Replaced 1 occurrence of "table1" in code "b0.c0", block "b0"',
        ),
        # Replace with empty string
        (
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'SELECT * FROM table1'},
                        ],
                    },
                ]
            },
            TfStrReplace(op='str_replace', block_id='b0', code_id='b0.c0', search_for='SELECT * ', replace_with=''),
            {
                'blocks': [
                    {
                        'id': 'b0',
                        'name': 'Block A',
                        'codes': [
                            {'id': 'b0.c0', 'name': 'Code X', 'script': 'FROM table1'},
                        ],
                    },
                ]
            },
            'Replaced 1 occurrence of "SELECT * " in code "b0.c0", block "b0"',
        ),
    ],
)
def test_str_replace_success(initial_params, operation, expected_params, expected_msg):
    """Test successfully replacing strings in scripts."""
    params = copy.deepcopy(initial_params)
    result_params, result_msg = str_replace(params, operation, sql_dialect='snowflake')
    assert result_params == expected_params
    assert result_msg == expected_msg


@pytest.mark.parametrize(
    ('block_id', 'code_id', 'search_for', 'replace_with', 'error_match'),
    [
        # Empty search string
        ('b0', 'b0.c0', '', 'replacement', 'Invalid operation: search string is empty'),
        # Search and replace are the same
        ('b0', 'b0.c0', 'table', 'table', 'Invalid operation: search string and replace string are the same'),
        # Search string not found
        ('b0', 'b0.c0', 'nonexistent', 'replacement', 'Search string "nonexistent" not found'),
        # Invalid block ID
        ('nonexistent', None, 'table', 'new_table', 'No scripts found'),
        # Invalid code ID
        ('b0', 'nonexistent', 'table', 'new_table', 'No scripts found'),
    ],
)
def test_str_replace_error(sample_params, block_id, code_id, search_for, replace_with, error_match):
    """Test error cases when replacing strings in scripts."""
    params = copy.deepcopy(sample_params)
    operation = TfStrReplace(
        op='str_replace',
        block_id=block_id,
        code_id=code_id,
        search_for=search_for,
        replace_with=replace_with,
    )

    with pytest.raises(ValueError, match=error_match):
        str_replace(params, operation, sql_dialect='snowflake')


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


def test_multiple_operations_sequence(sample_params):
    """Test applying multiple operations in sequence."""
    params = copy.deepcopy(sample_params)

    # 1. Add a new block
    params, _ = add_block(
        params,
        TfAddBlock(
            op='add_block',
            block=SimplifiedTfBlocks.Block(
                name='New Block',
                codes=[SimplifiedTfBlocks.Block.Code(name='New Code', script='SELECT 1')],
            ),
            position='end',
        ),
        'snowflake',
    )
    assert len(params['blocks']) == 3

    # 2. Rename an existing block
    params, _ = rename_block(
        params, TfRenameBlock(op='rename_block', block_id='b0', block_name='Renamed Block A'), sql_dialect='snowflake'
    )
    assert params['blocks'][0]['name'] == 'Renamed Block A'

    # 3. Add code to existing block
    params, _ = add_code(
        params,
        TfAddCode(
            op='add_code',
            block_id='b0',
            code=SimplifiedTfBlocks.Block.Code(name='Additional Code', script='SELECT * FROM new_table'),
            position='end',
        ),
        sql_dialect='snowflake',
    )

    # 4. Replace string in all scripts
    params, _ = str_replace(
        params,
        TfStrReplace(op='str_replace', block_id=None, code_id=None, search_for='FROM', replace_with='IN'),
        sql_dialect='snowflake',
    )

    # Verify final state
    assert len(params['blocks']) == 3
    assert len(params['blocks'][0]['codes']) == 3  # Original 2 + 1 added
    # Verify string replacement worked
    assert 'IN' in params['blocks'][0]['codes'][0]['script']


def test_operations_preserve_unaffected_data(sample_params):
    """Test that operations don't modify unrelated blocks or codes."""
    params = copy.deepcopy(sample_params)

    # Store original second block
    original_second_block = copy.deepcopy(params['blocks'][1])

    # Modify first block
    params, _ = rename_block(
        params, TfRenameBlock(op='rename_block', block_id='b0', block_name='Modified Block'), sql_dialect='snowflake'
    )

    # Verify second block unchanged
    assert params['blocks'][1] == original_second_block
