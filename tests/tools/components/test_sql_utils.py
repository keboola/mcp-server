"""
Tests for SQL splitting and joining utilities.

Ported from the Keboola UI's splitSqlQueries.test.ts to ensure
the Python implementation matches the production-proven JavaScript logic.
"""

import pytest

from keboola_mcp_server.tools.components.sql_utils import (
    blocks_to_string,
    join_sql_statements,
    split_sql_statements,
    string_to_blocks,
    validate_blocks_round_trip,
    validate_round_trip,
)


@pytest.mark.parametrize(
    ('input_sql', 'expected', 'timeout_seconds', 'test_id'),
    [
        # Simple queries
        (
            '\nSELECT 1;\nSelect 2;\nSELECT 3;',
            ['SELECT 1;', 'Select 2;', 'SELECT 3;'],
            1.0,
            'simple_queries',
        ),
        # Multi-line comments with /* */ syntax
        (
            '\nSELECT 1;\n/*\n  Select 2;\n*/\nSELECT 3;',
            ['SELECT 1;', '/*\n  Select 2;\n*/\nSELECT 3;'],
            1.0,
            'multi_line_comments',
        ),
        # Single line comments with -- syntax
        (
            '\nSELECT 1;\n-- Select 2;\nSELECT 3;',
            ['SELECT 1;', '-- Select 2;\nSELECT 3;'],
            1.0,
            'single_line_comment_double_dash',
        ),
        # Single line comments with # syntax
        (
            '\nSELECT 1;\n# Select 2;\nSELECT 3;',
            ['SELECT 1;', '# Select 2;\nSELECT 3;'],
            1.0,
            'single_line_comment_hash',
        ),
        # Single line comments with // syntax
        (
            '\nSELECT 1;\n// Select 2;\nSELECT 3;',
            ['SELECT 1;', '// Select 2;\nSELECT 3;'],
            1.0,
            'single_line_comment_double_slash',
        ),
        # Dollar-quoted blocks with $$ syntax
        (
            '\nSELECT 1;\nexecute immediate $$\n  SELECT 2;\n  SELECT 3;\n$$;',
            ['SELECT 1;', 'execute immediate $$\n  SELECT 2;\n  SELECT 3;\n$$;'],
            1.0,
            'dollar_quoted_blocks',
        ),
        # Empty string
        (
            '',
            [],
            1.0,
            'empty_string',
        ),
        # Whitespace only
        (
            '   ',
            [],
            1.0,
            'whitespace_only',
        ),
        # Single statement without semicolon
        (
            'SELECT 1',
            ['SELECT 1'],
            1.0,
            'single_statement_no_semicolon',
        ),
        # Single statement with semicolon
        (
            'SELECT 1;',
            ['SELECT 1;'],
            1.0,
            'single_statement_with_semicolon',
        ),
        # Semicolons in single-quoted strings
        (
            "SELECT 'test;test' AS col1; SELECT 2;",
            ["SELECT 'test;test' AS col1;", 'SELECT 2;'],
            1.0,
            'semicolons_in_single_quoted_strings',
        ),
        # Semicolons in double-quoted strings
        (
            'SELECT "test;test" AS col1; SELECT 2;',
            ['SELECT "test;test" AS col1;', 'SELECT 2;'],
            1.0,
            'semicolons_in_double_quoted_strings',
        ),
        # Escaped quotes in strings
        (
            "SELECT 'it\\'s a test'; SELECT 2;",
            ["SELECT 'it\\'s a test';", 'SELECT 2;'],
            1.0,
            'escaped_quotes',
        ),
        # Complex query with timeout
        (
            (
                'SELECT 1;\n-- Comment line\nexecute immediate $$\n  SELECT 2;\n  '
                "SELECT 'value;still string';\n$$;\nSELECT 3;\n"
                '-- Another comment\nSELECT "double" as col;\n'
            ),
            [
                'SELECT 1;',
                ('-- Comment line\nexecute immediate $$\n  SELECT 2;\n  ' "SELECT 'value;still string';\n$$;"),
                'SELECT 3;',
                '-- Another comment\nSELECT "double" as col;',
            ],
            1.0,
            'complex_query_with_timeout',
        ),
        # Nested dollar quotes
        (
            'CREATE FUNCTION f() $$ SELECT $$nested$$; $$;',
            ['CREATE FUNCTION f() $$ SELECT $$nested$$; $$;'],
            1.0,
            'nested_dollar_quotes',
        ),
        # Mixed single and double quotes
        (
            "SELECT 'single', \"double\"; SELECT 2;",
            ["SELECT 'single', \"double\";", 'SELECT 2;'],
            1.0,
            'mixed_quotes',
        ),
        # Windows-style line endings (carriage returns)
        (
            'SELECT 1;\r\nSELECT 2;\r\n',
            ['SELECT 1;', 'SELECT 2;'],
            1.0,
            'carriage_returns',
        ),
    ],
)
@pytest.mark.asyncio
async def test_split_sql_statements(input_sql, expected, timeout_seconds, test_id):
    """Test SQL splitting with various inputs and scenarios."""
    result = await split_sql_statements(input_sql, timeout_seconds=timeout_seconds)
    assert result == expected


@pytest.mark.parametrize(
    ('statements', 'expected', 'test_id'),
    [
        # Empty list
        (
            [],
            '',
            'empty_list',
        ),
        # Single statement
        (
            ['SELECT 1'],
            'SELECT 1;\n\n',
            'single_statement',
        ),
        # Multiple statements
        (
            ['SELECT 1', 'SELECT 2', 'SELECT 3'],
            'SELECT 1;\n\nSELECT 2;\n\nSELECT 3;\n\n',
            'multiple_statements',
        ),
        # Preserve existing semicolons
        (
            ['SELECT 1;', 'SELECT 2;'],
            'SELECT 1;\n\nSELECT 2;\n\n',
            'existing_semicolons',
        ),
        # Statement ending with line comment
        (
            ['SELECT 1 -- comment', 'SELECT 2'],
            'SELECT 1 -- comment\n\nSELECT 2;\n\n',
            'ending_with_line_comment',
        ),
        # Statement ending with block comment
        (
            ['SELECT 1 /* comment */', 'SELECT 2'],
            'SELECT 1 /* comment */\n\nSELECT 2;\n\n',
            'ending_with_block_comment',
        ),
        # Mixed statements
        (
            ['SELECT 1;', 'SELECT 2', 'SELECT 3 -- comment', 'SELECT 4'],
            'SELECT 1;\n\nSELECT 2;\n\nSELECT 3 -- comment\n\nSELECT 4;\n\n',
            'mixed_statements',
        ),
        # Filter empty statements
        (
            ['SELECT 1', '', '  ', 'SELECT 2'],
            'SELECT 1;\n\nSELECT 2;\n\n',
            'filter_empty_statements',
        ),
        # Preserve internal whitespace
        (
            ['SELECT  \n  1'],
            'SELECT  \n  1;\n\n',
            'preserve_whitespace',
        ),
    ],
)
def test_join_sql_statements(statements, expected, test_id):
    """Test SQL joining with various inputs and scenarios."""
    result = join_sql_statements(statements)
    assert result == expected


@pytest.mark.parametrize(
    ('original', 'expected', 'test_id'),
    [
        # Simple queries
        (
            'SELECT 1;\nSELECT 2;\nSELECT 3;',
            True,
            'simple_queries',
        ),
        # With comments
        (
            'SELECT 1;\n-- comment\nSELECT 2;',
            True,
            'with_comments',
        ),
        # With dollar-quoted blocks
        (
            'SELECT 1;\nexecute immediate $$\n  SELECT 2;\n$$;',
            True,
            'with_dollar_quotes',
        ),
        # Complex SQL
        (
            "CREATE TABLE test (id INT);\nINSERT INTO test VALUES (1);\nSELECT * FROM test WHERE name = 'test;test';",
            True,
            'complex_sql',
        ),
    ],
)
@pytest.mark.asyncio
async def test_validate_round_trip(original, expected, test_id):
    """Test round-trip validation with various inputs and scenarios."""
    result = await validate_round_trip(original)
    assert result is expected


@pytest.mark.parametrize(
    ('blocks', 'expected', 'ignore_delimiters', 'test_id'),
    [
        # Single block, single code
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1', 'SELECT 2'],
                        }
                    ],
                }
            ],
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;\n\nSELECT 2;\n',
            False,
            'single_block_single_code',
        ),
        # Multiple blocks
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1'],
                        }
                    ],
                },
                {
                    'name': 'Block 2',
                    'codes': [
                        {
                            'name': 'Second Code',
                            'script': ['SELECT 2'],
                        }
                    ],
                },
            ],
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;\n\n'
            '/* ===== BLOCK: Block 2 ===== */\n\n/* ===== CODE: Second Code ===== */\n\nSELECT 2;\n',
            False,
            'multiple_blocks',
        ),
        # Multiple codes in block
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1'],
                        },
                        {
                            'name': 'Second Code',
                            'script': ['SELECT 2'],
                        },
                    ],
                }
            ],
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;\n\n'
            '/* ===== CODE: Second Code ===== */\n\nSELECT 2;\n',
            False,
            'multiple_codes_in_block',
        ),
        # Script with existing semicolons
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1;', 'SELECT 2;'],
                        }
                    ],
                }
            ],
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;\n\nSELECT 2;\n',
            False,
            'existing_semicolons',
        ),
        # Empty blocks list
        (
            [],
            '',
            False,
            'empty_blocks',
        ),
        # Empty script
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'Empty Code',
                            'script': [],
                        }
                    ],
                }
            ],
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: Empty Code ===== */\n',
            False,
            'empty_script',
        ),
        # Ignore delimiters
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1'],
                        }
                    ],
                }
            ],
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1\n',
            True,
            'ignore_delimiters',
        ),
        # Script ending with comment
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1 /* comment */'],
                        }
                    ],
                }
            ],
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1 /* comment */\n',
            False,
            'ending_with_comment',
        ),
        # Complex multi-block structure
        (
            [
                {
                    'name': 'Setup',
                    'codes': [
                        {
                            'name': 'Create Tables',
                            'script': ['CREATE TABLE users (id INT)', 'CREATE TABLE orders (id INT)'],
                        }
                    ],
                },
                {
                    'name': 'Data Load',
                    'codes': [
                        {
                            'name': 'Insert Users',
                            'script': ['INSERT INTO users VALUES (1)'],
                        },
                        {
                            'name': 'Insert Orders',
                            'script': ['INSERT INTO orders VALUES (1)'],
                        },
                    ],
                },
            ],
            '/* ===== BLOCK: Setup ===== */\n\n/* ===== CODE: Create Tables ===== */\n\nCREATE TABLE users (id INT);'
            '\n\nCREATE TABLE orders (id INT);\n\n/* ===== BLOCK: Data Load ===== */\n\n'
            '/* ===== CODE: Insert Users ===== */\n\nINSERT INTO users VALUES (1);\n\n'
            '/* ===== CODE: Insert Orders ===== */\n\nINSERT INTO orders VALUES (1);\n',
            False,
            'complex_multi_block',
        ),
    ],
)
def test_blocks_to_string(blocks, expected, ignore_delimiters, test_id):
    """Test converting blocks to formatted SQL string."""
    result = blocks_to_string(blocks, ignore_delimiters=ignore_delimiters)
    assert result == expected


@pytest.mark.parametrize(
    ('code_string', 'expected', 'test_id'),
    [
        # Single block, single code
        (
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;\nSELECT 2;',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1;', 'SELECT 2;'],
                        }
                    ],
                }
            ],
            'single_block_single_code',
        ),
        # Multiple blocks
        (
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;\n\n'
            '/* ===== BLOCK: Block 2 ===== */\n\n/* ===== CODE: Second Code ===== */\n\nSELECT 2;',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1;'],
                        }
                    ],
                },
                {
                    'name': 'Block 2',
                    'codes': [
                        {
                            'name': 'Second Code',
                            'script': ['SELECT 2;'],
                        }
                    ],
                },
            ],
            'multiple_blocks',
        ),
        # Multiple codes in block
        (
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;\n\n'
            '/* ===== CODE: Second Code ===== */\n\nSELECT 2;',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1;'],
                        },
                        {
                            'name': 'Second Code',
                            'script': ['SELECT 2;'],
                        },
                    ],
                }
            ],
            'multiple_codes_in_block',
        ),
        # Empty string
        (
            '',
            [],
            'empty_string',
        ),
        # Empty code content
        (
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: Empty Code ===== */\n\n',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'Empty Code',
                            'script': [],
                        }
                    ],
                }
            ],
            'empty_code_content',
        ),
        # Shared code marker
        (
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== SHARED CODE: Shared Utils ===== */\n\nSELECT 1;',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'Shared Utils',
                            'script': ['SELECT 1;'],
                        }
                    ],
                }
            ],
            'shared_code_marker',
        ),
        # Complex multi-statement code
        (
            '/* ===== BLOCK: Setup ===== */\n\n/* ===== CODE: Create Tables ===== */\n\nCREATE TABLE users (id INT);'
            '\nCREATE TABLE orders (id INT);',
            [
                {
                    'name': 'Setup',
                    'codes': [
                        {
                            'name': 'Create Tables',
                            'script': ['CREATE TABLE users (id INT);', 'CREATE TABLE orders (id INT);'],
                        }
                    ],
                }
            ],
            'complex_multi_statement',
        ),
        # Whitespace variations
        (
            '/*=====BLOCK:Block 1=====*/\n\n/*=====CODE:First Code=====*/\n\nSELECT 1;',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1;'],
                        }
                    ],
                }
            ],
            'whitespace_variations',
        ),
        # Code with comments
        (
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */\n\nSELECT 1;'
            '\n-- Comment\nSELECT 2;',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1;', '-- Comment\nSELECT 2;'],
                        }
                    ],
                }
            ],
            'code_with_comments',
        ),
        # Code with dollar-quoted blocks
        (
            '/* ===== BLOCK: Block 1 ===== */\n\n/* ===== CODE: First Code ===== */'
            '\n\nexecute immediate $$\n  SELECT 1;\n$$;',
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['execute immediate $$\n  SELECT 1;\n$$;'],
                        }
                    ],
                }
            ],
            'dollar_quoted_blocks',
        ),
    ],
)
@pytest.mark.asyncio
async def test_string_to_blocks(code_string, expected, test_id):
    """Test parsing formatted SQL string back to blocks."""
    result = await string_to_blocks(code_string)
    assert result == expected


@pytest.mark.parametrize(
    ('blocks', 'expected', 'test_id'),
    [
        # Simple single block
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1', 'SELECT 2'],
                        }
                    ],
                }
            ],
            True,
            'simple_single_block',
        ),
        # Multiple blocks
        (
            [
                {
                    'name': 'Block 1',
                    'codes': [
                        {
                            'name': 'First Code',
                            'script': ['SELECT 1'],
                        }
                    ],
                },
                {
                    'name': 'Block 2',
                    'codes': [
                        {
                            'name': 'Second Code',
                            'script': ['SELECT 2'],
                        }
                    ],
                },
            ],
            True,
            'multiple_blocks',
        ),
        # Empty blocks
        (
            [],
            True,
            'empty_blocks',
        ),
        # Complex structure
        (
            [
                {
                    'name': 'Setup',
                    'codes': [
                        {
                            'name': 'Create Tables',
                            'script': ['CREATE TABLE users (id INT)', 'CREATE TABLE orders (id INT)'],
                        },
                        {
                            'name': 'Create Views',
                            'script': ['CREATE VIEW user_orders AS SELECT * FROM users JOIN orders'],
                        },
                    ],
                },
                {
                    'name': 'Data Load',
                    'codes': [
                        {
                            'name': 'Insert Data',
                            'script': ['INSERT INTO users VALUES (1)', 'INSERT INTO orders VALUES (1)'],
                        }
                    ],
                },
            ],
            True,
            'complex_structure',
        ),
    ],
)
@pytest.mark.asyncio
async def test_validate_blocks_round_trip(blocks, expected, test_id):
    """Test round-trip validation for block conversion."""
    result = await validate_blocks_round_trip(blocks)
    assert result is expected
