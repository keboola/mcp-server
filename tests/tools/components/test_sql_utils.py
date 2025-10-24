"""
Tests for SQL splitting and joining utilities.

Ported from the Keboola UI's splitSqlQueries.test.ts to ensure
the Python implementation matches the production-proven JavaScript logic.
"""

import pytest

from keboola_mcp_server.tools.components.sql_utils import join_sql_statements, split_sql_statements


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
        # Statement with trailing whitespace
        (
            ['SELECT 1;   ', 'SELECT 2  '],
            'SELECT 1;\n\nSELECT 2;\n\n',
            'trailing_whitespace',
        ),
        # Multi-line statement with line comment in middle (not at end)
        (
            ['SELECT 1 -- comment\nFROM table'],
            'SELECT 1 -- comment\nFROM table;\n\n',
            'multiline_comment_in_middle',
        ),
        # Statement ending with hash comment
        (
            ['SELECT 1 # hash comment'],
            'SELECT 1 # hash comment\n\n',
            'ending_with_hash_comment',
        ),
        # Statement ending with double slash comment
        (
            ['SELECT 1 // slash comment'],
            'SELECT 1 // slash comment\n\n',
            'ending_with_slash_comment',
        ),
        # Multi-line with comment not at end
        (
            ['SELECT 1\n-- comment\nFROM table\nWHERE id = 1'],
            'SELECT 1\n-- comment\nFROM table\nWHERE id = 1;\n\n',
            'multiline_comment_not_at_end',
        ),
        # Statement with comment in middle and semicolon at end
        (
            ['SELECT 1 -- comment\nFROM table;'],
            'SELECT 1 -- comment\nFROM table;\n\n',
            'comment_middle_semicolon_end',
        ),
        # Block comment in middle of statement
        (
            ['SELECT /* inline comment */ 1'],
            'SELECT /* inline comment */ 1;\n\n',
            'block_comment_in_middle',
        ),
        # Multiple trailing spaces and tabs
        (
            ['SELECT 1  \t  ', '  \t SELECT 2'],
            'SELECT 1;\n\nSELECT 2;\n\n',
            'mixed_whitespace',
        ),
        # Statement with newline and ending comment
        (
            ['SELECT 1\nFROM table -- get data'],
            'SELECT 1\nFROM table -- get data\n\n',
            'newline_with_ending_comment',
        ),
        # Multiple empty strings (should be filtered)
        (
            ['SELECT 1', '', '   ', '\t'],
            'SELECT 1;\n\n',
            'with_multiple_empty',
        ),
        # Pure comment statement with double dash
        (
            ['-- This is just a comment', 'SELECT 1'],
            '-- This is just a comment\n\nSELECT 1;\n\n',
            'pure_comment_double_dash',
        ),
        # Pure comment statement with hash
        (
            ['# This is just a comment', 'SELECT 1'],
            '# This is just a comment\n\nSELECT 1;\n\n',
            'pure_comment_hash',
        ),
        # Complex multi-line with various comment positions
        (
            ['SELECT a -- comment 1\n, b -- comment 2\nFROM table -- comment 3'],
            'SELECT a -- comment 1\n, b -- comment 2\nFROM table -- comment 3\n\n',
            'complex_multiline_comments',
        ),
        # Statement ending with semicolon and trailing whitespace
        (
            ['SELECT 1;  \n  ', 'SELECT 2'],
            'SELECT 1;\n\nSELECT 2;\n\n',
            'semicolon_with_trailing_whitespace',
        ),
        # Comment in middle followed by more code without comment at end
        (
            ['SELECT 1 -- inline\n, 2\nFROM t'],
            'SELECT 1 -- inline\n, 2\nFROM t;\n\n',
            'inline_comment_no_end_comment',
        ),
    ],
)
def test_join_sql_statements(statements, expected, test_id):
    """Test SQL joining with various inputs and scenarios."""
    result = join_sql_statements(statements)
    assert result == expected


@pytest.mark.parametrize(
    ('original', 'test_id'),
    [
        # Simple queries
        (
            'SELECT 1;\nSELECT 2;\nSELECT 3;',
            'simple_queries',
        ),
        # With comments
        (
            'SELECT 1;\n-- comment\nSELECT 2;',
            'with_comments',
        ),
        # With dollar-quoted blocks
        (
            'SELECT 1;\nexecute immediate $$\n  SELECT 2;\n$$;',
            'with_dollar_quotes',
        ),
        # Complex SQL
        (
            "CREATE TABLE test (id INT);\nINSERT INTO test VALUES (1);\nSELECT * FROM test WHERE name = 'test;test';",
            'complex_sql',
        ),
    ],
)
@pytest.mark.asyncio
async def test_validate_round_trip(original, test_id):
    """
    Test round-trip validation: split(join(split(x))) == split(x).

    This ensures that splitting and joining logic is consistent.
    """
    # Split original
    split_original = await split_sql_statements(original)

    # Join and split again
    joined = join_sql_statements(split_original)
    split_again = await split_sql_statements(joined)

    # Verify round-trip consistency
    assert split_original == split_again
