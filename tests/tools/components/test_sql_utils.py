"""
Tests for SQL splitting and joining utilities.

Ported from the Keboola UI's splitSqlQueries.test.ts to ensure
the Python implementation matches the production-proven JavaScript logic.
"""

import pytest

from keboola_mcp_server.tools.components.sql_utils import (
    join_sql_statements,
    split_sql_statements,
    validate_round_trip,
)


@pytest.mark.parametrize(
    ('input_sql', 'expected', 'timeout_seconds', 'test_id'),
    [
        # Simple queries
        (
            '\nSELECT 1;\nSelect 2;\nSELECT 3;',
            ['SELECT 1;', 'Select 2;', 'SELECT 3;'],
            5.0,
            'simple_queries',
        ),
        # Multi-line comments with /* */ syntax
        (
            '\nSELECT 1;\n/*\n  Select 2;\n*/\nSELECT 3;',
            ['SELECT 1;', '/*\n  Select 2;\n*/\nSELECT 3;'],
            5.0,
            'multi_line_comments',
        ),
        # Single line comments with -- syntax
        (
            '\nSELECT 1;\n-- Select 2;\nSELECT 3;',
            ['SELECT 1;', '-- Select 2;\nSELECT 3;'],
            5.0,
            'single_line_comment_double_dash',
        ),
        # Single line comments with # syntax
        (
            '\nSELECT 1;\n# Select 2;\nSELECT 3;',
            ['SELECT 1;', '# Select 2;\nSELECT 3;'],
            5.0,
            'single_line_comment_hash',
        ),
        # Single line comments with // syntax
        (
            '\nSELECT 1;\n// Select 2;\nSELECT 3;',
            ['SELECT 1;', '// Select 2;\nSELECT 3;'],
            5.0,
            'single_line_comment_double_slash',
        ),
        # Dollar-quoted blocks with $$ syntax
        (
            '\nSELECT 1;\nexecute immediate $$\n  SELECT 2;\n  SELECT 3;\n$$;',
            ['SELECT 1;', 'execute immediate $$\n  SELECT 2;\n  SELECT 3;\n$$;'],
            5.0,
            'dollar_quoted_blocks',
        ),
        # Empty string
        (
            '',
            [],
            5.0,
            'empty_string',
        ),
        # Whitespace only
        (
            '   ',
            [],
            5.0,
            'whitespace_only',
        ),
        # Single statement without semicolon
        (
            'SELECT 1',
            ['SELECT 1'],
            5.0,
            'single_statement_no_semicolon',
        ),
        # Single statement with semicolon
        (
            'SELECT 1;',
            ['SELECT 1;'],
            5.0,
            'single_statement_with_semicolon',
        ),
        # Semicolons in single-quoted strings
        (
            "SELECT 'test;test' AS col1; SELECT 2;",
            ["SELECT 'test;test' AS col1;", 'SELECT 2;'],
            5.0,
            'semicolons_in_single_quoted_strings',
        ),
        # Semicolons in double-quoted strings
        (
            'SELECT "test;test" AS col1; SELECT 2;',
            ['SELECT "test;test" AS col1;', 'SELECT 2;'],
            5.0,
            'semicolons_in_double_quoted_strings',
        ),
        # Escaped quotes in strings
        (
            "SELECT 'it\\'s a test'; SELECT 2;",
            ["SELECT 'it\\'s a test';", 'SELECT 2;'],
            5.0,
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
            2.0,
            'complex_query_with_timeout',
        ),
        # Nested dollar quotes
        (
            'CREATE FUNCTION f() $$ SELECT $$nested$$; $$;',
            ['CREATE FUNCTION f() $$ SELECT $$nested$$; $$;'],
            5.0,
            'nested_dollar_quotes',
        ),
        # Mixed single and double quotes
        (
            "SELECT 'single', \"double\"; SELECT 2;",
            ["SELECT 'single', \"double\";", 'SELECT 2;'],
            5.0,
            'mixed_quotes',
        ),
        # Windows-style line endings (carriage returns)
        (
            'SELECT 1;\r\nSELECT 2;\r\n',
            ['SELECT 1;', 'SELECT 2;'],
            5.0,
            'carriage_returns',
        ),
    ],
)
def test_split_sql_statements(input_sql, expected, timeout_seconds, test_id):
    """Test SQL splitting with various inputs and scenarios."""
    result = split_sql_statements(input_sql, timeout_seconds=timeout_seconds)
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
def test_validate_round_trip(original, expected, test_id):
    """Test round-trip validation with various inputs and scenarios."""
    result = validate_round_trip(original)
    assert result is expected
