"""
Tests for SQL splitting and joining utilities.

Ported from the Keboola UI's splitSqlQueries.test.ts to ensure
the Python implementation matches the production-proven JavaScript logic.
"""

import pytest

from keboola_mcp_server.tools.components.sql_utils import (
    format_sql,
    join_sql_statements,
    split_sql_statements,
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
        # Complex SQL with division operators and table names containing dashes
        (
            (
                'CREATE TABLE `top_20_products_revenue` AS\n'
                'SELECT\n'
                '/* comment */\n'
                'ROW_NUMBER() OVER (ORDER BY SUM(CAST(`line_items_quantity` AS INT64) *'
                ' CAST(`line_items_price` AS FLOAT64)) DESC) as revenue_rank,\n'
                '`line_items_product_id` as product_id,\n'
                '`line_items_title` as product_title,\n'
                'COUNT(DISTINCT `line_items_variant_id`) as variant_count,\n'
                'SUM(CAST(`line_items_quantity` AS INT64)) as total_quantity_sold,\n'
                'ROUND(SUM(CAST(`line_items_quantity` AS INT64) * CAST(`line_items_price` AS FLOAT64)), 2)'
                ' as total_revenue,\n'
                'ROUND(AVG(CAST(`line_items_price` AS FLOAT64)), 2) as avg_unit_price,\n'
                'COUNT(*) as total_orders,\n'
                'ROUND(SUM(CAST(`line_items_quantity` AS INT64) * CAST(`line_items_price` AS FLOAT64)) /'
                ' SUM(CAST(`line_items_quantity` AS INT64)), 2) as revenue_per_unit,\n'
                'MIN(`created_at`) as first_sale_date,\n'
                'MAX(`updated_at`) as last_sale_date,\n'
                'CURRENT_TIMESTAMP() as report_generated_at\n'
                'FROM `in.c-kds-team-ex-shopify-01k368x27c4gpd4k5v0nwmcn98.orders`\n'
                '-- comment\n'
                "WHERE `financial_status` IN ('paid', 'partially_paid')\n"
                'AND `line_items_product_id` IS NOT NULL\n'
                'GROUP BY `line_items_product_id`, `line_items_title`\n'
                'QUALIFY revenue_rank <= 20\n'
                'ORDER BY revenue_rank'
            ),
            [
                (
                    'CREATE TABLE `top_20_products_revenue` AS\n'
                    'SELECT\n'
                    '/* comment */\n'
                    'ROW_NUMBER() OVER (ORDER BY SUM(CAST(`line_items_quantity` AS INT64)'
                    ' * CAST(`line_items_price` AS FLOAT64)) DESC) as revenue_rank,\n'
                    '`line_items_product_id` as product_id,\n'
                    '`line_items_title` as product_title,\n'
                    'COUNT(DISTINCT `line_items_variant_id`) as variant_count,\n'
                    'SUM(CAST(`line_items_quantity` AS INT64)) as total_quantity_sold,\n'
                    'ROUND(SUM(CAST(`line_items_quantity` AS INT64) * CAST(`line_items_price` AS FLOAT64)), 2)'
                    ' as total_revenue,\n'
                    'ROUND(AVG(CAST(`line_items_price` AS FLOAT64)), 2) as avg_unit_price,\n'
                    'COUNT(*) as total_orders,\n'
                    'ROUND(SUM(CAST(`line_items_quantity` AS INT64) * CAST(`line_items_price` AS FLOAT64)) /'
                    ' SUM(CAST(`line_items_quantity` AS INT64)), 2) as revenue_per_unit,\n'
                    'MIN(`created_at`) as first_sale_date,\n'
                    'MAX(`updated_at`) as last_sale_date,\n'
                    'CURRENT_TIMESTAMP() as report_generated_at\n'
                    'FROM `in.c-kds-team-ex-shopify-01k368x27c4gpd4k5v0nwmcn98.orders`\n'
                    '-- comment\n'
                    "WHERE `financial_status` IN ('paid', 'partially_paid')\n"
                    'AND `line_items_product_id` IS NOT NULL\n'
                    'GROUP BY `line_items_product_id`, `line_items_title`\n'
                    'QUALIFY revenue_rank <= 20\n'
                    'ORDER BY revenue_rank'
                )
            ],
            1.0,
            'complex_create_table_with_division_and_dashes',
        ),
        # Empty strings (single and double quotes)
        (
            "SELECT '' AS empty1, \"\" AS empty2; SELECT 2;",
            ["SELECT '' AS empty1, \"\" AS empty2;", 'SELECT 2;'],
            1.0,
            'empty_quoted_strings',
        ),
        # Strings with escaped backslashes
        (
            "SELECT 'C:\\\\path\\\\to\\\\file' AS path; SELECT 2;",
            ["SELECT 'C:\\\\path\\\\to\\\\file' AS path;", 'SELECT 2;'],
            1.0,
            'strings_with_escaped_backslashes',
        ),
        # Double-quoted strings with escaped quotes
        (
            'SELECT "test\\"quoted\\"value" AS col; SELECT 2;',
            ['SELECT "test\\"quoted\\"value" AS col;', 'SELECT 2;'],
            1.0,
            'double_quoted_with_escaped_quotes',
        ),
        # Strings containing newlines
        (
            "SELECT 'line1\nline2\nline3' AS multiline; SELECT 2;",
            ["SELECT 'line1\nline2\nline3' AS multiline;", 'SELECT 2;'],
            1.0,
            'strings_with_newlines',
        ),
        # Multiple consecutive escaped quotes
        (
            "SELECT 'test\\'\\'\\'value' AS col; SELECT 2;",
            ["SELECT 'test\\'\\'\\'value' AS col;", 'SELECT 2;'],
            1.0,
            'multiple_escaped_quotes',
        ),
        # Multi-line comment with only asterisks
        (
            'SELECT 1; /* **** */ SELECT 2;',
            ['SELECT 1;', '/* **** */ SELECT 2;'],
            1.0,
            'block_comment_all_asterisks',
        ),
        # Empty multi-line comment
        (
            'SELECT 1; /**/ SELECT 2;',
            ['SELECT 1;', '/**/ SELECT 2;'],
            1.0,
            'empty_block_comment',
        ),
        # Multi-line comment with asterisks in middle
        (
            'SELECT 1; /* comment with *** asterisks */ SELECT 2;',
            ['SELECT 1;', '/* comment with *** asterisks */ SELECT 2;'],
            1.0,
            'block_comment_with_asterisks',
        ),
        # Comments that look like they might be nested (but aren't)
        (
            'SELECT 1; /* comment /* not nested */ */ SELECT 2;',
            ['SELECT 1;', '/* comment /* not nested */ */ SELECT 2;'],
            1.0,
            'block_comment_pseudo_nested',
        ),
        # Dollar-quoted block with special characters
        (
            'SELECT 1; $$ SELECT "test"; -- comment; $$; SELECT 2;',
            ['SELECT 1;', '$$ SELECT "test"; -- comment; $$;', 'SELECT 2;'],
            1.0,
            'dollar_quoted_with_special_chars',
        ),
        # Dollar-quoted block at start
        (
            '$$ SELECT 1; $$; SELECT 2;',
            ['$$ SELECT 1; $$;', 'SELECT 2;'],
            1.0,
            'dollar_quoted_at_start',
        ),
        # Multiple dollar signs in a row (not dollar quotes)
        (
            'SELECT $1, $2, $3; SELECT 2;',
            ['SELECT $1, $2, $3;', 'SELECT 2;'],
            1.0,
            'multiple_dollar_signs',
        ),
        # Division operator in arithmetic
        (
            'SELECT 10 / 2 AS result; SELECT 20 / 4;',
            ['SELECT 10 / 2 AS result;', 'SELECT 20 / 4;'],
            1.0,
            'division_operators',
        ),
        # Negative numbers
        (
            'SELECT -1, -2.5, -10 / 2; SELECT 2;',
            ['SELECT -1, -2.5, -10 / 2;', 'SELECT 2;'],
            1.0,
            'negative_numbers',
        ),
        # Table name with dash (not a comment)
        (
            'SELECT * FROM table-name; SELECT 2;',
            ['SELECT * FROM table-name;', 'SELECT 2;'],
            1.0,
            'table_name_with_dash',
        ),
        # Comment at start of statement
        (
            '-- Leading comment\nSELECT 1; SELECT 2;',
            ['-- Leading comment\nSELECT 1;', 'SELECT 2;'],
            1.0,
            'comment_at_start',
        ),
        # Comment at end of statement
        (
            'SELECT 1; -- Trailing comment\nSELECT 2;',
            ['SELECT 1;', '-- Trailing comment\nSELECT 2;'],
            1.0,
            'comment_at_end',
        ),
        # Hash comment at start
        (
            '# Leading hash comment\nSELECT 1; SELECT 2;',
            ['# Leading hash comment\nSELECT 1;', 'SELECT 2;'],
            1.0,
            'hash_comment_at_start',
        ),
        # C-style comment at start
        (
            '// Leading slash comment\nSELECT 1; SELECT 2;',
            ['// Leading slash comment\nSELECT 1;', 'SELECT 2;'],
            1.0,
            'slash_comment_at_start',
        ),
        # Multiple consecutive statements with comments
        (
            'SELECT 1; /* comment */ SELECT 2; -- comment\nSELECT 3;',
            ['SELECT 1;', '/* comment */ SELECT 2;', '-- comment\nSELECT 3;'],
            1.0,
            'multiple_statements_with_comments',
        ),
        # String containing comment-like text
        (
            "SELECT '-- not a comment' AS col; SELECT 2;",
            ["SELECT '-- not a comment' AS col;", 'SELECT 2;'],
            1.0,
            'string_with_comment_like_text',
        ),
        # String containing hash
        (
            "SELECT 'price #123' AS col; SELECT 2;",
            ["SELECT 'price #123' AS col;", 'SELECT 2;'],
            1.0,
            'string_with_hash',
        ),
        # String containing slashes
        (
            "SELECT 'path/to/file' AS col; SELECT 2;",
            ["SELECT 'path/to/file' AS col;", 'SELECT 2;'],
            1.0,
            'string_with_slashes',
        ),
        # String containing dollar signs
        (
            "SELECT 'cost $100' AS col; SELECT 2;",
            ["SELECT 'cost $100' AS col;", 'SELECT 2;'],
            1.0,
            'string_with_dollar_signs',
        ),
        # Mixed quotes and comments
        (
            "SELECT 'single' AS s, \"double\" AS d; -- comment\nSELECT 2;",
            ["SELECT 'single' AS s, \"double\" AS d;", '-- comment\nSELECT 2;'],
            1.0,
            'mixed_quotes_and_comments',
        ),
        # Statement with only whitespace before semicolon
        (
            'SELECT 1   ;   SELECT 2;',
            ['SELECT 1   ;', 'SELECT 2;'],
            1.0,
            'whitespace_before_semicolon',
        ),
        # Statement with tabs and spaces
        (
            '\tSELECT 1;\tSELECT 2;\nSELECT 3;',
            ['SELECT 1;', 'SELECT 2;', 'SELECT 3;'],
            1.0,
            'tabs_and_spaces',
        ),
        # Multiple semicolons (should split)
        (
            'SELECT 1;; SELECT 2;',
            ['SELECT 1;', 'SELECT 2;'],
            1.0,
            'multiple_semicolons',
        ),
        # Unicode characters in strings
        (
            "SELECT 'café' AS name, '🚀' AS emoji; SELECT 2;",
            ["SELECT 'café' AS name, '🚀' AS emoji;", 'SELECT 2;'],
            1.0,
            'unicode_in_strings',
        ),
        # Unicode characters in SQL
        (
            'SELECT 1; SELECT 2; -- café comment',
            ['SELECT 1;', 'SELECT 2;', '-- café comment'],
            1.0,
            'unicode_in_comments',
        ),
        # Very long statement (tests performance)
        (
            'SELECT ' + 'x' * 1000 + '; SELECT 2;',
            ['SELECT ' + 'x' * 1000 + ';', 'SELECT 2;'],
            1.0,
            'very_long_statement',
        ),
        # Statement with only a comment
        (
            '-- Only comment\nSELECT 1;',
            ['-- Only comment\nSELECT 1;'],
            1.0,
            'comment_only_statement',
        ),
        # Block comment only statement
        (
            '/* Only comment */\nSELECT 1;',
            ['/* Only comment */\nSELECT 1;'],
            1.0,
            'block_comment_only_statement',
        ),
        # Multiple block comments
        (
            'SELECT 1; /* comment1 */ SELECT 2; /* comment2 */ SELECT 3;',
            ['SELECT 1;', '/* comment1 */ SELECT 2;', '/* comment2 */ SELECT 3;'],
            1.0,
            'multiple_block_comments',
        ),
        # Dollar-quoted with nested dollar signs
        (
            'SELECT 1; $$ SELECT $variable; $$; SELECT 2;',
            ['SELECT 1;', '$$ SELECT $variable; $$;', 'SELECT 2;'],
            1.0,
            'dollar_quoted_with_nested_dollar',
        ),
        # Complex arithmetic with division and subtraction
        (
            'SELECT (100 - 20) / 2 AS result; SELECT 2;',
            ['SELECT (100 - 20) / 2 AS result;', 'SELECT 2;'],
            1.0,
            'complex_arithmetic',
        ),
        # Mixed line endings
        (
            'SELECT 1;\rSELECT 2;\nSELECT 3;\r\nSELECT 4;',
            ['SELECT 1;', 'SELECT 2;', 'SELECT 3;', 'SELECT 4;'],
            1.0,
            'mixed_line_endings',
        ),
        # Comment with Windows line ending
        (
            'SELECT 1; -- comment\r\nSELECT 2;',
            ['SELECT 1;', '-- comment\r\nSELECT 2;'],
            1.0,
            'comment_with_crlf',
        ),
        # Hash comment with Windows line ending
        (
            'SELECT 1; # comment\r\nSELECT 2;',
            ['SELECT 1;', '# comment\r\nSELECT 2;'],
            1.0,
            'hash_comment_with_crlf',
        ),
        # Multiple statements without semicolons
        (
            'SELECT 1\nSELECT 2\nSELECT 3',
            ['SELECT 1\nSELECT 2\nSELECT 3'],
            1.0,
            'multiple_statements_no_semicolons',
        ),
        # Statement with comment and no semicolon
        (
            'SELECT 1 -- comment\nSELECT 2',
            ['SELECT 1 -- comment\nSELECT 2'],
            1.0,
            'statement_with_comment_no_semicolon',
        ),
        # Empty string between statements
        (
            'SELECT 1;\n\n\nSELECT 2;',
            ['SELECT 1;', 'SELECT 2;'],
            1.0,
            'empty_lines_between_statements',
        ),
        # Statement starting with whitespace
        (
            '   SELECT 1;   SELECT 2;',
            ['SELECT 1;', 'SELECT 2;'],
            1.0,
            'leading_whitespace',
        ),
        # Block comment spanning multiple lines with complex content
        (
            'SELECT 1; /*\n * Multi-line\n * comment\n * with stars\n */ SELECT 2;',
            ['SELECT 1;', '/*\n * Multi-line\n * comment\n * with stars\n */ SELECT 2;'],
            1.0,
            'multi_line_block_comment_with_stars',
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
            'SELECT 1\n\n',
            'single_statement',
        ),
        # Multiple statements
        (
            ['SELECT 1', 'SELECT 2', 'SELECT 3'],
            'SELECT 1\n\nSELECT 2\n\nSELECT 3\n\n',
            'multiple_statements',
        ),
        # Preserve existing semicolons
        (
            ['SELECT 1;', 'SELECT 2;'],
            'SELECT 1;\n\nSELECT 2;\n\n',
            'existing_semicolons',
        ),
        # Mixed statements (with and without semicolons)
        (
            ['SELECT 1;', 'SELECT 2', 'SELECT 3'],
            'SELECT 1;\n\nSELECT 2\n\nSELECT 3\n\n',
            'mixed_statements',
        ),
        # Filter empty statements
        (
            ['SELECT 1', '', '  ', 'SELECT 2'],
            'SELECT 1\n\nSELECT 2\n\n',
            'filter_empty_statements',
        ),
        # Preserve internal whitespace
        (
            ['SELECT  \n  1'],
            'SELECT  \n  1\n\n',
            'preserve_whitespace',
        ),
        # Statement with trailing whitespace
        (
            ['SELECT 1;   ', 'SELECT 2  '],
            'SELECT 1;\n\nSELECT 2\n\n',
            'trailing_whitespace',
        ),
        # Multiple trailing spaces and tabs
        (
            ['SELECT 1  \t  ', '  \t SELECT 2'],
            'SELECT 1\n\nSELECT 2\n\n',
            'mixed_whitespace',
        ),
        # Multiple empty strings (should be filtered)
        (
            ['SELECT 1', '', '   ', '\t'],
            'SELECT 1\n\n',
            'with_multiple_empty',
        ),
        # Pure comment statement (comments are preserved like any other statement)
        (
            ['-- This is just a comment', 'SELECT 1'],
            '-- This is just a comment\n\nSELECT 1\n\n',
            'pure_comment_statement',
        ),
        # Multi-line statement with comments (preserved as-is)
        (
            ['SELECT a -- comment 1\n, b -- comment 2\nFROM table'],
            'SELECT a -- comment 1\n, b -- comment 2\nFROM table\n\n',
            'multiline_with_comments',
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


@pytest.mark.parametrize(
    ('input_sql', 'dialect', 'expected', 'test_id'),
    [
        # Pure comment - should not get a semicolon
        (
            '-- This is a comment',
            'snowflake',
            '-- This is a comment',
            'pure_line_comment',
        ),
        # Pure block comment - should not get a semicolon
        (
            '/* This is a comment */',
            'snowflake',
            '/* This is a comment */',
            'pure_block_comment',
        ),
        # Single statement without semicolon - should add semicolon
        (
            'SELECT 1',
            'snowflake',
            'SELECT\n  1;',
            'single_statement_no_semicolon',
        ),
        # Single statement with semicolon - should preserve semicolon
        (
            'SELECT 1;',
            'snowflake',
            'SELECT\n  1;',
            'single_statement_with_semicolon',
        ),
        # Multiple statements without semicolons - should add semicolons
        (
            'SELECT 1; SELECT 2',
            'snowflake',
            'SELECT\n  1;\n\nSELECT\n  2;',
            'multiple_statements_no_semicolons',
        ),
        # Multiple statements with semicolons - should preserve semicolons
        (
            'SELECT 1; SELECT 2;',
            'snowflake',
            'SELECT\n  1;\n\nSELECT\n  2;',
            'multiple_statements_with_semicolons',
        ),
        # Statements with inline comment - comment preserved, semicolon added
        (
            'SELECT 1;\n-- comment\nSELECT 2',
            'snowflake',
            'SELECT\n  1;\n\n/* comment */\nSELECT\n  2;',
            'statements_with_inline_comment',
        ),
        # Statements with inline comment - comment preserved, semicolon added
        (
            'SELECT 1;\n// comment\nSELECT 2',
            'snowflake',
            'SELECT\n  1;\n\n/* comment */\nSELECT\n  2;',
            'statements_with_inline_comment_cpp',
        ),
        # Complex statement - properly formatted with semicolon
        (
            'SELECT a, b FROM table WHERE x > 10',
            'snowflake',
            'SELECT\n  a,\n  b\nFROM table\nWHERE\n  x > 10;',
            'complex_statement',
        ),
        # BigQuery dialect tests
        # Pure comment - should not get a semicolon
        (
            '-- This is a comment',
            'bigquery',
            '-- This is a comment',
            'bigquery_pure_line_comment',
        ),
        # Pure block comment - should not get a semicolon
        (
            '/* This is a comment */',
            'bigquery',
            '/* This is a comment */',
            'bigquery_pure_block_comment',
        ),
        # Single statement without semicolon - should add semicolon
        (
            'SELECT 1',
            'bigquery',
            'SELECT\n  1;',
            'bigquery_single_statement_no_semicolon',
        ),
        # Single statement with semicolon - should preserve semicolon
        (
            'SELECT 1;',
            'bigquery',
            'SELECT\n  1;',
            'bigquery_single_statement_with_semicolon',
        ),
        # Multiple statements - should add semicolons
        (
            'SELECT 1; SELECT 2',
            'bigquery',
            'SELECT\n  1;\n\nSELECT\n  2;',
            'bigquery_multiple_statements',
        ),
        # Statements with inline comment - comment preserved, semicolon added
        (
            'SELECT 1;\n-- comment\nSELECT 2',
            'bigquery',
            'SELECT\n  1;\n\n/* comment */\nSELECT\n  2;',
            'bigquery_statements_with_inline_comment',
        ),
        # Complex statement - properly formatted with semicolon
        (
            'SELECT a, b FROM table WHERE x > 10',
            'bigquery',
            'SELECT\n  a,\n  b\nFROM table\nWHERE\n  x > 10;',
            'bigquery_complex_statement',
        ),
        # BigQuery-specific: backtick-quoted identifiers
        (
            'SELECT * FROM `project.dataset.table` WHERE id > 10',
            'bigquery',
            'SELECT\n  *\nFROM `project.dataset.table`\nWHERE\n  id > 10;',
            'bigquery_backtick_identifiers',
        ),
        # BigQuery-specific: STRUCT syntax
        (
            'SELECT STRUCT(1 AS a, 2 AS b) AS my_struct',
            'bigquery',
            'SELECT\n  STRUCT(1 AS a, 2 AS b) AS my_struct;',
            'bigquery_struct_syntax',
        ),
        # BigQuery-specific: ARRAY syntax (preserved as-is)
        (
            'SELECT [1, 2, 3] AS my_array',
            'bigquery',
            'SELECT\n  [1, 2, 3] AS my_array;',
            'bigquery_array_syntax',
        ),
    ],
)
def test_format_sql(input_sql, dialect, expected, test_id):
    """Test SQL formatting with semicolon and comment handling."""
    result = format_sql(input_sql, dialect)
    assert result == expected


@pytest.mark.parametrize(
    ('input_sql', 'dialect', 'test_id'),
    [
        # Invalid dialect handling
        (
            'SELECT 1',
            'invalid_dialect_name',
            'invalid_dialect',
        ),
        (
            'SELECT * FROM table',
            'nonexistent_sql_dialect',
            'nonexistent_dialect',
        ),
        # SQL that sqlglot cannot parse
        (
            'SELECT FROM WHERE',
            'snowflake',
            'malformed_sql_missing_table',
        ),
        (
            'SELECT * FROM',
            'bigquery',
            'malformed_sql_incomplete',
        ),
        (
            'SELECT * FROM table WHERE x =',
            'snowflake',
            'malformed_sql_incomplete_where',
        ),
        (
            'CREATE TABLE (id INT)',
            'bigquery',
            'malformed_sql_missing_table_name',
        ),
        (
            'SELECT * FROM table WHERE x = (SELECT',
            'snowflake',
            'malformed_sql_unclosed_subquery',
        ),
        (
            'SELECT * FROM table WHERE x = "unclosed string',
            'bigquery',
            'malformed_sql_unclosed_string',
        ),
        (
            "SELECT * FROM table WHERE x = 'unclosed string",
            'snowflake',
            'malformed_sql_unclosed_single_quote',
        ),
        (
            'SELECT * FROM table WHERE x = /* unclosed comment',
            'bigquery',
            'malformed_sql_unclosed_comment',
        ),
        (
            'SELECT * FROM table WHERE x = $$ unclosed dollar quote',
            'snowflake',
            'malformed_sql_unclosed_dollar_quote',
        ),
        # Unsupported comment: '#'
        (
            'SELECT 1;\n-- comment1\nSELECT 2;\n# comment2\nSELECT 3;\n// comment3',
            'snowflake',
            'unsupported_comment_hash',
        ),
        # Empty strings and whitespace-only input
        (
            '',
            'snowflake',
            'empty_string',
        ),
        (
            '',
            'bigquery',
            'empty_string_bigquery',
        ),
        (
            '   ',
            'snowflake',
            'whitespace_only_spaces',
        ),
        (
            '\t\t',
            'bigquery',
            'whitespace_only_tabs',
        ),
        (
            '\n\n\n',
            'snowflake',
            'whitespace_only_newlines',
        ),
        (
            ' \t\n\r ',
            'bigquery',
            'whitespace_only_mixed',
        ),
    ],
)
def test_format_sql_error(input_sql, dialect, test_id):
    result = format_sql(input_sql, dialect)
    # On error, format_sql returns the original SQL unchanged
    assert result == input_sql
