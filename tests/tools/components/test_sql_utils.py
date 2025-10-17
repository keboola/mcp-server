"""
Tests for SQL splitting and joining utilities.

Ported from the Keboola UI's splitSqlQueries.test.ts to ensure
the Python implementation matches the production-proven JavaScript logic.
"""

from keboola_mcp_server.tools.components.sql_utils import (
    join_sql_statements,
    split_sql_statements,
    validate_round_trip,
)


class TestSplitSqlStatements:
    """Test SQL splitting functionality."""

    def test_split_simple_queries(self):
        """Should properly split simple SQL queries."""
        result = split_sql_statements(
            """
SELECT 1;
Select 2;
SELECT 3;"""
        )
        assert result == ['SELECT 1;', 'Select 2;', 'SELECT 3;']

    def test_split_with_multi_line_comments(self):
        """Should support multi-line comments with /* */ syntax."""
        result = split_sql_statements(
            """
SELECT 1;
/*
  Select 2;
*/
SELECT 3;"""
        )
        assert result == ['SELECT 1;', '/*\n  Select 2;\n*/\nSELECT 3;']

    def test_split_with_single_line_comments_double_dash(self):
        """Should support single line comments with -- syntax."""
        result = split_sql_statements(
            """
SELECT 1;
-- Select 2;
SELECT 3;"""
        )
        assert result == ['SELECT 1;', '-- Select 2;\nSELECT 3;']

    def test_split_with_single_line_comments_hash(self):
        """Should support single line comments with # syntax."""
        result = split_sql_statements(
            """
SELECT 1;
# Select 2;
SELECT 3;"""
        )
        assert result == ['SELECT 1;', '# Select 2;\nSELECT 3;']

    def test_split_with_single_line_comments_double_slash(self):
        """Should support single line comments with // syntax."""
        result = split_sql_statements(
            """
SELECT 1;
// Select 2;
SELECT 3;"""
        )
        assert result == ['SELECT 1;', '// Select 2;\nSELECT 3;']

    def test_split_with_dollar_quoted_blocks(self):
        """Should support multi-line code with $$ syntax."""
        result = split_sql_statements(
            """
SELECT 1;
execute immediate $$
  SELECT 2;
  SELECT 3;
$$;"""
        )
        assert result == ['SELECT 1;', 'execute immediate $$\n  SELECT 2;\n  SELECT 3;\n$$;']

    def test_split_empty_string(self):
        """Should handle empty strings."""
        assert split_sql_statements('') == []
        assert split_sql_statements('   ') == []

    def test_split_single_statement_no_semicolon(self):
        """Should handle single statement without semicolon."""
        result = split_sql_statements('SELECT 1')
        assert result == ['SELECT 1']

    def test_split_single_statement_with_semicolon(self):
        """Should handle single statement with semicolon."""
        result = split_sql_statements('SELECT 1;')
        assert result == ['SELECT 1;']

    def test_split_with_semicolons_in_strings(self):
        """Should not split on semicolons inside string literals."""
        result = split_sql_statements("SELECT 'test;test' AS col1; SELECT 2;")
        assert result == ["SELECT 'test;test' AS col1;", 'SELECT 2;']

    def test_split_with_double_quoted_strings(self):
        """Should handle double-quoted strings."""
        result = split_sql_statements('SELECT "test;test" AS col1; SELECT 2;')
        assert result == ['SELECT "test;test" AS col1;', 'SELECT 2;']

    def test_split_with_escaped_quotes(self):
        """Should handle escaped quotes in strings."""
        result = split_sql_statements("SELECT 'it\\'s a test'; SELECT 2;")
        assert result == ["SELECT 'it\\'s a test';", 'SELECT 2;']

    def test_split_complex_query_with_timeout(self):
        """Should parse a complex query without timing out (sanity check)."""
        complex_query = (
            'SELECT 1;\n'
            '-- Comment line\n'
            'execute immediate $$\n'
            '  SELECT 2;\n'
            "  SELECT 'value;still string';\n"
            '$$;\n'
            'SELECT 3;\n'
            '-- Another comment\n'
            'SELECT "double" as col;\n'
        )
        result = split_sql_statements(complex_query, timeout_seconds=2.0)
        assert isinstance(result, list)
        assert len(result) >= 3


class TestJoinSqlStatements:
    """Test SQL joining functionality."""

    def test_join_empty_list(self):
        """Should handle empty list."""
        assert join_sql_statements([]) == ''

    def test_join_single_statement(self):
        """Should join single statement with semicolon."""
        result = join_sql_statements(['SELECT 1'])
        assert result == 'SELECT 1;\n\n'

    def test_join_multiple_statements(self):
        """Should join multiple statements with semicolons."""
        result = join_sql_statements(['SELECT 1', 'SELECT 2', 'SELECT 3'])
        assert result == 'SELECT 1;\n\nSELECT 2;\n\nSELECT 3;\n\n'

    def test_join_statements_with_existing_semicolons(self):
        """Should preserve existing semicolons."""
        result = join_sql_statements(['SELECT 1;', 'SELECT 2;'])
        assert result == 'SELECT 1;\n\nSELECT 2;\n\n'

    def test_join_statements_ending_with_comments(self):
        """Should not add semicolon after comments."""
        result = join_sql_statements(['SELECT 1 -- comment', 'SELECT 2'])
        assert result == 'SELECT 1 -- comment\n\nSELECT 2;\n\n'

    def test_join_statements_ending_with_block_comments(self):
        """Should not add semicolon after block comments."""
        result = join_sql_statements(['SELECT 1 /* comment */', 'SELECT 2'])
        assert result == 'SELECT 1 /* comment */\n\nSELECT 2;\n\n'

    def test_join_mixed_statements(self):
        """Should handle mixed statements correctly."""
        result = join_sql_statements([
            'SELECT 1;',
            'SELECT 2',
            'SELECT 3 -- comment',
            'SELECT 4'
        ])
        assert result == 'SELECT 1;\n\nSELECT 2;\n\nSELECT 3 -- comment\n\nSELECT 4;\n\n'

    def test_join_filters_empty_statements(self):
        """Should filter out empty statements."""
        result = join_sql_statements(['SELECT 1', '', '  ', 'SELECT 2'])
        assert result == 'SELECT 1;\n\nSELECT 2;\n\n'


class TestRoundTripValidation:
    """Test round-trip validation functionality."""

    def test_round_trip_simple_queries(self):
        """Should validate round-trip for simple queries."""
        original = 'SELECT 1;\nSELECT 2;\nSELECT 3;'
        assert validate_round_trip(original) is True

    def test_round_trip_with_comments(self):
        """Should validate round-trip with comments."""
        original = 'SELECT 1;\n-- comment\nSELECT 2;'
        assert validate_round_trip(original) is True

    def test_round_trip_with_dollar_quotes(self):
        """Should validate round-trip with dollar-quoted blocks."""
        original = 'SELECT 1;\nexecute immediate $$\n  SELECT 2;\n$$;'
        assert validate_round_trip(original) is True

    def test_round_trip_complex_sql(self):
        """Should validate round-trip for complex SQL."""
        original = """CREATE TABLE test (id INT);
INSERT INTO test VALUES (1);
SELECT * FROM test WHERE name = 'test;test';"""
        assert validate_round_trip(original) is True


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_split_with_nested_dollar_quotes(self):
        """Should handle nested dollar-quoted blocks."""
        result = split_sql_statements('CREATE FUNCTION f() $$ SELECT $$nested$$; $$;')
        assert len(result) == 1

    def test_split_with_mixed_quotes(self):
        """Should handle mixed single and double quotes."""
        result = split_sql_statements("SELECT 'single', \"double\"; SELECT 2;")
        assert result == ["SELECT 'single', \"double\";", 'SELECT 2;']

    def test_join_preserves_whitespace_in_statements(self):
        """Should preserve internal whitespace in statements."""
        result = join_sql_statements(['SELECT  \n  1'])
        assert 'SELECT  \n  1' in result

    def test_split_handles_carriage_returns(self):
        """Should handle Windows-style line endings."""
        result = split_sql_statements('SELECT 1;\r\nSELECT 2;\r\n')
        assert result == ['SELECT 1;', 'SELECT 2;']
