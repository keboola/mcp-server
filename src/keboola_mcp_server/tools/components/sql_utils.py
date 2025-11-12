"""
SQL splitting and joining utilities for SQL transformations.

This module provides functionality to split SQL scripts into individual
statements and join them back together, using regex-based parsing similar
to the Keboola UI's splitQueriesWorker.worker.ts implementation.
"""

import asyncio
import logging
import re
from typing import Iterable

import sqlglot

from keboola_mcp_server.tools.components.model import SimplifiedTfBlocks

LOG = logging.getLogger(__name__)

SQL_SPLIT_REGEX = re.compile(
    r'\s*('
    r'(?:'  # Start non-capturing group for alternatives
    r"'[^'\\]*(?:\\.[^'\\]*)*'|"  # Single-quoted strings
    r'"[^"\\]*(?:\\.[^"\\]*)*"|'  # Double-quoted strings
    r'\$\$(?:(?!\$\$)[\s\S])*\$\$|'  # Multi-line blocks $$...$$ (using [\s\S] for any char)
    r'/\*[^*]*\*+(?:[^*/][^*]*\*+)*/|'  # Multi-line comments /* ... */
    r'#[^\n\r]*|'  # Hash comments
    r'--[^\n\r]*|'  # SQL comments
    r'//[^\n\r]*|'  # C-style comments
    r'/(?![*/])|'  # Division operator: / not followed by * or /
    r'-(?!-)|'  # Dash/minus: - not followed by another -
    r'\$(?!\$)|'  # Dollar sign: $ not followed by another $
    r'[^"\';#/$-]+'  # Everything else except special chars (greedy match for performance)
    r')+'  # End non-capturing group, one or more times
    r'(?:;|$)'  # Statement terminator: semicolon or end
    r')',  # End capturing group
    re.MULTILINE,
)

# Regex for detecting line comments (single-line style: --, //, #)
LINE_COMMENT_REGEX = re.compile(r'(--|//|#).*$')

# Regex patterns for parsing block/code structure markers
BLOCK_MARKER_REGEX = re.compile(r'/\*\s*=+\s*BLOCK:\s*([^*]+?)\s*=+\s*\*/', re.MULTILINE)
CODE_MARKER_REGEX = re.compile(r'/\*\s*=+\s*CODE:\s*([^*]+?)\s*=+\s*\*/', re.MULTILINE)


async def split_sql_statements(script: str, timeout_seconds: float = 1.0) -> list[str]:
    """
    Splits a SQL script string into individual statements.

    Uses regex-based parsing similar to UI's splitQueriesWorker.worker.ts.
    Includes timeout protection to prevent catastrophic backtracking.

    :param script: The SQL script string to split
    :param timeout_seconds: Maximum time to allow for regex processing
        (default: 1.0)
    :return: List of individual SQL statements (trimmed, non-empty)
    :raises ValueError: If the script is invalid or regex times out
    """
    if not script or not script.strip():
        return []

    try:
        try:
            statements = await asyncio.wait_for(asyncio.to_thread(_split_with_regex, script), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            raise ValueError(
                f'SQL parsing took too long (possible catastrophic backtracking). Timeout: {timeout_seconds}s'
            )

        if statements is None:
            raise ValueError('SQL script is not valid (no matches found)')

        normalized = [stmt.strip() for stmt in statements if stmt.strip()]

        return normalized

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        LOG.exception(f'Failed to split SQL statements: {e}')
        raise ValueError(f'Failed to parse SQL script: {e}')


def _split_with_regex(script: str) -> list[str]:
    """
    Internal function to split SQL using regex.

    This is separated to allow timeout handling in the calling function.

    :param script: The SQL script string to split
    :return: List of matched statement strings (may include empty strings)
    """
    matches = SQL_SPLIT_REGEX.findall(script)
    return matches if matches else []


def join_sql_statements(statements: Iterable[str]) -> str:
    """
    Joins SQL statements into a single script string.

    :param statements: List of SQL statements to join
    :return: Concatenated SQL script string separated by double newlines
    """
    if not statements:
        return ''

    result_parts = []

    for stmt in statements:
        trimmed_stmt = stmt.strip()
        if not trimmed_stmt:
            continue

        result_parts.append(trimmed_stmt)
        result_parts.append('\n\n')

    return ''.join(result_parts)


def format_sql(sql: str, dialect: str) -> str:
    """
    Formats SQL code using sqlglot for better readability.

    :param sql: Raw SQL code (may contain multiple statements)
    :param dialect: SQL dialect
    :return: Formatted SQL code, or original if formatting fails
    """
    try:
        # transpile returns a list - one item per statement/comment
        formatted_items = sqlglot.transpile(sql, read=dialect.lower(), pretty=True)

        if not formatted_items:
            return sql

        def process_item(item: str) -> str | None:
            """Process a single formatted item, returning None if it should be skipped."""
            item = item.rstrip()
            if not item:
                return None

            # Check if it's ONLY a comment (no SQL after it)
            # Remove block comments and line comments, then check if anything substantial remains
            sql_content = re.sub(r'/\*.*?\*/', '', item, flags=re.DOTALL)
            sql_content = re.sub(r'(--.*)$', '', sql_content, flags=re.MULTILINE).strip()

            is_only_comment = not sql_content

            # Add semicolon only to actual SQL statements (not pure comments)
            if not is_only_comment and not item.endswith(';'):
                item += ';'

            return item

        result = [processed for item in formatted_items if (processed := process_item(item)) is not None]

        if not result:
            return sql

        # Join with double newlines (consistent with join_sql_statements)
        return '\n\n'.join(result)
    except Exception as e:
        LOG.warning(f'Failed to format SQL statement in {dialect} dialect: {sql}. Error: {e}')
        return sql


def format_simplified_tf_code(
    code: SimplifiedTfBlocks.Block.Code, dialect: str
) -> tuple[SimplifiedTfBlocks.Block.Code, bool]:
    """
    Formats the simplified transformation code using sqlglot for better readability.

    :param code: The simplified transformation code
    :param dialect: SQL dialect ('snowflake' or 'bigquery')
    :return: Tuple of (formatted simplified transformation code,
      bool representing if the script was changed by formatting)
    """
    formatted_script = format_sql(sql=code.script, dialect=dialect)

    return SimplifiedTfBlocks.Block.Code(name=code.name, script=formatted_script), formatted_script != code.script


def format_simplified_tf_block(block: SimplifiedTfBlocks.Block, dialect: str) -> tuple[SimplifiedTfBlocks.Block, bool]:
    """
    Formats the simplified transformation block using sqlglot for better readability.

    :param block: The simplified transformation block
    :param dialect: SQL dialect ('snowflake' or 'bigquery')
    :return: Tuple of (formatted simplified transformation block,
      bool representing if the block was changed by formatting)
    """
    formatted_codes = []
    is_changed = False
    for code in block.codes:
        formatted_code, is_changed_code = format_simplified_tf_code(code=code, dialect=dialect)
        is_changed = is_changed or is_changed_code
        formatted_codes.append(formatted_code)
    return SimplifiedTfBlocks.Block(name=block.name, codes=formatted_codes), is_changed
