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

from keboola_mcp_server.tools.components.model import TransformationConfiguration


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
    Split a SQL script string into individual statements.

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
                f'SQL parsing took too long ' f'(possible catastrophic backtracking). ' f'Timeout: {timeout_seconds}s'
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
    Join SQL statements into a single script string.

    :param statements: List of SQL statements to join
    :return: Concatenated SQL script string with proper delimiters
    """
    if not statements:
        return ''

    result_parts = []

    for stmt in statements:
        trimmed_stmt = stmt.strip()
        if not trimmed_stmt:
            continue

        ends_with_semicolon = trimmed_stmt.endswith(';')

        # Check if ends with block comment or line comment at the end of the string
        ends_with_comment = trimmed_stmt.endswith('*/') or bool(re.search(r'(--|//|#)[^\n]*$', trimmed_stmt))

        result_parts.append(trimmed_stmt)
        if not ends_with_semicolon and not ends_with_comment:
            result_parts.append(';')

        result_parts.append('\n\n')

    return ''.join(result_parts)


def format_sql_statement(sql: str, dialect: str) -> str:
    """
    Format SQL statement using sqlglot for better readability.

    :param sql: Raw SQL statement string
    :param dialect: SQL dialect ('snowflake' or 'bigquery')
    :return: Formatted SQL string, or original if formatting fails
    """
    try:
        formatted = sqlglot.transpile(sql, read=dialect, pretty=True)[0]
        return formatted
    except Exception:
        return sql


def format_code_blocks(
    code_blocks: Iterable[TransformationConfiguration.Parameters.Block.Code],
    dialect: str,
    conditional: bool = False,
) -> list[TransformationConfiguration.Parameters.Block.Code]:
    """
    Format SQL statements in code blocks using sqlglot for better readability.

    :param code_blocks: Sequence of code blocks containing SQL statements
    :param dialect: SQL dialect ('snowflake' or 'bigquery')
    :param conditional: If True, only format statements without newlines (default: False)
    :return: List of formatted code blocks
    """
    formatted_blocks = []
    for block in code_blocks:
        formatted_statements = []
        for stmt in block.script:
            if conditional and '\n' in stmt:
                formatted_statements.append(stmt)
            else:
                formatted_stmt = format_sql_statement(stmt, dialect)
                formatted_statements.append(formatted_stmt)

        formatted_block = TransformationConfiguration.Parameters.Block.Code(
            name=block.name,
            script=formatted_statements,
        )
        formatted_blocks.append(formatted_block)

    return formatted_blocks


def format_transformation_parameters(
    parameters: TransformationConfiguration.Parameters,
    dialect: str,
    conditional: bool = False,
) -> TransformationConfiguration.Parameters:
    """
    Format SQL statements in transformation parameters using sqlglot for better readability.

    :param parameters: Transformation parameters containing blocks with code
    :param dialect: SQL dialect ('snowflake' or 'bigquery')
    :param conditional: If True, only format statements without newlines (default: False)
    :return: Formatted transformation parameters
    """

    formatted_blocks = []
    for block in parameters.blocks:
        formatted_codes = format_code_blocks(block.codes, dialect, conditional)
        formatted_block = TransformationConfiguration.Parameters.Block(
            name=block.name,
            codes=formatted_codes,
        )
        formatted_blocks.append(formatted_block)

    return TransformationConfiguration.Parameters(blocks=formatted_blocks)
