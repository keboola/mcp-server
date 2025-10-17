"""
SQL splitting and joining utilities for SQL transformations.

This module provides functionality to split SQL scripts into individual statements
and join them back together, using regex-based parsing similar to the Keboola UI's
splitQueriesWorker.worker.ts implementation.
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import List

LOG = logging.getLogger(__name__)

SQL_SPLIT_REGEX = re.compile(
    r'\s*('
    r"(?:'[^'\\]*(?:\\.[^'\\]*)*'|"  # Single-quoted strings
    r'"[^"\\]*(?:\\.[^"\\]*)*"|'  # Double-quoted strings
    r'\$\$(?:.|\n|\r)*?\$\$|'  # Multi-line blocks $$...$$
    r'/\*[^*]*\*+(?:[^*/][^*]*\*+)*/|'  # Multi-line comments /* ... */
    r'#.*|'  # Hash comments
    r'--.*|'  # SQL comments
    r'//.*|'  # C-style comments
    r'[^"\';#])+(?:;|$))',  # Everything else until semicolon or end
    re.MULTILINE,
)


def split_sql_statements(script: str, timeout_seconds: float = 5.0) -> List[str]:
    """
    Split a SQL script string into individual statements.

    Uses regex-based parsing similar to UI's splitQueriesWorker.worker.ts.
    Includes timeout protection to prevent catastrophic backtracking.

    Args:
        script: The SQL script string to split
        timeout_seconds: Maximum time to allow for regex processing (default: 5.0)

    Returns:
        List of individual SQL statements (trimmed, non-empty)

    Raises:
        ValueError: If the script is invalid or regex times out
    """
    if not script or not script.strip():
        return []

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_split_with_regex, script)
            try:
                statements = future.result(timeout=timeout_seconds)
            except FuturesTimeoutError:
                future.cancel()
                raise ValueError(
                    f'SQL parsing took too long (possible catastrophic backtracking). '
                    f'Timeout: {timeout_seconds}s'
                )

        if statements and ''.join(statements) != script:
            raise ValueError('SQL script is not valid (round-trip validation failed)')

        normalized = [stmt.strip() for stmt in statements if stmt.strip()]

        return normalized

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        LOG.exception(f'Failed to split SQL statements: {e}')
        raise ValueError(f'Failed to parse SQL script: {e}')


def _split_with_regex(script: str) -> List[str]:
    """
    Internal function to split SQL using regex.

    This is separated to allow timeout handling in the calling function.

    Args:
        script: The SQL script string to split

    Returns:
        List of matched statement strings (may include empty strings)
    """
    matches = SQL_SPLIT_REGEX.findall(script)
    return matches if matches else []


def join_sql_statements(statements: List[str]) -> str:
    """
    Join SQL statements into a single script string.

    Based on UI's helpers.js lines 187-233. Adds semicolons as delimiters
    when missing, and preserves existing semicolons.

    Args:
        statements: List of SQL statements to join

    Returns:
        Concatenated SQL script string with proper delimiters
    """
    if not statements:
        return ''

    result_parts = []

    for stmt in statements:
        trimmed_stmt = stmt.strip()
        if not trimmed_stmt:
            continue

        ends_with_semicolon = trimmed_stmt.endswith(';')

        ends_with_comment = (
            trimmed_stmt.endswith('*/')
            or re.search(r'(--|//|#).*$', trimmed_stmt) is not None
        )

        if ends_with_semicolon or ends_with_comment:
            result_parts.append(f'{stmt}\n\n')
        else:
            cleaned_stmt = stmt.rstrip(';')
            result_parts.append(f'{cleaned_stmt};\n\n')

    return ''.join(result_parts)


def validate_round_trip(original: str) -> bool:
    """
    Validate that split(join(split(x))) == split(x).

    Based on UI's splitSqlQueries.ts line 52 validation.
    This ensures that the splitting and joining logic is consistent.

    Args:
        original: The original SQL script string

    Returns:
        True if round-trip is valid, False otherwise
    """
    try:
        split_original = split_sql_statements(original)

        joined = join_sql_statements(split_original)
        split_again = split_sql_statements(joined)

        return split_original == split_again

    except Exception as e:
        LOG.warning(f'Round-trip validation failed: {e}')
        return False
