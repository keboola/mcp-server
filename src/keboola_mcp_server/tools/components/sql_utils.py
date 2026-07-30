"""
SQL splitting and joining utilities for SQL transformations.

This module provides functionality to split SQL scripts into individual
statements and join them back together, using the same token vocabulary as
the Keboola UI's splitQueriesWorker.worker.ts implementation.
"""

import asyncio
import logging
import re
import time
from typing import Iterable

import sqlglot

from keboola_mcp_server.tools.components.model import SimplifiedTfBlocks

LOG = logging.getLogger(__name__)

# The maximum length (in characters) of the SQL we are willing to split.
#
# It is enforced twice: on each individual code block (in `split_sql_statements()`) and on the
# sum of all code blocks of a single transformation (in `check_total_sql_length()`, called from
# `SimplifiedTfBlocks.to_raw_parameters()`). The aggregate check is the one that matters --
# `to_raw_parameters()` splits every code block of the request, so many medium blocks cost the
# same as one huge one and a per-block-only cap would be trivially bypassed.
#
# 1 MiB is roughly 27,000 lines / 3,900 statements of formatted SQL, which is orders of magnitude
# above any legitimate transformation script (real ones are single-digit KB). At that size the
# linear scan measures ~15 ms on realistic SQL and ~0.4 s on the worst possible token density
# (a script that is nothing but ';' or '-' characters), so it stays inside the default
# `timeout_seconds=1.0` budget of `split_sql_statements()`.
MAX_SQL_SCRIPT_LENGTH = 1024 * 1024

# A single, non-nested alternation of SQL tokens, scanned left to right with `re.finditer()`.
#
# The LAST branch is a guaranteed-match single character (`[\s\S]`), so a token match can never
# fail. The scan therefore always advances by at least one character and never backtracks across
# a token boundary: every character is consumed exactly once and the whole scan is linear in the
# length of the script.
#
# The previous implementation matched whole statements with one
# `\s*((?: ... | [^"';#/$-]+ )+(?:;|$))` mega-pattern. That nested quantifier (`+` over an
# alternation whose last branch is itself `+`-quantified) has no linear bound. An unpairable
# quote makes the trailing `(?:;|$)` unmatchable, and the engine then enumerates the partitions
# of the preceding run of ordinary characters -- exponential in the length of that run, with a
# further quadratic factor because `findall()` retries the failing match at every offset. The
# individual string/comment branches below are Friedl-unrolled and linear on their own, so they
# are kept verbatim; only the statement-level nesting is gone.
SQL_TOKEN_REGEX = re.compile(
    r"'[^'\\]*(?:\\.[^'\\]*)*'"  # Single-quoted strings
    r'|"[^"\\]*(?:\\.[^"\\]*)*"'  # Double-quoted strings
    r'|\$\$(?:(?!\$\$)[\s\S])*\$\$'  # Multi-line blocks $$...$$ (using [\s\S] for any char)
    r'|/\*[^*]*\*+(?:[^*/][^*]*\*+)*/'  # Multi-line comments /* ... */
    r'|#[^\n\r]*'  # Hash comments
    r'|--[^\n\r]*'  # SQL comments
    r'|//[^\n\r]*'  # C-style comments
    r'|[^"\';#/$-]+'  # Everything else except special chars (greedy match for performance)
    # Fallback: exactly one character, whatever it is. This makes the alternation total, which is
    # what removes the backtracking. It consumes the ';' separators, a lone '-', '/' or '$' (the
    # old pattern spelled those out as `-(?!-)`, `/(?![*/])` and `\$(?!\$)`), and also an
    # unpairable quote or the opening of an unterminated /* or $$ block -- see `_split_with_regex()`.
    r'|[\s\S]'
)

# Number of tokens consumed between two `time.monotonic()` deadline checks. Large enough that the
# check costs nothing measurable, small enough to stop a runaway scan promptly.
_DEADLINE_CHECK_INTERVAL = 8192

# Regex for detecting line comments (single-line style: --, //, #)
LINE_COMMENT_REGEX = re.compile(r'(--|//|#).*$')

# Regex patterns for parsing block/code structure markers
BLOCK_MARKER_REGEX = re.compile(r'/\*\s*=+\s*BLOCK:\s*([^*]+?)\s*=+\s*\*/', re.MULTILINE)
CODE_MARKER_REGEX = re.compile(r'/\*\s*=+\s*CODE:\s*([^*]+?)\s*=+\s*\*/', re.MULTILINE)


def check_total_sql_length(scripts: Iterable[str]) -> None:
    """
    Checks that all the SQL scripts of a single transformation together fit within the size limit.

    :param scripts: The SQL scripts of all code blocks of one transformation
    :raises ValueError: If the combined length exceeds `MAX_SQL_SCRIPT_LENGTH`
    """
    total = sum(len(script) for script in scripts)
    if total > MAX_SQL_SCRIPT_LENGTH:
        raise ValueError(
            f"The transformation's total SQL is too large to process: {total} characters across all code blocks, "
            f'but the maximum supported total is {MAX_SQL_SCRIPT_LENGTH} characters. '
            f'Do not retry with the same input. Split the SQL across several smaller code blocks and, if the '
            f'total still exceeds the limit, across several transformations, so that each transformation stays '
            f'under {MAX_SQL_SCRIPT_LENGTH} characters in total.'
        )


async def split_sql_statements(script: str, timeout_seconds: float = 1.0) -> list[str]:
    """
    Splits a SQL script string into individual statements.

    Uses the same token vocabulary as the UI's splitQueriesWorker.worker.ts, but scans the script
    with a single linear left-to-right pass (see `SQL_TOKEN_REGEX` and `_split_with_regex()`).

    :param script: The SQL script string to split
    :param timeout_seconds: Maximum time to allow for the scan (default: 1.0)
    :return: List of individual SQL statements (trimmed, non-empty)
    :raises ValueError: If the script exceeds `MAX_SQL_SCRIPT_LENGTH`, cannot be parsed, or the
        scan exceeds `timeout_seconds`
    """
    if not script or not script.strip():
        return []

    if len(script) > MAX_SQL_SCRIPT_LENGTH:
        raise ValueError(
            f'The SQL script is too large to process: {len(script)} characters, but the maximum supported '
            f'length of a single code block is {MAX_SQL_SCRIPT_LENGTH} characters. '
            f'Do not retry with the same input. Split the SQL into several smaller code blocks, each well '
            f'under {MAX_SQL_SCRIPT_LENGTH} characters.'
        )

    try:
        try:
            # Run in a worker thread so that the event loop keeps running: unlike the C-level
            # `re.findall()` call this replaced, the scan loop is Python bytecode and releases the
            # GIL periodically. There is deliberately no `asyncio.wait_for()` around it -- a
            # thread cannot be cancelled, so that guard never actually stopped anything. The scan
            # enforces its own deadline instead.
            statements = await asyncio.to_thread(_split_with_regex, script, timeout_seconds)
        except (asyncio.TimeoutError, TimeoutError):
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


def _split_with_regex(script: str, timeout_seconds: float | None = None) -> list[str]:
    """
    Splits SQL into statements with a single linear left-to-right token scan.

    `SQL_TOKEN_REGEX` always matches, so `finditer()` tokenizes the whole script in one pass with
    no backtracking: strings, comments and `$$` blocks are consumed as single tokens (and can thus
    contain `;` safely), and a statement is cut at every remaining top-level `;`. The handling of a
    `;` that has nothing but whitespace in front of it is inherited verbatim from the previous
    pattern -- see the comment in the loop.

    Unterminated constructs (a quote with no closing quote, an unclosed `/*` or `$$`) degrade
    gracefully: the opening character falls through to the single-character fallback branch and the
    remainder of the script is scanned as ordinary text. Nothing is raised, nothing hangs, and no
    input is discarded -- the text is preserved verbatim in the statement it belongs to, which the
    old pattern did not manage (it silently dropped characters around the unpaired quote).

    :param script: The SQL script string to split
    :param timeout_seconds: Wall-clock budget for the scan; `None` disables the deadline
    :return: List of statement strings, each still carrying its trailing `;` if it had one
    :raises TimeoutError: If the scan does not finish within `timeout_seconds`
    """
    deadline = time.monotonic() + timeout_seconds if timeout_seconds else None
    tokens_until_check = _DEADLINE_CHECK_INTERVAL

    statements: list[str] = []
    start = 0  # Offset in `script` where the statement being scanned starts.

    for match in SQL_TOKEN_REGEX.finditer(script):
        if deadline is not None:
            tokens_until_check -= 1
            if tokens_until_check <= 0:
                tokens_until_check = _DEADLINE_CHECK_INTERVAL
                if time.monotonic() > deadline:
                    raise TimeoutError(f'SQL splitting exceeded the {timeout_seconds}s budget.')

        # Only the one-character fallback branch can ever match a ';', so any token that starts
        # with ';' is exactly that separator.
        if match.group() == ';':
            chunk = script[start : match.start()]
            start = match.end()
            if chunk.strip():
                statements.append(f'{chunk};'.strip())
            elif chunk:
                # Bug-for-bug compatible with the pattern this replaced: its leading `\s*` could
                # give the whitespace back so that the whitespace itself became the statement body.
                # So `SELECT 1; ;` yielded a bare ';' statement while `SELECT 1;;` yielded nothing.
                # Preserved deliberately -- a hotfix should not change what valid input produces.
                statements.append(';')

    trailing = script[start:].strip()
    if trailing:
        statements.append(trailing)

    return statements


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
