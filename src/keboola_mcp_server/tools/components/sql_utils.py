"""
SQL splitting and joining utilities for SQL transformations.

This module provides functionality to split SQL scripts into individual
statements and join them back together, using regex-based parsing similar
to the Keboola UI's splitQueriesWorker.worker.ts implementation.
"""

import asyncio
import logging
import re

from keboola_mcp_server.tools.components.model import TransformationConfiguration

TransformationBlocks = TransformationConfiguration.Parameters

LOG = logging.getLogger(__name__)

SQL_SPLIT_REGEX = re.compile(
    r'\s*('
    r"(?:'[^'\\]*(?:\\.[^'\\]*)*'|"  # Single-quoted strings
    r'"[^"\\]*(?:\\.[^"\\]*)*"|'  # Double-quoted strings
    r'\$\$(?:(?!\$\$)(?:.|\n|\r))*\$\$|'  # Multi-line blocks $$...$$
    r'/\*[^*]*\*+(?:[^*/][^*]*\*+)*/|'  # Multi-line comments /* ... */
    r'#[^\n\r]*|'  # Hash comments
    r'--[^\n\r]*|'  # SQL comments
    r'//[^\n\r]*|'  # C-style comments
    r'[^"\';#/$-])+(?:;|$))',  # Everything else until semicolon or end
    re.MULTILINE,
)

# Regex for detecting line comments (single-line style: --, //, #)
LINE_COMMENT_REGEX = re.compile(r'(--|//|#).*$')

# Regex patterns for parsing block/code structure markers
BLOCK_MARKER_REGEX = re.compile(r'/\*\s*=+\s*BLOCK:\s*([^*]+?)\s*=+\s*\*/', re.MULTILINE)
CODE_MARKER_REGEX = re.compile(r'/\*\s*=+\s*(?:CODE|SHARED CODE):\s*([^*]+?)\s*=+\s*\*/', re.MULTILINE)


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


def join_sql_statements(statements: list[str]) -> str:
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

        ends_with_comment = trimmed_stmt.endswith('*/') or LINE_COMMENT_REGEX.search(trimmed_stmt) is not None

        if ends_with_semicolon or ends_with_comment:
            result_parts.append(f'{stmt}\n\n')
        else:
            cleaned_stmt = stmt.rstrip(';')
            result_parts.append(f'{cleaned_stmt};\n\n')

    return ''.join(result_parts)


def blocks_to_string(blocks: list[dict] | TransformationBlocks, ignore_delimiters: bool = False) -> str:
    """
    Convert structured blocks to a single formatted SQL string with metadata comments.

    :param blocks: Either a list of block dictionaries or TransformationBlocks model with structure:
        List format:
        [
            {
                'name': 'Block Name',
                'codes': [
                    {
                        'name': 'Code Name',
                        'script': ['SELECT 1', 'SELECT 2']  # List of SQL statements or None
                    }
                ]
            }
        ]
        Or TransformationBlocks(blocks=[Block(...)])
    :param ignore_delimiters: If True, don't add SQL delimiters (useful for diffs)
    :return: Formatted SQL string with block and code metadata comments

    Example output:
        /* ===== BLOCK: Block 1 ===== */

        /* ===== CODE: First Code ===== */

        SELECT 1;

        SELECT 2;
    """
    # Convert TransformationBlocks to list of dicts for backward compatibility
    if isinstance(blocks, TransformationBlocks):
        blocks_list: list[dict] = blocks.model_dump(by_alias=True)['blocks']
    else:
        blocks_list = blocks

    if not blocks_list:
        return ''

    def should_add_delimiter(statement: str) -> bool:
        """Check if a SQL statement needs a delimiter added."""
        trimmed = statement.strip()
        if not trimmed:
            return False
        # Don't add delimiter if already ends with semicolon or comment
        if trimmed.endswith(';') or trimmed.endswith('*/'):
            return False
        # Check for line-ending comments (pure comment statements)
        if trimmed.startswith(('--', '#', '//')) or (trimmed.startswith('/*') and trimmed.endswith('*/')):
            return False
        return True

    def format_statement(statement: str) -> str:
        """Add delimiter to statement if needed."""
        if not statement:
            return ''
        if ignore_delimiters or not should_add_delimiter(statement):
            return statement
        return statement.rstrip() + ';'

    result_parts = []

    for block in blocks_list:
        block_name = block.get('name', 'Untitled Block')
        codes = block.get('codes', [])

        # Add block header
        result_parts.append(f'/* ===== BLOCK: {block_name} ===== */\n\n')

        for code in codes:
            code_name = code.get('name', 'Untitled Code')
            script = code.get('script')

            # Add code header
            result_parts.append(f'/* ===== CODE: {code_name} ===== */\n\n')

            # Process script elements (must be list or None)
            if script:
                formatted_scripts = [format_statement(str(s)) for s in script if s]
                code_content = '\n\n'.join(formatted_scripts)

                if code_content:
                    result_parts.append(code_content)
                    result_parts.append('\n\n')

    result = ''.join(result_parts).rstrip()
    return result + '\n' if result else ''


async def string_to_blocks(code_string: str) -> TransformationBlocks:
    """
    Parse a formatted SQL string back into structured blocks.

    :param code_string: Formatted SQL string with block/code metadata comments
    :return: TransformationBlocks (TransformationConfiguration.Parameters) with structured blocks

    Example input:
        /* ===== BLOCK: Block 1 ===== */

        /* ===== CODE: First Code ===== */

        SELECT 1;
        SELECT 2;

    Example output:
        TransformationBlocks(
            blocks=[
                Block(
                    name='Block 1',
                    codes=[
                        Code(
                            name='First Code',
                            sql_statements=['SELECT 1;', 'SELECT 2;']
                        )
                    ]
                )
            ]
        )
    """
    if not code_string or not code_string.strip():
        return TransformationBlocks(blocks=[])

    blocks = []

    # Split by block markers - creates [before, name1, content1, name2, content2, ...]
    block_splits = BLOCK_MARKER_REGEX.split(code_string)

    # Process pairs of (block_name, block_content)
    # Start at index 1 to skip content before first block marker
    for idx in range(1, len(block_splits), 2):
        block_name = block_splits[idx].strip()
        block_content = block_splits[idx + 1]  # Safe due to range step

        # Split block content by code markers
        code_splits = CODE_MARKER_REGEX.split(block_content)

        codes = []

        # Process pairs of (code_name, code_content)
        # Start at index 1 to skip content before first code marker
        for code_idx in range(1, len(code_splits), 2):
            code_name = code_splits[code_idx].strip()
            code_content = code_splits[code_idx + 1].strip()  # Safe due to range step

            if not code_content:
                codes.append(TransformationConfiguration.Parameters.Block.Code(name=code_name, sql_statements=[]))
                continue

            # Split SQL statements
            try:
                script_array = await split_sql_statements(code_content)
            except ValueError as e:
                LOG.warning(f'Failed to split SQL for code "{code_name}": {e}')
                # Fallback: treat as single statement
                script_array = [code_content]

            codes.append(TransformationConfiguration.Parameters.Block.Code(name=code_name, sql_statements=script_array))

        if codes:
            blocks.append(TransformationConfiguration.Parameters.Block(name=block_name, codes=codes))

    return TransformationBlocks(blocks=blocks)
