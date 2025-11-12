"""
Functions for updating SQL transformation parameters.

This module provides operations for modifying transformation blocks and codes
using JSONPath for locating and manipulating elements.
"""

from jsonpath_ng.ext import parse as parse_jsonpath

from keboola_mcp_server.tools.components.model import (
    TfAddBlock,
    TfAddCode,
    TfAddScript,
    TfRemoveBlock,
    TfRemoveCode,
    TfRenameBlock,
    TfRenameCode,
    TfSetCode,
    TfStrReplace,
)
from keboola_mcp_server.tools.components.sql_utils import (
    format_simplified_tf_block,
    format_simplified_tf_code,
    format_sql,
)

# Operations that change the structure of the transformation
STRUCTURAL_OPS = frozenset[str]({'add_block', 'add_code', 'remove_block', 'remove_code'})


def add_block(params: dict, op: TfAddBlock, sql_dialect: str) -> tuple[dict, str]:
    """
    Add a new block to the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The add_block operation
    :param sql_dialect: The SQL dialect of the transformation
    :return: Tuple of (modified parameters dictionary, change summary message)
    :raises ValueError: If params doesn't contain 'blocks' key or block name is empty/whitespace
    """
    if 'blocks' not in params:
        raise ValueError("Invalid parameters: must contain 'blocks' key")

    if not op.block.name.strip():
        raise ValueError('Invalid operation: block name cannot be empty')

    new_block, is_reformatted = format_simplified_tf_block(block=op.block, dialect=sql_dialect)
    new_block_dict = new_block.model_dump()

    if op.position == 'start':
        params['blocks'].insert(0, new_block_dict)
    else:  # 'end'
        params['blocks'].append(new_block_dict)

    message = f'Added block with name "{op.block.name}"'
    if is_reformatted:
        message += ' (code was automatically reformatted)'

    return params, message


def remove_block(params: dict, op: TfRemoveBlock, sql_dialect: str) -> tuple[dict, str]:
    """
    Remove an existing block from the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The remove_block operation
    :return: Tuple of (modified parameters dictionary, change summary message)
    """
    expr = parse_jsonpath(f"$.blocks[?(@.id = '{op.block_id}')]")
    matches = expr.find(params)

    if not matches:
        raise ValueError(f"Block with id '{op.block_id}' does not exist")

    # Remove the block using `filter`
    return expr.filter(lambda x: True, params), ''


def rename_block(params: dict, op: TfRenameBlock, sql_dialect: str) -> tuple[dict, str]:
    """
    Rename an existing block in the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The rename_block operation
    :return: Tuple of (modified parameters dictionary, change summary message)
    :raises ValueError: If block_id doesn't exist or block_name is empty/whitespace
    """
    if not op.block_name.strip():
        raise ValueError('Invalid operation: block name cannot be empty')

    expr = parse_jsonpath(f"$.blocks[?(@.id = '{op.block_id}')].name")
    matches = expr.find(params)

    if not matches:
        raise ValueError(f"Block with id '{op.block_id}' does not exist")

    return expr.update(params, op.block_name), ''


def add_code(params: dict, op: TfAddCode, sql_dialect: str) -> tuple[dict, str]:
    """
    Add a new code to an existing block in the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The add_code operation
    :param sql_dialect: The SQL dialect of the transformation
    :return: Tuple of (modified parameters dictionary, change summary message)
    :raises ValueError: If block_id doesn't exist or code name is empty/whitespace
    """
    if not op.code.name.strip():
        raise ValueError('Invalid operation: code name cannot be empty')

    expr = parse_jsonpath(f"$.blocks[?(@.id = '{op.block_id}')].codes")
    matches = expr.find(params)

    if not matches:
        raise ValueError(f"Block with id '{op.block_id}' does not exist")

    codes = matches[0].value

    new_code, is_reformatted = format_simplified_tf_code(code=op.code, dialect=sql_dialect)
    new_code_dict = new_code.model_dump()

    if op.position == 'start':
        codes.insert(0, new_code_dict)
    else:  # 'end'
        codes.append(new_code_dict)

    message = f'Added code with name "{op.code.name}"'
    if is_reformatted:
        message += ' (code was automatically reformatted)'

    return params, message


def remove_code(params: dict, op: TfRemoveCode, sql_dialect: str) -> tuple[dict, str]:
    """
    Remove an existing code from an existing block in the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The remove_code operation
    :return: Tuple of (modified parameters dictionary, change summary message)
    """
    # Target the specific code in the specific block
    expr = parse_jsonpath(f"$.blocks[?(@.id = '{op.block_id}')].codes[?(@.id = '{op.code_id}')]")
    matches = expr.find(params)

    if not matches:
        raise ValueError(f"Code with id '{op.code_id}' in block '{op.block_id}' does not exist")

    # Remove the code using `filter`
    return expr.filter(lambda x: True, params), ''


def rename_code(params: dict, op: TfRenameCode, sql_dialect: str) -> tuple[dict, str]:
    """
    Rename an existing code in an existing block in the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The rename_code operation
    :return: Tuple of (modified parameters dictionary, change summary message)
    :raises ValueError: If block_id or code_id doesn't exist or code_name is empty/whitespace
    """
    if not op.code_name.strip():
        raise ValueError('Invalid operation: code name cannot be empty')

    # Target the specific code's name field directly
    expr = parse_jsonpath(f"$.blocks[?(@.id = '{op.block_id}')].codes[?(@.id = '{op.code_id}')].name")
    matches = expr.find(params)

    if not matches:
        raise ValueError(f"Code with id '{op.code_id}' in block '{op.block_id}' does not exist")

    return expr.update(params, op.code_name), ''


def set_code(params: dict, op: TfSetCode, sql_dialect: str) -> tuple[dict, str]:
    """
    Set the SQL script of an existing code in an existing block in the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The set_code operation
    :return: Tuple of (modified parameters dictionary, change summary message)
    """
    if not op.script.strip():
        raise ValueError('Invalid operation: script cannot be empty')

    formatted_script = format_sql(sql=op.script, dialect=sql_dialect)
    is_reformatted = formatted_script != op.script

    # Target the specific code's script field directly
    expr = parse_jsonpath(f"$.blocks[?(@.id = '{op.block_id}')].codes[?(@.id = '{op.code_id}')].script")
    matches = expr.find(params)

    if not matches:
        raise ValueError(f"Code with id '{op.code_id}' in block '{op.block_id}' does not exist")

    # Update the script field
    message = f"Changed code with id '{op.code_id}' in block '{op.block_id}'"
    if is_reformatted:
        message += ' (code was automatically reformatted)'
    return expr.update(params, formatted_script), message


def add_script(params: dict, op: TfAddScript, sql_dialect: str) -> tuple[dict, str]:
    """
    Append or prepend SQL script text to an existing code in an existing block in the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The add_script operation
    :return: Tuple of (modified parameters dictionary, change summary message)
    :raises ValueError: If block_id or code_id doesn't exist or script is empty/whitespace
    """
    if not op.script.strip():
        raise ValueError('Invalid operation: script cannot be empty')

    # Target the specific code's script field directly
    expr = parse_jsonpath(f"$.blocks[?(@.id = '{op.block_id}')].codes[?(@.id = '{op.code_id}')].script")
    matches = expr.find(params)

    if not matches:
        raise ValueError(f"Code with id '{op.code_id}' in block '{op.block_id}' does not exist")

    current_script = matches[0].value

    # Compute new script based on position
    if op.position == 'start':
        new_script = f'{op.script} {current_script}' if current_script else op.script
    else:  # 'end'
        new_script = f'{current_script} {op.script}' if current_script else op.script

    formatted_script = format_sql(sql=new_script, dialect=sql_dialect)
    is_reformatted = formatted_script != new_script

    # Update the script field
    message = f"Added script to code with id '{op.code_id}' in block '{op.block_id}'"
    if is_reformatted:
        message += ' (code was automatically reformatted)'
    return expr.update(params, formatted_script), message


def str_replace(params: dict, op: TfStrReplace, sql_dialect: str) -> tuple[dict, str]:
    """
    Replace a substring in SQL statements in the transformation.

    :param params: The transformation parameters dictionary with 'blocks' key
    :param op: The str_replace operation
    :return: Tuple of (modified parameters dictionary, change summary message)
    :raises ValueError: If search string is empty, search and replace are the same,
                        search string not found, or target doesn't exist
    """
    if not op.search_for:
        raise ValueError('Invalid operation: search string is empty')

    if op.search_for == op.replace_with:
        raise ValueError(f'Invalid operation: search string and replace string are the same: "{op.search_for}"')

    # Determine the JSONPath based on scope
    if op.block_id is None:
        # Replace in all blocks and all codes
        jsonpath = '$.blocks[*].codes[*].script'
        scope = 'the transformation'
    elif op.code_id is None:
        # Replace in all codes of a specific block
        jsonpath = f"$.blocks[?(@.id = '{op.block_id}')].codes[*].script"
        scope = f'block "{op.block_id}"'
    else:
        # Replace in a specific code of a specific block
        jsonpath = f"$.blocks[?(@.id = '{op.block_id}')].codes[?(@.id = '{op.code_id}')].script"
        scope = f'code "{op.code_id}", block "{op.block_id}"'

    expr = parse_jsonpath(jsonpath)
    matches = expr.find(params)

    if not matches:
        raise ValueError(f'No scripts found in {scope}')

    replace_cnt = 0

    for match in matches:
        script = match.value

        if op.search_for in script:
            # Count actual occurrences in this script
            occurrences_in_script = script.count(op.search_for)
            replace_cnt += occurrences_in_script
            new_script = script.replace(op.search_for, op.replace_with)
            params = match.full_path.update(params, new_script)

    # Validate that at least one replacement was made
    if replace_cnt == 0:
        raise ValueError(f'Search string "{op.search_for}" not found in {scope}')

    occurrence_word = 'occurrence' if replace_cnt == 1 else 'occurrences'
    return params, f'Replaced {replace_cnt} {occurrence_word} of "{op.search_for}" in {scope}'
