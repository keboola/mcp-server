"""
Utility functions for Keboola component and configuration management.

This module contains helper functions and utilities used across the component tools:

## Component Retrieval
- fetch_component: Fetches component details with AI Service/Storage API fallback
- handle_component_types: Normalizes component type filtering

## Configuration Listing
- list_configs_by_types: Retrieves components+configs filtered by type
- list_configs_by_ids: Retrieves components+configs filtered by ID

## SQL Transformation Utilities
- get_sql_transformation_id_from_sql_dialect: Maps SQL dialect to component ID
- get_transformation_configuration: Builds transformation config payloads
- clean_bucket_name: Sanitizes bucket names for transformations

## Data Models
- TransformationConfiguration: Pydantic model for SQL transformation structure
"""

import copy
import logging
import re
import unicodedata
from typing import Any, Mapping, Sequence, TypeVar, cast

import jsonpath_ng
from httpx import HTTPStatusError

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import ComponentAPIResponse, ConfigurationAPIResponse
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.components import tf_update
from keboola_mcp_server.tools.components.model import (
    ALL_COMPONENT_TYPES,
    ComponentSummary,
    ComponentType,
    ComponentWithConfigurations,
    ConfigParamUpdate,
    ConfigurationSummary,
    SimplifiedTfBlocks,
    TfParamUpdate,
    TransformationConfiguration,
)
from keboola_mcp_server.tools.components.sql_utils import format_simplified_tf_block

LOG = logging.getLogger(__name__)
T = TypeVar('T')


# ============================================================================
# CONSTANTS
# ============================================================================

SNOWFLAKE_TRANSFORMATION_ID = 'keboola.snowflake-transformation'
BIGQUERY_TRANSFORMATION_ID = 'keboola.google-bigquery-transformation'


# ============================================================================
# CONFIGURATION LISTING UTILITIES
# ============================================================================


def expand_component_types(component_types: Sequence[ComponentType]) -> tuple[ComponentType, ...]:
    """
    Expand empty component types list to all component types.

    :param component_types: Sequence of component types to expand
    :return: Tuple of component types, or all component types if input is empty
    """
    if not component_types:
        return ALL_COMPONENT_TYPES

    out_component_types = set(component_types)

    return tuple(sorted(out_component_types))


async def list_configs_by_types(
    client: KeboolaClient, component_types: Sequence[ComponentType]
) -> list[ComponentWithConfigurations]:
    """
    Retrieves components with their configurations filtered by component types.

    Used by:
    - list_configs tool

    :param client: Authenticated Keboola client instance
    :param component_types: Types of components to retrieve (extractor, writer, application, transformation)
    :return: List of components paired with their configuration summaries
    """
    components_with_configurations = []

    for comp_type in component_types:
        # Fetch raw components with configurations included
        raw_components_with_configurations_by_type = await client.storage_client.component_list(
            component_type=comp_type, include=['configuration']
        )

        # Process each component and its configurations
        for raw_component in raw_components_with_configurations_by_type:
            raw_configuration_responses = [
                ConfigurationAPIResponse.model_validate(raw_configuration | {'component_id': raw_component['id']})
                for raw_configuration in cast(list[JsonDict], raw_component.get('configurations', []))
            ]

            # Convert to domain models
            configuration_summaries = [
                ConfigurationSummary.from_api_response(api_config) for api_config in raw_configuration_responses
            ]

            # Process component
            api_component = ComponentAPIResponse.model_validate(raw_component)
            domain_component = ComponentSummary.from_api_response(api_component)

            components_with_configurations.append(
                ComponentWithConfigurations(
                    component=domain_component,
                    configurations=configuration_summaries,
                )
            )

    total_configurations = sum(len(component.configurations) for component in components_with_configurations)
    LOG.info(
        f'Found {len(components_with_configurations)} components with total of {total_configurations} configurations '
        f'for types {component_types}.'
    )
    return components_with_configurations


async def list_configs_by_ids(client: KeboolaClient, component_ids: Sequence[str]) -> list[ComponentWithConfigurations]:
    """
    Retrieves components with their configurations filtered by specific component IDs.

    Used by:
    - list_configs tool (when specific component IDs are requested)

    :param client: Authenticated Keboola client instance
    :param component_ids: Specific component IDs to retrieve
    :return: List of components paired with their configuration summaries
    """
    components_with_configurations = []

    for component_id in component_ids:
        # Fetch configurations and component details
        raw_configurations = await client.storage_client.configuration_list(component_id=component_id)
        raw_component = await client.storage_client.component_detail(component_id=component_id)

        # Process component
        api_component = ComponentAPIResponse.model_validate(raw_component)
        domain_component = ComponentSummary.from_api_response(api_component)

        # Process configurations
        raw_configuration_responses = [
            ConfigurationAPIResponse.model_validate({**raw_configuration, 'component_id': raw_component['id']})
            for raw_configuration in raw_configurations
        ]
        configuration_summaries = [
            ConfigurationSummary.from_api_response(api_config) for api_config in raw_configuration_responses
        ]

        components_with_configurations.append(
            ComponentWithConfigurations(
                component=domain_component,
                configurations=configuration_summaries,
            )
        )

    total_configurations = sum(len(component.configurations) for component in components_with_configurations)
    LOG.info(
        f'Found {len(components_with_configurations)} components with total of {total_configurations} configurations '
        f'for ids {component_ids}.'
    )
    return components_with_configurations


# ============================================================================
# COMPONENT FETCHING
# ============================================================================


async def fetch_component(
    client: KeboolaClient,
    component_id: str,
) -> ComponentAPIResponse:
    """
    Fetches a component by ID, returning the raw API response.

    First tries to get component from the AI service catalog. If the component
    is not found (404) or returns empty data (private components), falls back to using the
    Storage API endpoint.

    Used by:
    - get_component tool
    - Configuration creation/update operations that need component schemas

    :param client: Authenticated Keboola client instance
    :param component_id: Unique identifier of the component to fetch
    :return: Unified API component response with available metadata
    :raises HTTPStatusError: If component is not found in either API
    """
    try:
        # First attempt: AI Service catalog (includes documentation & schemas)
        raw_component = await client.ai_service_client.get_component_detail(component_id=component_id)
        LOG.info(f'Retrieved component {component_id} from AI service catalog.')

        return ComponentAPIResponse.model_validate(raw_component)

    except HTTPStatusError as e:
        if e.response.status_code == 404:
            # Fallback: Storage API (basic component info only)
            LOG.info(
                f'Component {component_id} not found in AI service catalog (possibly private). '
                f'Falling back to Storage API.'
            )

            raw_component = await client.storage_client.component_detail(component_id=component_id)
            LOG.info(f'Retrieved component {component_id} from Storage API.')

            return ComponentAPIResponse.model_validate(raw_component)
        else:
            # If it's not a 404, re-raise the error
            raise


# ============================================================================
# SQL TRANSFORMATION UTILITIES
# ============================================================================


def get_sql_transformation_id_from_sql_dialect(
    sql_dialect: str,
) -> str:
    """
    Map SQL dialect to the appropriate transformation component ID.

    Keboola has different transformation components for different SQL dialects.
    This function maps the workspace SQL dialect to the correct component ID.

    :param sql_dialect: SQL dialect from workspace configuration (e.g., 'snowflake', 'bigquery')
    :return: Component ID for the appropriate SQL transformation
    :raises ValueError: If the SQL dialect is not supported
    """
    if sql_dialect.lower() == 'snowflake':
        return SNOWFLAKE_TRANSFORMATION_ID
    elif sql_dialect.lower() == 'bigquery':
        return BIGQUERY_TRANSFORMATION_ID
    else:
        raise ValueError(f'Unsupported SQL dialect: {sql_dialect}')


def clean_bucket_name(bucket_name: str) -> str:
    """
    Cleans the bucket name:
    - Converts the bucket name to ASCII. (Handle diacritics like český -> cesky)
    - Converts spaces to dashes.
    - Removes leading underscores, dashes, and whitespace.
    - Removes any character that is not alphanumeric, dash, or underscore.

    :param bucket_name: Raw bucket name to clean
    :return: Cleaned bucket name suitable for Keboola storage
    """
    max_bucket_length = 96
    bucket_name = bucket_name.strip()
    # Convert the bucket name to ASCII
    bucket_name = unicodedata.normalize('NFKD', bucket_name)
    bucket_name = bucket_name.encode('ascii', 'ignore').decode('ascii')  # český -> cesky
    # Replace all whitespace (including tabs, newlines) with dashes
    bucket_name = re.sub(r'\s+', '-', bucket_name)
    # Remove any character that is not alphanumeric, dash, or underscore
    bucket_name = re.sub(r'[^a-zA-Z0-9_-]', '', bucket_name)
    # Remove leading underscores if present
    bucket_name = re.sub(r'^_+', '', bucket_name)
    bucket_name = bucket_name[:max_bucket_length]
    return bucket_name


async def create_transformation_configuration(
    codes: Sequence[SimplifiedTfBlocks.Block.Code],
    transformation_name: str,
    output_tables: Sequence[str],
    sql_dialect: str,
) -> TransformationConfiguration:
    """
    Creates transformation configuration from simplified code blocks and output tables.
    Handles splitting the SQL `script`s into arrays of statements and creating the storage configuration.

    :param codes: The code blocks
    :param transformation_name: The name of the transformation from which the bucket name is derived as in the UI
    :param output_tables: The output tables of the transformation, created by the code statements
    :param sql_dialect: The SQL dialect of the transformation
    :return: TransformationConfiguration with parameters and storage
    """
    storage = TransformationConfiguration.Storage()
    # for simplicity, we create a single block with the name 'Blocks'
    block = SimplifiedTfBlocks.Block(
        name='Blocks',
        codes=list(codes),
    )
    block, _ = format_simplified_tf_block(block=block, dialect=sql_dialect)
    parameters = SimplifiedTfBlocks(blocks=[block])
    raw_parameters = await parameters.to_raw_parameters()

    if output_tables:
        # if the query creates new tables, output_table_mappings should contain the table names (llm generated)
        # we create bucket name from the sql query name adding `out.c-` prefix as in the UI and use it as destination
        # expected output table name format is `out.c-<sql_query_name>.<table_name>`
        bucket_name = clean_bucket_name(transformation_name)
        destination = f'out.c-{bucket_name}'
        storage.output.tables = [
            TransformationConfiguration.Storage.Destination.Table(
                # here the source refers to the table name from the sql statement
                # and the destination to the full bucket table name
                # WARNING: when implementing input.tables, source and destination are swapped.
                source=out_table,
                destination=f'{destination}.{out_table}',
            )
            for out_table in output_tables
        ]

    return TransformationConfiguration(parameters=raw_parameters, storage=storage)


async def set_cfg_creation_metadata(client: KeboolaClient, component_id: str, configuration_id: str) -> None:
    """
    Sets the configuration metadata to indicate it was created by MCP.

    :param client: KeboolaClient instance
    :param component_id: ID of the component
    :param configuration_id: ID of the configuration
    """
    try:
        await client.storage_client.configuration_metadata_update(
            component_id=component_id,
            configuration_id=configuration_id,
            metadata={MetadataField.CREATED_BY_MCP: 'true'},
        )
    except HTTPStatusError as e:
        logging.exception(
            f'Failed to set "{MetadataField.CREATED_BY_MCP}" metadata for configuration {configuration_id}: {e}'
        )


async def set_cfg_update_metadata(
    client: KeboolaClient,
    component_id: str,
    configuration_id: str,
    configuration_version: int,
) -> None:
    """
    Sets the configuration metadata to indicate it was updated by MCP.

    :param client: KeboolaClient instance
    :param component_id: ID of the component
    :param configuration_id: ID of the configuration
    :param configuration_version: Version of the configuration
    """
    updated_by_md_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{configuration_version}'
    try:
        await client.storage_client.configuration_metadata_update(
            component_id=component_id,
            configuration_id=configuration_id,
            metadata={updated_by_md_key: 'true'},
        )
    except HTTPStatusError as e:
        logging.exception(f'Failed to set "{updated_by_md_key}" metadata for configuration {configuration_id}: {e}')


# ============================================================================
# PARAMETER UPDATE UTILITIES
# ============================================================================


def get_nested(obj: Mapping[str, Any] | None, key: str, *, default: T | None = None) -> T | None:
    """
    Gets a value from a nested mapping associated with the key.

    :param obj: Mapping (dictionary) object to search in
    :param key: Dot-separated key path (e.g., 'database.host')
    :param default: Default value to return if key is not found
    :return: Value associated with the key, or default if not found
    """
    d = obj
    for k in key.split('.'):
        d = d.get(k) if isinstance(d, Mapping) else None
        if d is None:
            return default
    return d


def set_nested_value(data: dict[str, Any], path: str, value: Any) -> None:
    """
    Sets a value in a nested dictionary using a dot-separated path.

    :param data: The dictionary to modify
    :param path: Dot-separated path (e.g., 'database.host')
    :param value: The value to set
    :raises ValueError: If a non-dict value is encountered in the path
    """
    keys = path.split('.')
    current = data

    for i, key in enumerate(keys[:-1]):
        if key not in current:
            current[key] = {}
        current = current[key]
        if not isinstance(current, dict):
            path_so_far = '.'.join(keys[: i + 1])
            raise ValueError(
                f'Cannot set nested value at path "{path}": '
                f'encountered non-dict value at "{path_so_far}" (type: {type(current).__name__})'
            )

    current[keys[-1]] = value


def _apply_param_update(params: dict[str, Any], update: ConfigParamUpdate) -> dict[str, Any]:
    """
    Applies a single parameter update to the given parameters dictionary.

    Note: This function modifies the input dictionary in place for efficiency.
    The caller (update_params) is responsible for creating a copy if needed.

    :param params: Current parameter values (will be modified in place)
    :param update: Parameter update operation to apply
    :return: The modified parameters dictionary
    :raises ValueError: If trying to set a nested value through a non-dict value in the path
    """
    jsonpath_expr = jsonpath_ng.parse(update.path)

    if update.op == 'set':
        try:
            matches = jsonpath_expr.find(params)
            if not matches:
                # path doesn't exist, create it manually
                set_nested_value(params, update.path, update.value)
            else:
                params = jsonpath_expr.update(params, update.value)
        except Exception as e:
            raise ValueError(f'Failed to set nested value at path "{update.path}": {e}')
        return params

    elif update.op == 'str_replace':

        if not update.search_for:
            raise ValueError('Search string is empty')

        if update.search_for == update.replace_with:
            raise ValueError(f'Search string and replace string are the same: "{update.search_for}"')

        matches = jsonpath_expr.find(params)

        if not matches:
            raise ValueError(f'Path "{update.path}" does not exist')

        replace_cnt = 0
        for match in matches:
            current_value = match.value
            if not isinstance(current_value, str):
                raise ValueError(f'Path "{match.full_path}" is not a string')

            new_value = current_value.replace(update.search_for, update.replace_with)
            if new_value != current_value:
                replace_cnt += 1
                params = match.full_path.update(params, new_value)

        if replace_cnt == 0:
            raise ValueError(f'Search string "{update.search_for}" not found in path "{update.path}"')

        return params

    elif update.op == 'remove':
        matches = jsonpath_expr.find(params)

        if not matches:
            raise ValueError(f'Path "{update.path}" does not exist')

        return jsonpath_expr.filter(lambda x: True, params)

    elif update.op == 'list_append':
        matches = jsonpath_expr.find(params)

        if not matches:
            raise ValueError(f'Path "{update.path}" does not exist')

        for match in matches:
            current_value = match.value
            if not isinstance(current_value, list):
                raise ValueError(f'Path "{match.full_path}" is not a list')

            current_value.append(update.value)

        return params


def update_params(params: dict[str, Any], updates: Sequence[ConfigParamUpdate]) -> dict[str, Any]:
    """
    Applies a list of parameter updates to the given parameters dictionary.
    The original dictionary is not modified.

    :param params: Current parameter values
    :param updates: Sequence of parameter update operations
    :return: New dictionary with all updates applied
    """
    # Create a deep copy to avoid mutating the original
    params = copy.deepcopy(params)
    for update in updates:
        params = _apply_param_update(params, update)
    return params


def _apply_tf_param_update(
    parameters: dict[str, Any], update: TfParamUpdate, sql_dialect: str
) -> tuple[dict[str, Any], str]:
    """
    Applies a single parameter update to the given transformation parameters.

    Note: This function modifies the input dictionary in place for efficiency.
    The caller (update_transformation_parameters) is responsible for creating a copy if needed.

    :param parameters: The transformation parameters
    :param update: Parameter update operation to apply
    :param sql_dialect: The SQL dialect of the transformation
    :return: Tuple of (updated transformation parameters, change summary message)
    """
    operation = update.op
    tf_update_func = getattr(tf_update, operation)
    return tf_update_func(params=parameters, op=update, sql_dialect=sql_dialect)


def add_ids(parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Adds IDs to the parameters dictionary.
    Blocks are numbered sequentially from 0.
    Codes are numbered sequentially from 0 within each block and prefixed with the block ID.

    :param parameters: Transformation parameters dictionary
    :return: Parameters dictionary with IDs added to blocks and codes
    """
    for bidx, block in enumerate(parameters['blocks']):
        block['id'] = f'b{bidx}'
        for cidx, code in enumerate(block['codes']):
            code['id'] = f'b{bidx}.c{cidx}'
    return parameters


def structure_summary(parameters: dict[str, Any]) -> str:
    """
    Generate a markdown summary of transformation structure showing block IDs, code IDs, and SQL snippets.

    :param parameters: Transformation parameters dictionary with blocks containing IDs
    :return: Markdown formatted summary of the transformation structure
    """
    lines = ['## Updated Transformation Structure', '']

    blocks = parameters.get('blocks', [])

    if not blocks:
        return '## Updated Transformation Structure\n\nNo blocks found in transformation.\n'

    for block in blocks:
        block_id = block['id']
        block_name = block.get('name', '')

        lines.append(f'### Block id: `{block_id}`, name: `{block_name}`')
        lines.append('')

        codes = block.get('codes', [])

        if not codes:
            lines.append('*No code blocks*')
            lines.append('')
            continue

        for code in codes:
            code_id = code['id']
            code_name = code.get('name', '')
            script = code.get('script', '')

            lines.append(f'- **Code id: `{code_id}`, name: `{code_name}`** SQL snippet:')
            lines.append('')

            # SQL snippet (first 150 characters)
            if script:
                snippet = script.strip()
                if len(snippet) > 150:
                    truncated_chars = len(snippet) - 150
                    snippet = snippet[:150] + f'... ({truncated_chars} chars truncated)'
                lines.append('  ```sql')
                lines.append(f'  {snippet}')
                lines.append('  ```')
            else:
                lines.append('  *Empty script*')

            lines.append('')

    return '\n'.join(lines)


def update_transformation_parameters(
    parameters: SimplifiedTfBlocks, updates: Sequence[TfParamUpdate], sql_dialect: str
) -> tuple[SimplifiedTfBlocks, str]:
    """
    Applies a list of parameter updates to the given transformation parameters.
    The original parameters are not modified.

    :param parameters: The transformation parameters
    :param updates: Sequence of parameter update operations
    :param sql_dialect: The SQL dialect of the transformation
    :return: The updated transformation parameters and a summary of the changes.
    """
    is_structure_change = any(update.op in tf_update.STRUCTURAL_OPS for update in updates)
    parameters_dict = add_ids(parameters.model_dump())
    messages = []
    for update in updates:
        parameters_dict, message = _apply_tf_param_update(
            parameters=parameters_dict, update=update, sql_dialect=sql_dialect
        )

        if message:
            messages.append(message)

    if is_structure_change:
        # re-assign IDs to reflect changes in the structure
        parameters_dict = add_ids(parameters_dict)
        messages.append(structure_summary(parameters_dict))

    change_summary = '\n'.join(messages)
    return SimplifiedTfBlocks.model_validate(parameters_dict, extra='ignore'), change_summary
