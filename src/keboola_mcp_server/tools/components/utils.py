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
from typing import Any, Optional, Sequence, Union, cast, get_args

import jsonpath_ng
from httpx import HTTPStatusError
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import ComponentAPIResponse, ConfigurationAPIResponse
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.components.model import (
    AllComponentTypes,
    ComponentSummary,
    ComponentType,
    ComponentWithConfigurations,
    ConfigParamUpdate,
    ConfigurationSummary,
)

LOG = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

SNOWFLAKE_TRANSFORMATION_ID = 'keboola.snowflake-transformation'
BIGQUERY_TRANSFORMATION_ID = 'keboola.google-bigquery-transformation'


# ============================================================================
# COMPONENT TYPE HANDLING
# ============================================================================


def handle_component_types(
    types: Optional[Union[ComponentType, Sequence[ComponentType]]],
) -> Sequence[ComponentType]:
    """
    Utility function to handle the component types [extractors, writers, applications, all].
    If the types include "all", it will be removed and the remaining types will be returned.

    :param types: The component types/type to process.
    :return: The processed component types.
    """
    if not types:
        return [component_type for component_type in get_args(ComponentType)]
    if isinstance(types, str):
        types = [types]
    return types


# ============================================================================
# CONFIGURATION LISTING UTILITIES
# ============================================================================


async def list_configs_by_types(
    client: KeboolaClient, component_types: Sequence[AllComponentTypes]
) -> list[ComponentWithConfigurations]:
    """
    Retrieve components with their configurations filtered by component types.

    Used by:
    - list_configs tool
    - list_transformations tool

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
    Retrieve components with their configurations filtered by specific component IDs.

    Used by:
    - list_configs tool (when specific component IDs are requested)
    - list_transformations tool (when specific transformation IDs are requested)

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
    Utility function to fetch a component by ID, returning the raw API response.

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
    Utility function to clean the bucket name.
    Converts the bucket name to ASCII. (Handle diacritics like český -> cesky)
    Converts spaces to dashes.
    Removes leading underscores, dashes, and whitespace.
    Removes any character that is not alphanumeric, dash, or underscore.
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


# ============================================================================
# DATA MODELS
# ============================================================================


class TransformationConfiguration(BaseModel):
    """
    Utility class to create the transformation configuration, a schema for the transformation configuration in the API.
    Currently, the storage configuration uses only input and output tables, excluding files, etc.
    """

    class Parameters(BaseModel):
        """The parameters for the transformation."""

        class Block(BaseModel):
            """The transformation block."""

            class Code(BaseModel):
                """The code block for the transformation block."""

                name: str = Field(description='The name of the current code block describing the purpose of the block')
                sql_statements: Sequence[str] = Field(
                    description=(
                        'The executable SQL query statements written in the current SQL dialect. '
                        'Each statement must be executable and a separate item in the list.'
                    ),
                    # We use sql_statements for readability but serialize to script due to api expected request
                    serialization_alias='script',
                    validation_alias=AliasChoices('sql_statements', 'script'),
                )

            name: str = Field(description='The name of the current block')
            codes: list[Code] = Field(description='The code scripts')

        blocks: list[Block] = Field(description='The blocks for the transformation')

    class Storage(BaseModel):
        """The storage configuration for the transformation. For now it stores only input and output tables."""

        class Destination(BaseModel):
            """Tables' destinations for the transformation. Either input or output tables."""

            class Table(BaseModel):
                """The table used in the transformation"""

                destination: Optional[str] = Field(description='The destination table name', default=None)
                source: Optional[str] = Field(description='The source table name', default=None)

            tables: list[Table] = Field(description='The tables used in the transformation', default_factory=list)

        input: Destination = Field(description='The input tables for the transformation', default_factory=Destination)
        output: Destination = Field(description='The output tables for the transformation', default_factory=Destination)

    parameters: Parameters = Field(description='The parameters for the transformation')
    storage: Storage = Field(description='The storage configuration for the transformation')


def get_transformation_configuration(
    codes: Sequence[TransformationConfiguration.Parameters.Block.Code],
    transformation_name: str,
    output_tables: Sequence[str],
) -> TransformationConfiguration:
    """
    Utility function to set the transformation configuration from code statements.
    It creates the expected configuration for the transformation, parameters and storage.

    :param statements: The code blocks (sql for now)
    :param transformation_name: The name of the transformation from which the bucket name is derived as in the UI
    :param output_tables: The output tables of the transformation, created by the code statements
    :return: Dictionary with parameters and storage following the TransformationConfiguration schema
    """
    storage = TransformationConfiguration.Storage()
    # build parameters configuration out of code blocks
    parameters = TransformationConfiguration.Parameters(
        blocks=[
            TransformationConfiguration.Parameters.Block(
                name='Blocks',
                codes=list(codes),
            )
        ]
    )
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
    return TransformationConfiguration(parameters=parameters, storage=storage)


async def set_cfg_creation_metadata(client: KeboolaClient, component_id: str, configuration_id: str) -> None:
    """
    Sets configuration metadata to indicate it was created by MCP.

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
    Sets configuration metadata to indicate it was updated by MCP.

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


def _set_nested_value(data: dict[str, Any], path: str, value: Any) -> None:
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
                _set_nested_value(params, update.path, update.new_val)
            else:
                params = jsonpath_expr.update(params, update.new_val)
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
