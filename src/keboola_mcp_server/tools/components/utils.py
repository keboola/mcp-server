import logging
import re
import unicodedata
from typing import Any, Optional, Sequence, Union, cast, get_args

from httpx import HTTPStatusError
from pydantic import BaseModel, Field

from keboola_mcp_server.client import JsonDict, KeboolaClient
from keboola_mcp_server.tools._validate import validate_parameters, validate_storage
from keboola_mcp_server.tools.components.model import (
    AllComponentTypes,
    Component,
    ComponentConfigurationMetadata,
    ComponentConfigurationResponse,
    ComponentType,
    ComponentWithConfigurations,
    ReducedComponent,
)

LOG = logging.getLogger(__name__)


def _handle_component_types(
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


async def _retrieve_components_configurations_by_types(
    client: KeboolaClient, component_types: Sequence[AllComponentTypes]
) -> list[ComponentWithConfigurations]:
    """
    Utility function to retrieve components with configurations by types.

    Used in tools:
    - retrieve_components_configurations
    - retrieve_transformation_configurations

    :param client: The Keboola client
    :param component_types: The component types/type to retrieve
    :return: A list of items, each containing a component and its associated configurations
    """
    endpoint = f'branch/{client.storage_client.branch_id}/components'
    # retrieve components by types - unable to use list of types as parameter, we need to iterate over types

    components_with_configurations = []

    for _type in component_types:
        params = {
            'include': 'configuration',
            'componentType': _type,
        }
        raw_components_with_configurations_by_type = cast(
            list[dict[str, Any]], await client.storage_client.get(endpoint, params=params)
        )
        # extend the list with the raw components with configurations
        # TODO: ugly, refactor
        for raw_component in raw_components_with_configurations_by_type:
            # build components with configurations list, each item contains a component and its
            # associated configurations
            raw_configuration_responses = [
                ComponentConfigurationResponse.model_validate(
                    {**raw_configuration, 'component_id': raw_component['id']}
                )
                for raw_configuration in raw_component.get('configurations', [])
            ]
            configurations_metadata = [
                ComponentConfigurationMetadata.from_component_configuration_response(raw_response)
                for raw_response in raw_configuration_responses
            ]

            components_with_configurations.append(
                ComponentWithConfigurations(
                    component=ReducedComponent.model_validate(raw_component),
                    configurations=configurations_metadata,
                )
            )

    total_configurations = sum(len(component.configurations) for component in components_with_configurations)
    LOG.info(
        f'Found {len(components_with_configurations)} components with total of {total_configurations} configurations '
        f'for types {component_types}.'
    )
    return components_with_configurations


async def _retrieve_components_configurations_by_ids(
    client: KeboolaClient, component_ids: Sequence[str]
) -> list[ComponentWithConfigurations]:
    """
    Utility function to retrieve components with configurations by component IDs.

    Used in tools:
    - retrieve_components_configurations
    - retrieve_transformation_configurations

    :param client: The Keboola client
    :param component_ids: The component IDs to retrieve
    :return: A list of items, each containing a component and its associated configurations
    """
    components_with_configurations = []
    for component_id in component_ids:
        # retrieve configurations for component ids
        raw_configurations = await client.storage_client.configuration_list(component_id=component_id)
        # retrieve components
        raw_component = cast(
            JsonDict,
            await client.storage_client.get(
                endpoint=f'branch/{client.storage_client.branch_id}/components/{component_id}'
            ),
        )
        # build component configurations list grouped by components
        raw_configuration_responses = [
            ComponentConfigurationResponse.model_validate({**raw_configuration, 'component_id': raw_component['id']})
            for raw_configuration in raw_configurations
        ]
        configurations_metadata = [
            ComponentConfigurationMetadata.from_component_configuration_response(raw_response)
            for raw_response in raw_configuration_responses
        ]

        components_with_configurations.append(
            ComponentWithConfigurations(
                component=ReducedComponent.model_validate(raw_component),
                configurations=configurations_metadata,
            )
        )

    total_configurations = sum(len(component.configurations) for component in components_with_configurations)
    LOG.info(
        f'Found {len(components_with_configurations)} components with total of {total_configurations} configurations '
        f'for ids {component_ids}.'
    )
    return components_with_configurations


async def _get_component(
    client: KeboolaClient,
    component_id: str,
) -> Component:
    """
    Utility function to retrieve a component by ID.

    First tries to get component from the AI service catalog. If the component
    is not found (404) or returns empty data (private components), falls back to using the
    Storage API endpoint.

    Used in tools:
    - get_component_configuration_details

    :param client: The Keboola client
    :param component_id: The ID of the component to retrieve
    :return: The component
    """
    try:
        raw_component = await client.ai_service_client.get_component_detail(component_id=component_id)
        LOG.info(f'Retrieved component {component_id} from AI service catalog.')
        return Component.model_validate(raw_component)
    except HTTPStatusError as e:
        if e.response.status_code == 404:
            LOG.info(
                f'Component {component_id} not found in AI service catalog (possibly private). '
                f'Falling back to Storage API.'
            )

            endpoint = f'branch/{client.storage_client.branch_id}/components/{component_id}'
            raw_component = await client.storage_client.get(endpoint=endpoint)
            LOG.info(f'Retrieved component {component_id} from Storage API.')
            return Component.model_validate(raw_component)
        else:
            # If it's not a 404, re-raise the error
            raise


def _get_sql_transformation_id_from_sql_dialect(
    sql_dialect: str,
) -> str:
    """
    Utility function to retrieve the SQL transformation ID from the given SQL dialect.

    :param sql_dialect: The SQL dialect
    :return: The SQL transformation ID
    :raises ValueError: If the SQL dialect is not supported
    """
    if sql_dialect.lower() == 'snowflake':
        return 'keboola.snowflake-transformation'
    elif sql_dialect.lower() == 'bigquery':
        return 'keboola.google-bigquery-transformation'
    else:
        raise ValueError(f'Unsupported SQL dialect: {sql_dialect}')


class TransformationConfiguration(BaseModel):
    """
    Utility class to create the transformation configuration, a schema for the transformation configuration in the API.
    Currently, the storage configuration uses only input and output tables, excluding files, etc.
    """

    class Parameters(BaseModel):
        """The parameters for the transformation."""

        class Block(BaseModel):
            """The block for the transformation."""

            class Code(BaseModel):
                """The code for the transformation block."""

                name: str = Field(description='The name of the current code script')
                script: list[str] = Field(description='List of current code statements')

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


def _clean_bucket_name(bucket_name: str) -> str:
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


def _get_transformation_configuration(
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
        bucket_name = _clean_bucket_name(transformation_name)
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


STORAGE_VALIDATION_INITIAL_MESSAGE = 'The provided storage configuration input does not follow the storage schema.\n'
ROOT_PARAMETERS_VALIDATION_INITIAL_MESSAGE = (
    'The provided Root parameters configuration input does not follow the Root parameter json schema for component '
    'id: {component_id}.\n'
)
ROW_PARAMETERS_VALIDATION_INITIAL_MESSAGE = (
    'The provided Row parameters configuration input does not follow the Row parameter json schema for component '
    'id: {component_id}.\n'
)


def validate_storage_configuration(
    storage: Optional[JsonDict],
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Validates the storage configuration and extracts the storage key contents.
    :param storage: The storage configuration to validate received from the agent.
    :param initial_message: The initial message to include in the error message.
    :return: The contents of the 'storage' key from the validated configuration,
              or an empty dict if no storage is provided.
    """
    if not storage or storage is None or storage.get('storage', {}) is None:
        LOG.warning('No storage configuration provided, skipping validation.')
        return {}
    initial_message = (initial_message or '') + '\n'
    initial_message += STORAGE_VALIDATION_INITIAL_MESSAGE
    normalized_storage = validate_storage(storage, initial_message)
    return cast(JsonDict, normalized_storage['storage'])


async def validate_root_parameters_configuration(
    client: KeboolaClient,
    parameters: JsonDict,
    component_id: str,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the root parameters configuration.
    :param client: The Keboola client
    :param parameters: The parameters of the configuration to validate
    :param component_id: The ID of the component for which the configuration is provided
    :param initial_message: The initial message to include in the error message
    :return: The contents of the 'parameters' key from the validated configuration
    """
    initial_message = (initial_message or '') + '\n'
    initial_message += ROOT_PARAMETERS_VALIDATION_INITIAL_MESSAGE.format(component_id=component_id)
    component = await _get_component(client=client, component_id=component_id)
    return _validate_parameters_configuration(parameters, component.configuration_schema, component_id, initial_message)


async def validate_row_parameters_configuration(
    client: KeboolaClient,
    parameters: JsonDict,
    component_id: str,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the row parameters configuration.
    :param client: The Keboola client
    :param parameters: The parameters of the configuration to validate
    :param component_id: The ID of the component for which the configuration is provided
    :param initial_message: The initial message to include in the error message
    :return: The contents of the 'parameters' key from the validated configuration
    """
    initial_message = (initial_message or '') + '\n'
    initial_message += ROW_PARAMETERS_VALIDATION_INITIAL_MESSAGE.format(component_id=component_id)
    component = await _get_component(client=client, component_id=component_id)
    return _validate_parameters_configuration(
        parameters, component.configuration_row_schema, component_id, initial_message
    )


def _validate_parameters_configuration(
    parameters: JsonDict,
    schema: Optional[JsonDict],
    component_id: str,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the parameters configuration.
    :param parameters: The parameters configuration to validate
    :param schema: The schema to validate against
    :param component_id: The ID of the component
    :param initial_message: The initial message to include in the error message
    :return: The contents of the 'parameters' key from the validated configuration
    """
    if not schema:
        LOG.warning(f'No schema provided for component {component_id}, skipping validation.')
        return parameters

    # we expect the parameters to be a dictionary of parameter configurations without the "parameters" key
    normalized_parameters = validate_parameters(parameters, schema, initial_message)
    return cast(JsonDict, normalized_parameters['parameters'])
