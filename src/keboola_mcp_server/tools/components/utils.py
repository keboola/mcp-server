import logging
from typing import Any, Optional, Sequence, Union, cast, get_args

from httpx import HTTPStatusError
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.components.model import (
    AllComponentTypes,
    Component,
    ComponentType,
    ComponentWithConfigurations,
    ReducedComponent,
    ReducedComponentConfiguration,
)

LOG = logging.getLogger(__name__)


def _handle_component_types(
    types: Optional[Union[ComponentType, Sequence[ComponentType]]],
) -> Sequence[ComponentType]:
    """
    Utility function to handle the component types [extractors, writers, applications, all]
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
    Utility function to retrieve components with configurations by types - used in tools:
    - retrieve_components_configurations
    - retrieve_transformation_configurations
    :param client: The Keboola client
    :param component_types: The component types/type to retrieve
    :return: a list of items, each containing a component and its associated configurations
    """

    endpoint = f'branch/{client.storage_client_sync._branch_id}/components'
    # retrieve components by types - unable to use list of types as parameter, we need to iterate over types

    raw_components_with_configurations = []
    for type in component_types:
        # retrieve components by type with configurations
        params = {
            'include': 'configuration',
            'componentType': type,
        }
        raw_components_with_configurations_by_type = cast(
            list[dict[str, Any]], await client.storage_client.get(endpoint, params=params)
        )
        # extend the list with the raw components with configurations
        raw_components_with_configurations.extend(raw_components_with_configurations_by_type)

    # build components with configurations list, each item contains a component and its associated configurations
    components_with_configurations = [
        ComponentWithConfigurations(
            component=ReducedComponent.model_validate(raw_component),
            configurations=[
                ReducedComponentConfiguration.model_validate({**raw_configuration, 'component_id': raw_component['id']})
                for raw_configuration in raw_component.get('configurations', [])
            ],
        )
        for raw_component in raw_components_with_configurations
    ]

    # perform logging
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
    Utility function to retrieve components with configurations by component IDs - used in tools:
    - retrieve_components_configurations
    - retrieve_transformation_configurations
    :param client: The Keboola client
    :param component_ids: The component IDs to retrieve
    :return: a list of items, each containing a component and its associated configurations
    """
    components_with_configurations = []
    for component_id in component_ids:
        # retrieve configurations for component ids
        raw_configurations = client.storage_client_sync.configurations.list(component_id)
        # retrieve component details
        endpoint = f'branch/{client.storage_client_sync._branch_id}/components/{component_id}'
        raw_component = await client.storage_client.get(endpoint)
        # build component configurations list grouped by components
        components_with_configurations.append(
            ComponentWithConfigurations(
                component=ReducedComponent.model_validate(raw_component),
                configurations=[
                    ReducedComponentConfiguration.model_validate(
                        {**raw_configuration, 'component_id': raw_component['id']}
                    )
                    for raw_configuration in raw_configurations
                ],
            )
        )

    # perform logging
    total_configurations = sum(len(component.configurations) for component in components_with_configurations)
    LOG.info(
        f'Found {len(components_with_configurations)} components with total of {total_configurations} configurations '
        f'for ids {component_ids}.'
    )
    return components_with_configurations


async def _get_component_details(
    client: KeboolaClient,
    component_id: str,
) -> Component:
    """
    Utility function to retrieve the component details by component ID, used in tools:
    - get_component_configuration_details

    First tries to get component details from the AI service catalog. If the component
    is not found (404) or returns empty data (private components), falls back to using the
    Storage API endpoint.

    :param component_id: The ID of the Keboola component/transformation you want details about
    :param client: The Keboola client
    :return: The component details
    """
    try:
        raw_component = await client.ai_service_client.get_component_detail(component_id)
        LOG.info(f'Retrieved component details for component {component_id} from AI service catalog.')
        return Component.model_validate(raw_component)
    except HTTPStatusError as e:
        if e.response.status_code == 404:
            LOG.info(
                f'Component {component_id} not found in AI service catalog (possibly private). '
                f'Falling back to Storage API.'
            )

            endpoint = f'branch/{client.storage_client_sync._branch_id}/components/{component_id}'
            raw_component = await client.storage_client.get(endpoint)
            LOG.info(f'Retrieved component details for component {component_id} from Storage API.')
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
        return 'keboola.bigquery-transformation'
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

            tables: list[Table] = Field(description='The tables used in the transformation', default=[])

        input: Destination = Field(description='The input tables for the transformation', default=Destination())
        output: Destination = Field(description='The output tables for the transformation', default=Destination())

    parameters: Parameters = Field(description='The parameters for the transformation')
    storage: Storage = Field(description='The storage configuration for the transformation')


def _get_transformation_configuration(
    statements: Sequence[str], transformation_name: str, output_tables: Sequence[str]
) -> TransformationConfiguration:
    """
    Utility function to set the transformation configuration from code statements.
    It creates the expected configuration for the transformation, parameters and storage.
    :param statements: The code statements (sql for now)
    :param transformation_name: The name of the transformation from which the bucket name is derived as in the UI
    :param output_tables: The output tables of the transformation, created by the code statements
    :return: dictionary with parameters and storage following the TransformationConfiguration schema
    """
    # init Storage Configuration with empty input and output tables
    storage = TransformationConfiguration.Storage()
    # build parameters configuration out of code statements
    parameters = TransformationConfiguration.Parameters(
        blocks=[
            TransformationConfiguration.Parameters.Block(
                name='Block 0',
                codes=[TransformationConfiguration.Parameters.Block.Code(name='Code 0', script=list(statements))],
            )
        ]
    )
    if output_tables:
        # if the query creates new tables, output_table_mappings should contain the table names (llm generated)
        # we create bucket name from the sql query name adding `out.c-` prefix as in the UI and use it as destination
        # expected output table name format is `out.c-<sql_query_name>.<table_name>`
        bucket_name = '-'.join(transformation_name.lower().split())
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
