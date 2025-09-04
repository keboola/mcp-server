from typing import Any, Callable

import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components import (
    add_config_row,
    create_config,
    create_sql_transformation,
    get_config,
    get_config_examples,
    list_configs,
    list_transformations,
    update_config,
    update_config_row,
    update_sql_transformation,
)
from keboola_mcp_server.tools.components.model import (
    ComponentSummary,
    ComponentWithConfigurations,
    ConfigToolOutput,
    Configuration,
    ConfigurationRootSummary,
    ConfigurationSummary,
    ListConfigsOutput,
    ListTransformationsOutput,
)
from keboola_mcp_server.tools.components.utils import TransformationConfiguration, clean_bucket_name
from keboola_mcp_server.workspace import WorkspaceManager


@pytest.fixture
def assert_retrieve_components() -> Callable[
    [
        ListConfigsOutput | ListTransformationsOutput,
        list[dict[str, Any]],
        list[dict[str, Any]],
    ],
    None,
]:
    """Assert that the _retrieve_components_in_project tool returns the correct components and configurations."""

    def _assert_retrieve_components(
        result: ListConfigsOutput | ListTransformationsOutput,
        components: list[dict[str, Any]],
        configurations: list[dict[str, Any]],
    ):
        components_with_configurations = result.components_with_configurations

        assert len(components_with_configurations) == len(components)
        # assert basics
        assert all(isinstance(component, ComponentWithConfigurations) for component in components_with_configurations)
        assert all(isinstance(component.component, ComponentSummary) for component in components_with_configurations)
        assert all(isinstance(component.configurations, list) for component in components_with_configurations)
        assert all(
            all(isinstance(config, ConfigurationSummary) for config in component.configurations)
            for component in components_with_configurations
        )
        # assert component list details
        assert all(
            returned.component.component_id == expected['id']
            for returned, expected in zip(components_with_configurations, components)
        )
        assert all(
            returned.component.component_name == expected['name']
            for returned, expected in zip(components_with_configurations, components)
        )
        assert all(
            returned.component.component_type == expected['type']
            for returned, expected in zip(components_with_configurations, components)
        )
        assert all(not hasattr(returned.component, 'version') for returned in components_with_configurations)

        # assert configurations list details
        assert all(len(component.configurations) == len(configurations) for component in components_with_configurations)
        assert all(
            all(isinstance(config.configuration_root, ConfigurationRootSummary) for config in component.configurations)
            for component in components_with_configurations
        )
        # use zip to iterate over the result and mock_configurations since we artificially mock the .get method
        assert all(
            all(
                config.configuration_root.configuration_id == expected['id']
                for config, expected in zip(component.configurations, configurations)
            )
            for component in components_with_configurations
        )
        assert all(
            all(
                config.configuration_root.name == expected['name']
                for config, expected in zip(component.configurations, configurations)
            )
            for component in components_with_configurations
        )

    return _assert_retrieve_components


@pytest.mark.asyncio
async def test_list_configs_by_types(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_components: list[dict[str, Any]],
    mock_configurations: list[dict[str, Any]],
    assert_retrieve_components: Callable[[ListConfigsOutput, list[dict[str, Any]], list[dict[str, Any]]], None],
):
    """Test list_configs when component types are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    # mock the component_list method to return the mock_component with the mock_configurations
    # simulate the response from the API
    keboola_client.storage_client.component_list = mocker.AsyncMock(
        side_effect=[[{**component, 'configurations': mock_configurations}] for component in mock_components]
    )

    result = await list_configs(ctx=context, component_types=[])

    assert_retrieve_components(result, mock_components, mock_configurations)

    # Verify the calls were made with the correct arguments
    keboola_client.storage_client.component_list.assert_has_calls(
        [
            mocker.call(component_type='application', include=['configuration']),
            mocker.call(component_type='extractor', include=['configuration']),
            mocker.call(component_type='writer', include=['configuration']),
        ]
    )


@pytest.mark.asyncio
async def test_list_transformations(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configurations: list[dict[str, Any]],
    assert_retrieve_components: Callable[[ListTransformationsOutput, list[dict[str, Any]], list[dict[str, Any]]], None],
):
    """Test list_transformations."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    # mock the component_list method to return the mock_component with the mock_configurations
    # simulate the response from the API
    keboola_client.storage_client.component_list = mocker.AsyncMock(
        return_value=[{**mock_component, 'configurations': mock_configurations}]
    )

    result = await list_transformations(context)

    assert_retrieve_components(result, [mock_component], mock_configurations)

    # Verify the calls were made with the correct arguments
    keboola_client.storage_client.component_list.assert_called_once_with(
        component_type='transformation', include=['configuration']
    )


@pytest.mark.asyncio
async def test_list_configs_from_ids(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configurations: list[dict[str, Any]],
    mock_component: dict[str, Any],
    assert_retrieve_components: Callable[[ListConfigsOutput, list[dict[str, Any]], list[dict[str, Any]]], None],
):
    """Test list_configs when component IDs are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=mock_configurations)
    keboola_client.storage_client.component_detail = mocker.AsyncMock(return_value=mock_component)

    result = await list_configs(context, component_ids=[mock_component['id']])

    assert_retrieve_components(result, [mock_component], mock_configurations)

    # Verify the calls were made with the correct arguments
    keboola_client.storage_client.configuration_list.assert_called_once_with(component_id=mock_component['id'])
    keboola_client.storage_client.component_detail.assert_called_once_with(component_id=mock_component['id'])


@pytest.mark.asyncio
async def test_list_transformations_from_ids(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configurations: list[dict[str, Any]],
    mock_component: dict[str, Any],
    assert_retrieve_components: Callable[[ListTransformationsOutput, list[dict[str, Any]], list[dict[str, Any]]], None],
):
    """Test list_transformations when transformation IDs are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=mock_configurations)
    keboola_client.storage_client.component_detail = mocker.AsyncMock(return_value=mock_component)

    result = await list_transformations(context, transformation_ids=[mock_component['id']])

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configuration_list.assert_called_once_with(component_id=mock_component['id'])
    keboola_client.storage_client.component_detail.assert_called_once_with(component_id=mock_component['id'])


@pytest.mark.asyncio
async def test_get_config(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configuration: dict[str, Any],
    mock_component: dict[str, Any],
    mock_metadata: list[dict[str, Any]],
):
    """Test get_config tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    mock_ai_service = mocker.MagicMock()
    mock_ai_service.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    keboola_client.ai_service_client = mock_ai_service
    # mock the configuration_detail method to return the mock_configuration
    # simulate the response from the API
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(
        return_value={**mock_configuration, 'component': mock_component, 'configurationMetadata': mock_metadata}
    )

    result = await get_config(
        component_id=mock_component['id'],
        configuration_id=mock_configuration['id'],
        ctx=context,
    )

    assert isinstance(result, Configuration)
    assert result.configuration_root.configuration_id == mock_configuration['id']
    assert result.configuration_root.name == mock_configuration['name']
    assert result.component is not None
    assert result.component.component_id == mock_component['id']
    assert result.component.component_name == mock_component['name']
    assert set(result.links) == {
        Link(
            type='ui-detail',
            title=f'Configuration: {mock_configuration["name"]}',
            url=(
                f'https://connection.test.keboola.com/admin/projects/69420/components/'
                f'{mock_component["id"]}/{mock_configuration["id"]}'
            ),
        ),
        Link(
            type='ui-dashboard',
            title=f'{mock_component["id"]} Configurations Dashboard',
            url=f'https://connection.test.keboola.com/admin/projects/69420/components/{mock_component["id"]}',
        ),
    }

    # Verify the calls were made with the correct arguments
    keboola_client.storage_client.configuration_detail.assert_called_once_with(
        component_id=mock_component['id'], configuration_id=mock_configuration['id']
    )


@pytest.mark.parametrize(
    ('sql_dialect', 'expected_component_id', 'expected_configuration_id'),
    [
        ('Snowflake', 'keboola.snowflake-transformation', '1234'),
        ('BigQuery', 'keboola.google-bigquery-transformation', '5678'),
    ],
)
@pytest.mark.asyncio
async def test_create_sql_transformation(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    sql_dialect: str,
    expected_component_id: str,
    expected_configuration_id: str,
):
    """Test create_sql_transformation tool."""
    context = mcp_context_components_configs

    # Mock the WorkspaceManager
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)
    # Mock the KeboolaClient
    keboola_client = KeboolaClient.from_state(context.session.state)
    component = mock_component
    component['id'] = expected_component_id
    configuration = mock_configuration
    configuration['id'] = expected_configuration_id

    # Set up the mock for ai_service_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=component)
    keboola_client.storage_client.configuration_create = mocker.AsyncMock(return_value=configuration)

    transformation_name = mock_configuration['name']
    bucket_name = clean_bucket_name(transformation_name)
    description = mock_configuration['description']
    code_blocks = [
        TransformationConfiguration.Parameters.Block.Code(name='Code 0', sql_statements=['SELECT * FROM test']),
        TransformationConfiguration.Parameters.Block.Code(name='Code 1', sql_statements=['SELECT * FROM test2']),
    ]
    created_table_name = 'test_table_1'

    # Test the create_sql_transformation tool
    new_transformation_configuration = await create_sql_transformation(
        ctx=context,
        name=transformation_name,
        description=description,
        sql_code_blocks=code_blocks,
        created_table_names=[created_table_name],
    )

    assert isinstance(new_transformation_configuration, ConfigToolOutput)
    assert new_transformation_configuration.component_id == expected_component_id
    assert new_transformation_configuration.configuration_id == mock_configuration['id']
    assert new_transformation_configuration.description == mock_configuration['description']
    assert new_transformation_configuration.version == mock_configuration['version']

    keboola_client.storage_client.configuration_create.assert_called_once_with(
        component_id=expected_component_id,
        name=transformation_name,
        description=description,
        configuration={
            'parameters': {
                'blocks': [
                    {
                        'name': 'Blocks',
                        'codes': [{'name': code.name, 'script': code.sql_statements} for code in code_blocks],
                    }
                ]
            },
            'storage': {
                'input': {'tables': []},
                'output': {
                    'tables': [
                        {
                            'source': created_table_name,
                            'destination': f'out.c-{bucket_name}.{created_table_name}',
                        }
                    ]
                },
            },
        },
    )


@pytest.mark.parametrize('sql_dialect', ['Unknown'])
@pytest.mark.asyncio
async def test_create_sql_transformation_fail(
    mocker: MockerFixture,
    sql_dialect: str,
    mcp_context_components_configs: Context,
):
    """Test create_sql_transformation tool which should raise an error if the sql dialect is unknown."""
    context = mcp_context_components_configs
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)

    with pytest.raises(ValueError, match='Unsupported SQL dialect'):
        _ = await create_sql_transformation(
            ctx=context,
            name='test_name',
            description='test_description',
            sql_code_blocks=[
                TransformationConfiguration.Parameters.Block.Code(name='Code 0', sql_statements=['SELECT * FROM test'])
            ],
        )


@pytest.mark.parametrize(
    ('sql_dialect', 'expected_component_id', 'parameters', 'storage', 'expected_config'),
    [
        pytest.param(
            'Snowflake',
            'keboola.snowflake-transformation',
            {'blocks': [{'name': 'Blocks', 'codes': [{'name': 'Code 0', 'script': ['SELECT * FROM test']}]}]},
            {'output': {'tables': []}},
            {
                'parameters': {
                    'blocks': [{'name': 'Blocks', 'codes': [{'name': 'Code 0', 'script': ['SELECT * FROM test']}]}]
                },
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='snowflake_both_provided',
        ),
        pytest.param(
            'BigQuery',
            'keboola.google-bigquery-transformation',
            {'blocks': [{'name': 'Blocks', 'codes': [{'name': 'Code 0', 'script': ['SELECT * FROM test']}]}]},
            None,
            {
                'parameters': {
                    'blocks': [{'name': 'Blocks', 'codes': [{'name': 'Code 0', 'script': ['SELECT * FROM test']}]}]
                },
                'storage': {'input': {'tables': ['existing_table']}},
                'other_field': 'should_be_preserved',
            },
            id='bigquery_parameters_only',
        ),
        pytest.param(
            'Snowflake',
            'keboola.snowflake-transformation',
            None,
            {'output': {'tables': []}},
            {
                'parameters': {
                    'blocks': [{'name': 'Existing', 'codes': [{'name': 'Existing Code', 'script': ['SELECT 1']}]}]
                },
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='snowflake_storage_only',
        ),
    ],
)
@pytest.mark.asyncio
async def test_update_sql_transformation(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    sql_dialect: str,
    expected_component_id: str,
    parameters: dict[str, Any] | None,
    storage: dict[str, Any] | None,
    expected_config: dict[str, Any],
):
    """Test update_sql_transformation tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    # Mock the WorkspaceManager
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)

    existing_configuration = {
        'id': mock_configuration['id'],
        'name': 'Existing Transformation',
        'description': 'Existing description',
        'configuration': {
            'parameters': {
                'blocks': [{'name': 'Existing', 'codes': [{'name': 'Existing Code', 'script': ['SELECT 1']}]}]
            },
            'storage': {'input': {'tables': ['existing_table']}},
            'other_field': 'should_be_preserved',
        },
        'version': 1,
    }

    new_change_description = 'Test transformation update'
    updated_configuration = mock_configuration.copy()
    updated_configuration['version'] = 2
    mock_component['id'] = expected_component_id

    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value=existing_configuration)
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value=updated_configuration)
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    # Convert parameters dict to TransformationConfiguration.Parameters if provided
    transformation_parameters = None
    if parameters is not None:
        transformation_parameters = TransformationConfiguration.Parameters.model_validate(parameters)

    updated_result = await update_sql_transformation(
        context,
        mock_configuration['id'],
        new_change_description,
        parameters=transformation_parameters,
        storage=storage,
        updated_description=str(),
        is_disabled=False,
    )

    assert isinstance(updated_result, ConfigToolOutput)
    assert updated_result.component_id == expected_component_id
    assert updated_result.configuration_id == mock_configuration['id']
    assert updated_result.description == mock_configuration['description']
    assert updated_result.version == updated_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_with(component_id=expected_component_id)
    keboola_client.storage_client.configuration_update.assert_called_once_with(
        component_id=expected_component_id,
        configuration_id=mock_configuration['id'],
        change_description=new_change_description,
        configuration=expected_config,
        updated_description='',
        is_disabled=False,
    )


@pytest.mark.asyncio
async def test_get_config_examples(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
):
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Setup mock to return test data
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    text = await get_config_examples(component_id='keboola.ex-aws-s3', ctx=context)
    assert (
        text
        == """# Configuration Examples for `keboola.ex-aws-s3`

## Root Configuration Examples

1. Root Configuration:
```json
{
  "foo": "root"
}
```

## Row Configuration Examples

1. Row Configuration:
```json
{
  "foo": "row"
}
```

"""
    )


@pytest.mark.asyncio
async def test_create_config(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
):
    """Test create_component_root_configuration tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration = mock_configuration
    configuration['id'] = 'test-config-id'

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_create = mocker.AsyncMock(return_value=configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    name = 'Test Configuration'
    description = 'Test configuration description'
    parameters = {'test_param': 'test_value'}
    storage = {'input': {'tables': []}}

    # Test the create_component_root_configuration tool
    result = await create_config(
        ctx=context,
        name=name,
        description=description,
        component_id=component_id,
        parameters=parameters,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration['id']
    assert result.description == description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_create.assert_called_once_with(
        component_id=component_id,
        name=name,
        description=description,
        configuration={'storage': storage, 'parameters': parameters},
    )


@pytest.mark.asyncio
async def test_add_config_row(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
):
    """Test create_component_row_configuration tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration_id = 'test-config-id'
    row_configuration = {'id': 'test-row-id', 'name': 'Test Row', 'version': 1}

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_row_create = mocker.AsyncMock(return_value=row_configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    name = 'Test Row Configuration'
    description = 'Test row configuration description'
    parameters = {'row_param': 'row_value'}
    storage = {}

    # Test the create_component_row_configuration tool
    result = await add_config_row(
        ctx=context,
        name=name,
        description=description,
        component_id=component_id,
        configuration_id=configuration_id,
        parameters=parameters,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration_id
    assert result.description == description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == row_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_row_create.assert_called_once_with(
        component_id=component_id,
        config_id=configuration_id,
        name=name,
        description=description,
        configuration={'storage': storage, 'parameters': parameters},
    )


@pytest.mark.parametrize(
    ('parameters', 'storage', 'expected_config'),
    [
        pytest.param(
            {'updated_param': 'updated_value'},
            {'output': {'tables': []}},
            {
                'parameters': {'updated_param': 'updated_value'},
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='both_provided',
        ),
        pytest.param(
            {'updated_param': 'updated_value'},
            None,
            {
                'parameters': {'updated_param': 'updated_value'},
                'storage': {'input': {'tables': ['existing_table']}},
                'other_field': 'should_be_preserved',
            },
            id='parameters_only',
        ),
        pytest.param(
            None,
            {'output': {'tables': []}},
            {
                'parameters': {'existing_param': 'existing_value'},
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='storage_only',
        ),
    ],
)
@pytest.mark.asyncio
async def test_update_config(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    parameters: dict[str, Any] | None,
    storage: dict[str, Any] | None,
    expected_config: dict[str, Any],
):
    """Test update_component_root_configuration tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration_id = 'test-config-id'

    existing_configuration = {
        'id': configuration_id,
        'name': 'Existing Config',
        'description': 'Existing description',
        'configuration': {
            'parameters': {'existing_param': 'existing_value'},
            'storage': {'input': {'tables': ['existing_table']}},
            'other_field': 'should_be_preserved',
        },
        'version': 1,
    }

    updated_name = 'Updated Configuration'
    updated_description = 'Updated configuration description'
    updated_configuration = {
        **existing_configuration,
        'name': updated_name,
        'description': updated_description,
        'version': 2,
    }

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value=existing_configuration)
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value=updated_configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    change_description = 'Test update'

    # Test the update_component_root_configuration tool
    result = await update_config(
        ctx=context,
        name=updated_name,
        description=updated_description,
        change_description=change_description,
        component_id=component_id,
        configuration_id=configuration_id,
        parameters=parameters,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration_id
    assert result.description == updated_description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == updated_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_update.assert_called_once_with(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration=expected_config,
        change_description=change_description,
        updated_name=updated_name,
        updated_description=updated_description,
    )


@pytest.mark.parametrize(
    ('parameters', 'storage', 'expected_config'),
    [
        pytest.param(
            {'updated_param': 'updated_value'},
            {'output': {'tables': []}},
            {
                'parameters': {'updated_param': 'updated_value'},
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='both_provided',
        ),
        pytest.param(
            {'updated_param': 'updated_value'},
            None,
            {
                'parameters': {'updated_param': 'updated_value'},
                'storage': {'input': {'tables': ['existing_table']}},
                'other_field': 'should_be_preserved',
            },
            id='parameters_only',
        ),
        pytest.param(
            None,
            {'output': {'tables': []}},
            {
                'parameters': {'existing_param': 'existing_value'},
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='storage_only',
        ),
    ],
)
@pytest.mark.asyncio
async def test_update_config_row(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    parameters: dict[str, Any] | None,
    storage: dict[str, Any] | None,
    expected_config: dict[str, Any],
):
    """Test update_component_row_configuration tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration_id = 'test-config-id'
    configuration_row_id = 'test-row-id'

    existing_row_configuration = {
        'configuration': {
            'parameters': {'existing_param': 'existing_value'},
            'storage': {'input': {'tables': ['existing_table']}},
            'other_field': 'should_be_preserved',
        },
        'version': 1,
    }

    updated_name = 'Updated Row Configuration'
    updated_description = 'Updated row configuration description'
    updated_row_configuration = {
        **existing_row_configuration,
        'name': updated_name,
        'description': updated_description,
        'version': 2,
    }

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_row_detail = mocker.AsyncMock(return_value=existing_row_configuration)
    keboola_client.storage_client.configuration_row_update = mocker.AsyncMock(return_value=updated_row_configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    change_description = 'Test row update'

    # Test the update_component_row_configuration tool
    result = await update_config_row(
        ctx=context,
        name=updated_name,
        description=updated_description,
        change_description=change_description,
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_row_id=configuration_row_id,
        parameters=parameters,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration_id
    assert result.description == updated_description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == updated_row_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_row_update.assert_called_once_with(
        component_id=component_id,
        config_id=configuration_id,
        configuration_row_id=configuration_row_id,
        configuration=expected_config,
        change_description=change_description,
        updated_name=updated_name,
        updated_description=updated_description,
    )
