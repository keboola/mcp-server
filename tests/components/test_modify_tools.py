
from typing import Any, Callable, Sequence, Union
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.components.model import (
    ComponentConfiguration,
)
from keboola_mcp_server.components.modify_tools import (
    create_sql_transformation,
)
from keboola_mcp_server.sql_tools import WorkspaceManager

@pytest.mark.parametrize(
    "sql_dialect, expected_component_id, expected_configuration_id",
    [
        ("Snowflake", "keboola.snowflake-transformation", "1234"),
        ("BigQuery", "keboola.bigquery-transformation", "5678"),
    ],
)
@pytest.mark.asyncio
async def test_create_transformation_configuration(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    sql_dialect: str,
    expected_component_id: str,
    expected_configuration_id: str,
    mock_branch_id: str,
):
    """Test create_transformation_configuration tool."""
    context = mcp_context_components_configs

    # Mock the WorkspaceManager
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = AsyncMock(return_value=sql_dialect)
    # Mock the KeboolaClient
    keboola_client = KeboolaClient.from_state(context.session.state)
    component = mock_component
    component["id"] = expected_component_id
    configuration = mock_configuration
    configuration["id"] = expected_configuration_id

    keboola_client.get = AsyncMock(return_value=component)
    keboola_client.post = AsyncMock(return_value=configuration)

    transformation_name = mock_configuration["name"]
    bucket_name = "-".join(transformation_name.lower().split())
    description = mock_configuration["description"]
    sql_statements = ["SELECT * FROM test", "SELECT * FROM test2"]
    created_table_name = "test_table_1"

    # Test the create_sql_transformation tool
    new_transformation_configuration = await create_sql_transformation(
        context,
        transformation_name,
        description,
        sql_statements,
        created_table_names=[created_table_name],
    )
    assert isinstance(new_transformation_configuration, ComponentConfiguration)
    assert new_transformation_configuration.component is not None
    assert new_transformation_configuration.component.component_id == expected_component_id
    assert new_transformation_configuration.component_id == expected_component_id
    assert new_transformation_configuration.configuration_id == expected_configuration_id
    assert new_transformation_configuration.configuration_name == transformation_name
    assert new_transformation_configuration.configuration_description == description

    keboola_client.get.assert_called_once_with(
        f"branch/{mock_branch_id}/components/{expected_component_id}"
    )

    keboola_client.post.assert_called_once_with(
        f"branch/{mock_branch_id}/components/{expected_component_id}/configs",
        data={
            "name": transformation_name,
            "description": description,
            "configuration": {
                "parameters": {
                    "blocks": [
                        {
                            "name": "Block 0",
                            "codes": [{"name": "Code 0", "script": sql_statements}],
                        }
                    ]
                },
                "storage": {
                    "input": {"tables": []},
                    "output": {
                        "tables": [
                            {
                                "source": created_table_name,
                                "destination": f"out.c-{bucket_name}.{created_table_name}",
                            }
                        ]
                    },
                },
            },
        },
    )


@pytest.mark.parametrize("sql_dialect", ["Unknown"])
@pytest.mark.asyncio
async def test_create_transformation_configuration_fail(
    sql_dialect: str,
    mcp_context_components_configs: Context,
):
    """Test create_sql_transformation tool which should raise an error if the sql dialect is unknown."""
    context = mcp_context_components_configs
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = AsyncMock(return_value=sql_dialect)

    with pytest.raises(ValueError):
        _ = await create_sql_transformation(
            context,
            "test_name",
            "test_description",
            "SELECT * FROM test",
        )
