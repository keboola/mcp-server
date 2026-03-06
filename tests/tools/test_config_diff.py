import copy

import pytest
from fastmcp import Context

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.config_diff import preview_config_diff

ORIGINAL_CONFIG = {
    'id': 'config-123',
    'name': 'My Config',
    'description': 'Original description',
    'configuration': {
        'parameters': {
            'bucket': 's3-bucket',
            'prefix': '/data',
        }
    },
}


@pytest.mark.asyncio
async def test_preview_config_diff_returns_valid_diff(mcp_context_client: Context, mocker):
    """Test that a valid update_config preview returns originalConfig and updatedConfig."""
    mock_client = KeboolaClient.from_state(mcp_context_client.session.state)

    async def mock_config_detail(**kwargs):
        return copy.deepcopy(ORIGINAL_CONFIG)

    mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

    from keboola_mcp_server.clients.storage import ComponentAPIResponse

    async def mock_fetch_component(**kwargs):
        return ComponentAPIResponse.model_validate(
            {
                'id': 'keboola.ex-test',
                'name': 'Test',
                'type': 'extractor',
                'configurationSchema': {},
                'component_flags': [],
            }
        )

    mocker.patch(
        'keboola_mcp_server.tools.components.tools.fetch_component',
        side_effect=mock_fetch_component,
    )

    result = await preview_config_diff(
        ctx=mcp_context_client,
        tool_name='update_config',
        tool_params={
            'component_id': 'keboola.ex-test',
            'configuration_id': 'config-123',
            'change_description': 'Update bucket',
            'parameter_updates': [
                {'op': 'set', 'path': 'bucket', 'value': 'new-bucket'},
            ],
        },
    )

    assert result['isValid'] is True
    assert result['coordinates']['componentId'] == 'keboola.ex-test'
    assert result['coordinates']['configurationId'] == 'config-123'
    assert result['originalConfig']['configuration']['parameters']['bucket'] == 's3-bucket'
    assert result['updatedConfig']['configuration']['parameters']['bucket'] == 'new-bucket'


@pytest.mark.asyncio
async def test_preview_config_diff_unsupported_tool(mcp_context_client: Context):
    """Test that an unsupported tool_name returns isValid=False."""
    result = await preview_config_diff(
        ctx=mcp_context_client,
        tool_name='create_config',
        tool_params={},
    )

    assert result['isValid'] is False
    assert len(result['validationErrors']) == 1
    assert 'Unsupported tool_name' in result['validationErrors'][0]


@pytest.mark.asyncio
async def test_preview_config_diff_mutator_error(mcp_context_client: Context, mocker):
    """Test that a mutator error returns isValid=False with error message."""
    mock_client = KeboolaClient.from_state(mcp_context_client.session.state)

    async def mock_config_detail(**kwargs):
        raise ValueError('Configuration not found')

    mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

    from keboola_mcp_server.clients.storage import ComponentAPIResponse

    async def mock_fetch_component(**kwargs):
        return ComponentAPIResponse.model_validate(
            {
                'id': 'keboola.ex-test',
                'name': 'Test',
                'type': 'extractor',
                'configurationSchema': {},
                'component_flags': [],
            }
        )

    mocker.patch(
        'keboola_mcp_server.tools.components.tools.fetch_component',
        side_effect=mock_fetch_component,
    )

    result = await preview_config_diff(
        ctx=mcp_context_client,
        tool_name='update_config',
        tool_params={
            'component_id': 'keboola.ex-test',
            'configuration_id': 'config-123',
            'change_description': 'Test',
            'parameter_updates': [{'op': 'set', 'path': 'x', 'value': 'y'}],
        },
    )

    assert result['isValid'] is False
    assert 'Configuration not found' in result['validationErrors'][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'tool_name',
    [
        'update_config',
        'update_config_row',
        'update_sql_transformation',
        'update_flow',
        'modify_flow',
        'modify_data_app',
    ],
)
async def test_preview_config_diff_supported_tools(mcp_context_client: Context, tool_name: str):
    """Test that all 6 supported tool names are accepted (not rejected as unsupported)."""
    try:
        result = await preview_config_diff(
            ctx=mcp_context_client,
            tool_name=tool_name,
            tool_params={},
        )
    except Exception:
        # Mutator errors (e.g. TypeError for missing required params) are
        # expected when tool_params is empty.  The important thing is that
        # we did NOT get the early "Unsupported tool_name" rejection.
        return

    # If a result was returned, verify it is not the unsupported-tool error.
    if not result['isValid'] and result.get('validationErrors'):
        assert 'Unsupported tool_name' not in result['validationErrors'][0]
