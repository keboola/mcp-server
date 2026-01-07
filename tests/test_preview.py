import copy
from typing import Generator

import pytest
import pytest_asyncio
from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.testclient import TestClient

from keboola_mcp_server import cli
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.preview import preview_config_diff
from keboola_mcp_server.server import create_server


@pytest_asyncio.fixture
async def starlette_app() -> Starlette:
    """Create a Starlette app with the preview endpoint."""
    config = Config(
        storage_token='test-token',
        storage_api_url='https://connection.test.keboola.com',
        workspace_schema='test-workspace',
    )
    server_state = ServerState(config=config, runtime_info=ServerRuntimeInfo(transport='stdio'))
    mcp_server = create_server(config, runtime_info=server_state.runtime_info)
    assert isinstance(mcp_server, FastMCP)

    app = Starlette(exception_handlers=cli._exception_handlers)
    app.state.server_state = server_state
    app.state.mcp_tools_input_schema = {tool.name: tool.parameters for tool in (await mcp_server.get_tools()).values()}

    app.add_route('/preview/configuration', preview_config_diff, methods=['POST'])

    return app


@pytest.fixture
def test_client(starlette_app: Starlette) -> Generator[TestClient, None, None]:
    """Create a test client for the Starlette app."""
    with TestClient(starlette_app) as client:
        yield client


class TestPreviewConfigDiff:
    """Tests for the POST /preview/configuration endpoint."""

    def test_preview_update_config_success(self, test_client: TestClient, mocker):
        """Test successful preview of update_config tool."""
        # Mock the storage client's configuration_detail method
        original_config_data = {
            'id': 'config-123',
            'name': 'Original Config Name',
            'description': 'Original description',
            'configuration': {
                'parameters': {
                    'foo': 'bar',
                    'baz': 42,
                }
            },
        }

        # Mock fetch_component to return a ComponentAPIResponse
        from keboola_mcp_server.clients.storage import ComponentAPIResponse

        async def mock_fetch_component(**kwargs):
            return ComponentAPIResponse.model_validate(
                {
                    'id': 'keboola.ex-test',
                    'name': 'Test Extractor',
                    'type': 'extractor',
                    'configurationSchema': {},
                    'component_flags': [],
                }
            )

        mocker.patch(
            'keboola_mcp_server.tools.components.tools.fetch_component',
            side_effect=mock_fetch_component,
        )

        # Mock the KeboolaClient.from_state to return a mocked client
        mock_client = mocker.AsyncMock(KeboolaClient)

        # Properly mock async method
        async def mock_config_detail(**kwargs):
            return copy.deepcopy(original_config_data)

        mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Request payload
        request_payload = {
            'toolName': 'update_config',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                'configuration_id': 'config-123',
                'change_description': 'Test change',
                'name': 'Updated Config Name',
                'description': 'Updated description',
                'parameter_updates': [
                    {'op': 'set', 'path': 'foo', 'value': 'updated_bar'},
                    {'op': 'set', 'path': 'new_param', 'value': 'new_value'},
                ],
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()

        # Check response structure
        assert 'coordinates' in result
        assert 'originalConfig' in result
        assert 'updatedConfig' in result
        assert 'isValid' in result

        # Check coordinates
        assert result['coordinates']['componentId'] == 'keboola.ex-test'
        assert result['coordinates']['configurationId'] == 'config-123'
        assert 'configurationRowId' not in result['coordinates']

        # Check that isValid is True
        assert result['isValid'] is True
        assert 'validationErrors' not in result

        # Check original config
        assert result['originalConfig']['id'] == 'config-123'
        assert result['originalConfig']['name'] == 'Original Config Name'
        assert result['originalConfig']['description'] == 'Original description'
        assert result['originalConfig']['configuration']['parameters']['foo'] == 'bar'
        assert result['originalConfig']['configuration']['parameters']['baz'] == 42

        # Check updated config
        assert result['updatedConfig']['id'] == 'config-123'
        assert result['updatedConfig']['name'] == 'Updated Config Name'
        assert result['updatedConfig']['description'] == 'Updated description'
        assert result['updatedConfig']['configuration']['parameters']['foo'] == 'updated_bar'
        assert result['updatedConfig']['configuration']['parameters']['new_param'] == 'new_value'
        assert result['updatedConfig']['configuration']['parameters']['baz'] == 42
        assert result['updatedConfig']['changeDescription'] == 'Test change'

    def test_preview_update_config_validation_error(self, test_client: TestClient, mocker):
        """Test preview with validation error."""
        # Mock the storage client to raise a validation error
        mock_client = mocker.AsyncMock(KeboolaClient)

        # Properly mock async method that raises an error
        async def mock_config_detail(**kwargs):
            raise ValueError('Invalid configuration ID')

        mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Request payload
        request_payload = {
            'toolName': 'update_config',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                'configuration_id': 'invalid-config',
                'change_description': 'Test change',
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()

        # Check that isValid is False
        assert result['isValid'] is False
        assert 'validationErrors' in result
        assert len(result['validationErrors']) > 0
        assert 'Invalid configuration ID' in result['validationErrors'][0]

        # Check that configs are not in the response (excluded by exclude_none=True)
        assert 'originalConfig' not in result
        assert 'updatedConfig' not in result

    def test_preview_invalid_tool_name(self, test_client: TestClient, mocker):
        """Test preview with invalid tool name."""
        mock_client = mocker.AsyncMock(KeboolaClient)
        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Request payload with invalid tool name
        request_payload = {
            'toolName': 'invalid_tool',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                'configuration_id': 'config-123',
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        assert response.status_code == 400

    def test_preview_update_config_only_required_params(self, test_client: TestClient, mocker):
        """Test preview with only required parameters."""
        from keboola_mcp_server.clients.storage import ComponentAPIResponse

        original_config_data = {
            'id': 'config-123',
            'name': 'Original Config',
            'description': 'Original description',
            'configuration': {'parameters': {'foo': 'bar'}},
        }

        # Mock fetch_component
        async def mock_fetch_component(**kwargs):
            return ComponentAPIResponse.model_validate(
                {
                    'id': 'keboola.ex-test',
                    'name': 'Test Extractor',
                    'type': 'extractor',
                    'configurationSchema': {},
                    'component_flags': [],
                }
            )

        mocker.patch(
            'keboola_mcp_server.tools.components.tools.fetch_component',
            side_effect=mock_fetch_component,
        )

        mock_client = mocker.AsyncMock(KeboolaClient)

        # Properly mock async method
        async def mock_config_detail(**kwargs):
            return copy.deepcopy(original_config_data)

        mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Request payload with minimal params
        request_payload = {
            'toolName': 'update_config',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                'configuration_id': 'config-123',
                'change_description': 'Test change',
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()

        assert result['isValid'] is True
        # Name and description should remain unchanged
        assert result['updatedConfig']['name'] == 'Original Config'
        assert result['updatedConfig']['description'] == 'Original description'
        # Configuration should remain the same
        assert result['updatedConfig']['configuration']['parameters']['foo'] == 'bar'

    def test_preview_update_config_row_success(self, test_client: TestClient, mocker):
        """Test successful preview of update_config_row tool."""
        from keboola_mcp_server.clients.storage import ComponentAPIResponse

        # Mock the configuration row data
        original_row_data = {
            'id': 'row-456',
            'name': 'Original Row Name',
            'description': 'Original row description',
            'configuration': {
                'parameters': {
                    'foo': 'bar',
                    'baz': 42,
                }
            },
        }

        # Mock fetch_component
        async def mock_fetch_component(**kwargs):
            return ComponentAPIResponse.model_validate(
                {
                    'id': 'keboola.ex-test',
                    'name': 'Test Extractor',
                    'type': 'extractor',
                    'configurationSchema': {},
                    'component_flags': [],
                }
            )

        mocker.patch(
            'keboola_mcp_server.tools.components.tools.fetch_component',
            side_effect=mock_fetch_component,
        )

        # Mock the KeboolaClient
        mock_client = mocker.AsyncMock(KeboolaClient)

        # Mock async method for configuration row detail
        async def mock_row_detail(**kwargs):
            return copy.deepcopy(original_row_data)

        mock_client.storage_client.configuration_row_detail = mocker.AsyncMock(side_effect=mock_row_detail)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Request payload
        request_payload = {
            'toolName': 'update_config_row',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                'configuration_id': 'config-123',
                'configuration_row_id': 'row-456',
                'change_description': 'Test row change',
                'name': 'Updated Row Name',
                'description': 'Updated row description',
                'parameter_updates': [
                    {'op': 'set', 'path': 'foo', 'value': 'updated_bar'},
                ],
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()

        # Check response structure
        assert result['isValid'] is True
        assert result['coordinates']['componentId'] == 'keboola.ex-test'
        assert result['coordinates']['configurationId'] == 'config-123'
        assert result['coordinates']['configurationRowId'] == 'row-456'

        # Check updated config
        assert result['updatedConfig']['name'] == 'Updated Row Name'
        assert result['updatedConfig']['description'] == 'Updated row description'
        assert result['updatedConfig']['configuration']['parameters']['foo'] == 'updated_bar'

    def test_preview_update_sql_transformation_success(self, test_client: TestClient, mocker):
        """Test successful preview of update_sql_transformation tool."""
        from keboola_mcp_server.clients.storage import ComponentAPIResponse

        # Mock the transformation configuration data
        original_config_data = {
            'id': 'config-123',
            'name': 'Original Transformation',
            'description': 'Original transformation description',
            'configuration': {
                'parameters': {
                    'blocks': [
                        {
                            'name': 'Block 1',
                            'codes': [{'name': 'Code 1', 'script': ['SELECT * FROM table1;']}],
                        }
                    ],
                },
                'storage': {
                    'input': {'tables': []},
                    'output': {'tables': []},
                },
            },
        }

        # Mock fetch_component for transformation
        async def mock_fetch_component(**kwargs):
            return ComponentAPIResponse.model_validate(
                {
                    'id': 'keboola.snowflake-transformation',
                    'name': 'Snowflake Transformation',
                    'type': 'transformation',
                    'configurationSchema': {},
                    'component_flags': [],
                }
            )

        mocker.patch(
            'keboola_mcp_server.tools.components.tools.fetch_component',
            side_effect=mock_fetch_component,
        )

        # Mock the KeboolaClient
        mock_client = mocker.AsyncMock(KeboolaClient)

        async def mock_config_detail(**kwargs):
            return copy.deepcopy(original_config_data)

        mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Mock WorkspaceManager
        mock_workspace_manager = mocker.AsyncMock()
        mock_workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value='snowflake')

        mocker.patch(
            'keboola_mcp_server.preview.WorkspaceManager.from_state',
            return_value=mock_workspace_manager,
        )

        # Request payload
        request_payload = {
            'toolName': 'update_sql_transformation',
            'toolParams': {
                'configuration_id': 'config-123',
                'change_description': 'Update transformation',
                'parameter_updates': [
                    {
                        'op': 'add_block',
                        'block': {'name': 'Updated Block', 'codes': [{'name': 'Updated Code', 'script': 'SELECT 1'}]},
                        'position': 'end',
                    },
                ],
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()

        # Check response structure
        assert result['isValid'] is True
        assert result['coordinates']['componentId'] == 'keboola.snowflake-transformation'
        assert result['coordinates']['configurationId'] == 'config-123'

        # Check that configuration was updated
        assert result['updatedConfig']['configuration']['parameters']['blocks'][-1]['name'] == 'Updated Block'

    def test_preview_update_flow_success(self, test_client: TestClient, mocker):
        """Test successful preview of update_flow tool."""
        # Mock the flow configuration data
        original_config_data = {
            'id': 'flow-123',
            'name': 'Original Flow',
            'description': 'Original flow description',
            'configuration': {
                'phases': [
                    {
                        'id': 'phase1',
                        'name': 'Simple Phase',
                        'description': 'A simple conditional flow phase',
                        'next': [{'id': 'transition1', 'name': 'Simple Transition', 'goto': None}],
                    },
                ],
                'tasks': [
                    {
                        'id': 'task1',
                        'name': 'Simple Task',
                        'phase': 'phase1',
                        'enabled': True,
                        'task': {
                            'componentId': 'keboola.ex-test',
                            'type': 'notification',
                            'recipients': [{'channel': 'email', 'address': 'admin@company.com'}],
                            'title': 'Simple Notification',
                            'message': 'This is a simple notification task',
                        },
                    },
                ],
            },
        }

        # Mock the KeboolaClient
        mock_client = mocker.AsyncMock(KeboolaClient)

        async def mock_config_detail(**kwargs):
            return copy.deepcopy(original_config_data)

        mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Request payload
        request_payload = {
            'toolName': 'update_flow',
            'toolParams': {
                'flow_type': 'keboola.orchestrator',
                'configuration_id': 'flow-123',
                'change_description': 'Update flow',
                'name': 'Updated Flow',
                'description': 'Updated flow description',
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()

        # Check response structure
        assert result['isValid'] is True
        assert result['coordinates']['componentId'] == 'keboola.orchestrator'
        assert result['coordinates']['configurationId'] == 'flow-123'

        # Check updated config
        assert result['updatedConfig']['name'] == 'Updated Flow'
        assert result['updatedConfig']['description'] == 'Updated flow description'

    def test_preview_modify_data_app_success(self, test_client: TestClient, mocker):
        """Test successful preview of modify_data_app tool."""
        from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID

        # Mock the data app configuration data
        original_config_data = {
            'id': 'app-123',
            'name': 'Original Data App',
            'description': 'Original data app description',
            'configuration': {
                'parameters': {
                    'dataApp': {
                        'slug': 'old-slug',
                        'secrets': {'FOO': 'old', 'KEEP': 'x'},
                    },
                    'script': ['old'],
                    'packages': ['numpy'],
                },
                'authorization': {},
            },
        }

        # Mock the KeboolaClient
        mock_client = mocker.AsyncMock(KeboolaClient)

        async def mock_config_detail(**kwargs):
            return copy.deepcopy(original_config_data)

        async def mock_encrypt(*args, **kwargs):
            return args[0]

        mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)
        mock_client.storage_client.project_id = mocker.AsyncMock(return_value='test-project')
        mock_client.encryption_client.encrypt = mocker.AsyncMock(side_effect=mock_encrypt)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Mock WorkspaceManager
        mock_workspace_manager = mocker.AsyncMock()
        mock_workspace_manager.get_workspace_id = mocker.AsyncMock(return_value=123)
        mock_workspace_manager.get_branch_id = mocker.AsyncMock(return_value=456)
        mock_workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value='snowflake')

        mocker.patch(
            'keboola_mcp_server.preview.WorkspaceManager.from_state',
            return_value=mock_workspace_manager,
        )

        # Mock _fetch_data_app function
        from keboola_mcp_server.tools.data_apps import DataApp

        mock_data_app = DataApp(
            name='Original Data App',
            description='Original data app description',
            component_id='keboola.data-apps',
            configuration_id='app-123',
            data_app_id='data-app-123',
            project_id='project-123',
            branch_id='456',
            config_version='1',
            state='running',
            type='streamlit',
            parameters={
                'dataApp': {
                    'slug': 'old-slug',
                    'secrets': {'FOO': 'old', 'KEEP': 'x'},
                },
                'script': ['old'],
                'packages': ['numpy'],
            },
            authorization={},
            storage={},
        )

        async def mock_fetch_data_app(client, **kwargs):
            return mock_data_app

        mocker.patch(
            'keboola_mcp_server.tools.data_apps._fetch_data_app',
            side_effect=mock_fetch_data_app,
        )

        # Request payload
        request_payload = {
            'toolName': 'modify_data_app',
            'toolParams': {
                'configuration_id': 'app-123',
                'change_description': 'Update data app',
                'name': 'Updated Data App',
                'description': 'Updated data app description',
                'source_code': 'print("Hello World")',
                'packages': ['streamlit', 'pandas'],
                'authentication_type': 'default',
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()

        # Check response structure
        assert result['isValid'] is True
        assert result['coordinates']['componentId'] == DATA_APP_COMPONENT_ID
        assert result['coordinates']['configurationId'] == 'app-123'

        # Check updated config
        assert result['updatedConfig']['name'] == 'Updated Data App'
        assert result['updatedConfig']['description'] == 'Updated data app description'

    def test_preview_validation_missing_required_param(self, test_client: TestClient):
        """Test validation error for missing required parameter."""
        # Request missing required 'configuration_id'
        request_payload = {
            'toolName': 'update_config',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                # 'configuration_id': missing!
                'change_description': 'Test',
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()
        print(result)
        assert result['isValid'] is False
        assert 'validationErrors' in result
        assert 'configuration_id' in str(result['validationErrors'])

        # Check that configs are not in the response (excluded by exclude_none=True)
        assert 'originalConfig' not in result
        assert 'updatedConfig' not in result

    def test_preview_validation_invalid_param_type(self, test_client: TestClient, mocker):
        """Test validation error for invalid parameter type.

        Note: Type validation errors at the API level return 400 Bad Request
        rather than 200 with validation errors, as they represent malformed requests.
        """
        request_payload = {
            'toolName': 'update_config',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                'configuration_id': 'config-123',
                'change_description': 'Test change',
                'name': 'Updated Config Name',
                'description': 'Updated description',
                'parameter_updates': [
                    {'op': 'set', 'path': 'foo', 'value': 'updated_bar'},
                    {'op': 'foo', 'path': 'new_param', 'value': 'new_value'},  # Invalid op
                ],
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)
        print(response.json())

        assert response.status_code == 200
        result = response.json()
        assert result['isValid'] is False
        assert 'validationErrors' in result
        assert 'parameter_updates.1' in str(result['validationErrors'])

        # Check that configs are not in the response (excluded by exclude_none=True)
        assert 'originalConfig' not in result
        assert 'updatedConfig' not in result

    def test_preview_validation_passes_for_valid_params(self, test_client: TestClient, mocker):
        """Test that validation passes for valid parameters and processing continues."""
        from keboola_mcp_server.clients.storage import ComponentAPIResponse

        original_config_data = {
            'id': 'config-123',
            'name': 'Original Config',
            'description': 'Original description',
            'configuration': {'parameters': {'foo': 'bar'}},
        }

        # Mock fetch_component
        async def mock_fetch_component(**kwargs):
            return ComponentAPIResponse.model_validate(
                {
                    'id': 'keboola.ex-test',
                    'name': 'Test Extractor',
                    'type': 'extractor',
                    'configurationSchema': {},
                    'component_flags': [],
                }
            )

        mocker.patch(
            'keboola_mcp_server.tools.components.tools.fetch_component',
            side_effect=mock_fetch_component,
        )

        # Mock the KeboolaClient
        mock_client = mocker.AsyncMock(KeboolaClient)

        async def mock_config_detail(**kwargs):
            return copy.deepcopy(original_config_data)

        mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

        mocker.patch('keboola_mcp_server.preview.KeboolaClient.from_state', return_value=mock_client)

        # Request payload with valid params (should pass validation)
        request_payload = {
            'toolName': 'update_config',
            'toolParams': {
                'component_id': 'keboola.ex-test',
                'configuration_id': 'config-123',
                'change_description': 'Test change',
            },
        }

        # Make the request
        response = test_client.post('/preview/configuration', json=request_payload)

        # Assertions
        assert response.status_code == 200
        result = response.json()
        # Validation passed, so processing continued and isValid should be True
        assert result['isValid'] is True
        assert 'validationErrors' not in result
        # Config should be returned
        assert 'originalConfig' in result
        assert 'updatedConfig' in result
