from typing import Any, Dict, List

import pytest


@pytest.fixture
def mock_project_id() -> str:
    """Mocks a project id."""
    return '1'


@pytest.fixture
def mock_raw_flow_config() -> Dict[str, Any]:
    """Mock raw flow configuration as returned by Keboola API."""
    return {
        'id': '21703284',
        'name': 'Test Flow',
        'description': 'Test flow description',
        'version': 1,
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {
            'phases': [
                {'id': 1, 'name': 'Data Extraction', 'description': 'Extract data from sources', 'dependsOn': []},
                {'id': 2, 'name': 'Data Processing', 'description': 'Process extracted data', 'dependsOn': [1]},
            ],
            'tasks': [
                {
                    'id': 20001,
                    'name': 'Extract AWS S3',
                    'phase': 1,
                    'enabled': True,
                    'continueOnFailure': False,
                    'task': {'componentId': 'keboola.ex-aws-s3', 'configId': '12345', 'mode': 'run'},
                },
                {
                    'id': 20002,
                    'name': 'Process Data',
                    'phase': 2,
                    'enabled': True,
                    'continueOnFailure': False,
                    'task': {'componentId': 'keboola.snowflake-transformation', 'configId': '67890', 'mode': 'run'},
                },
            ],
        },
        'changeDescription': 'Initial creation',
        'metadata': [],
        'created': '2025-05-25T06:33:41+0200',
    }


@pytest.fixture
def mock_empty_flow_config() -> Dict[str, Any]:
    """Mock empty flow configuration."""
    return {
        'id': '21703285',
        'name': 'Empty Flow',
        'description': 'Empty test flow',
        'version': 1,
        'isDisabled': False,
        'isDeleted': False,
        'configuration': {'phases': [], 'tasks': []},
        'changeDescription': None,
        'metadata': [],
        'created': '2025-05-25T07:00:00+0200',
    }


@pytest.fixture
def sample_phases() -> List[Dict[str, Any]]:
    """Sample phase definitions for testing."""
    return [
        {'name': 'Data Extraction', 'dependsOn': [], 'description': 'Extract data'},
        {'name': 'Data Processing', 'dependsOn': [1], 'description': 'Process data'},
        {'name': 'Data Output', 'dependsOn': [2], 'description': 'Output processed data'},
    ]


@pytest.fixture
def sample_tasks() -> List[Dict[str, Any]]:
    """Sample task definitions for testing."""
    return [
        {'name': 'Extract from S3', 'phase': 1, 'task': {'componentId': 'keboola.ex-aws-s3', 'configId': '12345'}},
        {
            'name': 'Transform Data',
            'phase': 2,
            'task': {'componentId': 'keboola.snowflake-transformation', 'configId': '67890'},
        },
        {
            'name': 'Export to BigQuery',
            'phase': 3,
            'task': {'componentId': 'keboola.wr-google-bigquery-v2', 'configId': '11111'},
        },
    ]
