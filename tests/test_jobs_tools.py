from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.jobs_tools import (
    JobDetail,
    JobListItem,
    filter_by_component_id,
    filter_by_config_id,
    get_job_details,
    list_component_config_jobs,
    list_component_jobs,
    list_jobs,
)


@pytest.mark.asyncio
async def test_list_jobs(mcp_context_client):
    """Test list_jobs tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.jobs = MagicMock()

    # Mock data
    mock_jobs = [
        {
            "id": "123",
            "status": "success",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
        },
        {
            "id": "124",
            "status": "processing",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
        },
    ]
    mock_client.storage_client.jobs.list = MagicMock(return_value=mock_jobs)

    result = await list_jobs(context)

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(j.id == item["id"] for j, item in zip(result, mock_jobs))
    assert all(j.status == item["status"] for j, item in zip(result, mock_jobs))
    assert all(j.created_time == item["createdTime"] for j, item in zip(result, mock_jobs))
    assert all(j.start_time == item["startTime"] for j, item in zip(result, mock_jobs))
    assert all(j.end_time == item["endTime"] for j, item in zip(result, mock_jobs))

    mock_client.storage_client.jobs.list.assert_called_once()


@pytest.mark.asyncio
async def test_get_job_details(mcp_context_client):
    """Test get_job_details tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.jobs = MagicMock()

    # Mock data
    mock_job = {
        "id": "123",
        "status": "success",
        "createdTime": "2024-01-01T00:00:00Z",
        "startTime": "2024-01-01T00:00:01Z",
        "endTime": "2024-01-01T00:00:02Z",
        "url": "https://connection.keboola.com/jobs/123",
        "operationName": "tableImport",
        "operationParams": {"source": "file.csv"},
        "runId": "456",
        "results": {"import": "successful"},
        "metrics": {"rows": 1000},
        "additional_field": "some value",  # this will be removed
    }

    # Setup mock to return test data
    mock_client.storage_client.jobs.detail = MagicMock(return_value=mock_job)

    result = await get_job_details("123", context)

    assert isinstance(result, JobDetail)
    assert result.id == "123"
    assert result.status == "success"
    assert result.url == "https://connection.keboola.com/jobs/123"
    assert result.operation_name == "tableImport"
    assert result.operation_params == {"source": "file.csv"}
    assert result.run_id == "456"
    assert result.results == {"import": "successful"}
    assert result.metrics == {"rows": 1000}

    mock_client.storage_client.jobs.detail.assert_called_once_with("123")


@pytest.mark.asyncio
async def test_list_component_config_jobs(mcp_context_client):
    """Test list_component_config_jobs tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.jobs = MagicMock()

    # Mock data
    mock_jobs = [
        {
            "id": "123",
            "status": "success",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            'url': 'https://foo.bar',
            "operationParams": {"configurationId": "config-123", "componentId": "keboola.ex-aws-s3"},
            "operationName": "tableImport",
        },
        {
            "id": "124",
            "status": "processing",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            'url': 'https://foo.bar',
            "operationParams": {"configurationId": "config-124", "componentId": "keboola.ex-aws-s3"},
            "operationName": "tableImport",
        },
        {
            "id": "125",
            "status": "error",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            'url': 'https://foo.bar',
            "operationParams": {"configurationId": "config-456", "componentId": "keboola.ex-db-mysql"},
            "operationName": "tableImport",
        },
    ]
    mock_client.storage_client.jobs.list = MagicMock(return_value=mock_jobs)

    result = await list_component_config_jobs("keboola.ex-aws-s3", "config-123", context)

    assert len(result) == 1
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(j.id == item["id"] for j, item in zip(result, mock_jobs))


@pytest.mark.asyncio
async def test_list_component_jobs(mcp_context_client):
    """Test list_component_jobs tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.jobs = MagicMock()

    # Mock data
    mock_jobs = [
        {
            "id": "123",
            "status": "success",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            'url': 'https://foo.bar',
            "operationParams": {"configurationId": "config-123", "componentId": "keboola.ex-aws-s3"},
            "operationName": "tableImport",
        },
        {
            "id": "124",
            "status": "processing",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            'url': 'https://foo.bar',
            "operationParams": {"configurationId": "config-124", "componentId": "keboola.ex-aws-s3"},
            "operationName": "tableImport",
        },
        {
            "id": "125",
            "status": "error",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            'url': 'https://foo.bar',
            "operationParams": {"configurationId": "config-456", "componentId": "keboola.ex-db-mysql"},
            "operationName": "tableImport",
        },
    ]
    mock_client.storage_client.jobs.list = MagicMock(return_value=mock_jobs)

    result = await list_component_jobs("keboola.ex-aws-s3", context)

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(j.id == item["id"] for j, item in zip(result, mock_jobs))

@pytest.mark.parametrize(
    "config_id, search_config_id, expected",
    [("config-123", "config-123", True), ("config-456", "config-123", False)],
)
def test_filter_by_config_id(config_id, search_config_id, expected):
    """Test filter_by_config_id function."""
    # Test job with matching config_id
    job = JobDetail.model_validate(
        {
            "id": "123",
            "status": "success",
            "url": "https://example.com",
            "operationName": "test",
            "operationParams": {"configurationId": config_id},
        }
    )
    assert filter_by_config_id(job, search_config_id) is expected

@pytest.mark.parametrize(
    "component_id, search_component_id, expected",
    [("keboola.ex-aws-s3", "keboola.ex-aws-s3", True), ("keboola.ex-db-mysql", "keboola.ex-aws-s3", False)],
)
def test_filter_by_component_id(component_id, search_component_id, expected):
    """Test filter_by_component_id function."""
    # Test job with matching component_id
    job = JobDetail.model_validate(
        {
            "id": "123",
            "status": "success",
            "url": "https://example.com",
            "operationName": "test",
            "operationParams": {"componentId": component_id},
        }
    )
    print(component_id, search_component_id, expected)
    assert filter_by_component_id(job, search_component_id) is expected

def test_filter_by_config_id_edge_cases():
    """Test filter_by_config_id function with edge cases."""
    job = JobDetail(
        id="123",
        status="success",
        url="https://example.com",
        operation_name="test",
        operation_params={},
    )
    assert filter_by_config_id(job, "config-123") is False
    # Test job with no operation_params
    job = JobDetail(
        id="123",
        status="success",
        url="https://example.com",
        operation_name="test",
        operation_params={},
    )
    assert filter_by_config_id(job, "config-123") is False

def test_filter_by_component_id_edge_cases():
    """Test filter_by_component_id function with edge cases."""
    # Test job with no component_id
    job = JobDetail(
        id="123",
        status="success",
        url="https://example.com",
        operation_name="test",
        operation_params={},
    )
    assert filter_by_component_id(job, "keboola.ex-aws-s3") is False
    # Test job with no operation_params
    job = JobDetail(
        id="123",
        status="success",
        url="https://example.com",
        operation_name="test",
        operation_params=None,
    )
    assert filter_by_component_id(job, "keboola.ex-aws-s3") is False

