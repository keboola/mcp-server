import logging

import pytest
from mcp.server.fastmcp import Context

from integtests.conftest import ConfigDef, ProjectDef
from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components.tools import create_config
from keboola_mcp_server.tools.jobs import JobDetail, JobListItem, ListJobsOutput, get_job, list_jobs, run_job

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_list_jobs_all(mcp_context: Context):
    """Tests that `list_jobs` returns all jobs in the project."""

    result = await list_jobs(ctx=mcp_context)
    assert isinstance(result, ListJobsOutput)

    assert len(result.links) > 0
    for link in result.links:
        assert isinstance(link, Link)

    for job in result.jobs:
        assert isinstance(job, JobListItem)
        assert job.id is not None
        assert job.status is not None


@pytest.mark.asyncio
async def test_list_jobs_with_status_filter(mcp_context: Context):
    """Tests that `list_jobs` works with status filtering."""

    # Test filtering by success status
    result_success = await list_jobs(ctx=mcp_context, status='success', limit=10)
    assert isinstance(result_success, ListJobsOutput)

    # All returned jobs should have success status
    for job in result_success.jobs:
        assert job.status == 'success'

    # Test filtering by error status
    result_error = await list_jobs(ctx=mcp_context, status='error', limit=10)
    assert isinstance(result_error, ListJobsOutput)

    # All returned jobs should have error status
    for job in result_error.jobs:
        assert job.status == 'error'


@pytest.mark.asyncio
async def test_list_jobs_with_component_filter(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `list_jobs` works with component filtering."""

    # Use first config to filter by component
    test_config = configs[0]
    component_id = test_config.component_id

    result = await list_jobs(ctx=mcp_context, component_id=component_id, limit=20)
    assert isinstance(result, ListJobsOutput)

    # All returned jobs should be for the specified component
    for job in result.jobs:
        if job.component_id is not None:  # Some jobs might not have component_id
            assert job.component_id == component_id


@pytest.mark.asyncio
async def test_list_jobs_with_config_filter(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `list_jobs` works with component and config filtering."""

    # Use first config to filter by component and config
    test_config = configs[0]
    component_id = test_config.component_id
    configuration_id = test_config.configuration_id

    result = await list_jobs(ctx=mcp_context, component_id=component_id, config_id=configuration_id, limit=10)
    assert isinstance(result, ListJobsOutput)

    # All returned jobs should be for the specified component and config
    for job in result.jobs:
        if job.component_id is not None and job.config_id is not None:
            assert job.component_id == component_id
            assert job.config_id == configuration_id


@pytest.mark.asyncio
async def test_run_job_and_get_job(mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef):
    """Tests that `run_job` creates a job and `get_job` retrieves its details."""
    test_config = configs[0]
    component_id = test_config.component_id
    configuration_id = test_config.configuration_id

    started_job = await run_job(ctx=mcp_context, component_id=component_id, configuration_id=configuration_id)

    # Verify the started job response
    assert isinstance(started_job, JobDetail)
    assert started_job.id is not None
    assert started_job.component_id == component_id
    assert started_job.config_id == configuration_id
    assert started_job.status is not None
    assert isinstance(started_job.links, list)
    assert len(started_job.links) > 0

    job_detail = await get_job(job_id=started_job.id, ctx=mcp_context)

    # Verify the job detail response
    assert isinstance(job_detail, JobDetail)
    assert job_detail.id == started_job.id
    assert job_detail.component_id == component_id
    assert job_detail.config_id == configuration_id
    assert job_detail.status is not None
    assert job_detail.url is not None
    assert isinstance(job_detail.links, list)
    assert len(job_detail.links) > 0

    # Verify job appears in list_jobs for this component
    jobs_list = await list_jobs(ctx=mcp_context, component_id=component_id, limit=50)
    job_ids = [job.id for job in jobs_list.jobs]
    assert started_job.id in job_ids


@pytest.mark.asyncio
async def test_get_job(mcp_context: Context):
    """Tests`get_job`."""

    # First get a list of jobs to find one to examine in detail
    jobs_list = await list_jobs(ctx=mcp_context, limit=5)

    if len(jobs_list.jobs) > 0:
        # Get details for the first job
        first_job = jobs_list.jobs[0]
        job_detail = await get_job(job_id=first_job.id, ctx=mcp_context)

        # Verify all expected fields are present
        assert isinstance(job_detail, JobDetail)
        assert job_detail.id == first_job.id
        assert job_detail.status is not None
        assert job_detail.url is not None
        assert isinstance(job_detail.links, list)

        # Verify inherited JobListItem fields
        assert hasattr(job_detail, 'component_id')
        assert hasattr(job_detail, 'config_id')
        assert hasattr(job_detail, 'is_finished')
        assert hasattr(job_detail, 'created_time')
        assert hasattr(job_detail, 'start_time')
        assert hasattr(job_detail, 'end_time')
        assert hasattr(job_detail, 'duration_seconds')

        # Verify JobDetail specific fields
        assert hasattr(job_detail, 'config_data')
        assert hasattr(job_detail, 'config_row')
        assert hasattr(job_detail, 'run_id')
        assert hasattr(job_detail, 'result')


@pytest.mark.asyncio
async def test_run_job_with_newly_created_config(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `run_job` works with a newly created configuration."""

    test_config = configs[0]
    component_id = test_config.component_id

    # Create a new configuration for testing
    new_config = await create_config(
        ctx=mcp_context,
        name='Test Config for Job Run',
        description='Test configuration created for job run test',
        component_id=component_id,
        parameters={},
        storage={},
    )

    try:
        # Run a job on the new configuration
        started_job = await run_job(
            ctx=mcp_context, component_id=component_id, configuration_id=new_config.configuration_id
        )

        # Verify the job was started successfully
        assert isinstance(started_job, JobDetail)
        assert started_job.id is not None
        assert started_job.component_id == component_id
        assert started_job.config_id == new_config.configuration_id
        assert started_job.status is not None

        # Verify job can be retrieved
        job_detail = await get_job(job_id=started_job.id, ctx=mcp_context)
        assert isinstance(job_detail, JobDetail)
        assert job_detail.id == started_job.id

    finally:
        # Clean up: Delete the configuration
        client = KeboolaClient.from_state(mcp_context.session.state)
        await client.storage_client.configuration_delete(
            component_id=component_id, configuration_id=new_config.configuration_id, skip_trash=True
        )
