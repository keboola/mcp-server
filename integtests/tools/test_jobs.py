import asyncio
import logging

import pytest
from mcp.server.fastmcp import Context

from integtests.conftest import ConfigDef, ProjectDef
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components import create_config
from keboola_mcp_server.tools.jobs import JobDetail, ListJobsOutput, get_job, list_jobs, run_job

LOG = logging.getLogger(__name__)


async def _wait_for_job_in_list(
    mcp_context: Context,
    job_id: str,
    component_id: str | None,
    config_id: str | None,
    max_retries: int = 10,
    delay: float = 0.5,
) -> ListJobsOutput:
    """
    Wait for a job to appear in the job list with retry mechanism.

    :param mcp_context: MCP context
    :param job_id: ID of the job to find
    :param component_id: Component ID to filter by (can be None)
    :param config_id: Config ID to filter by (can be None)
    :param max_retries: Maximum number of retry attempts
    :param delay: Delay between retries in seconds
    :return: ListJobsOutput containing the job
    :raises AssertionError: If job is not found after all retries
    """
    for attempt in range(max_retries):
        result = await list_jobs(
            ctx=mcp_context,
            component_id=component_id,
            config_id=config_id,
            limit=10,
            sort_by='startTime',
            sort_order='desc',
        )

        job_ids = {job.id for job in result.jobs}
        if job_id in job_ids:
            LOG.info(f'Job {job_id} found in list after {attempt + 1} attempts')
            return result

        if attempt < max_retries - 1:
            LOG.info(f'Job {job_id} not found in list, attempt {attempt + 1}/{max_retries}, retrying in {delay}s...')
            await asyncio.sleep(delay)

    raise AssertionError(f'Job {job_id} not found in job list after {max_retries} attempts')


@pytest.mark.asyncio
async def test_list_jobs_with_component_and_config_filter(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `list_jobs` works with component and config filtering."""

    # Use first config to create jobs for testing
    test_config = configs[0]
    component_id = test_config.component_id
    configuration_id = test_config.configuration_id

    job = await run_job(ctx=mcp_context, component_id=component_id, configuration_id=configuration_id)

    # Wait for the job to appear in the list (handles race condition)
    result = await _wait_for_job_in_list(
        mcp_context=mcp_context,
        job_id=job.id,
        component_id=component_id,
        config_id=configuration_id,
    )

    assert isinstance(result, ListJobsOutput)
    assert len(result.jobs) >= 1

    # Verify our created jobs appear in the results
    job_ids = {job.id for job in result.jobs}
    assert job.id in job_ids

    for job in result.jobs:
        assert job.component_id == component_id
        assert job.config_id == configuration_id


@pytest.mark.asyncio
async def test_run_job_and_get_job(mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef):
    """Tests that `run_job` creates a job and `get_job` retrieves its details."""

    project_id = keboola_project.project_id

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
    assert frozenset(started_job.links) == frozenset(
        [
            Link(
                type='ui-detail',
                title=f'Job: {started_job.id}',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/queue/{started_job.id}',
            ),
            Link(
                type='ui-dashboard',
                title='Jobs in the project',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/queue',
            ),
        ]
    )

    job_detail = await get_job(job_id=started_job.id, ctx=mcp_context)

    # Verify the job detail response
    assert isinstance(job_detail, JobDetail)
    assert job_detail.id == started_job.id
    assert job_detail.component_id == component_id
    assert job_detail.config_id == configuration_id
    assert job_detail.status is not None
    assert job_detail.url is not None
    assert frozenset(job_detail.links) == frozenset(
        [
            Link(
                type='ui-detail',
                title=f'Job: {job_detail.id}',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/queue/{job_detail.id}',
            ),
            Link(
                type='ui-dashboard',
                title='Jobs in the project',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/queue',
            ),
        ]
    )


@pytest.mark.asyncio
async def test_get_job(mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef):
    """Tests `get_job` by creating a job and then retrieving its details."""

    project_id = keboola_project.project_id

    # Use first config to create a specific job
    test_config = configs[0]
    component_id = test_config.component_id
    configuration_id = test_config.configuration_id

    # Create a specific job to test get_job with
    created_job = await run_job(ctx=mcp_context, component_id=component_id, configuration_id=configuration_id)

    # Now test get_job on the job we just created
    job_detail = await get_job(job_id=created_job.id, ctx=mcp_context)

    # Verify all expected fields are present
    assert isinstance(job_detail, JobDetail)
    assert job_detail.id == created_job.id
    assert job_detail.component_id == component_id
    assert job_detail.config_id == configuration_id
    assert job_detail.status is not None
    assert frozenset(job_detail.links) == frozenset(
        [
            Link(
                type='ui-detail',
                title=f'Job: {created_job.id}',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/queue/{created_job.id}',
            ),
            Link(
                type='ui-dashboard',
                title='Jobs in the project',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/queue',
            ),
        ]
    )


@pytest.mark.asyncio
async def test_run_job_with_newly_created_config(
    mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef
):
    """Tests that `run_job` works with a newly created configuration."""

    project_id = keboola_project.project_id

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
        assert frozenset(started_job.links) == frozenset(
            [
                Link(
                    type='ui-detail',
                    title=f'Job: {started_job.id}',
                    url=f'https://connection.keboola.com/admin/projects/{project_id}/queue/{started_job.id}',
                ),
                Link(
                    type='ui-dashboard',
                    title='Jobs in the project',
                    url=f'https://connection.keboola.com/admin/projects/{project_id}/queue',
                ),
            ]
        )

        # Verify job can be retrieved
        job_detail = await get_job(job_id=started_job.id, ctx=mcp_context)
        assert isinstance(job_detail, JobDetail)
        assert job_detail.id == started_job.id
        assert job_detail.component_id == component_id
        assert job_detail.config_id == new_config.configuration_id

    finally:
        # Clean up: Delete the configuration
        client = KeboolaClient.from_state(mcp_context.session.state)
        await client.storage_client.configuration_delete(
            component_id=component_id, configuration_id=new_config.configuration_id, skip_trash=True
        )
