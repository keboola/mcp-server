import datetime
import logging
from typing import Annotated, Any, Literal, Optional, Sequence, Union

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import AliasChoices, BaseModel, Field, field_validator

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer, process_concurrently, toon_serializer, unwrap_results

LOG = logging.getLogger(__name__)

JOB_TOOLS_TAG = 'jobs'


# Add jobs tools to MCP SERVER ##################################


def add_job_tools(mcp: KeboolaMcpServer) -> None:
    """Add job tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            get_jobs,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer,
            tags={JOB_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            run_job,
            annotations=ToolAnnotations(destructiveHint=True),
            tags={JOB_TOOLS_TAG},
        )
    )

    LOG.info('Job tools added to the MCP server.')


# Job Base Models ########################################

JOB_STATUS = Literal[
    'waiting',  # job is waiting for other jobs to finish
    'processing',  # job is being executed
    'success',  # job finished successfully
    'error',  # job finished with error
    'created',  # job is created but not started executing
    'warning',  # job finished but one of its child jobs failed
    'terminating',  # user requested to abort the job
    'cancelled',  # job was aborted before execution began
    'terminated',  # job was aborted during execution
]


class JobListItem(BaseModel):
    """Represents a summary of a job with minimal information, used in lists where detailed job data is not required."""

    id: str = Field(description='The ID of the job.')
    status: JOB_STATUS = Field(description='The status of the job.')
    component_id: Optional[str] = Field(
        description='The ID of the component that the job is running on.',
        validation_alias=AliasChoices('componentId', 'component', 'component_id', 'component-id'),
        serialization_alias='componentId',
        default=None,
    )
    config_id: Optional[str] = Field(
        description='The ID of the component configuration that the job is running on.',
        validation_alias=AliasChoices('configId', 'config', 'config_id', 'config-id'),
        serialization_alias='configId',
        default=None,
    )
    is_finished: bool = Field(
        description='Whether the job is finished.',
        validation_alias=AliasChoices('isFinished', 'is_finished', 'is-finished'),
        serialization_alias='isFinished',
        default=False,
    )
    created_time: Optional[datetime.datetime] = Field(
        description='The creation time of the job.',
        validation_alias=AliasChoices('createdTime', 'created_time', 'created-time'),
        serialization_alias='createdTime',
        default=None,
    )
    start_time: Optional[datetime.datetime] = Field(
        description='The start time of the job.',
        validation_alias=AliasChoices('startTime', 'start_time', 'start-time'),
        serialization_alias='startTime',
        default=None,
    )
    end_time: Optional[datetime.datetime] = Field(
        description='The end time of the job.',
        validation_alias=AliasChoices('endTime', 'end_time', 'end-time'),
        serialization_alias='endTime',
        default=None,
    )
    duration_seconds: Optional[float] = Field(
        description='The duration of the job in seconds.',
        validation_alias=AliasChoices('durationSeconds', 'duration_seconds', 'duration-seconds'),
        serialization_alias='durationSeconds',
        default=None,
    )


class JobDetail(JobListItem):
    """Represents a detailed job with all available information."""

    url: str = Field(description='The URL of the job.')

    config_data: Optional[dict[str, Any]] = Field(
        description='The data of the configuration.',
        validation_alias=AliasChoices('configData', 'config_data', 'config-data'),
        serialization_alias='configData',
        default=None,
    )
    config_row: Optional[str] = Field(
        description='The configuration row ID.',
        validation_alias=AliasChoices('configRow', 'config_row', 'config-row'),
        serialization_alias='configRow',
        default=None,
    )
    run_id: Optional[str] = Field(
        description='The ID of the run that the job is running on.',
        validation_alias=AliasChoices('runId', 'run_id', 'run-id'),
        serialization_alias='runId',
        default=None,
    )
    result: Optional[dict[str, Any]] = Field(
        description='The results of the job.',
        default=None,
    )
    links: list[Link] = Field(..., description='The links relevant to the job.')

    @field_validator('result', 'config_data', mode='before')
    @classmethod
    def validate_dict_fields(cls, current_value: Union[list[Any], dict[str, Any], None]) -> dict[str, Any]:
        # Ensures that if the result or config_data field is passed as an empty list [] or None,
        # it gets converted to an empty dict {}.Why? Because the result is expected to be an Object, but create job
        # endpoint sends [], perhaps it means "empty". This avoids type errors.
        if not isinstance(current_value, dict):
            if not current_value:
                return dict()
            if isinstance(current_value, list):
                raise ValueError(
                    'Field "result" or "config_data" cannot be a list, expecting dictionary, ' f'got: {current_value}.'
                )
        return current_value


class GetJobsListOutput(BaseModel):
    """Output of get_jobs tool when listing (no specific job_ids)."""

    jobs: list[JobListItem] = Field(..., description='List of jobs.')
    links: list[Link] = Field(..., description='Links relevant to the jobs listing.')


class GetJobsDetailOutput(BaseModel):
    """Output of get_jobs tool when retrieving specific job_ids."""

    jobs: list[JobDetail] = Field(..., description='List of jobs with full details.')


GetJobsOutput = Union[GetJobsListOutput, GetJobsDetailOutput]


# End of Job Base Models ########################################

# MCP tools ########################################


SORT_BY_VALUES = Literal['startTime', 'endTime', 'createdTime', 'durationSeconds', 'id']
SORT_ORDER_VALUES = Literal['asc', 'desc']


@tool_errors()
async def get_jobs(
    ctx: Context,
    job_ids: Annotated[
        Sequence[str],
        Field(
            description=(
                'IDs of jobs to retrieve full details for. '
                'When provided (non-empty), returns full job details including status, parameters, '
                'results, and metadata. '
                'When empty [], lists jobs in the project as summaries with optional filtering.'
            )
        ),
    ] = tuple(),
    status: Annotated[
        JOB_STATUS,
        Field(
            description=(
                'The optional status of the jobs to filter by when listing (ignored if job_ids is provided). '
                'If None then all statuses are included.'
            ),
        ),
    ] = None,
    component_id: Annotated[
        str,
        Field(
            description=(
                'The optional ID of the component whose jobs you want to list '
                '(ignored if job_ids is provided). Default = None.'
            ),
        ),
    ] = None,
    config_id: Annotated[
        str,
        Field(
            description=(
                'The optional ID of the component configuration whose jobs you want to list '
                '(ignored if job_ids is provided). Default = None.'
            ),
        ),
    ] = None,
    limit: Annotated[
        int,
        Field(
            description=(
                'The number of jobs to list when listing (ignored if job_ids is provided), ' 'default = 100, max = 500.'
            ),
            ge=1,
            le=500,
        ),
    ] = 100,
    offset: Annotated[
        int,
        Field(
            description=('The offset of the jobs to list when listing (ignored if job_ids is provided), default = 0.'),
            ge=0,
        ),
    ] = 0,
    sort_by: Annotated[
        SORT_BY_VALUES,
        Field(
            description=(
                'The field to sort the jobs by when listing (ignored if job_ids is provided), ' 'default = "startTime".'
            ),
        ),
    ] = 'startTime',
    sort_order: Annotated[
        SORT_ORDER_VALUES,
        Field(
            description=(
                'The order to sort the jobs by when listing (ignored if job_ids is provided), default = "desc".'
            ),
        ),
    ] = 'desc',
) -> GetJobsOutput:
    """
    Retrieves job execution information from the Keboola project.

    CONTEXT:
    Jobs in Keboola are execution records of components (extractors, transformations, writers, flows).
    Each job represents a single run with its status, timing, configuration, and results.

    TWO MODES OF OPERATION (controlled by job_ids parameter):

    MODE 1: GET DETAILS FOR SPECIFIC JOBS (job_ids is non-empty)
    - Provide one or more job IDs: job_ids=["12345", "67890"]
    - Returns: FULL details for each job including status, config_data, results, timing, and metadata
    - Ignores: All filtering/sorting parameters (status, component_id, config_id, limit, offset, sort_by, sort_order)
    - Use when: You know specific job IDs and need complete information about them

    MODE 2: LIST/SEARCH JOBS (job_ids is empty)
    - Leave job_ids empty: job_ids=[]
    - Returns: SUMMARY list of jobs (id, status, component_id, config_id, timing only - no config_data or results)
    - Supports: Filtering by status/component_id/config_id, pagination with limit/offset, sorting
    - Use when: You need to find jobs, see recent executions, or monitor job history

    DECISION GUIDE:
    - Start with MODE 2 (list) to find jobs → then use MODE 1 (details) if you need full information
    - If you already know job IDs → use MODE 1 directly
    - For monitoring/browsing → use MODE 2 with filters

    COMMON WORKFLOWS:
    1. Find failed jobs: job_ids=[], status="error" → identify problematic job IDs → get details with MODE 1
    2. Check recent runs: job_ids=[], component_id="...", limit=10 → see latest executions
    3. Monitor specific job: job_ids=["123"] → poll for status and results
    4. Troubleshoot config: job_ids=[], component_id="...", config_id="...", status="error" → find which runs failed

    EXAMPLES:

    MODE 1 - Get full details:
    - job_ids=["12345"] → detailed info for job 12345
    - job_ids=["12345", "67890"] → detailed info for multiple jobs

    MODE 2 - List/search jobs:
    - job_ids=[] → list latest 100 jobs (default)
    - job_ids=[], status="error" → list only failed jobs
    - job_ids=[], status="processing" → list currently running jobs
    - job_ids=[], component_id="keboola.ex-aws-s3" → list jobs for S3 extractor
    - job_ids=[], component_id="keboola.ex-aws-s3", config_id="12345" → list jobs for specific configuration
    - job_ids=[], limit=50, offset=100 → pagination (skip first 100, get next 50)
    - job_ids=[], sort_by="endTime", sort_order="asc" → oldest completed first
    - job_ids=[], sort_by="durationSeconds", sort_order="desc" → longest running first
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    # Case 1: job_ids provided - return full details for those jobs
    if job_ids:

        async def fetch_job_detail(job_id: str) -> JobDetail:
            raw_job = await client.jobs_queue_client.get_job_detail(job_id)
            links = links_manager.get_job_links(job_id)
            LOG.info(f'Found job details for {job_id}.' if raw_job else f'Job {job_id} not found.')
            return JobDetail.model_validate(raw_job | {'links': links})

        results = await process_concurrently(job_ids, fetch_job_detail)
        jobs = unwrap_results(results, 'Failed to fetch one or more jobs')

        LOG.info(f'Retrieved full details for {len(jobs)} jobs.')
        return GetJobsDetailOutput(jobs=jobs)

    # Case 2: no job_ids - list jobs as summaries with optional filtering
    _status = [status] if status else None

    raw_jobs = await client.jobs_queue_client.search_jobs_by(
        component_id=component_id,
        config_id=config_id,
        limit=limit,
        offset=offset,
        status=_status,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    LOG.info(f'Found {len(raw_jobs)} jobs for limit {limit}, offset {offset}, status {status}.')
    jobs = [JobListItem.model_validate(raw_job) for raw_job in raw_jobs]
    links = [links_manager.get_jobs_dashboard_link()]
    return GetJobsListOutput(jobs=jobs, links=links)


@tool_errors()
async def run_job(
    ctx: Context,
    component_id: Annotated[
        str,
        Field(description='The ID of the component or transformation for which to start a job.'),
    ],
    configuration_id: Annotated[str, Field(description='The ID of the configuration for which to start a job.')],
) -> JobDetail:
    """
    Starts a new job for a given component or transformation.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    try:
        raw_job = await client.jobs_queue_client.create_job(
            component_id=component_id, configuration_id=configuration_id
        )
        links_manager = await ProjectLinksManager.from_client(client)
        links = links_manager.get_job_links(str(raw_job['id']))
        job = JobDetail.model_validate(raw_job | {'links': links})
        LOG.info(
            f'Started a new job with id: {job.id} for component {component_id} and configuration {configuration_id}.'
        )
        return job
    except Exception as exception:
        LOG.exception(
            f'Error when starting a new job for component {component_id} and configuration {configuration_id}: '
            f'{exception}'
        )
        raise exception


# End of MCP tools ########################################
