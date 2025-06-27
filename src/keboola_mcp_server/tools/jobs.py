import datetime
import logging
from typing import Annotated, Any, Literal, Optional, Union

from fastmcp import Context
from fastmcp.tools import FunctionTool
from pydantic import AliasChoices, BaseModel, Field, field_validator

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer, listing_output_serializer, with_session_state

LOG = logging.getLogger(__name__)


# Add jobs tools to MCP SERVER ##################################


def add_job_tools(mcp: KeboolaMcpServer) -> None:
    """Add job tools to the MCP server."""
    mcp.add_tool(FunctionTool.from_function(get_job))
    mcp.add_tool(FunctionTool.from_function(list_jobs, serializer=listing_output_serializer))
    mcp.add_tool(FunctionTool.from_function(run_job))

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
        validation_alias=AliasChoices('component', 'componentId', 'component_id', 'component-id'),
        serialization_alias='componentId',
        default=None,
    )
    config_id: Optional[str] = Field(
        description='The ID of the component configuration that the job is running on.',
        validation_alias=AliasChoices('config', 'configId', 'config_id', 'config-id'),
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
        validation_alias='result',
        serialization_alias='result',
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


class ListJobsOutput(BaseModel):
    jobs: list[JobListItem] = Field(..., description='List of jobs.')
    links: list[Link] = Field(..., description='Links relevant to the jobs listing.')

# End of Job Base Models ########################################

# MCP tools ########################################


SORT_BY_VALUES = Literal['startTime', 'endTime', 'createdTime', 'durationSeconds', 'id']
SORT_ORDER_VALUES = Literal['asc', 'desc']


# The Parameter Optional annotation is not working with MCP and when the bot tries to call the tool with appropriate
# parameters, it raises `Invalid type for parameter 'status' in tool retrieve_jobs`, either bot cannot use the tool or
# mcp parsing the parameters is not working. So we need to use Annotated[JOB_STATUS, ...] = None instead of
# Optional[JOB_STATUS] = None despite having type check errors in the code.
@tool_errors()
@with_session_state()
async def list_jobs(
    ctx: Context,
    status: Annotated[
        JOB_STATUS,
        Field(
            Optional[JOB_STATUS],
            description='The optional status of the jobs to filter by, if None then default all.',
        ),
    ] = None,
    component_id: Annotated[
        str,
        Field(
            Optional[str],
            description='The optional ID of the component whose jobs you want to list, default = None.',
        ),
    ] = None,
    config_id: Annotated[
        str,
        Field(
            Optional[str],
            description='The optional ID of the component configuration whose jobs you want to list, default = None.',
        ),
    ] = None,
    limit: Annotated[
        int,
        Field(int, description='The number of jobs to list, default = 100, max = 500.', ge=1, le=500),
    ] = 100,
    offset: Annotated[int, Field(int, description='The offset of the jobs to list, default = 0.', ge=0)] = 0,
    sort_by: Annotated[
        SORT_BY_VALUES,
        Field(
            Optional[SORT_BY_VALUES],
            description='The field to sort the jobs by, default = "startTime".',
        ),
    ] = 'startTime',
    sort_order: Annotated[
        SORT_ORDER_VALUES,
        Field(
            Optional[SORT_ORDER_VALUES],
            description='The order to sort the jobs by, default = "desc".',
        ),
    ] = 'desc',
) -> ListJobsOutput:
    """
    Retrieves all jobs in the project, or filter jobs by a specific component_id or config_id, with optional status
    filtering. Additional parameters support pagination (limit, offset) and sorting (sort_by, sort_order).

    USAGE:
    - Use when you want to list jobs for a given component_id and optionally for given config_id.
    - Use when you want to list all jobs in the project or filter them by status.

    EXAMPLES:
    - If status = "error", only jobs with status "error" will be listed.
    - If status = None, then all jobs with arbitrary status will be listed.
    - If component_id = "123" and config_id = "456", then the jobs for the component with id "123" and configuration
      with id "456" will be listed.
    - If limit = 100 and offset = 0, the first 100 jobs will be listed.
    - If limit = 100 and offset = 100, the second 100 jobs will be listed.
    - If sort_by = "endTime" and sort_order = "asc", the jobs will be sorted by the end time in ascending order.
    """
    _status = [status] if status else None

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

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
    return ListJobsOutput(jobs=jobs, links=links)


@tool_errors()
@with_session_state()
async def get_job(
    job_id: Annotated[
        str,
        Field(description='The unique identifier of the job whose details should be retrieved.'),
    ],
    ctx: Context,
) -> Annotated[JobDetail, Field(JobDetail, description='The detailed information about the job.')]:
    """
    Retrieves detailed information about a specific job, identified by the job_id, including its status, parameters,
    results, and any relevant metadata.

    EXAMPLES:
    - If job_id = "123", then the details of the job with id "123" will be retrieved.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    raw_job = await client.jobs_queue_client.get_job_detail(job_id)
    links = links_manager.get_job_links(job_id)
    LOG.info(f'Found job details for {job_id}.' if raw_job else f'Job {job_id} not found.')
    return JobDetail.model_validate(raw_job | {'links': links})


@tool_errors()
@with_session_state()
async def run_job(
    ctx: Context,
    component_id: Annotated[
        str,
        Field(description='The ID of the component or transformation for which to start a job.'),
    ],
    configuration_id: Annotated[str, Field(description='The ID of the configuration for which to start a job.')],
) -> Annotated[JobDetail, Field(description='The newly started job details.')]:
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
