import datetime
import logging
from typing import Annotated, Any, Literal, Optional, Union

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, field_validator

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors

LOG = logging.getLogger(__name__)


# Add jobs tools to MCP SERVER ##################################


def add_job_tools(mcp: FastMCP) -> None:
    """Add job tools to the MCP server."""
    jobs_tools = [
        retrieve_jobs,
        get_job_detail,
        start_job,
    ]
    for tool in jobs_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(tool)

    LOG.info('Job tools initialized.')


# Job Base Models ########################################

JOB_STATUS = Literal[
    'waiting',
    'processing',
    'success',
    'error',
    'created',
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
    table_id: Optional[str] = Field(
        description='The ID of the table that the job is running on.',
        validation_alias=AliasChoices('tableId', 'table_id', 'table-id'),
        serialization_alias='tableId',
        default=None,
    )
    config_data: Optional[list[Any]] = Field(
        description='The data of the configuration.',
        validation_alias=AliasChoices('configData', 'config_data', 'config-data'),
        serialization_alias='configData',
        default=None,
    )
    config_row_ids: Optional[list[str]] = Field(
        description='The row IDs of the configuration.',
        validation_alias=AliasChoices('configRowIds', 'config_row_ids', 'config-row-ids'),
        serialization_alias='configRowIds',
        default=None,
    )
    run_id: Optional[str] = Field(
        description='The ID of the run that the job is running on.',
        validation_alias=AliasChoices('runId', 'run_id', 'run-id'),
        serialization_alias='runId',
        default=None,
    )
    parent_run_id: Optional[str] = Field(
        description='The ID of the parent run that the job is running on.',
        validation_alias=AliasChoices('parentRunId', 'parent_run_id', 'parent-run-id'),
        serialization_alias='parentRunId',
        default=None,
    )
    result: Optional[dict[str, Any]] = Field(
        description='The results of the job.',
        validation_alias='result',
        serialization_alias='result',
        default=None,
    )

    @field_validator('result', mode='before')
    @classmethod
    def validate_result_field(cls, current_value: Union[list[Any], dict[str, Any], None]) -> dict[str, Any]:
        # Ensures that if the result field is passed as an empty list [] or None, it gets converted to an empty dict {}.
        # Why? Because the result is expected to be an Object, but create job endpoint sends [], perhaps it means
        # "empty". This avoids type errors.
        if not isinstance(current_value, dict):
            if not current_value:
                return dict()
            if isinstance(current_value, list):
                raise ValueError(f'Field "result" cannot be a list, expecting dictionary, got: {current_value}.')
        return current_value


# End of Job Base Models ########################################

# MCP tools ########################################

SORT_BY_VALUES = Literal['startTime', 'endTime', 'createdTime', 'durationSeconds', 'id']
SORT_ORDER_VALUES = Literal['asc', 'desc']


# The Parameter Optional annotation is not working with MCP and when the bot tries to call the tool with appropriate
# parameters, it raises `Invalid type for parameter 'status' in tool retrieve_jobs`, either bot cannot use the tool or
# mcp parsing the parameters is not working. So we need to use Annotated[JOB_STATUS, ...] = None instead of
# Optional[JOB_STATUS] = None despite having type check errors in the code.
@tool_errors()
async def retrieve_jobs(
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
) -> Annotated[
    list[JobListItem],
    Field(
        list[JobListItem],
        description='The retrieved list of jobs list items. If empty then no jobs were found.',
    ),
]:
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
    client = KeboolaClient.from_state(ctx.session.state)
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
    return [JobListItem.model_validate(raw_job) for raw_job in raw_jobs]


@tool_errors()
async def get_job_detail(
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

    raw_job = await client.jobs_queue_client.get_job_detail(job_id)
    LOG.info(f'Found job details for {job_id}.' if raw_job else f'Job {job_id} not found.')
    return JobDetail.model_validate(raw_job)


@tool_errors()
async def start_job(
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
        job = JobDetail.model_validate(raw_job)
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
