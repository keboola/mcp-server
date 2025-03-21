import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

STR_INT = Union[str, int]


class JobListItem(BaseModel):
    """A list item representing a Keboola job."""

    id: STR_INT = Field(description="The ID of the job.")
    status: Literal["waiting", "processing", "success", "error"] = Field(
        description="The status of the job."
    )
    created_time: str = Field(description="The creation time of the job.", alias="createdTime")
    start_time: str = Field(description="The start time of the job.", alias="startTime")
    end_time: str = Field(description="The end time of the job.", alias="endTime")


class JobDetail(JobListItem):
    """Detailed information about a Keboola job."""

    url: str = Field(description="The URL of the job.")
    table_id: Optional[str] = Field(description="The ID of the table that the job is running on.")
    operation_name: str = Field(description="The name of the operation.", alias="operationName")
    operation_params: Dict[str, Any] = Field(
        description="The parameters of the operation.", alias="operationParams"
    )
    run_id: Optional[STR_INT] = Field(description="The ID of the run that the job is running on.")
    results: Optional[str] = Field(description="The results of the job.")
    metrics: Optional[Dict[str, Any]] = Field(description="The metrics of the job.")

    def __init__(self, **data: Any) -> None:
        """Initialize Job model and populate additional_details from remaining fields."""

        model_fields = set(self.model_fields.keys())
        # Extract fields that are not part of the model definition
        additional = {k: v for k, v in data.items() if k not in model_fields}

        for k in additional:
            data.pop(k)

        # Initialize the model with defined fields
        super().__init__(**data)

        # Set additional_details with remaining fields
        self.additional_details = additional


def add_jobs_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(list_jobs)
    mcp.add_tool(get_job_details)
    logger.info("Jobs tools added to the MCP server.")


async def list_jobs(ctx: Context) -> List[JobListItem]:
    """List all jobs."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_jobs = client.storage_client.jobs.list()
    logger.info(f"Found {len(r_jobs)} jobs.")
    return [JobListItem.model_validate(r_job) for r_job in r_jobs]


async def get_job_details(job_id: Annotated[str, "The ID of the job."], ctx: Context) -> JobDetail:
    """Get the details of a job."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_job = client.storage_client.jobs.detail(job_id)
    logger.info(f"Found job details for {job_id}." if r_job else f"Job {job_id} not found.")
    return JobDetail.model_validate(r_job)
