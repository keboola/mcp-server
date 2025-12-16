"""
Keboola Scheduler API client.

This client handles communication with the Scheduler API (scheduler.keboola.com)
for managing scheduled flow executions.
"""

from datetime import datetime
import logging
from typing import Any

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import KeboolaServiceClient, RawKeboolaClient

LOG = logging.getLogger(__name__)


class ScheduleConfiguration(BaseModel):
    """Schedule configuration model."""

    cron_tab: str = Field(alias='cronTab', description='Cron expression for scheduling')
    timezone: str = Field(description='Timezone for the schedule')
    state: str = Field(description='Schedule state (enabled/disabled)')


class TargetConfiguration(BaseModel):
    """Target configuration model."""

    component_id: str = Field(alias='componentId', description='Component ID to execute')
    configuration_id: str = Field(alias='configurationId', description='Configuration ID to execute')
    mode: str = Field(description='Execution mode (run)')
    tag: str | None = Field(default=None, description='Optional tag version')


class SchedulerExecution(BaseModel):
    """Scheduler execution model."""

    job_id: str = Field(alias='jobId', description='Job ID of the execution')
    execution_time: datetime = Field(alias='executionTime', description='Execution time')


class SchedulerModelApiResponse(BaseModel):
    """Scheduler API response model."""

    id: str = Field(description='Scheduler ID (numeric string)')
    token_id: str = Field(alias='tokenId', description='Token ID used for authentication')
    configuration_id: str = Field(alias='configurationId', description='Configuration ID from Storage API')
    configuration_version_id: str = Field(alias='configurationVersionId', description='Configuration version ID')
    schedule: ScheduleConfiguration = Field(description='Schedule configuration')
    target: TargetConfiguration = Field(description='Target configuration')
    executions: list[SchedulerExecution] = Field(default_factory=list, description='List of recent executions')

class SchedulerClient(KeboolaServiceClient):
    """Client for interacting with the Keboola Scheduler API."""

    def __init__(self, raw_client: RawKeboolaClient, branch_id: str | None = None) -> None:
        """
        Creates a SchedulerClient from a RawKeboolaClient.

        :param raw_client: The raw client to use
        :param branch_id: The id of the branch (currently unused for Scheduler API)
        """
        super().__init__(raw_client=raw_client)
        self._branch_id = branch_id

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str | None,
        branch_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> 'SchedulerClient':
        """
        Creates a SchedulerClient from a Keboola Storage API token.

        :param root_url: The root URL of the Scheduler API
        :param token: The Keboola Storage API token. If None, the client will not send any authorization header.
        :param branch_id: The id of the Keboola project branch (currently unused for Scheduler API)
        :param headers: Additional headers for the requests
        :return: A new instance of SchedulerClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
            ),
            branch_id=branch_id,
        )

    async def activate_scheduler(self, scheduler_id: str) -> SchedulerModelApiResponse:
        """
        Activate a scheduler in the Scheduler API.

        This is the second step in scheduler creation, after the scheduler configuration
        has been created in Storage API.

        :param scheduler_id: The new scheduler configuration ID (keboola.scheduler config ID)
        :return: The scheduler response with id, schedule, target, etc.
        """
        payload = {'configurationId': scheduler_id}
        response = await self.post(endpoint='schedules', data=payload)
        return SchedulerModelApiResponse.model_validate(response)

    async def get_scheduler(self, scheduler_id: str) -> SchedulerModelApiResponse:
        """
        Get scheduler details by scheduler ID.

        :param scheduler_id: The scheduler ID (numeric string)
        :return: The scheduler details
        """
        response = await self.get(endpoint=f'schedules/{scheduler_id}')
        return SchedulerModelApiResponse.model_validate(response)

    async def get_schedulers_by_config_id(self, component_id: str, configuration_id: str) -> list[SchedulerModelApiResponse]:
        """
        Get scheduler details by Storage API component and configuration ID.

        :param component_id: The Storage API component ID
        :param configuration_id: The Storage API configuration ID
        :return: The list of scheduler details
        """
        params = {
            'componentId': component_id,
            'configurationId': configuration_id,
        }
        response = await self.get(endpoint=f'schedules', params=params)
        return [SchedulerModelApiResponse.model_validate(scheduler) for scheduler in response]

    async def list_schedulers(self) -> list[SchedulerModelApiResponse]:
        """
        List all schedulers for the current project/token.

        :return: The list of scheduler details
        """
        response = await self.get(endpoint='schedules')
        if isinstance(response, list):
            return [SchedulerModelApiResponse.model_validate(scheduler) for scheduler in response]
        return [SchedulerModelApiResponse.model_validate(response)]

    async def deactivate_scheduler(self, scheduler_id: str) -> None:
        """
        Deactivate a scheduler by its Scheduler API ID.

        This is the first step in scheduler deletion. After this, the configuration
        should also be deleted from Storage API.

        :param scheduler_id: The Scheduler API ID
        """
        await self.delete(endpoint=f'schedules/{scheduler_id}')

    async def delete_scheduler(self, configuration_id: str) -> None:
        """
        Delete a scheduler by its Storage API configuration ID.

        :param configuration_id: The Scheduler API configuration ID
        """
        await self.delete(endpoint=f'configurations/{configuration_id}')
