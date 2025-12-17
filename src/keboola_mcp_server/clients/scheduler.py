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


class ScheduleExecution(BaseModel):
    """Schedule execution model."""

    job_id: str = Field(alias='jobId', description='Job ID of the execution')
    execution_time: datetime = Field(alias='executionTime', description='Execution time')


class ScheduleModelApiResponse(BaseModel):
    """Schedule API response model."""

    id: str = Field(description='Schedule ID (numeric string)')
    token_id: str = Field(alias='tokenId', description='Token ID used for authentication')
    configuration_id: str = Field(alias='configurationId', description='Configuration ID from Storage API')
    configuration_version_id: str = Field(alias='configurationVersionId', description='Configuration version ID')
    schedule: ScheduleConfiguration = Field(description='Schedule configuration')
    target: TargetConfiguration = Field(description='Target configuration')
    executions: list[ScheduleExecution] = Field(default_factory=list, description='List of recent executions')


class SchedulerClient(KeboolaServiceClient):
    """Client for interacting with the Keboola Scheduler API."""

    def __init__(self, raw_client: RawKeboolaClient, branch_id: str | None = None) -> None:
        """
        Creates a SchedulerClient from a RawKeboolaClient.

        :param raw_client: The raw client to use
        :param branch_id: The id of the branch
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

    async def activate_schedule(self, schedule_configuration_id: str) -> ScheduleModelApiResponse:
        """
        Activate a schedule in the Scheduler API.

        This is the second step in schedule creation, after the schedule configuration
        has been created in Storage API.

        :param schedule_configuration_id: The new schedule configuration ID (keboola.scheduler config ID)
        :return: The schedule response with id, schedule, target, etc.
        """
        payload = {'configurationId': schedule_configuration_id}
        response = await self.post(endpoint='schedules', data=payload)
        return ScheduleModelApiResponse.model_validate(response)

    async def get_schedule(self, schedule_id: str) -> ScheduleModelApiResponse:
        """
        Get schedule details by schedule ID.

        :param schedule_id: The schedule ID (numeric string)
        :return: The schedule details
        """
        response = await self.get(endpoint=f'schedules/{schedule_id}')
        return ScheduleModelApiResponse.model_validate(response)

    async def list_schedules_by_config_id(
        self, component_id: str, configuration_id: str
    ) -> list[ScheduleModelApiResponse]:
        """
        Get schedules details by Storage API component and configuration ID.

        :param component_id: The Storage API component ID
        :param configuration_id: The Storage API configuration ID
        :return: The list of schedules details
        """
        params = {
            'componentId': component_id,
            'configurationId': configuration_id,
        }
        response = await self.get(endpoint=f'schedules', params=params)
        return [ScheduleModelApiResponse.model_validate(schedule) for schedule in response]

    async def list_schedules(self) -> list[ScheduleModelApiResponse]:
        """
        List all schedules for the current project/token.

        :return: The list of schedules details
        """
        response = await self.get(endpoint='schedules')
        if isinstance(response, list):
            return [ScheduleModelApiResponse.model_validate(schedule) for schedule in response]
        return [ScheduleModelApiResponse.model_validate(response)]

    async def deactivate_schedule(self, schedule_id: str) -> None:
        """
        Deactivate a schedule by its Schedule API ID.

        This is the first step in schedule deletion. After this, the configuration
        should also be deleted from Storage API.

        :param schedule_id: The Schedule API ID
        """
        await self.delete(endpoint=f'schedules/{schedule_id}')

    async def delete_schedule(self, configuration_id: str) -> None:
        """
        Delete a schedule by its Storage API configuration ID.

        :param configuration_id: The Schedule API configuration ID
        """
        await self.delete(endpoint=f'configurations/{configuration_id}')
