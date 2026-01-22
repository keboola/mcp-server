"""
Keboola Scheduler API client.

This client handles communication with the Scheduler API (scheduler.keboola.com)
for managing scheduled flow executions.
"""

import logging
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import KeboolaServiceClient, RawKeboolaClient

LOG = logging.getLogger(__name__)


class Schedule(BaseModel):

    cron_tab: str = Field(alias='cronTab', description='Cron expression for scheduling')
    timezone: str = Field(description='Timezone for the schedule')
    state: str = Field(description='Schedule state (enabled/disabled)')


class TargetConfiguration(BaseModel):

    component_id: str = Field(alias='componentId', description='Component ID to execute')
    configuration_id: str = Field(alias='configurationId', description='Configuration ID to execute')
    mode: str = Field(description='Execution mode (run)')
    tag: str | None = Field(default=None, description='Optional tag version')


class TargetExecution(BaseModel):
    """Target execution model having information about the execution of the target component configuration."""

    job_id: str = Field(alias='jobId', description='Job ID of the execution')
    execution_time: datetime = Field(alias='executionTime', description='Execution time')


class ScheduleApiResponse(BaseModel):
    """Schedule API response model."""

    id: str = Field(description='Schedule ID (numeric string)')
    token_id: str = Field(alias='tokenId', description='Token ID used for authentication')
    configuration_id: str = Field(alias='configurationId', description='Configuration ID from Storage API')
    configuration_version_id: str = Field(alias='configurationVersionId', description='Configuration version ID')
    schedule: Schedule = Field(description='Schedule configuration')
    target: TargetConfiguration = Field(description='Target configuration')
    executions: list[TargetExecution] = Field(default_factory=list, description='List of recent executions')


class SchedulerClient(KeboolaServiceClient):
    """Client for interacting with the Keboola Scheduler API."""

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        """
        Creates a SchedulerClient from a RawKeboolaClient.

        :param raw_client: The raw client to use
        :param branch_id: The id of the branch
        """
        super().__init__(raw_client=raw_client)

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str | None,
        headers: dict[str, Any] | None = None,
        readonly: bool | None = None,
    ) -> 'SchedulerClient':
        """
        Creates a SchedulerClient from a Keboola Storage API token.

        :param root_url: The root URL of the Scheduler API
        :param token: The Keboola Storage API token. If None, the client will not send any authorization header.
        :param headers: Additional headers for the requests
        :param readonly: If True, the client will only use HTTP GET, HEAD operations.
        :return: A new instance of SchedulerClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
                readonly=readonly,
            )
        )

    async def activate_schedule(self, schedule_config_id: str) -> ScheduleApiResponse:
        """
        Activate a schedule in the Scheduler API by its Storage API configuration ID.

        This is the second step in schedule creation, after the schedule configuration
        has been created in Storage API.

        :param schedule_config_id: The schedule configuration ID in Storage API
        :return: The schedule response with id, schedule, target, etc.
        """
        payload = {'configurationId': schedule_config_id}
        response = await self.post(endpoint='schedules', data=payload)
        return ScheduleApiResponse.model_validate(response)

    async def get_schedule(self, schedule_id: str) -> ScheduleApiResponse:
        """
        Get schedule details by schedule ID from Scheduler API.

        :param schedule_id: The schedule ID (numeric string)
        :return: The schedule details
        """
        response = await self.get(endpoint=f'schedules/{schedule_id}')
        return ScheduleApiResponse.model_validate(response)

    async def list_schedules_by_config_id(self, component_id: str, configuration_id: str) -> list[ScheduleApiResponse]:
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
        response = await self.get(endpoint='schedules', params=params)
        return [ScheduleApiResponse.model_validate(schedule) for schedule in response]

    async def list_schedules(self) -> list[ScheduleApiResponse]:
        """
        List all schedules for the current project/token.

        :return: The list of schedules details
        """
        response = await self.get(endpoint='schedules')
        if isinstance(response, list):
            return [ScheduleApiResponse.model_validate(schedule) for schedule in response]
        return [ScheduleApiResponse.model_validate(response)]

    async def delete_schedule(self, schedule_config_id: str) -> None:
        """
        Delete a schedule by its Storage API configuration ID.

        :param schedule_config_id: The schedule configuration ID in Storage API
        """
        await self.delete(endpoint=f'configurations/{schedule_config_id}')
