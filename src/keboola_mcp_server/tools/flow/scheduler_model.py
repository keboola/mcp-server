"""
Pydantic models for representing scheduler details and requests for Agent tool output and inputs.

These models represent the structure of schedulers used to automate flow execution.
"""

from typing import Literal

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.clients.scheduler import ScheduleApiResponse, TargetExecution
from keboola_mcp_server.links import Link


class ScheduleRequest(BaseModel):
    action: Literal['add', 'update', 'remove'] = Field(description='Action to perform on the schedule.')
    schedule_id: str | None = Field(
        description='ID of the schedule configuration to update. None if creating a new schedule.',
        default=None,
        serialization_alias='scheduleId',
        validation_alias=AliasChoices('scheduleId', 'schedule_id'),
    )
    timezone: str | None = Field(description='Timezone for the schedule. Default UTC if None provided.', default=None)
    cron_tab: str | None = Field(
        description=(
            'Cron expression for the schedule following the format: `* * * * *`.'
            'Where 1. minutes, 2. hours, 3. days of month, 4. months, 5. days of week. Example: `15,45 1,13 * * 0`'
        ),
        default=None,
        validation_alias=AliasChoices('cronTab', 'cron_tab'),
        serialization_alias='cronTab',
    )
    state: Literal['enabled', 'disabled'] | None = Field(description='Enable or disable the schedule.', default=None)


class SchedulesOutput(BaseModel):
    """Schedules output used in the flow models when getting details."""

    schedules: list['ScheduleDetail'] = Field(description='List of schedules', default_factory=list)
    n_schedules: int = Field(description='Number of schedules', default=0)
    links: list[Link] = Field(description='List of links', default_factory=list)


class ScheduleDetail(BaseModel):
    """Schedule model for flow tools."""

    schedule_id: str = Field(
        description='Schedule configuration ID',
        serialization_alias='scheduleId',
        validation_alias=AliasChoices('id', 'schedule_id', 'scheduleId'),
    )
    timezone: str = Field(description='Timezone')
    state: Literal['enabled', 'disabled'] = Field(description='Schedule state')
    timezone: str = Field(description='Timezone')
    cron_tab: str = Field(
        description=(
            'Cron Tab `* * * * *`. Where 1. minutes, 2. hours, 3. days of month, 4. months, 5. days of week.'
            'Example: `15,45 1,13 * * 0`'
        ),
        serialization_alias='cronTab',
        validation_alias=AliasChoices('cronTab', 'cron_tab'),
    )
    target_executions: list[TargetExecution] = Field(default_factory=list, description='List of recent target runs')

    @classmethod
    def from_api_response(cls, schedule_api: ScheduleApiResponse) -> 'ScheduleDetail':
        """Create a schedule detail from a schedule response."""
        return cls.model_construct(
            schedule_id=schedule_api.configuration_id,
            timezone=schedule_api.schedule.timezone,
            state=schedule_api.schedule.state,
            cron_tab=schedule_api.schedule.cron_tab,
            target_executions=schedule_api.executions,
        )
