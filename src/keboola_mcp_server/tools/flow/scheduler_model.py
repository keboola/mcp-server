"""
Pydantic models for Keboola Scheduler API.

These models represent the structure of schedulers used to automate flow execution.
"""

import logging
from typing import Literal, Optional

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.clients.scheduler import ScheduleModelApiResponse, ScheduleExecution
from keboola_mcp_server.links import Link

LOGGER = logging.getLogger(__name__)


ScheduleType = Literal['yearly', 'monthly', 'weekly', 'daily', 'hourly']


class ScheduleRequest(BaseModel):
    """Input scheduler model."""

    schedule_id: str = Field(
        description='Schedule API ID',
        serialization_alias='scheduleId',
        validation_alias=AliasChoices('id'),
        default_factory=str,
    )
    action: Literal['enable', 'disable', 'delete', 'modify', 'create'] = Field(description='Schedule state')
    schedule: Optional['SimplifiedCronSchedule'] = Field(description='Cron schedule', default_factory=None)


class SchedulesOutput(BaseModel):
    """Scheduler output model."""

    schedules: list['Schedule'] = Field(description='List of schedules', default_factory=list)
    n_schedules: int = Field(description='Number of schedules', default=0)
    links: list[Link] = Field(description='List of links', default_factory=list)


class SimplifiedCronSchedule(BaseModel):
    """Schedule configuration model."""

    type: ScheduleType = Field(
        description=('Schedule period type. Depending on the type, only the relevant fields should be defined')
    )
    timezone: str = Field(description='Timezone', default='UTC')
    in_months: list[int] = Field(description='Months to run (1-12)', default_factory=list)
    on_days: list[int] = Field(
        description='Days of the month to run (1-31 if monthly, 0-6 if weekly)', default_factory=list
    )
    at_hour: list[int] = Field(description='Hour of the day to run (0-23)', default_factory=list)
    at_minutes: list[int] = Field(description='Minute of the hour to run (0-59)', default_factory=list)

    @classmethod
    def from_cron_tab(cls, cron_tab: str) -> 'SimplifiedCronSchedule':
        """Create a simplified cron schedule from a cron expression."""
        split_cron_tab = cron_tab.split()
        assert len(split_cron_tab) == 5, "Cron expression must have 5 parts"
        minutes, hours, days, months, weekdays = [x.split(',') if x != '*' else [] for x in split_cron_tab]
        schedule_type: Optional[ScheduleType] = None
        if minutes and hours and days and months:
            schedule_type = 'yearly'
        elif minutes and hours and days:
            schedule_type = 'monthly'
        elif minutes and hours and weekdays:
            schedule_type = 'weekly'
        elif minutes and hours:
            schedule_type = 'daily'
        elif minutes:
            schedule_type = 'hourly'
        else:
            LOGGER.warning(f"Could not determine schedule type from cron expression: {cron_tab}")

        return cls.model_construct(
            type=schedule_type or 'yearly',
            at_minutes=minutes,
            at_hour=hours,
            on_days=days or weekdays,
            in_months=months,
        )

    def to_cron_tab(self) -> str:
        """Convert the simplified cron schedule to a cron expression."""

        def elem_to_cron(list: list[int]) -> str:
            return ','.join(str(item) for item in list) if list else '*'

        at_minutes = elem_to_cron(self.at_minutes)
        at_hour = elem_to_cron(self.at_hour)
        on_days = elem_to_cron(self.on_days)
        in_months = elem_to_cron(self.in_months)

        if self.type == "weekly":
            assert all(0 <= day <= 6 for day in on_days), "Days of the week must be between 0 and 6"
            return f'{at_minutes} {at_hour} * * {on_days}'
        else:
            return f'{at_minutes} {at_hour} {on_days} {in_months} *'


class Schedule(BaseModel):
    """Lightweight schedule summary for flow models."""

    schedule_id: str = Field(
        description='Schedule API ID', serialization_alias='scheduleId', validation_alias=AliasChoices('id')
    )
    timezone: str = Field(description='Timezone')
    state: Literal['enabled', 'disabled'] = Field(description='Schedule state')
    simplified_schedule: SimplifiedCronSchedule = Field(
        description='Cron expression',
        validation_alias=AliasChoices('cron_tab', 'cronTab', 'cron-tab'),
        serialization_alias='cronTab',
    )
    executions: list[ScheduleExecution] = Field(default_factory=list, description='List of recent executions')

    @classmethod
    def from_scheduler_response(cls, schedule_api: ScheduleModelApiResponse) -> 'Schedule':
        """Create a schedule from a schedule response."""
        return cls.model_construct(
            scheduler_id=schedule_api.id,
            timezone=schedule_api.schedule.timezone,
            state=schedule_api.schedule.state,
            simplified_schedule=SimplifiedCronSchedule.from_cron_tab(schedule_api.schedule.cron_tab),
        )


# CREATE
# connection.keboola.com
# :method
# POST
# :path
# /v2/storage/branch/880986/components/keboola.scheduler/config
# {"name":"Scheduler for 01kbmdg16s25c0swy92wq891ky","configuration":"{\"schedule\":{\"cronTab\":\"55 2 * * 6,0\",\"timezone\":\"UTC\",\"state\":\"enabled\"},\"target\":{\"componentId\":\"keboola.flow\",\"configurationId\":\"01kbmdg16s25c0swy92wq891ky\",\"mode\":\"run\"}}"}
# RESPONSE
# {
#     "id": "01kcpahx3zpmww3b8z577macz9",
#     "name": "Scheduler for 01kbmdg16s25c0swy92wq891ky",
#     "description": "",
#     "created": "2025-12-17T15:13:48+0100",
#     "creatorToken": {
#         "id": 6416140,
#         "description": "marian.krotil@keboola.com"
#     },
#     "version": 1,
#     "changeDescription": "Configuration created",
#     "isDisabled": false,
#     "isDeleted": false,
#     "configuration": {
#         "schedule": {
#             "cronTab": "55 2 * * 6,0",
#             "timezone": "UTC",
#             "state": "enabled"
#         },
#         "target": {
#             "componentId": "keboola.flow",
#             "configurationId": "01kbmdg16s25c0swy92wq891ky",
#             "mode": "run"
#         }
#     },
#     "state": {},
#     "currentVersion": {
#         "created": "2025-12-17T15:13:48+0100",
#         "creatorToken": {
#             "id": 6416140,
#             "description": "marian.krotil@keboola.com"
#         },
#         "changeDescription": "Configuration created",
#         "versionIdentifier": "01KCPAHX49GQQFPYP1GDXD845G"
#     }
# }
# :authority
# scheduler.keboola.com
# :method
# POST
# :path
# /schedules
# {configurationId: "01kcpahx3zpmww3b8z577macz9"}
# configurationId
# :
# "01kcpahx3zpmww3b8z577macz9"

# {
#     "id": "35049",
#     "tokenId": "6699560",
#     "configurationId": "01kcpahx3zpmww3b8z577macz9",
#     "configurationVersionId": "1",
#     "schedule": {
#         "cronTab": "55 2 * * 6,0",
#         "timezone": "UTC",
#         "state": "enabled"
#     },
#     "target": {
#         "componentId": "keboola.flow",
#         "configurationId": "01kbmdg16s25c0swy92wq891ky",
#         "configurationRowIds": [],
#         "mode": "run",
#         "tag": ""
#     },
#     "executions": []
# }

# UPDATE
# :authority
# connection.keboola.com
# :method
# PUT
# :path
# /v2/storage/branch/880986/components/keboola.scheduler/configs/01kcpa717z4gc0z3refpvveej0

# scheduler.keboola.com
# :method
# POST
# :path
# /schedules
# {configurationId: "01kcpa717z4gc0z3refpvveej0"}
# configurationId
# :
# "01kcpa717z4gc0z3refpvveej0"
# DELETE
# Request URL
# https://scheduler.keboola.com/configurations/01kcpa94375jb3qvsadvm9ycgz
# Request Method
# DELETE
# Status Code
