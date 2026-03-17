"""Scheduler management functions for creating, updating, and deleting schedulers."""

import logging
from typing import Any, Sequence

from pydantic import AliasChoices, BaseModel, Field, field_validator

from keboola_mcp_server.clients.client import FlowType, KeboolaClient
from keboola_mcp_server.clients.storage import CreateConfigurationAPIResponse
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.tools.components.utils import set_cfg_creation_metadata, set_cfg_update_metadata
from keboola_mcp_server.tools.flow.model import Flow, FlowSummary
from keboola_mcp_server.tools.flow.scheduler_model import (
    ScheduleDetail,
    ScheduleRequest,
    SchedulesOutput,
)

LOG = logging.getLogger(__name__)

SCHEDULER_COMPONENT_ID = 'keboola.scheduler'


CRON_TAB_INSTRUCTIONS = """
Cron Tab Expression should be in the format: `* * * * *`.
Field order:
1. Minute (0-59)
2. Hour (0-23)
3. Day of month (1-31, or L for last day of month)
4. Month (1-12)
5. Day of week (0-6, where 0 = Sunday)

Examples:
1. schedule daily at 1:00 PM and 1:00 AM would be `0 1,13 * * *`
2. schedule weekly on Monday at 9:00 AM would be `0 9 * * 1`
3. schedule monthly on the 1st and 20th day of the month at 10:00 AM would be `0 10 1,20 * *`
4. schedule yearly on the 1st of january and august at 11:00 AM would be `0 11 1 1,8 *`
5. schedule hourly every 15 minutes would be `0,15,30,45 * * * *`
6. schedule monthly on the last day of the month at 10:00 AM would be `0 10 L * *`
"""


def validate_cron_tab(cron_tab: str | None) -> None:
    """Validate the cron tab expression."""
    try:
        if cron_tab is None:
            return None
        split_cron_tab = cron_tab.strip().split()
        if len(split_cron_tab) != 5:
            raise ValueError(
                f'Cron expression must have exactly 5 parts got: {cron_tab} which has {len(split_cron_tab)} parts.'
            )

        def to_int_list(field: str, allow_l: bool = False) -> tuple[list[int], bool]:
            """Parse a cron field into a list of integers and a flag indicating if L was found."""
            if field == '*':
                return [], False
            has_l = False
            parts = []
            for x in field.split(','):
                x = x.strip()
                if allow_l and x.upper() == 'L':
                    has_l = True
                else:
                    try:
                        parts.append(int(x))
                    except ValueError:
                        raise ValueError(f'Cron expression must have only digits got: {field} in "{cron_tab}".')
            if allow_l and has_l and parts:
                raise ValueError('Day of month must use either `L` or numeric values, not both.')
            return parts, has_l

        minutes, _ = to_int_list(split_cron_tab[0].strip())
        hours, _ = to_int_list(split_cron_tab[1].strip())
        days, has_last_day = to_int_list(split_cron_tab[2].strip(), allow_l=True)
        months, _ = to_int_list(split_cron_tab[3].strip())
        weekdays, _ = to_int_list(split_cron_tab[4].strip())

        if any(x < 0 or x > 59 for x in minutes):
            raise ValueError(f'Minutes of hour `M _ _ _ _` must be between 0 and 59, got: {split_cron_tab[0]}')
        if any(x < 0 or x > 23 for x in hours):
            raise ValueError(f'Hours of day `_ H _ _ _` must be between 0 and 23, got: {split_cron_tab[1]}')
        if any(x < 1 or x > 31 for x in days):
            raise ValueError(f'Days of month `_ _ D _ _`must be between 1 and 31, got: {split_cron_tab[2]}')
        if any(x < 1 or x > 12 for x in months):
            raise ValueError(f'Months of year `_ _ _ M _` must be between 1 and 12, got: {split_cron_tab[3]}')
        if any(x < 0 or x > 6 for x in weekdays):
            raise ValueError(
                f'Days of week `_ _ _ _ W` must be between 0=Sunday and 6=Saturday, got: {split_cron_tab[4]}'
            )
        if months and not days and not has_last_day:
            raise ValueError('Months of year must be specified with days of month. Example: `35 12 31 1,3 *`')
        if (days or has_last_day) and not hours:
            raise ValueError('Days of month must be specified with hours of day. Example: `55 12 31 * *`')
        if hours and not minutes:
            raise ValueError('Hours of day must be specified with minutes of hour. Example: `55 12 * * *`')
        if weekdays and not hours:
            raise ValueError('Days of week must be specified with hours of day. Example: `55 12 * * 0`')
        if weekdays and (days or months or has_last_day):
            raise ValueError('Days of week must not be specified with days of month nor months of year.')
    except ValueError as e:
        raise ValueError(f'Invalid cron tab expression: {str(e)}.\n{CRON_TAB_INSTRUCTIONS}') from e


class SimplifiedSchedule(BaseModel):
    """Simplified schedule dictionary."""

    schedule_id: str | None = Field(
        description='The schedule ID',
        validation_alias=AliasChoices('scheduleId', 'schedule_id'),
        serialization_alias='scheduleId',
        default=None,
    )
    cron_tab: str = Field(
        description='The cron tab',
        validation_alias=AliasChoices('cronTab', 'cron_tab'),
        serialization_alias='cronTab',
    )
    timezone: str = Field(
        description='The timezone',
        default='UTC',
    )
    state: str = Field(
        description='The state',
        default='enabled',
    )

    @field_validator('cron_tab')
    @classmethod
    def _validate_cron_tab(cls, value: str) -> str:
        validate_cron_tab(value)
        return value

    def update_from_request(self, request: ScheduleRequest) -> 'SimplifiedSchedule':
        """Return a new schedule with the updated fields from the request."""
        if self.schedule_id != request.schedule_id:
            raise ValueError(f'Cannot update schedule with different ID: {self.schedule_id} != {request.schedule_id}')
        return SimplifiedSchedule(
            schedule_id=self.schedule_id,
            cron_tab=self.cron_tab if request.cron_tab is None else request.cron_tab,
            timezone=self.timezone if request.timezone is None else request.timezone,
            state=self.state if request.state is None else request.state,
        )


async def _update_schedulers_internal(
    *,
    client: KeboolaClient,
    configuration_id: str,
    component_id: str,
    schedules: Sequence[ScheduleRequest] = tuple(),
) -> tuple[dict[str, SimplifiedSchedule], dict[str, SimplifiedSchedule | None], list[SimplifiedSchedule]]:
    """
    Compute original, updated and new schedulers for preview by adding/updating/removing schedules.

    :param client: KeboolaClient instance
    :param configuration_id: The configuration ID to schedule
    :param component_id: The component ID to schedule
    :param schedules: The list of schedule requests to compute the preview for
    :return: A tuple of the original, updated and new schedulers
    """

    current_schedulers = await list_schedules_for_config(
        client=client, component_id=component_id, configuration_id=configuration_id
    )

    original_schedulers: dict[str, SimplifiedSchedule] = {
        schedule.schedule_id: SimplifiedSchedule(
            schedule_id=schedule.schedule_id,
            cron_tab=schedule.cron_tab,
            timezone=schedule.timezone,
            state=schedule.state,
        )
        for schedule in current_schedulers
    }
    new_schedulers: list[SimplifiedSchedule] = []
    updated_schedulers: dict[str, SimplifiedSchedule | None] = {}
    for request in schedules:
        if request.action == 'add':
            new_schedulers.append(
                SimplifiedSchedule.model_validate(request.model_dump(by_alias=True, exclude_none=True))
            )
        elif request.action == 'update':
            if request.schedule_id not in original_schedulers:
                raise ValueError(
                    f'Schedule (ID: {request.schedule_id}) cannot be updated because it was not found in the '
                    'existing schedulers.'
                )
            updated_schedulers[request.schedule_id] = original_schedulers[request.schedule_id].update_from_request(
                request
            )
        elif request.action == 'remove':
            if request.schedule_id not in original_schedulers:
                raise ValueError(
                    f'Schedule (ID: {request.schedule_id}) cannot be removed because it was not found in the '
                    'existing schedulers.'
                )
            updated_schedulers[request.schedule_id] = None
        else:
            raise ValueError(f'Invalid action for schedulers: {request.action}.')
    return original_schedulers, updated_schedulers, new_schedulers


async def compute_schedulers_preview(
    *,
    client: KeboolaClient,
    configuration_id: str,
    flow_type: FlowType,
    schedules: Sequence[ScheduleRequest],
) -> dict[str, list[dict[str, Any]]]:
    """
    Compute the preview of the schedulers for a configuration.

    :param client: KeboolaClient instance
    :param configuration_id: The configuration ID to schedule
    :param flow_type: The type of flow to schedule
    :param schedules: The list of schedule requests to compute the preview for
    :return: A mutator preview payload with original and updated schedulers
    """
    original_schedulers, updated_schedulers, new_schedulers = await _update_schedulers_internal(
        client=client, configuration_id=configuration_id, component_id=flow_type, schedules=schedules
    )

    # Sync the updated schedulers with the original schedulers and sort them by schedule_id for diff preview
    synced_updated_list = []
    original_list = []
    for prev in sorted(original_schedulers.values(), key=lambda x: x.schedule_id):
        original_list.append(prev.model_dump(by_alias=True, exclude_none=False))
        if prev.schedule_id in updated_schedulers:
            if updated_schedulers[prev.schedule_id] is None:
                # Explicit remove action -> the schedule should not appear in the updated preview list.
                continue
            # Update schedule -> add the updated schedule.
            synced_updated_list.append(
                updated_schedulers[prev.schedule_id].model_dump(by_alias=True, exclude_none=False)
            )
        else:
            # No update -> sync the original schedule as it is.
            synced_updated_list.append(prev.model_dump(by_alias=True, exclude_none=False))

    new_list = [s.model_dump(by_alias=True, exclude_none=False) for s in new_schedulers]
    return {
        'original_schedulers': original_list,
        'updated_schedulers': synced_updated_list + new_list,
    }


async def process_schedule_request(
    client: KeboolaClient,
    target_component_id: str,
    target_configuration_id: str,
    requests: Sequence[ScheduleRequest],
) -> list[str]:
    """
    Process a schedule request and perform the appropriate action.

    :param client: KeboolaClient instance
    :param target_component_id: The component ID to schedule (e.g., 'keboola.flow')
    :param target_configuration_id: The configuration ID to schedule
    :param request: ScheduleUpdateRequest object
    :param flow_name: Optional name of the flow (used for generating schedule names)
    :return: ScheduleDetail for the created/modified schedule
    """

    _, updated_schedulers, new_schedulers = await _update_schedulers_internal(
        client=client, configuration_id=target_configuration_id, component_id=target_component_id, schedules=requests
    )
    responses: list[str] = []
    try:
        for schedule_id, schedule in updated_schedulers.items():
            if schedule is None:
                # Remove schedule if schedule is None
                await remove_schedule(client=client, schedule_config_id=schedule_id)
                responses.append(f'Removed schedule: {schedule_id}')
            else:
                # Update schedule if schedule is not None
                await update_schedule(
                    client=client,
                    schedule_config_id=schedule_id,
                    cron_tab=schedule.cron_tab,
                    timezone=schedule.timezone,
                    state=schedule.state,
                    change_description='Schedule Updated',
                )
                responses.append(f'Updated schedule: {schedule_id}')
        for new_scheduler in new_schedulers:
            response = await create_schedule(
                client=client,
                target_component_id=target_component_id,
                target_configuration_id=target_configuration_id,
                cron_tab=new_scheduler.cron_tab,
                timezone=new_scheduler.timezone,
                state=new_scheduler.state,
                schedule_name=f'Schedule for {target_configuration_id}',
                schedule_description=f'Automated schedule for {target_configuration_id}',
                target_mode='run',
            )
            responses.append(f'Created schedule: {response.schedule_id}')
    except Exception as e:
        raise ValueError(f'Error processing schedule requests: {str(e)}') from e
    return responses


async def create_schedule(
    client: KeboolaClient,
    target_component_id: str,
    target_configuration_id: str,
    cron_tab: str,
    timezone: str,
    state: str,
    schedule_name: str | None = None,
    schedule_description: str = '',
    target_mode: str = 'run',
    target_tag: str | None = None,
) -> ScheduleDetail:
    """
    Create a scheduler for a component configuration.

    This is a two-step process:
    1. Create a scheduler configuration in Storage API (keboola.scheduler component)
    2. Activate the scheduler in the Scheduler API

    :param client: KeboolaClient instance
    :param target_component_id: The component ID to schedule (e.g., 'keboola.flow')
    :param target_configuration_id: The configuration ID to schedule
    :param schedule: SimplifiedCronSchedule with schedule details
    :param schedule_name: Name for the scheduler configuration (defaults to 'Scheduler for {config_id}')
    :param schedule_description: Description for the scheduler configuration
    :param timezone: Timezone for the scheduler
    :param target_mode: Execution mode (default: 'run')
    :param target_tag: Optional tag for the target configuration
    :return: ScheduleDetail with the activated scheduler details
    """
    if schedule_name is None:
        schedule_name = f'Schedule for {target_configuration_id}'

    # Step 1: Create scheduler configuration in Storage API
    scheduler_config = {
        'schedule': {
            'cronTab': cron_tab,
            'timezone': timezone,
            'state': state,
        },
        'target': {
            'componentId': target_component_id,
            'configurationId': target_configuration_id,
            'mode': target_mode,
        },
    }

    if target_tag:
        scheduler_config['target']['tag'] = target_tag

    # Storage API expects configuration as a dict, will be converted appropriately
    storage_response = CreateConfigurationAPIResponse.model_validate(
        await client.storage_client.configuration_create(
            component_id=SCHEDULER_COMPONENT_ID,
            name=schedule_name,
            description=schedule_description,
            configuration=scheduler_config,
        )
    )
    schedule_config_id = storage_response.id
    LOG.info(f'Created schedule configuration in Storage API: {schedule_config_id}')

    # Step 2: Activate scheduler in Scheduler API
    schedule_response = await client.scheduler_client.activate_schedule(schedule_config_id)
    LOG.info(f'Activated schedule in Scheduler API: {schedule_response.id}')
    await set_cfg_creation_metadata(
        client,
        component_id=SCHEDULER_COMPONENT_ID,
        configuration_id=schedule_config_id,
    )

    return ScheduleDetail.from_api_response(schedule_response)


async def update_schedule(
    client: KeboolaClient,
    schedule_config_id: str,
    cron_tab: str | None,
    timezone: str | None,
    state: str | None,
    scheduler_name: str | None = None,
    scheduler_description: str | None = None,
    change_description: str = 'Scheduler updated',
) -> ScheduleDetail:
    """
    Update an existing scheduler.

    This is a two-step process:
    1. Update the scheduler configuration in Storage API
    2. Reactivate the scheduler in the Scheduler API (posts the updated config)

    :param client: KeboolaClient instance
    :param schedule_config_id: The schedule configuration ID in Storage API
    :param cron_tab: Optional cron tab to update schedule details
    :param timezone: Optional timezone to update schedule details
    :param state: Optional state to update schedule details
    :param scheduler_name: Optional new name for the scheduler
    :param scheduler_description: Optional new description
    :param change_description: Description of the change
    :return: ScheduleDetail with updated scheduler details
    """

    # Get current configuration to merge with updates
    current_config = CreateConfigurationAPIResponse.model_validate(
        await client.storage_client.configuration_detail(
            component_id=SCHEDULER_COMPONENT_ID, configuration_id=schedule_config_id
        )
    )

    current_scheduler_config = current_config.configuration

    if cron_tab is not None:
        current_scheduler_config['schedule']['cronTab'] = cron_tab
    if timezone is not None:
        current_scheduler_config['schedule']['timezone'] = timezone
    if state is not None:
        current_scheduler_config['schedule']['state'] = state

    # Step 1: Update configuration in Storage API
    updated_confg = CreateConfigurationAPIResponse.model_validate(
        await client.storage_client.configuration_update(
            component_id=SCHEDULER_COMPONENT_ID,
            configuration_id=schedule_config_id,
            configuration=current_scheduler_config,
            change_description=change_description,
            updated_name=scheduler_name,
            updated_description=scheduler_description,
        )
    )
    LOG.info(f'Updated schedule configuration in Storage API: {schedule_config_id}')

    # Step 2: Reactivate in Scheduler API to apply changes
    scheduler_response = await client.scheduler_client.activate_schedule(schedule_config_id)
    LOG.info(f'Reactivated scheduler in Scheduler API: {scheduler_response.id}')

    await set_cfg_update_metadata(
        client,
        component_id=SCHEDULER_COMPONENT_ID,
        configuration_id=schedule_config_id,
        configuration_version=updated_confg.version,
    )

    return ScheduleDetail.from_api_response(scheduler_response)


async def remove_schedule(client: KeboolaClient, schedule_config_id: str) -> None:
    """
    Remove a schedule completely.

    This is a two-step process:
    1. Remove the schedule from Scheduler API
    2. Remove the configuration from Storage API

    :param client: KeboolaClient instance
    :param schedule_config_id: The schedule configuration ID in Storage API
    """
    LOG.info(f'Deleting schedule: {schedule_config_id}')

    # Step 1: Delete from Scheduler API
    await client.scheduler_client.delete_schedule(schedule_config_id)
    LOG.info(f'Deleted schedule from Scheduler API: {schedule_config_id}')

    # Step 2: Delete from Storage API
    await client.storage_client.configuration_delete(
        component_id=SCHEDULER_COMPONENT_ID, configuration_id=schedule_config_id
    )
    LOG.info(f'Deleted schedule configuration from Storage API: {schedule_config_id}')


async def list_schedules_for_config(
    client: KeboolaClient, component_id: str, configuration_id: str
) -> list[ScheduleDetail]:
    """
    List all schedules for a configuration.

    :param client: KeboolaClient instance
    :param component_id: The component ID
    :param configuration_id: The configuration ID
    :return: List of Schedules
    """
    schedules_api = await client.scheduler_client.list_schedules_by_config_id(
        component_id=component_id, configuration_id=configuration_id
    )
    return [ScheduleDetail.from_api_response(schedule) for schedule in schedules_api]


async def fetch_schedules_for_flow_summaries(
    client: KeboolaClient, flow_summaries: list[FlowSummary]
) -> list[FlowSummary]:
    """
    Fetch schedules for a list of flow summaries.

    :param client: KeboolaClient instance
    :param flow_summaries: The list of flow summaries to add the schedule to
    :return: The list of flow summaries with the schedules added
    """
    for flow_summary in flow_summaries:
        schedules = await list_schedules_for_config(
            client=client, component_id=flow_summary.component_id, configuration_id=flow_summary.configuration_id
        )
        flow_summary.schedules_count = len(schedules)
    return flow_summaries


async def fetch_schedules_for_flows(
    client: KeboolaClient, links_manager: ProjectLinksManager, list_of_flows: list[Flow]
) -> list[Flow]:
    """
    Fetch schedules for a list of flows.

    :param client: KeboolaClient instance
    :param links_manager: The links manager to use
    :param list_of_flows: The list of flows to fetch the schedules for
    :return: The list of flows with the schedules added
    """
    for flow in list_of_flows:
        schedules = await list_schedules_for_config(
            client=client, component_id=flow.component_id, configuration_id=flow.configuration_id
        )
        link = links_manager.get_scheduler_detail_link(flow.configuration_id, flow.component_id)
        flow.schedules = SchedulesOutput(schedules=schedules, n_schedules=len(schedules), links=[link])
    return list_of_flows
