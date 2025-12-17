"""Scheduler management functions for creating, updating, and deleting schedulers."""

import logging

from pydantic.type_adapter import R

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.scheduler import ScheduleModelApiResponse
from keboola_mcp_server.tools.flow.scheduler_model import SimplifiedCronSchedule, ScheduleRequest

LOG = logging.getLogger(__name__)

SCHEDULER_COMPONENT_ID = 'keboola.scheduler'


async def create_scheduler(
    client: KeboolaClient,
    target_component_id: str,
    target_configuration_id: str,
    schedule: SimplifiedCronSchedule,
    schedule_name: str | None = None,
    schedule_description: str = '',
    target_mode: str = 'run',
    target_tag: str | None = None,
) -> ScheduleModelApiResponse:
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
    :param target_mode: Execution mode (default: 'run')
    :param target_tag: Optional tag for the target configuration
    :return: ScheduleModelApiResponse with the activated scheduler details
    """
    if schedule_name is None:
        schedule_name = f'Schedule for {target_configuration_id}'

    cron_tab = schedule.to_cron_tab()
    LOG.info(f'Creating schedule: {schedule_name} with cron: {cron_tab}')

    # Step 1: Create scheduler configuration in Storage API
    scheduler_config = {
        'schedule': {
            'cronTab': cron_tab,
            'timezone': schedule.timezone,
            'state': 'enabled',
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
    storage_response = await client.storage_client.configuration_create(
        component_id=SCHEDULER_COMPONENT_ID,
        name=schedule_name,
        description=schedule_description,
        configuration=scheduler_config,
    )

    schedule_configuration_id = storage_response['id']
    LOG.info(f'Created schedule configuration in Storage API: {schedule_configuration_id}')

    # Step 2: Activate scheduler in Scheduler API
    schedule_response = await client.scheduler_client.activate_schedule(schedule_configuration_id)
    LOG.info(f'Activated schedule in Scheduler API: {schedule_response.id}')

    return schedule_response


async def update_scheduler(
    client: KeboolaClient,
    schedule_configuration_id: str,
    schedule: SimplifiedCronSchedule | None = None,
    target_component_id: str | None = None,
    target_configuration_id: str | None = None,
    scheduler_name: str | None = None,
    scheduler_description: str | None = None,
    target_mode: str | None = None,
    target_tag: str | None = None,
    state: str = 'enabled',
    change_description: str = 'Scheduler updated',
) -> ScheduleModelApiResponse:
    """
    Update an existing scheduler.

    This is a two-step process:
    1. Update the scheduler configuration in Storage API
    2. Reactivate the scheduler in the Scheduler API (posts the updated config)

    :param client: KeboolaClient instance
    :param schedule_configuration_id: The schedule configuration ID in Storage API
    :param schedule: Optional SimplifiedCronSchedule to update schedule details
    :param target_component_id: Optional component ID to update target
    :param target_configuration_id: Optional configuration ID to update target
    :param scheduler_name: Optional new name for the scheduler
    :param scheduler_description: Optional new description
    :param target_mode: Optional execution mode
    :param target_tag: Optional tag for the target
    :param state: Schedule state ('enabled' or 'disabled')
    :param change_description: Description of the change
    :return: ScheduleModelApiResponse with updated scheduler details
    """
    LOG.info(f'Updating schedule configuration: {schedule_configuration_id}')

    # Get current configuration to merge with updates
    current_config = await client.storage_client.configuration_detail(
        component_id=SCHEDULER_COMPONENT_ID, configuration_id=schedule_configuration_id
    )

    current_scheduler_config = current_config['configuration']

    # Update schedule if provided
    if schedule is not None:
        current_scheduler_config['schedule'] = {
            'cronTab': schedule.to_cron_tab(),
            'timezone': schedule.timezone,
            'state': state,
        }
    else:
        # Just update the state if no new schedule provided
        if 'schedule' in current_scheduler_config:
            current_scheduler_config['schedule']['state'] = state

    # Update target if provided
    if target_component_id is not None:
        current_scheduler_config['target']['componentId'] = target_component_id
    if target_configuration_id is not None:
        current_scheduler_config['target']['configurationId'] = target_configuration_id
    if target_mode is not None:
        current_scheduler_config['target']['mode'] = target_mode
    if target_tag is not None:
        current_scheduler_config['target']['tag'] = target_tag

    # Step 1: Update configuration in Storage API
    await client.storage_client.configuration_update(
        component_id=SCHEDULER_COMPONENT_ID,
        configuration_id=schedule_configuration_id,
        configuration=current_scheduler_config,
        change_description=change_description,
        updated_name=scheduler_name,
        updated_description=scheduler_description,
    )

    LOG.info(f'Updated schedule configuration in Storage API: {schedule_configuration_id}')

    # Step 2: Reactivate in Scheduler API to apply changes
    scheduler_response = await client.scheduler_client.activate_schedule(schedule_configuration_id)
    LOG.info(f'Reactivated scheduler in Scheduler API: {scheduler_response.id}')

    return scheduler_response


async def delete_schedule(client: KeboolaClient, schedule_configuration_id: str) -> None:
    """
    Delete a schedule completely.

    This is a two-step process:
    1. Delete the schedule from Scheduler API
    2. Delete the configuration from Storage API

    :param client: KeboolaClient instance
    :param schedule_configuration_id: The schedule configuration ID in Storage API
    """
    LOG.info(f'Deleting schedule: {schedule_configuration_id}')

    # Step 1: Delete from Scheduler API
    await client.scheduler_client.delete_schedule(schedule_configuration_id)
    LOG.info(f'Deleted schedule from Scheduler API: {schedule_configuration_id}')

    # Step 2: Delete from Storage API
    await client.storage_client.configuration_delete(
        component_id=SCHEDULER_COMPONENT_ID, configuration_id=schedule_configuration_id
    )
    LOG.info(f'Deleted schedule configuration from Storage API: {schedule_configuration_id}')


async def enable_schedule(client: KeboolaClient, schedule_configuration_id: str) -> ScheduleModelApiResponse:
    """
    Enable a disabled schedule.

    :param client: KeboolaClient instance
    :param schedule_configuration_id: The schedule configuration ID in Storage API
    :return: ScheduleModelApiResponse with updated scheduler details
    """
    LOG.info(f'Enabling schedule: {schedule_configuration_id}')
    return await update_scheduler(
        client=client,
        schedule_configuration_id=schedule_configuration_id,
        state='enabled',
        change_description='Schedule enabled',
    )


async def disable_schedule(client: KeboolaClient, schedule_configuration_id: str) -> ScheduleModelApiResponse:
    """
    Disable an active schedule.

    :param client: KeboolaClient instance
    :param schedule_configuration_id: The schedule configuration ID in Storage API
    :return: ScheduleModelApiResponse with updated scheduler details
    """
    LOG.info(f'Disabling schedule: {schedule_configuration_id}')
    return await update_scheduler(
        client=client,
        schedule_configuration_id=schedule_configuration_id,
        state='disabled',
        change_description='Schedule disabled',
    )


async def list_schedules_for_config(
    client: KeboolaClient, component_id: str, configuration_id: str
) -> list[ScheduleModelApiResponse]:
    """
    List all schedules for a configuration.

    :param client: KeboolaClient instance
    :param component_id: The component ID
    :param configuration_id: The configuration ID
    :return: List of ScheduleModelApiResponse
    """
    return await client.scheduler_client.list_schedules_by_config_id(
        component_id=component_id, configuration_id=configuration_id
    )


async def process_schedule_request(
    client: KeboolaClient,
    target_component_id: str,
    target_configuration_id: str,
    request: ScheduleRequest,
) -> ScheduleModelApiResponse:
    """
    Process a schedule request and perform the appropriate action.

    :param client: KeboolaClient instance
    :param target_component_id: The component ID to schedule (e.g., 'keboola.flow')
    :param target_configuration_id: The configuration ID to schedule
    :param request: ScheduleUpdateRequest object
    :param flow_name: Optional name of the flow (used for generating schedule names)
    :return: ScheduleModelApiResponse for the created/modified schedule
    """

    action = request.action
    schedule_id = request.schedule_id
    schedule = request.schedule

    if action == 'create':
        schedule_name = f'Schedule for {target_configuration_id}'
        response = await create_scheduler(
            client=client,
            target_component_id=target_component_id,
            target_configuration_id=target_configuration_id,
            schedule=schedule,
            schedule_name=schedule_name,
            schedule_description=f'Automated schedule for {target_configuration_id}',
            target_mode='run',
        )
        return response
    elif action == 'modify':
        return await update_scheduler(
            client=client,
            schedule_configuration_id=schedule_id,
            schedule=schedule,
            state='enabled',
            change_description='Schedule modified',
        )
    elif action == 'enable':
        return await enable_schedule(client=client, schedule_configuration_id=schedule_id)
    elif action == 'disable':
        return await disable_schedule(client=client, schedule_configuration_id=schedule_id)
    elif action == 'delete':
        return await delete_schedule(client=client, schedule_configuration_id=schedule_id)
    else:
        raise ValueError(f'Unknown action: {action}')
