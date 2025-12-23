import logging

import pytest

from keboola_mcp_server.tools.flow.scheduler import (
    create_schedule,
    list_schedules_for_config,
    remove_schedule,
    update_schedule,
)
from keboola_mcp_server.tools.flow.scheduler_model import ScheduleDetail

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_scheduler_lifecycle(mcp_context, configs, keboola_client) -> None:
    """
    Test complete scheduler lifecycle: create, retrieve, update, and delete.

    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    :param keboola_client: KeboolaClient instance.
    """
    assert configs
    assert configs[0].configuration_id is not None

    # Use the first config as our target for scheduling
    target_component_id = configs[0].component_id
    target_configuration_id = configs[0].configuration_id

    # Initial schedule parameters
    initial_cron_tab = '0 8 * * *'  # Daily at 8 AM
    initial_timezone = 'UTC'
    initial_state = 'enabled'
    schedule_name = 'Integration Test Schedule'
    schedule_description = 'Schedule created by integration test'

    created_schedule: ScheduleDetail | None = None

    try:
        # Step 1: Create a schedule
        LOG.info(f'Creating schedule for {target_component_id}/{target_configuration_id}')
        created_schedule = await create_schedule(
            client=keboola_client,
            target_component_id=target_component_id,
            target_configuration_id=target_configuration_id,
            cron_tab=initial_cron_tab,
            timezone=initial_timezone,
            state=initial_state,
            schedule_name=schedule_name,
            schedule_description=schedule_description,
        )

        assert isinstance(created_schedule, ScheduleDetail)
        assert created_schedule.schedule_id is not None
        assert created_schedule.cron_tab == initial_cron_tab
        assert created_schedule.timezone == initial_timezone
        assert created_schedule.state == initial_state
        LOG.info(f'Created schedule with ID: {created_schedule.schedule_id}')

        # Step 2: Retrieve the schedule using list_schedules_for_config
        LOG.info('Retrieving schedules for configuration')
        schedules = await list_schedules_for_config(
            client=keboola_client,
            component_id=target_component_id,
            configuration_id=target_configuration_id,
        )

        assert len(schedules) >= 1, 'At least one schedule should exist'
        found_schedule = next((s for s in schedules if s.schedule_id == created_schedule.schedule_id), None)
        assert found_schedule is not None, 'Created schedule should be in the list'
        assert found_schedule.cron_tab == initial_cron_tab
        assert found_schedule.timezone == initial_timezone
        assert found_schedule.state == initial_state

        # Step 3: Update the schedule
        updated_cron_tab = '0 12 * * *'  # Daily at 12 PM
        updated_timezone = 'America/New_York'
        updated_state = 'disabled'

        LOG.info(f'Updating schedule {created_schedule.schedule_id}')
        updated_schedule = await update_schedule(
            client=keboola_client,
            schedule_config_id=created_schedule.schedule_id,
            cron_tab=updated_cron_tab,
            timezone=updated_timezone,
            state=updated_state,
            change_description='Integration test update',
        )

        assert isinstance(updated_schedule, ScheduleDetail)
        assert updated_schedule.schedule_id == created_schedule.schedule_id
        assert updated_schedule.cron_tab == updated_cron_tab
        assert updated_schedule.timezone == updated_timezone
        assert updated_schedule.state == updated_state

        # Step 4: Retrieve the schedule again to verify the update
        LOG.info('Retrieving schedules after update')
        schedules_after_update = await list_schedules_for_config(
            client=keboola_client,
            component_id=target_component_id,
            configuration_id=target_configuration_id,
        )

        found_updated_schedule = next(
            (s for s in schedules_after_update if s.schedule_id == created_schedule.schedule_id), None
        )
        assert found_updated_schedule is not None
        assert found_updated_schedule.cron_tab == updated_cron_tab
        assert found_updated_schedule.timezone == updated_timezone
        assert found_updated_schedule.state == updated_state

        # Step 5: Delete the schedule
        LOG.info(f'Deleting schedule {created_schedule.schedule_id}')
        await remove_schedule(
            client=keboola_client,
            schedule_config_id=created_schedule.schedule_id,
        )

        # Step 6: Verify the schedule is deleted
        LOG.info('Verifying schedule deletion')
        schedules_after_delete = await list_schedules_for_config(
            client=keboola_client,
            component_id=target_component_id,
            configuration_id=target_configuration_id,
        )

        deleted_schedule_exists = any(s.schedule_id == created_schedule.schedule_id for s in schedules_after_delete)
        assert not deleted_schedule_exists, 'Schedule should be deleted'

        LOG.info('Scheduler lifecycle test completed successfully')

    except Exception as e:
        LOG.error(f'Error during scheduler lifecycle test: {e}')
        # Clean up if schedule was created
        if created_schedule is not None:
            try:
                await remove_schedule(
                    client=keboola_client,
                    schedule_config_id=created_schedule.schedule_id,
                )
                LOG.info(f'Cleaned up schedule {created_schedule.schedule_id}')
            except Exception as cleanup_error:
                LOG.warning(f'Failed to clean up schedule: {cleanup_error}')
        raise
