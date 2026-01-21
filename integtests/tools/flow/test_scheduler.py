import logging
from typing import Any, cast

import pytest
from fastmcp import Client

from integtests.conftest import ConfigDef
from keboola_mcp_server.clients.client import ORCHESTRATOR_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.tools.flow.model import GetFlowsDetailOutput
from keboola_mcp_server.tools.flow.scheduler import (
    SCHEDULER_COMPONENT_ID,
    create_schedule,
    list_schedules_for_config,
    remove_schedule,
    update_schedule,
)
from keboola_mcp_server.tools.flow.scheduler_model import ScheduleDetail
from keboola_mcp_server.tools.flow.tools import FlowToolOutput

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_scheduler_lifecycle(mcp_context, configs, keboola_client) -> None:
    """
    Test complete scheduler lifecycle: create, retrieve, update, and delete.

    :param mcp_context: The test context fixture.
    :param configs: List of real configuration definitions.
    :param keboola_client: KeboolaClient instance.
    """
    token_info = await keboola_client.storage_client.verify_token()
    admin_data = token_info.get('admin', {})
    token_role = admin_data.get('role') if isinstance(admin_data, dict) else None
    if token_role != 'admin':
        pytest.skip('Scheduler tooling requires an admin token, skipping test.')

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
    scheduler_id: str | None = None
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
        scheduler_id = created_schedule.schedule_id
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
    finally:
        if scheduler_id:
            try:
                await remove_schedule(client=keboola_client, schedule_config_id=scheduler_id)
                await remove_schedule(client=keboola_client, schedule_config_id=scheduler_id)
                await keboola_client.storage_client.configuration_delete(
                    component_id=SCHEDULER_COMPONENT_ID, configuration_id=scheduler_id, skip_trash=True
                )
            except Exception:
                LOG.info('Schedule cleanup error; schedule already removed.')


@pytest.mark.asyncio
async def test_scheduler_lifecycle_tooling(
    initial_lf: FlowToolOutput, mcp_client: Client, configs: list[ConfigDef], keboola_client: KeboolaClient
) -> None:
    """
    Test scheduler lifecycle using MCP tools: create schedule for a flow, update, and remove it.
    """
    token_info = await keboola_client.storage_client.verify_token()
    token_role = (token_info.get('admin', {}) or {}).get('role')
    if token_role != 'admin':
        pytest.skip('Scheduler tooling requires an admin token, skipping test.')

    assert configs
    assert configs[0].configuration_id is not None

    flow_id = initial_lf.configuration_id

    schedule_id: str | None = None
    try:
        initial_cron_tab = '0 8 * * *'
        initial_timezone = 'UTC'

        add_result = await mcp_client.call_tool(
            name='modify_flow',
            arguments={
                'configuration_id': flow_id,
                'flow_type': ORCHESTRATOR_COMPONENT_ID,
                'change_description': 'Add scheduler via tooling',
                'schedules': [
                    {
                        'action': 'add',
                        'cron_tab': initial_cron_tab,
                        'timezone': initial_timezone,
                        'state': 'enabled',
                    }
                ],
            },
        )
        add_output = FlowToolOutput.model_validate(add_result.structured_content)
        assert add_output.success is True

        tool_call_result = await mcp_client.call_tool(name='get_flows', arguments={'flow_ids': [flow_id]})
        struct_call_result = cast(dict[str, Any], tool_call_result.structured_content)
        flow_detail_result = GetFlowsDetailOutput.model_validate(struct_call_result['result'])
        flow_detail = flow_detail_result.flows[0]
        schedule = flow_detail.schedules.schedules[0]
        schedule_id = schedule.schedule_id

        assert flow_detail.schedules is not None
        assert flow_detail.schedules.n_schedules == 1
        assert schedule.cron_tab == initial_cron_tab
        assert schedule.timezone == initial_timezone
        assert schedule.state == 'enabled'

        updated_cron_tab = '0 12 * * *'
        updated_timezone = 'America/New_York'

        update_result = await mcp_client.call_tool(
            name='modify_flow',
            arguments={
                'configuration_id': flow_id,
                'flow_type': ORCHESTRATOR_COMPONENT_ID,
                'change_description': 'Update scheduler via tooling',
                'schedules': [
                    {
                        'action': 'update',
                        'schedule_id': schedule_id,
                        'cron_tab': updated_cron_tab,
                        'timezone': updated_timezone,
                        'state': 'disabled',
                    }
                ],
            },
        )
        update_output = FlowToolOutput.model_validate(update_result.structured_content)
        assert update_output.success is True

        tool_call_result = await mcp_client.call_tool(name='get_flows', arguments={'flow_ids': [flow_id]})
        struct_call_result = cast(dict[str, Any], tool_call_result.structured_content)
        flow_detail_result = GetFlowsDetailOutput.model_validate(struct_call_result['result'])
        flow_detail = flow_detail_result.flows[0]

        assert flow_detail.schedules is not None
        assert flow_detail.schedules.n_schedules == 1
        schedule = flow_detail.schedules.schedules[0]
        assert schedule.schedule_id == schedule_id
        assert schedule.cron_tab == updated_cron_tab
        assert schedule.timezone == updated_timezone
        assert schedule.state == 'disabled'

        remove_result = await mcp_client.call_tool(
            name='modify_flow',
            arguments={
                'configuration_id': flow_id,
                'flow_type': ORCHESTRATOR_COMPONENT_ID,
                'change_description': 'Remove scheduler via tooling',
                'schedules': [{'action': 'remove', 'schedule_id': schedule_id}],
            },
        )
        remove_output = FlowToolOutput.model_validate(remove_result.structured_content)
        assert remove_output.success is True

        tool_call_result = await mcp_client.call_tool(name='get_flows', arguments={'flow_ids': [flow_id]})
        struct_call_result = cast(dict[str, Any], tool_call_result.structured_content)
        flow_detail_result = GetFlowsDetailOutput.model_validate(struct_call_result['result'])
        flow_detail = flow_detail_result.flows[0]

        assert flow_detail.schedules is not None
        assert flow_detail.schedules.n_schedules == 0
        assert flow_detail.schedules.schedules == []
    finally:
        if schedule_id:
            try:
                await remove_schedule(client=keboola_client, schedule_config_id=schedule_id)
                await remove_schedule(client=keboola_client, schedule_config_id=schedule_id)
                await keboola_client.storage_client.configuration_delete(
                    component_id=SCHEDULER_COMPONENT_ID, configuration_id=schedule_id, skip_trash=True
                )
            except Exception:
                LOG.info('Schedule cleanup error; schedule already removed.')
