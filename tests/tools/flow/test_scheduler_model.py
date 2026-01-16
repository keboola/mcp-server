from datetime import datetime

from keboola_mcp_server.clients.scheduler import Schedule, ScheduleApiResponse, TargetConfiguration, TargetExecution
from keboola_mcp_server.tools.flow.scheduler_model import ScheduleDetail


class TestScheduleDetail:
    """Test ScheduleDetail model and from_api_response method."""

    def test_from_api_response_basic(self):
        """Test ScheduleDetail.from_api_response with basic schedule data."""
        schedule = Schedule.model_construct(cron_tab='0 8 * * 1-5', timezone='UTC', state='enabled')
        target = TargetConfiguration.model_construct(
            component_id='keboola.flow', configuration_id='12345', mode='run', tag=None
        )
        schedule_api = ScheduleApiResponse.model_construct(
            id='100',
            token_id='token123',
            configuration_id='config123',
            configuration_version_id='version1',
            schedule=schedule,
            target=target,
            executions=[],
        )

        schedule_detail = ScheduleDetail.from_api_response(schedule_api)

        assert schedule_detail.schedule_id == 'config123'
        assert schedule_detail.timezone == 'UTC'
        assert schedule_detail.state == 'enabled'
        assert schedule_detail.cron_tab == '0 8 * * 1-5'
        assert schedule_detail.target_executions == []

    def test_from_api_response_with_executions(self):
        """Test ScheduleDetail.from_api_response with target executions."""
        schedule = Schedule.model_construct(cron_tab='15,45 1,13 * * 0', timezone='America/New_York', state='enabled')
        target = TargetConfiguration.model_construct(
            component_id='keboola.orchestrator', configuration_id='67890', mode='run', tag='v1.0'
        )
        execution1 = TargetExecution.model_construct(job_id='job123', execution_time=datetime(2024, 1, 15, 10, 30, 0))
        execution2 = TargetExecution.model_construct(job_id='job456', execution_time=datetime(2024, 1, 16, 10, 30, 0))
        schedule_api = ScheduleApiResponse.model_construct(
            id='200',
            token_id='token456',
            configuration_id='config456',
            configuration_version_id='version2',
            schedule=schedule,
            target=target,
            executions=[execution1, execution2],
        )

        schedule_detail = ScheduleDetail.from_api_response(schedule_api)

        assert schedule_detail.schedule_id == 'config456'
        assert schedule_detail.timezone == 'America/New_York'
        assert schedule_detail.state == 'enabled'
        assert schedule_detail.cron_tab == '15,45 1,13 * * 0'
        assert len(schedule_detail.target_executions) == 2
        assert schedule_detail.target_executions[0].job_id == 'job123'
        assert schedule_detail.target_executions[1].job_id == 'job456'

    def test_from_api_response_disabled_schedule(self):
        """Test ScheduleDetail.from_api_response with disabled schedule."""
        schedule = Schedule.model_construct(cron_tab='0 0 * * *', timezone='Europe/Prague', state='disabled')
        target = TargetConfiguration.model_construct(
            component_id='keboola.flow', configuration_id='99999', mode='run', tag=None
        )
        schedule_api = ScheduleApiResponse.model_construct(
            id='300',
            token_id='token789',
            configuration_id='config789',
            configuration_version_id='version3',
            schedule=schedule,
            target=target,
            executions=[],
        )

        schedule_detail = ScheduleDetail.from_api_response(schedule_api)

        assert schedule_detail.schedule_id == 'config789'
        assert schedule_detail.timezone == 'Europe/Prague'
        assert schedule_detail.state == 'disabled'
        assert schedule_detail.cron_tab == '0 0 * * *'
        assert schedule_detail.target_executions == []

    def test_from_api_response_empty_executions(self):
        """Test ScheduleDetail.from_api_response with empty executions list."""
        schedule = Schedule.model_construct(cron_tab='*/30 * * * *', timezone='Asia/Tokyo', state='enabled')
        target = TargetConfiguration.model_construct(
            component_id='keboola.orchestrator', configuration_id='11111', mode='run', tag=None
        )
        schedule_api = ScheduleApiResponse.model_construct(
            id='400',
            token_id='token000',
            configuration_id='config000',
            configuration_version_id='version4',
            schedule=schedule,
            target=target,
            executions=[],
        )

        schedule_detail = ScheduleDetail.from_api_response(schedule_api)

        assert schedule_detail.schedule_id == 'config000'
        assert schedule_detail.timezone == 'Asia/Tokyo'
        assert schedule_detail.state == 'enabled'
        assert schedule_detail.cron_tab == '*/30 * * * *'
        assert isinstance(schedule_detail.target_executions, list)
        assert len(schedule_detail.target_executions) == 0
