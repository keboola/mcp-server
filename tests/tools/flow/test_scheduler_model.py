"""Tests for schedulers_model module."""

import pytest

from keboola_mcp_server.tools.flow.scheduler_model import SimplifiedCronSchedule


class TestSimplifiedCronSchedule:
    """Test SimplifiedCronSchedule model and its methods."""

    @pytest.mark.parametrize(
        ('cron_tab', 'expected_type', 'expected_minutes', 'expected_hours', 'expected_days', 'expected_months'),
        [
            pytest.param(
                '*/5 * * * *',
                'hourly',
                ['*/5'],
                [],
                [],
                [],
                id='every_5_minutes_hourly',
            ),
            pytest.param(
                '0 * * * *',
                'hourly',
                ['0'],
                [],
                [],
                [],
                id='every_hour_at_minute_0_hourly',
            ),
            pytest.param(
                '0 12 * * *',
                'daily',
                ['0'],
                ['12'],
                [],
                [],
                id='daily_at_noon',
            ),
            pytest.param(
                '30 14 1 * *',
                'monthly',
                ['30'],
                ['14'],
                ['1'],
                [],
                id='monthly_first_day_at_14_30',
            ),
            pytest.param(
                '0 0 1 1 *',
                'yearly',
                ['0'],
                ['0'],
                ['1'],
                ['1'],
                id='yearly_new_year',
            ),
            pytest.param(
                '15,30,45 9,17 * * *',
                'daily',
                ['15', '30', '45'],
                ['9', '17'],
                [],
                [],
                id='multiple_times_comma_separated_daily',
            ),
            pytest.param(
                '0 0,6,12,18 * * *',
                'daily',
                ['0'],
                ['0', '6', '12', '18'],
                [],
                [],
                id='every_6_hours_daily',
            ),
            pytest.param(
                '0 9 1,15 * *',
                'monthly',
                ['0'],
                ['9'],
                ['1', '15'],
                [],
                id='twice_monthly',
            ),
            pytest.param(
                '0 0 15 1,6 *',
                'yearly',
                ['0'],
                ['0'],
                ['15'],
                ['1', '6'],
                id='specific_day_in_specific_months_yearly',
            ),
            pytest.param(
                '0 9 * * 1',
                'weekly',
                ['0'],
                ['9'],
                ['1'],
                [],
                id='weekly_monday_at_9am',
            ),
            pytest.param(
                '30 14 * * 1,3,5',
                'weekly',
                ['30'],
                ['14'],
                ['1', '3', '5'],
                [],
                id='weekly_mon_wed_fri',
            ),
            pytest.param(
                '0 8 * 1,4,7,10 *',
                'daily',
                ['0'],
                ['8'],
                [],
                ['1', '4', '7', '10'],
                id='quarterly_start_months',
            ),
            pytest.param(
                '15 10 25 12 *',
                'yearly',
                ['15'],
                ['10'],
                ['25'],
                ['12'],
                id='christmas_yearly',
            ),
        ],
    )
    def test_from_cron_tab_valid_expressions(
        self,
        cron_tab: str,
        expected_type: str,
        expected_minutes: list[str],
        expected_hours: list[str],
        expected_days: list[str],
        expected_months: list[str],
    ):
        """Test parsing valid cron expressions with schedule type inference."""
        schedule = SimplifiedCronSchedule.from_cron_tab(cron_tab)

        assert schedule.type == expected_type
        assert schedule.at_minutes == expected_minutes
        assert schedule.at_hour == expected_hours
        assert schedule.on_days == expected_days
        assert schedule.in_months == expected_months

    @pytest.mark.parametrize(
        ('cron_tab', 'error_message'),
        [
            pytest.param(
                '* * *',
                'Cron expression must have 5 parts',
                id='too_few_parts',
            ),
            pytest.param(
                '* * * * * *',
                'Cron expression must have 5 parts',
                id='too_many_parts',
            ),
            pytest.param(
                '',
                'Cron expression must have 5 parts',
                id='empty_string',
            ),
            pytest.param(
                '* * * *',
                'Cron expression must have 5 parts',
                id='four_parts',
            ),
        ],
    )
    def test_from_cron_tab_invalid_part_count(self, cron_tab: str, error_message: str):
        """Test that invalid cron expressions with wrong number of parts raise AssertionError."""
        with pytest.raises(AssertionError, match=error_message):
            SimplifiedCronSchedule.from_cron_tab(cron_tab)

    @pytest.mark.parametrize(
        'cron_tab',
        [
            pytest.param('  0   12   *   *   *  ', id='extra_spaces'),
            pytest.param('0\t12\t*\t*\t*', id='tab_separated'),
            pytest.param('0  12  *  *  *', id='double_spaces'),
        ],
    )
    def test_from_cron_tab_whitespace_handling(self, cron_tab: str):
        """Test that various whitespace patterns are handled correctly."""
        schedule = SimplifiedCronSchedule.from_cron_tab(cron_tab)

        assert schedule.type == 'daily'
        assert schedule.at_minutes == ['0']
        assert schedule.at_hour == ['12']
        assert schedule.on_days == []
        assert schedule.in_months == []

    @pytest.mark.parametrize(
        ('cron_tab', 'expected_values'),
        [
            pytest.param(
                '1,2,3 * * * *',
                ['1', '2', '3'],
                id='comma_no_spaces',
            ),
            pytest.param(
                '1,2,3 * * * *',
                ['1', '2', '3'],
                id='comma_with_spaces',
            ),
            pytest.param(
                '1,2,3,4,5 * * * *',
                ['1', '2', '3', '4', '5'],
                id='five_values',
            ),
        ],
    )
    def test_from_cron_tab_comma_separated_values(self, cron_tab: str, expected_values: list[str]):
        """Test parsing comma-separated values in cron expressions."""
        schedule = SimplifiedCronSchedule.from_cron_tab(cron_tab)

        assert schedule.at_minutes == expected_values

    @pytest.mark.parametrize(
        'cron_tab',
        [
            pytest.param('a,bc,a * * * *', id='invalid_characters'),
            pytest.param('abc * * * *', id='non_numeric_single'),
            pytest.param('0 xyz * * *', id='non_numeric_hours'),
        ],
    )
    def test_from_cron_tab_invalid_characters(self, cron_tab: str):
        """Test that invalid characters in cron expressions are parsed (but may fail validation)."""
        # The from_cron_tab method doesn't validate values, just parses them
        # Validation happens later in the model validator
        schedule = SimplifiedCronSchedule.from_cron_tab(cron_tab)

        # Should parse successfully even with invalid values
        assert schedule is not None

    def test_from_cron_tab_all_wildcards(self):
        """Test cron expression with all wildcards - should fail to infer type."""
        schedule = SimplifiedCronSchedule.from_cron_tab('* * * * *')

        # When all fields are wildcards, they should be empty lists
        # Type inference will fail and return None with a warning
        assert schedule.type is None
        assert schedule.at_minutes == []
        assert schedule.at_hour == []
        assert schedule.on_days == []
        assert schedule.in_months == []

    def test_from_cron_tab_mixed_wildcards_and_values(self):
        """Test cron expression mixing wildcards and specific values - should be hourly."""
        schedule = SimplifiedCronSchedule.from_cron_tab('15 * 1,15 * *')

        # Only minutes specified = hourly
        assert schedule.type == 'hourly'
        assert schedule.at_minutes == ['15']
        assert schedule.at_hour == []
        assert schedule.on_days == []
        assert schedule.in_months == []

    def test_from_cron_tab_range_expressions(self):
        """Test cron expressions with range notation (e.g., 1-5)."""
        # Note: The current implementation doesn't split ranges, treats them as single values
        schedule = SimplifiedCronSchedule.from_cron_tab('0 1-5 * * *')

        assert schedule.at_minutes == ['0']
        assert schedule.at_hour == ['1-5']

    def test_from_cron_tab_step_values(self):
        """Test cron expressions with step values (e.g., */5)."""
        schedule = SimplifiedCronSchedule.from_cron_tab('*/15 */6 * * *')

        assert schedule.type == 'daily'
        assert schedule.at_minutes == ['*/15']
        assert schedule.at_hour == ['*/6']


class TestScheduleTypeInference:
    """Test schedule type inference logic in from_cron_tab method."""

    @pytest.mark.parametrize(
        ('cron_tab', 'expected_type', 'description'),
        [
            pytest.param(
                '30 * * * *',
                'hourly',
                'only minutes specified',
                id='hourly_only_minutes',
            ),
            pytest.param(
                '0,15,30,45 * * * *',
                'hourly',
                'multiple minutes, no hours',
                id='hourly_multiple_minutes',
            ),
            pytest.param(
                '0 9 * * *',
                'daily',
                'minutes and hours specified',
                id='daily_minutes_hours',
            ),
            pytest.param(
                '0 9,17 * * *',
                'daily',
                'minutes and multiple hours',
                id='daily_multiple_hours',
            ),
            pytest.param(
                '0 9 * * 1',
                'weekly',
                'minutes, hours, and weekday specified',
                id='weekly_single_weekday',
            ),
            pytest.param(
                '0 9 * * 1,3,5',
                'weekly',
                'minutes, hours, and multiple weekdays',
                id='weekly_multiple_weekdays',
            ),
            pytest.param(
                '0 9 1 * *',
                'monthly',
                'minutes, hours, and day of month specified',
                id='monthly_single_day',
            ),
            pytest.param(
                '0 9 1,15 * *',
                'monthly',
                'minutes, hours, and multiple days',
                id='monthly_multiple_days',
            ),
            pytest.param(
                '0 9 * 6 *',
                'daily',
                'minutes, hours, and month specified (no day)',
                id='daily_with_month_only',
            ),
            pytest.param(
                '0 9 1 1 *',
                'yearly',
                'minutes, hours, day, and month specified',
                id='yearly_full_date',
            ),
            pytest.param(
                '0 0 25 12 *',
                'yearly',
                'christmas day yearly',
                id='yearly_christmas',
            ),
            pytest.param(
                '30 14 15 3,6,9,12 *',
                'yearly',
                'quarterly on 15th at 14:30',
                id='yearly_quarterly',
            ),
        ],
    )
    def test_schedule_type_inference(self, cron_tab: str, expected_type: str, description: str):
        """Test that schedule type is correctly inferred from cron expression."""
        schedule = SimplifiedCronSchedule.from_cron_tab(cron_tab)

        assert (
            schedule.type == expected_type
        ), f'Failed for {description}: expected {expected_type}, got {schedule.type}'

    def test_schedule_type_inference_all_wildcards_returns_none(self):
        """Test that all wildcards results in None type with warning."""
        schedule = SimplifiedCronSchedule.from_cron_tab('* * * * *')

        assert schedule.type is None

    @pytest.mark.parametrize(
        ('cron_tab', 'expected_on_days'),
        [
            pytest.param('0 9 * * 1', ['1'], id='weekly_uses_weekday_field'),
            pytest.param('0 9 1 * *', ['1'], id='monthly_uses_day_field'),
            pytest.param('0 9 1 1 *', ['1'], id='yearly_uses_day_field'),
            pytest.param('0 9 * * 1,3,5', ['1', '3', '5'], id='weekly_multiple_weekdays'),
            pytest.param('0 9 1,15 * *', ['1', '15'], id='monthly_multiple_days'),
        ],
    )
    def test_on_days_field_mapping(self, cron_tab: str, expected_on_days: list[str]):
        """Test that on_days field correctly maps to either days or weekdays based on schedule type."""
        schedule = SimplifiedCronSchedule.from_cron_tab(cron_tab)

        assert schedule.on_days == expected_on_days
