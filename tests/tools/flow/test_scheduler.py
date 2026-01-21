import re

import pytest

from keboola_mcp_server.tools.flow.scheduler import validate_cron_tab


class TestValidateCronTab:
    """Test validate_cron_tab function."""

    @pytest.mark.parametrize(
        'cron_tab',
        [
            pytest.param(None, id='none_value'),
            pytest.param('0 1,13 * * *', id='daily_at_1am_and_1pm'),
            pytest.param('0 9 * * 1', id='weekly_monday_9am'),
            pytest.param('0 10 1,20 * *', id='monthly_1st_and_20th_10am'),
            pytest.param('0 11 1 1,8 *', id='yearly_jan_aug_1st_11am'),
            pytest.param('0,15,30,45 * * * *', id='hourly_every_15_minutes'),
            pytest.param('30 14 * * *', id='daily_2_30pm'),
            pytest.param('0 0 * * *', id='midnight_daily'),
            pytest.param('59 23 * * *', id='max_values'),
        ],
    )
    def test_valid_cron_tab(self, cron_tab: str | None):
        """Test valid cron tab expressions."""
        result = validate_cron_tab(cron_tab)
        if cron_tab is None:
            assert result is None
        else:
            assert result is None  # Function returns None on success

    @pytest.mark.parametrize(
        ('cron_tab', 'error_match'),
        [
            pytest.param('', 'Cron expression must have exactly 5 parts', id='empty_string'),
            pytest.param('0 8 * *', 'Cron expression must have exactly 5 parts', id='too_few_parts'),
            pytest.param('0 8 * * * *', 'Cron expression must have exactly 5 parts', id='too_many_parts'),
            pytest.param('0 8 * *', 'Cron expression must have exactly 5 parts', id='four_parts'),
            pytest.param('0 8 * * * * *', 'Cron expression must have exactly 5 parts', id='seven_parts'),
            pytest.param('60 8 * * *', 'Minutes of hour.*must be between 0 and 59', id='minutes_too_high'),
            pytest.param('-1 8 * * *', 'Minutes of hour.*must be between 0 and 59', id='minutes_negative'),
            pytest.param('abc 8 * * *', 'Cron expression must have only digits', id='minutes_non_digit'),
            pytest.param('0 24 * * *', 'Hours of day.*must be between 0 and 23', id='hours_too_high'),
            pytest.param('0 -1 * * *', 'Hours of day.*must be between 0 and 23', id='hours_negative'),
            pytest.param('0 abc * * *', 'Cron expression must have only digits', id='hours_non_digit'),
            pytest.param('0 8 0 * *', 'Days of month.*must be between 1 and 31', id='days_zero'),
            pytest.param('0 8 32 * *', 'Days of month.*must be between 1 and 31', id='days_too_high'),
            pytest.param('0 8 -1 * *', 'Days of month.*must be between 1 and 31', id='days_negative'),
            pytest.param('0 8 abc * *', 'Cron expression must have only digits', id='days_non_digit'),
            pytest.param('0 8 * 0 *', 'Months of year.*must be between 1 and 12', id='months_zero'),
            pytest.param('0 8 * 13 *', 'Months of year.*must be between 1 and 12', id='months_too_high'),
            pytest.param('0 8 * -1 *', 'Months of year.*must be between 1 and 12', id='months_negative'),
            pytest.param('0 8 * abc *', 'Cron expression must have only digits', id='months_non_digit'),
            pytest.param('0 8 * * 7', 'Days of week.*must be between 0=Sunday and 6=Saturday', id='weekdays_too_high'),
            pytest.param('0 8 * * -1', 'Days of week.*must be between 0=Sunday and 6=Saturday', id='weekdays_negative'),
            pytest.param('0 8 * * abc', 'Cron expression must have only digits', id='weekdays_non_digit'),
            pytest.param('* 1,3 * *', 'Cron expression must have exactly 5 parts', id='missing_weekday'),
            pytest.param(
                '0 8 * 1,3 *', 'Months of year must be specified with days of month', id='months_without_days'
            ),
            pytest.param('0 * 1,3 * *', 'Days of month must be specified with hours of day', id='days_without_hours'),
            pytest.param(
                '* 8 * * *', 'Hours of day must be specified with minutes of hour', id='hours_without_minutes'
            ),
            pytest.param('* * * * 0', 'Days of week must be specified with hours of day', id='weekdays_without_hours'),
            pytest.param('0 8 1 * 0', 'Days of week must not be specified with days of month', id='weekdays_with_days'),
            pytest.param(
                '0 8 1,3 1,3 0', 'Days of week must not be specified with days of month', id='weekdays_with_both'
            ),
        ],
    )
    def test_invalid_cron_tab(self, cron_tab: str, error_match: str):
        """Test invalid cron tab expressions."""
        with pytest.raises(ValueError, match='Invalid cron tab expression') as exc_info:
            validate_cron_tab(cron_tab)
        error_message = str(exc_info.value)
        # Check that the error message starts with "Invalid cron tab expression: "
        assert error_message.startswith('Invalid cron tab expression: ')
        # Check that the error message contains the specific error (using regex if needed)
        assert re.search(error_match, error_message) is not None, f'Error message does not contain: {error_match}'
        # Check that the error message includes the instructions
        assert 'Cron Tab Expression should be in the format: `* * * * *`' in error_message
        assert 'Field order:' in error_message
        assert '1. Minute (0-59)' in error_message
        assert '2. Hour (0-23)' in error_message
        assert '3. Day of month (1-31)' in error_message
        assert '4. Month (1-12)' in error_message
        assert '5. Day of week (0-6, where 0 = Sunday)' in error_message

    def test_cron_tab_with_whitespace(self):
        """Test that cron tab handles whitespace correctly."""
        # Should work with extra whitespace
        validate_cron_tab('  0  8  *  *  *  ')
        validate_cron_tab('0 8 * * *')
        # Should fail with wrong number of parts after stripping
        with pytest.raises(ValueError, match='Invalid cron tab expression') as exc_info:
            validate_cron_tab('  0  8  *  *  ')
        error_message = str(exc_info.value)
        assert 'Cron expression must have exactly 5 parts' in error_message
        assert 'Cron Tab Expression should be in the format: `* * * * *`' in error_message
