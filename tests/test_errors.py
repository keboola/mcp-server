import logging

import pytest

from keboola_mcp_server.errors import ToolException, tool_errors


# --- Fixtures ---
@pytest.fixture
def function_with_value_error():
    """A function that raises ValueError."""

    def func():
        raise ValueError("Simulated ValueError")

    return func


# --- Test Cases ---


# --- Test tool_errors ---
@pytest.mark.parametrize(
    "function_fixture, default_recovery, recovery_instructions, expected_recovery_message, exception_message",
    [
        # Case with both default_recovery and recovery_instructions specified
        (
            "function_with_value_error",
            "General recovery message.",
            {ValueError: "Check that data has valid types."},
            "Check that data has valid types.",
            "Simulated ValueError",
        ),
        # Case where only default_recovery is provided
        (
            "function_with_value_error",
            "General recovery message.",
            {},
            "General recovery message.",
            "Simulated ValueError",
        ),
        # Case with only recovery_instructions provided
        (
            "function_with_value_error",
            None,
            {ValueError: "Check that data has valid types."},
            "Check that data has valid types.",
            "Simulated ValueError",
        ),
        # Case with no recovery instructions provided
        (
            "function_with_value_error",
            None,
            {},
            "No recovery instructions available.",
            "Simulated ValueError",
        ),
    ],
)
def test_tool_function_recovery_instructions(
    function_fixture,
    default_recovery,
    recovery_instructions,
    expected_recovery_message,
    exception_message,
    request,
):
    """
    Test that the appropriate recovery message is applied based on the exception type.
    Verifies that the tool_errors decorator handles various combinations of recovery parameters.
    """
    tool_func = request.getfixturevalue(function_fixture)
    decorated_func = tool_errors(
        default_recovery=default_recovery, recovery_instructions=recovery_instructions
    )(tool_func)

    with pytest.raises(ToolException) as excinfo:
        decorated_func()

    assert isinstance(excinfo.value, ToolException)
    assert exception_message in str(excinfo.value)
    assert expected_recovery_message in str(excinfo.value.recovery_instruction)


# --- Test Logging ---
def test_logging_on_tool_exception(caplog, function_with_value_error):
    """Test if logging works correctly with the tool function."""
    decorated_func = tool_errors(default_recovery="General recovery message.")(
        function_with_value_error
    )

    with caplog.at_level(logging.ERROR):
        try:
            decorated_func()
        except ToolException:
            pass

    # Capture and assert the correct logging output
    assert "failed to run tool" in caplog.text.lower()
    assert "simulated valueerror" in caplog.text.lower()
    assert "raise valueerror" in caplog.text.lower()
