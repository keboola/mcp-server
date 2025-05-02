import logging
from typing import Any, Callable, Dict, Optional, Type, TypeVar, cast

LOG = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
        super().__init__(f"{str(original_exception)} | Recovery: {recovery_instruction}")


def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[Dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
    """
    The MCP tool function decorator that logs exceptions and adds recovery instructions for LLMs.

    Args:
        default_recovery (Optional[str]): A fallback recovery instruction for any exception.
        recovery_instructions (Optional[Dict[Type[Exception], str]]): Specific instructions per exception type.

    Returns:
        Callable: Decorated function with error handling.
    """

    def decorator(func: Callable):
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                recovery_msg = default_recovery

                if recovery_instructions:
                    for exc_type, msg in recovery_instructions.items():
                        if isinstance(e, exc_type):
                            recovery_msg = msg
                            break

                if not recovery_msg:
                    recovery_msg = "No recovery instructions available."

                logging.exception(f"Failed to run tool {func.__name__}: {e}")
                raise ToolException(e, recovery_msg) from e

        return cast(F, wrapped)

    return decorator
