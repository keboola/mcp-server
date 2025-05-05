import logging
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, cast

LOG = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])


class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
        super().__init__(f'{str(original_exception)} | Recovery: {recovery_instruction}')


def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
    """
    The MCP tool function decorator that logs exceptions and adds recovery instructions for LLMs.

    :param default_recovery: A fallback recovery instruction to use when no specific instruction
                             is found for the exception.
    :param recovery_instructions: A dictionary mapping exception types to recovery instructions.
    :return: The decorated function with error-handling logic applied.
    """

    def decorator(func: Callable):

        @wraps(func)
        async def wrapped(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logging.exception(f'Failed to run tool {func.__name__}: {e}')

                recovery_msg = default_recovery
                if recovery_instructions:
                    for exc_type, msg in recovery_instructions.items():
                        if isinstance(e, exc_type):
                            recovery_msg = msg
                            break

                if not recovery_msg:
                    raise e

                raise ToolException(e, recovery_msg) from e

        return cast(F, wrapped)

    return decorator
