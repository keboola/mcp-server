import logging
import re
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, cast

import httpx

LOG = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])



class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
        self.recovery_message = recovery_instruction
        super().__init__(f'{str(original_exception)} | Recovery: {recovery_instruction}')


def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
    """
    Enhanced MCP tool function decorator with improved HTTP 500 error handling.
    
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

                # Enhanced recovery message for HTTP 500 errors
                recovery_msg = default_recovery
                if recovery_instructions:
                    for exc_type, msg in recovery_instructions.items():
                        if isinstance(e, exc_type):
                            recovery_msg = msg
                            break
                
                raise ToolException(e, recovery_msg) from e

        return cast(F, wrapped)

    return decorator
