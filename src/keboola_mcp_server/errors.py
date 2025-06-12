import logging
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, cast

import httpx

LOG = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])


class KeboolaHTTPException(httpx.HTTPStatusError):
    """Enhanced HTTP exception that includes Keboola API error details for HTTP 500 errors."""
    
    def __init__(self, original_exception: httpx.HTTPStatusError, exception_id: str | None = None, error_details: dict | None = None):
        self.exception_id = exception_id
        self.error_details = error_details or {}
        self.original_exception = original_exception
        
        # Build enhanced error message
        message = self._build_error_message()
        super().__init__(message, request=original_exception.request, response=original_exception.response)
    
    def _build_error_message(self) -> str:
        """Build a comprehensive error message including exceptionId for HTTP 500 errors."""
        base_message = str(self.original_exception)
        
        # Only enhance HTTP 500 errors with exception ID
        if self.original_exception.response.status_code == 500 and self.exception_id:
            base_message += f" (Exception ID: {self.exception_id})"
        
        if self.error_details:
            # Add relevant error details without exposing sensitive information
            if 'message' in self.error_details:
                base_message += f" - {self.error_details['message']}"
        
        return base_message


class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
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
                
                # Special handling for KeboolaHTTPException (HTTP 500 errors only)
                if isinstance(e, KeboolaHTTPException) and e.original_exception.response.status_code == 500:
                    if e.exception_id:
                        recovery_msg = f"{recovery_msg or 'Please try again later.'} For support reference Exception ID: {e.exception_id}"

                if not recovery_msg:
                    raise e

                raise ToolException(e, recovery_msg) from e

        return cast(F, wrapped)

    return decorator
