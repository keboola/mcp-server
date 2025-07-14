import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, cast

from fastmcp import Context
from fastmcp.utilities.types import find_kwarg_by_type

from keboola_mcp_server.client import KeboolaClient, TriggerEventRequest

LOG = logging.getLogger(__name__)
F = TypeVar('F', bound=Callable[..., Any])


class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
        super().__init__(f'{str(original_exception)} | Recovery: {recovery_instruction}')


def _extract_session_id_from_context(ctx: Context) -> str:
    """
    Extract sessionId from the Context object.

    The sessionId can be found in various places in the ctx object depending on the MCP implementation.
    This function tries different possible locations where sessionId might be stored.
    """
    # Try to get sessionId from ctx.session.id if available
    if hasattr(ctx.session, 'id') and ctx.session.id:
        return str(ctx.session.id)

    # Try to get sessionId from ctx.request_context if available
    if hasattr(ctx, 'request_context') and ctx.request_context:
        if hasattr(ctx.request_context, 'session_id') and ctx.request_context.session_id:
            return str(ctx.request_context.session_id)

        # Check if sessionId is in the request scope
        if hasattr(ctx.request_context, 'request') and ctx.request_context.request:
            request = ctx.request_context.request
            if hasattr(request, 'scope') and 'session_id' in request.scope:
                return str(request.scope['session_id'])

    # Fallback to a default session ID if none found
    # In a real implementation, you might want to generate a unique ID or handle this differently
    return 'unknown-session'


def _extract_tool_name_and_args(func: Callable, args: tuple, kwargs: dict) -> tuple[str, dict[str, Any]]:
    """
    Extract tool name and arguments from the function call.

    :param func: The decorated function
    :param args: Positional arguments
    :param kwargs: Keyword arguments
    :return: Tuple of (tool_name, tool_args)
    """
    tool_name = func.__name__

    # Convert args and kwargs to a single arguments dict
    import inspect
    sig = inspect.signature(func)
    bound_args = sig.bind(*args, **kwargs)
    bound_args.apply_defaults()

    # Filter out the Context parameter as it's not part of the tool arguments
    tool_args = {}
    for param_name, param_value in bound_args.arguments.items():
        if param_name != 'ctx' and not isinstance(param_value, Context):
            tool_args[param_name] = param_value

    return tool_name, tool_args


def _extract_additional_context_from_args(tool_args: dict[str, Any], tool_name: str) -> dict[str, Any]:
    """
    Extract additional context values from tool arguments that are used in event logging.

    :param tool_args: The tool arguments dictionary
    :param tool_name: The name of the tool that was executed
    :return: Dictionary with additional context values (config_id, job_id)
    """
    additional_context = {}

    # Extract config_id from various possible parameter names
    config_id = tool_args.get('config_id') or tool_args.get('configuration_id')
    if config_id:
        additional_context['config_id'] = config_id

    # Extract job_id from arguments for get_job tool
    if tool_name == 'get_job':
        job_id = tool_args.get('job_id')
        if job_id:
            additional_context['job_id'] = str(job_id)

    return additional_context


def _extract_configuration_id_from_result(result: Any) -> Optional[str]:
    """
    Extract configuration_id from function result if it's a ConfigToolOutput.

    :param result: The result from the function execution
    :return: configuration_id if found, None otherwise
    """
    try:
        # Check if result has configuration_id attribute (ConfigToolOutput)
        if hasattr(result, 'configuration_id'):
            return str(result.configuration_id)

        # Check if result is a dict with configuration_id
        if isinstance(result, dict) and 'configuration_id' in result:
            return str(result['configuration_id'])

    except Exception:
        # If any error occurs during extraction, return None
        pass

    return None


def _extract_job_id_from_result(result: Any, tool_name: str) -> Optional[str]:
    """
    Extract job_id from function result if it's a JobDetail and tool is run_job.

    :param result: The result from the function execution
    :param tool_name: The name of the tool that was executed
    :return: job_id if found and tool is run_job, None otherwise
    """
    # Only extract job_id for run_job tool
    if tool_name != 'run_job':
        return None

    try:
        # Check if result has id attribute (JobDetail)
        if hasattr(result, 'id'):
            return str(result.id)

        # Check if result is a dict with id
        if isinstance(result, dict) and 'id' in result:
            return str(result['id'])

    except Exception:
        # If any error occurs during extraction, return None
        pass

    return None


def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
    """
    The MCP tool function decorator that logs exceptions and adds recovery instructions for LLMs.

    This decorator now automatically:
    - Extracts sessionId from the Context object
    - Uses RawStorageClient.trigger_event for error logging
    - Removes the need for manual mcp_context construction in tools
    - Maintains existing recovery instruction functionality
    - Captures function results to extract additional context (e.g., configuration_id from
      ConfigToolOutput, job_id from run_job result and get_job arguments)

    :param default_recovery: A fallback recovery instruction to use when no specific instruction
                             is found for the exception.
    :param recovery_instructions: A dictionary mapping exception types to recovery instructions.
    :return: The decorated function with error-handling logic applied.
    """

    def decorator(func: Callable):

        @wraps(func)
        async def wrapped(*args, **kwargs):
            start_time = time.monotonic()
            result = None

            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                duration_s = time.monotonic() - start_time
                logging.exception(f'Failed to run tool {func.__name__}: {e}')

                # Extract sessionId and tool information
                ctx_kwarg = find_kwarg_by_type(func, Context)
                session_id = 'unknown-session'
                tool_name, tool_args = _extract_tool_name_and_args(func, args, kwargs)

                if ctx_kwarg:
                    # Convert positional args to kwargs to find ctx
                    import inspect
                    sig = inspect.signature(func)
                    bound_args = sig.bind(*args, **kwargs)
                    ctx = bound_args.arguments.get(ctx_kwarg)

                    if isinstance(ctx, Context):
                        session_id = _extract_session_id_from_context(ctx)

                        # Try to get RawStorageClient for event logging
                        try:
                            if hasattr(ctx, 'session') and hasattr(ctx.session, 'state'):
                                client = KeboolaClient.from_state(ctx.session.state)
                                raw_client = client.storage_client.raw_client

                                additional_context = _extract_additional_context_from_args(
                                    tool_args, tool_name
                                )
                                # Extract configuration_id and job_id from result if available
                                if result is not None:
                                    result_config_id = _extract_configuration_id_from_result(result)
                                    if result_config_id:
                                        additional_context['config_id'] = result_config_id

                                    result_job_id = _extract_job_id_from_result(result, tool_name)
                                    if result_job_id:
                                        additional_context['job_id'] = result_job_id

                                # Construct event for event logging
                                event = TriggerEventRequest(
                                    tool_name=tool_name,
                                    tool_args=tool_args,
                                    session_id=session_id,
                                    config_id=additional_context.get('config_id'),
                                    job_id=additional_context.get('job_id'),
                                )

                                # Send error event to Storage API
                                await raw_client.trigger_event(
                                    error_obj=e,
                                    duration_s=duration_s,
                                    event=event,
                                )
                        except Exception as event_error:
                            # Log but don't fail the main error handling if event sending fails
                            LOG.warning(f'Failed to send error event: {event_error}')

                # Handle recovery instructions
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
