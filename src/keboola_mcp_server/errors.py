import inspect
import logging
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, cast

from fastmcp import Context
from fastmcp.utilities.types import find_kwarg_by_type

from keboola_mcp_server.client import KeboolaClient, TriggerEventRequest
from keboola_mcp_server.mcp import ServerState

LOG = logging.getLogger(__name__)
F = TypeVar('F', bound=Callable[..., Any])


class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
        super().__init__(f'{str(original_exception)} | Recovery: {recovery_instruction}')


@dataclass
class TriggerEventContext:
    """
    Holds all context information needed for event logging and error handling.
    """
    session_id: str
    tool_name: str
    tool_args: dict[str, Any]
    additional_context: dict[str, Any] = field(default_factory=dict)


def _get_session_id(ctx: Context) -> str:
    """
    Gets session ID from the Context object. For the HTTP-based transports this is the HTTP session ID.
    For other transports, this is the server ID.
    """
    if ctx.session_id:
        return ctx.session_id
    else:
        server_state = ctx.request_context.lifespan_context
        assert isinstance(server_state, ServerState), 'ServerState is not available in the context.'
        return server_state.server_id


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
    sig = inspect.signature(func)
    bound_args = sig.bind(*args, **kwargs)
    bound_args.apply_defaults()

    # Filter out the Context parameter as it's not part of the tool arguments
    tool_args = {}
    for param_name, param_value in bound_args.arguments.items():
        if param_name != 'ctx' and not isinstance(param_value, Context):
            tool_args[param_name] = param_value

    return tool_name, tool_args


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


def _extract_trigger_event_context(
    func: Callable,
    args: tuple,
    kwargs: dict,
    result: Any = None,
) -> TriggerEventContext:
    """
    Extracts all relevant context for event logging and error handling in one place.
    Returns a TriggerEventContext object.
    """
    # Extract tool name and arguments
    tool_name, tool_args = _extract_tool_name_and_args(func, args, kwargs)

    # Try to extract ctx (Context) from args/kwargs
    ctx_kwarg = find_kwarg_by_type(func, Context)
    session_id = 'unknown-session'
    ctx = None
    if ctx_kwarg:
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        ctx = bound_args.arguments.get(ctx_kwarg)
        if isinstance(ctx, Context):
            session_id = _get_session_id(ctx)

    # Build additional_context dict
    additional_context = {}
    # config_id from tool_args or result
    config_id = tool_args.get('config_id') or tool_args.get('configuration_id')
    if not config_id and result is not None:
        config_id = _extract_configuration_id_from_result(result)
    if config_id:
        additional_context['config_id'] = config_id
    # job_id from tool_args (for get_job) or result (for run_job)
    if tool_name == 'get_job':
        job_id = tool_args.get('job_id')
        if job_id:
            additional_context['job_id'] = str(job_id)
    elif tool_name == 'run_job' and result is not None:
        job_id = _extract_job_id_from_result(result, tool_name)
        if job_id:
            additional_context['job_id'] = job_id

    return TriggerEventContext(
        session_id=session_id,
        tool_name=tool_name,
        tool_args=tool_args,
        additional_context=additional_context,
    )


def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
    """
    The MCP tool function decorator that logs exceptions and adds recovery instructions for LLMs.

    This decorator now uses a single context extraction function to gather all relevant event context.
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

                # Extract all event context in one place
                event_ctx = _extract_trigger_event_context(func, args, kwargs, result)

                # Try to get RawStorageClient for event logging if possible
                ctx_kwarg = find_kwarg_by_type(func, Context)
                ctx = None
                if ctx_kwarg:
                    sig = inspect.signature(func)
                    bound_args = sig.bind(*args, **kwargs)
                    ctx = bound_args.arguments.get(ctx_kwarg)

                if isinstance(ctx, Context):
                    try:
                        if hasattr(ctx, 'session') and hasattr(ctx.session, 'state'):
                            client = KeboolaClient.from_state(ctx.session.state)
                            raw_client = client.storage_client.raw_client

                            # Construct event for event logging
                            event = TriggerEventRequest(
                                tool_name=event_ctx.tool_name,
                                tool_args=event_ctx.tool_args,
                                session_id=event_ctx.session_id,
                                config_id=event_ctx.additional_context.get('config_id'),
                                job_id=event_ctx.additional_context.get('job_id'),
                            )

                            # Send error event to Storage API
                            await raw_client.trigger_event(
                                error_obj=e,
                                duration_s=duration_s,
                                event=event,
                            )
                    except Exception as event_error:
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
