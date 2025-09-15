import inspect
import json
import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, cast

from fastmcp import Context
from fastmcp.utilities.types import find_kwarg_by_type
from pydantic import BaseModel

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.mcp import ServerState, get_http_request_or_none

LOG = logging.getLogger(__name__)
F = TypeVar('F', bound=Callable[..., Any])


class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
        super().__init__(f'{str(original_exception)} | Recovery: {recovery_instruction}')


class _JsonWrapper(BaseModel):
    """
    Utility class for safely encoding arbitrary Python objects to JSON strings.

    Uses Pydantic's serialization to handle complex objects as well as simple types like int, float, bool, str, etc.
    Primary use case is serializing tool function parameters for Keboola Storage API events.
    """

    data: Any  # The arbitrary object to be JSON serialized

    @classmethod
    def encode(cls, obj: Any) -> str:
        return json.dumps(cls(data=obj).model_dump()['data'], ensure_ascii=False)


async def _trigger_event(
    func: Callable, args: tuple, kwargs: dict, exception: Exception | None, execution_time: float
) -> None:
    # TODO: This is not always correct. In general tool functions can be registered
    #  in the MCP server under different names.
    tool_name = func.__name__

    sig = inspect.signature(func)
    bound_args = sig.bind(*args, **kwargs)
    bound_args.apply_defaults()

    ctx_param_name = find_kwarg_by_type(func, Context)
    assert ctx_param_name, f'The tool function {tool_name} must have a "Context" parameter.'

    ctx = bound_args.arguments.get(ctx_param_name)
    assert isinstance(ctx, Context), (
        f'The tool function {tool_name} has invalid "{ctx_param_name}" parameter. '
        f'Expecting instance of "Context", got {type(ctx)}.'
    )

    server_state = ServerState.from_context(ctx)
    user_agent: str | None = None
    if client_params := ctx.session.client_params:
        user_agent = f'{client_params.clientInfo.name}/{client_params.clientInfo.version}'
    if not user_agent:
        user_agent = ctx.client_id
    if not user_agent:
        if http_rq := get_http_request_or_none():
            user_agent = http_rq.headers.get('User-Agent')

    # See # https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.mcp-server-tool.json
    # for the JSON schema describing the 'keboola.mcp-server-tool' component's event params.
    event_params: dict[str, Any] = {
        'mcpServerContext': {
            'appEnv': server_state.runtime_info.app_version,
            'version': server_state.runtime_info.server_version,
            'userAgent': user_agent or '',
            # For the HTTP-based transports use the HTTP session ID. For other transports use the server ID.
            'sessionId': ctx.session_id or server_state.runtime_info.server_id,
            'serverTransport': server_state.runtime_info.transport.split('/')[-1],
        },
        'tool': {
            'name': tool_name,
            'arguments': [
                {'key': param_name, 'value': _JsonWrapper.encode(param_value)}
                for param_name, param_value in bound_args.arguments.items()
                if param_name not in [ctx_param_name, 'self', 'cls']
            ],
        },
    }
    if exception:
        message = f'MCP tool "{tool_name}" call failed. {type(exception).__name__}: {exception}'
        event_type = 'error'
    else:
        message = f'MCP tool "{tool_name}" call succeeded.'
        event_type = 'success'

    client = KeboolaClient.from_state(ctx.session.state)
    resp = await client.storage_client.trigger_event(
        message=message,
        component_id='keboola.mcp-server-tool',
        event_type=event_type,
        params=event_params,
        duration=execution_time,
    )
    LOG.debug(f'Tool call SAPI event triggered: {resp}')


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
            exception: Exception | None = None
            start = time.perf_counter()

            try:
                return await func(*args, **kwargs)
            except Exception as e:
                LOG.exception(f'Failed to run tool {func.__name__}: {e}')

                recovery_msg = default_recovery
                if recovery_instructions:
                    for exc_type, msg in recovery_instructions.items():
                        if isinstance(e, exc_type):
                            recovery_msg = msg
                            break

                try:
                    if not recovery_msg:
                        raise e

                    raise ToolException(e, recovery_msg) from e
                except Exception as e:
                    exception = e
                    raise

            finally:
                try:
                    await _trigger_event(func, args, kwargs, exception, time.perf_counter() - start)
                except Exception as e:
                    LOG.exception(f'Failed to trigger tool event for {func.__name__}: {e}')
                    raise

        return cast(F, wrapped)

    return decorator
