import inspect
import json
import logging
import re
import time
from collections.abc import Callable, Mapping
from functools import wraps
from http import HTTPStatus
from typing import Any, TypeVar, cast

import httpx
import jsonschema
import yaml
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.exceptions import ValidationError as FastMCPValidationError
from fastmcp.server import middleware as fmw
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.utilities.types import find_kwarg_by_type
from mcp import types as mt
from pydantic import BaseModel, ValidationError
from pydantic_core import ErrorDetails

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.encryption import REDACTED_SECRET_VALUE, SECRET_KEY_PREFIX, is_encrypted_value
from keboola_mcp_server.clients.storage import StorageEventType
from keboola_mcp_server.config import deployed_sa_token_path
from keboola_mcp_server.mcp import CONVERSATION_ID, ServerState, get_http_request_or_none

LOG = logging.getLogger(__name__)
F = TypeVar('F', bound=Callable[..., Any])

_USER_AGENT_TO_COMPONENT_ID: Mapping[str, str] = {
    'read-only-chat': 'keboola.ai-chat',
    'kai-assistant': 'keboola.kai-assistant',
}


MAX_ARG_VALUE_LEN = 10_000  # Maximum length (bytes) of a single tool argument value in the Storage Events payload.

# 4xx statuses that signal throttling/timeouts of the upstream service rather than a caller mistake.
_UPSTREAM_4XX = (HTTPStatus.REQUEST_TIMEOUT, HTTPStatus.TOO_MANY_REQUESTS)


def _is_caller_error(exc: BaseException) -> bool:
    """
    True if the tool failed because of the caller's input/credentials rather than a server-side fault.

    A pydantic ``ValidationError`` raised inside a tool is deliberately NOT a caller error: the tool's
    arguments are validated before ``tool_errors`` runs, so it means our own models rejected data.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return 400 <= status < 500 and status not in _UPSTREAM_4XX
    return isinstance(exc, jsonschema.ValidationError)


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

    @classmethod
    def encode_truncated(cls, obj: Any) -> str:
        """Encode obj to JSON, replacing the value with a truncation notice if it exceeds MAX_ARG_VALUE_LEN."""
        encoded = cls.encode(obj)
        # Measure the size of the value as it will appear in the final JSON payload,
        # i.e. once it is JSON-encoded again as a string value.
        payload_encoded = json.dumps(encoded, ensure_ascii=False)
        encoded_bytes = len(payload_encoded.encode('utf-8'))
        if encoded_bytes <= MAX_ARG_VALUE_LEN:
            return encoded
        return json.dumps(f'[value truncated, original length: {encoded_bytes} bytes]')


# Keys of path-based update operations (e.g. `update_config`'s `parameter_updates`) that carry the value
# written to the `path`. They are masked when the `path` points at a secret ('#'-prefixed) key.
_PATH_UPDATE_VALUE_KEYS = frozenset({'value', 'search_for', 'replace_with'})
# A '#' at the start of a path segment: `#password`, `db.#token`, `db['#token']`, `db["#token"]`.
_SECRET_PATH_SEGMENT = re.compile(r'''(?:^|[.\[]\s*['"]?)#''')
# `#key: value`, `#key=value`, `'#key': 'value'` fragments as rendered by str(), repr() or YAML.
_SECRET_KEY_FRAGMENT = re.compile(r'''(['"]?#[^\s'":=,{}\[\]]+['"]?\s*[:=]\s*)('[^']*'|"[^"]*"|[^\s,}\]]+)''')
# Pydantic's str(ValidationError) echoes the invalid input: `[type=..., input_value=..., input_type=...]`.
_PYDANTIC_INPUT_VALUE = re.compile(r'input_value=.*?(?=, input_type=|\]$)', re.MULTILINE)
_MAX_CAUSE_DEPTH = 5
# Shorter secret values are not replaced everywhere in exception text, as that would corrupt unrelated
# substrings such as HTTP status codes or IDs; the `#key: value` fragment masking still covers them.
_MIN_REPLACED_SECRET_LEN = 6
# jsonschema validators whose `validator_value` comes from the schema and is safe to report.
_REPORTED_SCHEMA_VALIDATORS = frozenset({'required', 'enum', 'type'})


def _is_secret_path(path: Any) -> bool:
    return isinstance(path, str) and bool(_SECRET_PATH_SEGMENT.search(path))


def _collect_leaves(value: Any, found: set[str]) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _collect_leaves(item, found)
    elif isinstance(value, list):
        for item in value:
            _collect_leaves(item, found)
    elif isinstance(value, str) and len(value) >= _MIN_REPLACED_SECRET_LEN:
        found.add(value)


def _mask_secret_fragments(text: str, found: set[str]) -> str:
    def mask(match: re.Match) -> str:
        _collect_leaves(match.group(2).strip('\'"'), found)
        return f'{match.group(1)}{REDACTED_SECRET_VALUE}'

    return _SECRET_KEY_FRAGMENT.sub(mask, text)


def _redact_value(value: Any, found: set[str]) -> Any:
    """
    Returns a redacted copy of a JSON-compatible value and adds every masked plaintext value to `found`.

    Applies the same rule as `redact_secrets` (plaintext values of '#' keys are masked, 'KBC::' ciphers kept),
    and additionally masks the value of path-based updates targeting a '#' key and descends into strings
    holding a JSON object or array. Other strings (Python literals, invalid or code-fenced JSON, YAML,
    .env text) get their `#key: value` / `#key=value` fragments masked.
    """
    if isinstance(value, dict):
        secret_path = _is_secret_path(value.get('path'))
        redacted: dict[Any, Any] = {}
        for key, item in value.items():
            is_secret_key = isinstance(key, str) and key.startswith(SECRET_KEY_PREFIX)
            if (is_secret_key or (secret_path and key in _PATH_UPDATE_VALUE_KEYS)) and not is_encrypted_value(item):
                _collect_leaves(item, found)
                redacted[key] = REDACTED_SECRET_VALUE
            else:
                redacted[key] = _redact_value(item, found)
        return redacted
    if isinstance(value, list):
        return [_redact_value(item, found) for item in value]
    if isinstance(value, str):
        if value.lstrip()[:1] in ('{', '['):
            try:
                parsed = json.loads(value)
            except ValueError:
                parsed = None
            if isinstance(parsed, (dict, list)):
                return json.dumps(_redact_value(parsed, found), ensure_ascii=False)
        return _mask_secret_fragments(value, found)
    return value


def _redact_arguments(arguments: Mapping[str, Any]) -> tuple[list[dict[str, str]], set[str]]:
    """
    Serializes tool arguments for the Storage event with secrets masked.

    Pydantic models are dumped to plain JSON data first, so the actual arguments are never mutated.
    If redaction fails, only the argument names are reported, never their raw values.
    """
    found: set[str] = set()
    try:
        encoded = [
            {
                'key': name,
                'value': _JsonWrapper.encode_truncated(
                    _redact_value(_JsonWrapper(data=value).model_dump(mode='json')['data'], found)
                ),
            }
            for name, value in arguments.items()
        ]
    except Exception as e:
        LOG.warning(f'Failed to redact tool arguments, reporting argument names only: {type(e).__name__}')
        return [{'key': name, 'value': json.dumps('[redaction failed]')} for name in arguments], found
    return encoded, found


def _find_cause(exception: BaseException, exc_type: type[BaseException] | tuple) -> BaseException | None:
    current: BaseException | None = exception
    for _ in range(_MAX_CAUSE_DEPTH):
        if current is None:
            return None
        if isinstance(current, exc_type):
            return current
        current = current.__cause__ or current.__context__
    return None


def _safe_exception_summary(exception: BaseException, secrets: set[str]) -> str:
    """
    Describes a failed tool call without echoing the tool's input.

    Validation errors are summarized from their structured details (location, message, type) only,
    because their text echoes the invalid input (pydantic `input_value`, jsonschema instance).
    In all cases the plaintext secret values found in the arguments, `#key: value` fragments
    and pydantic `input_value`s are then masked in the resulting text.
    """
    if pydantic_error := _find_cause(exception, ValidationError):
        pydantic_error = cast(ValidationError, pydantic_error)
        errors = '; '.join(
            f'{err.get("msg")} ({err.get("type")}) at {".".join(str(loc) for loc in err.get("loc", ()))}'
            for err in pydantic_error.errors(include_url=False, include_input=False, include_context=False)
        )
        text = f'{pydantic_error.title} validation failed: {errors}'
    elif schema_error := _find_cause(exception, jsonschema.ValidationError):
        schema_error = cast(jsonschema.ValidationError, schema_error)
        text = f'Schema validation failed at {schema_error.json_path}: "{schema_error.validator}" constraint violated'
        if schema_error.validator in _REPORTED_SCHEMA_VALIDATORS:
            text += f' (schema: {json.dumps(schema_error.validator_value, ensure_ascii=False, default=str)})'
    else:
        text = str(exception)

    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, REDACTED_SECRET_VALUE)
    text = _PYDANTIC_INPUT_VALUE.sub(f'input_value={REDACTED_SECRET_VALUE}', text)
    return _mask_secret_fragments(text, set())


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

    runtime_info = ServerState.from_context(ctx).runtime_info

    user_agent: str | None = None
    if client_params := ctx.session.client_params:
        user_agent = f'{client_params.client_info.name}/{client_params.client_info.version}'
    if not user_agent:
        user_agent = ctx.client_id
    if not user_agent and (http_rq := get_http_request_or_none()):
        user_agent = http_rq.headers.get('User-Agent')
    if not user_agent:
        user_agent = ''

    arguments, secrets = _redact_arguments(
        {
            param_name: param_value
            for param_name, param_value in bound_args.arguments.items()
            if param_name not in [ctx_param_name, 'self', 'cls']
        }
    )

    # See # https://github.com/keboola/event-schema/blob/main/schema/ext.keboola.mcp-server-tool.json
    # for the JSON schema describing the 'keboola.mcp-server-tool' component's event params.
    event_params: dict[str, Any] = {
        'mcpServerContext': {
            'appEnv': runtime_info.app_version,
            'version': runtime_info.server_version,
            'userAgent': user_agent,
            # For the HTTP-based transports use the HTTP session ID. For other transports use the server ID.
            'sessionId': ctx.session_id or runtime_info.server_id,
            'serverTransport': runtime_info.transport.split('/')[-1],
            'conversationId': ctx.session.state.get(CONVERSATION_ID) or '',
        },
        'tool': {
            'name': tool_name,
            'arguments': arguments,
        },
    }
    if exception:
        summary = _safe_exception_summary(exception, secrets)
        message = f'MCP tool "{tool_name}" call failed. {type(exception).__name__}: {summary}'
        event_type: StorageEventType = 'error'
    else:
        message = f'MCP tool "{tool_name}" call succeeded.'
        event_type: StorageEventType = 'success'

    if KeboolaClient.STATE_KEY not in ctx.session.state:
        # A bootstrap session (agent_provisioning RFC) has no credential and no project, so there
        # is nowhere to write a Storage event to -- `create_project` is the only tool that runs in
        # one. Skipping quietly beats the caller-visible warning a raised `from_state` would log on
        # every such call; the provisioning itself is still attributed, by the `clientId` the tool
        # sends to Connection (auditLog.agentProvisioning.projectProvisioned).
        LOG.debug(f'No Keboola client in the session state; skipping the event for the "{tool_name}" tool.')
        return

    client = KeboolaClient.from_state(ctx.session.state)
    # Emitting a storage event is a write; a read-only user's own token is denied (403).
    # On the Keboola-deployed server (KBC_KUBERNETES_TOKEN_PATH set) send the SA JWT as the
    # step-up header so Connection waives the missing permission on the event-create action
    # (storage:events:create). Read from the process env only — never from Config/HTTP — and
    # fall back to the user's own client when not deployed/configured. `step_up_storage_client()`
    # attaches the JWT only when the session talks to this server's own stack and falls back to
    # the user's own client otherwise — this code runs in a `finally:` block that swallows its
    # errors, so it must not depend on anything failing loudly.
    storage_client = client.storage_client
    if kubernetes_token_path := deployed_sa_token_path():
        storage_client = client.step_up_storage_client(kubernetes_token_path)
    resp = await storage_client.trigger_event(
        message=message,
        component_id=_USER_AGENT_TO_COMPONENT_ID.get(user_agent.split(sep='/')[0]) or 'keboola.mcp-server-tool',
        event_type=event_type,
        params=event_params,
        duration=execution_time,
    )
    LOG.debug(f'Tool call SAPI event triggered: {resp}')


def tool_errors(
    default_recovery: str | None = None,
    recovery_instructions: dict[type[Exception], str] | None = None,
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
                recovery_msg = default_recovery
                if recovery_instructions:
                    for exc_type, msg in recovery_instructions.items():
                        if isinstance(e, exc_type):
                            recovery_msg = msg
                            break

                error_msg: str | None = None

                if isinstance(e, ValidationError):
                    error_msg = prettify_validation_error(e)
                    if recovery_msg:
                        error_msg += f'\nRecovery: {recovery_msg}'
                elif isinstance(e, jsonschema.ValidationError):
                    error_msg = str(e)
                    if recovery_msg:
                        error_msg += f'\nRecovery: {recovery_msg}'
                elif recovery_msg:
                    error_msg = f'{e}\nRecovery: {recovery_msg}'

                caller_fault = _is_caller_error(e)
                extra = {
                    'tool_name': func.__name__,
                    'error_type': type(e).__name__,
                    'error_category': 'client' if caller_fault else 'internal',
                }
                try:
                    if error_msg:
                        raise ToolError(error_msg) from e
                    else:
                        raise
                except Exception as e:
                    if caller_fault:
                        # Routine (expired token, missing object, bad input): no traceback, not an ERROR.
                        LOG.warning(f'MCP tool "{func.__name__}" call failed: {str(e).splitlines()[0]}', extra=extra)
                    else:
                        LOG.exception(f'MCP tool "{func.__name__}" call failed.', extra=extra)
                    exception = e
                    raise

            finally:
                try:
                    await _trigger_event(func, args, kwargs, exception, time.perf_counter() - start)
                except Exception as e:
                    # Event logging is best-effort telemetry — never fail the tool because of it.
                    # The tool result (success or failure) is already determined before this point.
                    LOG.warning(f'Failed to trigger tool event for "{func.__name__}" tool: {e}', exc_info=True)

        return cast(F, wrapped)

    return decorator


def _format_validation_errors(errors: list[ErrorDetails]) -> dict[str, Any]:
    """
    Formats Pydantic validation errors into a structured dictionary.

    :param errors: List of error dictionaries from ValidationError.errors()
    :return: Dictionary with formatted errors including field, message, and extra fields
    """
    formatted_errors: list[dict[str, Any]] = []
    for error in errors:
        error_dict: dict[str, Any] = {
            'field': '.'.join(str(i) for i in error.get('loc', [])),
            'message': error.get('msg', 'Validation error'),
            'extra': {str(key): str(value) for key, value in error.items() if key not in {'loc', 'msg'}},
        }
        formatted_errors.append(error_dict)
    return {'errors': formatted_errors}


def prettify_validation_error(error: ValidationError) -> str:
    """
    Formats a Pydantic ValidationError into a human and LLM-readable YAML string.

    :param error: The Pydantic ValidationError to format
    :return: A formatted YAML string with error details
    """
    error_count = len(error.errors())
    model_name = getattr(error, 'title', 'unknown')
    header = f'Found {error_count} validation error(s) for {model_name}'
    formatted = _format_validation_errors(error.errors())
    try:
        yaml_str = yaml.dump(formatted, default_flow_style=False, sort_keys=False, allow_unicode=True)
    except Exception:
        yaml_str = str(formatted)

    return f'{header}\n{yaml_str}'


class ValidationErrorMiddleware(fmw.Middleware):
    """
    Middleware that catches Pydantic ValidationError and formats it with explicit field locations.

    This middleware intercepts tool calls and catches any Pydantic ValidationError that occurs
    during argument validation. It then formats the error message to clearly show which fields
    are missing or invalid, making it easier for both humans and LLMs to understand the issue.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        await self._coerce_empty_string_arrays(context)
        try:
            return await call_next(context)
        except ValidationError as e:
            raise ToolError(prettify_validation_error(e)) from e
        except FastMCPValidationError as e:
            # fastmcp wraps a pydantic ValidationError raised during argument validation
            # (a bad call) in its own ValidationError, keeping the original as __cause__.
            if isinstance(e.__cause__, ValidationError):
                raise ToolError(prettify_validation_error(e.__cause__)) from e
            raise

    @staticmethod
    async def _coerce_empty_string_arrays(context: MiddlewareContext[mt.CallToolRequestParams]) -> None:
        """Some tool-calling clients send `""` instead of omitting/`[]` for an unset array-typed
        argument (observed live: `get_configs` called with `component_types=""`,
        `component_ids=""`), which pydantic-core always rejects for a `Sequence[...]` field
        ("'str' instances are not allowed as a Sequence value") -- a plain string is never a valid
        value for an argument the tool's own schema declares as `array`, so there's no legitimate
        case this coercion could clobber. Mutates ``context.message.arguments`` in place before
        ``call_next`` so the tool never sees the invalid input at all, rather than just prettifying
        the resulting error.
        """
        ctx = context.fastmcp_context
        args = getattr(context.message, 'arguments', None)
        if ctx is None or not isinstance(args, dict):
            return
        try:
            tool = await ctx.fastmcp.get_tool(context.message.name)
        except Exception:
            return
        props = (tool.parameters or {}).get('properties', {}) if tool else {}
        for key, value in args.items():
            if value == '' and props.get(key, {}).get('type') == 'array':
                args[key] = []
