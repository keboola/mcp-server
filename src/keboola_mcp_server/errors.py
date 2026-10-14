import ast
import inspect
import json
import logging
import re
import time
import traceback
from collections.abc import Callable, Mapping
from functools import wraps
from http import HTTPStatus
from typing import Any, NamedTuple, TypeVar, cast

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
from keboola_mcp_server.clients.encryption import REDACTED_SECRET_VALUE, is_secret_key
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
# A '#' at the start of a path segment: `#password`, `"#password"`, `db.#token`, `db['#token']`, `db['a', '#token']`.
_SECRET_PATH_SEGMENT = re.compile(r'''(?:^|[.\[,])\s*['"]?#''')
# The `#key:` / `#key=` / `'#key': ` prefix of a fragment as rendered by str(), repr(), JSON, YAML or .env text.
_SECRET_KEY_FRAGMENT = re.compile(r'''['"]?#[^\s'":=,{}\[\]]+['"]?\s*[:=]\s*''')
# Evidence that a string holds a secret key it could not be parsed for: a quoted `'#key': ` (spaces allowed),
# an unquoted `#key:` / `#key=`, or a key whose '#' is escaped (`\\u0023`, `\\x23`, `%23`, `&#35;`).
_SECRET_MARKER = re.compile(
    r'''['"]#[^'"\n]*['"]\s*[:=]|(?:^|[\s{,\[(])#[^\s'":=,{}\[\]]+\s*[:=]|\\u0023|\\x23|%23|&#0*35;|&#x0*23;''',
    re.IGNORECASE | re.MULTILINE,
)
_CODE_FENCE = re.compile(r'^\s*```[\w-]*\s*\n(?P<body>.*?)\n?\s*```\s*$', re.DOTALL)
_CLOSING_BRACKETS = {'{': '}', '[': ']'}
_UNQUOTED_VALUE_END = re.compile(r'[\n,}\]]')
_DOUBLE_QUOTED = re.compile(r'"(?:\\.|[^"\\\n])*"')
_TOKEN = re.compile(r'''[^\s'",:={}\[\]]+''')
# Pydantic's str(ValidationError) echoes the invalid input: `[type=..., input_value=..., input_type=...]`.
_PYDANTIC_INPUT_VALUE = re.compile(r'input_value=.*?(?=, input_type=|\]$)', re.MULTILINE)
_MAX_CAUSE_DEPTH = 5
# Shorter secret values are replaced in exception text only where they stand alone (quoted, or as a whole
# non-numeric word), as replacing them everywhere would corrupt unrelated substrings such as HTTP status
# codes or IDs.
_MIN_REPLACED_SECRET_LEN = 6
_MIN_WORD_REPLACED_SECRET_LEN = 3
_MIN_SCALAR_SCRUBBED_LEN = 4
_CREDENTIAL_WORDS = r'credentials?|passwords?|passwd|passphrase|secret|token|pin|code|key'
# jsonschema validators whose `validator_value` comes from the schema and is safe to report.
_REDACTION_FAILED_SUMMARY = '[details omitted: argument redaction failed]'
_REPORTED_SCHEMA_VALIDATORS = frozenset({'required', 'enum', 'type'})


def _is_secret_path(path: Any) -> bool:
    return isinstance(path, str) and bool(_SECRET_PATH_SEGMENT.search(path))


def _secret_variants(secret: str) -> set[str]:
    return {secret, json.dumps(secret, ensure_ascii=False)[1:-1], json.dumps(secret)[1:-1], repr(secret)[1:-1]}


def _scalar_variants(value: bool | float) -> set[str]:
    return {str(value), json.dumps(value)}


def _collect_leaves(value: Any, found: set[str]) -> None:
    if isinstance(value, dict):
        for key, item in [*getattr(value, 'shadowed', ()), *value.items()]:
            _collect_leaves(key, found)
            _collect_leaves(item, found)
    elif isinstance(value, list):
        for item in value:
            _collect_leaves(item, found)
    elif isinstance(value, str) and value:
        found.update(_secret_variants(value))
        if _SECRET_MARKER.search(value):
            _collect_opaque_tokens(value, found)
        if isinstance(decoded := _parse_container(value), (dict, list)):
            _collect_leaves(decoded, found)
    elif isinstance(value, (bool, int, float)):
        found.update(_scalar_variants(value))


class _RedactionIncomplete(Exception):
    """A secret value that cannot be examined within the resource limits."""


def _ensure_examinable(value: Any) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _ensure_examinable(item)
    elif isinstance(value, list):
        for item in value:
            _ensure_examinable(item)
    elif isinstance(value, str) and len(value) > MAX_ARG_VALUE_LEN:
        raise _RedactionIncomplete


class _ShadowingDict(dict):
    """A dict decoded from JSON that remembers the values of duplicate keys it replaced."""

    def __init__(self, pairs: list[tuple[Any, Any]]):
        super().__init__()
        self.shadowed: list[tuple[Any, Any]] = []
        for key, item in pairs:
            if key in self:
                self.shadowed.append((key, self[key]))
            self[key] = item


def _decode_json(raw: str) -> Any:
    return json.loads(raw, object_pairs_hook=_ShadowingDict)


def _has_duplicate_keys(tree: ast.AST) -> bool:
    """True if a dictionary literal repeats an evaluated key; keys that cannot be evaluated count as repeated."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            try:
                keys = [ast.literal_eval(key) for key in node.keys if key is not None]
                if len(keys) != len(set(keys)):
                    return True
            except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
                return True
    return False


def _decode_literal(raw: str) -> Any:
    tree = ast.parse(raw.strip(), mode='eval')
    if _has_duplicate_keys(tree):
        raise ValueError('The literal repeats a dictionary key, which would hide a value from redaction.')
    return _to_json_compatible(ast.literal_eval(tree))


def _to_json_compatible(value: Any) -> Any:
    """Turns a decoded Python literal into plain JSON data, so that redaction sees every container and leaf."""
    if isinstance(value, dict):
        return {
            (key if isinstance(key, (str, int, float, bool)) or key is None else str(key)): _to_json_compatible(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_json_compatible(item) for item in value]
    if isinstance(value, bytes):
        return value.decode('utf-8', 'replace')
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _decode_fragment(raw: str) -> Any | None:
    for decode in (_decode_json, _decode_literal):
        try:
            return decode(raw)
        except (ValueError, SyntaxError, MemoryError, RecursionError):
            continue
    return None


def _parse_container(text: str) -> dict | list | None:
    """The object or array held by a JSON, Python literal or code-fenced JSON string, else None."""
    if len(text) > MAX_ARG_VALUE_LEN:
        return None
    if fenced := _CODE_FENCE.match(text):
        text = fenced.group('body')
    if text.lstrip()[:1] not in ('{', '['):
        return None
    decoded = _decode_fragment(text)
    return decoded if isinstance(decoded, (dict, list)) else None


def _collect_opaque_tokens(text: str, found: set[str]) -> None:
    """Collects the whole text and every token that could be part of a secret, for text that cannot be parsed."""
    found.update(_secret_variants(text))
    found.update(_TOKEN.findall(text))
    for quoted in _DOUBLE_QUOTED.findall(text):
        try:
            decoded = json.loads(quoted)
        except ValueError:
            continue
        found.update(_secret_variants(decoded))
        found.update(_TOKEN.findall(decoded))


def _collect_fragment_leaves(raw: str, found: set[str]) -> None:
    """Collects the secret leaves of a rendered value; every token of it when the format is not understood."""
    _collect_leaves(raw.strip('\'"'), found)
    decoded = _decode_fragment(raw)
    if decoded is None:
        found.update(_TOKEN.findall(raw))
    else:
        _collect_leaves(decoded, found)


def _quoted_end(text: str, start: int) -> int:
    quote = text[start]
    index = start + 1
    while index < len(text):
        if text[index] == '\\':
            index += 2
        elif text[index] == quote:
            return index + 1
        else:
            index += 1
    return len(text)


def _bracketed_end(text: str, start: int) -> int:
    depth = 0
    index = start
    while index < len(text):
        char = text[index]
        if char in ('"', "'"):
            index = _quoted_end(text, index)
            continue
        if char in _CLOSING_BRACKETS:
            depth += 1
        elif char in _CLOSING_BRACKETS.values():
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return len(text)


def _fragment_value_end(text: str, start: int) -> int:
    """The end of the value that starts at `start`; everything up to the end when its extent is unclear."""
    if start >= len(text):
        return len(text)
    first = text[start]
    if first in ('"', "'"):
        return _quoted_end(text, start)
    if first in _CLOSING_BRACKETS:
        return _bracketed_end(text, start)
    if first in ('|', '>'):
        return len(text)
    end = _UNQUOTED_VALUE_END.search(text, start)
    return end.start() if end else len(text)


def _mask_key_fragments(text: str, found: set[str]) -> str:
    parts: list[str] = []
    cursor = 0
    for match in _SECRET_KEY_FRAGMENT.finditer(text):
        if match.start() < cursor:
            continue
        value_end = _fragment_value_end(text, match.end())
        _collect_fragment_leaves(text[match.end() : value_end], found)
        parts.extend((text[cursor : match.end()], REDACTED_SECRET_VALUE))
        cursor = value_end
    parts.append(text[cursor:])
    return ''.join(parts)


def _mask_known_value(text: str, secret: str) -> str:
    if len(secret) >= _MIN_REPLACED_SECRET_LEN:
        return text.replace(secret, REDACTED_SECRET_VALUE)
    quoted = re.compile(rf'(?P<quote>[\'"]){re.escape(secret)}(?P=quote)')
    text = quoted.sub(lambda m: f'{m["quote"]}{REDACTED_SECRET_VALUE}{m["quote"]}', text)
    if len(secret) >= _MIN_WORD_REPLACED_SECRET_LEN and not secret.isdigit():
        text = re.sub(rf'(?<!\w){re.escape(secret)}(?!\w)', REDACTED_SECRET_VALUE, text)
    after_credential_word = re.compile(
        rf'(?P<context>\b(?:{_CREDENTIAL_WORDS})\b\W{{0,3}}(?:(?:is|was)\W{{1,3}})?){re.escape(secret)}(?!\w)',
        re.IGNORECASE,
    )
    return after_credential_word.sub(lambda m: f'{m["context"]}{REDACTED_SECRET_VALUE}', text)


def _mask_known_values(text: str, secrets: set[str]) -> str:
    for secret in sorted(secrets, key=len, reverse=True):
        text = _mask_known_value(text, secret)
    return text


def _redact_value(value: Any, found: set[str]) -> Any:
    """
    Returns a redacted copy of a JSON-compatible value and adds every masked plaintext value to `found`.

    Masks every value of a '#' key, whatever it looks like, since a 'KBC::' prefix is no proof of encryption,
    and additionally masks the value of path-based updates targeting a '#' key and descends into strings
    holding a JSON object, a Python literal or code-fenced JSON. Any other string that shows a secret key (YAML,
    .env text, invalid JSON) is masked as a whole, as the extent of its secret values cannot be told.
    """
    if isinstance(value, dict):
        secret_path = _is_secret_path(value.get('path'))
        redacted: dict[Any, Any] = {}
        for key, item in [*getattr(value, 'shadowed', ()), *value.items()]:
            masked_key = is_secret_key(key)
            if masked_key or (secret_path and key in _PATH_UPDATE_VALUE_KEYS):
                _ensure_examinable(item)
                _collect_leaves(item, found)
                redacted[key] = REDACTED_SECRET_VALUE
            else:
                redacted[key] = _redact_value(item, found)
        return redacted
    if isinstance(value, list):
        return [_redact_value(item, found) for item in value]
    if isinstance(value, str):
        if (parsed := _parse_container(value)) is not None:
            return json.dumps(_redact_value(parsed, found), ensure_ascii=False)
        if _SECRET_MARKER.search(value):
            _collect_opaque_tokens(value, found)
            return REDACTED_SECRET_VALUE
    return value


class _RedactedCall(NamedTuple):
    arguments: list[dict[str, str]]
    secrets: set[str]
    complete: bool


def _scrub_known_values(value: Any, known: set[str]) -> Any:
    """Masks the known secret values in a JSON-compatible value: exact matches first, then occurrences in strings."""
    if isinstance(value, dict):
        return {_scrub_known_values(key, known): _scrub_known_values(item, known) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub_known_values(item, known) for item in value]
    if isinstance(value, str):
        if value in known:
            return REDACTED_SECRET_VALUE
        if (parsed := _parse_container(value)) is not None:
            return json.dumps(_scrub_known_values(parsed, known), ensure_ascii=False)
        return _mask_known_values(value, known) if len(value) <= MAX_ARG_VALUE_LEN else value
    if isinstance(value, (bool, int, float)) and any(
        len(variant) >= _MIN_SCALAR_SCRUBBED_LEN and variant in known for variant in _scalar_variants(value)
    ):
        return REDACTED_SECRET_VALUE
    return value


def _redact_arguments(arguments: Mapping[str, Any]) -> _RedactedCall:
    """
    Serializes tool arguments for the Storage event with secrets masked.

    The secrets of all the arguments are collected first and then masked wherever else they occur, in the
    same argument or in another one, whichever comes first.

    Pydantic models are dumped to plain JSON data first, so the actual arguments are never mutated.
    If redaction fails, only the argument names are reported, never their raw values, and the returned
    secrets are incomplete, which the third item (`False`) tells the caller.
    """
    found: set[str] = set()
    try:
        redacted_values = {
            name: _redact_value(_JsonWrapper(data=value).model_dump(mode='json')['data'], found)
            for name, value in arguments.items()
        }
        encoded = [
            {'key': name, 'value': _JsonWrapper.encode_truncated(_scrub_known_values(value, found))}
            for name, value in redacted_values.items()
        ]
    except Exception as e:
        LOG.warning(
            f'Failed to redact tool arguments, reporting argument names only: {type(e).__name__}\n'
            f'{"".join(traceback.format_tb(e.__traceback__))}'
        )
        return _RedactedCall(
            [{'key': name, 'value': json.dumps('[redaction failed]')} for name in arguments], found, False
        )
    return _RedactedCall(encoded, found, True)


def _is_secret_enum(error: jsonschema.ValidationError) -> bool:
    return error.validator == 'enum' and any(is_secret_key(part) for part in error.absolute_path)


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
        if schema_error.validator in _REPORTED_SCHEMA_VALIDATORS and not _is_secret_enum(schema_error):
            text += f' (schema: {json.dumps(schema_error.validator_value, ensure_ascii=False, default=str)})'
        if validation_context := getattr(schema_error, 'validation_context', None):
            text += f' [{validation_context}]'
    else:
        text = str(exception)

    text = _mask_known_values(text, secrets)
    text = _PYDANTIC_INPUT_VALUE.sub(f'input_value={REDACTED_SECRET_VALUE}', text)
    return _mask_key_fragments(text, set())


def _redact_call(func: Callable, args: tuple, kwargs: dict) -> _RedactedCall:
    """Redacts the arguments of a tool call; reports no arguments and an incomplete secret set if that is impossible."""
    try:
        ctx_param_name = find_kwarg_by_type(func, Context)
        bound_args = inspect.signature(func).bind(*args, **kwargs)
        bound_args.apply_defaults()
        return _redact_arguments(
            {
                param_name: param_value
                for param_name, param_value in bound_args.arguments.items()
                if param_name not in [ctx_param_name, 'self', 'cls']
            }
        )
    except Exception as e:
        LOG.warning(f'Failed to redact tool call, details of its failure will be omitted: {type(e).__name__}')
        return _RedactedCall([], set(), False)


def _failure_summary(exception: BaseException, redacted: _RedactedCall) -> str:
    return _safe_exception_summary(exception, redacted.secrets) if redacted.complete else _REDACTION_FAILED_SUMMARY


def _sanitized_exception(exception: BaseException, redacted: _RedactedCall, depth: int = 0) -> BaseException:
    """A copy of the exception chain that keeps every traceback but carries only redacted text."""
    sanitized_type = type(type(exception).__name__, (Exception,), {'__module__': type(exception).__module__})
    sanitized = sanitized_type(_failure_summary(exception, redacted)).with_traceback(exception.__traceback__)
    cause = exception.__cause__ or (None if exception.__suppress_context__ else exception.__context__)
    if cause is not None and depth < _MAX_CAUSE_DEPTH:
        sanitized.__cause__ = _sanitized_exception(cause, redacted, depth + 1)
    return sanitized


def _sanitized_exc_info(exception: BaseException, redacted: _RedactedCall) -> tuple[type, BaseException, Any]:
    sanitized = _sanitized_exception(exception, redacted)
    return type(sanitized), sanitized, sanitized.__traceback__


async def _trigger_event(
    func: Callable,
    args: tuple,
    kwargs: dict,
    exception: Exception | None,
    execution_time: float,
    redacted: _RedactedCall | None = None,
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

    redacted = redacted or _redact_call(func, args, kwargs)

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
            'arguments': redacted.arguments,
        },
    }
    if exception:
        summary = _failure_summary(exception, redacted)
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
            redacted: _RedactedCall | None = None
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
                    redacted = _redact_call(func, args, kwargs)
                    summary = _failure_summary(e, redacted)
                    if caller_fault:
                        # Routine (expired token, missing object, bad input): no traceback, not an ERROR.
                        first_line = next(iter(summary.splitlines()), type(e).__name__)
                        LOG.warning(f'MCP tool "{func.__name__}" call failed: {first_line}', extra=extra)
                    else:
                        LOG.error(
                            f'MCP tool "{func.__name__}" call failed.',
                            exc_info=_sanitized_exc_info(e, redacted),
                            extra=extra,
                        )
                    exception = e
                    raise

            finally:
                try:
                    await _trigger_event(func, args, kwargs, exception, time.perf_counter() - start, redacted)
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
