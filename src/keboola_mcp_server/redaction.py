"""
Keeps secrets out of tool-call telemetry by failing closed.

A tool call that carries any secret (a '#' key, an update of a secret path, or text showing a secret key)
is reported without its argument values and without its failure text, instead of trying to mask the values
and to find them again wherever they are echoed. The detection is deliberately conservative: a false
positive only costs some telemetry detail.
"""

import json
import re
from typing import Any, NamedTuple

from keboola_mcp_server.clients.encryption import SECRET_KEY_PREFIX

# Strings longer than this are not examined at all, which counts as carrying a secret.
MAX_EXAMINED_LEN = 1_000_000
# JSON is decoded only up to this length and nesting, so decoding stays cheap.
_MAX_DECODED_LEN = 10_000
_MAX_DECODE_DEPTH = 3

# Keys of path-based update operations (e.g. `update_config`'s `parameter_updates`) that carry the value
# written to the `path`.
_PATH_UPDATE_VALUE_KEYS = frozenset({'value', 'search_for', 'replace_with'})
# A path of plain names and numeric indexes (`db.user`, `items[0].name`). Any other path can reach a `#` field
# in ways that are hard to rule out (`#password`, `\\#password`, `'x'|'#password'`, `db.*`, `$..x`).
_PLAIN_PATH = re.compile(r'[\w$-]+(?:\.[\w$-]+|\[\d+\])*')
# Text showing a secret key: a quoted `"#key":` (spaces and escapes allowed), an unquoted `#key:` or `#key=`
# (a purely numeric key such as `#1:` is not one),
# an escaped quote followed by '#', or an escaped '#'. Every alternative is matched in linear time.
_SECRET_MARKER = re.compile(
    r'''"#(?:\\.|[^"\\\n])*"\s*[:=]|\x27#(?:\\.|[^\x27\\\n])*\x27\s*[:=]|#(?!\d+\s*[:=])[^\s#\x27":=,{}\[\]]*\s*[:=]'''
    r'''|\\["\x27]#|\\u0023|\\x23|%23|&#0*35;|&#x0*23;''',
    re.IGNORECASE,
)

DETAILS_OMITTED_SECRET = '[details omitted: the call carries a secret]'
DETAILS_OMITTED_MARKER = '[details omitted: the message shows a secret key]'
DETAILS_OMITTED_UNREADABLE = '[details omitted: the message could not be read]'


class ExceptionText(NamedTuple):
    text: str
    omitted: bool


def _is_secret_path(path: Any) -> bool:
    return isinstance(path, str) and not _PLAIN_PATH.fullmatch(path)


def _string_shows_secret(text: str, depth: int) -> bool:
    if len(text) > MAX_EXAMINED_LEN or _SECRET_MARKER.search(text):
        return True
    if text.lstrip()[:1] not in ('{', '[', '"'):
        return False
    decoded = None
    if len(text) <= _MAX_DECODED_LEN and depth < _MAX_DECODE_DEPTH:
        try:
            decoded = json.loads(text)
        except (ValueError, RecursionError):
            decoded = None
    if decoded is None:
        return '#' in text
    return _contains_secret(decoded, depth + 1)


def _contains_secret(value: Any, depth: int) -> bool:
    if isinstance(value, dict):
        if any(isinstance(key, str) and key.startswith(SECRET_KEY_PREFIX) for key in value):
            return True
        if _is_secret_path(value.get('path')) and _PATH_UPDATE_VALUE_KEYS.intersection(value):
            return True
        return any(_contains_secret(item, depth) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_secret(item, depth) for item in value)
    if isinstance(value, str):
        return _string_shows_secret(value, depth)
    return False


def contains_secret(value: Any) -> bool:
    """
    True if the JSON-compatible value carries a secret, or if that cannot be told.

    Never raises: a value too deeply nested to walk counts as carrying a secret.
    """
    try:
        return _contains_secret(value, 0)
    except RecursionError:
        return True


def exception_text(exception: BaseException, call_carries_secret: bool) -> ExceptionText:
    """The text of the exception for telemetry, or a note on why it is omitted. Never raises."""
    if call_carries_secret:
        return ExceptionText(DETAILS_OMITTED_SECRET, True)
    try:
        text = str(exception)
    except Exception:
        return ExceptionText(DETAILS_OMITTED_UNREADABLE, True)
    if len(text) > MAX_EXAMINED_LEN or _SECRET_MARKER.search(text):
        return ExceptionText(DETAILS_OMITTED_MARKER, True)
    return ExceptionText(text, False)
