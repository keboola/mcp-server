import json
import time

import pytest

from keboola_mcp_server.redaction import (
    DETAILS_OMITTED_MARKER,
    DETAILS_OMITTED_SECRET,
    DETAILS_OMITTED_UNREADABLE,
    MAX_EXAMINED_LEN,
    contains_secret,
    exception_text,
)

CANARY = 'canary-s3cr3t-ai4029'


@pytest.mark.parametrize(
    'value',
    [
        {'#password': CANARY},
        {'db': {'host': 'h', 'hosts': [{'#token': CANARY}]}},
        {'#api_key': 'KBC::ProjectSecure::fake-cipher'},
        {'#pin': 1234, '#enabled': True},
        f'Events #_{CANARY}: clicks',
        '#2fa_token=x',
        '#šifra: x',
        '#пароль=x',
        'ok #1a: x',
        {'op': 'set', 'path': '#password', 'value': CANARY},
        {'op': 'set', 'path': 'db.#token', 'value': CANARY},
        {'op': 'str_replace', 'path': "db['#token']", 'search_for': CANARY, 'replace_with': 'x'},
        {'op': 'set', 'path': '"#password"', 'value': CANARY},
        {'op': 'set', 'path': "db['user', '#password']", 'value': CANARY},
        {'op': 'set', 'path': 'db.*', 'value': CANARY},
        {'op': 'set', 'path': 'db."*"', 'value': CANARY},
        {'op': 'set', 'path': '$..password', 'value': CANARY},
        {'op': 'set', 'path': 'db[?(@.a)]', 'value': CANARY},
        json.dumps({'user': 'alice', '#password': CANARY}),
        json.dumps(json.dumps({'#password': CANARY})),
        json.dumps([{'op': 'set', 'path': '#password', 'value': CANARY}]),
        json.dumps({'#credentials': {'password': CANARY}}),
        '```json\n{"user": "alice", "#password": "' + CANARY + '"}\n```',
        str({'user': 'alice', '#password': CANARY}),
        str({'#api key': CANARY}),
        f'user: alice\n"#password": prefix,{CANARY}\n',
        f'"#vendor\'s_api_key": {CANARY}',
        '"#key\\"x": ' + CANARY,
        f'USER=alice\n#PASSWORD={CANARY}\n',
        f'GET /callback?id=1#access_token={CANARY}',
        '{"\\u0023password": "' + CANARY + '"}',
        f'%23password={CANARY}',
        "{'path': '#password', 'value': 'x'}",
        'x' * (MAX_EXAMINED_LEN + 1),
        [[{'#password': CANARY}]],
    ],
    ids=lambda value: str(value)[:40],
)
def test_secret_is_detected(value):
    assert contains_secret(value)


@pytest.mark.parametrize(
    'value',
    [
        None,
        'plain text',
        {'user': 'alice', 'port': 5432, 'tags': ['a', 'b']},
        {'op': 'set', 'path': 'db.user', 'value': 'alice'},
        {'op': 'set', 'path': 'db.user'},
        {'path': '#password'},
        json.dumps({'user': 'alice', 'items': [1, 2, 3]}),
        'see issue #12 and C# and #hashtag',
        '[REDACTED]',
        '[draft]',
        'Events #1: clicks',
        '#1:',
        'Total #42=ok',
        '"just a quoted string"',
        '{not json}',
        'https://example.com/page#section',
        '#' * 50,
    ],
    ids=lambda value: str(value)[:40],
)
def test_no_secret_is_detected(value):
    assert not contains_secret(value)


@pytest.mark.parametrize(
    'text',
    [
        '#' * 20_000,
        '#' * 20_000 + ':',
        '"#' * 10_000,
        "'#" * 10_000,
        '#a' * 10_000,
        '#' + '1' * 20_000,
        '#1' * 10_000,
        '#1 ' * 10_000,
        '\\' * 20_000,
        '\\' * 20_000 + '"',
        '"#' + '\\' * 20_000,
        '```\n' + '\n' * 5_000 + 'x',
        '&#' + '0' * 20_000,
        '[' * 9_990,
        '(' * 9_990,
        '[' * 4_000 + ']' * 4_000,
        "{'a':" * 1_900 + '1' + '}' * 1_900,
        '[' + '-' * 9_980 + '1]',
        '[' + '9' * 9_980 + ']',
        '[' + '1+' * 4_900 + '1]',
        ' ' * 20_000 + '#',
    ],
    ids=lambda text: f'{text[:6]!r}..{len(text)}',
)
def test_hostile_text_is_examined_quickly(text):
    started = time.perf_counter()
    contains_secret(text)
    contains_secret({'path': text, 'value': 'x'})
    exception_text(RuntimeError(text), False)

    assert time.perf_counter() - started < 1


def test_deeply_nested_value_counts_as_carrying_a_secret():
    value: list = []
    for _ in range(5_000):
        value = [value]

    assert contains_secret(value)


class _Unreadable(Exception):
    def __str__(self) -> str:
        raise RuntimeError('str failed')


@pytest.mark.parametrize(
    ('exception', 'call_carries_secret', 'expected', 'omitted'),
    [
        (RuntimeError('Rejected 401 for config 7'), False, 'Rejected 401 for config 7', False),
        (RuntimeError(f'Rejected {CANARY}'), True, DETAILS_OMITTED_SECRET, True),
        (RuntimeError(f'Upstream said #PASSWORD={CANARY}'), False, DETAILS_OMITTED_MARKER, True),
        (RuntimeError('x' * (MAX_EXAMINED_LEN + 1)), False, DETAILS_OMITTED_MARKER, True),
        (_Unreadable(), False, DETAILS_OMITTED_UNREADABLE, True),
        (_Unreadable(), True, DETAILS_OMITTED_SECRET, True),
    ],
    ids=['plain', 'call_carries_secret', 'message_shows_key', 'too_long', 'unreadable', 'unreadable_with_secret'],
)
def test_exception_text(exception, call_carries_secret, expected, omitted):
    shown = exception_text(exception, call_carries_secret)

    assert (shown.text, shown.omitted) == (expected, omitted)
