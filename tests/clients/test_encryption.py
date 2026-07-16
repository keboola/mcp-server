from typing import Any

import pytest

from keboola_mcp_server.clients.encryption import (
    REDACTED_SECRET_VALUE,
    contains_plaintext_secrets,
    is_encrypted_value,
    iter_secret_items,
    redact_secrets,
)


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        ('KBC::ProjectSecure::abcd', True),
        ('plaintext', False),
        ('', False),
        (None, False),
        (123, False),
        ({'KBC::': 'foo'}, False),
    ],
)
def test_is_encrypted_value(value: Any, expected: bool) -> None:
    assert is_encrypted_value(value) == expected


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        # no secrets at all
        ({'host': 'db.example.com', 'port': 5432}, []),
        # top-level secret
        ({'#password': 'secret'}, [('#password', 'secret')]),
        # nested in dicts and lists
        (
            {'parameters': {'db': {'#password': 'secret'}, 'tables': [{'#api_key': 'key'}]}},
            [('#password', 'secret'), ('#api_key', 'key')],
        ),
        # non-dict, non-list values
        ('just-a-string', []),
        (None, []),
        # already encrypted values are still yielded (filtering is up to the caller)
        ({'#token': 'KBC::ProjectSecure::abcd'}, [('#token', 'KBC::ProjectSecure::abcd')]),
    ],
)
def test_iter_secret_items(value: Any, expected: list[tuple[str, Any]]) -> None:
    assert list(iter_secret_items(value)) == expected


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        ({'host': 'db.example.com'}, False),
        ({'#password': 'secret'}, True),
        ({'#password': 'KBC::ProjectSecure::abcd'}, False),
        ({'parameters': {'#password': 'KBC::ProjectSecure::abcd', 'nested': [{'#key': 'plain'}]}}, True),
        # a '#'-key holding a non-string value is treated as plaintext (fail-safe)
        ({'#config': {'user': 'admin'}}, True),
        ({}, False),
        (None, False),
    ],
)
def test_contains_plaintext_secrets(value: Any, expected: bool) -> None:
    assert contains_plaintext_secrets(value) == expected


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        # plaintext secret is masked
        ({'#password': 'secret'}, {'#password': REDACTED_SECRET_VALUE}),
        # encrypted secret is kept as-is
        ({'#password': 'KBC::ProjectSecure::abcd'}, {'#password': 'KBC::ProjectSecure::abcd'}),
        # non-secret values are kept, nested structures are walked
        (
            {'db': {'host': 'db.example.com', '#password': 'secret'}, 'list': [{'#api_key': 'key'}, 'foo']},
            {
                'db': {'host': 'db.example.com', '#password': REDACTED_SECRET_VALUE},
                'list': [{'#api_key': REDACTED_SECRET_VALUE}, 'foo'],
            },
        ),
        # non-container values pass through
        ('just-a-string', 'just-a-string'),
        (None, None),
    ],
)
def test_redact_secrets(value: Any, expected: Any) -> None:
    assert redact_secrets(value) == expected


def test_redact_secrets_does_not_mutate_input() -> None:
    original = {'db': {'#password': 'secret'}}
    redact_secrets(original)
    assert original == {'db': {'#password': 'secret'}}
