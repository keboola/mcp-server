from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from keboola_mcp_server.search_index.types import VerifiedSession

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


def test_valid_session_accepts_normal_inputs():
    session = VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)
    assert session.project_id == '1234'
    assert session.token_hash == 'abcdef0123456789'
    assert session.verified_at == _NOW


def test_session_is_frozen():
    session = VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)
    with pytest.raises(FrozenInstanceError):
        session.project_id = '5678'  # type: ignore[misc]


@pytest.mark.parametrize(
    'bad_project_id',
    [
        '',
        '..',
        '../other',
        'foo/bar',
        'foo\\bar',
        'foo bar',
        'foo\x00bar',
        'foo:bar',
        '.',
    ],
)
def test_session_rejects_unsafe_project_id(bad_project_id):
    with pytest.raises(ValueError, match='Invalid project_id'):
        VerifiedSession(project_id=bad_project_id, token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.mark.parametrize(
    'bad_token_hash',
    [
        '',
        'abcdef',
        'abcdef012345678',  # 15 chars
        'abcdef01234567890',  # 17 chars
        'ABCDEF0123456789',  # uppercase
        'ghij0123456789ab',  # non-hex
        '../0123456789abcd',
    ],
)
def test_session_rejects_unsafe_token_hash(bad_token_hash):
    with pytest.raises(ValueError, match='Invalid token_hash'):
        VerifiedSession(project_id='1234', token_hash=bad_token_hash, verified_at=_NOW)


def test_session_rejects_non_datetime_verified_at():
    with pytest.raises(TypeError, match='verified_at must be datetime'):
        VerifiedSession(
            project_id='1234',
            token_hash='abcdef0123456789',
            verified_at='2026-05-25',  # type: ignore[arg-type]
        )
