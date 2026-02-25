"""
Unit tests for integtests/project_lock.py.
All HTTP calls are mocked via pytest-mock; no real Storage API is required.
"""

import json
import os
import socket
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call

import pytest

from integtests.project_lock import (
    LOCK_KEY_PREFIX,
    LockInfo,
    ProjectLock,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_lock(
    *,
    lock_id: str = 'test-uuid',
    minutes_ago: float = 0,
    runner_info: str = 'host/1',
) -> dict:
    """Build a raw branch-metadata dict as returned by the Storage API."""
    acquired_at = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    payload = json.dumps(
        {
            'lock_id': lock_id,
            'acquired_at': acquired_at.isoformat(),
            'runner_info': runner_info,
        }
    )
    return {'key': LOCK_KEY_PREFIX + lock_id, 'value': payload}


def _released_entry(lock_id: str) -> dict:
    return {
        'key': LOCK_KEY_PREFIX + lock_id + '.released',
        'value': datetime.now(timezone.utc).isoformat(),
    }


def _make_project_lock(**kwargs) -> ProjectLock:
    defaults = dict(
        storage_api_url='https://connection.keboola.com',
        storage_api_token='test-token',
        ttl_minutes=60,
        poll_interval_seconds=1,
        max_wait_minutes=2,
        anti_collision_seconds=0,
    )
    defaults.update(kwargs)
    return ProjectLock(**defaults)


# ---------------------------------------------------------------------------
# test_acquire_happy_path
# ---------------------------------------------------------------------------


def test_acquire_happy_path(mocker):
    """Single runner: writes lock, reads it back as winner, returns LockInfo."""
    lock = _make_project_lock()
    my_lock_id = 'aaaaaaaa-0000-0000-0000-000000000001'

    post_mock = mocker.patch.object(lock, '_post', return_value=[])
    mocker.patch('uuid.uuid4', return_value=MagicMock(__str__=lambda _: my_lock_id))

    # _read_metadata returns only our own entry after the anti-collision sleep
    my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=0)
    mocker.patch.object(lock, '_read_metadata', return_value=[my_entry])
    mocker.patch('time.sleep')

    result = lock.acquire()

    assert isinstance(result, LockInfo)
    assert result.lock_id == my_lock_id
    assert result.metadata_key == LOCK_KEY_PREFIX + my_lock_id
    # Verify that the lock key was written
    written_keys = [
        entry['key']
        for call_args in post_mock.call_args_list
        for entry in call_args.kwargs.get('data', {}).get('metadata', [])
    ]
    assert LOCK_KEY_PREFIX + my_lock_id in written_keys


# ---------------------------------------------------------------------------
# test_acquire_anti_collision_waits
# ---------------------------------------------------------------------------


def test_acquire_anti_collision_waits(mocker):
    """time.sleep is called with anti_collision_seconds before reading back."""
    anti_collision = 3
    lock = _make_project_lock(anti_collision_seconds=anti_collision)
    my_lock_id = 'aaaaaaaa-0000-0000-0000-000000000002'

    mocker.patch.object(lock, '_post', return_value=[])
    mocker.patch('uuid.uuid4', return_value=MagicMock(__str__=lambda _: my_lock_id))

    my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=0)
    mocker.patch.object(lock, '_read_metadata', return_value=[my_entry])
    sleep_mock = mocker.patch('time.sleep')

    lock.acquire()

    # At minimum one sleep with the anti-collision value must have occurred
    assert any(c == call(anti_collision) for c in sleep_mock.call_args_list)


# ---------------------------------------------------------------------------
# test_acquire_win_oldest_timestamp
# ---------------------------------------------------------------------------


def test_acquire_win_oldest_timestamp(mocker):
    """Two active entries; ours is the oldest → we win."""
    lock = _make_project_lock()
    my_lock_id = 'aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa'
    other_lock_id = 'aaaaaaaa-0000-0000-0000-bbbbbbbbbbbb'

    mocker.patch.object(lock, '_post', return_value=[])
    mocker.patch('uuid.uuid4', return_value=MagicMock(__str__=lambda _: my_lock_id))

    # Our entry is 5 minutes older than the other
    my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=5)
    other_entry = _make_lock(lock_id=other_lock_id, minutes_ago=0)
    mocker.patch.object(lock, '_read_metadata', return_value=[my_entry, other_entry])
    mocker.patch('time.sleep')

    result = lock.acquire()
    assert result.lock_id == my_lock_id


# ---------------------------------------------------------------------------
# test_acquire_lose_to_older
# ---------------------------------------------------------------------------


def test_acquire_lose_to_older(mocker):
    """
    Two active entries; theirs is older → we release our candidate, sleep, then
    on the second iteration ours is oldest and we acquire.
    """
    lock = _make_project_lock(poll_interval_seconds=1, max_wait_minutes=5)

    my_lock_id_1 = 'my-lock-id-0001'
    my_lock_id_2 = 'my-lock-id-0002'
    other_lock_id = 'other-lock-id-00'

    uuid_iter = iter([my_lock_id_1, my_lock_id_2])
    mocker.patch('uuid.uuid4', side_effect=lambda: MagicMock(__str__=lambda _: next(uuid_iter)))

    post_mock = mocker.patch.object(lock, '_post', return_value=[])

    other_entry = _make_lock(lock_id=other_lock_id, minutes_ago=10)

    def metadata_side_effect():
        # Check how many times _post was called to determine which iteration we're in
        release_calls = [
            c
            for c in post_mock.call_args_list
            if any(
                k.endswith('.released')
                for entry in c.kwargs.get('data', {}).get('metadata', [])
                for k in [entry['key']]
            )
        ]
        if not release_calls:
            # First read: both entries active, other is older
            my_entry = _make_lock(lock_id=my_lock_id_1, minutes_ago=0)
            return [other_entry, my_entry]
        else:
            # Second iteration: only our new entry (other runner released theirs)
            my_entry2 = _make_lock(lock_id=my_lock_id_2, minutes_ago=0)
            return [my_entry2]

    mocker.patch.object(lock, '_read_metadata', side_effect=metadata_side_effect)
    mocker.patch('time.sleep')

    result = lock.acquire()
    assert result.lock_id == my_lock_id_2

    # We should have released the first candidate
    released_keys = [
        entry['key']
        for c in post_mock.call_args_list
        for entry in c.kwargs.get('data', {}).get('metadata', [])
        if entry['key'].endswith('.released')
    ]
    assert LOCK_KEY_PREFIX + my_lock_id_1 + '.released' in released_keys


# ---------------------------------------------------------------------------
# test_acquire_stale_detected
# ---------------------------------------------------------------------------


def test_acquire_stale_detected(mocker):
    """Stale entry detected → releases it, cleans project, re-acquires."""
    lock = _make_project_lock(ttl_minutes=60)

    stale_lock_id = 'stale-lock-id-001'
    my_lock_id = 'my-fresh-lock-001'

    uuid_iter = iter([my_lock_id, my_lock_id])  # second acquire returns same id
    mocker.patch('uuid.uuid4', side_effect=lambda: MagicMock(__str__=lambda _: next(uuid_iter)))

    post_mock = mocker.patch.object(lock, '_post', return_value=[])
    clean_mock = mocker.patch.object(lock, '_clean_project')

    # Build a stale entry (acquired 120 minutes ago, TTL=60)
    stale_entry = _make_lock(lock_id=stale_lock_id, minutes_ago=120)

    read_call_count = [0]

    def metadata_side_effect():
        read_call_count[0] += 1
        if read_call_count[0] == 1:
            # First read: stale entry + our pending entry
            my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=0)
            return [stale_entry, my_entry]
        else:
            # After cleanup: only our fresh entry
            my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=0)
            return [my_entry]

    mocker.patch.object(lock, '_read_metadata', side_effect=metadata_side_effect)
    mocker.patch('time.sleep')

    result = lock.acquire()

    # _clean_project must have been called
    clean_mock.assert_called_once()

    # The stale lock must have been released
    released_keys = [
        entry['key']
        for c in post_mock.call_args_list
        for entry in c.kwargs.get('data', {}).get('metadata', [])
        if entry['key'].endswith('.released')
    ]
    assert LOCK_KEY_PREFIX + stale_lock_id + '.released' in released_keys

    assert result.lock_id == my_lock_id


# ---------------------------------------------------------------------------
# test_clean_project_deletes_buckets
# ---------------------------------------------------------------------------


def test_clean_project_deletes_buckets(mocker):
    """_clean_project calls DELETE for each bucket with force=true."""
    lock = _make_project_lock()

    mocker.patch.object(
        lock,
        '_get',
        side_effect=lambda path, **params: (
            [{'id': 'in.c-bucket1'}, {'id': 'in.c-bucket2'}] if path.endswith('/buckets') else []
        ),
    )
    delete_mock = mocker.patch.object(lock, '_delete')

    lock._clean_project()

    delete_calls = [c for c in delete_mock.call_args_list if 'buckets' in c.args[0]]
    deleted_bucket_paths = {c.args[0] for c in delete_calls}
    assert '/v2/storage/buckets/in.c-bucket1' in deleted_bucket_paths
    assert '/v2/storage/buckets/in.c-bucket2' in deleted_bucket_paths
    # Each bucket deleted with force=true
    for c in delete_calls:
        assert c.kwargs.get('force') == 'true'


# ---------------------------------------------------------------------------
# test_clean_project_deletes_configs
# ---------------------------------------------------------------------------


def test_clean_project_deletes_configs(mocker):
    """_clean_project calls DELETE twice for each config (move to trash + purge)."""
    lock = _make_project_lock()

    components = [
        {
            'id': 'ex-generic-v2',
            'configurations': [{'id': '123'}, {'id': '456'}],
        }
    ]

    mocker.patch.object(
        lock,
        '_get',
        side_effect=lambda path, **params: ([] if path.endswith('/buckets') else components),
    )
    delete_mock = mocker.patch.object(lock, '_delete')

    lock._clean_project()

    config_delete_paths = [c.args[0] for c in delete_mock.call_args_list if 'configs' in c.args[0]]
    # Each config deleted twice
    assert config_delete_paths.count('/v2/storage/components/ex-generic-v2/configs/123') == 2
    assert config_delete_paths.count('/v2/storage/components/ex-generic-v2/configs/456') == 2


# ---------------------------------------------------------------------------
# test_release_writes_released_key
# ---------------------------------------------------------------------------


def test_release_writes_released_key(mocker):
    """release() writes the .released metadata key for the given lock_id."""
    lock = _make_project_lock()
    post_mock = mocker.patch.object(lock, '_post', return_value=[])

    lock_info = LockInfo(
        lock_id='release-test-id',
        acquired_at=datetime.now(timezone.utc),
        runner_info='host/99',
        metadata_key=LOCK_KEY_PREFIX + 'release-test-id',
    )
    lock.release(lock_info)

    written_keys = [
        entry['key'] for c in post_mock.call_args_list for entry in c.kwargs.get('data', {}).get('metadata', [])
    ]
    assert LOCK_KEY_PREFIX + 'release-test-id.released' in written_keys


# ---------------------------------------------------------------------------
# test_runner_info_includes_hostname_pid
# ---------------------------------------------------------------------------


def test_runner_info_includes_hostname_pid(monkeypatch):
    """runner_info contains hostname and PID."""
    monkeypatch.delenv('GITHUB_RUN_ID', raising=False)
    info = ProjectLock._runner_info()
    assert socket.gethostname() in info
    assert str(os.getpid()) in info


# ---------------------------------------------------------------------------
# test_runner_info_includes_ci_job
# ---------------------------------------------------------------------------


def test_runner_info_includes_ci_job(monkeypatch):
    """runner_info includes GITHUB_RUN_ID when set."""
    monkeypatch.setenv('GITHUB_RUN_ID', '987654321')
    info = ProjectLock._runner_info()
    assert 'CI=987654321' in info


# ---------------------------------------------------------------------------
# test_max_wait_exceeded_raises
# ---------------------------------------------------------------------------


def test_max_wait_exceeded_raises(mocker):
    """Raises TimeoutError after max_wait_minutes is exhausted."""
    lock = _make_project_lock(
        poll_interval_seconds=1,
        max_wait_minutes=0,  # expire immediately
        anti_collision_seconds=0,
    )

    other_lock_id = 'other-runner-lock'
    other_entry = _make_lock(lock_id=other_lock_id, minutes_ago=0)
    my_lock_id = 'my-candidate-lock'
    my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=0)

    mocker.patch('uuid.uuid4', return_value=MagicMock(__str__=lambda _: my_lock_id))
    mocker.patch.object(lock, '_post', return_value=[])
    # The other runner always holds the lock; their entry is older
    mocker.patch.object(lock, '_read_metadata', return_value=[other_entry, my_entry])
    mocker.patch('time.sleep')

    # Patch datetime.now to return a time past the deadline on the second call
    original_now = datetime.now

    call_count = [0]

    def fake_now(tz=None):
        call_count[0] += 1
        if call_count[0] <= 2:
            return original_now(tz)
        # Return a time far in the future to trigger the deadline
        return datetime(2099, 1, 1, tzinfo=timezone.utc)

    mocker.patch('integtests.project_lock.datetime', wraps=datetime)
    mocker.patch('integtests.project_lock.datetime.now', side_effect=fake_now)

    with pytest.raises(TimeoutError, match='Could not acquire project lock'):
        lock.acquire()
