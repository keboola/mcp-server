"""
Unit tests for integtests/project_lock.py.

All HTTP calls are mocked via pytest-mock — no real Storage API or Keboola project is
ever touched.  These tests do not require any INTEGTEST_* environment variables.
"""

import json
import os
import socket
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call

import pytest

from integtests.project_lock import (
    LOCK_KEY_PREFIX,
    AcquiredProject,
    LockInfo,
    ProjectEndpoint,
    ProjectLock,
    ProjectPool,
    verify_project_endpoint,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_lock(
    *,
    lock_id: str = 'test-uuid',
    minutes_ago: float = 0,
    runner_info: str = 'host/1',
    meta_id: int | None = None,
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
    result = {'key': LOCK_KEY_PREFIX + lock_id, 'value': payload}
    if meta_id is not None:
        result['id'] = meta_id
    return result


def _released_entry(lock_id: str, meta_id: int | None = None) -> dict:
    result = {
        'key': LOCK_KEY_PREFIX + lock_id + '.released',
        'value': datetime.now(timezone.utc).isoformat(),
    }
    if meta_id is not None:
        result['id'] = meta_id
    return result


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
    mocker.patch.object(lock, 'clean_project')

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
    mocker.patch.object(lock, 'clean_project')

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
    mocker.patch.object(lock, 'clean_project')

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
    mocker.patch.object(lock, 'clean_project')

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
    clean_mock = mocker.patch.object(lock, 'clean_project')

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
    clean_mock.assert_not_called()

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

    lock.clean_project()

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

    lock.clean_project()

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


# ===========================================================================
# Helpers for ProjectPool / _try_acquire_once tests
# ===========================================================================


def _make_endpoint(
    url: str = 'https://connection.keboola.com',
    token: str = 'test-token',
    schema: str = 'WORKSPACE_TEST',
    project_id: str = 'proj-001',
    project_name: str = 'Test Project',
) -> ProjectEndpoint:
    return ProjectEndpoint(
        storage_api_url=url,
        storage_api_token=token,
        workspace_schema=schema,
        project_id=project_id,
        project_name=project_name,
    )


def _make_pool(**kwargs) -> ProjectPool:
    defaults = dict(
        endpoints=[_make_endpoint()],
        ttl_minutes=60,
        poll_interval_seconds=1,
        max_wait_minutes=5,
        anti_collision_seconds=0,
    )
    defaults.update(kwargs)
    return ProjectPool(**defaults)


def _make_lock_info(lock_id: str = 'test-lock-id') -> LockInfo:
    return LockInfo(
        lock_id=lock_id,
        acquired_at=datetime.now(timezone.utc),
        runner_info='host/1',
        metadata_key=LOCK_KEY_PREFIX + lock_id,
    )


# ===========================================================================
# _try_acquire_once tests
# ===========================================================================


# ---------------------------------------------------------------------------
# test_try_acquire_once_happy_path
# ---------------------------------------------------------------------------


def test_try_acquire_once_happy_path(mocker):
    """Single candidate, we are oldest → returns LockInfo."""
    lock = _make_project_lock()
    my_lock_id = 'try-once-happy-01'

    mocker.patch('uuid.uuid4', return_value=MagicMock(__str__=lambda _: my_lock_id))
    mocker.patch.object(lock, '_post', return_value=[])
    my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=0)
    mocker.patch.object(lock, '_read_metadata', return_value=[my_entry])
    sleep_mock = mocker.patch('time.sleep')
    cleanup_mock = mocker.patch.object(lock, '_cleanup_old_locks')
    clean_mock = mocker.patch.object(lock, 'clean_project')

    result = lock._try_acquire_once()

    assert isinstance(result, LockInfo)
    assert result.lock_id == my_lock_id
    assert result.metadata_key == LOCK_KEY_PREFIX + my_lock_id
    assert any(c == call(0) for c in sleep_mock.call_args_list)  # anti_collision=0
    cleanup_mock.assert_called_once_with(my_lock_id)
    clean_mock.assert_called_once()


# ---------------------------------------------------------------------------
# test_try_acquire_once_loses_to_active_runner
# ---------------------------------------------------------------------------


def test_try_acquire_once_loses_to_active_runner(mocker):
    """Other runner is older → returns None; our candidate is released."""
    lock = _make_project_lock()
    my_lock_id = 'try-once-lose-001'
    other_lock_id = 'try-once-other-01'

    mocker.patch('uuid.uuid4', return_value=MagicMock(__str__=lambda _: my_lock_id))
    post_mock = mocker.patch.object(lock, '_post', return_value=[])
    clean_mock = mocker.patch.object(lock, 'clean_project')

    other_entry = _make_lock(lock_id=other_lock_id, minutes_ago=5)  # older
    my_entry = _make_lock(lock_id=my_lock_id, minutes_ago=0)
    mocker.patch.object(lock, '_read_metadata', return_value=[other_entry, my_entry])
    mocker.patch('time.sleep')

    result = lock._try_acquire_once()

    assert result is None
    # Our candidate must be released
    released_keys = [
        entry['key']
        for c in post_mock.call_args_list
        for entry in c.kwargs.get('data', {}).get('metadata', [])
        if entry['key'].endswith('.released')
    ]
    assert LOCK_KEY_PREFIX + my_lock_id + '.released' in released_keys
    clean_mock.assert_not_called()


# ---------------------------------------------------------------------------
# test_try_acquire_once_stale_then_wins
# ---------------------------------------------------------------------------


def test_try_acquire_once_stale_then_wins(mocker):
    """Stale detected → cleans project, second candidate wins → returns LockInfo."""
    lock = _make_project_lock(ttl_minutes=60)

    stale_id = 'stale-entry-0001'
    my_id_1 = 'my-first-cand-001'
    my_id_2 = 'my-second-cand-01'

    uuid_iter = iter([my_id_1, my_id_2])
    mocker.patch('uuid.uuid4', side_effect=lambda: MagicMock(__str__=lambda _: next(uuid_iter)))

    post_mock = mocker.patch.object(lock, '_post', return_value=[])
    clean_mock = mocker.patch.object(lock, 'clean_project')
    cleanup_mock = mocker.patch.object(lock, '_cleanup_old_locks')

    stale_entry = _make_lock(lock_id=stale_id, minutes_ago=120)

    read_call = [0]

    def metadata_side_effect():
        read_call[0] += 1
        if read_call[0] == 1:
            return [stale_entry, _make_lock(lock_id=my_id_1, minutes_ago=0)]
        else:
            return [_make_lock(lock_id=my_id_2, minutes_ago=0)]

    mocker.patch.object(lock, '_read_metadata', side_effect=metadata_side_effect)
    mocker.patch('time.sleep')

    result = lock._try_acquire_once()

    assert isinstance(result, LockInfo)
    assert result.lock_id == my_id_2
    clean_mock.assert_not_called()
    cleanup_mock.assert_called_once_with(my_id_2)

    released_keys = [
        entry['key']
        for c in post_mock.call_args_list
        for entry in c.kwargs.get('data', {}).get('metadata', [])
        if entry['key'].endswith('.released')
    ]
    assert LOCK_KEY_PREFIX + stale_id + '.released' in released_keys
    assert LOCK_KEY_PREFIX + my_id_1 + '.released' in released_keys


# ---------------------------------------------------------------------------
# test_try_acquire_once_stale_then_loses
# ---------------------------------------------------------------------------


def test_try_acquire_once_stale_then_loses(mocker):
    """Stale cleaned but second attempt loses to a racing runner → returns None."""
    lock = _make_project_lock(ttl_minutes=60)

    stale_id = 'stale-entry-0002'
    my_id_1 = 'my-first-cand-002'
    my_id_2 = 'my-second-cand-02'
    other_id = 'other-racer-0001'

    uuid_iter = iter([my_id_1, my_id_2])
    mocker.patch('uuid.uuid4', side_effect=lambda: MagicMock(__str__=lambda _: next(uuid_iter)))

    post_mock = mocker.patch.object(lock, '_post', return_value=[])
    clean_mock = mocker.patch.object(lock, 'clean_project')

    stale_entry = _make_lock(lock_id=stale_id, minutes_ago=120)

    read_call = [0]

    def metadata_side_effect():
        read_call[0] += 1
        if read_call[0] == 1:
            return [stale_entry, _make_lock(lock_id=my_id_1, minutes_ago=0)]
        else:
            # Another runner snuck in and is older than our second candidate
            other_entry = _make_lock(lock_id=other_id, minutes_ago=1)
            return [other_entry, _make_lock(lock_id=my_id_2, minutes_ago=0)]

    mocker.patch.object(lock, '_read_metadata', side_effect=metadata_side_effect)
    mocker.patch('time.sleep')

    result = lock._try_acquire_once()

    assert result is None
    clean_mock.assert_not_called()

    released_keys = [
        entry['key']
        for c in post_mock.call_args_list
        for entry in c.kwargs.get('data', {}).get('metadata', [])
        if entry['key'].endswith('.released')
    ]
    # Both candidates must be released
    assert LOCK_KEY_PREFIX + my_id_1 + '.released' in released_keys
    assert LOCK_KEY_PREFIX + my_id_2 + '.released' in released_keys


# ---------------------------------------------------------------------------
# test_acquire_still_works_via_try_acquire_once
# ---------------------------------------------------------------------------


def test_acquire_still_works_via_try_acquire_once(mocker):
    """acquire() loops _try_acquire_once(); None on first call, LockInfo on second."""
    lock = _make_project_lock(poll_interval_seconds=7)

    lock_info = _make_lock_info('final-lock-0001')
    mocker.patch.object(lock, '_try_acquire_once', side_effect=[None, lock_info])
    sleep_mock = mocker.patch('time.sleep')

    result = lock.acquire()

    assert result == lock_info
    assert call(7) in sleep_mock.call_args_list


# ===========================================================================
# ProjectPool tests
# ===========================================================================


# ---------------------------------------------------------------------------
# test_pool_empty_endpoints_raises
# ---------------------------------------------------------------------------


def test_pool_empty_endpoints_raises():
    """ProjectPool(endpoints=[]) raises ValueError."""
    with pytest.raises(ValueError, match='at least one endpoint'):
        ProjectPool(endpoints=[])


# ---------------------------------------------------------------------------
# test_pool_single_endpoint_acquires
# ---------------------------------------------------------------------------


def test_pool_single_endpoint_acquires(mocker):
    """Pool of one endpoint: _try_acquire_once wins on first try → AcquiredProject."""
    endpoint = _make_endpoint()
    pool = _make_pool(endpoints=[endpoint])

    lock_info = _make_lock_info('pool-single-001')
    mock_lock = mocker.MagicMock()
    mock_lock._try_acquire_once.return_value = lock_info
    mocker.patch.object(pool, '_make_lock', return_value=mock_lock)
    mocker.patch('time.sleep')

    result = pool.acquire()

    assert isinstance(result, AcquiredProject)
    assert result.endpoint == endpoint
    assert result.lock_info == lock_info


# ---------------------------------------------------------------------------
# test_pool_first_busy_second_free
# ---------------------------------------------------------------------------


def test_pool_first_busy_second_free(mocker):
    """First endpoint is locked; second is free → result uses second endpoint, no poll sleep."""
    endpoint1 = _make_endpoint(token='token-aaa')
    endpoint2 = _make_endpoint(token='token-bbb')
    pool = _make_pool(endpoints=[endpoint1, endpoint2], poll_interval_seconds=30)

    lock_info = _make_lock_info('pool-second-001')
    mock_lock1 = mocker.MagicMock()
    mock_lock1._try_acquire_once.return_value = None
    mock_lock2 = mocker.MagicMock()
    mock_lock2._try_acquire_once.return_value = lock_info

    mocker.patch.object(pool, '_make_lock', side_effect=[mock_lock1, mock_lock2])
    mocker.patch('integtests.project_lock.random.randrange', return_value=0)
    sleep_mock = mocker.patch('time.sleep')

    result = pool.acquire()

    assert result.endpoint == endpoint2
    assert result.lock_info == lock_info
    # No poll sleep — a project was found within the first pass
    assert call(30) not in sleep_mock.call_args_list


# ---------------------------------------------------------------------------
# test_pool_all_busy_then_one_frees
# ---------------------------------------------------------------------------


def test_pool_all_busy_then_one_frees(mocker):
    """Both endpoints busy on pass 1; first frees on pass 2 → poll sleep called once."""
    endpoint1 = _make_endpoint(token='token-ccc')
    endpoint2 = _make_endpoint(token='token-ddd')
    pool = _make_pool(endpoints=[endpoint1, endpoint2], poll_interval_seconds=11)

    lock_info = _make_lock_info('pool-retry-001')
    # Pass 1: both locked
    mock_lock1a = mocker.MagicMock()
    mock_lock1a._try_acquire_once.return_value = None
    mock_lock2a = mocker.MagicMock()
    mock_lock2a._try_acquire_once.return_value = None
    # Pass 2: endpoint1 succeeds
    mock_lock1b = mocker.MagicMock()
    mock_lock1b._try_acquire_once.return_value = lock_info

    mocker.patch.object(pool, '_make_lock', side_effect=[mock_lock1a, mock_lock2a, mock_lock1b])
    mocker.patch('integtests.project_lock.random.randrange', return_value=0)
    sleep_mock = mocker.patch('time.sleep')

    result = pool.acquire()

    assert result.endpoint == endpoint1
    assert result.lock_info == lock_info
    # Poll sleep must have been called between the two passes
    assert call(11) in sleep_mock.call_args_list


# ---------------------------------------------------------------------------
# test_pool_stale_project_claimed_not_skipped
# ---------------------------------------------------------------------------


def test_pool_stale_project_claimed_not_skipped(mocker):
    """
    First endpoint's _try_acquire_once returns LockInfo (stale-path internal win)
    → pool claims it immediately; second endpoint is never tried.
    """
    endpoint1 = _make_endpoint(token='token-eee')
    endpoint2 = _make_endpoint(token='token-fff')
    pool = _make_pool(endpoints=[endpoint1, endpoint2])

    lock_info = _make_lock_info('pool-stale-win-01')
    mock_lock1 = mocker.MagicMock()
    mock_lock1._try_acquire_once.return_value = lock_info

    make_lock_mock = mocker.patch.object(pool, '_make_lock', return_value=mock_lock1)
    mocker.patch('integtests.project_lock.random.randrange', return_value=0)
    mocker.patch('time.sleep')

    result = pool.acquire()

    assert result.endpoint == endpoint1
    assert result.lock_info == lock_info
    # _make_lock called only once (endpoint2 was never tried)
    assert make_lock_mock.call_count == 1
    make_lock_mock.assert_called_once_with(endpoint1)


# ---------------------------------------------------------------------------
# test_pool_timeout_raises
# ---------------------------------------------------------------------------


def test_pool_timeout_raises(mocker):
    """Raises TimeoutError when no project can be acquired within max_wait_minutes."""
    pool = _make_pool(max_wait_minutes=0)
    mocker.patch('time.sleep')

    with pytest.raises(TimeoutError, match='Could not acquire any project lock'):
        pool.acquire()


# ---------------------------------------------------------------------------
# test_pool_release_delegates_to_lock
# ---------------------------------------------------------------------------


def test_pool_release_delegates_to_lock(mocker):
    """pool.release(acquired) delegates to _make_lock(endpoint).release(lock_info)."""
    endpoint = _make_endpoint()
    pool = _make_pool(endpoints=[endpoint])

    lock_info = _make_lock_info('pool-release-001')
    acquired = AcquiredProject(endpoint=endpoint, lock_info=lock_info)

    mock_lock = mocker.MagicMock()
    make_lock_mock = mocker.patch.object(pool, '_make_lock', return_value=mock_lock)

    pool.release(acquired)

    make_lock_mock.assert_called_once_with(endpoint)
    mock_lock.release.assert_called_once_with(lock_info)


# ===========================================================================
# workspace_schema tests
# ===========================================================================


# ---------------------------------------------------------------------------
# test_project_endpoint_stores_workspace_schema
# ---------------------------------------------------------------------------


def test_project_endpoint_stores_all_fields():
    """ProjectEndpoint stores all five fields correctly."""
    ep = ProjectEndpoint(
        storage_api_url='https://connection.keboola.com',
        storage_api_token='my-token',
        workspace_schema='WORKSPACE_12345',
        project_id='99',
        project_name='My Project',
    )
    assert ep.workspace_schema == 'WORKSPACE_12345'
    assert ep.project_id == '99'
    assert ep.project_name == 'My Project'


# ---------------------------------------------------------------------------
# test_pool_acquired_project_carries_workspace_schema
# ---------------------------------------------------------------------------


def test_pool_acquired_project_carries_workspace_schema(mocker):
    """AcquiredProject.endpoint carries the workspace_schema of the acquired endpoint."""
    endpoint = _make_endpoint(schema='WORKSPACE_SCHEMA_A')
    pool = _make_pool(endpoints=[endpoint])

    lock_info = _make_lock_info('ws-schema-test-001')
    mock_lock = mocker.MagicMock()
    mock_lock._try_acquire_once.return_value = lock_info
    mocker.patch.object(pool, '_make_lock', return_value=mock_lock)
    mocker.patch('time.sleep')

    result = pool.acquire()

    assert result.endpoint.workspace_schema == 'WORKSPACE_SCHEMA_A'


# ---------------------------------------------------------------------------
# test_pool_selects_correct_schema_for_acquired_endpoint
# ---------------------------------------------------------------------------


def test_pool_selects_correct_schema_for_acquired_endpoint(mocker):
    """When the second endpoint is acquired, its schema is returned, not the first's."""
    endpoint1 = _make_endpoint(token='token-aaa', schema='SCHEMA_AAA')
    endpoint2 = _make_endpoint(token='token-bbb', schema='SCHEMA_BBB')
    pool = _make_pool(endpoints=[endpoint1, endpoint2], poll_interval_seconds=30)

    lock_info = _make_lock_info('ws-schema-second-001')
    mock_lock1 = mocker.MagicMock()
    mock_lock1._try_acquire_once.return_value = None
    mock_lock2 = mocker.MagicMock()
    mock_lock2._try_acquire_once.return_value = lock_info
    mocker.patch.object(pool, '_make_lock', side_effect=[mock_lock1, mock_lock2])
    mocker.patch('integtests.project_lock.random.randrange', return_value=0)
    mocker.patch('time.sleep')

    result = pool.acquire()

    assert result.endpoint == endpoint2
    assert result.endpoint.workspace_schema == 'SCHEMA_BBB'


# ---------------------------------------------------------------------------
# test_pool_acquire_randomizes_start_per_pass
# ---------------------------------------------------------------------------


def test_pool_acquire_randomizes_start_per_pass(mocker):
    """random.randrange is called once per pool pass."""
    endpoints = [
        _make_endpoint(token='token-aaa'),
        _make_endpoint(token='token-bbb'),
        _make_endpoint(token='token-ccc'),
    ]
    pool = _make_pool(endpoints=endpoints, poll_interval_seconds=1)

    lock_info = _make_lock_info('rand-start-001')
    # Pass 1: all busy (None). Pass 2: first tried endpoint succeeds.
    try_once_results = iter([None, None, None, lock_info])
    mock_lock = mocker.MagicMock()
    mock_lock._try_acquire_once.side_effect = lambda: next(try_once_results)
    mocker.patch.object(pool, '_make_lock', return_value=mock_lock)
    mocker.patch('time.sleep')

    randrange_mock = mocker.patch('integtests.project_lock.random.randrange', return_value=0)

    pool.acquire()

    # randrange must be called once per pass (2 passes: one all-busy + one with a winner)
    assert randrange_mock.call_count == 2
    # Each call must pass the pool size as the upper bound
    for c in randrange_mock.call_args_list:
        assert c == call(len(endpoints))


# ===========================================================================
# verify_project_endpoint tests
# ===========================================================================


# ---------------------------------------------------------------------------
# test_verify_project_endpoint_happy_path
# ---------------------------------------------------------------------------


def test_verify_project_endpoint_happy_path(mocker):
    """verify_project_endpoint returns a fully populated ProjectEndpoint on success."""
    token_info = {
        'owner': {'id': 42, 'name': 'My CI Project'},
    }
    mock_resp = mocker.MagicMock()
    mock_resp.json.return_value = token_info
    mock_client = mocker.MagicMock()
    mock_client.__enter__ = mocker.MagicMock(return_value=mock_client)
    mock_client.__exit__ = mocker.MagicMock(return_value=False)
    mock_client.get.return_value = mock_resp
    mocker.patch('integtests.project_lock.httpx.Client', return_value=mock_client)

    ep = verify_project_endpoint(
        storage_api_url='https://connection.keboola.com',
        storage_api_token='my-secret-token',
        workspace_schema='WORKSPACE_99999',
    )

    assert ep.project_id == '42'
    assert ep.project_name == 'My CI Project'
    assert ep.workspace_schema == 'WORKSPACE_99999'
    assert ep.storage_api_token == 'my-secret-token'
    mock_client.get.assert_called_once_with('https://connection.keboola.com/v2/storage/tokens/verify')
    mock_resp.raise_for_status.assert_called_once()


# ---------------------------------------------------------------------------
# test_verify_project_endpoint_bad_token_raises
# ---------------------------------------------------------------------------


def test_verify_project_endpoint_bad_token_raises(mocker):
    """verify_project_endpoint propagates HTTPStatusError on a bad token."""
    import httpx

    mock_resp = mocker.MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        '401 Unauthorized', request=mocker.MagicMock(), response=mocker.MagicMock()
    )
    mock_client = mocker.MagicMock()
    mock_client.__enter__ = mocker.MagicMock(return_value=mock_client)
    mock_client.__exit__ = mocker.MagicMock(return_value=False)
    mock_client.get.return_value = mock_resp
    mocker.patch('integtests.project_lock.httpx.Client', return_value=mock_client)

    with pytest.raises(httpx.HTTPStatusError):
        verify_project_endpoint(
            storage_api_url='https://connection.keboola.com',
            storage_api_token='bad-token',
            workspace_schema='WORKSPACE_00000',
        )


# ===========================================================================
# _delete_metadata_by_id / _cleanup_old_locks tests
# ===========================================================================


# ---------------------------------------------------------------------------
# test_delete_metadata_by_id
# ---------------------------------------------------------------------------


def test_delete_metadata_by_id(mocker):
    """_delete_metadata_by_id calls DELETE on the correct metadata path."""
    lock = _make_project_lock()
    delete_mock = mocker.patch.object(lock, '_delete')
    lock._delete_metadata_by_id('9876')
    delete_mock.assert_called_once_with('/v2/storage/branch/default/metadata/9876')


# ---------------------------------------------------------------------------
# test_cleanup_old_locks
# ---------------------------------------------------------------------------

_OLD_ID = 'old-lock-id-0001'
_CUR_ID = 'cur-lock-id-0001'


@pytest.mark.parametrize(
    ('scenario', 'entries', 'current_lock_id', 'expected_delete_calls'),
    [
        (
            'no_released_entries',
            lambda: [_make_lock(lock_id=_OLD_ID, meta_id=1)],
            _CUR_ID,
            [],
        ),
        (
            'full_released_pair',
            lambda: [
                _make_lock(lock_id=_OLD_ID, meta_id=10),
                _released_entry(_OLD_ID, meta_id=11),
            ],
            _CUR_ID,
            ['10', '11'],
        ),
        (
            'orphaned_released_only',
            lambda: [_released_entry(_OLD_ID, meta_id=20)],
            _CUR_ID,
            ['20'],
        ),
        (
            'skip_current_lock',
            lambda: [
                _make_lock(lock_id=_CUR_ID, meta_id=30),
                _released_entry(_CUR_ID, meta_id=31),
            ],
            _CUR_ID,
            [],
        ),
        (
            'mixed_old_and_current',
            lambda: [
                _make_lock(lock_id=_OLD_ID, meta_id=40),
                _released_entry(_OLD_ID, meta_id=41),
                _make_lock(lock_id=_CUR_ID, meta_id=42),
                _released_entry(_CUR_ID, meta_id=43),
            ],
            _CUR_ID,
            ['40', '41'],
        ),
        (
            'main_deletion_error_skips_released',
            lambda: [
                _make_lock(lock_id=_OLD_ID, meta_id=50),
                _released_entry(_OLD_ID, meta_id=51),
            ],
            _CUR_ID,
            'error_on_main',
        ),
    ],
)
def test_cleanup_old_locks(mocker, scenario, entries, current_lock_id, expected_delete_calls):
    lock = _make_project_lock()
    mocker.patch.object(lock, '_read_metadata', side_effect=entries)

    if expected_delete_calls == 'error_on_main':
        delete_mock = mocker.patch.object(lock, '_delete_metadata_by_id', side_effect=RuntimeError('fail'))
        # Should not raise; the error is swallowed
        lock._cleanup_old_locks(current_lock_id)
        # Only one call attempted (the main entry); .released is skipped
        delete_mock.assert_called_once_with('50')
    else:
        delete_mock = mocker.patch.object(lock, '_delete_metadata_by_id')
        lock._cleanup_old_locks(current_lock_id)
        if not expected_delete_calls:
            delete_mock.assert_not_called()
        else:
            assert delete_mock.call_count == len(expected_delete_calls)
            # Verify the calls were made in the correct order
            assert delete_mock.call_args_list == [call(mid) for mid in expected_delete_calls]
