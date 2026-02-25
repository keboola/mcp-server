"""
Integration tests for integtests/project_lock.py.
These tests call the real Keboola Storage API and require:
  INTEGTEST_STORAGE_API_URL
  INTEGTEST_STORAGE_TOKEN
"""

import json
import socket
from datetime import datetime, timedelta, timezone

import pytest

from integtests.project_lock import LOCK_KEY_PREFIX, LockInfo, ProjectLock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_lock(storage_api_url: str, storage_api_token: str) -> ProjectLock:
    return ProjectLock(
        storage_api_url=storage_api_url,
        storage_api_token=storage_api_token,
        ttl_minutes=5,
        poll_interval_seconds=5,
        max_wait_minutes=10,
        anti_collision_seconds=2,
    )


def _read_branch_metadata(lock: ProjectLock) -> list[dict]:
    return lock._read_metadata()


def _cleanup_lock_keys(lock: ProjectLock, *lock_ids: str) -> None:
    """Release all provided lock IDs to leave branch metadata clean."""
    for lid in lock_ids:
        try:
            lock._release_lock_entry(lid)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures('env_file_loaded')
def test_lock_appears_in_branch_metadata(storage_api_url: str, storage_api_token: str) -> None:
    """After acquire(), the lock key is present in branch metadata."""
    lock = _make_lock(storage_api_url, storage_api_token)
    lock_info = lock.acquire()
    try:
        metadata = _read_branch_metadata(lock)
        keys = {entry['key'] for entry in metadata}
        assert lock_info.metadata_key in keys, f'Expected {lock_info.metadata_key!r} in branch metadata, found: {keys}'
    finally:
        lock.release(lock_info)


@pytest.mark.usefixtures('env_file_loaded')
def test_release_appears_in_branch_metadata(storage_api_url: str, storage_api_token: str) -> None:
    """After release(), the .released key is present in branch metadata."""
    lock = _make_lock(storage_api_url, storage_api_token)
    lock_info = lock.acquire()
    lock.release(lock_info)

    metadata = _read_branch_metadata(lock)
    keys = {entry['key'] for entry in metadata}
    released_key = lock_info.metadata_key + '.released'
    assert released_key in keys, f'Expected {released_key!r} in branch metadata after release, found: {keys}'


@pytest.mark.usefixtures('env_file_loaded')
def test_full_acquire_release_cycle(storage_api_url: str, storage_api_token: str) -> None:
    """Lock acquired then released; branch metadata reflects both transitions."""
    lock = _make_lock(storage_api_url, storage_api_token)

    # --- Acquire ---
    lock_info = lock.acquire()
    assert isinstance(lock_info, LockInfo)
    assert lock_info.lock_id
    assert lock_info.acquired_at.tzinfo is not None

    active_before = lock._read_active_locks()
    our_ids = {li.lock_id for li in active_before}
    assert lock_info.lock_id in our_ids

    runner_expected = f'{socket.gethostname()}/'
    assert runner_expected in lock_info.runner_info

    # --- Release ---
    lock.release(lock_info)

    active_after = lock._read_active_locks()
    remaining_ids = {li.lock_id for li in active_after}
    assert lock_info.lock_id not in remaining_ids


@pytest.mark.usefixtures('env_file_loaded')
def test_stale_lock_cleared_and_reacquired(storage_api_url: str, storage_api_token: str) -> None:
    """
    Manually write a lock entry with an old timestamp.
    acquire() should detect it as stale, release it, clean the project,
    and successfully acquire a fresh lock.
    """
    lock = _make_lock(storage_api_url, storage_api_token)

    # Write a stale lock entry directly (acquired 120 minutes ago, TTL=5 minutes)
    stale_id = 'stale-test-' + 'x' * 8
    stale_acquired_at = datetime.now(timezone.utc) - timedelta(minutes=120)
    stale_payload = json.dumps(
        {
            'lock_id': stale_id,
            'acquired_at': stale_acquired_at.isoformat(),
            'runner_info': 'stale-runner/0',
        }
    )
    lock._write_metadata({LOCK_KEY_PREFIX + stale_id: stale_payload})

    try:
        # acquire() should detect the stale lock, release it, and acquire a fresh one
        lock_info = lock.acquire()
        try:
            # Our fresh lock should now be active
            active = lock._read_active_locks()
            active_ids = {li.lock_id for li in active}
            assert lock_info.lock_id in active_ids

            # Stale lock should no longer be active
            assert stale_id not in active_ids

            # .released key for stale lock should be present
            metadata = _read_branch_metadata(lock)
            keys = {entry['key'] for entry in metadata}
            assert LOCK_KEY_PREFIX + stale_id + '.released' in keys
        finally:
            lock.release(lock_info)
    except Exception:
        # Ensure stale entry is cleaned up even on failure
        _cleanup_lock_keys(lock, stale_id)
        raise
