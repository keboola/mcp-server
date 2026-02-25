"""
Distributed project lock backed by Keboola branch metadata.

Prevents concurrent CI runners from corrupting shared integration-test data via
a write-and-verify window + oldest-timestamp-wins protocol.

Lock state is represented by two metadata keys per runner:
  Active   : KBC.integtest.lock.<uuid>               (JSON payload)
  Released : KBC.integtest.lock.<uuid>.released      (ISO timestamp string)

This file has no dependency on the keboola_mcp_server package — it only uses
httpx (already a project dependency) for direct Storage API calls.
"""

import json
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

LOG = logging.getLogger(__name__)

LOCK_KEY_PREFIX = 'KBC.integtest.lock.'
DEFAULT_TTL_MINUTES = 60
DEFAULT_POLL_INTERVAL_SECONDS = 30
DEFAULT_MAX_WAIT_MINUTES = 90
DEFAULT_ANTI_COLLISION_SECONDS = 3


@dataclass(frozen=True)
class LockInfo:
    lock_id: str
    acquired_at: datetime  # UTC, timezone-aware
    runner_info: str
    metadata_key: str  # 'KBC.integtest.lock.<lock_id>'


class ProjectLock:
    def __init__(
        self,
        storage_api_url: str,
        storage_api_token: str,
        ttl_minutes: int = DEFAULT_TTL_MINUTES,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        max_wait_minutes: int = DEFAULT_MAX_WAIT_MINUTES,
        anti_collision_seconds: int = DEFAULT_ANTI_COLLISION_SECONDS,
    ) -> None:
        self._base_url = storage_api_url.rstrip('/')
        self._token = storage_api_token
        self._ttl_minutes = ttl_minutes
        self._poll_interval_seconds = poll_interval_seconds
        self._max_wait_minutes = max_wait_minutes
        self._anti_collision_seconds = anti_collision_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self) -> LockInfo:
        """Block until this runner owns the project lock; return LockInfo."""
        deadline = datetime.now(timezone.utc).timestamp() + self._max_wait_minutes * 60
        runner_info = self._runner_info()

        while True:
            if datetime.now(timezone.utc).timestamp() > deadline:
                raise TimeoutError(f'Could not acquire project lock within {self._max_wait_minutes} minutes')

            lock_id = str(uuid.uuid4())
            acquired_at = datetime.now(timezone.utc)
            key = LOCK_KEY_PREFIX + lock_id
            payload = json.dumps(
                {
                    'lock_id': lock_id,
                    'acquired_at': acquired_at.isoformat(),
                    'runner_info': runner_info,
                }
            )
            LOG.info(f'[project_lock] Writing candidate lock {lock_id} (runner: {runner_info})')
            self._write_metadata({key: payload})

            # Anti-collision window: let concurrent writers finish their writes
            time.sleep(self._anti_collision_seconds)

            active = self._read_active_locks()

            # Determine winner: oldest acquired_at; on exact tie, lowest lock_id string
            winner = min(active, key=lambda li: (li.acquired_at, li.lock_id)) if active else None

            if winner is not None and winner.lock_id == lock_id:
                LOG.info(f'[project_lock] Acquired lock {lock_id}')
                return LockInfo(
                    lock_id=lock_id,
                    acquired_at=acquired_at,
                    runner_info=runner_info,
                    metadata_key=key,
                )

            if winner is not None and self._is_stale(winner):
                LOG.warning(
                    f'[project_lock] Stale lock detected: {winner.lock_id} '
                    f'(acquired_at={winner.acquired_at.isoformat()}). '
                    'Releasing stale entries and cleaning project.'
                )
                for stale in active:
                    if self._is_stale(stale):
                        self._release_lock_entry(stale.lock_id)
                self._clean_project()
                # Release our pending entry so we retry cleanly
                self._release_lock_entry(lock_id)
                time.sleep(2)
                continue

            # Another runner holds an active (non-stale) lock
            if winner is not None:
                expires_approx = winner.acquired_at.timestamp() + self._ttl_minutes * 60
                expires_str = datetime.fromtimestamp(expires_approx, tz=timezone.utc).isoformat()
                LOG.info(
                    f'[project_lock] Lock held by {winner.runner_info} '
                    f'(id={winner.lock_id}), expires ~{expires_str}. '
                    f'Releasing our candidate and waiting {self._poll_interval_seconds}s.'
                )
            else:
                LOG.info(f'[project_lock] No winner determined yet. ' f'Waiting {self._poll_interval_seconds}s.')

            # Withdraw our candidate so we don't block the winner
            self._release_lock_entry(lock_id)
            time.sleep(self._poll_interval_seconds)

    def release(self, lock: LockInfo) -> None:
        """Mark the lock as released by writing the .released key."""
        released_key = lock.metadata_key + '.released'
        LOG.info(f'[project_lock] Releasing lock {lock.lock_id}')
        self._write_metadata({released_key: datetime.now(timezone.utc).isoformat()})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_metadata(self, kv: dict[str, str]) -> None:
        payload = {'metadata': [{'key': k, 'value': v} for k, v in kv.items()]}
        self._post('/v2/storage/branch/default/metadata', data=payload)

    def _read_metadata(self) -> list[dict[str, Any]]:
        return self._get('/v2/storage/branch/default/metadata')

    def _read_active_locks(self) -> list[LockInfo]:
        """Return all active (not-released) lock entries, sorted by acquired_at ASC."""
        entries = self._read_metadata()

        # Build sets of known lock UUIDs and released UUIDs
        lock_entries: dict[str, dict[str, Any]] = {}  # lock_id -> raw metadata entry
        released_ids: set[str] = set()

        for entry in entries:
            key: str = entry.get('key', '')
            if not key.startswith(LOCK_KEY_PREFIX):
                continue
            suffix = key[len(LOCK_KEY_PREFIX) :]
            if suffix.endswith('.released'):
                released_ids.add(suffix[: -len('.released')])
            else:
                lock_entries[suffix] = entry

        active: list[LockInfo] = []
        for lock_id, entry in lock_entries.items():
            if lock_id in released_ids:
                continue
            try:
                data = json.loads(entry['value'])
                acquired_at = datetime.fromisoformat(data['acquired_at'])
                if acquired_at.tzinfo is None:
                    acquired_at = acquired_at.replace(tzinfo=timezone.utc)
                active.append(
                    LockInfo(
                        lock_id=lock_id,
                        acquired_at=acquired_at,
                        runner_info=data.get('runner_info', ''),
                        metadata_key=LOCK_KEY_PREFIX + lock_id,
                    )
                )
            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                LOG.warning(f'[project_lock] Skipping malformed lock entry {lock_id!r}: {exc}')

        return sorted(active, key=lambda li: (li.acquired_at, li.lock_id))

    def _is_stale(self, lock: LockInfo) -> bool:
        expiry = lock.acquired_at.timestamp() + self._ttl_minutes * 60
        return expiry <= datetime.now(timezone.utc).timestamp()

    def _release_lock_entry(self, lock_id: str) -> None:
        released_key = LOCK_KEY_PREFIX + lock_id + '.released'
        self._write_metadata({released_key: datetime.now(timezone.utc).isoformat()})

    def _clean_project(self) -> None:
        """Delete all buckets and component configurations from the project."""
        LOG.info('[project_lock] Cleaning project (deleting all buckets and configs)')

        # Delete all buckets (force=True also removes tables inside them)
        buckets = self._get('/v2/storage/buckets')
        for bucket in buckets:
            bucket_id = bucket['id']
            LOG.info(f'[project_lock] Deleting bucket {bucket_id}')
            self._delete(f'/v2/storage/buckets/{bucket_id}', force='true')

        # Delete all component configurations
        components = self._get('/v2/storage/branch/default/components', include='configurations')
        for component in components:
            comp_id = component['id']
            for cfg in component.get('configurations', []):
                cfg_id = cfg['id']
                LOG.info(f'[project_lock] Deleting config {comp_id}/{cfg_id}')
                # First delete moves to trash; second delete removes from trash
                self._delete(f'/v2/storage/components/{comp_id}/configs/{cfg_id}')
                self._delete(f'/v2/storage/components/{comp_id}/configs/{cfg_id}')

    @staticmethod
    def _runner_info() -> str:
        hostname = socket.gethostname()
        pid = os.getpid()
        base = f'{hostname}/{pid}'
        run_id = os.getenv('GITHUB_RUN_ID')
        if run_id:
            return f'CI={run_id} {base}'
        return base

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _client(self) -> httpx.Client:
        return httpx.Client(
            headers={'X-StorageApi-Token': self._token},
            timeout=30.0,
        )

    def _get(self, path: str, **params: Any) -> Any:
        with self._client() as client:
            resp = client.get(self._base_url + path, params=params or None)
            resp.raise_for_status()
            return resp.json()

    def _post(self, path: str, data: dict[str, Any]) -> Any:
        with self._client() as client:
            resp = client.post(self._base_url + path, json=data)
            resp.raise_for_status()
            return resp.json()

    def _delete(self, path: str, **params: Any) -> None:
        with self._client() as client:
            resp = client.delete(self._base_url + path, params=params or None)
            resp.raise_for_status()
