"""
Distributed project lock backed by Keboola branch metadata.

Prevents concurrent CI runners from corrupting shared integration-test data via
a write-and-verify window + oldest-timestamp-wins protocol.

Lock state is represented by two metadata keys per runner:
  Active   : KBC.integtest.lock.<uuid>               (JSON payload)
  Released : KBC.integtest.lock.<uuid>.released      (ISO timestamp string)

For a pool of projects, use ProjectPool, which tries each ProjectEndpoint in
order and returns the first one that can be locked.

This file has no dependency on the keboola_mcp_server package — it only uses
httpx (already a project dependency) for direct Storage API calls.
"""

import json
import logging
import os
import random
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


@dataclass(frozen=True)
class ProjectEndpoint:
    """A Keboola project identified by its Storage API URL, token, workspace schema, and metadata."""

    storage_api_url: str
    storage_api_token: str
    workspace_schema: str
    project_id: str
    project_name: str


@dataclass(frozen=True)
class AcquiredProject:
    """Result of acquiring a lock from a pool: which project was selected and its lock."""

    endpoint: ProjectEndpoint
    lock_info: LockInfo


def verify_project_endpoint(
    storage_api_url: str,
    storage_api_token: str,
    workspace_schema: str,
) -> ProjectEndpoint:
    """
    Verify a Storage API token and return a fully populated ProjectEndpoint.

    Calls GET /v2/storage/tokens/verify to confirm the token is valid and to
    fetch the project name and ID.  Raises httpx.HTTPStatusError if the token
    is invalid or the request fails.
    """
    base_url = storage_api_url.rstrip('/')
    with httpx.Client(headers={'X-StorageApi-Token': storage_api_token}, timeout=30.0) as client:
        resp = client.get(f'{base_url}/v2/storage/tokens/verify')
        resp.raise_for_status()
        token_info = resp.json()
    project_id = str(token_info['owner']['id'])
    project_name = token_info['owner']['name']
    LOG.info(f'[project_lock] Verified token ...{storage_api_token[-4:]} — project "{project_name}" ({project_id})')
    return ProjectEndpoint(
        storage_api_url=storage_api_url,
        storage_api_token=storage_api_token,
        workspace_schema=workspace_schema,
        project_id=project_id,
        project_name=project_name,
    )


class ProjectLock:
    def __init__(
        self,
        storage_api_url: str,
        storage_api_token: str,
        workspace_schema: str | None = None,
        ttl_minutes: int = DEFAULT_TTL_MINUTES,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        max_wait_minutes: int = DEFAULT_MAX_WAIT_MINUTES,
        anti_collision_seconds: int = DEFAULT_ANTI_COLLISION_SECONDS,
    ) -> None:
        self._base_url = storage_api_url.rstrip('/')
        self._token = storage_api_token
        self._workspace_schema = workspace_schema
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

        while True:
            if datetime.now(timezone.utc).timestamp() > deadline:
                raise TimeoutError(f'Could not acquire project lock within {self._max_wait_minutes} minutes')

            result = self._try_acquire_once()
            if result is not None:
                return result

            LOG.info(f'[project_lock] Waiting {self._poll_interval_seconds}s before retrying.')
            time.sleep(self._poll_interval_seconds)

    def release(self, lock: LockInfo) -> None:
        """Mark the lock as released by writing the .released key."""
        released_key = lock.metadata_key + '.released'
        LOG.info(f'[project_lock] Releasing lock {lock.lock_id}')
        self._write_metadata({released_key: datetime.now(timezone.utc).isoformat()})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _try_acquire_once(self) -> LockInfo | None:
        """
        Single non-looping acquisition attempt.

        Returns LockInfo if this runner wins the lock (including after cleaning
        a stale lock).  Returns None if another runner holds an active non-stale
        lock — signals the caller to try a different project or wait and retry.
        """
        runner_info = self._runner_info()
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
        winner = min(active, key=lambda li: (li.acquired_at, li.lock_id)) if active else None

        # Case 1: We are the winner
        if winner is not None and winner.lock_id == lock_id:
            LOG.info(f'[project_lock] Acquired lock {lock_id}')
            self._cleanup_old_locks(lock_id)
            self.clean_project()
            return LockInfo(
                lock_id=lock_id,
                acquired_at=acquired_at,
                runner_info=runner_info,
                metadata_key=key,
            )

        # Case 2: The winner is stale — clean up and retry immediately once
        if winner is not None and self._is_stale(winner):
            LOG.warning(
                f'[project_lock] Stale lock detected: {winner.lock_id} '
                f'(acquired_at={winner.acquired_at.isoformat()}). '
                'Releasing stale entries and cleaning project.'
            )
            for stale in active:
                if self._is_stale(stale):
                    self._release_lock_entry(stale.lock_id)
            # Release our pending entry and write a fresh candidate
            self._release_lock_entry(lock_id)
            time.sleep(2)

            lock_id2 = str(uuid.uuid4())
            acquired_at2 = datetime.now(timezone.utc)
            key2 = LOCK_KEY_PREFIX + lock_id2
            payload2 = json.dumps(
                {
                    'lock_id': lock_id2,
                    'acquired_at': acquired_at2.isoformat(),
                    'runner_info': runner_info,
                }
            )
            LOG.info(f'[project_lock] Post-stale-clean: writing candidate lock {lock_id2}')
            self._write_metadata({key2: payload2})
            time.sleep(self._anti_collision_seconds)

            active2 = self._read_active_locks()
            winner2 = min(active2, key=lambda li: (li.acquired_at, li.lock_id)) if active2 else None
            if winner2 is not None and winner2.lock_id == lock_id2:
                LOG.info(f'[project_lock] Acquired lock {lock_id2} after stale cleanup')
                self._cleanup_old_locks(lock_id2)
                return LockInfo(
                    lock_id=lock_id2,
                    acquired_at=acquired_at2,
                    runner_info=runner_info,
                    metadata_key=key2,
                )
            # Another runner raced us after the cleanup — withdraw and signal the caller
            self._release_lock_entry(lock_id2)
            return None

        # Case 3: Another runner holds an active (non-stale) lock
        if winner is not None:
            expires_approx = winner.acquired_at.timestamp() + self._ttl_minutes * 60
            expires_str = datetime.fromtimestamp(expires_approx, tz=timezone.utc).isoformat()
            LOG.info(
                f'[project_lock] Lock held by {winner.runner_info} '
                f'(id={winner.lock_id}), expires ~{expires_str}. '
                'Releasing our candidate.'
            )
        else:
            LOG.info('[project_lock] No winner determined yet. Releasing candidate.')
        self._release_lock_entry(lock_id)
        return None

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

    def _delete_metadata_by_id(self, metadata_id: str) -> None:
        """Delete a single branch metadata entry by its numeric Storage API id."""
        self._delete(f'/v2/storage/branch/default/metadata/{metadata_id}')

    def _cleanup_old_locks(self, current_lock_id: str) -> None:
        """
        Delete all released lock metadata entries to prevent metadata accumulation.

        Deletion order per pair: main entry first, then .released entry.
        This prevents transient reappearance of a released lock as active.
        Errors are swallowed so cleanup never breaks lock acquisition.
        """
        try:
            entries = self._read_metadata()
        except Exception as exc:
            LOG.warning(f'[project_lock] _cleanup_old_locks: failed to read metadata: {exc}')
            return

        main_entries: dict[str, dict[str, Any]] = {}  # lock_id -> raw entry
        released_entries: dict[str, dict[str, Any]] = {}  # lock_id -> raw entry

        for entry in entries:
            key: str = entry.get('key', '')
            if not key.startswith(LOCK_KEY_PREFIX):
                continue
            suffix = key[len(LOCK_KEY_PREFIX) :]
            if suffix.endswith('.released'):
                lock_id = suffix[: -len('.released')]
                released_entries[lock_id] = entry
            else:
                main_entries[suffix] = entry

        for lock_id, released_entry in released_entries.items():
            if lock_id == current_lock_id:
                continue  # never touch the active lock
            main_entry = main_entries.get(lock_id)
            if main_entry is not None:
                # Delete main entry FIRST (safety: prevents transient reactivation)
                try:
                    LOG.info(f'[project_lock] Cleaning up released lock {lock_id} (main)')
                    self._delete_metadata_by_id(str(main_entry['id']))
                except Exception as exc:
                    LOG.warning(
                        f'[project_lock] Failed to delete main lock entry {lock_id}: {exc}. '
                        'Skipping .released deletion to avoid reactivating the lock.'
                    )
                    continue
                # Delete .released entry SECOND — only if main was successfully deleted
                try:
                    LOG.info(f'[project_lock] Cleaning up released lock {lock_id} (.released)')
                    self._delete_metadata_by_id(str(released_entry['id']))
                except Exception as exc:
                    LOG.warning(f'[project_lock] Failed to delete .released entry {lock_id}: {exc}')
            else:
                # Orphaned .released entry — main was already deleted or never written
                try:
                    LOG.info(f'[project_lock] Cleaning up orphaned .released entry {lock_id}')
                    self._delete_metadata_by_id(str(released_entry['id']))
                except Exception as exc:
                    LOG.warning(f'[project_lock] Failed to delete orphaned .released entry {lock_id}: {exc}')

    def _find_sandboxes_config_id_by_workspace_schema(self, workspace_schema: str) -> str | None:
        """
        Returns the configurationId of the keboola.sandboxes config whose workspace
        has the given schema name, or None if not found.
        """
        try:
            workspaces = self._get('/v2/storage/branch/default/workspaces')
            for workspace in workspaces:
                schema = workspace.get('connection', {}).get('schema')
                if schema == workspace_schema:
                    config_id = workspace.get('configurationId')
                    if config_id is not None:
                        return str(config_id)
        except Exception as exc:
            LOG.warning(f'[project_lock] Failed to look up workspace schema {workspace_schema!r}: {exc}')
        return None

    def clean_project(self) -> None:
        """Delete all buckets and component configurations from the project."""
        LOG.info('[project_lock] Cleaning project (deleting all buckets and configs)')

        # Find the keboola.sandboxes configuration to keep — the one whose workspace matches
        # the integration test workspace schema.
        sandboxes_config_to_keep: str | None = None
        if self._workspace_schema:
            sandboxes_config_to_keep = self._find_sandboxes_config_id_by_workspace_schema(
                self._workspace_schema
            )
            if sandboxes_config_to_keep:
                LOG.info(
                    f'[project_lock] Will keep keboola.sandboxes config {sandboxes_config_to_keep} '
                    f'(workspace schema: {self._workspace_schema})'
                )
            else:
                LOG.warning(
                    f'[project_lock] No workspace found for schema {self._workspace_schema!r}; '
                    'all keboola.sandboxes configs will be deleted'
                )

        # Delete all buckets (force=True also removes tables inside them)
        buckets = self._get('/v2/storage/buckets')
        for bucket in buckets:
            bucket_id = bucket['id']
            LOG.info(f'[project_lock] Deleting bucket {bucket_id}')
            self._delete(f'/v2/storage/buckets/{bucket_id}', force='true')

        # Delete all component configurations
        components = self._get('/v2/storage/branch/default/components', include='configuration')
        for component in components:
            comp_id = component['id']
            for cfg in component.get('configurations', []):
                cfg_id = cfg['id']

                if comp_id == 'keboola.sandboxes' and cfg_id == sandboxes_config_to_keep:
                    LOG.info(f'[project_lock] Keeping keboola.sandboxes config {cfg_id} (integration test workspace)')
                    continue

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


class ProjectPool:
    """
    Manages a pool of Keboola projects for integration tests.

    Tries each endpoint in order on every acquisition pass; returns the first
    project whose lock can be acquired.  If all projects are held by active
    runners, sleeps poll_interval_seconds and retries the whole pool.
    Raises TimeoutError after max_wait_minutes.

    Stale locks on any project are detected and cleaned automatically before
    claiming that project (handled inside ProjectLock._try_acquire_once).
    """

    def __init__(
        self,
        endpoints: list[ProjectEndpoint],
        ttl_minutes: int = DEFAULT_TTL_MINUTES,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        max_wait_minutes: int = DEFAULT_MAX_WAIT_MINUTES,
        anti_collision_seconds: int = DEFAULT_ANTI_COLLISION_SECONDS,
    ) -> None:
        if not endpoints:
            raise ValueError('ProjectPool requires at least one endpoint')
        self._endpoints = endpoints
        self._ttl_minutes = ttl_minutes
        self._poll_interval_seconds = poll_interval_seconds
        self._max_wait_minutes = max_wait_minutes
        self._anti_collision_seconds = anti_collision_seconds

    def acquire(self) -> AcquiredProject:
        """
        Try each endpoint in order; return the first one successfully locked.
        Retries the whole pool until max_wait_minutes is exceeded.
        """
        deadline = datetime.now(timezone.utc).timestamp() + self._max_wait_minutes * 60

        while True:
            if datetime.now(timezone.utc).timestamp() > deadline:
                raise TimeoutError(
                    f'Could not acquire any project lock within {self._max_wait_minutes} minutes '
                    f'(pool size: {len(self._endpoints)})'
                )

            start = random.randrange(len(self._endpoints))
            rotated = self._endpoints[start:] + self._endpoints[:start]
            for endpoint in rotated:
                LOG.info(
                    f'[project_pool] Trying to acquire lock for '
                    f'"{endpoint.project_name}" ({endpoint.project_id}) (...{endpoint.storage_api_token[-4:]})'
                )
                lock_info = self._make_lock(endpoint)._try_acquire_once()
                if lock_info is not None:
                    LOG.info(
                        f'[project_pool] Acquired project '
                        f'"{endpoint.project_name}" ({endpoint.project_id}) (...{endpoint.storage_api_token[-4:]})'
                    )
                    return AcquiredProject(endpoint=endpoint, lock_info=lock_info)

            LOG.info(
                f'[project_pool] All {len(self._endpoints)} projects busy. '
                f'Sleeping {self._poll_interval_seconds}s before retry.'
            )
            time.sleep(self._poll_interval_seconds)

    def release(self, acquired: AcquiredProject) -> None:
        """Release the lock held on acquired.endpoint."""
        LOG.info(
            f'[project_pool] Releasing lock on '
            f'"{acquired.endpoint.project_name}" ({acquired.endpoint.project_id})'
            f' (...{acquired.endpoint.storage_api_token[-4:]})'
        )
        self._make_lock(acquired.endpoint).release(acquired.lock_info)

    def _make_lock(self, endpoint: ProjectEndpoint) -> ProjectLock:
        return ProjectLock(
            storage_api_url=endpoint.storage_api_url,
            storage_api_token=endpoint.storage_api_token,
            workspace_schema=endpoint.workspace_schema,
            ttl_minutes=self._ttl_minutes,
            poll_interval_seconds=self._poll_interval_seconds,
            max_wait_minutes=self._max_wait_minutes,
            anti_collision_seconds=self._anti_collision_seconds,
        )
