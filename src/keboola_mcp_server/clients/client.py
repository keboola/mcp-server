"""Keboola Storage API client wrapper."""

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypeVar, cast
from urllib.parse import urlparse, urlunparse

import httpx

from keboola_mcp_server.clients.ai_service import AIServiceClient
from keboola_mcp_server.clients.auth_bridge import (
    StorageTokenExchangeError,
    StorageTokenResolver,
    is_programmatic_token,
    strip_bearer,
)
from keboola_mcp_server.clients.base import normalize_storage_api_url, read_service_account_jwt
from keboola_mcp_server.clients.data_science import DataScienceClient
from keboola_mcp_server.clients.encryption import EncryptionClient
from keboola_mcp_server.clients.jobs_queue import JobsQueueClient
from keboola_mcp_server.clients.metastore import MetastoreClient
from keboola_mcp_server.clients.scheduler import SchedulerClient
from keboola_mcp_server.clients.storage import AsyncStorageClient, JsonDict
from keboola_mcp_server.clients.sync_actions import SyncActionsClient
from keboola_mcp_server.config import is_same_stack

LOG = logging.getLogger(__name__)

T = TypeVar('T')

# Input types for the global search endpoint parameters
BranchType = Literal['production', 'development']


ORCHESTRATOR_COMPONENT_ID = 'keboola.orchestrator'
CONDITIONAL_FLOW_COMPONENT_ID = 'keboola.flow'
DATA_APP_COMPONENT_ID = 'keboola.data-apps'
FlowType = Literal['keboola.flow', 'keboola.orchestrator']
FLOW_TYPES: Sequence[FlowType] = (CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID)


def get_metadata_property(
    metadata: list[Mapping[str, Any]],
    key: str,
    *,
    provider: str | None = None,
    preferred_providers: list[str] | None = None,
    default: T | None = None,
) -> T | None:
    """
    Gets the value of a metadata property based on the provided key and optional provider. If multiple metadata entries
    exist with the same key, the most recent one is returned.

    :param metadata: A list of metadata entries.
    :param key: The metadata property key to search for.
    :param provider: Specifies the metadata provider name to filter by.
    :param preferred_providers: Specifies a list of preferred metadata providers to order the metadata items by.
    :param default: The default value to return if the metadata property is not found.

    :return: The value of the most recent matching metadata entry if found, or None otherwise.
    """
    if provider and preferred_providers:
        raise ValueError('Specifying both provider and preferred_providers makes no sense.')

    def _sort_key(m: Mapping[str, Any]) -> tuple[Any, ...]:
        # TODO: ideally we should first convert the timestamps to UTC
        if preferred_providers:
            if (_p := m.get('provider')) and _p in preferred_providers:
                _pidx = preferred_providers.index(_p)
            else:
                _pidx = len(preferred_providers)
            return -1 * _pidx, m.get('timestamp') or ''
        else:
            return (m.get('timestamp') or '',)

    filtered = [
        m for m in metadata if m['key'] == key and (not provider or ('provider' in m and m['provider'] == provider))
    ]
    item = max(filtered, key=_sort_key, default=None)
    value = item.get('value') if item else None
    return value if value is not None else default


class KeboolaClient:
    """Class holding clients for Keboola APIs: Storage API, Job Queue API, and AI Service."""

    STATE_KEY = 'sapi_client'

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'KeboolaClient':
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, KeboolaClient), f'Expected KeboolaClient, got: {instance}'
        return instance

    @classmethod
    async def create(
        cls,
        *,
        storage_api_url: str,
        storage_token: str,
        bearer_token: str | None = None,
        branch_id: str | None = None,
        project_id: str | int | None = None,
        token_resolver: StorageTokenResolver | None = None,
        headers: Mapping[str, Any] | None = None,
        readonly: bool | None = None,
        own_stack_storage_api_url: str | None = None,
    ) -> 'KeboolaClient':
        """
        Builds a client from the credential a caller was handed, classifying it and -- when it is
        a programmatic token narrowed to one project -- exchanging it for that project's legacy
        Storage token.

        `__init__` takes the two *already-classified* credentials (`legacy_storage_token` and
        `bearer_token`); this factory is the one place that decides which is which:

        * A programmatic token (``kbc_at_``/``kbc_pat_``) is forwarded downstream as a Bearer for
          Storage/data-science/scheduler/metastore. Jobs-queue/AI-service/sync-actions re-send
          whatever they receive to Storage as a legacy ``X-StorageApi-Token``, so that Bearer 401s
          there (INC-02580 / SUPPORT-17416); with `project_id` and a `token_resolver` we exchange
          it for the real per-project Storage token via the auth-bridge. On failure we keep the raw
          token, degrading to the pre-existing 401 rather than breaking session creation.
        * Anything else (a legacy project-bound Storage token, or an OAuth session whose bearer is
          supplied separately) is passed through untouched.

        :param storage_token: The credential the caller presented (legacy Storage token, or a
            programmatic kbc_at_/kbc_pat_ token, with or without a `Bearer ` scheme).
        :param bearer_token: An OAuth bearer, when the session has one alongside a legacy Storage
            token. Ignored (overwritten) for a programmatic `storage_token`.
        :param project_id: The project this client is narrowed to, if any. Only used to pick the
            project for the legacy-token exchange -- the caller still owns the X-KBC-ProjectId
            header.
        :param token_resolver: The auth-bridge resolver, or None to skip the exchange (a local/
            non-deployed server, or a request that must add no auth round-trips -- see
            `SessionStateMiddleware.on_request`'s `is_list`).
        """
        if not storage_api_url:
            raise ValueError('Storage API URL is not provided.')
        if not storage_token:
            raise ValueError('Storage API token is not provided.')

        legacy_storage_token = storage_token
        if is_programmatic_token(storage_token):
            bearer_token = strip_bearer(storage_token)
        if token_resolver is not None and project_id is not None and is_programmatic_token(bearer_token):
            try:
                # int() stays INSIDE the try: project_id can come from the caller-supplied
                # X-KBC-ProjectId header, and a non-numeric value must degrade to the warning
                # below, not 500 the request.
                legacy_storage_token = await token_resolver.resolve(
                    subject_token=cast(str, bearer_token), project_id=int(project_id)
                )
            except (StorageTokenExchangeError, OSError, ValueError) as e:
                # OSError/ValueError: the projected ServiceAccount JWT file is missing or empty.
                LOG.warning(
                    'Could not resolve a legacy Storage token for jobs-queue/AI-service/sync-actions '
                    f'calls on project {project_id}; those calls will keep 401ing: {e}',
                    exc_info=True,
                )

        client = cls(
            storage_api_url=storage_api_url,
            legacy_storage_token=legacy_storage_token,
            bearer_token=bearer_token,
            headers=headers,
            readonly=readonly,
            own_stack_storage_api_url=own_stack_storage_api_url,
        )
        return await client.with_branch_id(branch_id)

    async def with_branch_id(self, branch_id: str | None) -> 'KeboolaClient':
        """
        Gets a KeboolaClient configured for the given branch. It verifies that the branch exists
        and normalizes the default-branch ID to None.
        """
        if branch_id == self.branch_id:
            return self
        elif not branch_id:
            return KeboolaClient(
                storage_api_url=self.storage_api_url,
                legacy_storage_token=self.legacy_storage_token,
                bearer_token=self._bearer_token,
                branch_id=None,
                headers=self._headers,
                readonly=self.readonly,
                own_stack_storage_api_url=self._own_stack_storage_api_url,
            )
        else:
            try:
                detail = await self.storage_client.dev_branch_detail(branch_id)
                is_default = detail.get('isDefault') is True

            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    message = f'Branch "{branch_id}" not found'
                    LOG.error(f'{message}: {exc.response.text}')
                    raise httpx.HTTPStatusError(message, request=exc.request, response=exc.response) from exc
                else:
                    LOG.error(f'Failed to get details of "{branch_id}" branch: {exc.response.text}')
                    raise

            # Converts the branch id referring to the main/production branch to None as we expect
            normalized_branch_id = None if is_default else branch_id
            return KeboolaClient(
                storage_api_url=self.storage_api_url,
                legacy_storage_token=self.legacy_storage_token,
                bearer_token=self._bearer_token,
                branch_id=normalized_branch_id,
                headers=self._headers,
                readonly=self.readonly,
                own_stack_storage_api_url=self._own_stack_storage_api_url,
            )

    def __init__(
        self,
        *,
        storage_api_url: str,
        legacy_storage_token: str,
        bearer_token: str | None = None,
        branch_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
        readonly: bool | None = None,
        own_stack_storage_api_url: str | None = None,
    ) -> None:
        """
        Initialize the client from two already-classified credentials. A raw, not-yet-classified
        caller credential (e.g. straight off a request) should go through `create()` instead,
        which decides `legacy_storage_token` vs `bearer_token` and performs the legacy-token
        exchange when applicable.

        :param legacy_storage_token: A legacy project-scoped Storage API token. Sent as-is to
            jobs-queue/AI-service/sync-actions; also used for every other service when no
            `bearer_token` is given.
        :param storage_api_url: Keboola Storage API URL
        :param bearer_token: The access token issued by Keboola OAuth server
        :param branch_id: Keboola branch ID
        :param headers: Additional headers for the requests sent by all clients
        :param readonly: If True, the client will only use HTTP GET, HEAD operations.
        :param own_stack_storage_api_url: The Storage API URL of the Keboola stack that this MCP
            server instance belongs to (`ServerState.own_stack_storage_api_url`). It must come from
            the server's own configuration ('--api-url', 'KBC_STORAGE_API_URL', 'HOSTNAME_SUFFIX'),
            never from a per-request HTTP header. It is only used to decide whether the server's own
            Kubernetes ServiceAccount credential may be sent to `storage_api_url`; when it is None,
            the step-up is never attempted. See `step_up_storage_client()`.
        """
        self._legacy_storage_token = legacy_storage_token
        self._bearer_token = bearer_token
        self._branch_id = branch_id
        self._own_stack_storage_api_url = own_stack_storage_api_url
        self._headers = dict(headers) if headers else None
        self._features_cache: set[str] | None = None
        # Session-scoped cache of flow configuration schemas keyed by flow type (component id).
        # Mirrors _features_cache: fetched once per session so it is never stale across runs.
        self._flow_schema_cache: dict[str, JsonDict] = {}

        self._storage_api_url = normalize_storage_api_url(storage_api_url)
        self._hostname_suffix = cast(str, urlparse(self._storage_api_url).hostname).split('connection.')[1]
        metastore_api_url = urlunparse(('https', f'metastore.{self._hostname_suffix}', '', '', '', ''))
        queue_api_url = urlunparse(('https', f'queue.{self._hostname_suffix}', '', '', '', ''))
        ai_service_api_url = urlunparse(('https', f'ai.{self._hostname_suffix}', '', '', '', ''))
        data_science_api_url = urlunparse(('https', f'data-science.{self._hostname_suffix}', '', '', '', ''))
        encryption_api_url = urlunparse(('https', f'encryption.{self._hostname_suffix}', '', '', '', ''))
        scheduler_api_url = urlunparse(('https', f'scheduler.{self._hostname_suffix}', '', '', '', ''))
        sync_actions_api_url = urlunparse(('https', f'sync-actions.{self._hostname_suffix}', '', '', '', ''))

        # Initialize clients for individual services
        bearer_or_sapi_token = self._bearer_or_sapi_token = (
            f'Bearer {bearer_token}' if bearer_token else self._legacy_storage_token
        )
        # The encryption service does not require an authorization header, so we pass None as the token
        self._encryption_client = EncryptionClient.create(
            root_url=encryption_api_url, token=None, headers=self._headers
        )
        self._storage_client = AsyncStorageClient.create(
            root_url=self._storage_api_url,
            token=bearer_or_sapi_token,
            branch_id=branch_id,
            headers=self._headers,
            readonly=readonly,
            encryption_client=self._encryption_client,
        )
        # Jobs-queue / AI-service / sync-actions keep the legacy Storage token and must NOT be given
        # the OAuth bearer: the queue's NewJobFactory re-sends whatever it receives to Storage as a
        # legacy X-StorageApi-Token (hardcoded AuthType::STORAGE_TOKEN), so a bearer-shaped credential
        # arrives there as an invalid Storage token and every run_job 401s (INC-02580 / SUPPORT-17416).
        # Reverts the client.py part of AI-3755. For a programmatic token (kbc_at_/kbc_pat_), it is
        # `create()`'s job to already have exchanged it for the legacy per-project Storage token via
        # the auth-bridge (StorageTokenResolver) before this constructor runs; a raw, unresolved
        # kbc_at_/kbc_pat_ string 401s here exactly like an OAuth bearer would.
        self._jobs_queue_client = JobsQueueClient.create(
            root_url=queue_api_url,
            token=self._legacy_storage_token,
            branch_id=branch_id,
            headers=self._headers,
            readonly=readonly,
        )
        self._ai_service_client = AIServiceClient.create(
            root_url=ai_service_api_url, token=self._legacy_storage_token, headers=self._headers, readonly=readonly
        )
        # Data-science (sandboxes-service) git-repo credential endpoints require an admin-context
        # token (CanManageAppRepoCredentials -> StorageApiToken::isAdminToken()). The OAuth bearer
        # token carries admin context; the SAPI token minted for OAuth sessions does not. Pass the
        # bearer token when available so credential minting works for OAuth clients (falls back to
        # the SAPI token otherwise). See AI-3398.
        self._data_science_client = DataScienceClient.create(
            root_url=data_science_api_url,
            token=bearer_or_sapi_token,
            branch_id=branch_id,
            headers=self._headers,
            readonly=readonly,
        )
        self._scheduler_client = SchedulerClient.create(
            root_url=scheduler_api_url, token=bearer_or_sapi_token, headers=self._headers, readonly=readonly
        )
        self._sync_actions_client = SyncActionsClient.create(
            root_url=sync_actions_api_url,
            token=self._legacy_storage_token,
            branch_id=branch_id,
            headers=self._headers,
            readonly=readonly,
        )
        self._metastore_client = MetastoreClient.create(
            root_url=metastore_api_url,
            token=bearer_or_sapi_token,
            branch_id=branch_id,
            headers=self._headers,
            readonly=readonly,
        )

    @property
    def hostname_suffix(self) -> str:
        return self._hostname_suffix

    @property
    def storage_api_url(self) -> str:
        return self._storage_api_url

    @property
    def legacy_storage_token(self) -> str:
        return self._legacy_storage_token

    @property
    def bearer_token(self) -> str | None:
        """
        Gets the OAuth bearer token issued by Keboola OAuth server, if available.
        Returns None if only storage token authentication is used.
        """
        return self._bearer_token

    @property
    def branch_id(self) -> str | None:
        """
        Gets ID of the Keboola branch that the MCP server is bound to or None if it's bound
        to the main/production branch.
        """
        return self._branch_id

    async def has_feature(self, feature: str) -> bool:
        """Checks if the project has a specific feature enabled. Results are cached."""
        if self._features_cache is None:
            token_info = await self._storage_client.verify_token()
            owner = token_info.get('owner', {})
            self._features_cache = set(owner.get('features', []) if isinstance(owner, dict) else [])
        return feature in self._features_cache

    def get_cached_flow_schema(self, flow_type: str) -> JsonDict | None:
        """Return the cached configuration schema for the given flow type, or None if not cached."""
        return self._flow_schema_cache.get(flow_type)

    def cache_flow_schema(self, flow_type: str, schema: JsonDict) -> None:
        """Cache the configuration schema for the given flow type for the rest of the session."""
        self._flow_schema_cache[flow_type] = schema

    @property
    def headers(self) -> dict[str, Any] | None:
        return dict(self._headers) if self._headers else None

    @property
    def storage_client(self) -> 'AsyncStorageClient':
        return self._storage_client

    @property
    def readonly(self) -> bool | None:
        return self._storage_client.raw_client.readonly

    @property
    def writable_storage_client(self) -> 'AsyncStorageClient':
        """A Storage client identical to `storage_client` but never read-only.

        Used for server-side plumbing (workspace/config provisioning ahead of `query_data`) that
        must succeed even under a read-only confirmed scope: the read-only guarantee is about
        which tools the caller can use to mutate the project's own data, not whether the server
        may provision the read-only workspace it needs to serve reads at all -- see the
        "Security hardening" RFC increment.
        """
        return AsyncStorageClient.create(
            root_url=self._storage_api_url,
            token=self._bearer_or_sapi_token,
            branch_id=self._branch_id,
            headers=self._headers,
            readonly=None,
            encryption_client=self._encryption_client,
        )

    def step_up_storage_client(self, kubernetes_token_path: str) -> 'AsyncStorageClient':
        """
        Returns a Storage client that keeps this client's user token and additionally
        sends the projected Kubernetes ServiceAccount JWT as the X-Kubernetes-Authorization
        step-up header. Connection waives the permissions the user's token lacks on the
        step-up-enabled actions (workspace / config / event creation) when the
        ServiceAccount is authorized for them — no privileged token is minted and the
        user's token stays the audited principal. Always writable regardless of this client's
        own read-only setting (see `writable_storage_client`) -- provisioning is server-side
        plumbing, not a user-visible mutation, and step-up exists precisely to let it proceed on
        a token that otherwise couldn't.

        The ServiceAccount JWT is a credential of the MCP server deployment itself, so it is
        only ever sent to the Keboola stack that this server belongs to. The Storage API URL of a
        session can come from a per-request HTTP header, therefore it is checked against the URL of
        this server's own stack — resolved once when the server starts and passed to this client as
        `own_stack_storage_api_url` — before the header is attached. When the two differ, or when
        the server has no stack of its own (a locally run server), the step-up is skipped and this
        client's plain (but still writable) Storage client is returned, so the JWT is never sent
        anywhere else.

        The token file is read on each call so kubelet rotation needs no restart.

        :param kubernetes_token_path: Path to the projected ServiceAccount token file.
        :raises ValueError: If the token file is empty.
        """
        if not is_same_stack(self._storage_api_url, self._own_stack_storage_api_url):
            LOG.warning(
                f'Not sending the Kubernetes ServiceAccount step-up header to "{self._storage_api_url}": '
                f"it is not the Storage API URL of this server's own stack "
                f'({self._own_stack_storage_api_url or "not configured"}).'
            )
            return self.writable_storage_client

        jwt = read_service_account_jwt(kubernetes_token_path)

        headers = dict(self._headers or {})
        headers['X-Kubernetes-Authorization'] = f'Bearer {jwt}'
        return AsyncStorageClient.create(
            root_url=self._storage_api_url,
            # Bearer, not the raw legacy_storage_token: for a programmatic (kbc_at_/kbc_pat_) session
            # the raw token would be sent as X-StorageAPI-Token, which Storage API rejects outright
            # -- it only accepts a programmatic token via Authorization: Bearer.
            token=self._bearer_or_sapi_token,
            branch_id=self._branch_id,
            headers=headers,
            readonly=None,
        )

    @property
    def jobs_queue_client(self) -> 'JobsQueueClient':
        return self._jobs_queue_client

    @property
    def ai_service_client(self) -> 'AIServiceClient':
        return self._ai_service_client

    @property
    def data_science_client(self) -> 'DataScienceClient':
        return self._data_science_client

    @property
    def encryption_client(self) -> 'EncryptionClient':
        return self._encryption_client

    @property
    def scheduler_client(self) -> 'SchedulerClient':
        return self._scheduler_client

    @property
    def sync_actions_client(self) -> 'SyncActionsClient':
        return self._sync_actions_client

    @property
    def metastore_client(self) -> 'MetastoreClient':
        return self._metastore_client
