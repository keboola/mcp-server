"""Keboola Storage API client wrapper."""

import logging
from typing import Any, Literal, Mapping, Sequence, TypeVar
from urllib.parse import urlparse, urlunparse

from keboola_mcp_server.clients.ai_service import AIServiceClient
from keboola_mcp_server.clients.data_science import DataScienceClient
from keboola_mcp_server.clients.encryption import EncryptionClient
from keboola_mcp_server.clients.jobs_queue import JobsQueueClient
from keboola_mcp_server.clients.storage import AsyncStorageClient

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

    def with_branch_id(self, branch_id: str | None) -> 'KeboolaClient':
        if branch_id == self.branch_id:
            return self
        else:
            return KeboolaClient(
                storage_api_url=self.storage_api_url,
                storage_api_token=self.token,
                bearer_token=self._bearer_token,
                branch_id=branch_id,
                headers=self._headers,
            )

    def __init__(
        self,
        *,
        storage_api_url: str,
        storage_api_token: str,
        bearer_token: str | None = None,
        branch_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Initialize the client.

        :param storage_api_token: Keboola Storage API token
        :param storage_api_url: Keboola Storage API URL
        :param bearer_token: The access token issued by Keboola OAuth server
        :param branch_id: Keboola branch ID
        :param headers: Additional headers for the requests sent by all clients
        """
        self._token = storage_api_token
        self._bearer_token = bearer_token
        self._branch_id = branch_id
        self._headers = dict(headers) if headers else None

        sapi_url_parsed = urlparse(storage_api_url)
        if not sapi_url_parsed.hostname or not sapi_url_parsed.hostname.startswith('connection.'):
            raise ValueError(f'Invalid Keboola Storage API URL: {storage_api_url}')

        self._hostname_suffix = sapi_url_parsed.hostname.split('connection.')[1]
        self._storage_api_url = urlunparse(('https', f'connection.{self._hostname_suffix}', '', '', '', ''))
        queue_api_url = urlunparse(('https', f'queue.{self._hostname_suffix}', '', '', '', ''))
        ai_service_api_url = urlunparse(('https', f'ai.{self._hostname_suffix}', '', '', '', ''))
        data_science_api_url = urlunparse(('https', f'data-science.{self._hostname_suffix}', '', '', '', ''))
        encryption_api_url = urlunparse(('https', f'encryption.{self._hostname_suffix}', '', '', '', ''))

        # Initialize clients for individual services
        bearer_or_sapi_token = f'Bearer {bearer_token}' if bearer_token else self._token
        self._storage_client = AsyncStorageClient.create(
            root_url=self._storage_api_url, token=bearer_or_sapi_token, branch_id=branch_id, headers=self._headers
        )
        self._jobs_queue_client = JobsQueueClient.create(
            root_url=queue_api_url, token=self._token, branch_id=branch_id, headers=self._headers
        )
        self._ai_service_client = AIServiceClient.create(
            root_url=ai_service_api_url, token=self._token, headers=self._headers
        )
        self._data_science_client = DataScienceClient.create(
            root_url=data_science_api_url, token=self.token, branch_id=branch_id, headers=self._headers
        )
        # The encryption service does not require an authorization header, so we pass None as the token
        self._encryption_client = EncryptionClient.create(
            root_url=encryption_api_url, token=None, headers=self._headers
        )

    @property
    def hostname_suffix(self) -> str:
        return self._hostname_suffix

    @property
    def storage_api_url(self) -> str:
        return self._storage_api_url

    @property
    def token(self) -> str:
        return self._token

    @property
    def branch_id(self) -> str | None:
        """
        Gets ID of the Keboola branch that the MCP server is bound to or None if it's bound
        to the main/production branch.
        """
        return self._branch_id

    @property
    def headers(self) -> dict[str, Any] | None:
        return dict(self._headers) if self._headers else None

    @property
    def storage_client(self) -> 'AsyncStorageClient':
        return self._storage_client

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
