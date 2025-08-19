"""Keboola Storage API client wrapper."""

import importlib.metadata
import logging
import os
from typing import Any, Literal, Mapping, Optional, Sequence, TypeVar

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
    metadata: list[Mapping[str, Any]], key: str, provider: str | None = None, default: T | None = None
) -> Optional[T]:
    """
    Gets the value of a metadata property based on the provided key and optional provider. If multiple metadata entries
    exist with the same key, the most recent one is returned.

    :param metadata: A list of metadata entries.
    :param key: The metadata property key to search for.
    :param provider: Specifies the metadata provider name to filter by.
    :param default: The default value to return if the metadata property is not found.

    :return: The value of the most recent matching metadata entry if found, or None otherwise.
    """
    filtered = [
        m for m in metadata if m['key'] == key and (not provider or ('provider' in m and m['provider'] == provider))
    ]
    # TODO: ideally we should first convert the timestamps to UTC
    filtered.sort(key=lambda x: x.get('timestamp') or '', reverse=True)
    value = filtered[0].get('value') if filtered else None
    return value if value is not None else default


class KeboolaClient:
    """Class holding clients for Keboola APIs: Storage API, Job Queue API, and AI Service."""

    STATE_KEY = 'sapi_client'
    # Prefixes for the storage and queue API URLs, we do not use http:// or https:// here since we split the storage
    # api url by `connection` word
    _PREFIX_STORAGE_API_URL = 'connection.'
    _PREFIX_QUEUE_API_URL = 'https://queue.'
    _PREFIX_AISERVICE_API_URL = 'https://ai.'
    _PREFIX_DATA_SCIENCE_API_URL = 'https://data-science.'
    _PREFIX_ENCRYPTION_API_URL = 'https://encryption.'

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'KeboolaClient':
        instance = state[cls.STATE_KEY]
        assert isinstance(instance, KeboolaClient), f'Expected KeboolaClient, got: {instance}'
        return instance

    def __init__(self, storage_api_token: str, storage_api_url: str, bearer_token: str | None = None) -> None:
        """
        Initialize the client.

        :param storage_api_token: Keboola Storage API token
        :param storage_api_url: Keboola Storage API URL
        :param bearer_token: The access token issued by Keboola OAuth server
        """
        self.token = storage_api_token
        # Ensure the base URL has a scheme
        if not storage_api_url.startswith(('http://', 'https://')):
            storage_api_url = f'https://{storage_api_url}'

        # Construct the queue API URL from the storage API URL expecting the following format:
        # https://connection.REGION.keboola.com
        # Remove the prefix from the storage API URL https://connection.REGION.keboola.com -> REGION.keboola.com
        # and add the prefix for the queue API https://queue.REGION.keboola.com
        queue_api_url = f'{self._PREFIX_QUEUE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}'
        ai_service_api_url = f'{self._PREFIX_AISERVICE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}'
        data_science_api_url = (
            f'{self._PREFIX_DATA_SCIENCE_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}'
        )
        encryption_api_url = (
            f'{self._PREFIX_ENCRYPTION_API_URL}{storage_api_url.split(self._PREFIX_STORAGE_API_URL)[1]}'
        )

        # Initialize clients for individual services
        bearer_or_sapi_token = f'Bearer {bearer_token}' if bearer_token else storage_api_token
        self.storage_client = AsyncStorageClient.create(
            root_url=storage_api_url, token=bearer_or_sapi_token, headers=self._get_headers()
        )
        self.jobs_queue_client = JobsQueueClient.create(
            root_url=queue_api_url, token=self.token, headers=self._get_headers()
        )
        self.ai_service_client = AIServiceClient.create(
            root_url=ai_service_api_url, token=self.token, headers=self._get_headers()
        )
        self.data_science_client = DataScienceClient.create(
            root_url=data_science_api_url, token=self.token, headers=self._get_headers()
        )
        # The encryption service does not require an authorization header, so we pass None as the token
        self.encryption_client = EncryptionClient.create(
            root_url=encryption_api_url, token=None, headers=self._get_headers()
        )

    @classmethod
    def _get_user_agent(cls) -> str:
        """
        :return: User agent string.
        """
        try:
            version = importlib.metadata.version('keboola-mcp-server')
        except importlib.metadata.PackageNotFoundError:
            version = 'NA'

        app_env = os.getenv('APP_ENV', 'local')
        return f'Keboola MCP Server/{version} app_env={app_env}'

    @classmethod
    def _get_headers(cls) -> dict[str, Any]:
        """
        :return: Additional headers for the requests, namely the user agent.
        """
        return {'User-Agent': cls._get_user_agent()}
