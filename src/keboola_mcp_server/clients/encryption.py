from typing import Any, Optional

from keboola_mcp_server.clients.base import JsonStruct, KeboolaServiceClient, RawKeboolaClient


class AsyncEncryptionClient(KeboolaServiceClient):

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        """
        Creates an AsyncEncryptionClient from a RawKeboolaClient.

        :param raw_client: The raw client to use
        """
        super().__init__(raw_client=raw_client)

    @property
    def base_api_url(self) -> str:
        return self.raw_client.base_api_url.split('/apps')[0]

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str,
        headers: dict[str, Any] | None = None,
    ) -> 'AsyncEncryptionClient':
        """
        Creates an AsyncStorageClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token
        :param headers: Additional headers for the requests
        :return: A new instance of AsyncEncryptionClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
            )
        )

    async def encrypt(
        self, value: Any, component_id: str, project_id: str, config_id: Optional[str] = None
    ) -> JsonStruct:
        """
        Get a data app by its ID.

        :param data_app_id: The ID of the data app
        :return: The data app
        """
        response = await self.raw_client.post(
            endpoint='encrypt',
            params={
                'componentId': component_id,
                'projectId': project_id,
                'configId': config_id,
            },
            data=value,
        )
        return response
