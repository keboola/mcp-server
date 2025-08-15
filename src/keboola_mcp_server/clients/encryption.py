from typing import Any, Optional

from keboola_mcp_server.clients.base import KeboolaServiceClient, RawKeboolaClient


class AsyncEncryptionClient(KeboolaServiceClient):

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        """
        Creates an AsyncEncryptionClient from a RawKeboolaClient.

        :param raw_client: The raw client to use
        """
        super().__init__(raw_client=raw_client)

    @property
    def base_api_url(self) -> str:
        return self.raw_client.base_api_url

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str,
        headers: dict[str, Any] | None = None,
    ) -> 'AsyncEncryptionClient':
        """
        Creates an AsyncEncryptionClient from a Keboola Storage API token.

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
        self, value: Any, project_id: str, component_id: Optional[str] = None, config_id: Optional[str] = None
    ) -> Any:
        """
        Encrypt a value using the encryption service, returns encrypted value.
        if value is a dict, values whose keys start with '#' are encrypted.
        if value is a str, it is encrypted.
        if value contains already encrypted values, they are returned as is.

        :param value: The value to encrypt
        :param project_id: The project ID
        :param component_id: The component ID (optional)
        :param config_id: The config ID (optional)
        :return: The encrypted value, same type as input
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
