from typing import Any, Optional

from keboola_mcp_server.clients.base import KeboolaServiceClient, RawKeboolaClient


class EncryptionClient(KeboolaServiceClient):

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        """
        Creates an EncryptionClient from a RawKeboolaClient.

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
        token: Optional[str] = None,
        headers: dict[str, Any] | None = None,
    ) -> 'EncryptionClient':
        """
        Creates an EncryptionClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token. If None, the client will not send any authorization header.
        :param headers: Additional headers for the requests
        :return: A new instance of EncryptionClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
            )
        )

    async def encrypt(
        self, value: Any, project_id: Optional[str], component_id: Optional[str] = None, config_id: Optional[str] = None
    ) -> Any:
        """
        Encrypt a value using the encryption service, returns encrypted value. Parameters are optional and the ciphers
        created by the service are dependent on those parameters when decrypting. Decryption is done automatically
        when using encrypted values in a request to Storage API (for components)
        See: https://developers.keboola.com/overview/encryption/
        If value is a dict, values whose keys start with '#' are encrypted.
        If value is a str, it is encrypted.
        If value contains already encrypted values, they are returned as is.

        :param value: The value to encrypt
        :param project_id: The project ID
        :param component_id: The component ID (optional)
        :param config_id: The config ID (optional)
        :return: The encrypted value, same type as input
        """
        if component_id and project_id is None:
            raise ValueError('project_id is required if component_id is provided')
        if config_id and not component_id:
            raise ValueError('component_id is required if config_id is provided')

        params = {
            'componentId': component_id,
            'projectId': project_id,
            'configId': config_id,
        }
        params = {k: v for k, v in params.items() if v is not None}
        response = await self.raw_client.post(
            endpoint='encrypt',
            params=params,
            data=value,
        )
        return response
