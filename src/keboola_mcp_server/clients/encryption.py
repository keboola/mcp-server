from typing import Any, Iterator, cast

from keboola_mcp_server.clients.base import JsonDict, KeboolaServiceClient, RawKeboolaClient

EncValue = str | JsonDict

# Keys starting with this prefix hold secret values that must never be stored in plaintext.
SECRET_KEY_PREFIX = '#'
# Values already encrypted by the encryption service start with this prefix.
ENCRYPTED_VALUE_PREFIX = 'KBC::'
# Placeholder that the MCP server returns instead of plaintext secret values on config reads.
REDACTED_SECRET_VALUE = '[REDACTED]'


def is_encrypted_value(value: Any) -> bool:
    """Checks if the value is a cipher created by the encryption service."""
    return isinstance(value, str) and value.startswith(ENCRYPTED_VALUE_PREFIX)


def iter_secret_items(value: Any) -> Iterator[tuple[str, Any]]:
    """
    Recursively walks dicts and lists and yields (key, value) pairs for every key
    starting with the '#' secret prefix.
    """
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str) and key.startswith(SECRET_KEY_PREFIX):
                yield key, item
            else:
                yield from iter_secret_items(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_secret_items(item)


def contains_plaintext_secrets(value: Any) -> bool:
    """Checks if the value contains any '#'-prefixed key whose value is not yet encrypted."""
    return any(not is_encrypted_value(item) for _, item in iter_secret_items(value))


def redact_secrets(value: Any, *, mask: str = REDACTED_SECRET_VALUE) -> Any:
    """
    Returns a deep copy of the value with plaintext '#'-prefixed secret values replaced by the mask.
    Values already encrypted by the encryption service ('KBC::' ciphers) are kept as they are opaque.
    """
    if isinstance(value, dict):
        return {
            key: (
                mask
                if isinstance(key, str) and key.startswith(SECRET_KEY_PREFIX) and not is_encrypted_value(item)
                else redact_secrets(item, mask=mask)
            )
            for key, item in value.items()
        }
    elif isinstance(value, list):
        return [redact_secrets(item, mask=mask) for item in value]
    else:
        return value


class EncryptionClient(KeboolaServiceClient):

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> 'EncryptionClient':
        """
        Creates an EncryptionClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API.
        :param token: The Keboola Storage API token. If None, the client will not send any authorization header.
        :param headers: Additional headers for the requests.
        :return: A new instance of EncryptionClient
        """
        return cls(raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token, headers=headers))

    async def encrypt(
        self,
        value: EncValue,
        *,
        project_id: str | None = None,
        component_id: str | None = None,
        config_id: str | None = None,
    ) -> EncValue:
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
        if config_id and component_id is None:
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
            data=cast(dict[str, Any], value),
        )
        return cast(EncValue, response)
