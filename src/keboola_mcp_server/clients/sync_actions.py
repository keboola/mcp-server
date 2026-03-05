from typing import Any, cast

from keboola_mcp_server.clients.base import JsonStruct, KeboolaServiceClient, RawKeboolaClient


class SyncActionsClient(KeboolaServiceClient):
    """
    Async client for Keboola Sync Actions API.
    """

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str,
        headers: dict[str, Any] | None = None,
        readonly: bool | None = None,
    ) -> 'SyncActionsClient':
        """
        Creates a SyncActions client.

        :param root_url: Root url of API. e.g. "https://sync-actions.keboola.com/".
        :param token: The Keboola Storage API token
        :param headers: Additional headers for the requests.
        :param readonly: If True, the client will only use HTTP GET, HEAD operations.
        :return: A new instance of SyncActionsClient.
        """
        return cls(
            raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token, headers=headers, readonly=readonly),
        )

    async def execute_action(
        self,
        component_id: str,
        action: str,
        config_data: dict[str, Any],
        branch_id: str | None = None,
    ) -> JsonStruct:
        """
        Executes a synchronous action for a component.

        :param component_id: The ID of the component.
        :param action: The sync action to execute (e.g., "testConnection").
        :param config_data: The configuration data payload.
        :param branch_id: Optional branch ID.
        :return: The action result as a dict or list.
        """
        payload: dict[str, Any] = {
            'configData': config_data,
            'componentId': component_id,
            'action': action,
        }
        if branch_id:
            payload['branchId'] = branch_id
        return await self.post(endpoint='actions', data=payload)
