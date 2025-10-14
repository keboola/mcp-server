from typing import Any, Optional, cast

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict, KeboolaServiceClient, RawKeboolaClient


class DocsQuestionResponse(BaseModel):
    """
    The AI service response to a request to `/docs/question` endpoint.
    """

    text: str = Field(description='Text of the answer to a documentation query.')
    source_urls: list[str] = Field(
        description='List of URLs to the sources of the answer.',
        default_factory=list,
        alias='sourceUrls',
    )


class SuggestedComponent(BaseModel):
    """The AI service response to a /docs/suggest-component request."""

    component_id: str = Field(description='The component ID.', alias='componentId')
    score: float = Field(description='Score of the component suggestion.')
    source: str = Field(description='Source of the component suggestion.')


class ComponentSuggestionResponse(BaseModel):
    """The AI service response to a /suggest/component request."""

    components: list[SuggestedComponent] = Field(description='List of suggested components.', default_factory=list)


class AIServiceClient(KeboolaServiceClient):
    """Async client for Keboola AI Service."""

    @classmethod
    def create(cls, root_url: str, token: Optional[str], headers: dict[str, Any] | None = None) -> 'AIServiceClient':
        """
        Creates an AIServiceClient from a Keboola Storage API token.

        :param root_url: The root URL of the AI service API.
        :param token: The Keboola Storage API token. If None, the client will not send any authorization header.
        :param headers: Additional headers for the requests.
        :return: A new instance of AIServiceClient.
        """
        return cls(raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token, headers=headers))

    async def get_component_detail(self, component_id: str) -> JsonDict:
        """
        Retrieves information about a given component.

        :param component_id: The id of the component.
        :return: Component details as dictionary.
        """
        return cast(JsonDict, await self.get(endpoint=f'docs/components/{component_id}'))

    async def docs_question(self, query: str) -> DocsQuestionResponse:
        """
        Answers a question using the Keboola documentation as a source.
        :param query: The query to answer.
        :return: Response containing the answer and source URLs.
        """
        response = await self.raw_client.post(
            endpoint='docs/question',
            data={'query': query},
            headers={'Accept': 'application/json'},
        )

        return DocsQuestionResponse.model_validate(response)

    async def suggest_component(self, query: str) -> ComponentSuggestionResponse:
        """
        Provides list of component suggestions based on natural language query.
        :param query: The query to answer.
        :return: Response containing the list of suggested component IDs, their score and source.
        """
        response = await self.raw_client.post(
            endpoint='suggest/component',
            data={'prompt': query},
            headers={'Accept': 'application/json'},
        )

        return ComponentSuggestionResponse.model_validate(response)
