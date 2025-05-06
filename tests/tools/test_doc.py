import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import DocsQuestionResponse, KeboolaClient
from keboola_mcp_server.tools.doc import DocsAnswer, docs_query


@pytest.fixture
def mock_docs_response() -> DocsQuestionResponse:
    """Mock response from the AI service client docs_question method."""
    return DocsQuestionResponse(
        text='This is a test answer to the documentation query.',
        source_urls=['https://docs.keboola.com/page1', 'https://docs.keboola.com/page2'],
    )


@pytest.mark.asyncio
async def test_docs_query(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_docs_response: DocsQuestionResponse,
):
    """Tests docs_query tool with a mocked AI service client response."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.ai_service_client.docs_question = mocker.AsyncMock(return_value=mock_docs_response)

    query = 'How do I create a transformation?'
    result = await docs_query(context, query)

    assert isinstance(result, DocsAnswer)
    assert result.text == mock_docs_response.text
    assert result.source_urls == mock_docs_response.source_urls

    keboola_client.ai_service_client.docs_question.assert_called_once_with(query)
