import pytest
from fastmcp import Context

from keboola_mcp_server.tools.data_chart import visualize_data

CSV_SIMPLE = 'quarter,revenue,costs\nQ1,100,80\nQ2,150,90\nQ3,200,120'


@pytest.mark.asyncio
async def test_visualize_data_returns_structured_content(mcp_context_client: Context):
    result = await visualize_data(
        ctx=mcp_context_client,
        csv_data=CSV_SIMPLE,
        chart_type='bar',
        title='Revenue by Quarter',
        x_column='quarter',
        y_columns=['revenue'],
    )

    assert 'csvData' in result
    assert 'chartConfig' in result
    assert result['chartConfig']['chartType'] == 'bar'
    assert result['chartConfig']['title'] == 'Revenue by Quarter'
    assert result['chartConfig']['xColumn'] == 'quarter'
    assert result['chartConfig']['yColumns'] == ['revenue']
    assert result['chartConfig']['xLabel'] is None
    assert result['chartConfig']['yLabel'] is None


@pytest.mark.asyncio
async def test_visualize_data_with_labels(mcp_context_client: Context):
    result = await visualize_data(
        ctx=mcp_context_client,
        csv_data=CSV_SIMPLE,
        chart_type='line',
        title='Trends',
        x_column='quarter',
        y_columns=['revenue', 'costs'],
        x_label='Quarter',
        y_label='Amount ($)',
    )

    assert result['chartConfig']['xLabel'] == 'Quarter'
    assert result['chartConfig']['yLabel'] == 'Amount ($)'
    assert result['chartConfig']['yColumns'] == ['revenue', 'costs']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'chart_type',
    ['bar', 'line', 'pie', 'scatter', 'doughnut', 'area'],
)
async def test_visualize_data_all_chart_types(mcp_context_client: Context, chart_type: str):
    result = await visualize_data(
        ctx=mcp_context_client,
        csv_data=CSV_SIMPLE,
        chart_type=chart_type,
        title='Test',
        x_column='quarter',
        y_columns=['revenue'],
    )

    assert result['chartConfig']['chartType'] == chart_type


@pytest.mark.asyncio
async def test_visualize_data_empty_csv(mcp_context_client: Context):
    with pytest.raises(ValueError, match='csv_data must not be empty'):
        await visualize_data(
            ctx=mcp_context_client,
            csv_data='',
            chart_type='bar',
            title='Test',
            x_column='x',
            y_columns=['y'],
        )


@pytest.mark.asyncio
async def test_visualize_data_whitespace_only_csv(mcp_context_client: Context):
    with pytest.raises(ValueError, match='csv_data must not be empty'):
        await visualize_data(
            ctx=mcp_context_client,
            csv_data='   \n  ',
            chart_type='bar',
            title='Test',
            x_column='x',
            y_columns=['y'],
        )


@pytest.mark.asyncio
async def test_visualize_data_missing_x_column(mcp_context_client: Context):
    with pytest.raises(ValueError, match='x_column "missing" not found'):
        await visualize_data(
            ctx=mcp_context_client,
            csv_data=CSV_SIMPLE,
            chart_type='bar',
            title='Test',
            x_column='missing',
            y_columns=['revenue'],
        )


@pytest.mark.asyncio
async def test_visualize_data_missing_y_column(mcp_context_client: Context):
    with pytest.raises(ValueError, match='y_column "missing" not found'):
        await visualize_data(
            ctx=mcp_context_client,
            csv_data=CSV_SIMPLE,
            chart_type='bar',
            title='Test',
            x_column='quarter',
            y_columns=['missing'],
        )


@pytest.mark.asyncio
async def test_visualize_data_empty_y_columns(mcp_context_client: Context):
    with pytest.raises(ValueError, match='y_columns must not be empty'):
        await visualize_data(
            ctx=mcp_context_client,
            csv_data=CSV_SIMPLE,
            chart_type='bar',
            title='Test',
            x_column='quarter',
            y_columns=[],
        )


@pytest.mark.asyncio
async def test_visualize_data_csv_passthrough(mcp_context_client: Context):
    """The original CSV data is returned as-is for the JS to parse."""
    result = await visualize_data(
        ctx=mcp_context_client,
        csv_data=CSV_SIMPLE,
        chart_type='bar',
        title='Test',
        x_column='quarter',
        y_columns=['revenue'],
    )

    assert result['csvData'] == CSV_SIMPLE
