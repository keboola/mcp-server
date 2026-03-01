"""
MCP App tool for interactive data visualization.

The ``visualize_data`` tool is model-visible — the LLM calls it with CSV data
and chart configuration, and the MCP App renders an interactive Chart.js chart.
"""

import csv
import importlib.resources
import io
import logging
from typing import Any, Literal, Sequence

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations

from keboola_mcp_server.apps import APP_RESOURCE_MIME_TYPE, build_app_resource_meta, build_app_tool_meta
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer

LOG = logging.getLogger(__name__)

DATA_CHART_TAG = 'data_chart'
DATA_CHART_RESOURCE_URI = 'ui://keboola/data-chart'

CHART_TYPE = Literal['bar', 'line', 'pie', 'scatter', 'doughnut', 'area']


def add_data_chart_tools(mcp: KeboolaMcpServer) -> None:
    """Register the Data Chart MCP App tool and resource."""
    html_content = importlib.resources.read_text('keboola_mcp_server.apps', 'data_chart.html')

    resource_meta = build_app_resource_meta(
        csp_resource_domains=['https://unpkg.com', 'https://cdn.jsdelivr.net'],
    )

    @mcp.resource(
        DATA_CHART_RESOURCE_URI,
        name='Data Chart',
        description='Interactive data visualization with Chart.js.',
        mime_type=APP_RESOURCE_MIME_TYPE,
        meta=resource_meta,
    )
    def data_chart_resource() -> str:
        return html_content

    app_meta = build_app_tool_meta(
        resource_uri=DATA_CHART_RESOURCE_URI,
    )

    mcp.add_tool(
        FunctionTool.from_function(
            visualize_data,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={DATA_CHART_TAG},
            meta=app_meta,
        )
    )

    LOG.info('Data Chart MCP App tool registered.')


@tool_errors()
async def visualize_data(
    ctx: Context,
    csv_data: str,
    chart_type: CHART_TYPE,
    title: str,
    x_column: str,
    y_columns: Sequence[str],
    x_label: str = None,
    y_label: str = None,
) -> dict[str, Any]:
    """
    Renders an interactive chart from CSV data.

    Takes raw CSV data and chart configuration, then displays a Chart.js chart
    in an interactive iframe. Supports bar, line, pie, scatter, doughnut, and
    area chart types with automatic multi-series support.

    Use this tool after calling query_data to visualize the results. Analyze
    the CSV columns first, then pick appropriate x_column (labels/categories)
    and y_columns (numeric values to plot).

    EXAMPLES:
    - csv_data="quarter,revenue\\nQ1,100\\nQ2,150", chart_type="bar",
      title="Revenue", x_column="quarter", y_columns=["revenue"]
    - chart_type="line", x_column="date", y_columns=["sales", "costs"]
      -> multi-series line chart
    - chart_type="pie", x_column="category", y_columns=["count"]
      -> pie chart with category labels
    """
    if not csv_data or not csv_data.strip():
        raise ValueError('csv_data must not be empty.')

    if not y_columns:
        raise ValueError('y_columns must not be empty.')

    reader = csv.reader(io.StringIO(csv_data.strip()))
    try:
        headers = next(reader)
    except StopIteration:
        raise ValueError('csv_data must not be empty.')

    headers = [h.strip() for h in headers]

    if x_column not in headers:
        raise ValueError(f'x_column "{x_column}" not found in CSV headers: {headers}')

    for col in y_columns:
        if col not in headers:
            raise ValueError(f'y_column "{col}" not found in CSV headers: {headers}')

    return {
        'csvData': csv_data,
        'chartConfig': {
            'chartType': chart_type,
            'title': title,
            'xColumn': x_column,
            'yColumns': list(y_columns),
            'xLabel': x_label,
            'yLabel': y_label,
        },
    }
