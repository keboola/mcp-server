"""One-click prompts for local-backend mode."""

from typing import List

from fastmcp.prompts import Message


async def explore_local_data() -> List[Message]:
    """Explore data you already have — upload a CSV and create a dashboard.

    Workflow: write_table → query_data → create_data_app → run_data_app
    """
    return [
        Message(
            role='user',
            content=(
                'I have a CSV file I want to explore. '
                'Help me upload it, inspect the data with SQL, '
                'and build a dashboard to visualize the results.\n\n'
                'Start by asking me for the CSV content or file path.'
            ),
        )
    ]


async def extract_data_from_source() -> List[Message]:
    """Extract data from an external API, FTP, or database using a Keboola component.

    Workflow: find_component_id → get_component_schema → setup_component → run_component → create_data_app
    """
    return [
        Message(
            role='user',
            content=(
                'I need to extract data from an external source '
                '(API, FTP, database, or similar). '
                'Help me find the right Keboola component, configure it, '
                'run it via Docker, and visualize the extracted data.\n\n'
                'Start by asking me what data source I want to connect to.'
            ),
        )
    ]


async def push_local_work_to_keboola() -> List[Message]:
    """Push local tables and configs to the Keboola platform.

    Workflow: get_project_info → get_tables → query_data → migrate_to_keboola
    """
    return [
        Message(
            role='user',
            content=(
                'I have local tables and component configurations I want to upload '
                'to the Keboola platform. '
                'Help me review what I have, validate the data quality, '
                'and run migrate_to_keboola.\n\n'
                'Start by calling get_project_info to see what is available locally.'
            ),
        )
    ]
