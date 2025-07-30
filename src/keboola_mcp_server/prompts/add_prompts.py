"""Module to add prompts to the Keboola MCP server."""
from fastmcp.prompts import Prompt

from keboola_mcp_server.mcp import KeboolaMcpServer


def add_keboola_prompts(mcp: KeboolaMcpServer) -> None:
    """Add all Keboola-specific prompts to the MCP server.

    The prompt names and descriptions are automatically derived from the function
    names and docstrings.
    """
    # Import the prompt functions here to avoid circular imports
    from keboola_mcp_server.prompts.keboola_prompts import (
        analyze_project_structure,
        component_usage_summary,
        create_project_documentation,
        data_quality_assessment,
        error_analysis_report,
        project_health_check,
    )

    # ONE-CLICK PROMPTS (no required parameters)
    # Add project analysis prompt
    mcp.add_prompt(Prompt.from_function(analyze_project_structure))

    # Add project health check prompt
    mcp.add_prompt(Prompt.from_function(project_health_check))

    # Add data quality assessment prompt
    mcp.add_prompt(Prompt.from_function(data_quality_assessment))

    # Add component usage summary prompt
    mcp.add_prompt(Prompt.from_function(component_usage_summary))

    # Add error analysis report prompt
    mcp.add_prompt(Prompt.from_function(error_analysis_report))

    # Add documentation generator prompt
    mcp.add_prompt(Prompt.from_function(create_project_documentation))
