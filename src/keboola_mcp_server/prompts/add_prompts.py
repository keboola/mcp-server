"""Module to add prompts to the Keboola MCP server."""

from keboola_mcp_server.mcp import KeboolaMcpServer


def add_keboola_prompts(mcp: KeboolaMcpServer) -> None:
    """
    Add all Keboola-specific prompts to the MCP server.

    Args:
        mcp: The KeboolaMcpServer instance to add prompts to
    """
    # Import the prompt functions here to avoid circular imports
    from keboola_mcp_server.prompts.keboola_prompts import (
        analyze_project_structure,
        component_usage_summary,
        data_quality_assessment,
        documentation_generator,
        error_analysis_report,
        project_health_check,
    )

    # ONE-CLICK PROMPTS (no required parameters)
    # Add project analysis prompt
    mcp.add_prompt(
        analyze_project_structure,
        name='analyze-project-structure',
        description='🔍 Generate a comprehensive analysis of the Keboola project structure, components, and use cases'
    )

    # Add project health check prompt
    mcp.add_prompt(
        project_health_check,
        name='project-health-check',
        description='🏥 Perform a comprehensive health check to identify issues, risks, and optimization opportunities'
    )

    # Add data quality assessment prompt
    mcp.add_prompt(
        data_quality_assessment,
        name='data-quality-assessment',
        description='📊 Conduct a comprehensive data quality assessment across all project data'
    )

    # Add component usage summary prompt
    mcp.add_prompt(
        component_usage_summary,
        name='component-usage-summary',
        description='📋 Generate a comprehensive summary of all components and their usage patterns'
    )

    # Add error analysis report prompt
    mcp.add_prompt(
        error_analysis_report,
        name='error-analysis-report',
        description='🚨 Analyze recent errors and failures with troubleshooting recommendations'
    )

    # Add documentation generator prompt
    mcp.add_prompt(
        documentation_generator,
        name='documentation-generator',
        description='📚 Generate comprehensive project documentation for onboarding and maintenance'
    )
