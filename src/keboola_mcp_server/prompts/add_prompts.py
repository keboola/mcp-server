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
        create_data_pipeline_plan,
        data_quality_assessment,
        debug_transformation,
        documentation_generator,
        error_analysis_report,
        generate_project_descriptions,
        optimize_sql_query,
        performance_optimization_analysis,
        project_health_check,
        security_audit,
        troubleshoot_component_error,
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

    # Add security audit prompt
    mcp.add_prompt(
        security_audit,
        name='security-audit',
        description='🔒 Perform a security audit to identify vulnerabilities and compliance issues'
    )

    # Add performance optimization analysis prompt
    mcp.add_prompt(
        performance_optimization_analysis,
        name='performance-optimization-analysis',
        description='⚡ Analyze performance characteristics and identify optimization opportunities'
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

    # PROMPTS WITH OPTIONAL PARAMETERS

    # Add project descriptions prompt
    mcp.add_prompt(
        generate_project_descriptions,
        name='generate-project-descriptions',
        description='📝 Generate business-friendly descriptions for all tables and buckets in the project'
    )

    # PROMPTS WITH REQUIRED PARAMETERS (for specific use cases)

    # Add transformation debugging prompt
    mcp.add_prompt(
        debug_transformation,
        name='debug-transformation',
        description='🐛 Generate a debugging prompt for a specific Keboola transformation'
    )

    # Add data pipeline planning prompt
    mcp.add_prompt(
        create_data_pipeline_plan,
        name='create-data-pipeline-plan',
        description='🔧 Generate a prompt to create a comprehensive data pipeline plan in Keboola'
    )

    # Add SQL optimization prompt
    mcp.add_prompt(
        optimize_sql_query,
        name='optimize-sql-query',
        description='💡 Generate a prompt to optimize SQL queries for Keboola transformations'
    )

    # Add component troubleshooting prompt
    mcp.add_prompt(
        troubleshoot_component_error,
        name='troubleshoot-component-error',
        description='🔧 Generate a prompt to troubleshoot errors in Keboola components'
    )
