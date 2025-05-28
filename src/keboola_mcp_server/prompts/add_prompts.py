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
        create_data_pipeline_plan,
        debug_transformation,
        generate_project_descriptions,
        optimize_sql_query,
        troubleshoot_component_error,
    )
    
    # Add project analysis prompt
    mcp.add_prompt(
        analyze_project_structure,
        name="analyze-project-structure",
        description="Generate a comprehensive analysis prompt for a Keboola project's structure, components, and use cases"
    )
    
    # Add project descriptions prompt
    mcp.add_prompt(
        generate_project_descriptions,
        name="generate-project-descriptions",
        description="Generate comprehensive, business-friendly descriptions for all tables and buckets in a Keboola project"
    )
    
    # Add transformation debugging prompt
    mcp.add_prompt(
        debug_transformation,
        name="debug-transformation",
        description="Generate a debugging prompt for a specific Keboola transformation"
    )
    
    # Add data pipeline planning prompt
    mcp.add_prompt(
        create_data_pipeline_plan,
        name="create-data-pipeline-plan",
        description="Generate a prompt to create a comprehensive data pipeline plan in Keboola"
    )
    
    # Add SQL optimization prompt
    mcp.add_prompt(
        optimize_sql_query,
        name="optimize-sql-query",
        description="Generate a prompt to optimize SQL queries for Keboola transformations"
    )
    
    # Add component troubleshooting prompt
    mcp.add_prompt(
        troubleshoot_component_error,
        name="troubleshoot-component-error",
        description="Generate a prompt to troubleshoot errors in Keboola components"
    ) 