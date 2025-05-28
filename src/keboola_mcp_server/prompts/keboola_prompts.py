"""Keboola-specific prompts for the MCP server."""

from typing import List

from fastmcp import Context
from fastmcp.prompts import Message
from pydantic import Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.tools.workspace import WorkspaceManager


@with_session_state()
async def analyze_project_structure(ctx: Context) -> List[Message]:
    """
    Generate a comprehensive analysis prompt for a Keboola project's structure, components, and use cases.
    
    This prompt analyzes the project's components, data flow, buckets, tables, and configurations
    to provide insights into the project's capabilities and real-world applications.
    """
    try:
        client = KeboolaClient.from_state(ctx.session.state)
        storage_api_url = client.storage_client.raw_client.base_api_url
        
        return [
            Message(
                role="user",
                content=f"""Based on the components that are being used and the data available from all of the buckets in the project, give me a high-level understanding of what is going on inside of this project and the types of use cases that are being performed.

**Project Details:**
- Storage API: {storage_api_url}
- Request ID: {ctx.request_id}

**Analysis Requirements:**
Highlight the key functionalities being implemented, emphasizing the project's capability to address specific problems or tasks. Explore the range of use cases the project is designed for, detailing examples of real-world scenarios it can handle. Be sure to also include the FQIDs of real example buckets, tables & configurations that are within the project.

**Structure your output in the following format:**

## High-level Summary
• Bullet-point summary of the activities and use cases being performed

## Data Sources & Integrations
• List all data sources and external integrations
• Include specific extractor components and their configurations
• Mention connection types and data refresh patterns

## Data Processing & Transformation
• Detail transformation workflows and SQL logic
• Highlight data cleaning, enrichment, and aggregation processes
• Include specific transformation component FQIDs and examples

## Data Storage & Management
• Describe bucket organization and table structures
• Include real bucket and table FQIDs from the project
• Explain data retention and archival strategies

## Use Cases
• Identify specific business use cases and scenarios
• Provide real-world examples the project can handle
• Connect technical capabilities to business outcomes

Please provide a comprehensive analysis with specific examples and FQIDs from the actual project data."""
            )
        ]
    except Exception as e:
        return [
            Message(
                role="user",
                content=f"Error generating project analysis prompt: {str(e)}"
            )
        ]


@with_session_state()
async def generate_project_descriptions(
    ctx: Context,
    focus_area: str = Field(default="all", description="Focus area: 'buckets', 'tables', or 'all'"),
    include_technical_details: bool = Field(default=True, description="Include technical metadata and schemas")
) -> List[Message]:
    """
    Generate comprehensive descriptions for all tables and buckets in a Keboola project.
    
    Args:
        focus_area: Whether to focus on 'buckets', 'tables', or 'all' components
        include_technical_details: Whether to include technical metadata and schema information
    """
    try:
        client = KeboolaClient.from_state(ctx.session.state)
        storage_api_url = client.storage_client.raw_client.base_api_url
        
        technical_section = ""
        if include_technical_details:
            technical_section = """
## Technical Details (for each item)
• Schema information and column definitions
• Data types and constraints
• Row counts and data volume metrics
• Last update timestamps and refresh patterns"""

        focus_instruction = {
            "buckets": "Focus specifically on bucket-level descriptions and organization.",
            "tables": "Focus specifically on table-level descriptions and data structures.", 
            "all": "Provide comprehensive descriptions for both buckets and tables."
        }.get(focus_area, "Provide comprehensive descriptions for both buckets and tables.")
        
        return [
            Message(
                role="user",
                content=f"""Generate comprehensive, business-friendly descriptions for all tables and buckets in this Keboola project. {focus_instruction}

**Project Details:**
- Storage API: {storage_api_url}
- Request ID: {ctx.request_id}
- Focus Area: {focus_area}
- Include Technical Details: {include_technical_details}

**Requirements:**
Create clear, informative descriptions that help users understand:
1. What data each bucket/table contains
2. The business purpose and use cases
3. Data lineage and relationships
4. Quality and completeness indicators

**Structure your output as follows:**

## Bucket Descriptions
For each bucket, provide:
• **Bucket Name & FQID**: [bucket.name]
• **Purpose**: Business purpose and data category
• **Contents**: Types of tables and data contained
• **Use Cases**: How this data is typically used
• **Data Sources**: Where the data originates from{technical_section if focus_area in ['buckets', 'all'] else ''}

## Table Descriptions  
For each table, provide:
• **Table Name & FQID**: [bucket.table]
• **Description**: Clear business description of the data
• **Key Columns**: Most important fields and their meanings
• **Data Quality**: Completeness, accuracy, and freshness indicators
• **Relationships**: How it connects to other tables
• **Business Value**: Why this data matters and how it's used{technical_section if focus_area in ['tables', 'all'] else ''}

## Summary
• Overall data architecture insights
• Recommendations for improving descriptions
• Suggestions for better data organization

Please analyze the actual project data and provide specific, actionable descriptions for each component."""
            )
        ]
    except Exception as e:
        return [
            Message(
                role="user",
                content=f"Error generating project descriptions prompt: {str(e)}"
            )
        ]


@with_session_state()
async def debug_transformation(ctx: Context, transformation_name: str) -> List[Message]:
    """
    Generate a prompt to help debug a specific transformation.
    
    Args:
        transformation_name: Name of the transformation to debug
    """
    try:
        client = KeboolaClient.from_state(ctx.session.state)
        storage_api_url = client.storage_client.raw_client.base_api_url
        
        return [
            Message(
                role="user",
                content=f"""I need help debugging a Keboola transformation called "{transformation_name}".

Please help me:
1. Identify potential issues in the transformation logic
2. Check for common SQL errors or performance problems
3. Suggest optimization strategies
4. Recommend debugging approaches
5. Provide best practices for transformation development

The transformation is part of a Keboola project using Storage API: {storage_api_url}

What specific information would you need to effectively debug this transformation?"""
            )
        ]
    except Exception as e:
        return [
            Message(
                role="user",
                content=f"Error generating transformation debug prompt: {str(e)}"
            )
        ]


async def create_data_pipeline_plan(
    source_description: str, 
    target_description: str, 
    requirements: str = ""
) -> List[Message]:
    """
    Generate a prompt to create a data pipeline plan.
    
    Args:
        source_description: Description of the data source
        target_description: Description of the target/destination
        requirements: Additional requirements or constraints
    """
    requirements_text = f"\n\nAdditional requirements:\n{requirements}" if requirements else ""
    
    return [
        Message(
            role="user",
            content=f"""I need to create a data pipeline in Keboola Connection with the following specifications:

**Source:** {source_description}
**Target:** {target_description}{requirements_text}

Please help me design a comprehensive data pipeline plan that includes:

1. **Data Extraction Strategy**
   - Recommended extractors or data sources
   - Connection configuration considerations
   - Data refresh frequency recommendations

2. **Data Transformation Plan**
   - Required data cleaning and preparation steps
   - Transformation logic and SQL queries
   - Data quality checks and validation

3. **Data Loading Strategy**
   - Target storage configuration
   - Output format and structure
   - Performance optimization considerations

4. **Orchestration and Monitoring**
   - Recommended orchestration flow
   - Error handling and alerting
   - Monitoring and logging strategies

5. **Best Practices**
   - Security considerations
   - Scalability recommendations
   - Maintenance and documentation

Please provide a detailed, step-by-step implementation plan with specific Keboola components and configurations."""
        )
    ]


async def optimize_sql_query(sql_query: str, context: str = "") -> List[Message]:
    """
    Generate a prompt to optimize an SQL query for Keboola transformations.
    
    Args:
        sql_query: The SQL query to optimize
        context: Additional context about the query's purpose
    """
    context_text = f"\n\nContext: {context}" if context else ""
    
    return [
        Message(
            role="user",
            content=f"""Please analyze and optimize this SQL query for use in a Keboola transformation:{context_text}

```sql
{sql_query}
```

I need help with:

1. **Performance Optimization**
   - Identify potential bottlenecks
   - Suggest indexing strategies
   - Recommend query restructuring

2. **Best Practices**
   - Code readability and maintainability
   - Keboola-specific optimizations
   - Resource usage efficiency

3. **Error Prevention**
   - Common pitfalls to avoid
   - Data type considerations
   - Null handling improvements

4. **Alternative Approaches**
   - Different ways to achieve the same result
   - Trade-offs between approaches
   - Scalability considerations

Please provide the optimized query with explanations for each improvement."""
        )
    ]


async def troubleshoot_component_error(
    component_name: str, 
    error_message: str, 
    component_type: str = "unknown"
) -> List[Message]:
    """
    Generate a prompt to troubleshoot a component error.
    
    Args:
        component_name: Name of the component with the error
        error_message: The error message received
        component_type: Type of component (extractor, writer, transformation, etc.)
    """
    return [
        Message(
            role="user",
            content=f"""I'm experiencing an error with a Keboola component and need troubleshooting help:

**Component:** {component_name}
**Type:** {component_type}
**Error Message:**
```
{error_message}
```

Please help me:

1. **Diagnose the Issue**
   - Interpret the error message
   - Identify the root cause
   - Determine if it's a configuration, data, or system issue

2. **Provide Solutions**
   - Step-by-step troubleshooting guide
   - Configuration fixes or adjustments
   - Alternative approaches if needed

3. **Prevention Strategies**
   - How to avoid this error in the future
   - Best practices for component configuration
   - Monitoring and alerting recommendations

4. **Additional Investigation**
   - What additional information might be needed
   - Logs or metrics to check
   - Related components that might be affected

Please provide a comprehensive troubleshooting guide with specific actions I can take."""
        )
    ] 