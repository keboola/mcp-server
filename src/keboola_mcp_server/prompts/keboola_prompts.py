"""Keboola-specific prompts for the MCP server."""

from typing import List

from fastmcp import Context
from fastmcp.prompts import Message

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.tools.workspace import WorkspaceManager


@with_session_state()
async def analyze_project_structure(ctx: Context) -> List[Message]:
    """
    Generate a prompt to analyze the overall structure of a Keboola project.
    
    This prompt helps users understand their project's components, data flow,
    and overall architecture by providing a comprehensive analysis request.
    """
    try:
        client = KeboolaClient.from_state(ctx.session.state)
        
        # Get the Storage API URL from the client
        storage_api_url = client.storage_client.raw_client.base_api_url
        project_info = f"Keboola Project Analysis for project using Storage API: {storage_api_url}"
        
        return [
            Message(
                role="user",
                content=f"""Please analyze the structure and components of this Keboola project:

{project_info}

I would like you to:
1. Examine the data pipeline architecture
2. Identify key components and their relationships
3. Suggest potential optimizations or improvements
4. Highlight any potential issues or bottlenecks
5. Provide recommendations for best practices

Please provide a comprehensive analysis with actionable insights."""
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
async def debug_transformation(ctx: Context, transformation_name: str) -> List[Message]:
    """
    Generate a prompt to help debug a specific transformation.
    
    Args:
        transformation_name: Name of the transformation to debug
    """
    try:
        client = KeboolaClient.from_state(ctx.session.state)
        
        # Get the Storage API URL from the client
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