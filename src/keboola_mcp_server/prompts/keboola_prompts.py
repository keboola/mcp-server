"""Keboola-specific prompts for the MCP server."""

from typing import List

from fastmcp.prompts import Message


async def analyze_project_structure() -> List[Message]:
    """Generate a comprehensive analysis prompt for a Keboola project's structure.

    This prompt analyzes the project's components, data flow, buckets, tables,
    and configurations to provide insights into capabilities and applications.
    """
    return [
        Message(
            role='user',
            content="""Based on the components that are being used and the data available from all
of the buckets in the project, give me a high-level understanding of what is going on inside
of this project and the types of use cases that are being performed.

**Analysis Requirements:**
Highlight the key functionalities being implemented, emphasizing the project's
capability to address specific problems or tasks. Explore the range of use cases the
project is designed for, detailing examples of real-world scenarios it can handle. Be sure to also include
the names of real example buckets, tables & configurations that are within the project.

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
• Include specific transformation component names and examples

## Data Storage & Management
• Describe bucket organization and table structures
• Include real bucket and table names from the project
• Explain data retention and archival strategies

## Use Cases
• Identify specific business use cases and scenarios
• Provide real-world examples the project can handle
• Connect technical capabilities to business outcomes

Please provide a comprehensive analysis with specific examples and names from the actual project data."""
        )
    ]


async def project_health_check() -> List[Message]:
    """Generate a comprehensive health check analysis for the entire Keboola project.

    This one-click prompt analyzes project health, identifies issues, and provides recommendations.
    """
    return [
        Message(
            role='user',
            content="""Perform a comprehensive health check of this Keboola project and identify
any issues, risks, or optimization opportunities.

**Health Check Areas:**

## 1. Component Health
• Analyze all components for errors, warnings, or performance issues
• Check component configurations for best practices
• Identify unused or redundant components
• Review component update status and versions

## 2. Data Quality Assessment
• Examine tables for data completeness and consistency
• Identify tables with potential data quality issues
• Check for empty tables or tables with unusual patterns
• Analyze data freshness and update frequencies

## 3. Performance Analysis
• Identify slow-running transformations or jobs
• Check for resource-intensive operations
• Analyze job execution patterns and bottlenecks
• Review storage usage and optimization opportunities

## 4. Security & Access Review
• Review bucket and table permissions
• Check for potential security vulnerabilities
• Analyze token usage and access patterns
• Identify overprivileged configurations

## 5. Cost Optimization
• Identify cost optimization opportunities
• Review storage usage and retention policies
• Analyze job execution efficiency
• Suggest resource optimization strategies

## 6. Recommendations
• Prioritized list of issues to address
• Quick wins for immediate improvement
• Long-term optimization strategies
• Best practices implementation suggestions

Please provide specific findings with component and table names and actionable recommendations."""
        )
    ]


async def data_quality_assessment() -> List[Message]:
    """Generate a comprehensive data quality assessment for all project data.

    One-click analysis of data quality across all buckets and tables.
    """
    return [
        Message(
            role='user',
            content="""Conduct a comprehensive data quality assessment across all data in this Keboola project.

**Data Quality Analysis:**

## 1. Completeness Analysis
• Identify tables with missing or null values
• Calculate completeness percentages for key columns
• Flag tables with significant data gaps
• Analyze data volume trends and anomalies

## 2. Consistency Checks
• Check for data format inconsistencies
• Identify duplicate records across tables
• Analyze referential integrity between related tables
• Flag inconsistent naming conventions

## 3. Accuracy Assessment
• Identify potential data accuracy issues
• Check for outliers and anomalous values
• Analyze data validation patterns
• Review data transformation logic for accuracy

## 4. Timeliness Evaluation
• Assess data freshness across all tables
• Identify stale or outdated data
• Review data update frequencies
• Flag tables with irregular update patterns

## 5. Data Profiling Summary
• Statistical overview of each table
• Data type distribution and usage
• Value distribution analysis
• Schema evolution and changes

## 6. Quality Scores & Recommendations
• Overall quality score for each table
• Prioritized list of data quality issues
• Specific improvement recommendations
• Data governance suggestions

Please analyze the actual project data and provide specific findings with table names,
metrics, and actionable recommendations."""
        )
    ]


async def security_audit() -> List[Message]:
    """Generate a security audit for the Keboola project.

    One-click security assessment covering permissions, access, and best practices.
    """
    return [
        Message(
            role='user',
            content="""Perform a comprehensive security audit of this Keboola project to
identify potential vulnerabilities and security best practice violations.

**Security Audit Areas:**

## 1. Access Control Review
• Analyze bucket and table permissions
• Identify overly permissive access settings
• Review token usage and scopes
• Check for unused or stale access credentials

## 2. Data Privacy Assessment
• Identify tables containing sensitive or PII data
• Review data encryption and protection measures
• Check for proper data masking in non-production environments
• Analyze data retention and deletion policies

## 3. Component Security
• Review component configurations for security issues
• Check for hardcoded credentials or sensitive information
• Analyze external connection security
• Verify secure communication protocols

## 4. Compliance Check
• Review adherence to data governance policies
• Check for GDPR/data protection compliance
• Analyze audit trail and logging capabilities
• Verify backup and disaster recovery measures

## 5. Network & Infrastructure Security
• Review API access patterns and restrictions
• Check for suspicious or anomalous access attempts
• Analyze IP whitelisting and access controls
• Review integration security with external systems

## 6. Security Recommendations
• Critical security issues requiring immediate attention
• Medium-priority security improvements
• Security best practices implementation
• Compliance enhancement suggestions

Please provide specific findings with component and bucket names and prioritized security recommendations."""
        )
    ]


async def performance_optimization_analysis() -> List[Message]:
    """Generate a performance analysis and optimization recommendations.

    One-click performance audit identifying bottlenecks and optimization opportunities.
    """
    return [
        Message(
            role='user',
            content="""Analyze the performance characteristics of this Keboola project and
identify optimization opportunities.

**Performance Analysis Areas:**

## 1. Job Execution Performance
• Identify slow-running transformations and extractions
• Analyze job execution patterns and frequencies
• Check for failed or frequently retried jobs
• Review job queue efficiency and resource utilization

## 2. SQL Query Optimization
• Analyze transformation SQL for performance issues
• Identify queries with potential optimization opportunities
• Check for inefficient joins, subqueries, or aggregations
• Review indexing strategies and table design

## 3. Data Pipeline Efficiency
• Analyze end-to-end pipeline execution times
• Identify bottlenecks in data flow
• Review parallel processing opportunities
• Check for unnecessary data movement or duplication

## 4. Storage Optimization
• Analyze table sizes and growth patterns
• Identify opportunities for data archiving or compression
• Review partitioning and clustering strategies
• Check for unused or redundant data storage

## 5. Resource Utilization
• Review compute resource allocation and usage
• Analyze memory and processing requirements
• Check for resource contention or conflicts
• Identify cost-performance optimization opportunities

## 6. Optimization Recommendations
• High-impact performance improvements
• Quick wins for immediate performance gains
• Long-term optimization strategies
• Resource allocation recommendations

Please analyze actual project performance data and provide specific
recommendations with component names and expected performance improvements."""
        )
    ]


async def component_usage_summary() -> List[Message]:
    """Generate a comprehensive summary of all components and their usage patterns.

    One-click overview of project components, configurations, and usage analytics.
    """
    return [
        Message(
            role='user',
            content="""Generate a comprehensive summary of all components in this Keboola
project, their configurations, and usage patterns.

**Component Analysis:**

## 1. Component Inventory
• Complete list of all components by type (extractors, transformations, writers)
• Component versions and update status
• Configuration count per component
• Active vs inactive component status

## 2. Usage Analytics
• Job execution frequency per component
• Success/failure rates and reliability metrics
• Resource consumption patterns
• Peak usage times and scheduling analysis

## 3. Configuration Analysis
• Number of configurations per component
• Configuration complexity and parameter usage
• Shared vs component-specific configurations
• Configuration change history and evolution

## 4. Data Flow Mapping
• Input and output relationships between components
• Data dependencies and lineage
• Critical path analysis in data pipelines
• Component interdependency mapping

## 5. Health & Status Overview
• Component error rates and common issues
• Performance metrics and execution times
• Maintenance and update requirements
• Deprecated or outdated component usage

## 6. Optimization Opportunities
• Underutilized or redundant components
• Configuration consolidation opportunities
• Component upgrade recommendations
• Efficiency improvement suggestions

Please provide specific details including component names, configuration IDs, and
actionable insights for project optimization."""
        )
    ]


async def error_analysis_report() -> List[Message]:
    """Generate an analysis of recent errors and failures across the project.

    One-click error analysis with troubleshooting recommendations.
    """
    return [
        Message(
            role='user',
            content="""Analyze recent errors and failures across this Keboola project and
provide troubleshooting recommendations.

**Error Analysis:**

## 1. Error Frequency & Patterns
• Most common error types across all components
• Error frequency trends over time
• Components with highest failure rates
• Recurring vs one-time error patterns

## 2. Critical Errors
• High-priority errors affecting data pipelines
• Errors causing data quality issues
• Security-related errors or warnings
• Errors impacting business-critical processes

## 3. Component-Specific Issues
• Transformation errors and SQL issues
• Extractor connection and authentication problems
• Writer destination errors and data delivery failures
• Orchestration and scheduling conflicts

## 4. Root Cause Analysis
• Infrastructure vs configuration-related errors
• Data-related errors (missing files, schema changes)
• Permission and access-related issues
• External service dependency failures

## 5. Impact Assessment
• Business impact of each error category
• Data pipeline disruption analysis
• SLA and delivery timeline impacts
• Downstream system effect analysis

## 6. Resolution Recommendations
• Immediate fixes for critical errors
• Preventive measures for recurring issues
• Configuration improvements to reduce errors
• Monitoring and alerting enhancements

Please analyze actual error logs and job histories to provide specific error
instances with component names and detailed troubleshooting guidance."""
        )
    ]


async def create_project_documentation() -> List[Message]:
    """Generate comprehensive project documentation automatically.

    One-click documentation creation for the entire Keboola project.
    """
    return [
        Message(
            role='user',
            content="""Generate comprehensive, professional documentation for this Keboola
project that can be used for onboarding, maintenance, and knowledge sharing.

**Documentation Structure:**

## 1. Project Overview
• Executive summary of project purpose and objectives
• Key stakeholders and business owners
• Project scope and data processing capabilities
• Success metrics and KPIs

## 2. Architecture Documentation
• High-level system architecture diagram description
• Data flow and pipeline overview
• Component interaction and dependencies
• Technical infrastructure and requirements

## 3. Data Dictionary
• Complete inventory of all buckets and tables with names
• Column definitions and business meanings
• Data types, constraints, and validation rules
• Data lineage and source system mappings

## 4. Component Documentation
• Detailed description of each component and its purpose
• Configuration parameters and their meanings
• Input/output specifications
• Business logic and transformation rules

## 5. Operational Procedures
• Data pipeline monitoring and maintenance procedures
• Error handling and troubleshooting guides
• Backup and disaster recovery processes
• Change management and deployment procedures

## 6. User Guides
• End-user access and data consumption guides
• Report and dashboard usage instructions
• Data quality and validation procedures
• FAQ and common troubleshooting scenarios

## 7. Technical Reference
• API endpoints and integration specifications
• Security and access control documentation
• Performance tuning and optimization guides
• Development and testing procedures

Please create detailed, professional documentation using actual project data
including specific names, configurations, and real examples."""
        )
    ]


async def generate_project_descriptions(
    focus_area: str = 'all',
    include_technical_details: bool = True
) -> List[Message]:
    """Generate comprehensive descriptions for all tables and buckets in a Keboola project.

    The focus can be on buckets, tables, or all components. Technical details such as
    schema information and metadata can be optionally included.
    """
    technical_section = ''
    if include_technical_details:
        technical_section = """
## Technical Details (for each item)
• Schema information and column definitions
• Data types and constraints
• Row counts and data volume metrics
• Last update timestamps and refresh patterns"""

    focus_instruction = {
        'buckets': 'Focus specifically on bucket-level descriptions and organization.',
        'tables': 'Focus specifically on table-level descriptions and data structures.',
        'all': 'Provide comprehensive descriptions for both buckets and tables.'
    }.get(focus_area, 'Provide comprehensive descriptions for both buckets and tables.')

    # Pre-calculate conditional sections to avoid long lines
    bucket_tech_section = technical_section if focus_area in ['buckets', 'all'] else ''
    table_tech_section = technical_section if focus_area in ['tables', 'all'] else ''

    return [
        Message(
            role='user',
            content=f"""Generate comprehensive, business-friendly descriptions for all tables and buckets
in this Keboola project.
{focus_instruction}

**Requirements:**
Create clear, informative descriptions that help users understand:
1. What data each bucket/table contains
2. The business purpose and use cases
3. Data lineage and relationships
4. Quality and completeness indicators

**Structure your output as follows:**

## Bucket Descriptions
For each bucket, provide:
• **Bucket Name**: [bucket.name]
• **Purpose**: Business purpose and data category
• **Contents**: Types of tables and data contained
• **Use Cases**: How this data is typically used
• **Data Sources**: Where the data originates from{bucket_tech_section}

## Table Descriptions
For each table, provide:
• **Table Name**: [bucket.table]
• **Description**: Clear business description of the data
• **Key Columns**: Most important fields and their meanings
• **Data Quality**: Completeness, accuracy, and freshness indicators
• **Relationships**: How it connects to other tables
• **Business Value**: Why this data matters and how it's used{table_tech_section}

## Summary
• Overall data architecture insights
• Recommendations for improving descriptions
• Suggestions for better data organization

Please analyze the actual project data and provide specific, actionable descriptions for each component."""
        )
    ]


async def debug_transformation(transformation_name: str) -> List[Message]:
    """Generate a prompt to help debug a specific transformation.

    Provides debugging assistance for transformation logic, SQL errors, performance
    problems, and optimization strategies.
    """
    return [
        Message(
            role='user',
            content=f"""I need help debugging a Keboola transformation called "{transformation_name}".

Please help me:
1. Identify potential issues in the transformation logic
2. Check for common SQL errors or performance problems
3. Suggest optimization strategies
4. Recommend debugging approaches
5. Provide best practices for transformation development

What specific information would you need to effectively debug this transformation?"""
        )
    ]


async def create_data_pipeline_plan(
    source_description: str,
    target_description: str,
    requirements: str = ''
) -> List[Message]:
    """Generate a prompt to create a data pipeline plan.

    Creates a comprehensive data pipeline design based on source and target specifications
    with optional additional requirements.
    """
    requirements_text = f'\n\nAdditional requirements:\n{requirements}' if requirements else ''

    return [
        Message(
            role='user',
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


async def optimize_sql_query(sql_query: str, context: str = '') -> List[Message]:
    """Generate a prompt to optimize an SQL query for Keboola transformations.

    Analyzes the provided SQL query and suggests performance optimizations,
    best practices, and alternative approaches.
    """
    context_text = f'\n\nContext: {context}' if context else ''

    return [
        Message(
            role='user',
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
    component_type: str = 'unknown'
) -> List[Message]:
    """Generate a prompt to troubleshoot a component error.

    Provides comprehensive troubleshooting guidance for component errors including
    diagnosis, solutions, and prevention strategies.
    """
    return [
        Message(
            role='user',
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
