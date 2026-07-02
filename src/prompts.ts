import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

// Ported from prompts/keboola_prompts.py + prompts/add_prompts.py. Only the
// "one-click" prompts (no required parameters) are registered, matching
// add_keboola_prompts(). Each returns a single user message with fixed content;
// the prompt name is the function name and the description is its docstring.

type OneClickPrompt = {
  name: string;
  description: string;
  content: string;
};

const PROMPTS: OneClickPrompt[] = [
  {
    name: 'analyze_project_structure',
    description:
      'Generate a comprehensive analysis prompt for a Keboola project’s structure. ' +
      'This prompt analyzes the project’s components, data flow, buckets, tables, ' +
      'and configurations to provide insights into capabilities and applications.',
    content: `Based on the components that are being used and the data available from all
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

Please provide a comprehensive analysis with specific examples and names from the actual project data.`,
  },
  {
    name: 'project_health_check',
    description:
      'Generate a comprehensive health check analysis for the entire Keboola project. ' +
      'This one-click prompt analyzes project health, identifies issues, and provides recommendations.',
    content: `Perform a comprehensive health check of this Keboola project and identify
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

Please provide specific findings with component and table names and actionable recommendations.`,
  },
  {
    name: 'data_quality_assessment',
    description:
      'Generate a comprehensive data quality assessment for all project data. ' +
      'One-click analysis of data quality across all buckets and tables.',
    content: `Conduct a comprehensive data quality assessment across all data in this Keboola project.

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
metrics, and actionable recommendations.`,
  },
  {
    name: 'component_usage_summary',
    description:
      'Generate a comprehensive summary of all components and their usage patterns. ' +
      'One-click overview of project components, configurations, and usage analytics.',
    content: `Generate a comprehensive summary of all components in this Keboola
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
actionable insights for project optimization.`,
  },
  {
    name: 'error_analysis_report',
    description:
      'Generate an analysis of recent errors and failures across the project. ' +
      'One-click error analysis with troubleshooting recommendations.',
    content: `Analyze recent errors and failures across this Keboola project and
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
instances with component names and detailed troubleshooting guidance.`,
  },
  {
    name: 'create_project_documentation',
    description:
      'Generate comprehensive project documentation automatically. ' +
      'One-click documentation creation for the entire Keboola project.',
    content: `Generate comprehensive, professional documentation for this Keboola
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
including specific names, configurations, and real examples.`,
  },
];

/** Registers the Keboola one-click prompts (port of add_keboola_prompts). */
export const registerPrompts = (server: McpServer): void => {
  for (const prompt of PROMPTS) {
    server.registerPrompt(
      prompt.name,
      { title: prompt.name, description: prompt.description },
      () => ({
        messages: [{ role: 'user', content: { type: 'text', text: prompt.content } }],
      }),
    );
  }
};
