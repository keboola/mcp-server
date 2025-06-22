# Tools Documentation
This document provides details about the tools available in the Keboola MCP server.

## Index

### Storage Tools
- [get_bucket_detail](#get_bucket_detail): Gets detailed information about a specific bucket.
- [get_table_detail](#get_table_detail): Gets detailed information about a specific table including its DB identifier and column information.
- [retrieve_bucket_tables](#retrieve_bucket_tables): Retrieves all tables in a specific bucket with their basic information.
- [retrieve_buckets](#retrieve_buckets): Retrieves information about all buckets in the project.
- [update_bucket_description](#update_bucket_description): Update the description for a given Keboola bucket.
- [update_column_description](#update_column_description): Update the description for a given column in a Keboola table.
- [update_table_description](#update_table_description): Update the description for a given Keboola table.

### SQL Tools
- [get_sql_dialect](#get_sql_dialect): Gets the name of the SQL dialect used by Keboola project's underlying database.
- [query_table](#query_table): Executes an SQL SELECT query to get the data from the underlying database.

### Component Tools
- [create_component_root_configuration](#create_component_root_configuration): Creates a component configuration using the specified name, component ID, configuration JSON, and description.
- [create_component_row_configuration](#create_component_row_configuration): Creates a component configuration row in the specified configuration_id, using the specified name,
component ID, configuration JSON, and description.
- [create_flow](#create_flow): Creates a new flow configuration in Keboola.
- [create_sql_transformation](#create_sql_transformation): Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
description, and optionally a list of created table names if and only if they are generated within the SQL
statements.
- [find_component_id](#find_component_id): Returns list of component IDs that match the given query.
- [get_component](#get_component): Gets information about a specific component given its ID.
- [get_component_configuration](#get_component_configuration): Gets information about a specific component/transformation configuration.
- [get_component_configuration_examples](#get_component_configuration_examples): Retrieves sample configuration examples for a specific component.
- [get_flow_detail](#get_flow_detail): Gets detailed information about a specific flow configuration.
- [get_flow_schema](#get_flow_schema): Returns the JSON schema that defines the structure of Flow configurations.
- [retrieve_components_configurations](#retrieve_components_configurations): Retrieves configurations of components present in the project,
optionally filtered by component types or specific component IDs.
- [retrieve_flows](#retrieve_flows): Retrieves flow configurations from the project.
- [retrieve_transformations](#retrieve_transformations): Retrieves transformation configurations in the project, optionally filtered by specific transformation IDs.
- [update_component_root_configuration](#update_component_root_configuration): Updates a specific component configuration using given by component ID, and configuration ID.
- [update_component_row_configuration](#update_component_row_configuration): Updates a specific component configuration row in the specified configuration_id, using the specified name,
component ID, configuration JSON, and description.
- [update_flow](#update_flow): Updates an existing flow configuration in Keboola.
- [update_sql_transformation_configuration](#update_sql_transformation_configuration): Updates an existing SQL transformation configuration, optionally updating the description and disabling the
configuration.

### Jobs Tools
- [get_job_detail](#get_job_detail): Retrieves detailed information about a specific job, identified by the job_id, including its status, parameters,
results, and any relevant metadata.
- [retrieve_jobs](#retrieve_jobs): Retrieves all jobs in the project, or filter jobs by a specific component_id or config_id, with optional status
filtering.
- [start_job](#start_job): Starts a new job for a given component or transformation.

### Documentation Tools
- [docs_query](#docs_query): Answers a question using the Keboola documentation as a source.

### Other Tools
- [get_project_info](#get_project_info): Return structured project information pulled from multiple endpoints.

---

# Storage Tools
<a name="get_bucket_detail"></a>
## get_bucket_detail
**Description**:

Gets detailed information about a specific bucket.


**Input JSON Schema**:
```json
{
  "properties": {
    "bucket_id": {
      "description": "Unique ID of the bucket.",
      "title": "Bucket Id",
      "type": "string"
    }
  },
  "required": [
    "bucket_id"
  ],
  "type": "object"
}
```

---
<a name="get_table_detail"></a>
## get_table_detail
**Description**:

Gets detailed information about a specific table including its DB identifier and column information.


**Input JSON Schema**:
```json
{
  "properties": {
    "table_id": {
      "description": "Unique ID of the table.",
      "title": "Table Id",
      "type": "string"
    }
  },
  "required": [
    "table_id"
  ],
  "type": "object"
}
```

---
<a name="retrieve_bucket_tables"></a>
## retrieve_bucket_tables
**Description**:

Retrieves all tables in a specific bucket with their basic information.


**Input JSON Schema**:
```json
{
  "properties": {
    "bucket_id": {
      "description": "Unique ID of the bucket.",
      "title": "Bucket Id",
      "type": "string"
    }
  },
  "required": [
    "bucket_id"
  ],
  "type": "object"
}
```

---
<a name="retrieve_buckets"></a>
## retrieve_buckets
**Description**:

Retrieves information about all buckets in the project.


**Input JSON Schema**:
```json
{
  "properties": {},
  "type": "object"
}
```

---
<a name="update_bucket_description"></a>
## update_bucket_description
**Description**:

Update the description for a given Keboola bucket.


**Input JSON Schema**:
```json
{
  "properties": {
    "bucket_id": {
      "description": "The ID of the bucket to update.",
      "title": "Bucket Id",
      "type": "string"
    },
    "description": {
      "description": "The new description for the bucket.",
      "title": "Description",
      "type": "string"
    }
  },
  "required": [
    "bucket_id",
    "description"
  ],
  "type": "object"
}
```

---
<a name="update_column_description"></a>
## update_column_description
**Description**:

Update the description for a given column in a Keboola table.


**Input JSON Schema**:
```json
{
  "properties": {
    "table_id": {
      "description": "The ID of the table that contains the column.",
      "title": "Table Id",
      "type": "string"
    },
    "column_name": {
      "description": "The name of the column to update.",
      "title": "Column Name",
      "type": "string"
    },
    "description": {
      "description": "The new description for the column.",
      "title": "Description",
      "type": "string"
    }
  },
  "required": [
    "table_id",
    "column_name",
    "description"
  ],
  "type": "object"
}
```

---
<a name="update_table_description"></a>
## update_table_description
**Description**:

Update the description for a given Keboola table.


**Input JSON Schema**:
```json
{
  "properties": {
    "table_id": {
      "description": "The ID of the table to update.",
      "title": "Table Id",
      "type": "string"
    },
    "description": {
      "description": "The new description for the table.",
      "title": "Description",
      "type": "string"
    }
  },
  "required": [
    "table_id",
    "description"
  ],
  "type": "object"
}
```

---

# SQL Tools
<a name="get_sql_dialect"></a>
## get_sql_dialect
**Description**:

Gets the name of the SQL dialect used by Keboola project's underlying database.


**Input JSON Schema**:
```json
{
  "properties": {},
  "type": "object"
}
```

---
<a name="query_table"></a>
## query_table
**Description**:

Executes an SQL SELECT query to get the data from the underlying database.
* When constructing the SQL SELECT query make sure to check the SQL dialect
  used by the Keboola project's underlying database.
* When referring to tables always use fully qualified table names that include the database name,
  schema name and the table name.
* The fully qualified table name can be found in the table information, use a tool to get the information
  about tables. The fully qualified table name can be found in the response from that tool.
* Always use quoted column names when referring to table columns. The quoted column names can also be found
  in the response from the table information tool.


**Input JSON Schema**:
```json
{
  "properties": {
    "sql_query": {
      "description": "SQL SELECT query to run.",
      "title": "Sql Query",
      "type": "string"
    }
  },
  "required": [
    "sql_query"
  ],
  "type": "object"
}
```

---

# Component Tools
<a name="create_component_root_configuration"></a>
## create_component_root_configuration
**Description**:

Creates a component configuration using the specified name, component ID, configuration JSON, and description.

CONSIDERATIONS:
- The configuration JSON object must follow the root_configuration_schema of the specified component.
- Make sure the configuration parameters always adhere to the root_configuration_schema,
  which is available via the component_detail tool.
- The configuration JSON object should adhere to the component's configuration examples if found.

USAGE:
- Use when you want to create a new root configuration for a specific component.

EXAMPLES:
- user_input: `Create a new configuration for component X with these settings`
    - set the component_id and configuration parameters accordingly
    - returns the created component configuration if successful.


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "A short, descriptive name summarizing the purpose of the component configuration.",
      "title": "Name",
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the component configuration explaining its purpose and functionality.",
      "title": "Description",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component for which to create the configuration.",
      "title": "Component Id",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": true,
      "description": "The component configuration parameters, adhering to the root_configuration_schema",
      "title": "Parameters",
      "type": "object"
    },
    "storage": {
      "additionalProperties": true,
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that have tables or file input mapping defined",
      "title": "Storage",
      "type": "object"
    }
  },
  "required": [
    "name",
    "description",
    "component_id",
    "parameters"
  ],
  "type": "object"
}
```

---
<a name="create_component_row_configuration"></a>
## create_component_row_configuration
**Description**:

Creates a component configuration row in the specified configuration_id, using the specified name,
component ID, configuration JSON, and description.

CONSIDERATIONS:
- The configuration JSON object must follow the row_configuration_schema of the specified component.
- Make sure the configuration parameters always adhere to the row_configuration_schema,
  which is available via the component_detail tool.
- The configuration JSON object should adhere to the component's configuration examples if found.

USAGE:
- Use when you want to create a new row configuration for a specific component configuration.

EXAMPLES:
- user_input: `Create a new configuration for component X with these settings`
    - set the component_id, configuration_id and configuration parameters accordingly
    - returns the created component configuration if successful.


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "A short, descriptive name summarizing the purpose of the component configuration.",
      "title": "Name",
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the component configuration explaining its purpose and functionality.",
      "title": "Description",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component for which to create the configuration.",
      "title": "Component Id",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the configuration for which to create the configuration row.",
      "title": "Configuration Id",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": true,
      "description": "The component row configuration parameters, adhering to the row_configuration_schema",
      "title": "Parameters",
      "type": "object"
    },
    "storage": {
      "additionalProperties": true,
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that have tables or file input mapping defined",
      "title": "Storage",
      "type": "object"
    }
  },
  "required": [
    "name",
    "description",
    "component_id",
    "configuration_id",
    "parameters"
  ],
  "type": "object"
}
```

---
<a name="create_flow"></a>
## create_flow
**Description**:

Creates a new flow configuration in Keboola.
A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
Each flow is composed of:
- Tasks: individual component configurations (e.g., extractors, writers, transformations).
- Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

CONSIDERATIONS:
- The `phases` and `tasks` parameters must conform to the Keboola Flow JSON schema.
- Each task and phase must include at least: `id` and `name`.
- Each task must reference an existing component configuration in the project.
- Items in the `dependsOn` phase field reference ids of other phases.
- Links contained in the response should ALWAYS be presented to the user

USAGE:
Use this tool to automate multi-step data workflows. This is ideal for:
- Creating ETL/ELT orchestration.
- Coordinating dependencies between components.
- Structuring parallel and sequential task execution.

EXAMPLES:
- user_input: Orchestrate all my JIRA extractors.
    - fill `tasks` parameter with the tasks for the JIRA extractors
    - determine dependencies between the JIRA extractors
    - fill `phases` parameter by grouping tasks into phases


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "A short, descriptive name for the flow.",
      "title": "Name",
      "type": "string"
    },
    "description": {
      "description": "Detailed description of the flow purpose.",
      "title": "Description",
      "type": "string"
    },
    "phases": {
      "description": "List of phase definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "title": "Phases",
      "type": "array"
    },
    "tasks": {
      "description": "List of task definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "title": "Tasks",
      "type": "array"
    }
  },
  "required": [
    "name",
    "description",
    "phases",
    "tasks"
  ],
  "type": "object"
}
```

---
<a name="create_sql_transformation"></a>
## create_sql_transformation
**Description**:

Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
description, and optionally a list of created table names if and only if they are generated within the SQL
statements.

CONSIDERATIONS:
- Each SQL code block must include descriptive name that reflects its purpose and group one or more executable
  semantically related SQL statements.
- Each SQL query statement must be executable and follow the current SQL dialect, which can be retrieved using
  appropriate tool.
- When referring to the input tables within the SQL query, use fully qualified table names, which can be
  retrieved using appropriate tools.
- When creating a new table within the SQL query (e.g. CREATE TABLE ...), use only the quoted table name without
  fully qualified table name, and add the plain table name without quotes to the `created_table_names` list.
- Unless otherwise specified by user, transformation name and description are generated based on the SQL query
  and user intent.

USAGE:
- Use when you want to create a new SQL transformation.

EXAMPLES:
- user_input: `Can you save me the SQL query you generated as a new transformation?`
    - set the sql_statements to the query, and set other parameters accordingly.
    - returns the created SQL transformation configuration if successful.
- user_input: `Generate me an SQL transformation which [USER INTENT]`
    - set the sql_statements to the query based on the [USER INTENT], and set other parameters accordingly.
    - returns the created SQL transformation configuration if successful.


**Input JSON Schema**:
```json
{
  "$defs": {
    "Code": {
      "description": "The code block for the transformation block.",
      "properties": {
        "name": {
          "description": "The name of the current code block describing the purpose of the block",
          "title": "Name",
          "type": "string"
        },
        "sql_statements": {
          "description": "The executable SQL query statements written in the current SQL dialect. Each statement must be executable and a separate item in the list.",
          "items": {
            "type": "string"
          },
          "title": "Sql Statements",
          "type": "array"
        }
      },
      "required": [
        "name",
        "sql_statements"
      ],
      "title": "Code",
      "type": "object"
    }
  },
  "properties": {
    "name": {
      "description": "A short, descriptive name summarizing the purpose of the SQL transformation.",
      "title": "Name",
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the SQL transformation capturing the user intent, explaining the SQL query, and the expected output.",
      "title": "Description",
      "type": "string"
    },
    "sql_code_blocks": {
      "description": "The executable SQL query code blocks, each containing a descriptive name and a sequence of semantically related sql statements written in the current SQL dialect. Each sql statement isexecutable and a separate item in the list of sql statements.",
      "items": {
        "$ref": "#/$defs/Code"
      },
      "title": "Sql Code Blocks",
      "type": "array"
    },
    "created_table_names": {
      "default": [],
      "description": "An empty list or a list of created table names if and only if they are generated within SQL statements (e.g., using `CREATE TABLE ...`).",
      "items": {
        "type": "string"
      },
      "title": "Created Table Names",
      "type": "array"
    }
  },
  "required": [
    "name",
    "description",
    "sql_code_blocks"
  ],
  "type": "object"
}
```

---
<a name="find_component_id"></a>
## find_component_id
**Description**:

Returns list of component IDs that match the given query.

USAGE:
- Use when you want to find the component for a specific purpose.

EXAMPLES:
- user_input: `I am looking for a salesforce extractor component`
    - returns a list of component IDs that match the query, ordered by relevance/best match.


**Input JSON Schema**:
```json
{
  "properties": {
    "query": {
      "description": "Natural language query to find the requested component.",
      "title": "Query",
      "type": "string"
    }
  },
  "required": [
    "query"
  ],
  "type": "object"
}
```

---
<a name="get_component"></a>
## get_component
**Description**:

Gets information about a specific component given its ID.

USAGE:
- Use when you want to see the details of a specific component to get its documentation, configuration schemas,
  etc. Especially in situation when the users asks to create or update a component configuration.
  This tool is mainly for internal use by the agent.

EXAMPLES:
- user_input: `Create a generic extractor configuration for x`
    - Set the component_id if you know it or find the component_id by find_component_id
      or docs use tool and set it
    - returns the component


**Input JSON Schema**:
```json
{
  "properties": {
    "component_id": {
      "description": "ID of the component/transformation",
      "title": "Component Id",
      "type": "string"
    }
  },
  "required": [
    "component_id"
  ],
  "type": "object"
}
```

---
<a name="get_component_configuration"></a>
## get_component_configuration
**Description**:

Gets information about a specific component/transformation configuration.

USAGE:
- Use when you want to see the configuration of a specific component/transformation.

EXAMPLES:
- user_input: `give me details about this configuration`
    - set component_id and configuration_id to the specific component/transformation ID and configuration ID
      if you know it
    - returns the component/transformation configuration pair


**Input JSON Schema**:
```json
{
  "properties": {
    "component_id": {
      "description": "ID of the component/transformation",
      "title": "Component Id",
      "type": "string"
    },
    "configuration_id": {
      "description": "ID of the component/transformation configuration",
      "title": "Configuration Id",
      "type": "string"
    }
  },
  "required": [
    "component_id",
    "configuration_id"
  ],
  "type": "object"
}
```

---
<a name="get_component_configuration_examples"></a>
## get_component_configuration_examples
**Description**:

Retrieves sample configuration examples for a specific component.

USAGE:
- Use when you want to see example configurations for a specific component.

EXAMPLES:
- user_input: `Show me example configurations for component X`
    - set the component_id parameter accordingly
    - returns a markdown formatted string with configuration examples


**Input JSON Schema**:
```json
{
  "properties": {
    "component_id": {
      "description": "The ID of the component to get configuration examples for.",
      "title": "Component Id",
      "type": "string"
    }
  },
  "required": [
    "component_id"
  ],
  "type": "object"
}
```

---
<a name="get_flow_detail"></a>
## get_flow_detail
**Description**:

Gets detailed information about a specific flow configuration.


**Input JSON Schema**:
```json
{
  "properties": {
    "configuration_id": {
      "description": "ID of the flow configuration to retrieve.",
      "title": "Configuration Id",
      "type": "string"
    }
  },
  "required": [
    "configuration_id"
  ],
  "type": "object"
}
```

---
<a name="get_flow_schema"></a>
## get_flow_schema
**Description**:

Returns the JSON schema that defines the structure of Flow configurations.


**Input JSON Schema**:
```json
{
  "properties": {},
  "type": "object"
}
```

---
<a name="retrieve_components_configurations"></a>
## retrieve_components_configurations
**Description**:

Retrieves configurations of components present in the project,
optionally filtered by component types or specific component IDs.
If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding
component_types.

USAGE:
- Use when you want to see components configurations in the project for given component_types.
- Use when you want to see components configurations in the project for given component_ids.

EXAMPLES:
- user_input: `give me all components (in the project)`
    - returns all components configurations in the project
- user_input: `list me all extractor components (in the project)`
    - set types to ["extractor"]
    - returns all extractor components configurations in the project
- user_input: `give me configurations for following component/s` | `give me configurations for this component`
    - set component_ids to list of identifiers accordingly if you know them
    - returns all configurations for the given components in the project
- user_input: `give me configurations for 'specified-id'`
    - set component_ids to ['specified-id']
    - returns the configurations of the component with ID 'specified-id'


**Input JSON Schema**:
```json
{
  "properties": {
    "component_types": {
      "default": [],
      "description": "List of component types to filter by. If none, return all components.",
      "items": {
        "enum": [
          "application",
          "extractor",
          "writer"
        ],
        "type": "string"
      },
      "title": "Component Types",
      "type": "array"
    },
    "component_ids": {
      "default": [],
      "description": "List of component IDs to retrieve configurations for. If none, return all components.",
      "items": {
        "type": "string"
      },
      "title": "Component Ids",
      "type": "array"
    }
  },
  "type": "object"
}
```

---
<a name="retrieve_flows"></a>
## retrieve_flows
**Description**:

Retrieves flow configurations from the project.


**Input JSON Schema**:
```json
{
  "properties": {
    "flow_ids": {
      "description": "The configuration IDs of the flows to retrieve.",
      "items": {
        "type": "string"
      },
      "title": "Flow Ids",
      "type": "array"
    }
  },
  "type": "object"
}
```

---
<a name="retrieve_transformations"></a>
## retrieve_transformations
**Description**:

Retrieves transformation configurations in the project, optionally filtered by specific transformation IDs.

USAGE:
- Use when you want to see transformation configurations in the project for given transformation_ids.
- Use when you want to retrieve all transformation configurations, then set transformation_ids to an empty list.

EXAMPLES:
- user_input: `give me all transformations`
    - returns all transformation configurations in the project
- user_input: `give me configurations for following transformation/s` | `give me configurations for
  this transformation`
- set transformation_ids to list of identifiers accordingly if you know the IDs
    - returns all transformation configurations for the given transformations IDs
- user_input: `list me transformations for this transformation component 'specified-id'`
    - set transformation_ids to ['specified-id']
    - returns the transformation configurations with ID 'specified-id'


**Input JSON Schema**:
```json
{
  "properties": {
    "transformation_ids": {
      "default": [],
      "description": "List of transformation component IDs to retrieve configurations for.",
      "items": {
        "type": "string"
      },
      "title": "Transformation Ids",
      "type": "array"
    }
  },
  "type": "object"
}
```

---
<a name="update_component_root_configuration"></a>
## update_component_root_configuration
**Description**:

Updates a specific component configuration using given by component ID, and configuration ID.

CONSIDERATIONS:
- The configuration JSON object must follow the root_configuration_schema of the specified component.
- Make sure the configuration parameters always adhere to the root_configuration_schema,
  which is available via the component_detail tool.
- The configuration JSON object should adhere to the component's configuration examples if found

USAGE:
- Use when you want to update a root configuration of a specific component.

EXAMPLES:
- user_input: `Update a configuration for component X and configuration ID 1234 with these settings`
    - set the component_id, configuration_id and configuration parameters accordingly.
    - set the change_description to the description of the change made to the component configuration.
    - returns the updated component configuration if successful.


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "A short, descriptive name summarizing the purpose of the component configuration.",
      "title": "Name",
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the component configuration explaining its purpose and functionality.",
      "title": "Description",
      "type": "string"
    },
    "change_description": {
      "description": "Description of the change made to the component configuration.",
      "title": "Change Description",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component the configuration belongs to.",
      "title": "Component Id",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the configuration to update.",
      "title": "Configuration Id",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": true,
      "description": "The component configuration parameters, adhering to the root_configuration_schema schema",
      "title": "Parameters",
      "type": "object"
    },
    "storage": {
      "additionalProperties": true,
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that are not row-based and have tables or file input mapping defined",
      "title": "Storage",
      "type": "object"
    }
  },
  "required": [
    "name",
    "description",
    "change_description",
    "component_id",
    "configuration_id",
    "parameters"
  ],
  "type": "object"
}
```

---
<a name="update_component_row_configuration"></a>
## update_component_row_configuration
**Description**:

Updates a specific component configuration row in the specified configuration_id, using the specified name,
component ID, configuration JSON, and description.

CONSIDERATIONS:
- The configuration JSON object must follow the row_configuration_schema of the specified component.
- Make sure the configuration parameters always adhere to the row_configuration_schema,
  which is available via the component_detail tool.

USAGE:
- Use when you want to update a row configuration for a specific component and configuration.

EXAMPLES:
- user_input: `Update a configuration row of configuration ID 123 for component X with these settings`
    - set the component_id, configuration_id, configuration_row_id and configuration parameters accordingly
    - returns the updated component configuration if successful.


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "A short, descriptive name summarizing the purpose of the component configuration.",
      "title": "Name",
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the component configuration explaining its purpose and functionality.",
      "title": "Description",
      "type": "string"
    },
    "change_description": {
      "description": "Description of the change made to the component configuration.",
      "title": "Change Description",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component to update.",
      "title": "Component Id",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the configuration to update.",
      "title": "Configuration Id",
      "type": "string"
    },
    "configuration_row_id": {
      "description": "The ID of the configuration row to update.",
      "title": "Configuration Row Id",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": true,
      "description": "The component row configuration parameters, adhering to the row_configuration_schema",
      "title": "Parameters",
      "type": "object"
    },
    "storage": {
      "additionalProperties": true,
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that have tables or file input mapping defined",
      "title": "Storage",
      "type": "object"
    }
  },
  "required": [
    "name",
    "description",
    "change_description",
    "component_id",
    "configuration_id",
    "configuration_row_id",
    "parameters"
  ],
  "type": "object"
}
```

---
<a name="update_flow"></a>
## update_flow
**Description**:

Updates an existing flow configuration in Keboola.
A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
Each flow is composed of:
- Tasks: individual component configurations (e.g., extractors, writers, transformations).
- Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

CONSIDERATIONS:
- The `phases` and `tasks` parameters must conform to the Keboola Flow JSON schema.
- Each task and phase must include at least: `id` and `name`.
- Each task must reference an existing component configuration in the project.
- Items in the `dependsOn` phase field reference ids of other phases.
- The flow specified by `configuration_id` must already exist in the project.
- Links contained in the response should ALWAYS be presented to the user

USAGE:
Use this tool to update an existing flow.


**Input JSON Schema**:
```json
{
  "properties": {
    "configuration_id": {
      "description": "ID of the flow configuration to update.",
      "title": "Configuration Id",
      "type": "string"
    },
    "name": {
      "description": "Updated flow name.",
      "title": "Name",
      "type": "string"
    },
    "description": {
      "description": "Updated flow description.",
      "title": "Description",
      "type": "string"
    },
    "phases": {
      "description": "Updated list of phase definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "title": "Phases",
      "type": "array"
    },
    "tasks": {
      "description": "Updated list of task definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "title": "Tasks",
      "type": "array"
    },
    "change_description": {
      "description": "Description of changes made.",
      "title": "Change Description",
      "type": "string"
    }
  },
  "required": [
    "configuration_id",
    "name",
    "description",
    "phases",
    "tasks",
    "change_description"
  ],
  "type": "object"
}
```

---
<a name="update_sql_transformation_configuration"></a>
## update_sql_transformation_configuration
**Description**:

Updates an existing SQL transformation configuration, optionally updating the description and disabling the
configuration.

CONSIDERATIONS:
- The parameters configuration must include blocks with codes of SQL statements. Using one block with many codes of
  SQL statemetns is prefered and commonly used unless specified otherwise by the user.
- Each code contains SQL statements that are semantically related and have a descriptive name.
- Each SQL statement must be executable and follow the current SQL dialect, which can be retrieved using
  appropriate tool.
- The storage configuration must not be empty, and it should include input or output tables with correct mappings
  for the transformation.
- When the behavior of the transformation is not changed, the updated_description can be empty string.

EXAMPLES:
- user_input: `Can you edit this transformation configuration that [USER INTENT]?`
    - set the transformation configuration_id accordingly and update parameters and storage tool arguments based on
      the [USER INTENT]
    - returns the updated transformation configuration if successful.


**Input JSON Schema**:
```json
{
  "$defs": {
    "Block": {
      "description": "The transformation block.",
      "properties": {
        "name": {
          "description": "The name of the current block",
          "title": "Name",
          "type": "string"
        },
        "codes": {
          "description": "The code scripts",
          "items": {
            "$ref": "#/$defs/Code"
          },
          "title": "Codes",
          "type": "array"
        }
      },
      "required": [
        "name",
        "codes"
      ],
      "title": "Block",
      "type": "object"
    },
    "Code": {
      "description": "The code block for the transformation block.",
      "properties": {
        "name": {
          "description": "The name of the current code block describing the purpose of the block",
          "title": "Name",
          "type": "string"
        },
        "sql_statements": {
          "description": "The executable SQL query statements written in the current SQL dialect. Each statement must be executable and a separate item in the list.",
          "items": {
            "type": "string"
          },
          "title": "Sql Statements",
          "type": "array"
        }
      },
      "required": [
        "name",
        "sql_statements"
      ],
      "title": "Code",
      "type": "object"
    },
    "Parameters": {
      "description": "The parameters for the transformation.",
      "properties": {
        "blocks": {
          "description": "The blocks for the transformation",
          "items": {
            "$ref": "#/$defs/Block"
          },
          "title": "Blocks",
          "type": "array"
        }
      },
      "required": [
        "blocks"
      ],
      "title": "Parameters",
      "type": "object"
    }
  },
  "properties": {
    "configuration_id": {
      "description": "ID of the transformation configuration to update",
      "title": "Configuration Id",
      "type": "string"
    },
    "change_description": {
      "description": "Description of the changes made to the transformation configuration.",
      "title": "Change Description",
      "type": "string"
    },
    "parameters": {
      "$ref": "#/$defs/Parameters",
      "description": "The updated \"parameters\" part of the transformation configuration that contains the newly applied settings and preserves all other existing settings.",
      "title": "Parameters"
    },
    "storage": {
      "additionalProperties": true,
      "description": "The updated \"storage\" part of the transformation configuration that contains the newly applied settings and preserves all other existing settings.",
      "title": "Storage",
      "type": "object"
    },
    "updated_description": {
      "default": "",
      "description": "Updated transformation description reflecting the changes made in the behavior of the transformation. If no behavior changes are made, empty string preserves the original description.",
      "title": "Updated Description",
      "type": "string"
    },
    "is_disabled": {
      "default": false,
      "description": "Whether to disable the transformation configuration. Default is False.",
      "title": "Is Disabled",
      "type": "boolean"
    }
  },
  "required": [
    "configuration_id",
    "change_description",
    "parameters",
    "storage"
  ],
  "type": "object"
}
```

---

# Jobs Tools
<a name="get_job_detail"></a>
## get_job_detail
**Description**:

Retrieves detailed information about a specific job, identified by the job_id, including its status, parameters,
results, and any relevant metadata.

EXAMPLES:
- If job_id = "123", then the details of the job with id "123" will be retrieved.


**Input JSON Schema**:
```json
{
  "properties": {
    "job_id": {
      "description": "The unique identifier of the job whose details should be retrieved.",
      "title": "Job Id",
      "type": "string"
    }
  },
  "required": [
    "job_id"
  ],
  "type": "object"
}
```

---
<a name="retrieve_jobs"></a>
## retrieve_jobs
**Description**:

Retrieves all jobs in the project, or filter jobs by a specific component_id or config_id, with optional status
filtering. Additional parameters support pagination (limit, offset) and sorting (sort_by, sort_order).

USAGE:
- Use when you want to list jobs for a given component_id and optionally for given config_id.
- Use when you want to list all jobs in the project or filter them by status.

EXAMPLES:
- If status = "error", only jobs with status "error" will be listed.
- If status = None, then all jobs with arbitrary status will be listed.
- If component_id = "123" and config_id = "456", then the jobs for the component with id "123" and configuration
  with id "456" will be listed.
- If limit = 100 and offset = 0, the first 100 jobs will be listed.
- If limit = 100 and offset = 100, the second 100 jobs will be listed.
- If sort_by = "endTime" and sort_order = "asc", the jobs will be sorted by the end time in ascending order.


**Input JSON Schema**:
```json
{
  "properties": {
    "status": {
      "default": null,
      "description": "The optional status of the jobs to filter by, if None then default all.",
      "enum": [
        "waiting",
        "processing",
        "success",
        "error",
        "created",
        "warning",
        "terminating",
        "cancelled",
        "terminated"
      ],
      "title": "Status",
      "type": "string"
    },
    "component_id": {
      "default": null,
      "description": "The optional ID of the component whose jobs you want to list, default = None.",
      "title": "Component Id",
      "type": "string"
    },
    "config_id": {
      "default": null,
      "description": "The optional ID of the component configuration whose jobs you want to list, default = None.",
      "title": "Config Id",
      "type": "string"
    },
    "limit": {
      "default": 100,
      "description": "The number of jobs to list, default = 100, max = 500.",
      "maximum": 500,
      "minimum": 1,
      "title": "Limit",
      "type": "integer"
    },
    "offset": {
      "default": 0,
      "description": "The offset of the jobs to list, default = 0.",
      "minimum": 0,
      "title": "Offset",
      "type": "integer"
    },
    "sort_by": {
      "default": "startTime",
      "description": "The field to sort the jobs by, default = \"startTime\".",
      "enum": [
        "startTime",
        "endTime",
        "createdTime",
        "durationSeconds",
        "id"
      ],
      "title": "Sort By",
      "type": "string"
    },
    "sort_order": {
      "default": "desc",
      "description": "The order to sort the jobs by, default = \"desc\".",
      "enum": [
        "asc",
        "desc"
      ],
      "title": "Sort Order",
      "type": "string"
    }
  },
  "type": "object"
}
```

---
<a name="start_job"></a>
## start_job
**Description**:

Starts a new job for a given component or transformation.


**Input JSON Schema**:
```json
{
  "properties": {
    "component_id": {
      "description": "The ID of the component or transformation for which to start a job.",
      "title": "Component Id",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the configuration for which to start a job.",
      "title": "Configuration Id",
      "type": "string"
    }
  },
  "required": [
    "component_id",
    "configuration_id"
  ],
  "type": "object"
}
```

---

# Documentation Tools
<a name="docs_query"></a>
## docs_query
**Description**:

Answers a question using the Keboola documentation as a source.


**Input JSON Schema**:
```json
{
  "properties": {
    "query": {
      "description": "Natural language query to search for in the documentation.",
      "title": "Query",
      "type": "string"
    }
  },
  "required": [
    "query"
  ],
  "type": "object"
}
```

---

# Other Tools
<a name="get_project_info"></a>
## get_project_info
**Description**:

Return structured project information pulled from multiple endpoints.


**Input JSON Schema**:
```json
{
  "properties": {},
  "type": "object"
}
```

---
