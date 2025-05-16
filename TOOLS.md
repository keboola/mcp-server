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
- [create_sql_transformation](#create_sql_transformation): Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
description, and optionally a list of created table names if and only if they are generated within the SQL
statements.
- [get_component_details](#get_component_details): Gets detailed information about a specific Keboola component configuration given component/transformation ID and
configuration ID.
- [retrieve_components](#retrieve_components): Retrieves components configurations in the project, optionally filtered by component types or specific component IDs
If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding
component_types.
- [retrieve_transformations](#retrieve_transformations): Retrieves transformations configurations in the project, optionally filtered by specific transformation IDs.
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
  "title": "get_bucket_detailArguments",
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
  "title": "get_table_detailArguments",
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
  "title": "retrieve_bucket_tablesArguments",
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
  "title": "retrieve_bucketsArguments",
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
  "title": "update_bucket_descriptionArguments",
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
  "title": "update_column_descriptionArguments",
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
  "title": "update_table_descriptionArguments",
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
  "title": "get_sql_dialectArguments",
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
  "title": "query_tableArguments",
  "type": "object"
}
```

---

# Component Tools
<a name="create_sql_transformation"></a>
## create_sql_transformation
**Description**:

Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
description, and optionally a list of created table names if and only if they are generated within the SQL
statements.

CONSIDERATIONS:
- The SQL query statement is executable and must follow the current SQL dialect, which can be retrieved using
  appropriate tool.
- When referring to the input tables within the SQL query, use fully qualified table names, which can be
  retrieved using appropriate tools.
- When creating a new table within the SQL query (e.g. CREATE TABLE ...), use only the quoted table name without
  fully qualified table name, and add the plain table name without quotes to the `created_table_names` list.
- Unless otherwise specified by user, transformation name and description are generated based on the sql query
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
    "sql_statements": {
      "description": "The executable SQL query statements written in the current SQL dialect. Each statement should be a separate item in the list.",
      "items": {
        "type": "string"
      },
      "title": "Sql Statements",
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
    "sql_statements"
  ],
  "title": "create_sql_transformationArguments",
  "type": "object"
}
```

---
<a name="get_component_details"></a>
## get_component_details
**Description**:

Gets detailed information about a specific Keboola component configuration given component/transformation ID and
configuration ID.

USAGE:
- Use when you want to see the details of a specific component/transformation configuration.

EXAMPLES:
- user_input: `give me details about this configuration`
    - set component_id and configuration_id to the specific component/transformation ID and configuration ID
      if you know it
    - returns the details of the component/transformation configuration pair


**Input JSON Schema**:
```json
{
  "properties": {
    "component_id": {
      "description": "Unique identifier of the Keboola component/transformation",
      "title": "Component Id",
      "type": "string"
    },
    "configuration_id": {
      "description": "Unique identifier of the Keboola component/transformation configuration you want details about",
      "title": "Configuration Id",
      "type": "string"
    }
  },
  "required": [
    "component_id",
    "configuration_id"
  ],
  "title": "get_component_configuration_detailsArguments",
  "type": "object"
}
```

---
<a name="retrieve_components"></a>
## retrieve_components
**Description**:

Retrieves components configurations in the project, optionally filtered by component types or specific component IDs
If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding
component_types.

USAGE:
- Use when you want to see components configurations in the project for given component_types.
- Use when you want to see components configurations in the project for given component_ids.

EXAMPLES:
- user_input: `give me all components`
    - returns all components configurations in the project
- user_input: `list me all extractor components`
    - set types to ["extractor"]
    - returns all extractor components configurations in the project
- user_input: `give me configurations for following component/s` | `give me configurations for this component`
    - set component_ids to list of identifiers accordingly if you know them
    - returns all configurations for the given components
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
  "title": "retrieve_components_configurationsArguments",
  "type": "object"
}
```

---
<a name="retrieve_transformations"></a>
## retrieve_transformations
**Description**:

Retrieves transformations configurations in the project, optionally filtered by specific transformation IDs.

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
  "title": "retrieve_transformations_configurationsArguments",
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
- The configuration JSON data must follow the current Keboola transformation configuration schema.
- The SQL code statements should follow the current SQL dialect, which can be retrieved using appropriate tool.
- When the behavior of the transformation is not changed, the updated_description can be empty string.

EXAMPLES:
- user_input: `Can you edit this transformation configuration that [USER INTENT]?`
    - set the transformation_id and configuration_id accordingly and update configuration parameters based on
      the [USER INTENT]
    - returns the updated transformation configuration if successful.


**Input JSON Schema**:
```json
{
  "properties": {
    "configuration_id": {
      "description": "Unique identifier of the Keboola transformation configuration you want to update",
      "title": "Configuration Id",
      "type": "string"
    },
    "change_description": {
      "description": "Detailed description of the new changes to the transformation configuration.",
      "title": "Change Description",
      "type": "string"
    },
    "updated_configuration": {
      "description": "Updated transformation configuration JSON object containing both updated settings applied and all existing settings preserved.",
      "title": "Updated Configuration",
      "type": "object"
    },
    "updated_description": {
      "default": "",
      "description": "Updated existing transformation description reflecting the changes made in the behavior of the transformation. If no behavior changes are made, empty string preserves the original description.",
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
    "updated_configuration"
  ],
  "title": "update_sql_transformation_configurationArguments",
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
  "title": "get_job_detailArguments",
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
        "created"
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
  "title": "retrieve_jobsArguments",
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
  "title": "start_jobArguments",
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
  "title": "docs_queryArguments",
  "type": "object"
}
```

---
