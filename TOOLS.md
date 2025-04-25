# Tools Documentation
This document provides details about the tools available in the MCP server.

## Index
- [get_component_details](#get_component_details)
- [retrieve_components](#retrieve_components)
- [retrieve_transformations](#retrieve_transformations)
- [create_sql_transformation](#create_sql_transformation)
- [docs_query](#docs_query)
- [retrieve_jobs](#retrieve_jobs)
- [get_job_detail](#get_job_detail)
- [start_job](#start_job)
- [get_bucket_detail](#get_bucket_detail)
- [retrieve_buckets](#retrieve_buckets)
- [get_table_detail](#get_table_detail)
- [retrieve_bucket_tables](#retrieve_bucket_tables)
- [update_bucket_description](#update_bucket_description)
- [update_table_description](#update_table_description)
- [query_table](#query_table)
- [get_sql_dialect](#get_sql_dialect)
<a name="get_component_details"></a>
## get_component_details
**Description**: 
    Gets detailed information about a specific Keboola component configuration given component/transformation ID and
    configuration ID.
    USAGE:
        - Use when you want to see the details of a specific component/transformation configuration.
    EXAMPLES:
        - user_input: `give me details about this configuration`
            -> set component_id and configuration_id to the specific component/transformation ID and configuration ID
            if you know it
            -> returns the details of the component/transformation configuration pair
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

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
            -> returns all components configurations in the project
        - user_input: `list me all extractor components`
            -> set types to ["extractor"]
            -> returns all extractor components configurations in the project
        - user_input: `give me configurations for following component/s` | `give me configurations for this component`
            -> set component_ids to list of identifiers accordingly if you know them
            -> returns all configurations for the given components
        - user_input: `give me configurations for 'specified-id'`
            -> set component_ids to ['specified-id']
            -> returns the configurations of the component with ID 'specified-id'
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

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
            -> returns all transformation configurations in the project
        - user_input: `give me configurations for following transformation/s` | `give me configurations for
        this transformation`
            -> set transformation_ids to list of identifiers accordingly if you know the IDs
            -> returns all transformation configurations for the given transformations IDs
        - user_input: `list me transformations for this transformation component 'specified-id'`
            -> set transformation_ids to ['specified-id']
            -> returns the transformation configurations with ID 'specified-id'
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
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
            -> set the sql_statements to the query, and set other parameters accordingly.
            -> returns the created SQL transformation configuration if successful.
        - user_input: `Generate me an SQL transformation which [USER INTENT]`
            -> set the sql_statements to the query based on the [USER INTENT], and set other parameters accordingly.
            -> returns the created SQL transformation configuration if successful.
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="docs_query"></a>
## docs_query
**Description**: 
    Answers a question using the Keboola documentation as a source.
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="retrieve_jobs"></a>
## retrieve_jobs
**Description**: 
    Retrieve all jobs in the project, or filter jobs by a specific component_id or config_id, with optional status
    filtering. Additional parameters support pagination (limit, offset) and sorting (sort_by, sort_order).
    USAGE:
        Use when you want to list jobs for given component_id and optionally for given config_id.
        Use when you want to list all jobs in the project or filter them by status.
    EXAMPLES:
        - if status = "error", only jobs with status "error" will be listed.
        - if status = None, then all jobs with arbitrary status will be listed.
        - if component_id = "123" and config_id = "456", then the jobs for the component with id "123" and configuration
          with id "456" will be listed.
        - if limit = 100 and offset = 0, the first 100 jobs will be listed.
        - if limit = 100 and offset = 100, the second 100 jobs will be listed.
        - if sort_by = "endTime" and sort_order = "asc", the jobs will be sorted by the end time in ascending order.
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="get_job_detail"></a>
## get_job_detail
**Description**: 
    Retrieve a detailed information about a specific job, identified by the job_id, including its status, parameters,
    results, and any relevant metadata.
    EXAMPLES:
        - if job_id = "123", then the details of the job with id "123" will be retrieved.
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="start_job"></a>
## start_job
**Description**: 
    Starts a new job for a given component or transformation.
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="get_bucket_detail"></a>
## get_bucket_detail
**Description**: Gets detailed information about a specific bucket.

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="retrieve_buckets"></a>
## retrieve_buckets
**Description**: Retrieves information about all buckets in the project.

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="get_table_detail"></a>
## get_table_detail
**Description**: Gets detailed information about a specific table including its DB identifier and column information.

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="retrieve_bucket_tables"></a>
## retrieve_bucket_tables
**Description**: Retrieves all tables in a specific bucket with their basic information.

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="update_bucket_description"></a>
## update_bucket_description
**Description**: Update the description for a given Keboola bucket.

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="update_table_description"></a>
## update_table_description
**Description**: Update the description for a given Keboola table.

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

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
    

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
<a name="get_sql_dialect"></a>
## get_sql_dialect
**Description**: Gets the name of the SQL dialect used by Keboola project's underlying database.

### Parameters
- **name**: <class 'str'> - None
- **description**: str | None - None
- **inputSchema**: dict[str, typing.Any] - None

**Input JSON Schema**:
```json
{
  "additionalProperties": true,
  "description": "Definition for a tool the client can call.",
  "properties": {
    "name": {
      "title": "Name",
      "type": "string"
    },
    "description": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Description"
    },
    "inputSchema": {
      "title": "Inputschema",
      "type": "object"
    }
  },
  "required": [
    "name",
    "inputSchema"
  ],
  "title": "Tool",
  "type": "object"
}
```

**Return type**: Unknown

---
