# Tools Documentation
This document provides details about the tools available in the Keboola MCP server.

## Index

### Component Tools
- [add_config_row](#add_config_row): Creates a component configuration row in the specified configuration_id, using the specified name,
component ID, configuration JSON, and description.
- [create_config](#create_config): Creates a root component configuration using the specified name, component ID, configuration JSON, and description.
- [create_sql_transformation](#create_sql_transformation): Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
description, and a list of created table names.
- [get_component](#get_component): Gets information about a specific component given its ID.
- [get_config](#get_config): Gets information about a specific component/transformation configuration.
- [get_config_examples](#get_config_examples): Retrieves sample configuration examples for a specific component.
- [list_configs](#list_configs): Lists all component configurations in the project with optional filtering by component type or specific
component IDs.
- [update_config](#update_config): Updates an existing root component configuration by modifying its parameters, storage mappings, name or description.
- [update_config_row](#update_config_row): Updates an existing component configuration row by modifying its parameters, storage mappings, name, or description.
- [update_sql_transformation](#update_sql_transformation): Updates an existing SQL transformation configuration, optionally updating the description and disabling the
configuration.

### Documentation Tools
- [docs_query](#docs_query): Answers a question using the Keboola documentation as a source.

### Flow Tools
- [create_conditional_flow](#create_conditional_flow): Creates a new conditional flow configuration in Keboola.
- [create_flow](#create_flow): Creates a new flow configuration in Keboola.
- [get_flow](#get_flow): Gets detailed information about a specific flow configuration.
- [get_flow_examples](#get_flow_examples): Retrieves examples of valid flow configurations.
- [get_flow_schema](#get_flow_schema): Returns the JSON schema for the given flow type in markdown format.
- [list_flows](#list_flows): Retrieves flow configurations from the project.
- [update_flow](#update_flow): Updates an existing flow configuration in Keboola.

### Jobs Tools
- [get_job](#get_job): Retrieves detailed information about a specific job, identified by the job_id, including its status, parameters,
results, and any relevant metadata.
- [list_jobs](#list_jobs): Retrieves all jobs in the project, or filter jobs by a specific component_id or config_id, with optional status
filtering.
- [run_job](#run_job): Starts a new job for a given component or transformation.

### OAuth Tools
- [create_oauth_url](#create_oauth_url): Generates an OAuth authorization URL for a Keboola component configuration.

### Other Tools
- [deploy_data_app](#deploy_data_app): Deploys/redeploys a data app or stops running data app in the Keboola environment given the action and
configuration ID.
- [get_data_apps](#get_data_apps): Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by
providing their configuration IDs.
- [modify_data_app](#modify_data_app): Creates or updates a Streamlit data app.

### Project Tools
- [get_project_info](#get_project_info): Retrieves structured information about the current project,
including essential context and base instructions for working with it
(e.

### SQL Tools
- [query_data](#query_data): Executes an SQL SELECT query to get the data from the underlying database.

### Search Tools
- [find_component_id](#find_component_id): Returns list of component IDs that match the given query.

### Storage Tools
- [get_bucket](#get_bucket): Gets detailed information about a specific bucket.
- [get_table](#get_table): Gets detailed information about a specific table including its DB identifier and column information.
- [list_buckets](#list_buckets): Retrieves information about all buckets in the project.
- [list_tables](#list_tables): Retrieves all tables in a specific bucket with their basic information.
- [update_descriptions](#update_descriptions): Updates the description for a Keboola storage item.

---

# Component Tools
<a name="add_config_row"></a>
## add_config_row
**Annotations**: 

**Tags**: `components`

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
- user_input: `Create a new configuration row for component X with these settings`
    - set the component_id, configuration_id and configuration parameters accordingly
    - returns the created component configuration if successful.


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "A short, descriptive name summarizing the purpose of the component configuration.",
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the component configuration explaining its purpose and functionality.",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component for which to create the configuration.",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the configuration for which to create the configuration row.",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": true,
      "description": "The component row configuration parameters, adhering to the row_configuration_schema",
      "type": "object"
    },
    "storage": {
      "additionalProperties": true,
      "default": null,
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that have tables or file input mapping defined",
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
<a name="create_config"></a>
## create_config
**Annotations**: 

**Tags**: `components`

**Description**:

Creates a root component configuration using the specified name, component ID, configuration JSON, and description.

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
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the component configuration explaining its purpose and functionality.",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component for which to create the configuration.",
      "type": "string"
    },
    "parameters": {
      "additionalProperties": true,
      "description": "The component configuration parameters, adhering to the root_configuration_schema",
      "type": "object"
    },
    "storage": {
      "additionalProperties": true,
      "default": null,
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that have tables or file input mapping defined",
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
<a name="create_sql_transformation"></a>
## create_sql_transformation
**Annotations**: 

**Tags**: `components`

**Description**:

Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
description, and a list of created table names.

CONSIDERATIONS:
- By default, SQL transformation must create at least one table to produce a result; omit only if the user
  explicitly indicates that no table creation is needed.
- Each SQL code block must include descriptive name that reflects its purpose and group one or more executable
  semantically related SQL statements.
- Each SQL query statement within a code block must be executable and follow the current SQL dialect, which can be
  retrieved using appropriate tool.
- When referring to the input tables within the SQL query, use fully qualified table names, which can be
  retrieved using appropriate tools.
- When creating a new table within the SQL query (e.g. CREATE TABLE ...), use only the quoted table name without
  fully qualified table name, and add the plain table name without quotes to the `created_table_names` list.
- Unless otherwise specified by user, transformation name and description are generated based on the SQL query
  and user intent.

USAGE:
- Use when you want to create a new SQL transformation.

EXAMPLES:
- user_input: `Can you create a new transformation out of this sql query?`
    - set the sql_code_blocks to the query, and set other parameters accordingly.
    - returns the created SQL transformation configuration if successful.
- user_input: `Generate me an SQL transformation which [USER INTENT]`
    - set the sql_code_blocks to the query based on the [USER INTENT], and set other parameters accordingly.
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
          "type": "string"
        },
        "sql_statements": {
          "description": "The executable SQL query statements written in the current SQL dialect. Each statement must be executable and a separate item in the list.",
          "items": {
            "type": "string"
          },
          "type": "array"
        }
      },
      "required": [
        "name",
        "sql_statements"
      ],
      "type": "object"
    }
  },
  "properties": {
    "name": {
      "description": "A short, descriptive name summarizing the purpose of the SQL transformation.",
      "type": "string"
    },
    "description": {
      "description": "The detailed description of the SQL transformation capturing the user intent, explaining the SQL query, and the expected output.",
      "type": "string"
    },
    "sql_code_blocks": {
      "description": "The SQL query code blocks, each containing a descriptive name and a sequence of semantically related independently executable sql_statements written in the current SQL dialect.",
      "items": {
        "$ref": "#/$defs/Code"
      },
      "type": "array"
    },
    "created_table_names": {
      "default": [],
      "description": "A list of created table names if they are generated within the SQL query statements (e.g., using `CREATE TABLE ...`).",
      "items": {
        "type": "string"
      },
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
<a name="get_component"></a>
## get_component
**Annotations**: `read-only`

**Tags**: `components`

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
<a name="get_config"></a>
## get_config
**Annotations**: `read-only`

**Tags**: `components`

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
      "type": "string"
    },
    "configuration_id": {
      "description": "ID of the component/transformation configuration",
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
<a name="get_config_examples"></a>
## get_config_examples
**Annotations**: `read-only`

**Tags**: `components`

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
<a name="list_configs"></a>
## list_configs
**Annotations**: `read-only`

**Tags**: `components`

**Description**:

Lists all component configurations in the project with optional filtering by component type or specific
component IDs.

Returns a list of components, each containing:
- Component metadata (ID, name, type, description)
- All configurations for that component
- Links to the Keboola UI

PARAMETER BEHAVIOR:
- If component_ids is provided (non-empty): Returns ONLY those specific components, component_types is IGNORED
- If component_ids is empty [] and component_types is empty []: Returns ALL component types
  (application, extractor, transformation, writer)
- If component_ids is empty [] and component_types has values: Returns components matching ONLY those types

WHEN TO USE:
- User asks for "all configurations" or "list configurations" → Use component_types=[], component_ids=[]
- User asks for specific component types (e.g., "extractors", "writers") → Use component_types with specific types
- User asks for "all transformations" or "list transformations" → Use component_types=["transformation"]
- User asks for specific component by ID → Use component_ids with the specific ID(s)

EXAMPLES:
- user_input: "Show me all components in the project"
  → component_types=[], component_ids=[]
  → Returns ALL component types (application, extractor, transformation, writer) with their configurations

- user_input: "List all extractor configurations"
  → component_types=["extractor"], component_ids=[]
  → Returns only extractor component configurations

- user_input: "Show me all extractors and writers"
  → component_types=["extractor", "writer"], component_ids=[]
  → Returns extractor and writer configurations only

- user_input: "List all transformations"
  → component_types=["transformation"], component_ids=[]
  → Returns transformation configurations only

- user_input: "Show me configurations for keboola.ex-db-mysql"
  → component_types=[], component_ids=["keboola.ex-db-mysql"]
  → Returns only configurations for the MySQL extractor (component_types is ignored)

- user_input: "Get configs for these components: ex-db-mysql and wr-google-sheets"
  → component_types=[], component_ids=["keboola.ex-db-mysql", "keboola.wr-google-sheets"]
  → Returns configurations for both specified components (component_types is ignored)


**Input JSON Schema**:
```json
{
  "properties": {
    "component_types": {
      "default": [],
      "description": "Filter by component types. Options: \"application\", \"extractor\", \"transformation\", \"writer\". Empty list [] means ALL component types will be returned (application, extractor, transformation, writer). This parameter is IGNORED when component_ids is provided (non-empty).",
      "items": {
        "enum": [
          "application",
          "extractor",
          "transformation",
          "writer"
        ],
        "type": "string"
      },
      "type": "array"
    },
    "component_ids": {
      "default": [],
      "description": "Filter by specific component IDs (e.g., [\"keboola.ex-db-mysql\", \"keboola.wr-google-sheets\"]). Empty list [] uses component_types filtering instead. When provided (non-empty), this parameter takes PRECEDENCE over component_types and component_types is IGNORED.",
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "type": "object"
}
```

---
<a name="update_config"></a>
## update_config
**Annotations**: `destructive`

**Tags**: `components`

**Description**:

Updates an existing root component configuration by modifying its parameters, storage mappings, name or description.

This tool allows PARTIAL parameter updates - you only need to provide the fields you want to change.
All other fields will remain unchanged.
Use this tool when modifying existing configurations; for configuration rows, use update_config_row instead.

WHEN TO USE:
- Modifying configuration parameters (credentials, settings, API keys, etc.)
- Updating storage mappings (input/output tables or files)
- Changing configuration name or description
- Any combination of the above

PREREQUISITES:
- Configuration must already exist (use create_config for new configurations)
- You must know both component_id and configuration_id
- For parameter updates: Review the component's root_configuration_schema using get_component.
- For storage updates: Ensure mappings are valid for the component type

IMPORTANT CONSIDERATIONS:
- Parameter updates are PARTIAL - only specify fields you want to change
- parameter_updates supports granular operations: set individual keys, replace strings, or remove keys
- Parameters must conform to the component's root_configuration_schema
- Validate schemas before calling: use get_component to retrieve root_configuration_schema
- For row-based components, this updates the ROOT only (use update_config_row for individual rows)

WORKFLOW:
1. Retrieve current configuration using get_config (to understand current state)
2. Identify specific parameters/storage mappings to modify
3. Prepare parameter_updates list with targeted operations
4. Call update_config with only the fields to change


**Input JSON Schema**:
```json
{
  "$defs": {
    "ConfigParamRemove": {
      "description": "Remove a parameter key.",
      "properties": {
        "op": {
          "const": "remove",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the parameter key to remove",
          "type": "string"
        }
      },
      "required": [
        "op",
        "path"
      ],
      "type": "object"
    },
    "ConfigParamReplace": {
      "description": "Replace a substring in a string parameter.",
      "properties": {
        "op": {
          "const": "str_replace",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the parameter key to modify",
          "type": "string"
        },
        "search_for": {
          "description": "Substring to search for (non-empty)",
          "type": "string"
        },
        "replace_with": {
          "description": "Replacement string (can be empty for deletion)",
          "type": "string"
        }
      },
      "required": [
        "op",
        "path",
        "search_for",
        "replace_with"
      ],
      "type": "object"
    },
    "ConfigParamSet": {
      "description": "Set or create a parameter value at the specified path.\n\nUse this operation to:\n- Update an existing parameter value\n- Create a new parameter key\n- Replace a nested parameter value",
      "properties": {
        "op": {
          "const": "set",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the parameter key to set (e.g., \"api_key\", \"database.host\")",
          "type": "string"
        },
        "new_val": {
          "description": "New value to set",
          "title": "New Val"
        }
      },
      "required": [
        "op",
        "path",
        "new_val"
      ],
      "type": "object"
    }
  },
  "properties": {
    "change_description": {
      "description": "A clear, human-readable summary of what changed in this update. Be specific: e.g., \"Updated API key\", \"Added customers table to input mapping\".",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component the configuration belongs to.",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the configuration to update.",
      "type": "string"
    },
    "name": {
      "default": "",
      "description": "New name for the configuration. Only provide if changing the name. Name should be short (typically under 50 characters) and descriptive.",
      "type": "string"
    },
    "description": {
      "default": "",
      "description": "New detailed description for the configuration. Only provide if changing the description. Should explain the purpose, data sources, and behavior of this configuration.",
      "type": "string"
    },
    "parameter_updates": {
      "default": null,
      "description": "List of granular parameter update operations to apply. Each operation (set, str_replace, remove) modifies a specific parameter using JSONPath notation. Only provide if updating parameters - do not use for changing description or storage. Prefer simple dot-delimited JSONPaths and make the smallest possible updates - only change what needs changing. In case you need to replace the whole parameters, you can use the `set` operation with `$` as path.",
      "items": {
        "discriminator": {
          "mapping": {
            "remove": "#/$defs/ConfigParamRemove",
            "set": "#/$defs/ConfigParamSet",
            "str_replace": "#/$defs/ConfigParamReplace"
          },
          "propertyName": "op"
        },
        "oneOf": [
          {
            "$ref": "#/$defs/ConfigParamSet"
          },
          {
            "$ref": "#/$defs/ConfigParamReplace"
          },
          {
            "$ref": "#/$defs/ConfigParamRemove"
          }
        ]
      },
      "type": "array"
    },
    "storage": {
      "additionalProperties": true,
      "default": null,
      "description": "Complete storage configuration containing input/output table and file mappings. Only provide if updating storage mappings - this replaces the ENTIRE storage configuration. \n\nWhen to use:\n- Adding/removing input or output tables\n- Modifying table/file mappings\n- Updating table destinations or sources\n\nImportant:\n- Not applicable for row-based components (they use row-level storage)\n- Must conform to the Keboola storage schema\n- Replaces ALL existing storage config - include all mappings you want to keep\n- Use get_config first to see current storage configuration\n- Leave unfilled to preserve existing storage configuration",
      "type": "object"
    }
  },
  "required": [
    "change_description",
    "component_id",
    "configuration_id"
  ],
  "type": "object"
}
```

---
<a name="update_config_row"></a>
## update_config_row
**Annotations**: `destructive`

**Tags**: `components`

**Description**:

Updates an existing component configuration row by modifying its parameters, storage mappings, name, or description.

This tool allows PARTIAL parameter updates - you only need to provide the fields you want to change.
All other fields will remain unchanged.
Configuration rows are individual items within a configuration, often representing separate data sources,
tables, or endpoints that share the same component type and parent configuration settings.

WHEN TO USE:
- Modifying row-specific parameters (table sources, filters, credentials, etc.)
- Updating storage mappings for a specific row (input/output tables or files)
- Changing row name or description
- Any combination of the above

PREREQUISITES:
- The configuration row must already exist (use add_config_row for new rows)
- You must know component_id, configuration_id, and configuration_row_id
- For parameter updates: Review the component's row_configuration_schema using get_component
- For storage updates: Ensure mappings are valid for row-level storage

IMPORTANT CONSIDERATIONS:
- Parameter updates are PARTIAL - only specify fields you want to change
- parameter_updates supports granular operations: set individual keys, replace strings, or remove keys
- Parameters must conform to the component's row_configuration_schema (not root schema)
- Validate schemas before calling: use get_component to retrieve row_configuration_schema
- Each row operates independently - changes to one row don't affect others
- Row-level storage is separate from root-level storage configuration

WORKFLOW:
1. Retrieve current configuration using get_config to see existing rows
2. Identify the specific row to modify by its configuration_row_id
3. Prepare parameter_updates list with targeted operations for this row
4. Call update_config_row with only the fields to change


**Input JSON Schema**:
```json
{
  "$defs": {
    "ConfigParamRemove": {
      "description": "Remove a parameter key.",
      "properties": {
        "op": {
          "const": "remove",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the parameter key to remove",
          "type": "string"
        }
      },
      "required": [
        "op",
        "path"
      ],
      "type": "object"
    },
    "ConfigParamReplace": {
      "description": "Replace a substring in a string parameter.",
      "properties": {
        "op": {
          "const": "str_replace",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the parameter key to modify",
          "type": "string"
        },
        "search_for": {
          "description": "Substring to search for (non-empty)",
          "type": "string"
        },
        "replace_with": {
          "description": "Replacement string (can be empty for deletion)",
          "type": "string"
        }
      },
      "required": [
        "op",
        "path",
        "search_for",
        "replace_with"
      ],
      "type": "object"
    },
    "ConfigParamSet": {
      "description": "Set or create a parameter value at the specified path.\n\nUse this operation to:\n- Update an existing parameter value\n- Create a new parameter key\n- Replace a nested parameter value",
      "properties": {
        "op": {
          "const": "set",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the parameter key to set (e.g., \"api_key\", \"database.host\")",
          "type": "string"
        },
        "new_val": {
          "description": "New value to set",
          "title": "New Val"
        }
      },
      "required": [
        "op",
        "path",
        "new_val"
      ],
      "type": "object"
    }
  },
  "properties": {
    "change_description": {
      "description": "A clear, human-readable summary of what changed in this row update. Be specific.",
      "type": "string"
    },
    "component_id": {
      "description": "The ID of the component the configuration belongs to.",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the parent configuration containing the row to update.",
      "type": "string"
    },
    "configuration_row_id": {
      "description": "The ID of the specific configuration row to update.",
      "type": "string"
    },
    "name": {
      "default": "",
      "description": "New name for the configuration row. Only provide if changing the name. Name should be short (typically under 50 characters) and descriptive of this specific row.",
      "type": "string"
    },
    "description": {
      "default": "",
      "description": "New detailed description for the configuration row. Only provide if changing the description. Should explain the specific purpose and behavior of this individual row.",
      "type": "string"
    },
    "parameter_updates": {
      "default": null,
      "description": "List of granular parameter update operations to apply to this row. Each operation (set, str_replace, remove) modifies a specific parameter using JSONPath notation. Only provide if updating parameters - do not use for changing description or storage. Prefer simple dot-delimited JSONPaths and make the smallest possible updates - only change what needs changing. In case you need to replace the whole parameters, you can use the `set` operation with `$` as path.",
      "items": {
        "discriminator": {
          "mapping": {
            "remove": "#/$defs/ConfigParamRemove",
            "set": "#/$defs/ConfigParamSet",
            "str_replace": "#/$defs/ConfigParamReplace"
          },
          "propertyName": "op"
        },
        "oneOf": [
          {
            "$ref": "#/$defs/ConfigParamSet"
          },
          {
            "$ref": "#/$defs/ConfigParamReplace"
          },
          {
            "$ref": "#/$defs/ConfigParamRemove"
          }
        ]
      },
      "type": "array"
    },
    "storage": {
      "additionalProperties": true,
      "default": null,
      "description": "Complete storage configuration for this row containing input/output table and file mappings. Only provide if updating storage mappings - this replaces the ENTIRE storage configuration for this row. \n\nWhen to use:\n- Adding/removing input or output tables for this specific row\n- Modifying table/file mappings for this row\n- Updating table destinations or sources for this row\n\nImportant:\n- Must conform to the component's row storage schema\n- Replaces ALL existing storage config for this row - include all mappings you want to keep\n- Use get_config first to see current row storage configuration\n- Leave unfilled to preserve existing storage configuration",
      "type": "object"
    }
  },
  "required": [
    "change_description",
    "component_id",
    "configuration_id",
    "configuration_row_id"
  ],
  "type": "object"
}
```

---
<a name="update_sql_transformation"></a>
## update_sql_transformation
**Annotations**: `destructive`

**Tags**: `components`

**Description**:

Updates an existing SQL transformation configuration, optionally updating the description and disabling the
configuration.

CONSIDERATIONS:
- The parameters configuration must include blocks with codes of SQL statements. Using one block with many codes of
  SQL statements is preferred and commonly used unless specified otherwise by the user.
- Each code contains SQL statements that are semantically related and have a descriptive name.
- Each SQL statement must be executable and follow the current SQL dialect, which can be retrieved using
  appropriate tool.
- The storage configuration must not be empty, and it should include input or output tables with correct mappings
  for the transformation.
- When the behavior of the transformation is not changed, the updated_description can be empty string.
- SCHEMA CHANGES: If the transformation update results in a destructive
  schema change to the output table (such as removing columns, changing
  column types, or renaming columns), you MUST inform the user that they
  need to
  manually delete the output table completely before running the updated
  transformation. Otherwise, the transformation will fail with a schema
  mismatch error. Non-destructive changes (adding new columns) typically do
  not require table deletion.

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
          "type": "string"
        },
        "codes": {
          "description": "The code scripts",
          "items": {
            "$ref": "#/$defs/Code"
          },
          "type": "array"
        }
      },
      "required": [
        "name",
        "codes"
      ],
      "type": "object"
    },
    "Code": {
      "description": "The code block for the transformation block.",
      "properties": {
        "name": {
          "description": "The name of the current code block describing the purpose of the block",
          "type": "string"
        },
        "sql_statements": {
          "description": "The executable SQL query statements written in the current SQL dialect. Each statement must be executable and a separate item in the list.",
          "items": {
            "type": "string"
          },
          "type": "array"
        }
      },
      "required": [
        "name",
        "sql_statements"
      ],
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
          "type": "array"
        }
      },
      "required": [
        "blocks"
      ],
      "type": "object"
    }
  },
  "properties": {
    "configuration_id": {
      "description": "ID of the transformation configuration to update",
      "type": "string"
    },
    "change_description": {
      "description": "Description of the changes made to the transformation configuration.",
      "type": "string"
    },
    "parameters": {
      "$ref": "#/$defs/Parameters",
      "default": null,
      "description": "The updated \"parameters\" part of the transformation configuration that contains the newly applied settings and preserves all other existing settings. Only updated if provided.",
      "type": "object"
    },
    "storage": {
      "additionalProperties": true,
      "default": null,
      "description": "The updated \"storage\" part of the transformation configuration that contains the newly applied settings and preserves all other existing settings. Only updated if provided.",
      "type": "object"
    },
    "updated_description": {
      "default": "",
      "description": "Updated transformation description reflecting the changes made in the behavior of the transformation. If no behavior changes are made, empty string preserves the original description.",
      "type": "string"
    },
    "is_disabled": {
      "default": false,
      "description": "Whether to disable the transformation configuration. Default is False.",
      "type": "boolean"
    }
  },
  "required": [
    "configuration_id",
    "change_description"
  ],
  "type": "object"
}
```

---

# Other Tools
<a name="deploy_data_app"></a>
## deploy_data_app
**Annotations**: 

**Tags**: `data-apps`

**Description**:

Deploys/redeploys a data app or stops running data app in the Keboola environment given the action and
configuration ID.

Considerations:
- Redeploying a data app takes some time, and the app temporarily may have status "stopped" during this process
because it needs to restart.


**Input JSON Schema**:
```json
{
  "properties": {
    "action": {
      "description": "The action to perform.",
      "enum": [
        "deploy",
        "stop"
      ],
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the data app configuration.",
      "type": "string"
    }
  },
  "required": [
    "action",
    "configuration_id"
  ],
  "type": "object"
}
```

---
<a name="get_data_apps"></a>
## get_data_apps
**Annotations**: `read-only`

**Tags**: `data-apps`

**Description**:

Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by
providing their configuration IDs.

Considerations:
- If configuration_ids are provided, the tool will return details of the data apps by their configuration IDs.
- If no configuration_ids are provided, the tool will list all data apps in the project given the limit and offset.
- Data App details contain configurations, deployment info along with logs and links to the data app dashboard.


**Input JSON Schema**:
```json
{
  "properties": {
    "configuration_ids": {
      "default": [],
      "description": "The IDs of the data app configurations.",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "limit": {
      "default": 100,
      "description": "The limit of the data apps to fetch.",
      "type": "integer"
    },
    "offset": {
      "default": 0,
      "description": "The offset of the data apps to fetch.",
      "type": "integer"
    }
  },
  "type": "object"
}
```

---
<a name="modify_data_app"></a>
## modify_data_app
**Annotations**: `destructive`

**Tags**: `data-apps`

**Description**:

Creates or updates a Streamlit data app.

Considerations:
- The `source_code` parameter must be a complete and runnable Streamlit app. It must include a placeholder
`{QUERY_DATA_FUNCTION}` where a `query_data` function will be injected. This function accepts a string of SQL
query following current sql dialect and returns a pandas DataFrame with the results from the workspace.
- Write SQL queries so they are compatible with the current workspace backend, you can ensure this by using the
`query_data` tool to inspect the data in the workspace before using it in the data app.
- If you're updating an existing data app, provide the `configuration_id` parameter and the `change_description`
parameter.
- If the data app is updated while running, it must be redeployed for the changes to take effect.
- The Data App requires basic authorization by default for security reasons, unless explicitly specified otherwise
by the user.


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "Name of the data app.",
      "type": "string"
    },
    "description": {
      "description": "Description of the data app.",
      "type": "string"
    },
    "source_code": {
      "description": "Complete Python/Streamlit source code for the data app.",
      "type": "string"
    },
    "packages": {
      "description": "Python packages used in the source code that will be installed by `pip install` into the environment before the code runs. For example: [\"pandas\", \"requests~=2.32\"].",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "authorization_required": {
      "default": true,
      "description": "Whether the data app is authorized using simple password or not.",
      "type": "boolean"
    },
    "configuration_id": {
      "default": "",
      "description": "The ID of existing data app configuration when updating, otherwise empty string.",
      "type": "string"
    },
    "change_description": {
      "default": "",
      "description": "The description of the change when updating (e.g. \"Update Code\"), otherwise empty string.",
      "type": "string"
    }
  },
  "required": [
    "name",
    "description",
    "source_code",
    "packages"
  ],
  "type": "object"
}
```

---

# Documentation Tools
<a name="docs_query"></a>
## docs_query
**Annotations**: `read-only`

**Tags**: `docs`

**Description**:

Answers a question using the Keboola documentation as a source.


**Input JSON Schema**:
```json
{
  "properties": {
    "query": {
      "description": "Natural language query to search for in the documentation.",
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

# Flow Tools
<a name="create_conditional_flow"></a>
## create_conditional_flow
**Annotations**: 

**Tags**: `flows`

**Description**:

Creates a new conditional flow configuration in Keboola.

BEFORE USING THIS TOOL:
- Call `get_flow_schema` with flow_type='keboola.flow' to see the required schema structure
- Call `get_flow_examples` with flow_type='keboola.flow' to see working examples

REQUIREMENTS:
- All phase and task IDs must be unique strings
- The `phases` list cannot be empty
- The `phases` and `tasks` parameters must match the keboola.flow JSON schema structure
- Only include conditions/retry logic if the user explicitly requests them
- All phases must be connected: no dangling phases are allowed
- The flow must have exactly one entry point (one phase with no incoming transitions)
- Every phase must either transition to another phase or end the flow

WHEN TO USE:
- User asks to "create a flow" (conditional flows are the default flow type)
- User requests conditional logic, retry mechanisms, or error handling
- User needs a data pipeline with sophisticated branching or notifications


**Input JSON Schema**:
```json
{
  "properties": {
    "name": {
      "description": "A short, descriptive name for the flow.",
      "type": "string"
    },
    "description": {
      "description": "Detailed description of the flow purpose.",
      "type": "string"
    },
    "phases": {
      "description": "List of phase definitions for conditional flows.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "tasks": {
      "description": "List of task definitions for conditional flows.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
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
<a name="create_flow"></a>
## create_flow
**Annotations**: 

**Tags**: `flows`

**Description**:

Creates a new flow configuration in Keboola.
A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
Each flow is composed of:
- Tasks: individual component configurations (e.g., extractors, writers, transformations).
- Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

If you haven't already called it, always use the `get_flow_schema` tool using `keboola.orchestrator` flow type
to see the latest schema for flows and also look at the examples under `get_flow_examples` tool.

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
      "type": "string"
    },
    "description": {
      "description": "Detailed description of the flow purpose.",
      "type": "string"
    },
    "phases": {
      "description": "List of phase definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "tasks": {
      "description": "List of task definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
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
<a name="get_flow"></a>
## get_flow
**Annotations**: `read-only`

**Tags**: `flows`

**Description**:

Gets detailed information about a specific flow configuration.


**Input JSON Schema**:
```json
{
  "properties": {
    "configuration_id": {
      "description": "ID of the flow to retrieve.",
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
<a name="get_flow_examples"></a>
## get_flow_examples
**Annotations**: `read-only`

**Tags**: `flows`

**Description**:

Retrieves examples of valid flow configurations.

CONSIDERATIONS:
- If the project has conditional flows disabled, this tool will fail when requesting conditional flow examples.
- Projects with conditional flows enabled can fetch examples for both flow types.
- Projects with conditional flows disabled should use `keboola.orchestrator` for legacy flow examples.


**Input JSON Schema**:
```json
{
  "properties": {
    "flow_type": {
      "description": "The type of the flow to retrieve examples for.",
      "enum": [
        "keboola.flow",
        "keboola.orchestrator"
      ],
      "type": "string"
    }
  },
  "required": [
    "flow_type"
  ],
  "type": "object"
}
```

---
<a name="get_flow_schema"></a>
## get_flow_schema
**Annotations**: `read-only`

**Tags**: `flows`

**Description**:

Returns the JSON schema for the given flow type in markdown format.
`keboola.flow` = conditional flows
`keboola.orchestrator` = legacy flows

CONSIDERATIONS:
- If the project has conditional flows disabled, this tool will fail when requesting conditional flow schema.
- Otherwise, the returned schema matches the requested flow type.

Usage:
    Use this tool to inspect the required structure of phases and tasks for `create_flow` or `update_flow`.


**Input JSON Schema**:
```json
{
  "properties": {
    "flow_type": {
      "description": "The type of flow for which to fetch schema.",
      "enum": [
        "keboola.flow",
        "keboola.orchestrator"
      ],
      "type": "string"
    }
  },
  "required": [
    "flow_type"
  ],
  "type": "object"
}
```

---
<a name="list_flows"></a>
## list_flows
**Annotations**: `read-only`

**Tags**: `flows`

**Description**:

Retrieves flow configurations from the project. Optionally filtered by IDs.


**Input JSON Schema**:
```json
{
  "properties": {
    "flow_ids": {
      "default": [],
      "description": "IDs of the flows to retrieve.",
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "type": "object"
}
```

---
<a name="update_flow"></a>
## update_flow
**Annotations**: `destructive`

**Tags**: `flows`

**Description**:

Updates an existing flow configuration in Keboola.

A flow is a special type of Keboola component that orchestrates the execution of other components. It defines
how tasks are grouped and ordered — enabling control over parallelization** and sequential execution.
Each flow is composed of:
- Tasks: individual component configurations (e.g., extractors, writers, transformations).
- Phases: groups of tasks that run in parallel. Phases themselves run in order, based on dependencies.

PREREQUISITES:
- The flow specified by `configuration_id` must already exist in the project
- Use `get_flow` to retrieve the current flow configuration and determine its type
- Use `get_flow_schema` with the correct flow type to understand the required structure
- Ensure all referenced component configurations exist in the project

CONSIDERATIONS:
- The `flow_type` parameter **MUST** match the actual type of the flow being updated
- The `phases` and `tasks` parameters must conform to the appropriate JSON schema
- Each task and phase must include at least: `id` and `name`
- Each task must reference an existing component configuration in the project
- Items in the `dependsOn` phase field reference ids of other phases
- If the project has conditional flows disabled, this tool will fail when trying to update conditional flows
- Links contained in the response should ALWAYS be presented to the user

USAGE:
Use this tool to update an existing flow. You must specify the correct flow_type:
- Use `"keboola.flow"` for conditional flows
- Use `"keboola.orchestrator"` for legacy flows

EXAMPLES:
- user_input: "Add a new transformation phase to my existing flow"
    - First use `get_flow` to retrieve the current flow configuration
    - Determine the flow type from the response
    - Use `get_flow_schema` with the correct flow type
    - Update the phases and tasks arrays with the new transformation
    - Set `flow_type` to match the existing flow type
- user_input: "Update my flow to include error handling"
    - For conditional flows: add retry configurations and error conditions
    - For legacy flows: adjust `continueOnFailure` settings
    - Ensure the `flow_type` matches the existing flow


**Input JSON Schema**:
```json
{
  "properties": {
    "configuration_id": {
      "description": "ID of the flow configuration to update.",
      "type": "string"
    },
    "flow_type": {
      "description": "The type of flow to update. Use \"keboola.flow\" for conditional flows or \"keboola.orchestrator\" for legacy flows. This MUST match the existing flow type.",
      "enum": [
        "keboola.flow",
        "keboola.orchestrator"
      ],
      "type": "string"
    },
    "change_description": {
      "description": "Description of changes made.",
      "type": "string"
    },
    "phases": {
      "default": null,
      "description": "Updated list of phase definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "tasks": {
      "default": null,
      "description": "Updated list of task definitions.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "name": {
      "default": "",
      "description": "Updated flow name. Only updated if provided.",
      "type": "string"
    },
    "description": {
      "default": "",
      "description": "Updated flow description. Only updated if provided.",
      "type": "string"
    }
  },
  "required": [
    "configuration_id",
    "flow_type",
    "change_description"
  ],
  "type": "object"
}
```

---

# Jobs Tools
<a name="get_job"></a>
## get_job
**Annotations**: `read-only`

**Tags**: `jobs`

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
<a name="list_jobs"></a>
## list_jobs
**Annotations**: `read-only`

**Tags**: `jobs`

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
      "type": "string"
    },
    "component_id": {
      "default": null,
      "description": "The optional ID of the component whose jobs you want to list, default = None.",
      "type": "string"
    },
    "config_id": {
      "default": null,
      "description": "The optional ID of the component configuration whose jobs you want to list, default = None.",
      "type": "string"
    },
    "limit": {
      "default": 100,
      "description": "The number of jobs to list, default = 100, max = 500.",
      "maximum": 500,
      "minimum": 1,
      "type": "integer"
    },
    "offset": {
      "default": 0,
      "description": "The offset of the jobs to list, default = 0.",
      "minimum": 0,
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
      "type": "string"
    },
    "sort_order": {
      "default": "desc",
      "description": "The order to sort the jobs by, default = \"desc\".",
      "enum": [
        "asc",
        "desc"
      ],
      "type": "string"
    }
  },
  "type": "object"
}
```

---
<a name="run_job"></a>
## run_job
**Annotations**: `destructive`

**Tags**: `jobs`

**Description**:

Starts a new job for a given component or transformation.


**Input JSON Schema**:
```json
{
  "properties": {
    "component_id": {
      "description": "The ID of the component or transformation for which to start a job.",
      "type": "string"
    },
    "configuration_id": {
      "description": "The ID of the configuration for which to start a job.",
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

# OAuth Tools
<a name="create_oauth_url"></a>
## create_oauth_url
**Annotations**: `destructive`

**Tags**: `oauth`

**Description**:

Generates an OAuth authorization URL for a Keboola component configuration.

When using this tool, be very concise in your response. Just guide the user to click the
authorization link.

Note that this tool should be called specifically for the OAuth-requiring components after their
configuration is created e.g. keboola.ex-google-analytics-v4 and keboola.ex-gmail.


**Input JSON Schema**:
```json
{
  "properties": {
    "component_id": {
      "description": "The component ID to grant access to (e.g., \"keboola.ex-google-analytics-v4\").",
      "type": "string"
    },
    "config_id": {
      "description": "The configuration ID for the component.",
      "type": "string"
    }
  },
  "required": [
    "component_id",
    "config_id"
  ],
  "type": "object"
}
```

---

# Project Tools
<a name="get_project_info"></a>
## get_project_info
**Annotations**: `read-only`

**Tags**: `project`

**Description**:

Retrieves structured information about the current project,
including essential context and base instructions for working with it
(e.g., transformations, components, workflows, and dependencies).

Always call this tool at least once at the start of a conversation
to establish the project context before using other tools.


**Input JSON Schema**:
```json
{
  "properties": {},
  "type": "object"
}
```

---

# Search Tools
<a name="find_component_id"></a>
## find_component_id
**Annotations**: `read-only`

**Tags**: `search`

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

# SQL Tools
<a name="query_data"></a>
## query_data
**Annotations**: `read-only`

**Tags**: `sql`

**Description**:

Executes an SQL SELECT query to get the data from the underlying database.

CRITICAL SQL REQUIREMENTS:

* ALWAYS check the SQL dialect before constructing queries. The SQL dialect can be found in the project info.
* Do not include any comments in the SQL code

DIALECT-SPECIFIC REQUIREMENTS:
* Snowflake: Use double quotes for identifiers: "column_name", "table_name"
* BigQuery: Use backticks for identifiers: `column_name`, `table_name`
* Never mix quoting styles within a single query

TABLE AND COLUMN REFERENCES:
* Always use fully qualified table names that include database name, schema name and table name
* Get fully qualified table names using table information tools - use exact format shown
* Snowflake format: "DATABASE"."SCHEMA"."TABLE"
* BigQuery format: `project`.`dataset`.`table`
* Always use quoted column names when referring to table columns (exact quotes from table info)

CTE (WITH CLAUSE) RULES:
* ALL column references in main query MUST match exact case used in the CTE
* If you alias a column as "project_id" in CTE, reference it as "project_id" in subsequent queries
* For Snowflake: Unless columns are quoted in CTE, they become UPPERCASE. To preserve case, use quotes
* Define all column aliases explicitly in CTEs
* Quote identifiers in both CTE definition and references to preserve case

FUNCTION COMPATIBILITY:
* Snowflake: Use LISTAGG instead of STRING_AGG
* Check data types before using date functions (DATE_TRUNC, EXTRACT require proper date/timestamp types)
* Cast VARCHAR columns to appropriate types before using in date/numeric functions

ERROR PREVENTION:
* Never pass empty strings ('') where numeric or date values are expected
* Use NULLIF or CASE statements to handle empty values
* Always use TRY_CAST or similar safe casting functions when converting data types
* Check for division by zero using NULLIF(denominator, 0)

DATA VALIDATION:
* When querying columns with categorical values, use query_data tool to inspect distinct values beforehand
* Ensure valid filtering by checking actual data values first


**Input JSON Schema**:
```json
{
  "properties": {
    "sql_query": {
      "description": "SQL SELECT query to run.",
      "type": "string"
    },
    "query_name": {
      "description": "A concise, human-readable name for this query based on its purpose and what data it retrieves. Use normal words with spaces (e.g., \"Customer Orders Last Month\", \"Top Selling Products\", \"User Activity Summary\").",
      "type": "string"
    }
  },
  "required": [
    "sql_query",
    "query_name"
  ],
  "type": "object"
}
```

---

# Storage Tools
<a name="get_bucket"></a>
## get_bucket
**Annotations**: `read-only`

**Tags**: `storage`

**Description**:

Gets detailed information about a specific bucket.


**Input JSON Schema**:
```json
{
  "properties": {
    "bucket_id": {
      "description": "Unique ID of the bucket.",
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
<a name="get_table"></a>
## get_table
**Annotations**: `read-only`

**Tags**: `storage`

**Description**:

Gets detailed information about a specific table including its DB identifier and column information.


**Input JSON Schema**:
```json
{
  "properties": {
    "table_id": {
      "description": "Unique ID of the table.",
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
<a name="list_buckets"></a>
## list_buckets
**Annotations**: `read-only`

**Tags**: `storage`

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
<a name="list_tables"></a>
## list_tables
**Annotations**: `read-only`

**Tags**: `storage`

**Description**:

Retrieves all tables in a specific bucket with their basic information.


**Input JSON Schema**:
```json
{
  "properties": {
    "bucket_id": {
      "description": "Unique ID of the bucket.",
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
<a name="update_descriptions"></a>
## update_descriptions
**Annotations**: `destructive`

**Tags**: `storage`

**Description**:

Updates the description for a Keboola storage item.

This tool supports three item types, inferred from the provided item_id:

- bucket: item_id = "in.c-bucket"
- table: item_id = "in.c-bucket.table"
- column: item_id = "in.c-bucket.table.column"

Usage examples (payload uses a list of DescriptionUpdate objects):
- Update a bucket:
  updates=[DescriptionUpdate(item_id="in.c-my-bucket", description="New bucket description")]
- Update a table:
  updates=[DescriptionUpdate(item_id="in.c-my-bucket.my-table", description="New table description")]
- Update a column:
  updates=[DescriptionUpdate(item_id="in.c-my-bucket.my-table.my_column", description="New column description")]


**Input JSON Schema**:
```json
{
  "$defs": {
    "DescriptionUpdate": {
      "description": "Structured update describing a storage item and its new description.",
      "properties": {
        "item_id": {
          "description": "Storage item name: \"bucket_id\", \"bucket_id.table_id\", \"bucket_id.table_id.column_name\"",
          "type": "string"
        },
        "description": {
          "description": "New description to set for the storage item.",
          "type": "string"
        }
      },
      "required": [
        "item_id",
        "description"
      ],
      "type": "object"
    }
  },
  "properties": {
    "updates": {
      "description": "List of DescriptionUpdate objects with storage item_id and new description. Examples: \"bucket_id\", \"bucket_id.table_id\", \"bucket_id.table_id.column_name\"",
      "items": {
        "$ref": "#/$defs/DescriptionUpdate"
      },
      "type": "array"
    }
  },
  "required": [
    "updates"
  ],
  "type": "object"
}
```

---
