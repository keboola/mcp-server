# Tools Documentation
This document provides details about the tools available in the Keboola MCP server.

## Index

### Component Tools
- [add_config_row](#add_config_row): Creates a component configuration row in the specified configuration_id, using the specified name,
component ID, configuration JSON, and description.
- [create_config](#create_config): Creates a root component configuration using the specified name, component ID, configuration JSON, and description.
- [create_sql_transformation](#create_sql_transformation): Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
description, and a list of created table names.
- [get_components](#get_components): Retrieves detailed information about one or more components by their IDs.
- [get_config_examples](#get_config_examples): Retrieves sample configuration examples for a specific component.
- [get_configs](#get_configs): Retrieves component configurations in the project with optional filtering.
- [update_config](#update_config): Updates an existing root component configuration by modifying its parameters, storage mappings, name or description.
- [update_config_row](#update_config_row): Updates an existing component configuration row by modifying its parameters, storage mappings, name, or description.
- [update_sql_transformation](#update_sql_transformation): Updates an existing SQL transformation configuration by modifying its SQL code, storage mappings, or description.

### Documentation Tools
- [docs_query](#docs_query): Answers a question using the Keboola documentation as a source.

### Flow Tools
- [create_conditional_flow](#create_conditional_flow): Creates a new conditional flow configuration using `keboola.
- [create_flow](#create_flow): Creates a new legacy (non-conditional) flow using `keboola.
- [get_flow_examples](#get_flow_examples): Retrieves examples of valid flow configurations.
- [get_flow_schema](#get_flow_schema): Returns the JSON schema for the given flow type (markdown).
- [get_flows](#get_flows): Lists flows or retrieves full details for specific flows.
- [modify_flow](#modify_flow): Updates an existing flow configuration (either legacy `keboola.
- [update_flow](#update_flow): Updates an existing flow configuration (either legacy `keboola.

### Jobs Tools
- [get_jobs](#get_jobs): Retrieves job execution information from the Keboola project.
- [run_job](#run_job): Starts a new job for a given component or transformation.

### OAuth Tools
- [create_oauth_url](#create_oauth_url): Generates an OAuth authorization URL for a Keboola component configuration.

### Other Tools
- [deploy_data_app](#deploy_data_app): Deploys/redeploys a data app or stops running data app in the Keboola environment asynchronously given the action
and the configuration ID.
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
- [search](#search): Searches for Keboola items (tables, buckets, configurations, transformations, flows, etc.

### Storage Tools
- [get_buckets](#get_buckets): Lists buckets or retrieves full details of specific buckets.
- [get_tables](#get_tables): Lists tables in buckets or retrieves full details of specific tables, including fully qualified database name,
column definitions, and metadata.
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
    },
    "processors_before": {
      "default": null,
      "description": "The list of processors that will run before the configured component row runs.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "processors_after": {
      "default": null,
      "description": "The list of processors that will run after the configured component row runs.",
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
    },
    "processors_before": {
      "default": null,
      "description": "The list of processors that will run before the configured component runs.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "processors_after": {
      "default": null,
      "description": "The list of processors that will run after the configured component runs.",
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
          "description": "A descriptive name for the code block",
          "type": "string"
        },
        "script": {
          "description": "The SQL script of the code block",
          "type": "string"
        }
      },
      "required": [
        "name",
        "script"
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
      "description": "The SQL query code blocks, each containing a descriptive name and an executable SQL script written in the current SQL dialect. The query will be automatically reformatted to be more readable.",
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
<a name="get_components"></a>
## get_components
**Annotations**: `read-only`

**Tags**: `components`

**Description**:

Retrieves detailed information about one or more components by their IDs.

RETURNS FOR EACH COMPONENT:
- Component metadata (name, type, description)
- Documentation and usage instructions
- Configuration JSON schema (required for creating/updating configurations)
- Links to component dashboard in Keboola UI

WHEN TO USE:
- Before creating a new configuration: fetch the component to get its configuration schema
- Before updating a configuration: fetch the component to understand valid configuration options
- When user asks about component capabilities or documentation

PREREQUISITES:
- You must know the component_id(s). If unknown, first use `find_component_id` or `docs` tool to discover them.

EXAMPLES:
- User: "Create a generic extractor configuration"
  → First call `find_component_id` to get the component_id, then call this tool to get the schema
- User: "What options does the Snowflake writer support?"
  → Call this tool with the Snowflake writer component_id to retrieve its documentation and schema


**Input JSON Schema**:
```json
{
  "properties": {
    "component_ids": {
      "description": "IDs of the components",
      "items": {
        "type": "string"
      },
      "type": "array"
    }
  },
  "required": [
    "component_ids"
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
<a name="get_configs"></a>
## get_configs
**Annotations**: `read-only`

**Tags**: `components`

**Description**:

Retrieves component configurations in the project with optional filtering.

Can list summaries of multiple configurations (grouped by component) or retrieve full details
for specific configurations.

Returns a list of components, each containing:
- Component metadata (ID, name, type, description)
- Configurations for that component (summaries by default, full details if requested)
- Links to the Keboola UI

PARAMETER BEHAVIOR:
- If configs is provided (non-empty): Returns FULL details ONLY for those configs.
- Else if component_ids is provided (non-empty): Lists config summaries for those components.
- Else: Lists configs based on component_types (all types if empty).

WHEN TO USE:
- For listing: Use component_types/component_ids.
- For details: Use configs (can handle multiple).

EXAMPLES:
- List all configs (summaries): component_types=[], component_ids=[]
- List extractors (summaries): component_types=["extractor"]
- Get details for specific configs:
  configs=[{"component_id": "keboola.ex-db-mysql", "configuration_id": "12345"}]


**Input JSON Schema**:
```json
{
  "$defs": {
    "FullConfigId": {
      "description": "Composite configuration ID (component ID + configuration ID).",
      "properties": {
        "component_id": {
          "description": "ID of the component",
          "type": "string"
        },
        "configuration_id": {
          "description": "ID of the configuration",
          "type": "string"
        }
      },
      "required": [
        "component_id",
        "configuration_id"
      ],
      "type": "object"
    }
  },
  "properties": {
    "component_types": {
      "default": [],
      "description": "Filter by component types. Options: \"application\", \"extractor\", \"transformation\", \"writer\". Empty list [] means ALL component types will be returned. This parameter is IGNORED when configs is provided (non-empty) or component_ids is non-empty.",
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
      "description": "Filter by specific component IDs (e.g., [\"keboola.ex-db-mysql\", \"keboola.wr-google-sheets\"]). Empty list [] uses component_types filtering instead. When provided (non-empty) and configs is empty, lists summaries for these components. Ignored if configs is provided.",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "configs": {
      "default": [],
      "description": "List of specific configurations to retrieve full details for. Each dict must have \"component_id\" (str) and \"configuration_id\" (str). Example: [{\"component_id\": \"keboola.ex-db-mysql\", \"configuration_id\": \"12345\"}]. If provided (non-empty), ignores other filters and returns full details only for these configs, grouped by component. Use this for detailed retrieval.",
      "items": {
        "$ref": "#/$defs/FullConfigId"
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
- For parameter updates: Review the component's root_configuration_schema using get_components.
- For storage updates: Ensure mappings are valid for the component type

IMPORTANT CONSIDERATIONS:
- Parameter updates are PARTIAL - only specify fields you want to change
- parameter_updates supports granular operations: set keys, replace strings, remove keys, or append to lists
- Parameters must conform to the component's root_configuration_schema
- Validate schemas before calling: use get_components to retrieve root_configuration_schema
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
    "ConfigParamListAppend": {
      "description": "Append a value to a list parameter.",
      "properties": {
        "op": {
          "const": "list_append",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the list parameter",
          "type": "string"
        },
        "value": {
          "description": "Value to append to the list",
          "title": "Value"
        }
      },
      "required": [
        "op",
        "path",
        "value"
      ],
      "type": "object"
    },
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
        "value": {
          "description": "New value to set",
          "title": "Value"
        }
      },
      "required": [
        "op",
        "path",
        "value"
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
      "description": "List of granular parameter update operations to apply. Each operation (set, str_replace, remove, list_append) modifies a specific value using JSONPath notation. Only provide if updating parameters - do not use for changing description, storage or processors. Prefer simple JSONPaths (e.g., \"array_param[1]\", \"object_param.key\") and make the smallest possible updates - only change what needs changing. In case you need to replace the whole parameters section, you can use the `set` operation with `$` as path.",
      "items": {
        "discriminator": {
          "mapping": {
            "list_append": "#/$defs/ConfigParamListAppend",
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
          },
          {
            "$ref": "#/$defs/ConfigParamListAppend"
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
    },
    "processors_before": {
      "default": null,
      "description": "The list of processors that will run before the configured component row runs.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "processors_after": {
      "default": null,
      "description": "The list of processors that will run after the configured component row runs.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
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
- For parameter updates: Review the component's row_configuration_schema using get_components
- For storage updates: Ensure mappings are valid for row-level storage

IMPORTANT CONSIDERATIONS:
- Parameter updates are PARTIAL - only specify fields you want to change
- parameter_updates supports granular operations: set individual keys, replace strings, or remove keys
- Parameters must conform to the component's row_configuration_schema (not root schema)
- Validate schemas before calling: use get_components to retrieve row_configuration_schema
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
    "ConfigParamListAppend": {
      "description": "Append a value to a list parameter.",
      "properties": {
        "op": {
          "const": "list_append",
          "type": "string"
        },
        "path": {
          "description": "JSONPath to the list parameter",
          "type": "string"
        },
        "value": {
          "description": "Value to append to the list",
          "title": "Value"
        }
      },
      "required": [
        "op",
        "path",
        "value"
      ],
      "type": "object"
    },
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
        "value": {
          "description": "New value to set",
          "title": "Value"
        }
      },
      "required": [
        "op",
        "path",
        "value"
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
      "description": "List of granular parameter update operations to apply to this row. Each operation (set, str_replace, remove, list_append) modifies a specific parameter using JSONPath notation. Only provide if updating parameters - do not use for changing description or storage. Prefer simple dot-delimited JSONPaths and make the smallest possible updates - only change what needs changing. In case you need to replace the whole parameters, you can use the `set` operation with `$` as path.",
      "items": {
        "discriminator": {
          "mapping": {
            "list_append": "#/$defs/ConfigParamListAppend",
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
          },
          {
            "$ref": "#/$defs/ConfigParamListAppend"
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
    },
    "processors_before": {
      "default": null,
      "description": "The list of processors that will run before the configured component row runs.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
    },
    "processors_after": {
      "default": null,
      "description": "The list of processors that will run after the configured component row runs.",
      "items": {
        "additionalProperties": true,
        "type": "object"
      },
      "type": "array"
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

Updates an existing SQL transformation configuration by modifying its SQL code, storage mappings, or description.

This tool allows PARTIAL parameter updates for transformation SQL blocks and code - you only need to provide
the operations you want to perform. All other fields will remain unchanged.
Use this for modifying SQL transformations created with create_sql_transformation.

WHEN TO USE:
- Modifying SQL queries in transformation (add/edit/remove SQL statements)
- Updating transformation block or code block names
- Changing input/output table mappings for the transformation
- Updating the transformation description
- Enabling or disabling the transformation
- Any combination of the above

PREREQUISITES:
- Transformation must already exist (use create_sql_transformation for new transformations)
- You must know the configuration_id of the transformation
- SQL dialect is determined automatically from the workspace
- CRITICAL: Use get_config first to see the current transformation structure and get block_id/code_id values

TRANSFORMATION STRUCTURE:
A transformation has this hierarchy:
  transformation
  └─ blocks[] - List of transformation blocks (each has a unique block_id)
     └─ block.name - Descriptive name for the block
     └─ block.codes[] - List of code blocks within the block (each has a unique code_id)
        └─ code.name - Descriptive name for the code block
        └─ code.script - SQL script (string with SQL statements)

Example structure from get_config:
{
  "blocks": [
    {
      "id": "b0",  ← block_id needed for operations (format: b{index})
      "name": "Data Preparation",
      "codes": [
        {
          "id": "b0.c0",  ← code_id needed for operations (format: b{block_index}.c{code_index})
          "name": "Load customers",
          "script": "SELECT * FROM customers WHERE status = 'active';"
        }
      ]
    }
  ]
}

PARAMETER UPDATE OPERATIONS:
All operations use block_id and code_id to identify elements (get these from get_config first).

ID Format:
- block_id: "b0", "b1", "b2", etc. (format: b{index})
- code_id: "b0.c0", "b0.c1", "b1.c0", etc. (format: b{block_index}.c{code_index})

1. BLOCK OPERATIONS:
   - add_block: Create a new block in the transformation
     {"op": "add_block", "block": {"name": "New Block", "codes": []}, "position": "end"}

   - remove_block: Delete an entire block
     {"op": "remove_block", "block_id": "b0"}

   - rename_block: Change a block's name
     {"op": "rename_block", "block_id": "b2", "block_name": "Updated Name"}

2. CODE BLOCK OPERATIONS:
   - add_code: Create a new code block within an existing block
     {"op": "add_code", "block_id": "b1", "code": {"name": "New Code", "script": "SELECT 1;"}, "position": "end"}

   - remove_code: Delete a code block
     {"op": "remove_code", "block_id": "b0", "code_id": "b0.c0"}

   - rename_code: Change a code block's name
     {"op": "rename_code", "block_id": "b1", "code_id": "b1.c2", "code_name": "Updated Name"}

3. SQL SCRIPT OPERATIONS:
   - set_code: Replace the entire SQL script (overwrites existing)
     {"op": "set_code", "block_id": "b0", "code_id": "b0.c0", "script": "SELECT * FROM new_table;"}

   - add_script: Append or prepend SQL to existing script (preserves existing)
     {"op": "add_script", "block_id": "b2", "code_id": "b2.c1", "script": "WHERE date > '2024-01-01'",
      "position": "end"}

   - str_replace: Find and replace text in SQL scripts
     {"op": "str_replace", "search_for": "old_table", "replace_with": "new_table", "block_id": "b0",'
      "code_id": "b0.c0"}
     - Omit code_id to replace in all codes of a block
     - Omit both block_id and code_id to replace everywhere

IMPORTANT CONSIDERATIONS:
- Parameter updates are PARTIAL - only the operations you specify are applied
- All other parts of the transformation remain unchanged
- Each SQL script must be executable and follow the current SQL dialect
- Storage configuration is COMPLETE REPLACEMENT - include ALL mappings you want to keep
- Leave updated_description empty to preserve the original description
- SCHEMA CHANGES: Destructive schema changes (removing columns, changing types, renaming columns) require
  manually deleting the output table before running the updated transformation to avoid schema mismatch errors.
  Non-destructive changes (adding columns) typically do not require table deletion.

WORKFLOW:
1. Call get_config to retrieve current transformation structure and identify block_id/code_id values
2. Identify what needs to change (SQL code, storage, description)
3. For SQL changes: Prepare parameter_updates list with targeted operations
4. For storage changes: Build complete storage configuration (include all mappings)
5. Call update_sql_transformation with change_description and only the fields to change

EXAMPLE WORKFLOWS:

Example 1 - Update SQL script in existing code block:
Step 1: Get current config
  result = get_config(component_id="keboola.snowflake-transformation", configuration_id="12345")
  # Note the block_id (e.g., "b0") and code_id (e.g., "b0.c1") from result

Step 2: Update the SQL
  update_sql_transformation(
    configuration_id="12345",
    change_description="Updated WHERE clause to filter active customers only",
    parameter_updates=[
      {
        "op": "set_code",
        "block_id": "b0",      # from step 1
        "code_id": "b0.c0",    # from step 1
        "script": "SELECT * FROM customers WHERE status = 'active' AND region = 'US';"
      }
    ]
  )

Example 2 - Append a new code block to the second block of an existing transformation:
  update_sql_transformation(
    configuration_id="12345",
    change_description="Added aggregation step",
    parameter_updates=[
      {
        "op": "add_code",
        "block_id": "b1",  # second block
        "code": {
          "name": "Aggregate Sales",
          "script": "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id;"
        },
        "position": "end"
      }
    ]
  )

Example 3 - Replace table name across all SQL scripts:
  update_sql_transformation(
    configuration_id="12345",
    change_description="Renamed source table from old_customers to customers",
    parameter_updates=[
      {
        "op": "str_replace",
        "search_for": "old_customers",
        "replace_with": "customers"
        # No block_id or code_id = applies to all scripts
      }
    ]
  )

Example 4 - Update storage mappings:
  update_sql_transformation(
    configuration_id="12345",
    change_description="Added new input table",
    storage={
      "input": {
        "tables": [
          {
            "source": "in.c-main.customers",
            "destination": "customers"
          },
          {
            "source": "in.c-main.orders",
            "destination": "orders"
          }
        ]
      },
      "output": {
        "tables": [
          {
            "source": "result",
            "destination": "out.c-main.customer_summary"
          }
        ]
      }
    }
  )


**Input JSON Schema**:
```json
{
  "$defs": {
    "Block": {
      "description": "The transformation block.",
      "properties": {
        "name": {
          "description": "A descriptive name for the code block",
          "type": "string"
        },
        "codes": {
          "description": "SQL code sub-blocks",
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
          "description": "A descriptive name for the code block",
          "type": "string"
        },
        "script": {
          "description": "The SQL script of the code block",
          "type": "string"
        }
      },
      "required": [
        "name",
        "script"
      ],
      "type": "object"
    },
    "TfAddBlock": {
      "description": "Add a new block to the transformation.",
      "properties": {
        "op": {
          "const": "add_block",
          "type": "string"
        },
        "block": {
          "$ref": "#/$defs/Block",
          "description": "The block to add"
        },
        "position": {
          "default": "end",
          "description": "The position of the block to add",
          "enum": [
            "start",
            "end"
          ],
          "type": "string"
        }
      },
      "required": [
        "op",
        "block"
      ],
      "type": "object"
    },
    "TfAddCode": {
      "description": "Add a new code to an existing block in the transformation.",
      "properties": {
        "op": {
          "const": "add_code",
          "type": "string"
        },
        "block_id": {
          "description": "The ID of the block to add the code to",
          "type": "string"
        },
        "code": {
          "$ref": "#/$defs/Code",
          "description": "The code to add"
        },
        "position": {
          "default": "end",
          "description": "The position of the code to add",
          "enum": [
            "start",
            "end"
          ],
          "type": "string"
        }
      },
      "required": [
        "op",
        "block_id",
        "code"
      ],
      "type": "object"
    },
    "TfAddScript": {
      "description": "Append or prepend SQL script text to an existing code in an existing block in the transformation.",
      "properties": {
        "op": {
          "const": "add_script",
          "type": "string"
        },
        "block_id": {
          "description": "The ID of the block to add the script to",
          "type": "string"
        },
        "code_id": {
          "description": "The ID of the code to add the script to",
          "type": "string"
        },
        "script": {
          "description": "The SQL script to add",
          "type": "string"
        },
        "position": {
          "default": "end",
          "description": "The position of the script to add",
          "enum": [
            "start",
            "end"
          ],
          "type": "string"
        }
      },
      "required": [
        "op",
        "block_id",
        "code_id",
        "script"
      ],
      "type": "object"
    },
    "TfRemoveBlock": {
      "description": "Remove an existing block from the transformation.",
      "properties": {
        "op": {
          "const": "remove_block",
          "type": "string"
        },
        "block_id": {
          "description": "The ID of the block to remove",
          "type": "string"
        }
      },
      "required": [
        "op",
        "block_id"
      ],
      "type": "object"
    },
    "TfRemoveCode": {
      "description": "Remove an existing code from an existing block in the transformation.",
      "properties": {
        "op": {
          "const": "remove_code",
          "type": "string"
        },
        "block_id": {
          "description": "The ID of the block to remove the code from",
          "type": "string"
        },
        "code_id": {
          "description": "The ID of the code to remove",
          "type": "string"
        }
      },
      "required": [
        "op",
        "block_id",
        "code_id"
      ],
      "type": "object"
    },
    "TfRenameBlock": {
      "description": "Rename an existing block in the transformation.",
      "properties": {
        "op": {
          "const": "rename_block",
          "type": "string"
        },
        "block_id": {
          "description": "The ID of the block to rename",
          "type": "string"
        },
        "block_name": {
          "description": "The new name of the block",
          "type": "string"
        }
      },
      "required": [
        "op",
        "block_id",
        "block_name"
      ],
      "type": "object"
    },
    "TfRenameCode": {
      "description": "Rename an existing code in an existing block in the transformation.",
      "properties": {
        "op": {
          "const": "rename_code",
          "type": "string"
        },
        "block_id": {
          "description": "The ID of the block to rename the code in",
          "type": "string"
        },
        "code_id": {
          "description": "The ID of the code to rename",
          "type": "string"
        },
        "code_name": {
          "description": "The new name of the code",
          "type": "string"
        }
      },
      "required": [
        "op",
        "block_id",
        "code_id",
        "code_name"
      ],
      "type": "object"
    },
    "TfSetCode": {
      "description": "Set the SQL script of an existing code in an existing block in the transformation.",
      "properties": {
        "op": {
          "const": "set_code",
          "type": "string"
        },
        "block_id": {
          "description": "The ID of the block to set the code in",
          "type": "string"
        },
        "code_id": {
          "description": "The ID of the code to set",
          "type": "string"
        },
        "script": {
          "description": "The SQL script of the code to set",
          "type": "string"
        }
      },
      "required": [
        "op",
        "block_id",
        "code_id",
        "script"
      ],
      "type": "object"
    },
    "TfStrReplace": {
      "description": "Replace a substring in SQL statements in the transformation.",
      "properties": {
        "op": {
          "const": "str_replace",
          "type": "string"
        },
        "block_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The ID of the block to replace substrings in. If not provided, all blocks will be updated."
        },
        "code_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The ID of the code to replace substrings in. If not provided, all codes in the block will be updated."
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
        "search_for",
        "replace_with"
      ],
      "type": "object"
    }
  },
  "properties": {
    "configuration_id": {
      "description": "The ID of the transformation configuration to update.",
      "type": "string"
    },
    "change_description": {
      "description": "A clear, human-readable summary of what changed in this transformation update. Be specific: e.g., \"Added JOIN with customers table\", \"Updated WHERE clause to filter active records\".",
      "type": "string"
    },
    "parameter_updates": {
      "default": null,
      "description": "List of operations to apply to the transformation structure (blocks, codes, SQL scripts). Each operation modifies specific elements using block_id and code_id identifiers. Only provide if updating SQL code or block structure - do not use for description or storage changes. \n\nIMPORTANT: Use get_config first to retrieve the current transformation structure and identify the block_id and code_id values needed for your operations. IDs are automatically assigned.\n\nAvailable operations:\n1. add_block: Add a new block to the transformation\n   - Fields: op=\"add_block\", block={name, codes}, position=\"start\"|\"end\"\n2. remove_block: Remove an existing block\n   - Fields: op=\"remove_block\", block_id (e.g., \"b0\")\n3. rename_block: Rename an existing block\n   - Fields: op=\"rename_block\", block_id (e.g., \"b0\"), block_name\n4. add_code: Add a new code block to an existing block\n   - Fields: op=\"add_code\", block_id (e.g., \"b0\"), code={name, script}, position=\"start\"|\"end\"\n5. remove_code: Remove an existing code block\n   - Fields: op=\"remove_code\", block_id (e.g., \"b0\"), code_id (e.g., \"b0.c0\")\n6. rename_code: Rename an existing code block\n   - Fields: op=\"rename_code\", block_id (e.g., \"b0\"), code_id (e.g., \"b0.c0\"), code_name\n7. set_code: Replace the entire SQL script of a code block\n   - Fields: op=\"set_code\", block_id (e.g., \"b0\"), code_id (e.g., \"b0.c0\"), script\n8. add_script: Append or prepend SQL to a code block\n   - Fields: op=\"add_script\", block_id (e.g., \"b0\"), code_id (e.g., \"b0.c0\"), script,     position=\"start\"|\"end\"\n9. str_replace: Replace substring in SQL scripts\n   - Fields: op=\"str_replace\", search_for, replace_with, block_id (optional), code_id (optional)\n   - If block_id omitted: replaces in all blocks\n   - If code_id omitted: replaces in all codes of the specified block\n",
      "items": {
        "discriminator": {
          "mapping": {
            "add_block": "#/$defs/TfAddBlock",
            "add_code": "#/$defs/TfAddCode",
            "add_script": "#/$defs/TfAddScript",
            "remove_block": "#/$defs/TfRemoveBlock",
            "remove_code": "#/$defs/TfRemoveCode",
            "rename_block": "#/$defs/TfRenameBlock",
            "rename_code": "#/$defs/TfRenameCode",
            "set_code": "#/$defs/TfSetCode",
            "str_replace": "#/$defs/TfStrReplace"
          },
          "propertyName": "op"
        },
        "oneOf": [
          {
            "$ref": "#/$defs/TfAddBlock"
          },
          {
            "$ref": "#/$defs/TfRemoveBlock"
          },
          {
            "$ref": "#/$defs/TfRenameBlock"
          },
          {
            "$ref": "#/$defs/TfAddCode"
          },
          {
            "$ref": "#/$defs/TfRemoveCode"
          },
          {
            "$ref": "#/$defs/TfRenameCode"
          },
          {
            "$ref": "#/$defs/TfSetCode"
          },
          {
            "$ref": "#/$defs/TfAddScript"
          },
          {
            "$ref": "#/$defs/TfStrReplace"
          }
        ]
      },
      "type": "array"
    },
    "storage": {
      "additionalProperties": true,
      "default": null,
      "description": "Complete storage configuration for transformation input/output table mappings. Only provide if updating storage mappings - this replaces the ENTIRE storage configuration. \n\nWhen to use:\n- Adding/removing input tables for the transformation\n- Modifying output table mappings and destinations\n- Changing table aliases used in SQL\n\nImportant:\n- Must conform to transformation storage schema (input/output tables)\n- Replaces ALL existing storage config - include all mappings you want to keep\n- Use get_config first to see current storage configuration\n- Leave unfilled to preserve existing storage configuration",
      "type": "object"
    },
    "updated_description": {
      "default": "",
      "description": "New detailed description for the transformation. Only provide if changing the description. Should explain what the transformation does, data sources, and business logic. Leave empty to preserve the original description.",
      "type": "string"
    },
    "is_disabled": {
      "default": false,
      "description": "Whether to disable the transformation. Set to True to disable execution without deleting. Default is False (transformation remains enabled).",
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

Deploys/redeploys a data app or stops running data app in the Keboola environment asynchronously given the action
and the configuration ID.

Considerations:
- Redeploying a data app takes some time, and the app temporarily may have status "stopped" during this process
because it needs to restart.
- After deployment, the deployment info includes the app URL and the latest logs to diagnose in-app errors.


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
- Data App detail contains configuration, metadata, source code, links, and deployment info along with the latest
data app logs to investigate in-app errors. The logs may be updated after opening the data app URL.


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
`{QUERY_DATA_FUNCTION}` where a `query_data` function will be injected. This function queries the workspace to get
data, it accepts a string of SQL query following current sql dialect and returns a pandas DataFrame with the results
from the workspace.
- Write SQL queries so they are compatible with the current workspace backend, you can ensure this by using the
`query_data` tool to inspect the data in the workspace before using it in the data app.
- If you're updating an existing data app, provide the `configuration_id` parameter and the `change_description`
parameter. To keep existing data app values during an update, leave them as empty strings, lists, or None
appropriately based on the parameter type.
- If the data app is updated while running, it must be redeployed for the changes to take effect.
- New apps use the HTTP basic authentication by default for security unless explicitly specified otherwise; when
updating, set `authentication_type` to `default` to keep the existing authentication type configuration
(including OIDC setups) unless explicitly specified otherwise.


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
    "authentication_type": {
      "description": "Authentication type, \"no-auth\" removes authentication completely, \"basic-auth\" sets the data app to be secured using the HTTP basic authentication, and \"default\" keeps the existing authentication type when updating.",
      "enum": [
        "no-auth",
        "basic-auth",
        "default"
      ],
      "type": "string"
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
    "packages",
    "authentication_type"
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

Creates a new conditional flow configuration using `keboola.flow`.

PRE-REQUISITES:
- Always use `get_flow_schema` with flow_type="keboola.flow" and review `get_flow_examples` if unknown
- Gather component configuration IDs for all tasks you include

RULES:
- `phases` and `tasks` must follow the keboola.flow schema; each entry needs `id` and `name`
- Exactly one entry phase (no incoming transitions); all phases must be reachable
- Connect phases via `next` transitions; no cycles or dangling phases; empty `next` means flow end
- Task/phase failures already stop the flow; add retries/conditions only if the user requests them
- Always share the returned links with the user

WHEN TO USE:
- Flows needing branching, conditions, retries, or notifications
- Default choice when user simply says “create a flow,” unless they explicitly want legacy orchestrator behavior


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

Creates a new legacy (non-conditional) flow using `keboola.orchestrator`.

PRE-REQUISITES:
- Always use `get_flow_schema` with flow_type="keboola.orchestrator" and review `get_flow_examples` if unknown
- Collect component configuration IDs for every task you include

RULES:
- `phases` and `tasks` must follow the orchestrator schema; each entry must include `id` and `name`
- Phases run sequentially; tasks inside a phase run in parallel
- Use `dependsOn` on phases to sequence them; reference other phase ids
- Always share the returned links with the user

WHEN TO USE:
- Simple/linear orchestrations without branching or conditions
- ETL/ELT pipelines where phases just need ordering and parallel task groups


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
<a name="get_flow_examples"></a>
## get_flow_examples
**Annotations**: `read-only`

**Tags**: `flows`

**Description**:

Retrieves examples of valid flow configurations.

PRE-REQUISITES:
- Unknown examples for the target flow type: `keboola.flow` (conditional) or `keboola.orchestrator` (legacy) to help
build the specific flow configuration by mirroring the structure/fields.

RULES:
- Conditional-flow examples require conditional flows to be enabled; otherwise use legacy orchestrator examples
- Present the examples or cite unavailability to the user


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

Returns the JSON schema for the given flow type (markdown).

PRE-REQUISITES:
- Unknown schema for the target flow type: `keboola.flow` (conditional) or `keboola.orchestrator` (legacy)

RULES:
- Projects without conditional flows enabled cannot request `keboola.flow` schema
- Use the returned schema to shape `phases` and `tasks` for `create_flow` / `create_conditional_flow` /
`update_flow`


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
<a name="get_flows"></a>
## get_flows
**Annotations**: `read-only`

**Tags**: `flows`

**Description**:

Lists flows or retrieves full details for specific flows.

OPTIONS:
- `flow_ids=[]` → summaries of all flows in the project
- `flow_ids=["id1", ...]` → full details (including phases/tasks) for those flows


**Input JSON Schema**:
```json
{
  "properties": {
    "flow_ids": {
      "default": [],
      "description": "IDs of flows to retrieve full details for. When provided (non-empty), returns full flow configurations including phases and tasks. When empty [], lists all flows in the project as summaries.",
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
<a name="modify_flow"></a>
## modify_flow
**Annotations**: `destructive`

**Tags**: `flows`

**Description**:

Updates an existing flow configuration (either legacy `keboola.orchestrator` or conditional `keboola.flow`) or
manages schedules for this flow.

PRE-REQUISITES:
- Always use `get_flow_schema` (and `get_flow_examples`) for that flow type you want to update to follow the
required structure and see the examples if unknown
- Only pass `phases`/`tasks` when you want to replace them; omit to keep the existing ones unchanged

RULES (ALL FLOWS):
- `flow_type` must match the stored component id of the flow; do not switch flow types during update
- `phases` and `tasks` must follow the schema for the selected flow type; include at least `id` and `name`
- Tasks must reference existing component configurations; keep dependencies consistent
- Always provide a clear `change_description` and surface any links returned in the response to the user
- A flow can have multiple schedules for automation runs. Add/update/remove schedules only if requested.
- When updating a flow or a schedule, specify only the fields you want to update, others will be kept unchanged.

CONDITIONAL FLOWS (`keboola.flow`):
- Maintain a single entry phase and ensure every phase is reachable; connect phases via `next` transitions
- No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

LEGACY FLOWS (`keboola.orchestrator`):
- Phases run sequentially; tasks inside a phase run in parallel; `dependsOn` references other phase ids
- Use `continueOnFailure` or best-effort patterns only when the user explicitly asks for them

WHEN TO USE:
- Renaming a flow, updating descriptions, adding/removing phases or tasks, updating schedules or
adjusting dependencies


**Input JSON Schema**:
```json
{
  "$defs": {
    "ScheduleRequest": {
      "properties": {
        "action": {
          "description": "Action to perform on the schedule.",
          "enum": [
            "add",
            "update",
            "remove"
          ],
          "type": "string"
        },
        "scheduleId": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "ID of the schedule configuration to update. None if creating a new schedule."
        },
        "timezone": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Timezone for the schedule. Default UTC if None provided."
        },
        "cronTab": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Cron expression for the schedule following the format: `* * * * *`.Where 1. minutes, 2. hours, 3. days of month, 4. months, 5. days of week. Example: `15,45 1,13 * * 0`"
        },
        "state": {
          "anyOf": [
            {
              "enum": [
                "enabled",
                "disabled"
              ],
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Enable or disable the schedule."
        }
      },
      "required": [
        "action"
      ],
      "type": "object"
    }
  },
  "properties": {
    "configuration_id": {
      "description": "ID of the flow configuration.",
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
    },
    "schedules": {
      "default": [],
      "description": "Optional sequence of schedule requests to add/update/remove schedules for this flow. Each request must have \"action\": \"add\"|\"update\"|\"remove\". For add: include \"cron_tab\", \"state\" (\"enabled\"|\"disabled\"), \"timezone\". For update/remove: include \"schedule_id\". Example: [{\"action\": \"add\", \"cron_tab\": \"0 8 * * 1-5\", \"state\": \"enabled\", \"timezone\": \"UTC\"}]",
      "items": {
        "$ref": "#/$defs/ScheduleRequest"
      },
      "type": "array"
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
<a name="update_flow"></a>
## update_flow
**Annotations**: `destructive`

**Tags**: `flows`

**Description**:

Updates an existing flow configuration (either legacy `keboola.orchestrator` or conditional `keboola.flow`).

PRE-REQUISITES:
- Always use `get_flow_schema` (and `get_flow_examples`) for that flow type you want to update to follow the
required structure and see the examples if unknown
- Only pass `phases`/`tasks` when you want to replace them; omit to keep the existing ones unchanged

RULES (ALL FLOWS):
- `flow_type` must match the stored component id of the flow; do not switch flow types during update
- `phases` and `tasks` must follow the schema for the selected flow type; include at least `id` and `name`
- Tasks must reference existing component configurations; keep dependencies consistent
- Always provide a clear `change_description` and surface any links returned in the response to the user

CONDITIONAL FLOWS (`keboola.flow`):
- Maintain a single entry phase and ensure every phase is reachable; connect phases via `next` transitions
- No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

LEGACY FLOWS (`keboola.orchestrator`):
- Phases run sequentially; tasks inside a phase run in parallel; `dependsOn` references other phase ids
- Use `continueOnFailure` or best-effort patterns only when the user explicitly asks for them

WHEN TO USE:
- Renaming a flow, updating descriptions, adding/removing phases or tasks, or adjusting dependencies


**Input JSON Schema**:
```json
{
  "properties": {
    "configuration_id": {
      "description": "ID of the flow configuration.",
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
<a name="get_jobs"></a>
## get_jobs
**Annotations**: `read-only`

**Tags**: `jobs`

**Description**:

Retrieves job execution information from the Keboola project.

CONTEXT:
Jobs in Keboola are execution records of components (extractors, transformations, writers, flows).
Each job represents a single run with its status, timing, configuration, and results.

TWO MODES OF OPERATION (controlled by job_ids parameter):

MODE 1: GET DETAILS FOR SPECIFIC JOBS (job_ids is non-empty)
- Provide one or more job IDs: job_ids=["12345", "67890"]
- Returns: FULL details for each job including status, config_data, results, timing, and metadata
- Ignores: All filtering/sorting parameters (status, component_id, config_id, limit, offset, sort_by, sort_order)
- Use when: You know specific job IDs and need complete information about them

MODE 2: LIST/SEARCH JOBS (job_ids is empty)
- Leave job_ids empty: job_ids=[]
- Returns: SUMMARY list of jobs (id, status, component_id, config_id, timing only - no config_data or results)
- Supports: Filtering by status/component_id/config_id, pagination with limit/offset, sorting
- Use when: You need to find jobs, see recent executions, or monitor job history

DECISION GUIDE:
- Start with MODE 2 (list) to find jobs → then use MODE 1 (details) if you need full information
- If you already know job IDs → use MODE 1 directly
- For monitoring/browsing → use MODE 2 with filters

COMMON WORKFLOWS:
1. Find failed jobs: job_ids=[], status="error" → identify problematic job IDs → get details with MODE 1
2. Check recent runs: job_ids=[], component_id="...", limit=10 → see latest executions
3. Monitor specific job: job_ids=["123"] → poll for status and results
4. Troubleshoot config: job_ids=[], component_id="...", config_id="...", status="error" → find which runs failed

EXAMPLES:

MODE 1 - Get full details:
- job_ids=["12345"] → detailed info for job 12345
- job_ids=["12345", "67890"] → detailed info for multiple jobs

MODE 2 - List/search jobs:
- job_ids=[] → list latest 100 jobs (default)
- job_ids=[], status="error" → list only failed jobs
- job_ids=[], status="processing" → list currently running jobs
- job_ids=[], component_id="keboola.ex-aws-s3" → list jobs for S3 extractor
- job_ids=[], component_id="keboola.ex-aws-s3", config_id="12345" → list jobs for specific configuration
- job_ids=[], limit=50, offset=100 → pagination (skip first 100, get next 50)
- job_ids=[], sort_by="endTime", sort_order="asc" → oldest completed first
- job_ids=[], sort_by="durationSeconds", sort_order="desc" → longest running first


**Input JSON Schema**:
```json
{
  "properties": {
    "job_ids": {
      "default": [],
      "description": "IDs of jobs to retrieve full details for. When provided (non-empty), returns full job details including status, parameters, results, and metadata. When empty [], lists jobs in the project as summaries with optional filtering.",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "status": {
      "default": null,
      "description": "The optional status of the jobs to filter by when listing (ignored if job_ids is provided). If None then all statuses are included.",
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
      "description": "The optional ID of the component whose jobs you want to list (ignored if job_ids is provided). Default = None.",
      "type": "string"
    },
    "config_id": {
      "default": null,
      "description": "The optional ID of the component configuration whose jobs you want to list (ignored if job_ids is provided). Default = None.",
      "type": "string"
    },
    "limit": {
      "default": 100,
      "description": "The number of jobs to list when listing (ignored if job_ids is provided), default = 100, max = 500.",
      "maximum": 500,
      "minimum": 1,
      "type": "integer"
    },
    "offset": {
      "default": 0,
      "description": "The offset of the jobs to list when listing (ignored if job_ids is provided), default = 0.",
      "minimum": 0,
      "type": "integer"
    },
    "sort_by": {
      "default": "startTime",
      "description": "The field to sort the jobs by when listing (ignored if job_ids is provided), default = \"startTime\".",
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
      "description": "The order to sort the jobs by when listing (ignored if job_ids is provided), default = \"desc\".",
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

WHEN TO USE:
- Use when you want to find the component for a specific purpose.

USAGE EXAMPLES:
- user_input: "I am looking for a salesforce extractor component"
  → Returns a list of component IDs that match the query, ordered by relevance/best match.


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
<a name="search"></a>
## search
**Annotations**: `read-only`

**Tags**: `search`

**Description**:

Searches for Keboola items (tables, buckets, configurations, transformations, flows, etc.) in the current project
by matching patterns against item ID, name, display name, or description. Returns matching items grouped by type
with their IDs and metadata.

WHEN TO USE:
- User asks to "find", "locate", or "search for" something by name
- User mentions a partial name and you need to find the full item (e.g., "find the customer table")
- User asks "what tables/configs/flows do I have with X in the name?"
- You need to discover items before performing operations on them
- User asks to "list all items with [name] in it"
- DO NOT use for listing all items of a specific type. Use get_configs, list_tables, get_flows, etc instead.

HOW IT WORKS:
- Searches by regex pattern matching against id, name, displayName, and description fields
- For tables, also searches column names and column descriptions
- Case-insensitive search
- Multiple patterns work as OR condition - matches items containing ANY of the patterns
- Returns grouped results by item type (tables, buckets, configurations, flows, etc.)
- Each result includes the item's ID, name, creation date, and relevant metadata

IMPORTANT:
- Always use this tool when the user mentions a name but you don't have the exact ID
- The search returns IDs that you can use with other tools (e.g., get_table, get_configs, get_flows)
- Results are ordered by update time. The most recently updated items are returned first.
- For exact ID lookups, use specific tools like get_table, get_configs, get_flows instead
- Use find_component_id and get_configs tools to find configurations related to a specific component

USAGE EXAMPLES:
- user_input: "Find all tables with 'customer' in the name"
  → patterns=["customer"], item_types=["table"]
  → Returns all tables whose id, name, displayName, or description contains "customer"

- user_input: "Find tables with 'email' column"
  → patterns=["email"], item_types=["table"]
  → Returns all tables that have a column named "email" or with "email" in column description

- user_input: "Search for the sales transformation"
  → patterns=["sales"], item_types=["transformation"]
  → Returns transformations with "sales" in any searchable field

- user_input: "Find items named 'daily report' or 'weekly summary'"
  → patterns=["daily.*report", "weekly.*summary"], item_types=[]
  → Returns all items matching any of these patterns

- user_input: "Show me all configurations related to Google Analytics"
  → patterns=["google.*analytics"], item_types=["configuration"]
  → Returns configurations with matching patterns


**Input JSON Schema**:
```json
{
  "properties": {
    "patterns": {
      "description": "One or more search patterns to match against item ID, name, display name, or description. Supports regex patterns. Case-insensitive. Examples: [\"customer\"], [\"sales\", \"revenue\"], [\"test.*table\"]. Do not use empty strings or empty lists.",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "item_types": {
      "default": [],
      "description": "Optional filter for specific Keboola item types. Leave empty to search all types. Common values: \"table\" (data tables), \"bucket\" (table containers), \"transformation\" (SQL/Python transformations), \"configuration\" (extractor/writer configs), \"flow\" (orchestration flows). Use when you know what type of item you're looking for.",
      "items": {
        "enum": [
          "flow",
          "bucket",
          "table",
          "transformation",
          "configuration",
          "configuration-row",
          "workspace",
          "shared-code",
          "rows",
          "state"
        ],
        "type": "string"
      },
      "type": "array"
    },
    "limit": {
      "default": 50,
      "description": "Maximum number of items to return (default: 50, max: 100).",
      "type": "integer"
    },
    "offset": {
      "default": 0,
      "description": "Number of matching items to skip for pagination (default: 0).",
      "type": "integer"
    }
  },
  "required": [
    "patterns"
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
* Always use the LIMIT clause in your SELECT statements when fetching data. There are hard limits imposed
  by this tool on the maximum number of rows that can be fetched and the maximum number of characters.
  The tool will truncate the data if those limits are exceeded.

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
<a name="get_buckets"></a>
## get_buckets
**Annotations**: `read-only`

**Tags**: `storage`

**Description**:

Lists buckets or retrieves full details of specific buckets.

EXAMPLES:
- `bucket_ids=[]` → summaries of all buckets in the project
- `bucket_ids=["id1", ...]` → full details of the buckets with the specified IDs


**Input JSON Schema**:
```json
{
  "properties": {
    "bucket_ids": {
      "default": [],
      "description": "Filter by specific bucket IDs.",
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
<a name="get_tables"></a>
## get_tables
**Annotations**: `read-only`

**Tags**: `storage`

**Description**:

Lists tables in buckets or retrieves full details of specific tables, including fully qualified database name,
column definitions, and metadata.

RETURNS:
- With `bucket_ids`: Summaries of tables (ID, name, description, primary key).
- With `table_ids`: Full details including columns, data types, and fully qualified database names.

COLUMN DATA TYPES:
- database_native_type: The actual type in the storage backend (Snowflake, BigQuery, etc.)
  with precision, scale, and other implementation details
- keboola_base_type: Standardized type indicating the semantic data type. May not always be
  available. When present, it reveals the actual type of data stored in the column - for example,
  a column with database_native_type VARCHAR might have keboola_base_type INTEGER, indicating
  it stores integer values despite being stored as text in the backend.

EXAMPLES:
- `bucket_ids=["id1", ...]` → summary info of the tables in the buckets with the specified IDs
- `table_ids=["id1", ...]` → detailed info of the tables specified by their IDs
- `bucket_ids=[]` and `table_ids=[]` → empty list; you have to specify at least one filter


**Input JSON Schema**:
```json
{
  "properties": {
    "bucket_ids": {
      "default": [],
      "description": "Filter by specific bucket IDs.",
      "items": {
        "type": "string"
      },
      "type": "array"
    },
    "table_ids": {
      "default": [],
      "description": "Filter by specific table IDs.",
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
