# Tools Documentation
This document provides details about the tools available in the Keboola MCP server.

## Index

### Component Tools
- [add_config_row](#add_config_row): Creates a component configuration row in the specified configuration_id, using the specified name, component ID, configuration JSON, and description.
- [create_config](#create_config): Creates a root component configuration using the specified name, component ID, configuration JSON, and description.
- [create_sql_transformation](#create_sql_transformation): Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed description, and a list of created table names.
- [get_components](#get_components): Retrieves detailed information about one or more components by their IDs.
- [get_config_examples](#get_config_examples): Retrieves sample configuration examples for a specific component.
- [get_configs](#get_configs): Retrieves component configurations in the project with optional filtering.
- [run_sync_action](#run_sync_action): Executes a synchronous action for a component configuration or a component row configuration.
- [update_config](#update_config): Updates an existing root component configuration by modifying its parameters, storage mappings, name or description.
- [update_config_row](#update_config_row): Updates an existing component configuration row by modifying its parameters, storage mappings, name, or description.
- [update_sql_transformation](#update_sql_transformation): Updates an existing SQL transformation configuration by modifying its SQL code, storage mappings, name or description.

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
- [create_python_js_data_app_git_credential](#create_python_js_data_app_git_credential): Mints a one-time HTTPS token on a python-js **prod** data app so the caller can clone, pull, and push to the app's managed git repo over HTTPS.
- [delete_python_js_data_app_draft](#delete_python_js_data_app_draft): Deletes a python-js DRAFT data app — both the data-app instance (DSAPI) and its Storage configuration.
- [deploy_data_app](#deploy_data_app): Deploys/redeploys a data app or stops a running data app in the Keboola environment asynchronously, given the action and the configuration ID.
- [get_data_apps](#get_data_apps): Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by providing their configuration IDs.
- [modify_python_js_data_app](#modify_python_js_data_app): Creates or updates a python-js data app.
- [modify_streamlit_data_app](#modify_streamlit_data_app): Creates or updates a Streamlit data app.

### Project Tools
- [get_project_info](#get_project_info): Retrieves structured information about the current project, including essential context and base instructions for working with it (e.
- [update_project_description](#update_project_description): Updates the description of the current Keboola project.

### SQL Tools
- [query_data](#query_data): Executes an SQL SELECT query to get the data from the underlying database.

### Search Tools
- [find_component_id](#find_component_id): Returns a list of component IDs that match the given natural-language query.
- [search](#search): Searches for Keboola items (tables, buckets, components, configurations, transformations, flows, data-apps, etc.

### Semantic Tools
- [get_semantic_context](#get_semantic_context): Loads semantic objects grouped by semantic object type.
- [get_semantic_schema](#get_semantic_schema): Returns JSON schemas for the requested semantic object types.
- [search_semantic_context](#search_semantic_context): Searches semantic models and semantic objects using regex patterns matched against their names, descriptions and
stringified JSON attributes.
- [validate_semantic_query](#validate_semantic_query): Performs best-effort semantic validation of an SQL query against one or more semantic models and compares it with
the expected semantic objects provided.

### Storage Tools
- [get_buckets](#get_buckets): Lists buckets or retrieves full details of specific buckets, including descriptions,
lineage references (created/updated by), and links.
- [get_tables](#get_tables): Lists tables in buckets or retrieves full details of specific tables, including fully qualified database name,
column definitions, lineage references (created/updated by) and links.
- [update_descriptions](#update_descriptions): Updates the description for Keboola storage items (buckets, tables, or columns).

---

# Component Tools
<a name="add_config_row"></a>
## add_config_row
**Annotations**: 

**Tags**: `components`

**Description**:

Creates a component configuration row in the specified configuration_id, using the specified name, component ID, configuration JSON, and description.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "A short, descriptive name summarizing the purpose of the component configuration."
    },
    "description": {
      "type": "string",
      "description": "The detailed description of the component configuration explaining its purpose and functionality."
    },
    "component_id": {
      "type": "string",
      "description": "The ID of the component for which to create the configuration."
    },
    "configuration_id": {
      "type": "string",
      "description": "The ID of the configuration for which to create the configuration row."
    },
    "parameters": {
      "type": "object",
      "propertyNames": {
        "type": "string"
      },
      "additionalProperties": {},
      "description": "The component row configuration parameters, adhering to the configuration_row_schema"
    },
    "storage": {
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that have tables or file input mapping defined",
      "anyOf": [
        {
          "type": "object",
          "propertyNames": {
            "type": "string"
          },
          "additionalProperties": {}
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_before": {
      "description": "The list of processors that will run before the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_after": {
      "description": "The list of processors that will run after the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "name",
    "description",
    "component_id",
    "configuration_id",
    "parameters"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="create_config"></a>
## create_config
**Annotations**: 

**Tags**: `components`

**Description**:

Creates a root component configuration using the specified name, component ID, configuration JSON, and description.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "A short, descriptive name summarizing the purpose of the component configuration."
    },
    "description": {
      "type": "string",
      "description": "The detailed description of the component configuration explaining its purpose and functionality."
    },
    "component_id": {
      "type": "string",
      "description": "The ID of the component for which to create the configuration."
    },
    "parameters": {
      "type": "object",
      "propertyNames": {
        "type": "string"
      },
      "additionalProperties": {},
      "description": "The component configuration parameters, adhering to the configuration_schema"
    },
    "storage": {
      "description": "The table and/or file input / output mapping of the component configuration. It is present only for components that have tables or file input mapping defined",
      "anyOf": [
        {
          "type": "object",
          "propertyNames": {
            "type": "string"
          },
          "additionalProperties": {}
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_before": {
      "description": "The list of processors that will run before the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_after": {
      "description": "The list of processors that will run after the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "variables": {
      "description": "Variable definitions to attach to this configuration. Each entry specifies a name, type (\"string\" or \"vault\"), and an optional default value. On creation, both `None` (omitted) and `[]` (empty list) mean \"do not attach variables\" — no `keboola.variables` config is created. To remove variables from an existing configuration, use `update_config` with `variables=[]`.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "name": {
                "type": "string",
                "description": "Variable name."
              },
              "type": {
                "default": "string",
                "description": "Variable type: \"string\" or \"vault\".",
                "type": "string",
                "enum": [
                  "string",
                  "vault"
                ]
              },
              "default_value": {
                "description": "Optional default value bound at creation time.",
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ]
              }
            },
            "required": [
              "name"
            ]
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "name",
    "description",
    "component_id",
    "parameters"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="create_sql_transformation"></a>
## create_sql_transformation
**Annotations**: 

**Tags**: `components`

**Description**:

Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed description, and a list of created table names.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "A short, descriptive name summarizing the purpose of the SQL transformation."
    },
    "description": {
      "type": "string",
      "description": "The detailed description of the SQL transformation capturing the user intent, explaining the SQL query, and the expected output."
    },
    "sql_code_blocks": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string",
            "description": "A descriptive name for the code block"
          },
          "script": {
            "type": "string",
            "description": "The SQL script of the code block"
          }
        },
        "required": [
          "name",
          "script"
        ]
      },
      "description": "The SQL query code blocks, each containing a descriptive name and an executable SQL script written in the current SQL dialect. The query will be automatically reformatted to be more readable."
    },
    "created_table_names": {
      "default": [],
      "description": "A list of created table names if they are generated within the SQL query statements (e.g., using `CREATE TABLE ...`).",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "folder": {
      "default": "",
      "description": "Folder name to organize this transformation in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more transformations in the project. If there are 20 or more transformations, you should assign one of the existing folders or create a new one that clearly reflects the transformation purpose.",
      "type": "string"
    },
    "variables": {
      "description": "Variable definitions to attach to this transformation. Each entry specifies a name, type (\"string\" or \"vault\"), and an optional default value. On creation, both `None` (omitted) and `[]` (empty list) mean \"do not attach variables\" — no `keboola.variables` config is created. To remove variables from an existing transformation, use `update_sql_transformation` with `variables=[]`.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "name": {
                "type": "string",
                "description": "Variable name."
              },
              "type": {
                "default": "string",
                "description": "Variable type: \"string\" or \"vault\".",
                "type": "string",
                "enum": [
                  "string",
                  "vault"
                ]
              },
              "default_value": {
                "description": "Optional default value bound at creation time.",
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ]
              }
            },
            "required": [
              "name"
            ]
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "name",
    "description",
    "sql_code_blocks"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="get_components"></a>
## get_components
**Annotations**: `read-only`

**Tags**: `components`

**Description**:

Retrieves detailed information about one or more components by their IDs.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "component_ids": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "IDs of the components to retrieve."
    }
  },
  "required": [
    "component_ids"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="get_config_examples"></a>
## get_config_examples
**Annotations**: `read-only`

**Tags**: `components`

**Description**:

Retrieves sample configuration examples for a specific component.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "component_id": {
      "type": "string",
      "description": "The ID of the component to get configuration examples for."
    }
  },
  "required": [
    "component_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="get_configs"></a>
## get_configs
**Annotations**: `read-only`

**Tags**: `components`

**Description**:

Retrieves component configurations in the project with optional filtering.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "component_types": {
      "default": [],
      "description": "Filter by component types; empty = all. Ignored when configs/component_ids given.",
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "application",
          "extractor",
          "transformation",
          "writer"
        ]
      }
    },
    "component_ids": {
      "default": [],
      "description": "Filter by specific component IDs. Ignored when configs is given.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "configs": {
      "default": [],
      "description": "Specific configs to retrieve full details for (grouped by component).",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "component_id": {
            "type": "string"
          },
          "configuration_id": {
            "type": "string"
          }
        },
        "required": [
          "component_id",
          "configuration_id"
        ]
      }
    }
  },
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="run_sync_action"></a>
## run_sync_action
**Annotations**: 

**Tags**: `components`

**Description**:

Executes a synchronous action for a component configuration or a component row configuration.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "action_name": {
      "type": "string",
      "description": "The sync action to execute (e.g., \"testConnection\", \"getTables\")."
    },
    "component_id": {
      "type": "string",
      "description": "The ID of the component (e.g., \"keboola.ex-db-mysql\")."
    },
    "configuration_id": {
      "type": "string",
      "description": "The ID of the configuration to use for the sync action."
    },
    "configuration_row_id": {
      "description": "Optional row ID; row parameters/storage are shallow-merged on top of root config.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "action_name",
    "component_id",
    "configuration_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="update_config"></a>
## update_config
**Annotations**: `destructive`

**Tags**: `components, config-diff-preview`

**Description**:

Updates an existing root component configuration by modifying its parameters, storage mappings, name or description. Updates are PARTIAL — only provide the fields you want to change; parameter_updates apply granular diff operations to the existing parameters.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "change_description": {
      "type": "string",
      "description": "A clear, human-readable summary of what changed in this update. Be specific: e.g., \"Updated API key\", \"Added customers table to input mapping\"."
    },
    "component_id": {
      "type": "string",
      "description": "The ID of the component the configuration belongs to."
    },
    "configuration_id": {
      "type": "string",
      "description": "The ID of the configuration to update."
    },
    "name": {
      "default": "",
      "description": "New name for the configuration. Only provide if changing the name. Name should be short (typically under 50 characters) and descriptive.",
      "type": "string"
    },
    "description": {
      "default": "",
      "description": "New detailed description for the configuration. Only provide if changing the description. Should explain the purpose, data sources, and behavior of this configuration. Leave empty to preserve the original description.",
      "type": "string"
    },
    "parameter_updates": {
      "description": "List of granular parameter update operations to apply. Each operation (set, str_replace, remove, list_append) modifies a specific value using JSONPath notation. Only provide if updating parameters - do not use for changing description, storage or processors. Paths are relative to the `parameters` object, not the configuration root (e.g. use `tables`, not `parameters.tables`). Prefer simple JSONPaths (e.g., \"array_param[1]\", \"object_param.key\") and make the smallest possible updates - only change what needs changing. In case you need to replace the whole parameters section, you can use the `set` operation with `$` as path.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "oneOf": [
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "set"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the parameter key to set (e.g., \"api_key\", \"database.host\")"
                  },
                  "value": {
                    "description": "New value to set"
                  }
                },
                "required": [
                  "op",
                  "path",
                  "value"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "str_replace"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the parameter key to modify"
                  },
                  "search_for": {
                    "type": "string",
                    "description": "Substring to search for (non-empty)"
                  },
                  "replace_with": {
                    "type": "string",
                    "description": "Replacement string (can be empty for deletion)"
                  }
                },
                "required": [
                  "op",
                  "path",
                  "search_for",
                  "replace_with"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "remove"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the parameter key to remove"
                  }
                },
                "required": [
                  "op",
                  "path"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "list_append"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the list parameter"
                  },
                  "value": {
                    "description": "Value to append to the list"
                  }
                },
                "required": [
                  "op",
                  "path",
                  "value"
                ]
              }
            ]
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "storage": {
      "description": "Complete storage configuration containing input/output table and file mappings. Only provide if updating storage mappings - this replaces the ENTIRE storage configuration.",
      "anyOf": [
        {
          "type": "object",
          "propertyNames": {
            "type": "string"
          },
          "additionalProperties": {}
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_before": {
      "description": "The list of processors that will run before the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_after": {
      "description": "The list of processors that will run after the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "folder": {
      "description": "Folder name to organize this configuration in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more configurations in the project. If there are 20 or more configurations, you should assign one of the existing folders or create a new one that clearly reflects the configuration purpose.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    },
    "variables": {
      "description": "Variable definitions for this configuration. Provide a non-empty list to create or replace all variable definitions. Provide an empty list ([]) to remove all variables. Omit (None) to leave existing variables unchanged.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "name": {
                "type": "string",
                "description": "Variable name."
              },
              "type": {
                "default": "string",
                "description": "Variable type: \"string\" or \"vault\".",
                "type": "string",
                "enum": [
                  "string",
                  "vault"
                ]
              },
              "default_value": {
                "description": "Optional default value bound at creation time.",
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ]
              }
            },
            "required": [
              "name"
            ]
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "change_description",
    "component_id",
    "configuration_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="update_config_row"></a>
## update_config_row
**Annotations**: `destructive`

**Tags**: `components, config-diff-preview`

**Description**:

Updates an existing component configuration row by modifying its parameters, storage mappings, name, or description. Updates are PARTIAL — only provide the fields you want to change; parameter_updates apply granular diff operations to the existing row parameters.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "change_description": {
      "type": "string",
      "description": "A clear, human-readable summary of what changed in this row update. Be specific."
    },
    "component_id": {
      "type": "string",
      "description": "The ID of the component the configuration belongs to."
    },
    "configuration_id": {
      "type": "string",
      "description": "The ID of the parent configuration containing the row to update."
    },
    "configuration_row_id": {
      "type": "string",
      "description": "The ID of the specific configuration row to update."
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
      "description": "List of granular parameter update operations to apply to this row. Each operation (set, str_replace, remove, list_append) modifies a specific parameter using JSONPath notation. Only provide if updating parameters - do not use for changing description or storage. Paths are relative to the row's `parameters` object, not the row root (e.g. use `tables`, not `parameters.tables`). Prefer simple dot-delimited JSONPaths and make the smallest possible updates - only change what needs changing. In case you need to replace the whole parameters, you can use the `set` operation with `$` as path.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "oneOf": [
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "set"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the parameter key to set (e.g., \"api_key\", \"database.host\")"
                  },
                  "value": {
                    "description": "New value to set"
                  }
                },
                "required": [
                  "op",
                  "path",
                  "value"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "str_replace"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the parameter key to modify"
                  },
                  "search_for": {
                    "type": "string",
                    "description": "Substring to search for (non-empty)"
                  },
                  "replace_with": {
                    "type": "string",
                    "description": "Replacement string (can be empty for deletion)"
                  }
                },
                "required": [
                  "op",
                  "path",
                  "search_for",
                  "replace_with"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "remove"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the parameter key to remove"
                  }
                },
                "required": [
                  "op",
                  "path"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "list_append"
                  },
                  "path": {
                    "type": "string",
                    "description": "JSONPath to the list parameter"
                  },
                  "value": {
                    "description": "Value to append to the list"
                  }
                },
                "required": [
                  "op",
                  "path",
                  "value"
                ]
              }
            ]
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "storage": {
      "description": "Complete storage configuration for this row containing input/output table and file mappings. Only provide if updating storage mappings - this replaces the ENTIRE storage configuration for this row.",
      "anyOf": [
        {
          "type": "object",
          "propertyNames": {
            "type": "string"
          },
          "additionalProperties": {}
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_before": {
      "description": "The list of processors that will run before the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "processors_after": {
      "description": "The list of processors that will run after the configured component runs.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "is_disabled": {
      "description": "Enable or disable the configuration row. Set to True to disable execution (config row won't run), False to enable execution (config row will run). Only provide if changing the status, leave as null to preserve current state.",
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "change_description",
    "component_id",
    "configuration_id",
    "configuration_row_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="update_sql_transformation"></a>
## update_sql_transformation
**Annotations**: `destructive`

**Tags**: `components, config-diff-preview`

**Description**:

Updates an existing SQL transformation configuration by modifying its SQL code, storage mappings, name or description. parameter_updates apply PARTIAL, granular diff operations to the transformation blocks/codes; storage is a complete replacement.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "change_description": {
      "type": "string",
      "description": "A clear, human-readable summary of what changed in this transformation update. Be specific: e.g., \"Added JOIN with customers table\", \"Updated WHERE clause to filter active records\"."
    },
    "configuration_id": {
      "type": "string",
      "description": "The ID of the transformation configuration to update."
    },
    "name": {
      "default": "",
      "description": "New name for the transformation. Only provide if changing the name. Name should be short (typically under 50 characters) and descriptive.",
      "type": "string"
    },
    "description": {
      "default": "",
      "description": "New detailed description for the transformation. Only provide if changing the description. Should explain what the transformation does, data sources, and business logic. Leave empty to preserve the original description.",
      "type": "string"
    },
    "parameter_updates": {
      "description": "List of operations to apply to the transformation structure (blocks, codes, SQL scripts). Each operation modifies specific elements using block_id and code_id identifiers. Only provide if updating SQL code or block structure - do not use for description or storage changes. Use get_configs first to retrieve the current transformation structure and identify the block_id and code_id values needed for your operations. IDs are automatically assigned. Available operations: add_block, remove_block, rename_block, add_code, remove_code, rename_code, set_code, add_script, str_replace.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "oneOf": [
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "add_block"
                  },
                  "block": {
                    "type": "object",
                    "properties": {
                      "name": {
                        "type": "string",
                        "description": "A descriptive name for the code block"
                      },
                      "codes": {
                        "type": "array",
                        "items": {
                          "type": "object",
                          "properties": {
                            "name": {
                              "type": "string",
                              "description": "A descriptive name for the code block"
                            },
                            "script": {
                              "type": "string",
                              "description": "The SQL script of the code block"
                            }
                          },
                          "required": [
                            "name",
                            "script"
                          ]
                        },
                        "description": "SQL code sub-blocks"
                      }
                    },
                    "required": [
                      "name",
                      "codes"
                    ],
                    "description": "The block to add"
                  },
                  "position": {
                    "default": "end",
                    "type": "string",
                    "enum": [
                      "start",
                      "end"
                    ]
                  }
                },
                "required": [
                  "op",
                  "block"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "remove_block"
                  },
                  "block_id": {
                    "type": "string",
                    "description": "The ID of the block to remove"
                  }
                },
                "required": [
                  "op",
                  "block_id"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "rename_block"
                  },
                  "block_id": {
                    "type": "string"
                  },
                  "block_name": {
                    "type": "string",
                    "description": "The new name of the block"
                  }
                },
                "required": [
                  "op",
                  "block_id",
                  "block_name"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "add_code"
                  },
                  "block_id": {
                    "type": "string"
                  },
                  "code": {
                    "type": "object",
                    "properties": {
                      "name": {
                        "type": "string",
                        "description": "A descriptive name for the code block"
                      },
                      "script": {
                        "type": "string",
                        "description": "The SQL script of the code block"
                      }
                    },
                    "required": [
                      "name",
                      "script"
                    ],
                    "description": "The code to add"
                  },
                  "position": {
                    "default": "end",
                    "type": "string",
                    "enum": [
                      "start",
                      "end"
                    ]
                  }
                },
                "required": [
                  "op",
                  "block_id",
                  "code"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "remove_code"
                  },
                  "block_id": {
                    "type": "string"
                  },
                  "code_id": {
                    "type": "string"
                  }
                },
                "required": [
                  "op",
                  "block_id",
                  "code_id"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "rename_code"
                  },
                  "block_id": {
                    "type": "string"
                  },
                  "code_id": {
                    "type": "string"
                  },
                  "code_name": {
                    "type": "string",
                    "description": "The new name of the code"
                  }
                },
                "required": [
                  "op",
                  "block_id",
                  "code_id",
                  "code_name"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "set_code"
                  },
                  "block_id": {
                    "type": "string"
                  },
                  "code_id": {
                    "type": "string"
                  },
                  "script": {
                    "type": "string",
                    "description": "The SQL script of the code to set"
                  }
                },
                "required": [
                  "op",
                  "block_id",
                  "code_id",
                  "script"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "add_script"
                  },
                  "block_id": {
                    "type": "string"
                  },
                  "code_id": {
                    "type": "string"
                  },
                  "script": {
                    "type": "string",
                    "description": "The SQL script to add"
                  },
                  "position": {
                    "default": "end",
                    "type": "string",
                    "enum": [
                      "start",
                      "end"
                    ]
                  }
                },
                "required": [
                  "op",
                  "block_id",
                  "code_id",
                  "script"
                ]
              },
              {
                "type": "object",
                "properties": {
                  "op": {
                    "type": "string",
                    "const": "str_replace"
                  },
                  "block_id": {
                    "anyOf": [
                      {
                        "type": "string"
                      },
                      {
                        "type": "null"
                      }
                    ]
                  },
                  "code_id": {
                    "anyOf": [
                      {
                        "type": "string"
                      },
                      {
                        "type": "null"
                      }
                    ]
                  },
                  "search_for": {
                    "type": "string",
                    "description": "Substring to search for (non-empty)"
                  },
                  "replace_with": {
                    "type": "string",
                    "description": "Replacement string (can be empty for deletion)"
                  }
                },
                "required": [
                  "op",
                  "search_for",
                  "replace_with"
                ]
              }
            ]
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "storage": {
      "description": "Complete storage configuration for transformation input/output table mappings. Only provide if updating storage mappings - this replaces the ENTIRE storage configuration.",
      "anyOf": [
        {
          "type": "object",
          "propertyNames": {
            "type": "string"
          },
          "additionalProperties": {}
        },
        {
          "type": "null"
        }
      ]
    },
    "folder": {
      "description": "Folder name to organize this transformation in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more transformations in the project. If there are 20 or more transformations, you should assign one of the existing folders or create a new one that clearly reflects the transformation purpose.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    },
    "variables": {
      "description": "Variable definitions for this transformation. Provide a non-empty list to create or replace all variable definitions. Provide an empty list ([]) to remove all variables. Omit (None) to leave existing variables unchanged.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "name": {
                "type": "string",
                "description": "Variable name."
              },
              "type": {
                "default": "string",
                "description": "Variable type: \"string\" or \"vault\".",
                "type": "string",
                "enum": [
                  "string",
                  "vault"
                ]
              },
              "default_value": {
                "description": "Optional default value bound at creation time.",
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "type": "null"
                  }
                ]
              }
            },
            "required": [
              "name"
            ]
          }
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "change_description",
    "configuration_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---

# Other Tools
<a name="create_python_js_data_app_git_credential"></a>
## create_python_js_data_app_git_credential
**Annotations**: 

**Tags**: `data-apps`

**Description**:

Mints a one-time HTTPS token on a python-js **prod** data app so the caller can clone, pull, and push to the app's managed git repo over HTTPS.

**Always call against the prod app's configuration_id** — drafts have no managed repo of their own, so calling this on a draft fails. The prod app is the canonical repo owner; drafts iterate against branches of that same repo.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. This tool only mints credentials.

Returns a ready-to-use `git_clone_url` of the form `https://kai:<secret>@<host>/<path>.git` plus the raw `secret`. The token is returned **only** at creation — the platform cannot return it again on any subsequent read. Stash the URL (or the secret) somewhere the LLM can reuse for the rest of the session.

## Constraints
- Only python-js prod data apps have a managed git repo. Streamlit apps reject the call with a clear error.
- Permissions are always `readWrite`.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "configuration_id": {
      "type": "string",
      "description": "Storage configuration ID of the python-js data app."
    }
  },
  "required": [
    "configuration_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="delete_python_js_data_app_draft"></a>
## delete_python_js_data_app_draft
**Annotations**: `destructive`

**Tags**: `data-apps`

**Description**:

Deletes a python-js DRAFT data app — both the data-app instance (DSAPI) and its Storage configuration.

**MCP never runs git on your behalf.** Deleting the feature branch on the remote is your job; this tool only tears down the draft config and its data-app instance.

WHEN TO CALL: at the end of a promote-to-prod sequence, after you have merged the draft's branch into `main`, pushed, deleted the feature branch from the remote, and redeployed the prod app. The Keboola UI lists drafts under their parent prod app; once you call this tool, the draft disappears from that list.

WHAT THIS TOOL REFUSES:
  - prod apps (no `isDraft` flag) — protects against accidental prod deletion;
  - Streamlit apps — they have no draft concept.

WHAT THIS TOOL DOES NOT DO:
  - Run git. Deleting the feature branch on the remote is your job.
  - Revoke the prod-side git credential minted when the draft was created.

After a successful call, pivot back to the parent prod app (its configuration_id is returned in the response) or to `get_data_apps` for further work.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "configuration_id": {
      "type": "string",
      "description": "Storage configuration ID of the python-js draft data app to delete."
    }
  },
  "required": [
    "configuration_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="deploy_data_app"></a>
## deploy_data_app
**Annotations**: 

**Tags**: `data-apps`

**Description**:

Deploys/redeploys a data app or stops a running data app in the Keboola environment asynchronously, given the action and the configuration ID.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. This tool only triggers deploys against existing git state.

## Mode (python-js apps)
- `mode='dev'` deploys the target as a **dev version of the data app** — the runtime uses a development `setup.sh` (hot reload) and the data-app proxy enables an auto-auth path so an iframe preview can render without a manual login. Only meaningful on **draft** configs (python-js apps with `isDraft=true`).
- For prod redeploys (including after merging a draft's branch into `main`), use no `mode` — the prod app picks up the current `main`.
- The branch a draft deploys from is pinned in `parameters.dataApp.git.branch` at create time; there is no deploy-time override.
- python-js apps do NOT fetch a Storage `configVersion` for deployment (their source lives in git, not in the Storage configuration); this is handled automatically.

## Streamlit apps
Streamlit apps have no managed git repo, so `mode` has no effect on the deployed app. `mode=None` is the expected call shape.

## General considerations
- Redeploying a data app takes some time, and the app may temporarily report status "stopped" during the restart.
- After deployment, the deployment info includes the app URL and the latest logs to help diagnose in-app errors.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "action": {
      "type": "string",
      "enum": [
        "deploy",
        "stop"
      ],
      "description": "The action to perform."
    },
    "configuration_id": {
      "type": "string",
      "description": "The ID of the data app configuration."
    },
    "mode": {
      "description": "Deployment mode. Set to \"dev\" to deploy a python-js draft as a **dev version of the data app** — the runtime uses a development `setup.sh` (hot reload), and the data-app proxy enables an auto-auth path so an iframe preview can render without a manual login. Only meaningful on **draft** configs (python-js apps with `isDraft=true`). Leave None (default) for prod redeploys and for Streamlit apps.",
      "anyOf": [
        {
          "type": "string",
          "enum": [
            "dev",
            "production"
          ]
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "action",
    "configuration_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="get_data_apps"></a>
## get_data_apps
**Annotations**: `read-only`

**Tags**: `data-apps`

**Description**:

Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by providing their configuration IDs.

WHEN NOT TO USE:
- Do NOT list all data apps just to find one by name. Use `search` with item_types=["data-app"] instead.
- Only list all data apps when you need a complete inventory.

Considerations:
- If configuration_ids are provided, the tool will return details of the data apps by their configuration IDs.
- If no configuration_ids are provided, the tool will list all data apps in the project given the limit and offset.
- Data App detail contains configuration, metadata, source code, links, and deployment info along with the latest data app logs to investigate in-app errors. The logs may be updated after opening the data app URL.
- `deployment_info.last_run` carries the outcome of the most recent deployment attempt. For an app that fails to start, check its `failure_reason`/`failure_message` FIRST — they cover setup-phase failures (e.g. invalid secrets, git clone errors, failing setup scripts) that happen before the container starts and therefore never appear in the regular logs.
- `repo_url` (managed git repo URL for python-js apps) is ONLY populated on the detail path (when `configuration_ids` is provided). The inventory list always returns `repo_url=None`, even for python-js apps with a managed repo — to retrieve the URL, call this tool again with the target `configuration_ids`.
- When called with `configuration_ids=[<prod-cfg>]` for a python-js **prod** app, the response includes a `drafts: [...]` array of every draft (configs with `isDraft=true` and `parentConfigurationId == <prod-cfg>`) currently in the project. Drafts in trash are not included. The array is empty for drafts themselves and for Streamlit apps.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "configuration_ids": {
      "default": [],
      "description": "The IDs of the data app configurations.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "limit": {
      "default": 100,
      "description": "The limit of the data apps to fetch.",
      "type": "integer",
      "minimum": -9007199254740991,
      "maximum": 9007199254740991
    },
    "offset": {
      "default": 0,
      "description": "The offset of the data apps to fetch.",
      "type": "integer",
      "minimum": -9007199254740991,
      "maximum": 9007199254740991
    }
  },
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="modify_python_js_data_app"></a>
## modify_python_js_data_app
**Annotations**: `destructive`

**Tags**: `data-apps`

**Description**:

Creates or updates a python-js data app.

Two-app project model. Every python-js project has a persistent **prod app** that owns the only managed git repository for the project, and zero or more **drafts** parented to that prod app. A draft is a Storage configuration with `parameters.dataApp.isDraft=true` and `parameters.dataApp.parentConfigurationId=<prod cfg id>`; it's an *external-git* app that clones the parent prod's repo at a pinned branch on every deploy. Drafts are surfaced in the Keboola UI under their parent prod app. Use `deploy_data_app(mode='dev')` to deploy a draft as a dev version of the data app (hot reload + auto-auth for iframe preview); use `delete_python_js_data_app_draft` to tear a draft down after its branch has been promoted.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. MCP gives you authenticated clone URLs and manages configs/deploys; it never invokes git.

**The draft flow is mandatory — never edit prod source directly.** Every source-code change goes through a draft branch that the user previews and explicitly approves first. NEVER push directly to `main`: `main` only ever advances by merging an approved draft branch, and only after the user has approved that draft's preview.

## Argument rules
- `parent_configuration_id` is **create-only**. Rejected on update.
- `branch` is **create-only** and only valid when `parent_configuration_id` is set. Defaults to `'init'`. Must not be `'main'`. Rejected on prod create and on update.
- `slug` is required on create and immutable after.
- The **update path** (passing `configuration_id`) is for changing `name`, `description`, `authentication_type`, `auto_suspend_after_seconds`, `storage` on either a prod app or a draft. Source code changes go through the git flow above, not this tool.

## Authentication
New apps default to HTTP basic authentication for safety. Pass `authentication_type='no-auth'` to expose publicly. On update, `authentication_type='default'` preserves the existing `authorization` block (including OIDC setups configured outside the MCP); `'basic-auth'` / `'no-auth'` overwrite it.

## Slug constraint
Must be DNS-label-safe (lowercase letters, digits, hyphens, ≤63 chars). For drafts, append a short suffix (e.g. `-draft-abc123`) to keep slugs unique across the prod and its drafts.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "Name of the data app (max ~50 chars to fit DNS label limit)."
    },
    "description": {
      "type": "string",
      "description": "Description of the data app."
    },
    "configuration_id": {
      "default": "",
      "description": "The ID of existing data app configuration when updating, otherwise empty string.",
      "type": "string"
    },
    "change_description": {
      "default": "",
      "description": "The description of the change when updating (e.g. \"Bump image\"), otherwise empty string.",
      "type": "string"
    },
    "slug": {
      "description": "URL-safe slug for the data app (used as a subdomain). Required when creating; immutable after.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    },
    "parent_configuration_id": {
      "description": "Storage configuration ID of the prod python-js data app this draft will iterate against. When set on create, the new app is created as a **draft**: no managed repo is provisioned for it; instead its `parameters.dataApp.git` block is populated to point at the prod app's managed repo, with a freshly-minted prod-app HTTPS token and the chosen draft branch. Leave None on create to make a **prod app** (which gets its own managed repo). Rejected on update.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    },
    "branch": {
      "description": "Draft branch to pin the new draft to. Only valid on the draft create path (when `parent_configuration_id` is set). Defaults to `init` when unset. Must not be `main` (reserved for the prod app). Rejected on prod create and on update.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    },
    "authentication_type": {
      "default": "default",
      "description": "Authentication type. \"no-auth\" removes authentication completely, \"basic-auth\" secures the data app via HTTP basic authentication, and \"default\" means: on create, apply basic auth (safe default for new apps); on update, keep the existing authentication configuration (including OIDC setups configured outside the MCP).",
      "type": "string",
      "enum": [
        "no-auth",
        "basic-auth",
        "default"
      ]
    },
    "auto_suspend_after_seconds": {
      "default": 900,
      "description": "Number of seconds after which the running data app is automatically suspended.",
      "type": "integer",
      "minimum": -9007199254740991,
      "maximum": 9007199254740991
    },
    "storage": {
      "description": "Complete storage configuration for the data app (input/output table mappings). Replaces the ENTIRE storage block when updating an existing app. Leave unset (None) to preserve the existing storage configuration; pass an empty dict to explicitly clear it.",
      "anyOf": [
        {
          "type": "object",
          "propertyNames": {
            "type": "string"
          },
          "additionalProperties": {}
        },
        {
          "type": "null"
        }
      ]
    },
    "folder": {
      "description": "Folder name to organize this data app in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more data apps in the project. If there are 20 or more data apps, you should assign one of the existing folders or create a new one that clearly reflects the data app purpose.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "name",
    "description"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="modify_streamlit_data_app"></a>
## modify_streamlit_data_app
**Annotations**: `destructive`

**Tags**: `config-diff-preview, data-apps`

**Description**:

Creates or updates a Streamlit data app.

Considerations:
- The `source_code` parameter must be a complete and runnable Streamlit app. It must include a placeholder `{QUERY_DATA_FUNCTION}` where a `query_data` function will be injected. This function queries the workspace to get data, it accepts a string of SQL query following current sql dialect and returns a pandas DataFrame with the results from the workspace.
- Write SQL queries so they are compatible with the current workspace backend, you can ensure this by using the `query_data` tool to inspect the data in the workspace before using it in the data app.
- If you're updating an existing data app, provide the `configuration_id` parameter and the `change_description` parameter. To keep existing data app values during an update, leave them as empty strings, lists, or None appropriately based on the parameter type.
- After creating or updating a data app with this tool, ALWAYS call `deploy_data_app(action="deploy", configuration_id=...)` to start a new app or restart an existing app so changes take effect. Without this step, a newly created app will not start, and an existing app will keep running the previous deployment without the latest changes.
- New apps use the HTTP basic authentication by default for security unless explicitly specified otherwise; when updating, set `authentication_type` to `default` to keep the existing authentication type configuration (including OIDC setups) unless explicitly specified otherwise.

SQL & DATA TYPE RULES:
- Use delimited identifiers for the current SQL dialect for all column names and aliases in SQL. Match the exact identifier case used in SQL when referencing columns in Python code.
- `query_data` RETURNS ALL COLUMNS AS STRINGS regardless of SQL CAST. Always convert types in Python after loading: `df["col"] = pd.to_numeric(df["col"], errors="coerce").fillna(0)` and `df["date"] = pd.to_datetime(df["date"], errors="coerce")`.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "Name of the data app (max ~50 chars to fit DNS label limit)."
    },
    "description": {
      "type": "string",
      "description": "Description of the data app."
    },
    "source_code": {
      "type": "string",
      "description": "Complete Python/Streamlit source code for the data app."
    },
    "packages": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "Python packages used in the source code that will be installed by `pip install` into the environment before the code runs. For example: [\"pandas\", \"requests~=2.32\"]."
    },
    "authentication_type": {
      "type": "string",
      "enum": [
        "no-auth",
        "basic-auth",
        "default"
      ],
      "description": "Authentication type, \"no-auth\" removes authentication completely, \"basic-auth\" sets the data app to be secured using the HTTP basic authentication, and \"default\" keeps the existing authentication type when updating."
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
    },
    "folder": {
      "description": "Folder name to organize this data app in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more data apps in the project. If there are 20 or more data apps, you should assign one of the existing folders or create a new one that clearly reflects the data app purpose.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "name",
    "description",
    "source_code",
    "packages",
    "authentication_type"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
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
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Natural language query to search for in the documentation."
    }
  },
  "required": [
    "query"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
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
- Default choice when user simply says "create a flow," unless they explicitly want legacy orchestrator behavior


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "A short, descriptive name for the flow."
    },
    "description": {
      "type": "string",
      "description": "Detailed description of the flow purpose."
    },
    "phases": {
      "type": "array",
      "items": {
        "type": "object",
        "propertyNames": {
          "type": "string"
        },
        "additionalProperties": {}
      },
      "description": "List of phase definitions for conditional flows."
    },
    "tasks": {
      "type": "array",
      "items": {
        "type": "object",
        "propertyNames": {
          "type": "string"
        },
        "additionalProperties": {}
      },
      "description": "List of task definitions for conditional flows."
    },
    "folder": {
      "default": "",
      "description": "Folder name to organize this flow in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more flows in the project. If there are 20 or more flows, you should assign one of the existing folders or create a new one that clearly reflects the flow purpose.",
      "type": "string"
    }
  },
  "required": [
    "name",
    "description",
    "phases",
    "tasks"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
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
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "description": "A short, descriptive name for the flow."
    },
    "description": {
      "type": "string",
      "description": "Detailed description of the flow purpose."
    },
    "phases": {
      "type": "array",
      "items": {
        "type": "object",
        "propertyNames": {
          "type": "string"
        },
        "additionalProperties": {}
      },
      "description": "List of phase definitions."
    },
    "tasks": {
      "type": "array",
      "items": {
        "type": "object",
        "propertyNames": {
          "type": "string"
        },
        "additionalProperties": {}
      },
      "description": "List of task definitions."
    },
    "folder": {
      "default": "",
      "description": "Folder name to organize this flow in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more flows in the project. If there are 20 or more flows, you should assign one of the existing folders or create a new one that clearly reflects the flow purpose.",
      "type": "string"
    }
  },
  "required": [
    "name",
    "description",
    "phases",
    "tasks"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
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
  "type": "object",
  "properties": {
    "flow_type": {
      "type": "string",
      "enum": [
        "keboola.flow",
        "keboola.orchestrator"
      ],
      "description": "The type of the flow to retrieve examples for."
    }
  },
  "required": [
    "flow_type"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
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
  "type": "object",
  "properties": {
    "flow_type": {
      "type": "string",
      "enum": [
        "keboola.flow",
        "keboola.orchestrator"
      ],
      "description": "The type of flow for which to fetch schema."
    }
  },
  "required": [
    "flow_type"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="get_flows"></a>
## get_flows
**Annotations**: `read-only`

**Tags**: `flows`

**Description**:

Lists flows or retrieves full details for specific flows.

WHEN NOT TO USE:
- Do NOT call with `flow_ids=[]` just to find a flow by name. Use `search` with
  item_types=["flow"] instead.
- Only use `flow_ids=[]` when you need a complete list of all flows in the project.

OPTIONS:
- `flow_ids=[]` → summaries of all flows in the project
- `flow_ids=["id1", ...]` → full details (including phases/tasks) for those flows


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "flow_ids": {
      "default": [],
      "description": "IDs of flows to retrieve full details for. When provided (non-empty), returns full flow configurations including phases and tasks. When empty [], lists all flows in the project as summaries.",
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="modify_flow"></a>
## modify_flow
**Annotations**: `destructive`

**Tags**: `config-diff-preview, flows`

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
- Renaming a flow, updating descriptions, adding/removing phases or tasks, updating schedules,
adjusting dependencies, or enabling/disabling flow execution


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "configuration_id": {
      "type": "string",
      "description": "ID of the flow configuration."
    },
    "flow_type": {
      "type": "string",
      "enum": [
        "keboola.flow",
        "keboola.orchestrator"
      ],
      "description": "The type of flow to update. Use \"keboola.flow\" for conditional flows or \"keboola.orchestrator\" for legacy flows. This MUST match the existing flow type."
    },
    "change_description": {
      "type": "string",
      "description": "Description of changes made."
    },
    "phases": {
      "description": "Updated list of phase definitions.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "tasks": {
      "description": "Updated list of task definitions.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
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
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "action": {
            "type": "string",
            "enum": [
              "add",
              "update",
              "remove"
            ],
            "description": "Action to perform on the schedule."
          },
          "schedule_id": {
            "description": "ID of the schedule configuration to update. None if creating a new schedule.",
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ]
          },
          "timezone": {
            "description": "Timezone for the schedule. Default UTC if None provided.",
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ]
          },
          "cron_tab": {
            "description": "Cron expression for the schedule following the format: `* * * * *`.Where 1. minutes, 2. hours, 3. days of month, 4. months, 5. days of week. Example: `15,45 1,13 * * 0`",
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ]
          },
          "state": {
            "description": "Enable or disable the schedule.",
            "anyOf": [
              {
                "type": "string",
                "enum": [
                  "enabled",
                  "disabled"
                ]
              },
              {
                "type": "null"
              }
            ]
          }
        },
        "required": [
          "action"
        ]
      }
    },
    "is_disabled": {
      "description": "Enable or disable the flow. Set to True to disable execution (flow won't run), False to enable execution (flow will run). Only provide if changing the status, leave as null to preserve current state.",
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ]
    },
    "folder": {
      "description": "Folder name to organize this flow in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more flows in the project. If there are 20 or more flows, you should assign one of the existing folders or create a new one that clearly reflects the flow purpose.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "configuration_id",
    "flow_type",
    "change_description"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="update_flow"></a>
## update_flow
**Annotations**: `destructive`

**Tags**: `config-diff-preview, flows`

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
- Renaming a flow, updating descriptions, adding/removing phases or tasks, adjusting dependencies,
or enabling/disabling flow execution


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "configuration_id": {
      "type": "string",
      "description": "ID of the flow configuration."
    },
    "flow_type": {
      "type": "string",
      "enum": [
        "keboola.flow",
        "keboola.orchestrator"
      ],
      "description": "The type of flow to update. Use \"keboola.flow\" for conditional flows or \"keboola.orchestrator\" for legacy flows. This MUST match the existing flow type."
    },
    "change_description": {
      "type": "string",
      "description": "Description of changes made."
    },
    "phases": {
      "description": "Updated list of phase definitions.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
    },
    "tasks": {
      "description": "Updated list of task definitions.",
      "anyOf": [
        {
          "type": "array",
          "items": {
            "type": "object",
            "propertyNames": {
              "type": "string"
            },
            "additionalProperties": {}
          }
        },
        {
          "type": "null"
        }
      ]
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
    "is_disabled": {
      "description": "Enable or disable the flow. Set to True to disable execution (flow won't run), False to enable execution (flow will run). Only provide if changing the status, leave as null to preserve current state.",
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ]
    },
    "folder": {
      "description": "Folder name to organize this flow in the Keboola UI. Pass an empty string to remove an existing folder assignment. Existing folder names are returned in the response change_summary when no folder is provided and there are 20 or more flows in the project. If there are 20 or more flows, you should assign one of the existing folders or create a new one that clearly reflects the flow purpose.",
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "configuration_id",
    "flow_type",
    "change_description"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
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


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "job_ids": {
      "default": [],
      "description": "IDs of jobs to retrieve full details for; empty lists jobs as summaries.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "status": {
      "description": "Filter listed jobs by status (ignored if job_ids given).",
      "type": "string",
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
      ]
    },
    "component_id": {
      "description": "Filter listed jobs by component id (ignored if job_ids given).",
      "type": "string"
    },
    "config_id": {
      "description": "Filter listed jobs by configuration id (ignored if job_ids given).",
      "type": "string"
    },
    "limit": {
      "default": 100,
      "description": "Number of jobs to list (max 500).",
      "type": "integer",
      "minimum": 1,
      "maximum": 500
    },
    "offset": {
      "default": 0,
      "description": "Offset of jobs to list.",
      "type": "integer",
      "minimum": 0,
      "maximum": 9007199254740991
    },
    "sort_by": {
      "default": "startTime",
      "description": "Field to sort listed jobs by.",
      "type": "string",
      "enum": [
        "startTime",
        "endTime",
        "createdTime",
        "durationSeconds",
        "id"
      ]
    },
    "sort_order": {
      "default": "desc",
      "description": "Sort order for listed jobs.",
      "type": "string",
      "enum": [
        "asc",
        "desc"
      ]
    },
    "include_logs": {
      "default": false,
      "description": "Include execution logs (only when job_ids given).",
      "type": "boolean"
    },
    "log_tail_lines": {
      "default": 50,
      "description": "Max log events per job (most recent).",
      "type": "integer",
      "minimum": 1,
      "maximum": 500
    },
    "log_event_types": {
      "description": "Filter log events by type (only when include_logs=true).",
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "info",
          "warn",
          "error",
          "success"
        ]
      }
    }
  },
  "$schema": "http://json-schema.org/draft-07/schema#"
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
  "type": "object",
  "properties": {
    "component_id": {
      "type": "string",
      "description": "The ID of the component or transformation to start a job for."
    },
    "configuration_id": {
      "type": "string",
      "description": "The ID of the configuration to start a job for."
    },
    "configuration_row_ids": {
      "description": "Optional configuration row IDs to run; if omitted, all rows are executed.",
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "required": [
    "component_id",
    "configuration_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---

# OAuth Tools
<a name="create_oauth_url"></a>
## create_oauth_url
**Annotations**: 

**Tags**: `oauth`

**Description**:

Generates an OAuth authorization URL for a Keboola component configuration.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "component_id": {
      "type": "string",
      "description": "The component ID to grant access to (e.g., \"keboola.ex-google-analytics-v4\")."
    },
    "config_id": {
      "type": "string",
      "description": "The configuration ID for the component."
    }
  },
  "required": [
    "component_id",
    "config_id"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---

# Project Tools
<a name="get_project_info"></a>
## get_project_info
**Annotations**: `read-only`

**Tags**: `project`

**Description**:

Retrieves structured information about the current project, including essential context and base instructions for working with it (e.g., transformations, components, workflows, and dependencies).

Always call this tool at least once at the start of a conversation to establish the project context before using other tools.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {},
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="update_project_description"></a>
## update_project_description
**Annotations**: `destructive`

**Tags**: `project`

**Description**:

Updates the description of the current Keboola project.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "description": {
      "type": "string",
      "description": "The new project description text."
    }
  },
  "required": [
    "description"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---

# Search Tools
<a name="find_component_id"></a>
## find_component_id
**Annotations**: `read-only`

**Tags**: `search`

**Description**:

Returns a list of component IDs that match the given natural-language query.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Natural language query to find the requested component."
    }
  },
  "required": [
    "query"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="search"></a>
## search
**Annotations**: `read-only`

**Tags**: `search`

**Description**:

Searches for Keboola items (tables, buckets, components, configurations, transformations, flows, data-apps, etc.) in the current project and returns matching IDs and metadata. Supports textual search (matches item names, server-side) and config-based search (matches patterns against the configuration JSON content, optionally narrowed by JSONPath scopes). THIS IS THE PRIMARY DISCOVERY TOOL — use it before any get_* tool when you need to find items by name or configuration content. Multiple patterns work as an OR condition. Textual search prefers the current branch and, when nothing is found there, automatically widens to all branches of the project.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "patterns": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "One or more search patterns. For textual search they match item names (server-side, tokenized full-text); for config-based search they match the configuration JSON content. Case-insensitive by default. Examples: [\"customer\"], [\"sales\", \"revenue\"], [\"my_bucket\"]. Do not use empty strings or empty lists."
    },
    "item_types": {
      "default": [],
      "description": "Filter for specific Keboola item types. Common values: \"table\" (data tables), \"bucket\" (table containers), \"transformation\" (SQL/Python transformations), \"component\" (extractor/writer/application components), \"data-app\" (data apps), \"flow\" (orchestration flows). Use when you know what type of item you're looking for or leave empty to search all types.",
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "bucket",
          "table",
          "data-app",
          "flow",
          "transformation",
          "component",
          "configuration",
          "configuration-row",
          "workspace",
          "shared-code",
          "rows",
          "state"
        ]
      }
    },
    "search_type": {
      "default": "textual",
      "description": "Search mode: \"textual\" (name/id/description) or \"config-based\" (stringified configuration payloads). (default: \"textual\")",
      "type": "string",
      "enum": [
        "textual",
        "config-based"
      ]
    },
    "scopes": {
      "default": [],
      "description": "JSONPath expressions to narrow config-based search to specific parts of the configuration. Simple dot-notation (e.g. \"parameters\", \"storage.input\") and full JSONPath (e.g. \"$.tasks[*]\") are both supported (e.g. \"parameters.host\", \"storage.input[0].source\"). Leave empty to search the whole configuration.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "mode": {
      "default": "literal",
      "description": "How to interpret patterns. Applies to config-based search only: \"regex\" for regular expressions or \"literal\" for exact text (default: \"literal\"). Ignored by textual search, which is always a tokenized full-text name query (not typo-corrected) and rejects \"regex\".",
      "type": "string",
      "enum": [
        "regex",
        "literal"
      ]
    },
    "limit": {
      "default": 50,
      "description": "Maximum number of items to return (default: 50, max: 100).",
      "type": "number"
    },
    "offset": {
      "default": 0,
      "description": "Number of matching items to skip for pagination (default: 0).",
      "type": "number"
    }
  },
  "required": [
    "patterns"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---

# Semantic Tools
<a name="get_semantic_context"></a>
## get_semantic_context
**Annotations**: `read-only`

**Tags**: `semantic`

**Description**:

Loads semantic objects grouped by semantic object type.

CONSIDERATIONS:
- If a selection has empty `ids`, the tool returns all objects of that type in compact form.
- If a selection has non-empty `ids`, the tool returns only those specific objects with full attributes.
- `semantic_model_ids` optionally narrows the lookup to specific semantic models.

WHEN TO USE:
- When you already know IDs of the semantic objects you want to load and want to inspect them in detail.
- When you want to list all semantic objects of certain types or specific semantic models.
- When you want to list semantic models.

WHEN NOT TO USE:
- When you need to discover semantic objects.

EXAMPLES:
- List all semantic models:
  `semantic_objects=[{"object_type": "semantic-model"}]`
- List semantic datasets and metrics for specific semantic models:
  `semantic_objects=[{"object_type": "semantic-dataset"}, {"object_type": "semantic-metric"}],`
  `semantic_model_ids=["model-uuid-1", "model-uuid-2"]`
- Get detailed context for specific semantic objects by their id:
  `semantic_objects=[{"object_type": "semantic-dataset", "ids": ["dataset-uuid-1"]},`
  `{"object_type": "semantic-metric", "ids": ["metric-uuid-1", "metric-uuid-2"]}]`
- List all constraints for specific semantic models:
  `semantic_objects=[{"object_type": "semantic-constraint"}], semantic_model_ids=["model-uuid-1"]`


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "semantic_objects": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "object_type": {
            "type": "string",
            "enum": [
              "semantic-model",
              "semantic-dataset",
              "semantic-metric",
              "semantic-relationship",
              "semantic-glossary",
              "semantic-constraint"
            ],
            "description": "Semantic object type to load."
          },
          "ids": {
            "default": [],
            "description": "Specific object UUIDs to include. Empty list [] means include all objects of this type.",
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        },
        "required": [
          "object_type"
        ]
      },
      "description": "List of semantic object selections to load. Each item contains \"object_type\" and optional \"ids\". If \"ids\" is empty, all objects of that type are returned in compact form. If \"ids\" is non-empty, only those objects are returned with full attributes."
    },
    "semantic_model_ids": {
      "default": [],
      "description": "Optional list of semantic model IDs to restrict loading to specific models. Empty list [] means load across all semantic models.",
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "required": [
    "semantic_objects"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="get_semantic_schema"></a>
## get_semantic_schema
**Annotations**: `read-only`

**Tags**: `semantic`

**Description**:

Returns JSON schemas for the requested semantic object types.

WHEN TO USE:
- When you want to know the JSON schema of a semantic object type, e.g. before searching something specific.


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "semantic_types": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "semantic-model",
          "semantic-dataset",
          "semantic-metric",
          "semantic-relationship",
          "semantic-glossary",
          "semantic-constraint"
        ]
      },
      "description": "List of semantic object types for which JSON schemas should be returned. Each returned item contains the requested semantic type and its metastore schema."
    }
  },
  "required": [
    "semantic_types"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="search_semantic_context"></a>
## search_semantic_context
**Annotations**: `read-only`

**Tags**: `semantic`

**Description**:

Searches semantic models and semantic objects using regex patterns matched against their names, descriptions and
stringified JSON attributes.

Returns compact matches grouped by semantic model. Each match includes the semantic object type,
the paths where the patterns matched, and compact object view.

CONSIDERATIONS:
- The search is case-insensitive by default. Use `case_sensitive=True` when exact casing matters.
- The search is performed against semantic object names and data attributes which are stringified JSON objects
following their corresponding JSON schema.
- The search can be scoped to specific semantic models or semantic object types but prefer broader search without
scoping unless required by the context.

WHEN TO USE:
- When you need to discover which semantic objects are relevant to a user request.
- When you know business terms, column names, metric fragments, or rule names, but not exact object UUIDs.
- When you need to find semantic objects by keyword or values used in their attributes.

WHEN NOT TO USE:
- When you know the exact IDs.

EXAMPLES:
- Find semantic objects by business concepts for revenue or sales:
  `patterns=["revenue", "sales"]`
- Find semantic objects using a Keboola table ID:
  `patterns=["out.c-sales-main.fact_orders"]`
- Find semantic dataset for a certain table:
  `patterns=["in.c-sales-main.fact_orders"], semantic_types=["semantic-dataset"]`
- Find semantic datasets that mention a column name:
  `patterns=["column_name"], semantic_types=["semantic-dataset"]`
- Search semantic objects e.g. semantic metrics, relationships, and constraints using a certain semantic dataset:
  `patterns=["table-id-of-the-dataset"], semantic_types=["semantic-metric",`
  `"semantic-relationship", "semantic-constraint"]`
- Search semantic constraints using e.g. certain semantic metrics and certain semantic datasets:
  `patterns=["metric-name-1", "metric-name-2", "table-id-from-the-dataset"],`
  `semantic_types=["semantic-metric", "semantic-relationship"]`
- Search something within specific semantic models only:
  `patterns=["something"], semantic_model_ids=["<semantic-model-uuid-1>", "<semantic-model-uuid-2>"]`


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "patterns": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "One or more regex patterns used to search semantic metadata. The search checks semantic model names plus semantic object names and nested attribute values. Use multiple patterns when you need to find objects related to several business terms at once."
    },
    "semantic_types": {
      "default": [],
      "description": "Optional semantic object types to search. Empty list [] means ALL semantic object types are searched. Use this to narrow the search when you already know whether you want datasets, metrics, relationships, glossary terms, constraints, or models.",
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "semantic-model",
          "semantic-dataset",
          "semantic-metric",
          "semantic-relationship",
          "semantic-glossary",
          "semantic-constraint"
        ]
      }
    },
    "semantic_model_ids": {
      "default": [],
      "description": "Optional list of semantic model IDs to restrict the search to specific models. Empty list [] means search across all semantic models.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "case_sensitive": {
      "default": false,
      "description": "Whether regex matching should be case-sensitive. Leave false for normal discovery; set true only when exact casing matters.",
      "type": "boolean"
    },
    "max_results": {
      "default": 100,
      "description": "Maximum number of matched semantic objects to return. Use a smaller value for quick discovery and a larger value only when you need a broader result set.",
      "type": "integer",
      "minimum": -9007199254740991,
      "maximum": 9007199254740991
    }
  },
  "required": [
    "patterns"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="validate_semantic_query"></a>
## validate_semantic_query
**Annotations**: `read-only`

**Tags**: `semantic`

**Description**:

Performs best-effort semantic validation of an SQL query against one or more semantic models and compares it with
the expected semantic objects provided.

RETURNS:
- `validation_auto_detected`: semantic validation built from objects heuristically detected in the SQL
- `validation_detected_from_expected`: semantic validation built only from explicitly provided expected object IDs
- expected semantic objects that were matched or missing in the auto-detected result
- unexpected auto-detected objects outside the expected semantic scope

LIMITATIONS:
- Detection is heuristic and based on string matching over SQL and semantic metadata.
- The tool does not parse SQL semantically and does not execute the query.
- Auto-detected objects, missing objects, and relationship matches may therefore be imperfect.
- Use the result as a best-effort semantic check, not as a formal proof that the query is correct.

CONSIDERATIONS:
-  Prefer calling this tool before executing any SQL that touches semantic objects.
- This tool confirms the SQL dialect, surfaces semantic constraint violations, and provides post-execution checks.
- Only proceed to query_data once this tool returns valid=True and violations is empty. If violations are found,
fix the query first or consider the limitations of this tool.

WHEN TO USE:
- Before generating or approving a query that should follow a semantic model.
- When you want to validate a SQL query against the semantic objects before executing it using "query_data" tool
or creating a new SQL transformation out of it, especially when investigating data quality issues.
- When you want to verify that a query uses the intended semantic objects.
- When you need to surface semantic business-rule violations or follow-up checks.

EXAMPLES:
- Validate a SQL query against one semantic model:
  `sql_query="SELECT SUM(\"REVENUE\") FROM ...", semantic_model_ids=["semantic-model-uuid"],`
  `expected_semantic_objects=[{"object_type": "semantic-dataset"}]`
- Validate a cross-model query against two semantic models:
  `sql_query="SELECT * FROM ...", semantic_model_ids=["model-uuid-1", "model-uuid-2"],`
  `expected_semantic_objects=[{"object_type": "semantic-dataset", "ids": ["dataset-uuid-1"]}]`
- Validate a query and compare it against expected objects:
  `sql_query="SELECT SUM(\"REVENUE\") FROM ...", semantic_model_ids=["semantic-model-uuid"],`
  `expected_semantic_objects=[{"object_type": "semantic-metric", "ids": ["metric-uuid-1"]}]`


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "sql_query": {
      "type": "string",
      "description": "SQL query that should be checked against the semantic layer. The query is not executed; the tool performs best-effort semantic detection and rule validation using heuristic string matching, so the detected objects may be incomplete or imperfect."
    },
    "semantic_model_ids": {
      "type": "array",
      "items": {
        "type": "string"
      },
      "description": "One or more semantic model IDs against which the SQL should be validated. Contexts from all models are merged into a single universe for object detection. Constraint evaluation is performed per model to avoid cross-model rule contamination."
    },
    "expected_semantic_objects": {
      "default": [],
      "description": "Optional semantic object selections that define the expected semantic scope of the query. These expectations are compared with the objects actually detected in the SQL. Use `ids` when you want to assert that specific semantic objects should be present.",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "object_type": {
            "type": "string",
            "enum": [
              "semantic-model",
              "semantic-dataset",
              "semantic-metric",
              "semantic-relationship",
              "semantic-glossary",
              "semantic-constraint"
            ],
            "description": "Semantic object type to load."
          },
          "ids": {
            "default": [],
            "description": "Specific object UUIDs to include. Empty list [] means include all objects of this type.",
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        },
        "required": [
          "object_type"
        ]
      }
    }
  },
  "required": [
    "sql_query",
    "semantic_model_ids"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
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

    BEFORE QUERYING:
    * Always verify the table has a non-null fullyQualifiedName from get_tables tool.
      If it does not, the table is not SQL-accessible from this workspace — do not attempt the query and inform user.

    CRITICAL SQL REQUIREMENTS:

    * ALWAYS check the SQL dialect before constructing queries.
    * Do not include any comments in the SQL code
    * Use delimited identifiers and FQN format for the current SQL dialect.

    TABLE AND COLUMN REFERENCES:
    * Always use fully qualified table names in the exact FQN format provided by table information tools
    * Follow the identifier structure exactly as shown by table info tools for the current SQL dialect
    * Always use delimited identifiers when referring to table columns

    CTE (WITH CLAUSE) RULES:
    * ALL column references in main query MUST match exact case used in the CTE
    * If you alias a column in a CTE, reference it under the aliased name in the subsequent queries
    * Define all column aliases explicitly in CTEs
    * Use delimited identifiers in both CTE definition and references to preserve case

    FUNCTION COMPATIBILITY:
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
  "type": "object",
  "properties": {
    "sql_query": {
      "type": "string",
      "description": "SQL SELECT query to run."
    },
    "query_name": {
      "type": "string",
      "description": "A concise, human-readable name for this query based on its purpose and what data it retrieves. Use normal words with spaces (e.g., \"Customer Orders Last Month\", \"Top Selling Products\", \"User Activity Summary\")."
    }
  },
  "required": [
    "sql_query",
    "query_name"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---

# Storage Tools
<a name="get_buckets"></a>
## get_buckets
**Annotations**: `read-only`

**Tags**: `storage`

**Description**:

Lists buckets or retrieves full details of specific buckets, including descriptions,
lineage references (created/updated by), and links.

WHEN NOT TO USE:
- Do NOT call with `bucket_ids=[]` just to find a bucket by name. Use `search` with
  item_types=["bucket"] instead.
- Only use `bucket_ids=[]` when you need a complete inventory of all buckets in the project.

EXAMPLES:
- `bucket_ids=[]` → summaries of all buckets in the project
- `bucket_ids=["id1", ...]` → full details of the buckets with the specified IDs


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "bucket_ids": {
      "default": [],
      "description": "Filter by specific bucket IDs.",
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="get_tables"></a>
## get_tables
**Annotations**: `read-only`

**Tags**: `storage`

**Description**:

Lists tables in buckets or retrieves full details of specific tables, including fully qualified database name,
column definitions, lineage references (created/updated by) and links.

WHEN NOT TO USE:
- Do NOT list tables across buckets just to find a table by name. Use `search` with
  item_types=["table"] instead — it also matches column names and descriptions.
- Only use `bucket_ids` listing when you need all tables in specific known buckets.

RETURNS:
- With `bucket_ids`: Summaries of tables (ID, name, description, primary key).
- With `table_ids`: Full details including columns, data types, and fully qualified database names.
- With `table_ids` and `include_usage`: Full details plus components / transformations that use the tables
  in their input / output mappings. Use only when explicitly needed or evident from context; usage calculation
  might be demanding in big projects.

COLUMN DATA TYPES:
- database_native_type: The actual type in the storage backend (Snowflake, BigQuery, etc.)
  with precision, scale, and other implementation details
- keboola_base_type: Standardized type indicating the semantic data type. May not always be
  available. When present, it reveals the actual type of data stored in the column - for example,
  a column with database_native_type VARCHAR might have keboola_base_type INTEGER, indicating
  it stores integer values despite being stored as text in the backend.

QUERYABILITY RULE:
- A table is directly queryable via query_data tool only if fullyQualifiedName is present and non-null
  in the response.
- If fullyQualifiedName is absent or null (e.g. for linked/alias tables from other projects),
  the table cannot be queried via SQL from this workspace.
- Do not attempt to construct or guess the FQN — it will not work. In that case,
  inform the user of the limitation immediately.

EXAMPLES:
- `bucket_ids=["id1", ...]` → summary info of the tables in the buckets with the specified IDs
- `table_ids=["id1", ...]` → detailed info of the tables specified by their IDs
- `bucket_ids=[]` and `table_ids=[]` → empty list; you have to specify at least one filter


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "bucket_ids": {
      "default": [],
      "description": "Filter by specific bucket IDs.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "table_ids": {
      "default": [],
      "description": "Filter by specific table IDs.",
      "type": "array",
      "items": {
        "type": "string"
      }
    },
    "include_usage": {
      "default": false,
      "description": "Show components / transformations where each table is used.",
      "type": "boolean"
    }
  },
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
<a name="update_descriptions"></a>
## update_descriptions
**Annotations**: 

**Tags**: `storage`

**Description**:

Updates the description for Keboola storage items (buckets, tables, or columns).


**Input JSON Schema**:
```json
{
  "type": "object",
  "properties": {
    "updates": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "item_id": {
            "type": "string",
            "description": "Storage item: \"bucket_id\", \"bucket_id.table_id\", or \"bucket_id.table_id.column_name\"."
          },
          "description": {
            "type": "string",
            "description": "New description to set."
          }
        },
        "required": [
          "item_id",
          "description"
        ]
      },
      "description": "List of description updates to apply."
    }
  },
  "required": [
    "updates"
  ],
  "$schema": "http://json-schema.org/draft-07/schema#"
}
```

---
