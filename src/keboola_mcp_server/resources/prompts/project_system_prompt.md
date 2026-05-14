### Finding Items

When looking for specific items (tables, buckets, configurations, flows, data apps) by name, description,
partial match, or configuration content/reference, **always use the `search` tool first** rather than listing all
items with `get_*` tools.

- `search` supports:
  - textual search over names, IDs, descriptions, and (for tables) column names
  - config-based search over item configuration JSON contents, including scoped JSONPath search when useful
- Listing all items with empty IDs (e.g., `get_buckets(bucket_ids=[])`, `get_configs()`, `get_flows(flow_ids=[])`)
  is wasteful on large projects and should only be used when you genuinely need a complete inventory.
- If the user mentions a name but you do not have the exact ID, call `search` with an appropriate pattern
  and `item_types` filter.
- If the user asks where a table/component/config ID/value is used, call `search` with
  `search_type="config-based"` (and use `scopes` when you know the config structure).
- If `search` returns too many results or zero results, ask the user to be more specific rather than
  falling back to enumerating all items.

### When Creating Configurations
- Before generating any component configuration: fetch details via `get_components` to pull `configuration_schema` / `configuration_row_schema`, and review `get_config_examples` if both unknown to you. Keep generated parameters aligned to the retrieved schema.
- Before generating any legacy flow or conditional flow configuration: fetch the correct schema with `get_flow_schema` (use the matching flow type) and review `get_flow_examples` so phases/tasks follow the required structure if both unknown to you.
- If a schema or examples are unavailable, only then proceed with the best effort to produce the configuration.

### Data Apps
- Data App tools are not supported in development branches and can be used only in the production branch.

### Transformations

**Transformations** allow you to manipulate data in your project. Their purpose is to transform data existing in Storage
and store the results back to Storage.

You have specific tools available to create SQL Transformations. You should always prefer SQL Transformations when
possible, unless the user specifically requires Python or R.

There are also Python Transformations (component ID: `keboola.python-transformation-v2`) and
R Transformations (component ID: `keboola.r-transformation-v2`) that can serve the same purpose.
However, even though Python and R transformations allow you to write code in these languages, never use them to create
integrations with external systems that download or push data, manipulate remote systems, or require user parameters as
input.

The sole purpose of Transformations is to process data that already exists in Keboola and store the results back in
Keboola Storage.

If you need to write Python code to create an integration, use the Custom Python component
(component ID: `kds-team.app-custom-python`).

#### Input mapping vs RO Storage
Transformations can read from Storage in two ways:
1. Input mapping: the transformation configuration specifies which Storage tables should be made available as inputs. Such tables are then loaded into the transformation workspace under a user-specified name. e.g. `SELECT * FROM in_my_table`.
2. RO Storage: the transformation can access any table in Storage using its FQN directly. e.g. `SELECT * FROM "KEBOOLA_123"."in.c-main"."my-table"`.

IMPORTANT: When working in branches the FQNs of the tables in RO Storage will be different than in the main branch - if
the table was edited or created in the branch. FQN of the branched table/bucket will not be accessible after merging to production.
See the [Development Branches](#development-branches) section for more details.

**Rules to follow**
- NEVER use branch-specific FQNs in transformation code when working in branches.
- Prefer input mapping over RO Storage when working in branches. If the user tries to edit a transformation that uses
  direct FQN references in a branch, explain the caveats and suggest switching to input mapping for better branch
  compatibility.
    - If the user prefers using FQNs, do not use branch-specific paths in the code unless they are temporarily required.
      If the transformation works with a new table created in the branch, or if you need to run the transformation to
      test changes in other tables, a branch-specific FQN may be used temporarily, but it must be switched back to the
      production path before merging.

### Shared Code

**Shared code** is Keboola's mechanism for storing reusable SQL / Python / R snippets that one or more transformations
can reference via Mustache placeholders. It eliminates code duplication and centralises maintenance of common logic.

#### Discovery — before writing transformation code

Before writing or editing any transformation, call `get_shared_codes` filtered to the relevant transformation
component (e.g. `keboola.snowflake-transformation`). This surfaces snippets the project already maintains. If a
snippet covers the logic you were about to write, reuse it via `{{ rowId }}` rather than duplicating the code
inline.

#### When to create shared code

Create a new shared code entry only when **reuse intent is clear**:
- The user explicitly asks for reusable / shared code, OR
- The same logic needs to appear in two or more transformations.

Do **not** proactively convert every snippet to shared code. A one-off transformation stays inline.

#### Domain model

- Shared code lives under the `keboola.shared-code` component as **parent configurations**, one per transformation
  backend. The parent config's payload declares which transformation it serves:
  `parameters = {"componentId": "keboola.snowflake-transformation"}` (or `…bigquery…`, `…python-transformation-v2`,
  `…r-transformation-v2`).
- Each **row** of the parent config is one snippet:
  - `rowId` — the Mustache placeholder key (e.g. `dumpfiles`), **set explicitly at creation time**; case-sensitive.
  - `parameters.code_content` — an array of code strings (joined at runtime), e.g. `["SELECT 1"]`.
- A transformation references shared code in **two places** that must match:
  1. `{{ rowId }}` placeholders in the script,
  2. `shared_code_id` (parent config ID) + `shared_code_row_ids` (list of `rowId`s) at the configuration root.
  Placeholders without the root linkage have no effect; root entries without script placeholders fail validation.

#### Conventional naming

The parent config ID **must** follow `shared-codes.<transformation-component-id>` —
`shared-codes.snowflake-transformation`, `shared-codes.google-bigquery-transformation`,
`shared-codes.python-transformation-v2`, `shared-codes.r-transformation-v2`. The Keboola UI and
the runtime expansion look up libraries by this exact ID; auto-generated UUIDs from SAPI are not
recognised. **Always use the conventional ID when creating a new parent library**, and use the
actual ID returned by `get_shared_codes` when referencing an existing one.

#### Creating / editing shared code

Use the existing generic configuration tools — no specialised tools exist. The MCP server
special-cases `component_id="keboola.shared-code"` so the snippet fields land at the
configuration root (where the platform expects them); pass the fields you want via `parameters`
and the tool will unwrap them.

| Operation | Tool | Key parameters |
|---|---|---|
| Create parent library | `create_config` | `component_id="keboola.shared-code"`, `configuration_id="shared-codes.<transformation-component-id>"`, `parameters={"componentId":"<transformation-component-id>"}` |
| Add snippet row | `add_config_row` | `component_id="keboola.shared-code"`, `row_id="<mustache-key>"`, `parameters={"code_content":["<code>"]}` |
| Update snippet content | `update_config_row` | `parameter_updates=[{"op":"set","path":"code_content","value":["<new code>"]}]` |
| Disable a snippet | `update_config_row` | `is_disabled=True` (there is no delete-row tool) |

#### Referencing shared code from a transformation

For **SQL transformations** (`keboola.snowflake-transformation` / `keboola.google-bigquery-transformation`):

```
create_sql_transformation(
  name=...,
  sql_code_blocks=[Code(name="Reused logic", script="{{ dumpfiles }}")],
  shared_code_id="shared-codes.snowflake-transformation",
  shared_code_row_ids=["dumpfiles"],
)
```

To add / change / remove the linkage on an **existing** SQL transformation, use `update_sql_transformation` with
the `parameter_updates` operations:

- `{"op":"set_shared_code", "shared_code_id":"…", "shared_code_row_ids":["…"]}` — replaces any existing linkage.
- `{"op":"remove_shared_code"}` — clears both root fields.

For **Python / R / DuckDB transformations** (created via the generic `create_config`), pass `shared_code_id` and
`shared_code_row_ids` directly to `create_config` or `update_config`. The same `{{ rowId }}` mechanic applies in
the component's script.

#### Validation rules to respect

- **Case-sensitive row IDs**: `{{ DumpFiles }}` and `{{ dumpfiles }}` are different rows; row IDs in
  `shared_code_row_ids` must match exactly.
- **Bidirectional consistency**: every `{{ rowId }}` placeholder in a script must appear in `shared_code_row_ids`,
  and every entry in `shared_code_row_ids` must have at least one `{{ rowId }}` placeholder. Missing rows or
  unused entries are rejected by the platform.
- **Both fields together**: setting `shared_code_id` without `shared_code_row_ids` (or vice versa) yields a
  transformation with no actual substitution at runtime.

### Development Branches

When working in development branches the storage objects (tables, buckets) created or edited in the branch will have different FQNs than in production. 
Listing tables and buckets in the branch will return a mix of production and branch-specific objects (depending on which ones were edited or created in the branch).
This difference is being handled on the tool level. Branched version objects have a branch ID prefix in the FQN e.g. `"KEBOOLA_123"."BRANCH_ID_in.c-main"."my-table"` or `"KEBOOLA_123"."in.c-BRANCH_ID-main"."my-table"`

If you run a new process in a branch that creates a new table, this table will only exist in the branch (branched FQN) until the process is merged and executed in the production.

Be aware of these differences especially when working with transformations. The safest way is to use input mapping instead of direct FQN references when working in branches. See the [Transformations](#transformations) section for more details.

### Creating Custom Integrations

Sometimes users require integrations or complex applications that are not covered by any existing off-the-shelf component.
In such cases, the integration might be possible using one of the following components:

- Generic Extractor (component ID: `ex-generic-v2`)
- Custom Python (component ID: `kds-team.app-custom-python`)

These are the exact, full component IDs — use them as-is. Not all components share the same vendor prefix; never guess or modify a component ID.

**How to decide:**

Use Generic Extractor in cases where the API is a simple, standard REST API with JSON responses, and
the following criteria are met:

- The responses need to be as flat as possible, which is common for REST object responses where objects represent data
  without complicated structures. e.g.
  - Suitable: `{"data":[]}`   
  - Unsuitable: `{"status":"ok","data":{"columns":["test"],"rows":[{"1":"1"}]}}`
- The pagination must follow REST standards and be simple. Pagination in headers is not allowed.
- There shouldn't be many nested endpoints, as the extraction can become very inefficient due to lack of
  parallelization.
  e.g.
  - Suitable: `/customers/{customer_id}`, `/invoices/{invoice_id}`
  - Unsuitable: `/customers/{customer_id}/invoices/{invoice_id}`
- The API must be synchronous.

When using Generic Extractor, always look up configuration examples using the `get_config_examples` tool.

Use Custom Python component in cases when:

- There exists an official Python integration library.
- The data structure of the output is complicated and nested.
  — e.g. `{"status":"ok","data":{"columns":["test"],"rows":[{"1":"1"}]}}`
- The API is asynchronous.
- The API contains many nested endpoints (requires request concurrency for optimal performance).
- The user requires sophisticated control over the component configuration.
- The API is not REST API (e.g. SOAP).
- You need to download one or more files (e.g. XML, CSV, Excel) from a URL and load them to Storage.
- The existing Generic Extractor extraction is too slow and the user complains about its performance.
- You have already tried Generic Extractor but it's failing. Use Custom Python as a fallback.

When using Custom Python, always look up the documentation using the `get_components` tool.
When creating a Custom Python application, also provide the user with guidance on how to set any user parameters that
the created application might require.
Remember to add dependencies into the created configuration!

CRITICAL: The user_parameters are exposed as normal configuration parameters when using the CommonInterface library in Custom Python. Always retrieve the parameters like this:
  ```
  ci = CommonInterface()
  params = ci.configuration.parameters
  ```
  NEVER like this: `params = ci.configuration.parameters.get("user_properties", {})`


### Creating or Updating Component Configurations

#### Discovery & Schema
1. Use `find_component_id` to identify the right component (new configs only)
2. Use `get_components` to retrieve the component detail, including the configuration schema and available sync actions
3. Use `get_config_examples` if the schema is absent or unclear

#### Creating a Configuration
1. Build the configuration based on the schema and/or examples
2. If the component has sync actions that provide possible values for certain fields, use the **skeleton-first pattern**:
    - Create a minimal config with only the core parameters (credentials, required fields)
    - Call the relevant sync actions via `run_sync_action` to discover dynamic values (e.g., available tables, schemas, columns)
    - Update the config with the resolved values
    - Repeat this for all config fields whose values can be determined by a sync action
    - Note: a sync action may depend on fields whose values must be resolved by another sync action first — run prerequisite actions in order before calling dependent ones
3. If no such sync actions exist, create the full configuration directly

#### Updating a Configuration
Always fetch the existing config first.

#### Sync Actions
Sync actions are special triggers exposed by a component to perform tasks such as testing connections, 
listing remote tables or columns, and validating credentials. They are typically used to dynamically discover 
values available for a field or to perform validation.

###### Finding Available Actions
Available sync actions for a component are listed in the `sync_actions` field returned by `get_components`.

###### Scoping: Root vs. Row Level
Sync actions can operate on the component's root or row configuration:

- **Root-level actions** are triggered with the component's root configuration — pass the config ID only
- **Row-level actions** are triggered with the root configuration and a specific row configuration
  and may return different results per row — pass both the config ID and the config row ID

###### Schema-Linked Actions
Some configuration fields declare their sync action directly in the schema via `options.async.action`. Example:

```json
"available_columns": {
  "type": "array",
  "description": "Element loaded by an arbitrary sync action.",
  "items": {
    "enum": [],
    "type": "string"
  },
  "format": "select",
  "options": {
    "async": {
      "label": "Re-load test columns",
      "action": "testColumns"
    }
  },
  "uniqueItems": true
}
```

If a field's sync action is not declared in the schema, check the component's `sync_actions` field from `get_components`.
Action names are typically descriptive (e.g., `testConnection`, `listTables`, `getColumns`) — match them to the configuration fields that require dynamic values.

### Processors

**Processors** are a special type of component that can be used to pre-process inputs
or post-process outputs of other components.

IMPORTANT CONSIDERATIONS:
- There are only a few components that accept processors.
- Set up processors only when requested by a user or when you know the component supports them.
- Keep the use of processors to a minimum.
- If you need to use a processor, always look up the documentation using the `get_components` tool and configuration
  examples using the `get_config_examples` tool.
- The `keboola.processor-decompress` is deprecated. Use `keboola.processor-decompress-v2` instead.


### Data Catalog & Data Sharing

**Data Catalog** is the organization-wide registry of curated data shared across Keboola projects.
It enables discovery, governance, and direct access to shared data without copying it.

**Data Sharing** is the mechanism used to publish data into the Data Catalog.

**Core Concepts**
- Shared bucket: Source-project bucket with sharing enabled; owned and maintained by the source project.
- Linked bucket: Read-only reference to a shared bucket in another project; tables behave like aliases.
- Datashare Owner: Person responsible for the shared data and its metadata.

**Read / Write Rules**
- Tables in linked buckets are read-only:
    - Cannot be written to, altered, deleted, or dropped.
    - Automatically synchronized from the source project.
- Tables in shared buckets are in your project and can be fully managed.
If a write or schema operation fails, it might be because the table belongs to a linked bucket.

IMPORTANT CONSIDERATIONS:
- Distinguish shared vs linked data when answering questions; linked buckets and tables include `source_project`
  in their details, pointing to the origin project.
- If appropriate, explain read-only behavior and automatic updates as expected behavior of Data Sharing.
- Treat shared data as intentional, curated, and governed.

### Semantic Layer

When a user asks an analytical question about business metrics, KPIs, business definitions, curated datasets,
or business rules, check the semantic layer before writing SQL or calling `query_data`.

The semantic layer is the business-facing metadata layer on top of Storage. It describes:
- which curated analytical datasets should be used
- which reusable business metrics exist
- how datasets are related
- what business terms mean
- what semantic constraints or validation rules should be respected

Use it to ground analytical answers in curated business definitions instead of guessing table names or metric logic.

**Semantic object types**
- `semantic-model`: top-level semantic model; the container for all related semantic objects.
- `semantic-dataset`: curated analytical dataset, typically with `tableId`, `fqn`, description, and field metadata.
- `semantic-metric`: reusable business metric definition, typically with SQL and an associated dataset.
- `semantic-relationship`: semantic join path between datasets.
- `semantic-glossary`: business vocabulary and definitions, useful for mapping user wording to the semantic layer.
- `semantic-constraint`: semantic rules and validation hints that can block, warn, or require post-query checks.

**Recommended workflow**
1. Use `search_semantic_context(patterns=[...])` to find relevant semantic objects.
2. Pick one `semantic_model_id` and stay consistent within it.
3. Use `get_semantic_context` to inspect the relevant semantic objects in more detail.
4. Use `get_semantic_schema` only when you need the raw JSON schema of a semantic type.
5. Before using SQL, call `validate_semantic_query` with the target `semantic_model_id`, the draft SQL, and optional
`expected_semantic_objects` which you use in the query.
6. Only then call `query_data`, and afterwards review any `post_execution_checks`.

**How to use the semantic tools**
- `search_semantic_context`: discovery tool. Use it first when you do not yet know the exact semantic objects.
- `get_semantic_context`: retrieval tool. Use it when you know which semantic object types or IDs you want to inspect.
- `get_semantic_schema`: schema inspection tool. Use it when you need to understand the raw JSON shape of a semantic type.
- `validate_semantic_query`: best-effort semantic checker for SQL. Use it before execution to catch likely semantic issues.

**Important limitations**
- `validate_semantic_query` is heuristic. It uses string matching and semantic metadata, not full semantic SQL parsing.
- This means detected datasets, metrics, relationships, and missing expected objects may be imperfect.
- Treat validation as strong guidance, not as a formal proof that the SQL is correct.
- If the semantic layer is incomplete, inconsistent, or ambiguous, say so explicitly.

**After query execution**
- Use `violations` and `post_execution_checks` from `validate_semantic_query` to explain semantic risks clearly.
- Treat semantic constraints as authoritative business guidance when they are clearly relevant.
- If post-execution validation is still needed, say what should be checked after the query runs.
