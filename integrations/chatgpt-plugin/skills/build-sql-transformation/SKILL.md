---
name: build-sql-transformation
description: Create or change a Snowflake or BigQuery SQL transformation in Keboola. Use when the user wants to turn a query into a saved transformation, build a new table from existing Keboola tables, or edit the SQL of an existing transformation.
---

# Build a SQL transformation

Create a new SQL transformation, or update an existing one, in the one project the user confirms.

## Input

What the transformation should produce, from which tables, and whether to run it once it is saved. For an
existing transformation, its name or ID.

## Steps

1. **Pick the project.** Call `get_accessible_projects` (with `with_llm_instruction=true` on the first call in the
   conversation). Show the projects and ask the user which one the transformation belongs in. Never pick a project
   on your own, even when only one is listed: confirm it.
2. **Scope the session to that project.** Call `set_project_scope` with `project_ids=[<the confirmed id>]`. This
   must happen before any write. If the result carries a non-null `scope_token`, pass it on every later tool call.
3. **Get context.** Call `get_project_info`. Note the SQL dialect (Snowflake or BigQuery). Python, R and DuckDB
   transformations are out of scope for this skill; say so and stop.
4. **Find the inputs.** Use `search` with `item_types=["table"]` for the source tables, then `get_tables` with
   `table_ids=[...]` for their `fullyQualifiedName` and column `quotedName` values.
5. **Prototype the SQL.** Run the `SELECT` part through `query_data` with a small `LIMIT` and show the user sample
   rows. Fix errors here, before anything is saved.
6. **Save it.**
   - New transformation: call `create_sql_transformation`. Reference input tables by fully qualified name; create
     output tables with plain delimited names (`CREATE TABLE "daily_orders" AS ...`) and list them in
     `created_table_names=[...]`. Give it a name and description that state its purpose.
   - Existing transformation: find it with `search` and `item_types=["transformation"]`, read it with
     `get_configs` and `configs=[...]` to get the `block_id` and `code_id` values, then call
     `update_sql_transformation` with targeted operations and a `change_description=...`. Storage mappings are
     replaced as a whole: include every mapping you want to keep.
7. **Run it, if the user asked.** Ask before running. Call `run_job` with the transformation's `component_id=...`
   and `configuration_id=...`, then poll `get_jobs` with `job_ids=[...]` until the job ends. On failure, continue
   with the `debug-failed-job` skill.
8. **Report.** Give the transformation name, the output tables, the job result and the links the tools returned.

## Rules

- No write before step 2. If the user switches projects mid-conversation, go back to step 1.
- Confirm with the user before overwriting SQL in an existing transformation and before every `run_job`.
- Use the dialect's delimited identifiers everywhere and never mix quoting styles in one statement.
- A destructive schema change (dropping, renaming or retyping a column) needs the output table removed before the
  next run. Tell the user; do not try to drop it through `query_data`, which accepts only read-only SQL.
