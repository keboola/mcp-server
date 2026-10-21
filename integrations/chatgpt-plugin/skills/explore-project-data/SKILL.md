---
name: explore-project-data
description: Explore a Keboola project's data. Use when the user asks what data they have in Keboola, which buckets or tables exist, what a table contains, or wants a read-only SQL answer from their Keboola tables.
---

# Explore project data

Answer questions about the data in the user's Keboola project without changing anything.

## Input

A question about the data: which tables exist, what a table holds, or a question answerable with a `SELECT`.
If the user names no project, find it in step 1.

## Steps

1. **Pick the project.** Call `get_accessible_projects` with `with_llm_instruction=true` on the first call in the
   conversation. Show the projects with their organization and SQL dialect. If there is exactly one, say which one
   you will use; otherwise ask the user which project or projects to explore. Do not guess.
   Do this even when the session looks scoped already or a data tool would work without it: this plugin
   always narrows the session to the projects the user confirms. If `get_accessible_projects` fails because
   no programmatic token is present, the login is bound to one project: confirm it with the user and skip
   step 2.
2. **Scope the session.** Call `set_project_scope` with `project_ids=[...]` set to the user's choice and
   `read_only=true`. If the result carries a non-null `scope_token`, pass it on every later tool call. If the user
   later asks for a change, call `set_project_scope` again without `read_only` before any write.
3. **Get context.** Call `get_project_info` once for the project (pass `project_id=...` when 2+ projects are
   scoped). Note the SQL dialect: it decides identifier quoting in step 6.
4. **Find the data.** Use `search` with `item_types=["table"]` or `item_types=["bucket"]` and the names the user
   mentioned. Use `get_buckets` with `bucket_ids=[]` only when the user wants the full inventory. Never list every
   table just to find one by name.
5. **Describe tables.** Call `get_tables` with `table_ids=[...]` for the tables that matter, and with
   `project_ids=[<the table's project>]` when 2+ projects are scoped: without it the call runs in every scoped
   project. Report columns, types, description and row count. Check `fullyQualifiedName`: a table without one (for
   example a linked table from another project) cannot be queried. Say so and stop for that table.
6. **Query.** Call `query_data` with one `SELECT` (or `WITH ... SELECT`) statement, and with
   `project_ids=[<the table's project>]` when 2+ projects are scoped: a query runs in every scoped project
   otherwise. Copy table names from `fullyQualifiedName` and column names from `quotedName` exactly. Always add a
   `LIMIT`; compute counts and aggregates in SQL rather than pulling raw rows. Inspect distinct values before
   filtering on a categorical column.
7. **Answer.** Give the answer with the query you ran and the links the tools returned.

## Rules

- This skill never writes. Do not call create, update, modify, run or deploy tools here; hand over to the
  `build-sql-transformation` skill if the user wants to persist a result.
- `query_data` is for reading: send one `SELECT` per call, never `INSERT`, `UPDATE`, `DELETE`, `CREATE` or `DROP`.
- If the question is about Keboola itself rather than the user's data, use `docs_query`.
- If a tool fails because the login cannot reach a project, tell the user to reconnect the Keboola plugin or ask
  the project admin for access. Do not try another project on your own.
