---
name: debug-failed-job
description: Find out why a Keboola job failed and propose a fix. Use when the user says a Keboola job, extractor, transformation, writer or flow failed or errored, or asks why their pipeline is red.
---

# Debug a failed job

Find the failed job, read its error and logs, explain the cause, and fix it only after the user agrees.

## Input

A job ID, a configuration or flow name, or just "my last job failed". Ask for nothing more until step 4 shows
whether it is needed.

## Steps

1. **Pick the project.** Call `get_accessible_projects` (with `with_llm_instruction=true` on the first call in the
   conversation) and ask the user which project the job ran in. Do not guess.
   Do this even when the session looks scoped already or a data tool would work without it: this plugin
   always narrows the session to the projects the user confirms. If `get_accessible_projects` fails because
   no programmatic token is present, the login is bound to one project: confirm it with the user and skip
   step 2.
2. **Scope the session to that project.** Call `set_project_scope` with `project_ids=[<the confirmed id>]`. This
   must happen before any write in step 7. If the result carries a non-null `scope_token`, pass it on every later
   tool call.
3. **Find the job.**
   - Known job ID: go to step 4.
   - Known configuration or flow name: find its ID with `search` (`item_types=["configuration", "transformation",
     "flow"]`), then call `get_jobs` with `job_ids=[]`, `status="error"`, `component_id=...` and `config_id=...`.
   - Nothing known: call `get_jobs` with `job_ids=[]`, `status="error"` and `limit=10`, and ask the user which job
     they mean if more than one is recent.
4. **Read the failure.** Call `get_jobs` with `job_ids=["<id>"]`, `include_logs=true` and
   `log_event_types=["error", "warn"]`. If the error event is not enough, call again with a larger
   `log_tail_lines=...` and no event type filter.
5. **Read what ran.** Call `get_configs` with `configs=[...]` for the job's configuration. For a flow job, call
   `get_flows` with `flow_ids=[...]` and find the failed task, then repeat steps 3 and 4 for that task's job.
6. **Explain.** Name the cause in one or two sentences and quote the error line. Typical causes: an SQL error in
   a transformation (check columns with `get_tables` and reproduce the failing `SELECT` with `query_data`), a
   missing input table, expired credentials of an extractor or writer, or a schema change in the output table.
   Use `docs_query` for component-specific errors. Propose a fix.
7. **Fix, only on request.** Ask before changing anything. For an SQL transformation use
   `update_sql_transformation`; for another component use `get_components` for its schema and then
   `update_config`. Expired OAuth credentials need the user: create a link with `create_oauth_url` and ask them to
   reauthorize. Then, with the user's consent, call `run_job` and check the new job with `get_jobs`. If the server
   refuses a write because the user's role is read-only, give the user the proposed fix to pass to a project admin.

## Rules

- No write before step 2, and no write at all without the user's go-ahead.
- Never rerun a job just to see whether it fails again; read the logs first.
- Report what the logs say. If they do not show a cause, say so and list what you checked.
