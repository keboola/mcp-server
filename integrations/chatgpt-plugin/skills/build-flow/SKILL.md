---
name: build-flow
description: Create or change a Keboola flow that runs extractors, transformations and writers in order, optionally on a schedule. Use when the user wants to orchestrate, chain or schedule existing Keboola configurations.
---

# Build a flow

Create a flow from existing configurations, or change an existing flow, in the one project the user confirms.

## Input

Which configurations to run, in what order, and whether the flow needs a schedule. For an existing flow, its name
or ID.

## Steps

1. **Pick the project.** Call `get_accessible_projects` (with `with_llm_instruction=true` on the first call in the
   conversation) and ask the user which project the flow belongs in. Never pick a project on your own: confirm it.
   Do this even when the session looks scoped already or a data tool would work without it: this plugin
   always narrows the session to the projects the user confirms. If `get_accessible_projects` fails because
   no programmatic token is present, the login is bound to one project: confirm it with the user and skip
   step 2.
2. **Scope the session to that project.** Call `set_project_scope` with `project_ids=[<the confirmed id>]`. This
   must happen before any write. If the result carries a non-null `scope_token`, pass it on every later tool call.
   Every write below also passes the confirmed id as `project_id`: other conversations on the same login share the
   scope, and a scope they moved makes the write fail instead of landing in another project.
3. **Get context.** Call `get_project_info` with `project_id=<the confirmed id>`. It says whether conditional
   flows are enabled in the project, and its `toolset_restrictions` say whether the user's role may set schedules.
   If they say only read-only tools are available, tell the user they cannot create flows in this project and stop.
4. **Collect the tasks.** Find every configuration the flow should run with `search`
   (`item_types=["configuration", "transformation"]`) and confirm the list with the user. Each task needs a
   component ID and a configuration ID; use `get_configs` with `configs=[...]` when a search hit does not show
   them. Do not create the configurations here; a missing SQL transformation is built with the
   `build-sql-transformation` skill first. A data app is the exception: find it with `get_data_apps` and add it
   as a `keboola.data-app-control` task with `configData: {"parameters": {"appId": "<data_app_id>"}}` and no
   configuration ID.
5. **Load the schema.** Call `get_flow_schema` with `flow_type="keboola.flow"` when conditional flows are enabled,
   otherwise `flow_type="keboola.orchestrator"`. Call `get_flow_examples` with the same type if you have not seen
   one in this conversation.
6. **Show the plan.** List the phases, the tasks in each phase and their order. Tasks in one phase run in
   parallel; phases run in order. Wait for the user to agree.
7. **Create or update.**
   - New flow: call `create_conditional_flow` with `project_id=<the confirmed id>` for `keboola.flow`, or
     `create_flow` with `project_id=<the confirmed id>` for `keboola.orchestrator`. Give every phase and task an
     `id` and a `name`.
   - Existing flow: find it with `search` and `item_types=["flow"]`, read it with `get_flows` and
     `flow_ids=[...]`, then call `modify_flow` with `project_id=<the confirmed id>`, its
     `configuration_id=...`, the `flow_type=...` it already has, a `change_description=...` and only the fields
     that change.
8. **Schedule, if asked.** Call `modify_flow` with `project_id=<the confirmed id>` and `schedules=[...]` for the
   schedule the user asked for. Confirm the cron expression and time zone with the user first. If
   `toolset_restrictions` from step 3 say the role cannot set schedules, tell the user and skip this step.
9. **Run, if asked.** Ask before running. Call `run_job` with `project_id=<the confirmed id>`, the flow's
   `component_id=...` and `configuration_id=...`, then follow it with `get_jobs`. On failure, continue with the
   `debug-failed-job` skill.
10. **Report.** Give the flow name, its phases, the schedule and the links the tools returned.

## Rules

- No write before step 2. If the user switches projects mid-conversation, go back to step 1.
- Add retries, conditions or `continueOnFailure` only when the user asks for them; a failed task already stops
  the flow.
- Do not change the type of an existing flow.
