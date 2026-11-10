# RFC: Python/JS data-app drafts on the /v2 drafts API

Linear: [AJDA-3760](https://linear.app/keboola/issue/AJDA-3760) (parent [AJDA-3754](https://linear.app/keboola/issue/AJDA-3754))

## Problem

A draft of a python-js data app is a separate Storage configuration (`parameters.dataApp.isDraft`) that clones the
prod app's repo at a pinned branch. The agent creates it with `modify_python_js_data_app(parent_configuration_id=…)`,
deploys it in dev mode, and deletes the configuration after publishing. The data-science service now has drafts of its
own (`/v2/apps/{appId}/drafts`, PAT-2093): a workload on its own host that serves a git branch live, with the prod
app's configuration. The MCP tools do not use them.

## Required Behavior

| Tool | Behaviour |
| --- | --- |
| `create_python_js_data_app_draft(configuration_id, git_branch)` (new) | Creates a draft of the prod app on an existing branch, waits up to 30 s in total for its URL, returns `draft_id`, `git_branch`, `url`, `run_state`. Never retries the POST (not idempotent); a failed URL read still returns the draft. |
| `get_data_apps` (prod detail) | `drafts`: the v2 drafts; `drafts_unavailable: true` when they could not be listed. `legacy_drafts`: the old draft configurations. `last_run` is the latest run of the prod app's `desired_deployment_id` (the `deploymentId` filter), so the drafts' runs are left out. |
| `get_data_app_preview_link(configuration_id, draft_id?)` | With `draft_id`: a 60 s link to that draft. Without: unchanged, for a legacy draft. |
| `delete_python_js_data_app_draft(configuration_id, draft_id?)` | With `draft_id`: deletes that draft of the prod app. Without: deletes a legacy draft configuration. |
| `modify_python_js_data_app` | No longer creates drafts: `parent_configuration_id` and `allow_main_branch` are removed. |
| `deploy_data_app` | Refuses `mode="dev"` and any deploy of a legacy draft; the error says how to convert the legacy draft to a draft on its branch. |

## Resolution Strategy

- `clients/data_science.py`: `DraftResponse` and `create_draft`, `list_drafts`, `get_draft`, `delete_draft`,
  `create_draft_preview_link` on `v2/apps/{appId}/drafts`.
- `tools/data_apps.py`: the new tool, the `draft_id` paths, the `drafts` / `legacy_drafts` fields, the legacy-draft
  deploy refusal. The draft-create path of `modify_python_js_data_app` and its helpers are removed.
- Docstrings keep the per-tool contract; the workflow (branch first, then the draft; converting a legacy draft) lives in
  the `dataapp-development` skill (keboola/ai-kit#167).
- No feature toggle: the only consumer of the removed parameters is Kai, whose skill changes in the same release.

## Scope

- In scope: managed-repo python-js apps; existing legacy drafts can still be listed, previewed and deleted.
- Out of scope: drafts of apps on a customer's GitHub repository (AJDA-3353), bulk migration of legacy drafts.

## Testing / Verification

- Unit tests: the draft client endpoints, create/wait/error mapping, a failed URL read, delete and preview by
  `draft_id`, the legacy-draft deploy refusal, `last_run` skipping draft runs.
- Integration test `test_python_js_data_app_prod_and_draft_lifecycle`: prod app → branch → draft → preview link →
  merge and deploy → delete.
- Canary-orion with Kai (2026-11-06): a new app built into a draft, previewed, published and cleaned up; an app with a
  legacy draft converted to a draft on the same branch.
