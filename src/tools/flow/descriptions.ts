// Tool descriptions for the flow tools, preserved verbatim from the Python docstrings.
// Extracted from tools.ts to keep the handler module focused.

export const CREATE_FLOW_DESCRIPTION = `Creates a new legacy (non-conditional) flow using \`keboola.orchestrator\`.

PRE-REQUISITES:
- Always use \`get_flow_schema\` with flow_type="keboola.orchestrator" and review \`get_flow_examples\` if unknown
- Collect component configuration IDs for every task you include

RULES:
- \`phases\` and \`tasks\` must follow the orchestrator schema; each entry must include \`id\` and \`name\`
- Phases run sequentially; tasks inside a phase run in parallel
- Use \`dependsOn\` on phases to sequence them; reference other phase ids
- Always share the returned links with the user

WHEN TO USE:
- Simple/linear orchestrations without branching or conditions
- ETL/ELT pipelines where phases just need ordering and parallel task groups`;

export const CREATE_CONDITIONAL_FLOW_DESCRIPTION = `Creates a new conditional flow configuration using \`keboola.flow\`.

PRE-REQUISITES:
- Always use \`get_flow_schema\` with flow_type="keboola.flow" and review \`get_flow_examples\` if unknown
- Gather component configuration IDs for all tasks you include

RULES:
- \`phases\` and \`tasks\` must follow the keboola.flow schema; each entry needs \`id\` and \`name\`
- Exactly one entry phase (no incoming transitions); all phases must be reachable
- Connect phases via \`next\` transitions; no cycles or dangling phases; empty \`next\` means flow end
- Task/phase failures already stop the flow; add retries/conditions only if the user requests them
- Always share the returned links with the user

WHEN TO USE:
- Flows needing branching, conditions, retries, or notifications
- Default choice when user simply says "create a flow," unless they explicitly want legacy orchestrator behavior`;

export const UPDATE_FLOW_DESCRIPTION = `Updates an existing flow configuration (either legacy \`keboola.orchestrator\` or conditional \`keboola.flow\`).

PRE-REQUISITES:
- Always use \`get_flow_schema\` (and \`get_flow_examples\`) for that flow type you want to update to follow the
required structure and see the examples if unknown
- Only pass \`phases\`/\`tasks\` when you want to replace them; omit to keep the existing ones unchanged

RULES (ALL FLOWS):
- \`flow_type\` must match the stored component id of the flow; do not switch flow types during update
- \`phases\` and \`tasks\` must follow the schema for the selected flow type; include at least \`id\` and \`name\`
- Tasks must reference existing component configurations; keep dependencies consistent
- Always provide a clear \`change_description\` and surface any links returned in the response to the user

CONDITIONAL FLOWS (\`keboola.flow\`):
- Maintain a single entry phase and ensure every phase is reachable; connect phases via \`next\` transitions
- No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

LEGACY FLOWS (\`keboola.orchestrator\`):
- Phases run sequentially; tasks inside a phase run in parallel; \`dependsOn\` references other phase ids
- Use \`continueOnFailure\` or best-effort patterns only when the user explicitly asks for them

WHEN TO USE:
- Renaming a flow, updating descriptions, adding/removing phases or tasks, adjusting dependencies,
or enabling/disabling flow execution`;

export const MODIFY_FLOW_DESCRIPTION = `Updates an existing flow configuration (either legacy \`keboola.orchestrator\` or conditional \`keboola.flow\`) or
manages schedules for this flow.

PRE-REQUISITES:
- Always use \`get_flow_schema\` (and \`get_flow_examples\`) for that flow type you want to update to follow the
required structure and see the examples if unknown
- Only pass \`phases\`/\`tasks\` when you want to replace them; omit to keep the existing ones unchanged

RULES (ALL FLOWS):
- \`flow_type\` must match the stored component id of the flow; do not switch flow types during update
- \`phases\` and \`tasks\` must follow the schema for the selected flow type; include at least \`id\` and \`name\`
- Tasks must reference existing component configurations; keep dependencies consistent
- Always provide a clear \`change_description\` and surface any links returned in the response to the user
- A flow can have multiple schedules for automation runs. Add/update/remove schedules only if requested.
- When updating a flow or a schedule, specify only the fields you want to update, others will be kept unchanged.

CONDITIONAL FLOWS (\`keboola.flow\`):
- Maintain a single entry phase and ensure every phase is reachable; connect phases via \`next\` transitions
- No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

LEGACY FLOWS (\`keboola.orchestrator\`):
- Phases run sequentially; tasks inside a phase run in parallel; \`dependsOn\` references other phase ids
- Use \`continueOnFailure\` or best-effort patterns only when the user explicitly asks for them

WHEN TO USE:
- Renaming a flow, updating descriptions, adding/removing phases or tasks, updating schedules,
adjusting dependencies, or enabling/disabling flow execution`;

export const GET_FLOWS_DESCRIPTION = `Lists flows or retrieves full details for specific flows.

WHEN NOT TO USE:
- Do NOT call with \`flow_ids=[]\` just to find a flow by name. Use \`search\` with
  item_types=["flow"] instead.
- Only use \`flow_ids=[]\` when you need a complete list of all flows in the project.

OPTIONS:
- \`flow_ids=[]\` → summaries of all flows in the project
- \`flow_ids=["id1", ...]\` → full details (including phases/tasks) for those flows`;

export const GET_FLOW_SCHEMA_DESCRIPTION = `Returns the JSON schema for the given flow type (markdown).

PRE-REQUISITES:
- Unknown schema for the target flow type: \`keboola.flow\` (conditional) or \`keboola.orchestrator\` (legacy)

RULES:
- Projects without conditional flows enabled cannot request \`keboola.flow\` schema
- Use the returned schema to shape \`phases\` and \`tasks\` for \`create_flow\` / \`create_conditional_flow\` /
\`update_flow\``;

export const GET_FLOW_EXAMPLES_DESCRIPTION = `Retrieves examples of valid flow configurations.

PRE-REQUISITES:
- Unknown examples for the target flow type: \`keboola.flow\` (conditional) or \`keboola.orchestrator\` (legacy) to help
build the specific flow configuration by mirroring the structure/fields.

RULES:
- Conditional-flow examples require conditional flows to be enabled; otherwise use legacy orchestrator examples
- Present the examples or cite unavailability to the user`;
