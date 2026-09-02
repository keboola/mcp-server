# RFC: Kai Prompt & Instruction Governance

Linear: _to be filed_ (follows the 2026-07-09 cross-repo prompt audit; related: [AI-3531](https://linear.app/keboola/issue/AI-3531), [AI-3532](https://linear.app/keboola/issue/AI-3532))

## Problem

Kai's behavior is shaped by instructions authored across **three repos with no contract between them**, and the same guidance is duplicated across them where it then drifts. A 2026-07-09 audit of every prompt surface found **19 verified conflicts** (5 high / 8 medium / 6 low).

The three layers that compose Kai's live instructions:

| Layer | Where | What it contributes |
|---|---|---|
| Runtime + persona | `keboola/ui` `apps/kai-agent/src/services/prompt-builder.ts` | Kai identity, the React/Vite sandbox rules, a mandatory data-app skill gate, credential flow, per-request context |
| Capability + tools | `keboola/mcp-server` `resources/prompts/project_system_prompt.md` + ~39 tool docstrings (e.g. `tools/data_apps.py`) | The project system prompt and the agent-facing tool descriptions (docstrings **are** the descriptions) |
| Domain how-to | `keboola/ai-kit` skills (esp. `plugins/dataapp-developer/skills/dataapp-development`) | Reusable "how to build X" knowledge; the `dataapp-development` skill is **build-time vendored** into the Kai sandbox |

**Root cause.** ai-kit skills are authored for standalone Claude Code / kbagent CLI users, but Kai vendors the `dataapp-development` skill into a *different* runtime (MCP tools + a React scaffold, **no kbagent**). Standalone defaults get stated as universal truths, and Kai's own layer then contradicts them. Representative conflicts:

- **Stale capability fact (high):** the vendored skill (`deployment-paths.md`, `python-js-apps.md`) says Python/JS apps cannot deploy via MCP → use kbagent/customer-git, but `mcp-server` ships `modify_python_js_data_app` + `deploy_data_app` and Kai has no kbagent. Kai is told its only build path is impossible.
- **Contradicted default (high):** ai-kit makes CDN/vanilla + Chart.js the dashboard default (`python-js-apps.md`); Kai's co-loaded `dataapp-building-kai-extras` skill bans CDN/vanilla and mandates React/Vite.
- **Dangling references:** `keboola-git` routes to skills `dataapp-developer:dataapp-dev` / `:dataapp-deployment` that do not exist (only `dataapp-development` does); `kai-extras` points at `dataapp-development` for a draft/promote model that actually lives in `mcp-server` `data_apps.py` docstrings; `deploy_data_app` usage examples omit the required `action` argument.
- **Silent duplication:** an identical jargon ban is maintained in both `prompt-builder.ts` and `kai-extras/SKILL.md`.

Three structural gaps let these persist: (1) no single review ever sees the *composed* prompt Kai actually receives; (2) references across surfaces are unchecked, so they rot silently; (3) there is no automated signal that a prompt edit changed Kai's behavior.

## Required Behavior

The target end-state, treating prompts as a versioned, tested interface with owners:

1. **Single ownership.** Every category of instruction has exactly one owning surface; the others must not restate it — they link to or derive from the owner.
2. **Runtime-scoped content.** ai-kit skills declare which runtimes they target; content that differs by runtime is expressed as an explicit overlay, so the vendored-into-Kai copy *overrides* rather than contradicts the base.
3. **The composed prompt is reviewable.** The fully-assembled instruction text Kai receives is emitted as a committed artifact and diffed in CI, so a reviewer sees exactly what changed.
4. **References resolve.** Every skill slug, file path, and tool name (and example tool-call arguments) referenced in any prompt is validated in CI; an unresolved reference fails the build.
5. **Behavior is gated by evals.** Prompt-consistency behavior is covered by KaiBench eval cases that gate prompt-carrying PRs, so an edit that breaks instruction-following turns a check red.

## Resolution Strategy

**This is staged, not a build-everything plan.** The 80/20 is §1 (the specification of what goes where in each layer) plus a one-time reconciliation of the audit's 19 findings to that spec — that is cheap and it is most of the value, so it ships now. §3, §4, and §6 are prevention *infrastructure*; build them only if drift recurs after the spec+fix lands, cheapest-first, and stop as soon as it is enough. Do the cheap high-value work first and measure before building guards. The full sequencing is in [Sequencing](#sequencing) below.

### 1. Ownership contract

Assign each instruction category one owner and forbid the others from restating it:

| Category | Owner | Rule for the other surfaces |
|---|---|---|
| Capability facts (what tools exist, what can deploy where, tool signatures) | `mcp-server` (tool docstrings, generated `TOOLS.md`) | Never hard-code capability claims; reference/derive from the tool set |
| Domain how-to (building data apps, storage patterns, SQL patterns) | `ai-kit` skills | Runtime layers link to the skill, not re-explain it |
| Runtime + persona (Kai identity, sandbox, skill gate, credential flow) | `keboola/ui` `prompt-builder.ts` + `kai-extras` | ai-kit stays runtime-agnostic; Kai-specific rules live here |

Add `CODEOWNERS` entries per prompt surface so edits get the right review. Where layers legitimately overlap, the more specific (runtime) overlay wins **and says so explicitly**. Most of the 19 conflicts are one layer asserting something another layer owns; the contract dictates which side each fix lands on. This contract can be *distributed* rather than merely enforced by convention — see §6.

### 2. Runtime-scoped overlays (minimal, last resort)

Some content genuinely differs by runtime and cannot be single-sourced — Streamlit-allowed-vs-not, CDN-vs-React, cache-vs-live-query. For only these true forks, ai-kit skills carry an explicit `In Kai:` / `In standalone CLI:` callout (or frontmatter `targets:`), and the vendoring step (`packages/kai-agent-sandbox/scripts/vendor-external-skills.ts`) selects the right variant. This is deliberately scoped small: broad per-runtime tagging across every skill is manual discipline that rots — a form of the very problem this RFC addresses. The scalable backbone is distribution (§6) plus the composed-prompt artifact (§3) and evals (§5) catching divergence automatically; overlays cover only the residue those cannot.

### 3. Composed-prompt build artifact

Today the live prompt is assembled at runtime from four sources — `prompt-builder.ts` (base identity + skill gate + platform `llm_instruction` + per-request context), the vendored ai-kit skill markdown, the first-party `kai-extras` skill, and the MCP tool docstrings injected by the client — and **no human or test ever sees the whole assembled string**. Each fragment is reviewed in isolation, in a different repo, which is exactly how the high-severity contradictions slipped through.

Proposal: a generator + CI check that assembles and emits the composed prompt as a committed artifact, mirroring the pattern `mcp-server` already uses for tool docs (`python -m keboola_mcp_server.generate_tool_docs` → `TOOLS.md`, gated by `tox -e check-tools-docs`, which runs the generator then `git diff --exit-code TOOLS.md` so CI fails if it is stale).

How it works, concretely:

1. A generator script calls the **same** `buildSystemPrompt` the runtime uses (not a reimplementation) with checked-in fixture inputs — a canned platform `llm_instruction`, a sample project context, both skills loaded from the vendored dir.
2. It writes the assembled prompt to a committed file (`apps/kai-agent/COMPOSED_PROMPT.md`), one snapshot per distinct state (default, data-app-gated).
3. CI reruns the generator and `git diff --exit-code`s the snapshot. If the committed file no longer matches what today's code + skills + injected tool-docs produce, CI fails and the author commits the regenerated file — surfacing the change as a reviewable diff on the PR that caused it.

This is byte-for-byte the mechanism this repo already runs for `TOOLS.md` (`tox -e check-tools-docs`), pointed at the prompt instead — not new infrastructure.

- A generator runs `prompt-builder`'s assembly with representative fixtures (a canned platform `llm_instruction`, a sample project context, both skills loaded) and writes e.g. `apps/kai-agent/COMPOSED_PROMPT.md` — ideally one snapshot per distinct state (default, data-app-gated).
- CI runs it and `git diff --exit-code`: any change to the composed prompt — including one flowing in from a bumped vendored skill or a changed tool docstring — appears as a **reviewable diff on the PR that causes it**, so cross-repo drift becomes visible at the moment it lands.
- Reviewers see the actual text Kai gets, not fragments; the diff also surfaces unintended prompt-size / token-budget blowups.

Determinism boundary: the MCP tool docstrings are injected at runtime from the connected server, so the snapshot must be taken against a **pinned mcp-server version** (or embed the generated tool descriptions from `TOOLS.md`) to be reproducible. This shares a source with §5(d).

### 4. Cross-reference linter

Two whole conflict classes from the audit are pure dangling references: `keboola-git`'s non-existent skill slugs, `kai-extras`' wrong-file pointer, and `deploy_data_app` examples missing the required `action`. A linter parses every prompt / skill / docstring and validates that referenced identifiers resolve:

- **Skill references** — `plugin:skill` slugs, `references/*.md` paths, and `Skill(...)` invocations resolve to an actual skill/file.
- **File paths** referenced in prose exist.
- **Tool names** mentioned resolve to a tool in the registry (the `mcp-server` tool set plus the sandbox `ui-mcp-server` / `local-mcp-server` tools), and any example tool call's arguments type-check against that tool's schema (catches the missing `action`).
- **Cross-repo edges** — because ai-kit is vendored, references in the vendored copy are resolved against the **target runtime's** actual skills/tools, not ai-kit's own.

Enforcement is split to match the ownership contract: `mcp-server` validates tool-name / argument references in its docstrings and prompts (Python check, wired into `tox`); `keboola/ui` validates skill / file / tool references in `prompt-builder.ts` and the sandbox skills against the vendored skill set and a pinned tool list (TS check in CI). Both consume a shared manifest — known skill slugs + the generated tool list from `TOOLS.md` — so nothing is hard-coded. An unresolved reference exits non-zero and fails the build.

### 5. Prompt-consistency evals in KaiBench

KaiBench (`keboola/KaiBench`) already drives the **real** Kai backend over SSE and scores multi-turn tool use (numeric matching, LLM-judge, trace verification). Crucially it **already performs prompt-consistency-style assertions today**, driven from a case's `evaluator_config`: `expected_tools`, `forbidden_tools` + `forbidden_tool_penalty`, `max_post_run_status_calls` + `polling_penalty`, `max_tool_calls` (see `evaluators/component_creation.py::_apply_behavioral_penalties`). Real existing cases are effectively instruction-following tests: **CC-14** ("update the Python config, must not call `create/update_sql_transformation`"), **CC-16/CC-17** (same forbidden-SQL-tool pattern), **CC-15** (`max_post_run_status_calls: 1` — don't over-poll), **CC-13 (BQ)** + the MCP-08 suite (SQL dialect). It also already has `--repeat N` consistency metrics (`metrics/reliability.py`), regression detection (`metrics/regression.py`, score-delta + sign test), and a CI workflow (`.github/workflows/evaluate.yml`) that builds the MCP server from an input git ref and **posts a commit status back to a caller repo** to gate PRs.

So the strategy is to promote prompt-consistency to a **first-class track on the framework that exists**, not to build new tooling.

**(a) Encode each verified audit conflict as a behavioral eval case.** Each target-state instruction becomes a case run with `--repeat N`, gated on a minimum pass-rate (instruction-following is probabilistic):

| Instruction (target state) | Assertion in KaiBench |
|---|---|
| Python/JS deploys via MCP, not kbagent | `expected_tools: [modify_python_js_data_app, deploy_data_app]`; forbid any kbagent/git path |
| No Streamlit unless the user asks | `forbidden_tools` for the Streamlit deploy path + a response-text assertion; expect the React path |
| Correct SQL dialect / endpoint | argument assertion on identifier quoting (extends CC-13 / MCP-08) |
| Don't over-poll build status | `max_post_run_status_calls` (already supported) |

**(b) Close four framework gaps (the enabling work), in priority order:**

1. **Lift `_apply_behavioral_penalties` out of `ComponentCreationEvaluator`** into `BaseEvaluator` (or a `BehavioralMixin`), or add a dedicated `question_type: "Prompt Consistency"` + `InstructionFollowingEvaluator`, so *any* case can assert forbidden/expected tools without hijacking component-creation scoring. Highest leverage.
2. **Response-text assertions** — promptfoo-style `contains` / `not-contains` / `regex` over `kai_response` (for "never *mentions* Streamlit", capability claims, tone rules). None exist today.
3. **Tool-argument assertions** — a predicate / JSONPath / regex over `ToolCall.arguments` (the trace captures arguments but only `component_id` is inspected today) — for SQL dialect identifiers, correct component targeting.
4. **Wire the already-loaded-but-unused `evaluation_criteria` field** (`question_loader.py` → `EvaluationContext`, currently read by no evaluator) into the LLM judge for per-case rubric grading of fuzzy instructions.

Plus a **fixture-seeding helper**: several behavioral cases assume pre-seeded objects (CC-14 needs an existing transformation); cleanup exists, seeding does not.

**(c) Gate prompt-carrying PRs.** Extend `evaluate.yml`'s existing "build from ref + post commit status" mechanism to run the prompt-consistency track (a subset, with `--repeat`) on PRs that touch `prompt-builder.ts`, `kai-extras`, or bump the vendored ai-kit skill. Regression detection is score-delta vs a pinned baseline, so a behavioral drift from a prompt edit surfaces automatically as a red status.

**(d) True prompt-regression needs one dependency.** KaiBench cannot currently see or pin the *composed system prompt* — `KaiSession` sends only user text; the composed prompt lives on the backend and is not in `ConversationTrace`. So today prompt regression is observable only *indirectly*, via behavior + image/ref pinning. To assert on or diff the prompt text itself, the Kai backend must expose the composed prompt (a debug echo endpoint), after which KaiBench adds a trace field + snapshot assertion. This is the **same artifact as §3**, so the generator and this share a source. It is a cross-team dependency on the kai-assistant / kai-agent team, not something KaiBench can add alone.

### 6. Distribute the canonical guidance as a plugin companion

An alternative — and likely stronger — backbone for §1–§2: rather than enforcing single-ownership by convention and syncing via build-time vendoring, package the canonical guidance as a **plugin companion to the MCP server** and distribute it as one versioned artifact. This is the pattern Slack, Linear, and others use — the connector ships its agent-facing skills/prompts alongside it, and every client consumes one published bundle instead of hand-authoring or vendoring its own copy. It removes the drift class at the source.

Ownership still applies, but as *packaging* boundaries:

- Capability/tool guidance ships from **mcp-server** (this repo), physically next to the tools it describes so it cannot drift from the code — generated where possible (the `TOOLS.md` generator is the seed of this).
- Domain how-to skills stay in **ai-kit**, which is already a Claude plugin marketplace (`.claude-plugin/marketplace.json`).
- The published "Keboola" plugin is a thin bundle referencing both plus the MCP server connection config.

Caveat for Kai: Kai does not install plugins the way Claude Code does — it vendors skills and connects the MCP server directly — so adopting this means Kai consumes the *published* bundle instead of a raw ai-kit subpath. That is still a net improvement (single, versioned distribution), but Kai continues to need a small runtime overlay (§2) for genuinely runtime-specific content (React sandbox, no kbagent). The plugin shrinks the overlay surface; it does not fully remove it. Where the plugin bundle itself lives (mcp-server vs ai-kit's marketplace) is an open question — see below.

## Sequencing

**Ship now — the 80/20.**

1. Adopt §1's layer-ownership spec (what each of the three surfaces owns, and that the others link/derive rather than restate) + `CODEOWNERS` per surface.
2. Reconcile the audit's 19 conflicts to that spec in a single interactive pass — most are one-line moves of a claim to its owning layer. The first fix (the stale "Python/JS can't deploy via MCP" claim) is already in flight in ai-kit; the rest is the same exercise.

That is the bulk of the value and it needs no new infrastructure.

**Then measure.** Run Kai for a while and watch whether the conflicts recur. If the spec + one-time fix holds, stop here.

**Only if drift recurs — add guards cheapest-first, and stop when it's enough:**

1. §4 cross-reference linter — cheapest, and it catches a class a spec cannot (a dangling skill/tool/file reference that no one typed correctly). This is the first thing to reach for.
2. §3 composed-prompt build artifact — a generator + `git diff` gate; more setup than the linter.
3. §6 plugin-companion distribution — a structural change with the highest ceiling (removes the drift class at the source), but the largest investment; worth it only if per-layer drift keeps recurring.
4. §5 KaiBench prompt-consistency track — highest value for *behavioral* regressions specifically; adopt when prompt edits start silently changing behavior.

The guards are real infrastructure. Build them against evidence, not speculatively.

## Scope

**In scope:** the governance model (ownership contract + `CODEOWNERS`, runtime overlays). The immediate deliverable is §1's layer-ownership spec plus the one-time reconciliation of the audit's 19 conflicts to it; the two CI mechanisms (composed-prompt artifact, cross-reference linter) and the KaiBench prompt-consistency track plus its enabling framework changes are sequenced — deferred until drift recurs — per [Sequencing](#sequencing).

**Out of scope:** consolidating the three repos into one — explicitly rejected; ai-kit serves standalone/kbagent users, `mcp-server` is a product with its own consumers, and the Kai app is a third runtime, so merging would move the coupling inward rather than remove it. The fix is boundaries + automation. Also out of scope: rewriting the KaiBench judge, and implementing the kai-assistant composed-prompt debug endpoint (a dependency, tracked separately). Fixing the 19 individual conflicts is tracked by the audit; this RFC governs *which side* each fix lands on and how recurrence is prevented.

## Testing / Verification

- The composed-prompt generator and the cross-reference linter are self-verifying CI checks: `git diff --exit-code` on the artifact and a non-zero exit on any unresolved reference, mirroring `tox -e check-tools-docs`.
- **Acceptance test for the linter:** run it against current `HEAD` and confirm it flags the audit's known dead references (`keboola-git`'s `dataapp-developer:dataapp-dev` / `:dataapp-deployment`, `kai-extras`' wrong-file pointer, `deploy_data_app`'s missing `action`). If it does not catch these, it is incomplete.
- The KaiBench track verifies behavior; the existing `success_criteria` gate applies (min pass rate 0.7, min avg score 0.6, max regressions 0, min edge-case pass rate 0.5), run with `--repeat` for the consistency dimension.
- Manual: regenerate the composed prompt and confirm both skills + the tool docs appear in one artifact; add one behavioral case per high-severity conflict and confirm it fails on current `main` and passes after the reconciling fix.

## Open Questions

- Where does the composed-prompt generator live, given the prompt spans repos plus runtime-injected tool docs — in `keboola/ui` with a pinned `mcp-server` tool snapshot, or a small shared package?
- Do we eventually want a shared "prompt core" package the three repos consume, or is contract + vendoring + CI enough? Recommendation: start with the latter; revisit only if drift persists despite the checks.
- Who owns the shared cross-repo linter rule set, and who runs the KaiBench prompt-consistency track on **ai-kit** PRs (ai-kit has no Kai backend in its CI)?
- Which repo owns and publishes the "Keboola plugin" bundle — mcp-server (coupled to the tools) or ai-kit's existing plugin marketplace (already the skills home)? (Raised in review.)
