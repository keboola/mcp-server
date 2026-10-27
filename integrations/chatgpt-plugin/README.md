# Keboola plugin for ChatGPT and Codex

An [OpenAI plugin](https://developers.openai.com/plugins/build/plugins) package: four skills plus the hosted
Keboola MCP server. Design and rollout: RFC "Keboola as a ChatGPT Plugin" (keboola/connection
[#9019](https://github.com/keboola/connection/pull/9019), Linear
[DMD-2463](https://linear.app/keboola/issue/DMD-2463/keboola-as-a-chatgpt-plugin-rfc)).

| File | Purpose |
|---|---|
| `plugin.json` | Manifest and directory listing fields (`extensions.com.openai.interface`) |
| `mcp.json` | The one MCP server, OAuth with dynamic client registration |
| `skills/*/SKILL.md` | Workflows that name the server's tools in order |
| `assets/icon.png` | Listing logo and composer icon |

`mcp.json` points at the global router `https://mcp-router.keboola.com/mcp`. On login the user picks their Keboola
stack, and the router sends every request to that stack's MCP server. A published plugin's MCP URL cannot change, so
the listing must use this one URL for all stacks.

Every skill starts with `get_accessible_projects` and `set_project_scope`, and no skill writes before the
session is scoped to the project the user confirmed. This is a prompt-level rule: the server lets a write tool
target any project the OAuth consent granted. The skills do it even though the server instructions say not to
scope preemptively, because the RFC requires it for the plugin.

Under OAuth the server keeps the scope on the login's session, so every ChatGPT conversation on the same
connection shares it, and a write without `project_id` can land in a project another conversation scoped.
Every write step therefore passes `project_id=<the confirmed id>`: a moved scope then fails the write with
"outside the current scope" instead of writing elsewhere.

`tests/test_chatgpt_plugin.py` fails when a skill names a tool or a tool argument the server does not register, a
tool that an OAuth session cannot call, a tool outside backticks, a write tool before `set_project_scope`, or a
write tool without `project_id`.

## Try it locally

1. Copy this directory to `~/.codex/plugins/keboola`.
2. Add it to `~/.agents/plugins/marketplace.json`:

   ```json
   {
     "name": "keboola-local",
     "interface": { "displayName": "Keboola (local)" },
     "plugins": [
       {
         "name": "keboola",
         "source": { "source": "local", "path": "./.codex/plugins/keboola" },
         "policy": { "installation": "AVAILABLE", "authentication": "ON_INSTALL" },
         "category": "Developer Tools"
       }
     ]
   }
   ```

   `source.path` is resolved relative to the marketplace root (your home directory here).
3. Restart the ChatGPT desktop app (or Codex), install "Keboola" from the local marketplace, pick your Keboola stack
   on the router's page and log in with your Keboola account.
