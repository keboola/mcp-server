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

`mcp.json` points at the US AWS server `https://mcp.keboola.com/mcp`, so only US AWS projects work. That is
enough for the internal workspace plugin; the public listing switches to the global router URL (RFC Phase 4),
because a published plugin's MCP URL cannot change.

Every skill starts with `get_accessible_projects` and `set_project_scope`, and no skill writes before the
session is scoped to the project the user confirmed. This is a prompt-level rule: the server lets a write tool
target any project the OAuth consent granted. The skills do it even though the server instructions say not to
scope preemptively, because the RFC requires it for the plugin.

Under OAuth the server keeps the scope on the login's session, so every ChatGPT conversation on the same
connection shares it, and a write without `project_id` goes to whatever project another conversation scoped last.
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
3. Restart the ChatGPT desktop app (or Codex), install "Keboola" from the local marketplace and log in with a
   Keboola account on US AWS. Until ChatGPT is pre-registered on the stack, the login shows the client approval
   screen once.
