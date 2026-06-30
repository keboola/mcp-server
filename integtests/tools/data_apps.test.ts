import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest, type TestProject } from '../testproject/fixture';

import { createKeboolaClients } from '@/clients/keboola';
import { Config } from '@/config';
import { createDataScience, type DataScience } from '@/tools/data_apps/client';

// Ported from integtests/tools/test_data_apps.py.
//
// Data App tools are gated to the MAIN/production branch (filtering.ts
// DATA_APP_BRANCH_GATED_TOOLS). The pool leases its default (main) branch, so the tools
// are always available here — confirmed live: get_data_apps + modify_* + deploy_data_app
// are all listed and callable.
//
// These tests create REAL data-science apps. The MCP surface has no delete tool for
// Streamlit apps, so teardown talks to the data-science client directly (the same path
// the Python suite's `initial_data_app` fixture used:
// keboola_client.data_science_client.delete_data_app). Each created app is registered for
// best-effort suspend+delete in an afterEach so a leaked app never lingers in the project.
//
// The MCP client returns TOON text (snake_case field names preserved by the tools), so we
// assert on substrings / regexes rather than re-parsing structured content.

const SAMPLE_STREAMLIT_IMPORTS = 'import streamlit as st\n\n';
const SAMPLE_STREAMLIT_ENTRYPOINT =
  'def main():\n' +
  "    st.title('Integration Test Data App')\n" +
  "    st.write('Hello from integration test')\n\n" +
  'if __name__ == "__main__":\n' +
  '    main()\n';
// Mirrors the Python `sample_streamlit_app` fixture: imports + {QUERY_DATA_FUNCTION}
// placeholder (where the tool injects the query_data helper) + entrypoint.
const SAMPLE_STREAMLIT_APP = `${SAMPLE_STREAMLIT_IMPORTS}{QUERY_DATA_FUNCTION}\n\n${SAMPLE_STREAMLIT_ENTRYPOINT}`;

const PYTHON_JS_APP_PY =
  'from http.server import BaseHTTPRequestHandler, HTTPServer\n' +
  'import os\n\n' +
  'class H(BaseHTTPRequestHandler):\n' +
  '    def do_GET(self):\n' +
  '        self.send_response(200)\n' +
  '        self.end_headers()\n' +
  "        self.wfile.write(b'integration-test-ok')\n\n" +
  "if __name__ == '__main__':\n" +
  "    port = int(os.environ.get('PORT', '8000'))\n" +
  "    HTTPServer(('0.0.0.0', port), H).serve_forever()\n";

const uniqueSuffix = (): string => randomUUID().replace(/-/g, '').slice(0, 8);

// TOON renders string scalars that look numeric (or otherwise need quoting) wrapped in
// double quotes, e.g. `data_app_id: "74015536"`, while opaque ids stay bare, e.g.
// `configuration_id: 01kw...`. Strip an optional pair of surrounding quotes when extracting.
const unquote = (value: string): string => value.replace(/^"(.*)"$/, '$1');

/** Pulls the data_app_id out of a TOON tool response (`data_app_id: <id>`). */
const extractDataAppId = (toon: string): string | null => {
  const m = toon.match(/data_app_id:\s*("[^"]*"|\S+)/);
  return m ? unquote(m[1]!) : null;
};

/** Pulls the (first) configuration_id out of a TOON tool response. */
const extractConfigurationId = (toon: string): string | null => {
  const m = toon.match(/configuration_id:\s*("[^"]*"|\S+)/);
  return m ? unquote(m[1]!) : null;
};

/**
 * Provisions a read-only workspace (mirrors the Python `workspace_schema` fixture) and
 * returns its schema plus a deleter. Streamlit data app tools require a configured
 * workspace schema (resolveWorkspace throws otherwise), so the Streamlit tests build a
 * Config carrying this schema.
 */
const provisionWorkspace = async (
  project: TestProject,
): Promise<{ schema: string; remove: () => Promise<void> }> => {
  const base = `${project.storageApiUrl}/v2/storage`;
  const headers = {
    'X-StorageApi-Token': project.storageApiToken,
    'Content-Type': 'application/json',
  };
  const verify = (await (
    await fetch(`${base}/tokens/verify`, { headers })
  ).json()) as { owner?: { defaultBackend?: string } };
  const backend = verify.owner?.defaultBackend;
  const loginType =
    backend === 'snowflake' ? 'snowflake-person-sso' : backend === 'bigquery' ? 'default' : null;
  if (!loginType) throw new Error(`Unexpected project default backend: ${backend}`);

  const res = await fetch(`${base}/branch/default/workspaces`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ backend, loginType, readOnlyStorageAccess: true }),
  });
  const ws = (await res.json()) as {
    id?: number | string;
    connection?: { schema?: string };
  };
  if (!res.ok || !ws.id || !ws.connection?.schema) {
    throw new Error(`Failed to create workspace: ${res.status} ${JSON.stringify(ws)}`);
  }
  const schema = ws.connection.schema;
  const workspaceId = ws.id;
  return {
    schema,
    remove: async () => {
      try {
        await fetch(`${base}/branch/default/workspaces/${workspaceId}`, {
          method: 'DELETE',
          headers,
        });
      } catch {
        // best-effort
      }
    },
  };
};

/** Builds a Config identical to the leased one but carrying a workspace schema. */
const withWorkspaceSchema = (config: Config, schema: string): Config =>
  new Config({ ...config.toFields(), workspaceSchema: schema });

describe('data app tools (integration)', () => {
  // Best-effort teardown registry: { ds, dataAppId } pairs to suspend+delete after each test.
  let cleanup: { ds: DataScience; dataAppId: string }[] = [];
  // Workspaces provisioned for Streamlit tests, deleted after each test.
  let workspaceCleanup: (() => Promise<void>)[] = [];

  afterEach(async () => {
    for (const { ds, dataAppId } of cleanup.reverse()) {
      try {
        await ds.suspendDataApp(dataAppId);
      } catch {
        // ignore: app may already be stopped or gone.
      }
      try {
        await ds.deleteDataApp(dataAppId);
      } catch {
        // ignore: best-effort.
      }
    }
    cleanup = [];
    for (const remove of workspaceCleanup.reverse()) {
      await remove();
    }
    workspaceCleanup = [];
  });

  const dataScienceFor = (config: Config): DataScience =>
    createDataScience(createKeboolaClients(config), config);

  // Port of test_get_data_apps_listing: create an app, then get_data_apps must list it.
  it('get_data_apps lists a freshly created Streamlit app', async () => {
    const project = await getTestProjectForTest({ clean: false });
    const ws = await provisionWorkspace(project);
    workspaceCleanup.push(ws.remove);
    const config = withWorkspaceSchema(project.config, ws.schema);
    const ds = dataScienceFor(config);
    const session = await connectMcp(config);
    try {
      const appName = `Integration Test Data App ${uniqueSuffix()}`;
      const created = await callToolText(session.client, 'modify_streamlit_data_app', {
        name: appName,
        description: 'Data app created by integration test',
        source_code: SAMPLE_STREAMLIT_APP,
        packages: ['numpy', 'streamlit'],
        authentication_type: 'no-auth',
      });
      expect(created).toContain('response: created');
      const dataAppId = extractDataAppId(created);
      const configurationId = extractConfigurationId(created);
      expect(dataAppId).toBeTruthy();
      expect(configurationId).toBeTruthy();
      cleanup.push({ ds, dataAppId: dataAppId! });

      const listed = await callToolText(session.client, 'get_data_apps', { limit: 500 });
      // Listing returns DataAppSummary entries; our app's configuration_id must appear.
      expect(listed).toContain(configurationId!);
    } finally {
      await session.close();
    }
  });

  // Port of test_data_app_lifecycle (streamlit): create -> detail -> update -> detail.
  it('Streamlit data app create/detail/update lifecycle', async () => {
    const project = await getTestProjectForTest({ clean: false });
    const ws = await provisionWorkspace(project);
    workspaceCleanup.push(ws.remove);
    const config = withWorkspaceSchema(project.config, ws.schema);
    const ds = dataScienceFor(config);
    const session = await connectMcp(config);
    try {
      const appName = `Integration Test Data App ${uniqueSuffix()}`;
      const appDescription = 'Data app created by integration test';

      // Create.
      const created = await callToolText(session.client, 'modify_streamlit_data_app', {
        name: appName,
        description: appDescription,
        source_code: SAMPLE_STREAMLIT_APP,
        packages: ['numpy', 'streamlit'],
        authentication_type: 'no-auth',
      });
      expect(created).toContain('response: created');
      const dataAppId = extractDataAppId(created)!;
      const configurationId = extractConfigurationId(created)!;
      expect(dataAppId).toBeTruthy();
      expect(configurationId).toBeTruthy();
      cleanup.push({ ds, dataAppId });

      // Detail by configuration_id reflects the created app, the injected query_data
      // function, and the imports/entrypoint we sent.
      const detail = await callToolText(session.client, 'get_data_apps', {
        configuration_ids: [configurationId],
      });
      expect(detail).toContain(configurationId);
      expect(detail).toContain(dataAppId);
      expect(detail).toContain(appName);
      expect(detail).toContain(appDescription);
      expect(detail).toContain('import streamlit as st');
      expect(detail).toContain("st.title('Integration Test Data App')");

      // Update: new name/description, new source + packages.
      const updatedName = `${appName} - Updated`;
      const updatedDescription = 'Data app updated by integration test';
      const updated = await callToolText(session.client, 'modify_streamlit_data_app', {
        name: updatedName,
        description: updatedDescription,
        source_code: 'import numpy as np\n\n',
        packages: ['streamlit'],
        authentication_type: 'no-auth',
        configuration_id: configurationId,
        change_description: 'Update Code',
      });
      // Same app/config, response is an update (not "created").
      expect(updated).toContain(dataAppId);
      expect(updated).toContain(configurationId);
      expect(updated).not.toContain('response: created');

      // Detail reflects the updated name/description + the new source code; the old
      // streamlit imports/entrypoint are gone.
      const detail2 = await callToolText(session.client, 'get_data_apps', {
        configuration_ids: [configurationId],
      });
      expect(detail2).toContain(updatedName);
      expect(detail2).toContain(updatedDescription);
      expect(detail2).toContain('import numpy as np');
      expect(detail2).not.toContain("st.title('Integration Test Data App')");
    } finally {
      await session.close();
    }
  });

  // Port of test_python_js_data_app_prod_and_draft_lifecycle.
  //
  // create prod (managed repo) -> create draft (external-git pointing at prod's repo) ->
  // clone via embedded credential, push branch -> deploy draft mode='dev' -> assert the
  // prod detail surfaces the draft under `drafts:` -> merge into main, redeploy prod ->
  // delete the draft via delete_python_js_data_app_draft, assert it's gone from prod drafts.
  it('python-js prod + external-git draft lifecycle', async () => {
    const project = await getTestProjectForTest({ clean: false });
    const ds = dataScienceFor(project.config);
    const session = await connectMcp(project.config);
    const repoDir = mkdtempSync(join(tmpdir(), 'kbc-pyjs-'));
    const gitEnv = { ...process.env, GIT_TERMINAL_PROMPT: '0' };
    const git = (...args: string[]): void => {
      execFileSync('git', args, { cwd: repoDir, env: gitEnv, stdio: 'pipe' });
    };

    const unique = uniqueSuffix();
    let prodDataAppId: string | null = null;
    let draftDataAppId: string | null = null;
    let draftDeletedViaTool = false;

    try {
      // Step 1: create prod (managed repo). Response carries repo_url, no git_clone_url/branch.
      const prodResp = await callToolText(session.client, 'modify_python_js_data_app', {
        name: `Integration prod ${unique}`,
        description: 'AI-3286 prod app integration test',
        slug: `int-prod-${unique}`,
        authentication_type: 'no-auth',
      });
      expect(prodResp).toContain('response: created');
      const prodConfigId = extractConfigurationId(prodResp)!;
      prodDataAppId = extractDataAppId(prodResp)!;
      expect(prodConfigId).toBeTruthy();
      expect(prodDataAppId).toBeTruthy();
      cleanup.push({ ds, dataAppId: prodDataAppId });
      expect(prodResp).toMatch(/repo_url:\s*"?https:\/\//);

      // Step 2: create draft pointing at prod's repo. Branch defaults to 'init'.
      const draftResp = await callToolText(session.client, 'modify_python_js_data_app', {
        name: `Integration draft ${unique}`,
        description: 'AI-3286 draft integration test',
        slug: `int-draft-${unique}`,
        parent_configuration_id: prodConfigId,
        authentication_type: 'no-auth',
      });
      expect(draftResp).toContain('response: created');
      const draftConfigId = extractConfigurationId(draftResp)!;
      draftDataAppId = extractDataAppId(draftResp)!;
      expect(draftConfigId).toBeTruthy();
      expect(draftDataAppId).toBeTruthy();
      cleanup.push({ ds, dataAppId: draftDataAppId });
      // git_clone_url is an authenticated https://kai:<secret>@... clone URL; branch is 'init'.
      const cloneMatch = draftResp.match(/git_clone_url:\s*("[^"]*"|\S+)/);
      expect(cloneMatch).toBeTruthy();
      const gitCloneUrl = unquote(cloneMatch![1]!);
      expect(gitCloneUrl).toMatch(/^https:\/\/kai:/);
      expect(draftResp).toMatch(/branch:\s*"?init\b/);

      // Step 3: clone via the embedded credential; init main if empty, branch off, push.
      execFileSync('git', ['clone', gitCloneUrl, repoDir], { env: gitEnv, stdio: 'pipe' });
      git('config', 'user.email', 'mcp-integration@keboola.com');
      git('config', 'user.name', 'MCP Integration Test');
      let hasMain = true;
      try {
        execFileSync('git', ['rev-parse', '--verify', 'refs/heads/main'], {
          cwd: repoDir,
          env: gitEnv,
          stdio: 'pipe',
        });
      } catch {
        hasMain = false;
      }
      if (!hasMain) {
        git('checkout', '-b', 'main');
        writeFileSync(join(repoDir, 'README.md'), `# integration test ${unique}\n`);
        git('add', 'README.md');
        git('commit', '-m', 'init main');
        git('push', '-u', 'origin', 'main');
      }
      git('checkout', '-b', 'init', 'main');
      writeFileSync(join(repoDir, 'app.py'), PYTHON_JS_APP_PY);
      git('add', 'app.py');
      git('commit', '-m', `AI-3286 integration test commit ${unique}`);
      git('push', '-u', 'origin', 'init');

      // Step 4: deploy draft in mode='dev'. Fire-and-return: we only assert it was accepted.
      const draftDeploy = await callToolText(session.client, 'deploy_data_app', {
        action: 'deploy',
        configuration_id: draftConfigId,
        mode: 'dev',
      });
      expect(draftDeploy).toMatch(/state:/);

      // Prod detail must now list the draft under `drafts`.
      const prodDetailBefore = await callToolText(session.client, 'get_data_apps', {
        configuration_ids: [prodConfigId],
      });
      expect(prodDetailBefore).toContain(draftConfigId);

      // Draft's stored config carries the external-git block + parent linkage.
      const draftDetail = await callToolText(session.client, 'get_data_apps', {
        configuration_ids: [draftConfigId],
      });
      expect(draftDetail).toContain(prodConfigId); // parentConfigurationId
      expect(draftDetail).toMatch(/isDraft/);
      // Encrypted git password stored as KBC::-prefixed secret.
      expect(draftDetail).toContain('KBC::');

      // Step 5: merge into main and push.
      git('checkout', 'main');
      git('merge', '--no-ff', '-m', 'Merge init', 'init');
      git('push', 'origin', 'main');

      // Step 6: redeploy prod (no mode, no branch). Fire-and-return.
      const prodDeploy = await callToolText(session.client, 'deploy_data_app', {
        action: 'deploy',
        configuration_id: prodConfigId,
      });
      expect(prodDeploy).toMatch(/state:/);

      // Step 7: stop the draft (DSAPI delete requires desiredState == currentState), then
      // delete it via the MCP tool and verify it's gone from prod's drafts.
      try {
        await ds.suspendDataApp(draftDataAppId);
      } catch {
        // best-effort
      }
      const deleted = await callToolText(session.client, 'delete_python_js_data_app_draft', {
        configuration_id: draftConfigId,
      });
      expect(deleted).toContain('response: deleted');
      expect(deleted).toContain(draftConfigId);
      expect(deleted).toContain(prodConfigId); // parent_configuration_id
      draftDeletedViaTool = true;

      const prodDetailAfter = await callToolText(session.client, 'get_data_apps', {
        configuration_ids: [prodConfigId],
      });
      // The draft config id should no longer appear in the prod's drafts listing. (It may
      // still appear as part of the prod's own fields? No — drafts is the only place a draft
      // cfg id is surfaced on the prod detail, so absence is meaningful.)
      expect(prodDetailAfter).not.toContain(draftConfigId);
    } finally {
      // If the tool already deleted the draft, drop it from the cleanup registry so we don't
      // try to delete it twice (which would log a spurious DSAPI error).
      if (draftDeletedViaTool && draftDataAppId) {
        cleanup = cleanup.filter((c) => c.dataAppId !== draftDataAppId);
      }
      rmSync(repoDir, { recursive: true, force: true });
      await session.close();
    }
  });
});
