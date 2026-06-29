import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager } from '@/clients/keboola';
import type { Config } from '@/config';
import { DATA_APP_COMPONENT_ID } from '@/constants';
import type { Link } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import { toonSerializeCompact } from '@/serialize';
import {
  createDataScience,
  fetchDataApp,
  fetchDataAppDetailsTask,
  fetchLatestRun,
  fetchLogs,
  hasFeature,
  resolveWorkspace,
  storageHelpers,
  withDeploymentInfo,
} from './client';
import { applyFolderMetadata, setCfgCreationMetadata, setCfgUpdateMetadata } from './metadata';
import { actionSchema, authenticationTypeSchema, type DataApp, modeSchema } from './model';
import {
  asRecord,
  buildAuthenticatedCloneUrl,
  buildDataAppConfig,
  DATA_APPS_STORAGE_WORKSPACE_FEATURE,
  DEFAULT_DRAFT_BRANCH,
  encryptConfig,
  folderFieldDescription,
  getAuthorization,
  getSecrets,
  isDraftConfig,
  MANAGED_GIT_REPO_USERNAME,
  responseForState,
  SECRET_WORKSPACE_ID,
  summaryFromApiResponse,
  summaryFromDataApp,
  updateExistingCodeDataAppConfig,
  updateExistingDataAppConfig,
  usesBasicAuthentication,
  validateDataAppStorage,
} from './utils';

// Ported from tools/data_apps.py. Data App tools are blocked outside the main branch
// centrally via tool filtering; this module registers them normally.

// ===========================================================================
// Tool registration
// ===========================================================================

export const registerDataAppTools = (server: McpServer, config: Config): void => {
  const makeContext = () => {
    const clients = createKeboolaClients(config);
    const ds = createDataScience(clients, config);
    const helpers = storageHelpers(clients);
    return { clients, ds, helpers };
  };

  registerTool(server, {
    name: 'modify_streamlit_data_app',
    title: 'Modify Streamlit data app',
    description: `Creates or updates a Streamlit data app.

Considerations:
- The \`source_code\` parameter must be a complete and runnable Streamlit app. It must include a placeholder \`{QUERY_DATA_FUNCTION}\` where a \`query_data\` function will be injected. This function queries the workspace to get data, it accepts a string of SQL query following current sql dialect and returns a pandas DataFrame with the results from the workspace.
- Write SQL queries so they are compatible with the current workspace backend, you can ensure this by using the \`query_data\` tool to inspect the data in the workspace before using it in the data app.
- If you're updating an existing data app, provide the \`configuration_id\` parameter and the \`change_description\` parameter. To keep existing data app values during an update, leave them as empty strings, lists, or None appropriately based on the parameter type.
- After creating or updating a data app with this tool, ALWAYS call \`deploy_data_app(action="deploy", configuration_id=...)\` to start a new app or restart an existing app so changes take effect. Without this step, a newly created app will not start, and an existing app will keep running the previous deployment without the latest changes.
- New apps use the HTTP basic authentication by default for security unless explicitly specified otherwise; when updating, set \`authentication_type\` to \`default\` to keep the existing authentication type configuration (including OIDC setups) unless explicitly specified otherwise.

SQL & DATA TYPE RULES:
- Use delimited identifiers for the current SQL dialect for all column names and aliases in SQL. Match the exact identifier case used in SQL when referencing columns in Python code.
- \`query_data\` RETURNS ALL COLUMNS AS STRINGS regardless of SQL CAST. Always convert types in Python after loading: \`df["col"] = pd.to_numeric(df["col"], errors="coerce").fillna(0)\` and \`df["date"] = pd.to_datetime(df["date"], errors="coerce")\`.`,
    annotations: { destructiveHint: true },
    inputSchema: {
      name: z.string().describe('Name of the data app (max ~50 chars to fit DNS label limit).'),
      description: z.string().describe('Description of the data app.'),
      source_code: z.string().describe('Complete Python/Streamlit source code for the data app.'),
      packages: z
        .array(z.string())
        .describe(
          'Python packages used in the source code that will be installed by `pip install` ' +
            'into the environment before the code runs. For example: ["pandas", "requests~=2.32"].',
        ),
      authentication_type: authenticationTypeSchema.describe(
        'Authentication type, "no-auth" removes authentication completely, "basic-auth" sets the data ' +
          'app to be secured using the HTTP basic authentication, and "default" keeps the existing ' +
          'authentication type when updating.',
      ),
      configuration_id: z
        .string()
        .default('')
        .describe(
          'The ID of existing data app configuration when updating, otherwise empty string.',
        ),
      change_description: z
        .string()
        .default('')
        .describe(
          'The description of the change when updating (e.g. "Update Code"), otherwise empty string.',
        ),
      folder: z.string().nullish().describe(folderFieldDescription('data app', 'data apps')),
    },
    serializer: toonSerializeCompact,
    handler: async (args) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);
      const projectId = String(
        ((await clients.storage.tokens.verify()) as { owner: { id: string | number } }).owner.id,
      );
      const ws = await resolveWorkspace(config, helpers);
      const secrets = getSecrets(ws.workspaceId, ws.branchId);

      if (args.configuration_id) {
        const dataAppPre = await fetchDataApp(ds, helpers, args.configuration_id);
        let updatedConfig = updateExistingDataAppConfig(
          dataAppPre.configuration,
          args.name,
          args.source_code,
          args.packages,
          args.authentication_type,
          secrets,
          ws.sqlDialect,
        );
        updatedConfig = await encryptConfig(config, updatedConfig, {
          projectId,
          componentId: DATA_APP_COMPONENT_ID,
        });
        const updateResp = await helpers.configurationUpdate({
          configurationId: args.configuration_id,
          configuration: updatedConfig,
          changeDescription: args.change_description || 'Change Data App',
          updatedName: args.name || dataAppPre.name,
          updatedDescription: args.description || dataAppPre.description || undefined,
        });
        // --- write committed past this point; response building is best-effort ---
        const newVersion = String(updateResp.version ?? '');
        try {
          if (/^\d+$/.test(newVersion)) {
            await setCfgUpdateMetadata(helpers, args.configuration_id, Number(newVersion));
          }
          const folderHint = await applyFolderMetadata(
            helpers,
            args.configuration_id,
            args.folder,
            'data apps',
            'modify_streamlit_data_app',
          );
          const dataApp = await fetchDataApp(ds, helpers, args.configuration_id);
          const links = linksManager.getDataAppLinks(
            dataApp.configuration_id,
            args.name,
            dataApp.deployment_url ?? undefined,
            usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
          );
          return {
            response: responseForState(dataApp.state),
            change_summary: folderHint,
            data_app: summaryFromDataApp(dataApp),
            links,
          };
        } catch (error) {
          logger.error(
            { err: error },
            `Data app configuration ${args.configuration_id} was updated (version ${newVersion || '?'}) ` +
              `but building the response failed; returning a partial success.`,
          );
          const summary = summaryFromDataApp(dataAppPre);
          summary.config_version = newVersion || summary.config_version;
          let links: Link[] = [];
          try {
            links = linksManager.getDataAppLinks(
              args.configuration_id,
              args.name || dataAppPre.name,
              dataAppPre.deployment_url ?? undefined,
              usesBasicAuthentication(asRecord(dataAppPre.configuration.authorization)),
            );
          } catch {
            links = [];
          }
          return {
            response: responseForState(dataAppPre.state),
            change_summary:
              `The configuration WAS updated (version ${newVersion || 'unknown'}), but loading the full app ` +
              `details failed, so this response is partial. Do NOT retry the update -- the change is already ` +
              `applied. Call deploy_data_app to apply it to the running app.`,
            data_app: summary,
            links,
          };
        }
      }

      // Create new data app.
      let createCfg = buildDataAppConfig(
        args.name,
        args.source_code,
        args.packages,
        args.authentication_type,
        secrets,
        ws.sqlDialect,
      );
      createCfg = await encryptConfig(config, createCfg, {
        projectId,
        componentId: DATA_APP_COMPONENT_ID,
      });
      const dataAppResp = await ds.createDataApp({
        name: args.name,
        description: args.description,
        config: createCfg,
        branchId: config.branchId ?? null,
        appType: 'streamlit',
        useManagedGitRepo: false,
      });
      try {
        await setCfgCreationMetadata(helpers, dataAppResp.config_id);
        const folderHint = await applyFolderMetadata(
          helpers,
          dataAppResp.config_id,
          args.folder,
          'data apps',
          'modify_streamlit_data_app',
          true,
        );
        const links = linksManager.getDataAppLinks(
          dataAppResp.config_id,
          args.name,
          dataAppResp.url ?? undefined,
          usesBasicAuthentication(asRecord(createCfg.authorization)),
        );
        return {
          response: 'created',
          change_summary: folderHint,
          data_app: summaryFromApiResponse(dataAppResp),
          links,
        };
      } catch (error) {
        logger.error(
          { err: error },
          `Data app ${dataAppResp.id} was created (configuration ${dataAppResp.config_id}) but building ` +
            `the response failed; returning a partial success.`,
        );
        let links: Link[] = [];
        try {
          links = linksManager.getDataAppLinks(
            dataAppResp.config_id,
            args.name,
            dataAppResp.url ?? undefined,
            usesBasicAuthentication(asRecord(createCfg.authorization)),
          );
        } catch {
          links = [];
        }
        return {
          response: 'created',
          change_summary:
            `The data app WAS created (configuration ${dataAppResp.config_id}), but building the full response ` +
            `failed, so this response is partial. Do NOT retry creation -- it would create a duplicate. ` +
            `Call deploy_data_app to start the app.`,
          data_app: summaryFromApiResponse(dataAppResp),
          links,
        };
      }
    },
  });

  registerTool(server, {
    name: 'modify_python_js_data_app',
    title: 'Modify python-js data app',
    description: `Creates or updates a python-js data app.

Two-app project model. Every python-js project has a persistent **prod app** that owns the only managed git repository for the project, and zero or more **drafts** parented to that prod app. A draft is a Storage configuration with \`parameters.dataApp.isDraft=true\` and \`parameters.dataApp.parentConfigurationId=<prod cfg id>\`; it's an *external-git* app that clones the parent prod's repo at a pinned branch on every deploy. Drafts are surfaced in the Keboola UI under their parent prod app. Use \`deploy_data_app(mode='dev')\` to deploy a draft as a dev version of the data app (hot reload + auto-auth for iframe preview); use \`delete_python_js_data_app_draft\` to tear a draft down after its branch has been promoted.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. MCP gives you authenticated clone URLs and manages configs/deploys; it never invokes git.

**The draft flow is mandatory — never edit prod source directly.** Every source-code change goes through a draft branch that the user previews and explicitly approves first. NEVER push directly to \`main\`: \`main\` only ever advances by merging an approved draft branch, and only after the user has approved that draft's preview.

## Argument rules
- \`parent_configuration_id\` is **create-only**. Rejected on update.
- \`branch\` is **create-only** and only valid when \`parent_configuration_id\` is set. Defaults to \`'init'\`. Must not be \`'main'\`. Rejected on prod create and on update.
- \`slug\` is required on create and immutable after.
- The **update path** (passing \`configuration_id\`) is for changing \`name\`, \`description\`, \`authentication_type\`, \`auto_suspend_after_seconds\`, \`storage\` on either a prod app or a draft. Source code changes go through the git flow above, not this tool.

## Authentication
New apps default to HTTP basic authentication for safety. Pass \`authentication_type='no-auth'\` to expose publicly. On update, \`authentication_type='default'\` preserves the existing \`authorization\` block (including OIDC setups configured outside the MCP); \`'basic-auth'\` / \`'no-auth'\` overwrite it.

## Slug constraint
Must be DNS-label-safe (lowercase letters, digits, hyphens, ≤63 chars). For drafts, append a short suffix (e.g. \`-draft-abc123\`) to keep slugs unique across the prod and its drafts.`,
    annotations: { destructiveHint: true },
    inputSchema: {
      name: z.string().describe('Name of the data app (max ~50 chars to fit DNS label limit).'),
      description: z.string().describe('Description of the data app.'),
      configuration_id: z
        .string()
        .default('')
        .describe(
          'The ID of existing data app configuration when updating, otherwise empty string.',
        ),
      change_description: z
        .string()
        .default('')
        .describe(
          'The description of the change when updating (e.g. "Bump image"), otherwise empty string.',
        ),
      slug: z
        .string()
        .nullish()
        .describe(
          'URL-safe slug for the data app (used as a subdomain). Required when creating; immutable after.',
        ),
      parent_configuration_id: z
        .string()
        .nullish()
        .describe(
          'Storage configuration ID of the prod python-js data app this draft will iterate against. ' +
            'When set on create, the new app is created as a **draft**: no managed repo is provisioned ' +
            "for it; instead its `parameters.dataApp.git` block is populated to point at the prod app's " +
            'managed repo, with a freshly-minted prod-app HTTPS token and the chosen draft branch. ' +
            'Leave None on create to make a **prod app** (which gets its own managed repo). Rejected on update.',
        ),
      branch: z
        .string()
        .nullish()
        .describe(
          'Draft branch to pin the new draft to. Only valid on the draft create path ' +
            '(when `parent_configuration_id` is set). Defaults to `init` when unset. Must not be `main` ' +
            '(reserved for the prod app). Rejected on prod create and on update.',
        ),
      authentication_type: authenticationTypeSchema
        .default('default')
        .describe(
          'Authentication type. "no-auth" removes authentication completely, "basic-auth" secures the ' +
            'data app via HTTP basic authentication, and "default" means: on create, apply basic auth ' +
            '(safe default for new apps); on update, keep the existing authentication configuration ' +
            '(including OIDC setups configured outside the MCP).',
        ),
      auto_suspend_after_seconds: z
        .number()
        .int()
        .default(900)
        .describe('Number of seconds after which the running data app is automatically suspended.'),
      storage: z
        .record(z.string(), z.any())
        .nullish()
        .describe(
          'Complete storage configuration for the data app (input/output table mappings). ' +
            'Replaces the ENTIRE storage block when updating an existing app. Leave unset (None) to ' +
            'preserve the existing storage configuration; pass an empty dict to explicitly clear it.',
        ),
      folder: z.string().nullish().describe(folderFieldDescription('data app', 'data apps')),
    },
    serializer: toonSerializeCompact,
    handler: async (args) => {
      if (args.configuration_id) {
        if (args.slug) throw new Error('slug cannot be changed after the data app is created.');
        if (args.parent_configuration_id) {
          throw new Error(
            'parent_configuration_id is only valid when creating a draft (no configuration_id).',
          );
        }
        if (args.branch) {
          throw new Error('branch is only valid when creating a draft (no configuration_id).');
        }
      } else {
        if (!args.slug) {
          throw new Error('slug is required when creating a python-js data app.');
        }
        if (args.branch != null && !args.parent_configuration_id) {
          throw new Error(
            'branch is only valid on the draft create path (pair it with parent_configuration_id).',
          );
        }
      }

      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      const validatedStorage = validateDataAppStorage(args.storage);

      const hasStorageWorkspace = await hasFeature(clients, DATA_APPS_STORAGE_WORKSPACE_FEATURE);
      let legacySecrets: Record<string, unknown> | null = null;
      if (!hasStorageWorkspace) {
        const ws = await resolveWorkspace(config, helpers);
        legacySecrets = { [SECRET_WORKSPACE_ID]: ws.workspaceId };
      }

      if (args.configuration_id) {
        let dataApp = await fetchDataApp(ds, helpers, args.configuration_id);
        const updatedConfig = updateExistingCodeDataAppConfig(
          dataApp.configuration,
          args.auto_suspend_after_seconds,
          args.authentication_type,
          legacySecrets,
          validatedStorage,
        );
        await helpers.configurationUpdate({
          configurationId: args.configuration_id,
          configuration: updatedConfig,
          changeDescription: args.change_description || 'Update python-js data app',
          updatedName: args.name || dataApp.name,
          updatedDescription: args.description || dataApp.description || undefined,
        });
        dataApp = await fetchDataApp(ds, helpers, args.configuration_id);
        await setCfgUpdateMetadata(helpers, args.configuration_id, Number(dataApp.config_version));
        const folderHint = await applyFolderMetadata(
          helpers,
          args.configuration_id,
          args.folder,
          'data apps',
          'modify_python_js_data_app',
        );
        const repoUrl = dataApp.repo_url;
        const links = linksManager.getDataAppLinks(
          dataApp.configuration_id,
          args.name || dataApp.name,
          dataApp.deployment_url ?? undefined,
          usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
        );
        const summary = summaryFromDataApp(dataApp);
        summary.repo_url = repoUrl;
        return {
          response: responseForState(dataApp.state),
          change_summary: folderHint,
          data_app: summary,
          repo_url: repoUrl,
          links,
        };
      }

      // Create new python-js data app (prod or draft).
      const slug = args.slug!;
      const usesBasicAuth =
        args.authentication_type === 'basic-auth' || args.authentication_type === 'default';
      const authorizationModel = getAuthorization(usesBasicAuth);

      let gitCloneUrl: string | null = null;
      let draftBranch: string | null = null;
      let gitBlock: Record<string, unknown> | null = null;

      if (args.parent_configuration_id) {
        const parent = await fetchDataApp(ds, helpers, args.parent_configuration_id);
        if (parent.type !== 'python-js') {
          throw new Error(
            `parent_configuration_id "${args.parent_configuration_id}" is type "${parent.type}", but only ` +
              `python-js prod apps can parent a draft.`,
          );
        }
        if (isDraftConfig(parent.configuration)) {
          throw new Error(
            `parent_configuration_id "${args.parent_configuration_id}" is itself a python-js **draft**, ` +
              "not a prod app. Drafts iterate against the prod app's repo and cannot parent another " +
              "draft — pass the prod app's configuration_id (a draft's parentConfigurationId points to it).",
          );
        }
        if (!parent.repo_url) {
          throw new Error(
            `Parent python-js data app "${args.parent_configuration_id}" has no managed git repo URL. ` +
              'This indicates a platform-side bug — retry or contact support.',
          );
        }
        draftBranch = (args.branch || DEFAULT_DRAFT_BRANCH).trim();
        if (!draftBranch || /\s/.test(draftBranch)) {
          throw new Error(`branch "${args.branch}" is not a valid git branch name.`);
        }
        if (draftBranch === 'main') {
          throw new Error(
            'branch "main" is reserved for the prod app — pick a different draft branch.',
          );
        }
        const cred = await ds.createAppGitCredential(parent.data_app_id);
        if (!cred.secret) {
          throw new Error(
            `Parent data app ${parent.data_app_id} credentials endpoint returned no \`secret\` for an ` +
              `http_token credential. This indicates a platform-side bug — retry or contact support.`,
          );
        }
        gitBlock = {
          repository: parent.repo_url,
          username: MANAGED_GIT_REPO_USERNAME,
          '#password': cred.secret,
          branch: draftBranch,
        };
        gitCloneUrl = buildAuthenticatedCloneUrl(parent.repo_url, cred.secret);
      }

      const dataAppBlock: Record<string, unknown> = { slug };
      if (legacySecrets) dataAppBlock.secrets = legacySecrets;
      if (gitBlock) dataAppBlock.git = gitBlock;
      if (args.parent_configuration_id != null) {
        dataAppBlock.isDraft = true;
        dataAppBlock.parentConfigurationId = args.parent_configuration_id;
      }
      let configPayload: Record<string, unknown> = {
        parameters: {
          autoSuspendAfterSeconds: args.auto_suspend_after_seconds,
          dataApp: dataAppBlock,
        },
        authorization: authorizationModel,
      };
      if (hasStorageWorkspace) {
        configPayload.runtime = { workspace: { enabled: true } };
      }
      if (validatedStorage && Object.keys(validatedStorage).length > 0) {
        configPayload.storage = validatedStorage;
      }

      if (gitBlock !== null) {
        const projectId = String(
          ((await clients.storage.tokens.verify()) as { owner: { id: string | number } }).owner.id,
        );
        configPayload = await encryptConfig(config, configPayload, {
          projectId,
          componentId: DATA_APP_COMPONENT_ID,
        });
      }

      const dataAppResp = await ds.createDataApp({
        name: args.name,
        description: args.description,
        config: configPayload,
        branchId: config.branchId ?? null,
        appType: 'python-js',
        useManagedGitRepo: args.parent_configuration_id == null,
      });

      let repoUrl: string;
      if (args.parent_configuration_id) {
        repoUrl = gitBlock!.repository as string;
      } else {
        const repoResp = await ds.getAppGitRepo(dataAppResp.id);
        if (repoResp.https_url == null) {
          throw new Error(
            `Data app ${dataAppResp.id} reports no HTTPS clone URL despite having a managed git repo. ` +
              'This indicates a platform-side bug — retry or contact support.',
          );
        }
        repoUrl = repoResp.https_url;
      }
      await setCfgCreationMetadata(helpers, dataAppResp.config_id);
      const folderHint = await applyFolderMetadata(
        helpers,
        dataAppResp.config_id,
        args.folder,
        'data apps',
        'modify_python_js_data_app',
        true,
      );
      const links = linksManager.getDataAppLinks(
        dataAppResp.config_id,
        args.name,
        dataAppResp.url ?? undefined,
        usesBasicAuth,
      );
      const summary = summaryFromApiResponse(dataAppResp);
      summary.repo_url = repoUrl;
      return {
        response: 'created',
        change_summary: folderHint,
        data_app: summary,
        repo_url: repoUrl,
        git_clone_url: gitCloneUrl,
        branch: draftBranch,
        links,
      };
    },
  });

  registerTool(server, {
    name: 'create_python_js_data_app_git_credential',
    title: 'Create python-js data app git credential',
    description: `Mints a one-time HTTPS token on a python-js **prod** data app so the caller can clone, pull, and push to the app's managed git repo over HTTPS.

**Always call against the prod app's configuration_id** — drafts have no managed repo of their own, so calling this on a draft fails. The prod app is the canonical repo owner; drafts iterate against branches of that same repo.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. This tool only mints credentials.

Returns a ready-to-use \`git_clone_url\` of the form \`https://kai:<secret>@<host>/<path>.git\` plus the raw \`secret\`. The token is returned **only** at creation — the platform cannot return it again on any subsequent read. Stash the URL (or the secret) somewhere the LLM can reuse for the rest of the session.

## Constraints
- Only python-js prod data apps have a managed git repo. Streamlit apps reject the call with a clear error.
- Permissions are always \`readWrite\`.`,
    annotations: { destructiveHint: false },
    inputSchema: {
      configuration_id: z.string().describe('Storage configuration ID of the python-js data app.'),
    },
    serializer: toonSerializeCompact,
    handler: async ({ configuration_id }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      const dataApp = await fetchDataApp(ds, helpers, configuration_id);
      if (dataApp.type !== 'python-js') {
        throw new Error(
          `create_python_js_data_app_git_credential only supports python-js data apps, but configuration ` +
            `"${configuration_id}" is type "${dataApp.type}".`,
        );
      }
      if (isDraftConfig(dataApp.configuration)) {
        const dataAppBlock = asRecord(asRecord(dataApp.configuration.parameters).dataApp);
        const parentCfgId = dataAppBlock.parentConfigurationId;
        const hint =
          typeof parentCfgId === 'string' ? ` (parentConfigurationId="${parentCfgId}")` : '';
        throw new Error(
          `Configuration "${configuration_id}" is a python-js **draft**, which has no managed git repo ` +
            `of its own. Mint credentials against the parent prod app instead${hint}.`,
        );
      }

      const repoResp = await ds.getAppGitRepo(dataApp.data_app_id);
      if (repoResp.https_url == null) {
        throw new Error(
          `Data app ${dataApp.data_app_id} reports no HTTPS clone URL despite being a python-js managed-repo ` +
            `app. This indicates a platform-side bug — retry or contact support.`,
        );
      }

      const credentialResp = await ds.createAppGitCredential(dataApp.data_app_id);
      if (!credentialResp.secret) {
        throw new Error(
          `Data app ${dataApp.data_app_id} credentials endpoint returned no \`secret\` for an http_token ` +
            `credential. This indicates a platform-side bug — retry or contact support.`,
        );
      }

      const gitCloneUrl = buildAuthenticatedCloneUrl(repoResp.https_url, credentialResp.secret);
      const links = linksManager.getDataAppLinks(
        dataApp.configuration_id,
        dataApp.name,
        dataApp.deployment_url ?? undefined,
        false,
      );
      return {
        response: 'created',
        configuration_id: dataApp.configuration_id,
        data_app_id: dataApp.data_app_id,
        credential_id: credentialResp.id,
        git_clone_url: gitCloneUrl,
        secret: credentialResp.secret,
        permissions: credentialResp.permissions,
        links,
      };
    },
  });

  registerTool(server, {
    name: 'get_data_apps',
    title: 'Get data apps',
    description: `Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by providing their configuration IDs.

WHEN NOT TO USE:
- Do NOT list all data apps just to find one by name. Use \`search\` with item_types=["data-app"] instead.
- Only list all data apps when you need a complete inventory.

Considerations:
- If configuration_ids are provided, the tool will return details of the data apps by their configuration IDs.
- If no configuration_ids are provided, the tool will list all data apps in the project given the limit and offset.
- Data App detail contains configuration, metadata, source code, links, and deployment info along with the latest data app logs to investigate in-app errors. The logs may be updated after opening the data app URL.
- \`deployment_info.last_run\` carries the outcome of the most recent deployment attempt. For an app that fails to start, check its \`failure_reason\`/\`failure_message\` FIRST — they cover setup-phase failures (e.g. invalid secrets, git clone errors, failing setup scripts) that happen before the container starts and therefore never appear in the regular logs.
- \`repo_url\` (managed git repo URL for python-js apps) is ONLY populated on the detail path (when \`configuration_ids\` is provided). The inventory list always returns \`repo_url=None\`, even for python-js apps with a managed repo — to retrieve the URL, call this tool again with the target \`configuration_ids\`.
- When called with \`configuration_ids=[<prod-cfg>]\` for a python-js **prod** app, the response includes a \`drafts: [...]\` array of every draft (configs with \`isDraft=true\` and \`parentConfigurationId == <prod-cfg>\`) currently in the project. Drafts in trash are not included. The array is empty for drafts themselves and for Streamlit apps.`,
    annotations: { readOnlyHint: true },
    inputSchema: {
      configuration_ids: z
        .array(z.string())
        .default([])
        .describe('The IDs of the data app configurations.'),
      limit: z.number().int().default(100).describe('The limit of the data apps to fetch.'),
      offset: z.number().int().default(0).describe('The offset of the data apps to fetch.'),
    },
    serializer: toonSerializeCompact,
    handler: async ({ configuration_ids, limit, offset }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      if (configuration_ids.length > 0) {
        const details = await Promise.all(
          configuration_ids.map((id) => fetchDataAppDetailsTask(ds, helpers, linksManager, id)),
        );
        const found = details.filter((d): d is DataApp => typeof d !== 'string');
        const notFound = details.filter((d): d is string => typeof d === 'string');
        if (notFound.length > 0) {
          logger.error(`Could not find Data Apps Configurations for IDs: ${notFound.join(', ')}`);
        }
        return { data_apps: found };
      }

      let dataApps = await ds.listDataApps(limit, offset);
      dataApps = dataApps.filter((app) => app.component_id === DATA_APP_COMPONENT_ID);
      return {
        data_apps: dataApps.map(summaryFromApiResponse),
        links: [linksManager.getDataAppDashboardLink()],
      };
    },
  });

  registerTool(server, {
    name: 'deploy_data_app',
    title: 'Deploy data app',
    description: `Deploys/redeploys a data app or stops a running data app in the Keboola environment asynchronously, given the action and the configuration ID.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. This tool only triggers deploys against existing git state.

## Mode (python-js apps)
- \`mode='dev'\` deploys the target as a **dev version of the data app** — the runtime uses a development \`setup.sh\` (hot reload) and the data-app proxy enables an auto-auth path so an iframe preview can render without a manual login. Only meaningful on **draft** configs (python-js apps with \`isDraft=true\`).
- For prod redeploys (including after merging a draft's branch into \`main\`), use no \`mode\` — the prod app picks up the current \`main\`.
- The branch a draft deploys from is pinned in \`parameters.dataApp.git.branch\` at create time; there is no deploy-time override.
- python-js apps do NOT fetch a Storage \`configVersion\` for deployment (their source lives in git, not in the Storage configuration); this is handled automatically.

## Streamlit apps
Streamlit apps have no managed git repo, so \`mode\` has no effect on the deployed app. \`mode=None\` is the expected call shape.

## General considerations
- Redeploying a data app takes some time, and the app may temporarily report status "stopped" during the restart.
- After deployment, the deployment info includes the app URL and the latest logs to help diagnose in-app errors.`,
    annotations: { destructiveHint: false },
    inputSchema: {
      action: actionSchema.describe('The action to perform.'),
      configuration_id: z.string().describe('The ID of the data app configuration.'),
      mode: modeSchema
        .nullish()
        .describe(
          'Deployment mode. Set to "dev" to deploy a python-js draft as a **dev version of the data ' +
            'app** — the runtime uses a development `setup.sh` (hot reload), and the data-app proxy ' +
            'enables an auto-auth path so an iframe preview can render without a manual login. ' +
            'Only meaningful on **draft** configs (python-js apps with `isDraft=true`). Leave None ' +
            '(default) for prod redeploys and for Streamlit apps.',
        ),
    },
    serializer: toonSerializeCompact,
    handler: async ({ action, configuration_id, mode }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      if (action === 'deploy') {
        let dataApp = await fetchDataApp(ds, helpers, configuration_id);
        if (dataApp.state === 'stopping') {
          throw new Error('Data app is currently "stopping", could not be started at the moment.');
        }
        let configVersionArg: string | null = null;
        if (dataApp.type !== 'python-js') {
          const version = await helpers.configurationVersionLatest(dataApp.configuration_id);
          configVersionArg = String(version);
        }
        await ds.deployDataApp(dataApp.data_app_id, configVersionArg, mode ?? null);
        dataApp = await fetchDataApp(ds, helpers, configuration_id);
        dataApp = withDeploymentInfo(
          dataApp,
          await fetchLogs(ds, dataApp.data_app_id),
          await fetchLatestRun(ds, dataApp.data_app_id),
        );
        const links = linksManager.getDataAppLinks(
          dataApp.configuration_id,
          dataApp.name,
          dataApp.deployment_url ?? undefined,
          usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
        );
        return { state: dataApp.state, deployment_info: dataApp.deployment_info, links };
      }

      // action === 'stop'
      let dataApp = await fetchDataApp(ds, helpers, configuration_id);
      if (dataApp.state === 'starting' || dataApp.state === 'restarting') {
        throw new Error('Data app is currently "starting", could not be stopped at the moment.');
      }
      await ds.suspendDataApp(dataApp.data_app_id);
      dataApp = await fetchDataApp(ds, helpers, configuration_id);
      const links = linksManager.getDataAppLinks(
        dataApp.configuration_id,
        dataApp.name,
        undefined,
        usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
      );
      return { state: dataApp.state, deployment_info: null, links };
    },
  });

  registerTool(server, {
    name: 'delete_python_js_data_app_draft',
    title: 'Delete python-js data app draft',
    description: `Deletes a python-js DRAFT data app — both the data-app instance (DSAPI) and its Storage configuration.

**MCP never runs git on your behalf.** Deleting the feature branch on the remote is your job; this tool only tears down the draft config and its data-app instance.

WHEN TO CALL: at the end of a promote-to-prod sequence, after you have merged the draft's branch into \`main\`, pushed, deleted the feature branch from the remote, and redeployed the prod app. The Keboola UI lists drafts under their parent prod app; once you call this tool, the draft disappears from that list.

WHAT THIS TOOL REFUSES:
  - prod apps (no \`isDraft\` flag) — protects against accidental prod deletion;
  - Streamlit apps — they have no draft concept.

WHAT THIS TOOL DOES NOT DO:
  - Run git. Deleting the feature branch on the remote is your job.
  - Revoke the prod-side git credential minted when the draft was created.

After a successful call, pivot back to the parent prod app (its configuration_id is returned in the response) or to \`get_data_apps\` for further work.`,
    annotations: { destructiveHint: true },
    inputSchema: {
      configuration_id: z
        .string()
        .describe('Storage configuration ID of the python-js draft data app to delete.'),
    },
    serializer: toonSerializeCompact,
    handler: async ({ configuration_id }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      const dataApp = await fetchDataApp(ds, helpers, configuration_id);
      if (dataApp.type !== 'python-js') {
        throw new Error(
          `delete_python_js_data_app_draft only supports python-js data apps, but configuration ` +
            `"${configuration_id}" is type "${dataApp.type}".`,
        );
      }
      if (!isDraftConfig(dataApp.configuration)) {
        throw new Error(
          `Configuration "${configuration_id}" is a python-js **prod** app, not a draft ` +
            '(parameters.dataApp.isDraft is not true). This tool only deletes drafts — ' +
            'prod apps must be deleted from the Keboola UI.',
        );
      }

      const dataAppBlock = asRecord(asRecord(dataApp.configuration.parameters).dataApp);
      const parentCfgId = dataAppBlock.parentConfigurationId;
      const parentConfigurationId = typeof parentCfgId === 'string' ? parentCfgId : null;

      await ds.deleteDataApp(dataApp.data_app_id);

      const links = linksManager.getDataAppLinks(
        parentConfigurationId ?? configuration_id,
        parentConfigurationId ? 'parent prod app' : dataApp.name,
        undefined,
        false,
      );
      return {
        response: 'deleted',
        configuration_id,
        data_app_id: dataApp.data_app_id,
        parent_configuration_id: parentConfigurationId,
        links,
      };
    },
  });

  logger.info('Data app tools initialized.');
};
