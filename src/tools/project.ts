import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { readFileSync } from 'node:fs';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager, type KeboolaClients } from '@/clients/keboola';
import type { Config } from '@/config';
import { MetadataField } from '@/constants';
import type { Link } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import { resourcePath } from '@/resource-path';
import { createWorkspaceManager } from '@/tools/sql';

// Ported from tools/project.py.

// --- Project system prompt (LLM instruction) ---------------------------------
// The base prompt markdown lives next to this module's source. It is loaded once
// at runtime via fs, resolved relative to this module (import.meta.url), matching
// the Python `get_project_system_prompt` which reads the packaged resource file.
const PROMPT_PATH = resourcePath('prompts', 'project_system_prompt.md');

let cachedBasePrompt: string | undefined;
const loadBasePrompt = (): string => {
  if (cachedBasePrompt === undefined) {
    cachedBasePrompt = readFileSync(PROMPT_PATH, 'utf-8');
  }
  return cachedBasePrompt;
};

// Port of resources/prompts/__init__.py `_DIALECT_CONFIGS`.
type DialectConfig = {
  delimiter: string;
  col: string;
  fqn: string;
  newTable: string;
  extra: string[];
};

const DIALECT_CONFIGS: Record<string, DialectConfig> = {
  BigQuery: {
    delimiter: 'backtick (`` ` ``)',
    col: '`column_name`',
    fqn: '`project`.`dataset`.`table`',
    newTable: '`table_name`',
    extra: [],
  },
  Snowflake: {
    delimiter: 'double quote (`"`)',
    col: '"column_name"',
    fqn: '"DATABASE"."SCHEMA"."TABLE"',
    newTable: '"table_name"',
    extra: [
      'Unquoted identifiers and column aliases are auto-uppercased by Snowflake — ' +
        'always use delimited identifiers to preserve case.',
      'Use `LISTAGG` instead of `STRING_AGG`.',
      'In CTEs, use delimited identifiers for every column alias so the name survives ' +
        'into the outer query unchanged.',
    ],
  },
};

const buildDialectSection = (sqlDialect: string): string => {
  const cfg = DIALECT_CONFIGS[sqlDialect];
  if (!cfg) {
    logger.warn(
      `Unknown SQL dialect ${JSON.stringify(sqlDialect)} — no dialect-specific identifier guidance will be emitted.`,
    );
    return `### SQL Identifiers\n\nSQL dialect: **${sqlDialect}**.\n`;
  }
  const lines = [
    '### SQL Identifiers\n',
    `This project uses **${sqlDialect}** SQL dialect.`,
    `The delimited identifier character is the ${cfg.delimiter}.`,
    '**Always wrap every identifier** (column name, table name, alias) in delimited identifiers:\n',
    `- Column reference: ${cfg.col}`,
    `- Fully qualified table name: ${cfg.fqn}`,
    `- New table in CREATE TABLE (table name only, no FQN): ${cfg.newTable}`,
    '- Never mix delimiter styles within a single query.\n',
  ];
  for (const note of cfg.extra) {
    lines.push(`- ${note}`);
  }
  return lines.join('\n');
};

/** Port of `get_project_system_prompt`. */
const getProjectSystemPrompt = (sqlDialect = ''): string => {
  const base = loadBasePrompt();
  if (!sqlDialect) {
    return base;
  }
  return `${buildDialectSection(sqlDialect)}\n\n---\n\n${base}`;
};

// --- Toolset restrictions (port of `_get_toolset_restrictions`) --------------
const getToolsetRestrictions = (role: string): string | null => {
  const r = role.toLowerCase();
  if (r === 'readonly') {
    return (
      `Your Keboola user role is "${r}". ` +
      'Only read-only tools are available. ' +
      'All write operations (creating, updating, or deleting resources) are disabled.'
    );
  }
  if (!r || r === 'unknown') {
    return 'Your Keboola user role is unknown. You can manage flows but cannot set their schedules.';
  }
  if (r !== 'admin' && r !== 'share') {
    return `Your Keboola user role is "${r}". You can manage flows but cannot set their schedules.`;
  }
  return null;
};

// --- Branch context resolution (port of `_resolve_branch_context`) -----------
type BranchEntry = { id?: string | number; name?: string; isDefault?: boolean };

/**
 * Resolves the current branch's id, name, and dev-branch flag from the storage API.
 * The effective branch id is `config.branchId` (undefined on the default/production
 * branch), so we list branches and pick the matching entry or the `isDefault` one.
 */
const resolveBranchContext = async (
  config: Config,
  clients: KeboolaClients,
): Promise<[string | number, string, boolean]> => {
  const targetBranchId = config.branchId;
  const branches = (await clients.storage.branches.getDevBranches()) as BranchEntry[];

  let selected: BranchEntry | undefined;
  for (const branch of branches) {
    if (targetBranchId === undefined) {
      if (branch.isDefault === true) {
        selected = branch;
        break;
      }
    } else if (String(branch.id) === String(targetBranchId)) {
      selected = branch;
      break;
    }
  }

  if (selected === undefined) {
    // Should not happen in a healthy project, but stay defensive.
    const fallbackId: string | number = targetBranchId !== undefined ? targetBranchId : 'default';
    return [fallbackId, 'unknown', targetBranchId !== undefined];
  }

  const branchId = selected.id ?? (targetBranchId !== undefined ? targetBranchId : 'default');
  const branchName = selected.name ?? 'unknown';
  const isDevelopmentBranch = selected.isDefault !== true;
  return [branchId, branchName, isDevelopmentBranch];
};

/** Registers the project tools (Plan §4). Ported from tools/project.py. */
export const registerProjectTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'update_project_description',
    title: 'Update project description',
    description: 'Updates the description of the current Keboola project.',
    annotations: { destructiveHint: true },
    inputSchema: {
      description: z.string().describe('The new project description text.'),
    },
    handler: async ({ description }) => {
      const clients = createKeboolaClients(config);
      await clients.storage.branches.saveDevBranchMetadata(clients.branchId, [
        { key: MetadataField.PROJECT_DESCRIPTION, value: description },
      ]);
      logger.info('Project description updated successfully.');
      return { message: 'Project description updated successfully.' };
    },
  });

  registerTool(server, {
    name: 'get_project_info',
    title: 'Get project info',
    description:
      'Retrieves structured information about the current project, ' +
      'including essential context and base instructions for working with it ' +
      '(e.g., transformations, components, workflows, and dependencies).\n\n' +
      'Always call this tool at least once at the start of a conversation ' +
      'to establish the project context before using other tools.',
    annotations: { readOnlyHint: true },
    handler: async () => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const tokenData = (await clients.storage.tokens.verify()) as Record<string, unknown>;
      const projectData = (tokenData.owner ?? {}) as Record<string, unknown>;
      const projectId = (projectData.id ?? '') as string | number;
      const projectName = (projectData.name ?? '') as string;

      const organizationData = (tokenData.organization ?? {}) as Record<string, unknown>;
      const organizationId = (organizationData.id ?? '') as string | number;

      const adminData = (tokenData.admin ?? {}) as Record<string, unknown>;
      const userRole = (adminData.role as string) || 'unknown';

      const metadata = (await clients.storage.branches.getDevBranchMetadata(clients.branchId)) as {
        key: string;
        value: string;
      }[];
      const description =
        metadata.find((item) => item.key === MetadataField.PROJECT_DESCRIPTION)?.value ?? '';

      // Resolve sql_dialect + workspace_id via the shared WorkspaceManager, which finds the
      // MCP read-only workspace (by configured schema or branch metadata) and creates one
      // when absent — matching the Python get_project_info / query_data behavior.
      const workspaceManager = await createWorkspaceManager(config);
      const sqlDialect = await workspaceManager.getSqlDialect();
      const workspaceId = await workspaceManager.getWorkspaceId();

      const projectFeatures = (projectData.features ?? {}) as Record<string, unknown> | unknown[];
      const conditionalFlows = Array.isArray(projectFeatures)
        ? !projectFeatures.includes('hide-conditional-flows')
        : !('hide-conditional-flows' in projectFeatures);
      const links: Link[] = linksManager.getProjectLinks();

      const [branchId, branchName, isDevelopmentBranch] = await resolveBranchContext(
        config,
        clients,
      );

      logger.info('Returning unified project info.');
      return {
        project_id: projectId,
        project_name: projectName,
        project_description: description,
        organization_id: organizationId,
        sql_dialect: sqlDialect,
        workspace_id: workspaceId,
        conditional_flows: conditionalFlows,
        links,
        branch_id: branchId,
        branch_name: branchName,
        is_development_branch: isDevelopmentBranch,
        user_role: userRole,
        toolset_restrictions: getToolsetRestrictions(userRole),
        llm_instruction: getProjectSystemPrompt(sqlDialect),
      };
    },
  });
};
