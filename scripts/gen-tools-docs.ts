/**
 * TOOLS.md generator — TypeScript port of `keboola_mcp_server.generate_tool_docs`.
 *
 * Builds the MCP server with a dummy Config, lists every registered tool over an
 * in-memory MCP client (which gives us the same zod->JSON-schema conversion used on
 * the wire), and renders `TOOLS.md` in the exact format the Python generator used.
 *
 * TS tools carry no FastMCP-style tags, so the category + tag metadata that the
 * Python doc derived from tool tags is reproduced here from a name-keyed map. The
 * map is the single source of truth for both the category grouping and the
 * `**Tags**:` lines — keep it in sync when adding/removing tools.
 *
 * Usage:
 *   tsx scripts/gen-tools-docs.ts            # write TOOLS.md
 *   tsx scripts/gen-tools-docs.ts --check    # diff against committed TOOLS.md (CI gate)
 */
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import type { ToolAnnotations } from '@modelcontextprotocol/sdk/types.js';
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { Config } from '@/config';
import { createServer } from '@/server';

const OUTPUT_PATH = resolve(import.meta.dirname, '..', 'TOOLS.md');

/** Tool name kept out of the docs (scaffold/diagnostic tool, no public category). */
const HIDDEN_TOOLS = new Set<string>(['get_server_info']);

/**
 * Category + tags per tool name, mirroring the FastMCP tags the Python tools carried.
 * Tools whose category is not one of the listed categories fall into "Other Tools"
 * (parity with the Python `OTHER_CATEGORY`), matching the data-app tools' placement.
 */
type ToolMeta = { category: string; tags: string[] };

const TOOL_META: Record<string, ToolMeta> = {
  // Component Tools
  add_config_row: { category: 'Component Tools', tags: ['components'] },
  create_config: { category: 'Component Tools', tags: ['components'] },
  create_sql_transformation: { category: 'Component Tools', tags: ['components'] },
  get_components: { category: 'Component Tools', tags: ['components'] },
  get_config_examples: { category: 'Component Tools', tags: ['components'] },
  get_configs: { category: 'Component Tools', tags: ['components'] },
  run_sync_action: { category: 'Component Tools', tags: ['components'] },
  update_config: { category: 'Component Tools', tags: ['components', 'config-diff-preview'] },
  update_config_row: { category: 'Component Tools', tags: ['components', 'config-diff-preview'] },
  update_sql_transformation: {
    category: 'Component Tools',
    tags: ['components', 'config-diff-preview'],
  },
  // Other Tools (data apps)
  create_python_js_data_app_git_credential: { category: 'Other Tools', tags: ['data-apps'] },
  delete_python_js_data_app_draft: { category: 'Other Tools', tags: ['data-apps'] },
  deploy_data_app: { category: 'Other Tools', tags: ['data-apps'] },
  get_data_apps: { category: 'Other Tools', tags: ['data-apps'] },
  modify_python_js_data_app: { category: 'Other Tools', tags: ['data-apps'] },
  modify_streamlit_data_app: {
    category: 'Other Tools',
    tags: ['config-diff-preview', 'data-apps'],
  },
  // Documentation Tools
  docs_query: { category: 'Documentation Tools', tags: ['docs'] },
  // Flow Tools
  create_conditional_flow: { category: 'Flow Tools', tags: ['flows'] },
  create_flow: { category: 'Flow Tools', tags: ['flows'] },
  get_flow_examples: { category: 'Flow Tools', tags: ['flows'] },
  get_flow_schema: { category: 'Flow Tools', tags: ['flows'] },
  get_flows: { category: 'Flow Tools', tags: ['flows'] },
  modify_flow: { category: 'Flow Tools', tags: ['config-diff-preview', 'flows'] },
  update_flow: { category: 'Flow Tools', tags: ['config-diff-preview', 'flows'] },
  // Jobs Tools
  get_jobs: { category: 'Jobs Tools', tags: ['jobs'] },
  run_job: { category: 'Jobs Tools', tags: ['jobs'] },
  // OAuth Tools
  create_oauth_url: { category: 'OAuth Tools', tags: ['oauth'] },
  // Project Tools
  get_project_info: { category: 'Project Tools', tags: ['project'] },
  update_project_description: { category: 'Project Tools', tags: ['project'] },
  // Search Tools
  find_component_id: { category: 'Search Tools', tags: ['search'] },
  search: { category: 'Search Tools', tags: ['search'] },
  // Semantic Tools
  get_semantic_context: { category: 'Semantic Tools', tags: ['semantic'] },
  get_semantic_schema: { category: 'Semantic Tools', tags: ['semantic'] },
  search_semantic_context: { category: 'Semantic Tools', tags: ['semantic'] },
  validate_semantic_query: { category: 'Semantic Tools', tags: ['semantic'] },
  // SQL Tools
  query_data: { category: 'SQL Tools', tags: ['sql'] },
  // Storage Tools
  get_buckets: { category: 'Storage Tools', tags: ['storage'] },
  get_tables: { category: 'Storage Tools', tags: ['storage'] },
  update_descriptions: { category: 'Storage Tools', tags: ['storage'] },
};

const OTHER_CATEGORY = 'Other Tools';

/**
 * Detail-section category order — the order categories first appear when the Python
 * generator walked `list_tools()`. Reproduced verbatim to keep the committed file
 * stable; tools whose category is missing here are appended in first-seen order.
 */
const DETAIL_CATEGORY_ORDER = [
  'Component Tools',
  'Other Tools',
  'Documentation Tools',
  'Flow Tools',
  'Jobs Tools',
  'OAuth Tools',
  'Project Tools',
  'Search Tools',
  'Semantic Tools',
  'SQL Tools',
  'Storage Tools',
];

type ListedTool = {
  name: string;
  description?: string;
  inputSchema?: unknown;
  annotations?: ToolAnnotations;
};

/** Lists all registered tools over an in-memory MCP client, gating bypassed. */
const listAllTools = async (): Promise<ListedTool[]> => {
  const config = new Config({
    storageApiUrl: 'https://connection.test',
    storageToken: 'tok',
  });
  // skipGating: docs must list every tool regardless of project features/role.
  const server = createServer(config, { skipGating: true });

  const client = new Client({ name: 'tools-docs-generator', version: '0.0.0' });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await Promise.all([client.connect(clientTransport), server.server.connect(serverTransport)]);

  try {
    const { tools } = await client.listTools();
    return tools as ListedTool[];
  } finally {
    await client.close();
    await server.close();
  }
};

const annotationsLabel = (annotations: ToolAnnotations | undefined): string => {
  if (!annotations) return '';
  const labels: string[] = [];
  if (annotations.readOnlyHint) labels.push('read-only');
  if (annotations.destructiveHint) labels.push('destructive');
  if (annotations.idempotentHint) labels.push('idempotent');
  return labels.length ? `\`${labels.sort().join(', ')}\`` : '';
};

const tagsLabel = (tags: string[]): string =>
  tags.length ? `\`${[...tags].sort().join(', ')}\`` : '';

const firstSentence = (text: string | undefined): string => {
  if (!text) return 'No description available.';
  return `${text.split('.')[0]}.`.trim();
};

/** GitHub-style markdown anchor (port of `_generate_anchor`). */
const anchor = (text: string): string =>
  text
    .toLowerCase()
    .replace(/[^\w\s-]/g, '')
    .replace(/\s+/g, '-');

const categoryOf = (name: string): string => TOOL_META[name]?.category ?? OTHER_CATEGORY;
const tagsOf = (name: string): string[] => TOOL_META[name]?.tags ?? [];

/** Codepoint string comparison, matching Python's `sorted` (not locale-aware). */
const cmp = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);

const byName = (a: ListedTool, b: ListedTool): number => cmp(a.name, b.name);

const render = (tools: ListedTool[]): string => {
  const docTools = tools.filter((t) => !HIDDEN_TOOLS.has(t.name));

  const byCategory = new Map<string, ListedTool[]>();
  for (const tool of docTools) {
    const cat = categoryOf(tool.name);
    (byCategory.get(cat) ?? byCategory.set(cat, []).get(cat)!).push(tool);
  }

  const out: string[] = [];

  // Header
  out.push('# Tools Documentation');
  out.push(
    'This document provides details about the tools available in the Keboola MCP server.',
  );
  out.push('');

  // Index — categories sorted by name, tools sorted by name.
  out.push('## Index');
  const indexCategories = [...byCategory.keys()].sort(cmp);
  for (const category of indexCategories) {
    const catTools = [...byCategory.get(category)!].sort(byName);
    out.push('');
    out.push(`### ${category}`);
    for (const tool of catTools) {
      out.push(`- [${tool.name}](#${anchor(tool.name)}): ${firstSentence(tool.description)}`);
    }
  }
  out.push('');
  out.push('---');

  // Detail — categories in first-seen order, tools sorted by name.
  const detailCategories = [
    ...DETAIL_CATEGORY_ORDER.filter((c) => byCategory.has(c)),
    ...[...byCategory.keys()].filter((c) => !DETAIL_CATEGORY_ORDER.includes(c)),
  ];
  for (const category of detailCategories) {
    const catTools = [...byCategory.get(category)!].sort(byName);
    out.push('');
    out.push(`# ${category}`);
    for (const tool of catTools) {
      const a = anchor(tool.name);
      out.push(`<a name="${a}"></a>`);
      out.push(`## ${tool.name}`);
      out.push(`**Annotations**: ${annotationsLabel(tool.annotations)}`);
      out.push('');
      out.push(`**Tags**: ${tagsLabel(tagsOf(tool.name))}`);
      out.push('');
      out.push('**Description**:');
      out.push('');
      out.push(tool.description ?? '');
      out.push('');
      out.push('');
      out.push('**Input JSON Schema**:');
      out.push('```json');
      out.push(JSON.stringify(tool.inputSchema ?? {}, null, 2));
      out.push('```');
      out.push('');
      out.push('---');
    }
  }

  return `${out.join('\n')}\n`;
};

const main = async (): Promise<void> => {
  const check = process.argv.includes('--check');
  const tools = await listAllTools();
  const content = render(tools);

  if (check) {
    let committed = '';
    try {
      committed = readFileSync(OUTPUT_PATH, 'utf-8');
    } catch {
      committed = '';
    }
    if (committed !== content) {
      process.stderr.write(
        'TOOLS.md is out of date. Run `npm run gen:tools-docs` and commit the result.\n',
      );
      process.exit(1);
    }
    process.stdout.write('TOOLS.md is up to date.\n');
    return;
  }

  writeFileSync(OUTPUT_PATH, content, 'utf-8');
  process.stdout.write(`Wrote ${OUTPUT_PATH} (${tools.length} tools).\n`);
};

main().catch((error) => {
  process.stderr.write(`Failed to generate TOOLS.md: ${String(error)}\n`);
  process.exit(1);
});
