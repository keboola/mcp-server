/**
 * Model & helper layer for the component WRITE tools.
 *
 * Ported from:
 * - tools/components/sql_utils.py   (SQL split/join — note: no sqlglot reformat here)
 * - tools/components/tf_update.py   (transformation block/code structural ops)
 * - tools/components/utils.py       (param-update utils, transformation config builders,
 *                                     bucket-name cleaning, structure summary, check_suitable)
 * - tools/components/model.py       (Zod schemas for the discriminated-union update ops)
 */

import { z } from 'zod';

import {
  CONDITIONAL_FLOW_COMPONENT_ID,
  DATA_APP_COMPONENT_ID,
  ORCHESTRATOR_COMPONENT_ID,
} from '@/constants';

export const SNOWFLAKE_TRANSFORMATION_ID = 'keboola.snowflake-transformation';
export const BIGQUERY_TRANSFORMATION_ID = 'keboola.google-bigquery-transformation';
export const PYTHON_TRANSFORMATION_ID = 'keboola.python-transformation-v2';
export const R_TRANSFORMATION_ID = 'keboola.r-transformation-v2';
export const VARIABLES_COMPONENT_ID = 'keboola.variables';

/** Components for which update_config manages folder metadata. */
export const FOLDER_SUPPORTING_COMPONENT_IDS = new Set<string>([
  PYTHON_TRANSFORMATION_ID,
  R_TRANSFORMATION_ID,
]);

type JsonDict = Record<string, unknown>;

// ============================================================================
// VariableDefinition + zod schemas for update operations.
// ============================================================================

export const variableDefinitionSchema = z.object({
  name: z.string().describe('Variable name.'),
  type: z
    .enum(['string', 'vault'])
    .default('string')
    .describe('Variable type: "string" or "vault".'),
  default_value: z.string().nullish().describe('Optional default value bound at creation time.'),
});
export type VariableDefinition = z.infer<typeof variableDefinitionSchema>;

// --- Config (non-transformation) parameter updates ---
const configParamSet = z.object({
  op: z.literal('set'),
  path: z
    .string()
    .describe('JSONPath to the parameter key to set (e.g., "api_key", "database.host")'),
  value: z.any().describe('New value to set'),
});
const configParamReplace = z.object({
  op: z.literal('str_replace'),
  path: z.string().describe('JSONPath to the parameter key to modify'),
  search_for: z.string().describe('Substring to search for (non-empty)'),
  replace_with: z.string().describe('Replacement string (can be empty for deletion)'),
});
const configParamRemove = z.object({
  op: z.literal('remove'),
  path: z.string().describe('JSONPath to the parameter key to remove'),
});
const configParamListAppend = z.object({
  op: z.literal('list_append'),
  path: z.string().describe('JSONPath to the list parameter'),
  value: z.any().describe('Value to append to the list'),
});
export const configParamUpdateSchema = z.discriminatedUnion('op', [
  configParamSet,
  configParamReplace,
  configParamRemove,
  configParamListAppend,
]);
export type ConfigParamUpdate = z.infer<typeof configParamUpdateSchema>;

// --- Simplified transformation blocks ---
export const tfCodeSchema = z.object({
  name: z.string().describe('A descriptive name for the code block'),
  script: z.string().describe('The SQL script of the code block'),
});
export const tfBlockSchema = z.object({
  name: z.string().describe('A descriptive name for the code block'),
  codes: z.array(tfCodeSchema).describe('SQL code sub-blocks'),
});

const tfPosition = z.enum(['start', 'end']);

const tfAddBlock = z.object({
  op: z.literal('add_block'),
  block: tfBlockSchema.describe('The block to add'),
  position: tfPosition.default('end'),
});
const tfRemoveBlock = z.object({
  op: z.literal('remove_block'),
  block_id: z.string().describe('The ID of the block to remove'),
});
const tfRenameBlock = z.object({
  op: z.literal('rename_block'),
  block_id: z.string(),
  block_name: z.string().describe('The new name of the block'),
});
const tfAddCode = z.object({
  op: z.literal('add_code'),
  block_id: z.string(),
  code: tfCodeSchema.describe('The code to add'),
  position: tfPosition.default('end'),
});
const tfRemoveCode = z.object({
  op: z.literal('remove_code'),
  block_id: z.string(),
  code_id: z.string(),
});
const tfRenameCode = z.object({
  op: z.literal('rename_code'),
  block_id: z.string(),
  code_id: z.string(),
  code_name: z.string().describe('The new name of the code'),
});
const tfSetCode = z.object({
  op: z.literal('set_code'),
  block_id: z.string(),
  code_id: z.string(),
  script: z.string().describe('The SQL script of the code to set'),
});
const tfAddScript = z.object({
  op: z.literal('add_script'),
  block_id: z.string(),
  code_id: z.string(),
  script: z.string().describe('The SQL script to add'),
  position: tfPosition.default('end'),
});
const tfStrReplace = z
  .object({
    op: z.literal('str_replace'),
    block_id: z.string().nullish(),
    code_id: z.string().nullish(),
    search_for: z.string().describe('Substring to search for (non-empty)'),
    replace_with: z.string().describe('Replacement string (can be empty for deletion)'),
  })
  .refine((v) => !(v.block_id == null && v.code_id != null), {
    message: 'code_id must be None if block_id is None',
  });

export const tfParamUpdateSchema = z.discriminatedUnion('op', [
  tfAddBlock,
  tfRemoveBlock,
  tfRenameBlock,
  tfAddCode,
  tfRemoveCode,
  tfRenameCode,
  tfSetCode,
  tfAddScript,
  tfStrReplace,
]);
export type TfParamUpdate = z.infer<typeof tfParamUpdateSchema>;

export const STRUCTURAL_TF_OPS = new Set(['add_block', 'add_code', 'remove_block', 'remove_code']);

// ============================================================================
// SQL utilities (port of sql_utils.py split/join). sqlglot reformatting is NOT
// ported (no TS equivalent); scripts are split on statement boundaries as-is.
// ============================================================================

const SQL_SPLIT_REGEX = new RegExp(
  '\\s*(' +
    '(?:' +
    "'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'|" +
    '"[^"\\\\]*(?:\\\\.[^"\\\\]*)*"|' +
    '\\$\\$(?:(?!\\$\\$)[\\s\\S])*\\$\\$|' +
    '/\\*[^*]*\\*+(?:[^*/][^*]*\\*+)*/|' +
    '#[^\\n\\r]*|' +
    '--[^\\n\\r]*|' +
    '//[^\\n\\r]*|' +
    '/(?![*/])|' +
    '-(?!-)|' +
    '\\$(?!\\$)|' +
    '[^"\';#/$-]+' +
    ')+' +
    '(?:;|$)' +
    ')',
  'gm',
);

/** Splits a SQL script into individual statements (trimmed, non-empty). */
export const splitSqlStatements = (script: string): string[] => {
  if (!script || !script.trim()) return [];
  const matches = script.match(SQL_SPLIT_REGEX) ?? [];
  return matches.map((s) => s.trim()).filter((s) => s.length > 0);
};

/** Joins SQL statements into a single script separated by double newlines. */
export const joinSqlStatements = (statements: string[]): string => {
  if (!statements || statements.length === 0) return '';
  const parts: string[] = [];
  for (const stmt of statements) {
    const trimmed = stmt.trim();
    if (!trimmed) continue;
    parts.push(trimmed, '\n\n');
  }
  return parts.join('');
};

// ============================================================================
// Simplified <-> raw transformation parameter conversion.
// ============================================================================

export type SimplifiedBlocks = {
  blocks: { name: string; codes: { name: string; script: string }[] }[];
};
export type RawTfParameters = {
  blocks: { name: string; codes: { name: string; script: string[] }[] }[];
};

export const toRawParameters = (params: SimplifiedBlocks): RawTfParameters => ({
  blocks: params.blocks.map((block) => ({
    name: block.name,
    codes: block.codes.map((code) => ({
      name: code.name,
      script: splitSqlStatements(code.script),
    })),
  })),
});

export const toSimplifiedParameters = (raw: RawTfParameters): SimplifiedBlocks => ({
  blocks: (raw.blocks ?? []).map((block) => ({
    name: block.name,
    codes: (block.codes ?? []).map((code) => ({
      name: code.name,
      script: joinSqlStatements(
        Array.isArray(code.script) ? code.script : [String(code.script ?? '')],
      ),
    })),
  })),
});

// ============================================================================
// Transformation configuration builder (port of create_transformation_configuration).
// ============================================================================

export const cleanBucketName = (bucketName: string): string => {
  const maxBucketLength = 96;
  let name = bucketName.trim();
  // ASCII-fold (český -> cesky): NFKD then strip diacritics, drop non-ascii.
  name = name.normalize('NFKD').replace(/[̀-ͯ]/g, '');
  // eslint-disable-next-line no-control-regex
  name = name.replace(/[^\x00-\x7F]/g, '');
  name = name.replace(/\s+/g, '-');
  name = name.replace(/[^a-zA-Z0-9_-]/g, '');
  name = name.replace(/^_+/, '');
  return name.slice(0, maxBucketLength);
};

/** Builds the raw transformation configuration payload (parameters + storage). */
export const createTransformationConfiguration = (
  codes: { name: string; script: string }[],
  transformationName: string,
  outputTables: string[],
): JsonDict => {
  const rawParameters = toRawParameters({ blocks: [{ name: 'Blocks', codes }] });

  const storage: JsonDict = {
    input: { tables: [] as JsonDict[] },
    output: { tables: [] as JsonDict[] },
  };

  if (outputTables.length > 0) {
    const bucketName = cleanBucketName(transformationName);
    const destination = `out.c-${bucketName}`;
    (storage.output as JsonDict).tables = outputTables.map((outTable) => ({
      source: outTable,
      destination: `${destination}.${outTable}`,
    }));
  }

  return { parameters: rawParameters, storage };
};

// ============================================================================
// JSONPath-free param update utilities (port of utils.py).
// ============================================================================

/** Sets a value in a nested dict using a dot-separated path; creates intermediate dicts. */
export const setNestedValue = (data: JsonDict, path: string, value: unknown): void => {
  const keys = path.split('.');
  let current: JsonDict = data;
  for (let i = 0; i < keys.length - 1; i++) {
    const key = keys[i]!;
    if (!(key in current)) current[key] = {};
    const next = current[key];
    if (typeof next !== 'object' || next === null || Array.isArray(next)) {
      const pathSoFar = keys.slice(0, i + 1).join('.');
      throw new Error(
        `Cannot set nested value at path "${path}": encountered non-dict value at "${pathSoFar}".`,
      );
    }
    current = next as JsonDict;
  }
  current[keys[keys.length - 1]!] = value;
};

/**
 * Resolves a simple dot/bracket path to the parent container + final key.
 * Supports `a.b`, `a.b[2]`, `array[1]`, and quoted segments like `"#secret"`.
 * Returns null when an intermediate node is missing.
 */
type PathRef = { parent: JsonDict | unknown[]; key: string | number; exists: boolean };

const parsePathSegments = (path: string): (string | number)[] => {
  const segments: (string | number)[] = [];
  for (const rawSeg of path.split('.')) {
    const seg = rawSeg;
    // Strip surrounding quotes from a quoted field name.
    if ((seg.startsWith('"') && seg.endsWith('"')) || (seg.startsWith("'") && seg.endsWith("'"))) {
      segments.push(seg.slice(1, -1));
      continue;
    }
    // Split out bracket indices: name[0][1]
    const bracketRe = /\[(\d+)\]/g;
    const name = seg.replace(/\[\d+\]/g, '');
    if (name) segments.push(name);
    let m: RegExpExecArray | null;
    while ((m = bracketRe.exec(seg)) !== null) {
      segments.push(Number(m[1]));
    }
  }
  return segments;
};

const resolveRef = (root: JsonDict, path: string, create: boolean): PathRef | null => {
  const segments = parsePathSegments(path);
  if (segments.length === 0) return null;
  let current: JsonDict | unknown[] = root;
  for (let i = 0; i < segments.length - 1; i++) {
    const seg = segments[i]!;
    const next = Array.isArray(current)
      ? current[seg as number]
      : (current as JsonDict)[seg as string];
    if (next === undefined || next === null) {
      if (!create) return null;
      const child: JsonDict = {};
      if (Array.isArray(current)) (current as unknown[])[seg as number] = child;
      else (current as JsonDict)[seg as string] = child;
      current = child;
    } else {
      current = next as JsonDict | unknown[];
    }
  }
  const finalKey = segments[segments.length - 1]!;
  const exists = Array.isArray(current)
    ? (finalKey as number) < current.length
    : finalKey in (current as JsonDict);
  return { parent: current, key: finalKey, exists };
};

const getRefValue = (ref: PathRef): unknown =>
  Array.isArray(ref.parent)
    ? ref.parent[ref.key as number]
    : (ref.parent as JsonDict)[ref.key as string];

const setRefValue = (ref: PathRef, value: unknown): void => {
  if (Array.isArray(ref.parent)) ref.parent[ref.key as number] = value;
  else (ref.parent as JsonDict)[ref.key as string] = value;
};

const deleteRef = (ref: PathRef): void => {
  if (Array.isArray(ref.parent)) ref.parent.splice(ref.key as number, 1);
  else delete (ref.parent as JsonDict)[ref.key as string];
};

const applyParamUpdate = (params: JsonDict, update: ConfigParamUpdate): JsonDict => {
  // `$` targets the whole parameters object.
  if (update.path === '$') {
    if (update.op === 'set') return update.value as JsonDict;
  }

  if (update.op === 'set') {
    setNestedValue(params, update.path, update.value);
    return params;
  }

  if (update.op === 'str_replace') {
    if (!update.search_for) throw new Error('Search string is empty');
    if (update.search_for === update.replace_with) {
      throw new Error(`Search string and replace string are the same: "${update.search_for}"`);
    }
    const ref = resolveRef(params, update.path, false);
    if (!ref || !ref.exists) throw new Error(`Path "${update.path}" does not exist`);
    const value = getRefValue(ref);
    let replaceCnt = 0;
    if (typeof value === 'string') {
      const occ = value.split(update.search_for).length - 1;
      if (occ) {
        replaceCnt += occ;
        setRefValue(ref, value.split(update.search_for).join(update.replace_with));
      }
    } else if (Array.isArray(value)) {
      if (!value.every((item) => typeof item === 'string')) {
        throw new Error(`Path "${update.path}" is not a string or list of strings`);
      }
      const newValue = value.map((item) => {
        const s = item as string;
        const occ = s.split(update.search_for).length - 1;
        replaceCnt += occ;
        return occ ? s.split(update.search_for).join(update.replace_with) : s;
      });
      setRefValue(ref, newValue);
    } else {
      throw new Error(`Path "${update.path}" is not a string or list of strings`);
    }
    if (replaceCnt === 0) {
      throw new Error(`Search string "${update.search_for}" not found in path "${update.path}"`);
    }
    return params;
  }

  if (update.op === 'remove') {
    const ref = resolveRef(params, update.path, false);
    if (!ref || !ref.exists) throw new Error(`Path "${update.path}" does not exist`);
    deleteRef(ref);
    return params;
  }

  if (update.op === 'list_append') {
    const ref = resolveRef(params, update.path, false);
    if (!ref || !ref.exists) throw new Error(`Path "${update.path}" does not exist`);
    const value = getRefValue(ref);
    if (!Array.isArray(value)) throw new Error(`Path "${update.path}" is not a list`);
    value.push(update.value);
    return params;
  }

  return params;
};

/** Applies a list of parameter updates to a deep copy of `params`. */
export const updateParams = (params: JsonDict, updates: ConfigParamUpdate[]): JsonDict => {
  let result = structuredClone(params);
  for (const update of updates) {
    result = applyParamUpdate(result, update);
  }
  return result;
};

// ============================================================================
// Transformation structural updates (port of tf_update.py).
// ============================================================================

type TfBlock = { id?: string; name: string; codes: TfCode[] };
type TfCode = { id?: string; name: string; script: string };
type TfParams = { blocks: TfBlock[] };

/** Numbers blocks (b0, b1…) and codes (b0.c0…). */
export const addIds = (params: TfParams): TfParams => {
  params.blocks.forEach((block, bidx) => {
    block.id = `b${bidx}`;
    block.codes.forEach((code, cidx) => {
      code.id = `b${bidx}.c${cidx}`;
    });
  });
  return params;
};

const findBlock = (params: TfParams, blockId: string): TfBlock | undefined =>
  params.blocks.find((b) => b.id === blockId);

const findCode = (block: TfBlock | undefined, codeId: string): TfCode | undefined =>
  block?.codes.find((c) => c.id === codeId);

const applyTfUpdate = (params: TfParams, op: TfParamUpdate): [TfParams, string] => {
  switch (op.op) {
    case 'add_block': {
      if (!op.block.name.trim()) throw new Error('Invalid operation: block name cannot be empty');
      const newBlock: TfBlock = {
        name: op.block.name,
        codes: op.block.codes.map((c) => ({ ...c })),
      };
      if (op.position === 'start') params.blocks.unshift(newBlock);
      else params.blocks.push(newBlock);
      return [params, `Added block with name "${op.block.name}"`];
    }
    case 'remove_block': {
      const idx = params.blocks.findIndex((b) => b.id === op.block_id);
      if (idx === -1) throw new Error(`Block with id '${op.block_id}' does not exist`);
      params.blocks.splice(idx, 1);
      return [params, ''];
    }
    case 'rename_block': {
      if (!op.block_name.trim()) throw new Error('Invalid operation: block name cannot be empty');
      const block = findBlock(params, op.block_id);
      if (!block) throw new Error(`Block with id '${op.block_id}' does not exist`);
      block.name = op.block_name;
      return [params, ''];
    }
    case 'add_code': {
      if (!op.code.name.trim()) throw new Error('Invalid operation: code name cannot be empty');
      const block = findBlock(params, op.block_id);
      if (!block) throw new Error(`Block with id '${op.block_id}' does not exist`);
      const newCode: TfCode = { name: op.code.name, script: op.code.script };
      if (op.position === 'start') block.codes.unshift(newCode);
      else block.codes.push(newCode);
      return [params, `Added code with name "${op.code.name}"`];
    }
    case 'remove_code': {
      const block = findBlock(params, op.block_id);
      const code = findCode(block, op.code_id);
      if (!block || !code) {
        throw new Error(`Code with id '${op.code_id}' in block '${op.block_id}' does not exist`);
      }
      block.codes.splice(block.codes.indexOf(code), 1);
      return [params, ''];
    }
    case 'rename_code': {
      if (!op.code_name.trim()) throw new Error('Invalid operation: code name cannot be empty');
      const block = findBlock(params, op.block_id);
      const code = findCode(block, op.code_id);
      if (!code) {
        throw new Error(`Code with id '${op.code_id}' in block '${op.block_id}' does not exist`);
      }
      code.name = op.code_name;
      return [params, ''];
    }
    case 'set_code': {
      if (!op.script.trim()) throw new Error('Invalid operation: script cannot be empty');
      const block = findBlock(params, op.block_id);
      const code = findCode(block, op.code_id);
      if (!code) {
        throw new Error(`Code with id '${op.code_id}' in block '${op.block_id}' does not exist`);
      }
      code.script = op.script;
      return [params, `Changed code with id '${op.code_id}' in block '${op.block_id}'`];
    }
    case 'add_script': {
      if (!op.script.trim()) throw new Error('Invalid operation: script cannot be empty');
      const block = findBlock(params, op.block_id);
      const code = findCode(block, op.code_id);
      if (!code) {
        throw new Error(`Code with id '${op.code_id}' in block '${op.block_id}' does not exist`);
      }
      const current = code.script;
      code.script =
        op.position === 'start'
          ? current
            ? `${op.script} ${current}`
            : op.script
          : current
            ? `${current} ${op.script}`
            : op.script;
      return [params, `Added script to code with id '${op.code_id}' in block '${op.block_id}'`];
    }
    case 'str_replace': {
      if (!op.search_for) throw new Error('Invalid operation: search string is empty');
      if (op.search_for === op.replace_with) {
        throw new Error(
          `Invalid operation: search string and replace string are the same: "${op.search_for}"`,
        );
      }
      let targets: TfCode[];
      let scope: string;
      if (op.block_id == null) {
        targets = params.blocks.flatMap((b) => b.codes);
        scope = 'the transformation';
      } else if (op.code_id == null) {
        const block = findBlock(params, op.block_id);
        targets = block ? block.codes : [];
        scope = `block "${op.block_id}"`;
      } else {
        const block = findBlock(params, op.block_id);
        const code = findCode(block, op.code_id);
        targets = code ? [code] : [];
        scope = `code "${op.code_id}", block "${op.block_id}"`;
      }
      if (targets.length === 0) throw new Error(`No scripts found in ${scope}`);
      let replaceCnt = 0;
      for (const code of targets) {
        if (code.script.includes(op.search_for)) {
          replaceCnt += code.script.split(op.search_for).length - 1;
          code.script = code.script.split(op.search_for).join(op.replace_with);
        }
      }
      if (replaceCnt === 0) {
        throw new Error(`Search string "${op.search_for}" not found in ${scope}`);
      }
      const word = replaceCnt === 1 ? 'occurrence' : 'occurrences';
      return [params, `Replaced ${replaceCnt} ${word} of "${op.search_for}" in ${scope}`];
    }
    default:
      return [params, ''];
  }
};

/** Markdown summary of a transformation's block/code structure. */
export const structureSummary = (params: TfParams): string => {
  const lines = ['## Updated Transformation Structure', ''];
  const blocks = params.blocks ?? [];
  if (blocks.length === 0) {
    return '## Updated Transformation Structure\n\nNo blocks found in transformation.\n';
  }
  for (const block of blocks) {
    lines.push(`### Block id: \`${block.id}\`, name: \`${block.name ?? ''}\``, '');
    const codes = block.codes ?? [];
    if (codes.length === 0) {
      lines.push('*No code blocks*', '');
      continue;
    }
    for (const code of codes) {
      lines.push(`- **Code id: \`${code.id}\`, name: \`${code.name ?? ''}\`** SQL snippet:`, '');
      const script = code.script ?? '';
      if (script) {
        let snippet = script.trim();
        if (snippet.length > 150) {
          const truncated = snippet.length - 150;
          snippet = `${snippet.slice(0, 150)}... (${truncated} chars truncated)`;
        }
        lines.push('  ```sql', `  ${snippet}`, '  ```');
      } else {
        lines.push('  *Empty script*');
      }
      lines.push('');
    }
  }
  return lines.join('\n');
};

/**
 * Applies transformation parameter updates to a simplified-blocks structure.
 * Returns the updated simplified blocks and a change summary.
 */
export const updateTransformationParameters = (
  parameters: SimplifiedBlocks,
  updates: TfParamUpdate[],
): [SimplifiedBlocks, string] => {
  const isStructureChange = updates.some((u) => STRUCTURAL_TF_OPS.has(u.op));
  let paramsDict = addIds(structuredClone(parameters) as TfParams);
  const messages: string[] = [];
  for (const update of updates) {
    const [updated, message] = applyTfUpdate(paramsDict, update);
    paramsDict = updated;
    if (message) messages.push(message);
  }
  if (isStructureChange) {
    paramsDict = addIds(paramsDict);
    messages.push(structureSummary(paramsDict));
  }
  // Strip ids back out for the simplified shape (extra='ignore').
  const simplified: SimplifiedBlocks = {
    blocks: paramsDict.blocks.map((b) => ({
      name: b.name,
      codes: b.codes.map((c) => ({ name: c.name, script: c.script })),
    })),
  };
  return [simplified, messages.join('\n')];
};

// ============================================================================
// check_suitable (port of utils.py).
// ============================================================================

const UNSUITABLE_COMPONENTS_MESSAGES: Record<string, string> = {
  [DATA_APP_COMPONENT_ID]: 'Use the data applications tools.',
  [CONDITIONAL_FLOW_COMPONENT_ID]: 'Use the flows tools.',
  [ORCHESTRATOR_COMPONENT_ID]: 'Use the flows tools.',
  [BIGQUERY_TRANSFORMATION_ID]: 'Use the SQL transformation tools.',
  [SNOWFLAKE_TRANSFORMATION_ID]: 'Use the SQL transformation tools.',
};

export const checkSuitable = (toolName: string, componentId: string): void => {
  const message = UNSUITABLE_COMPONENTS_MESSAGES[componentId];
  if (message) {
    throw new Error(
      `The "${toolName}" tool cannot be used with ${componentId} component. ${message}`,
    );
  }
};

export const getSqlTransformationIdFromSqlDialect = (sqlDialect: string): string => {
  const d = sqlDialect.toLowerCase();
  if (d === 'snowflake') return SNOWFLAKE_TRANSFORMATION_ID;
  if (d === 'bigquery') return BIGQUERY_TRANSFORMATION_ID;
  throw new Error(`Unsupported SQL dialect: ${sqlDialect}`);
};

// ============================================================================
// Folder hint helpers (port of utils.py build_folder_hint / folder_field_description).
// ============================================================================

export const folderFieldDescription = (singular: string, plural: string): string =>
  `Folder name to organize this ${singular} in the Keboola UI. ` +
  `Pass an empty string to remove an existing folder assignment. ` +
  `Existing folder names are returned in the response change_summary when no folder is provided ` +
  `and there are 20 or more ${plural} in the project. ` +
  `If there are 20 or more ${plural}, you should assign one of the existing folders or ` +
  `create a new one that clearly reflects the ${singular} purpose.`;

export const buildFolderHint = (
  total: number,
  existingFolders: string[],
  configLabel: string,
  updateTool: string,
  lowerBound = false,
): string | null => {
  if (total < 20) return null;
  const countStr = lowerBound ? `at least ${total}` : String(total);
  let hint = `Note: This project already has ${countStr} ${configLabel}. Consider organizing them with folders. `;
  if (existingFolders.length > 0) {
    hint +=
      `Existing folders: ${existingFolders.join(', ')}. ` +
      `Call ${updateTool} with a folder= parameter to assign this to one.`;
  } else {
    hint += `No folders have been created yet. Call ${updateTool} with a folder= parameter to start organizing.`;
  }
  return hint;
};
