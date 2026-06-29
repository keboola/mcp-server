/**
 * Local JSONPath subset used by config-based search (port of the `_clean_jsonpath_path_str`
 * / scope-selection helpers from the Python `tools/search.py`). Kept standalone so the
 * matching model in `model.ts` and the tool handlers stay focused.
 */

export type JsonValue = unknown;

export type PathNode = { path: string; value: JsonValue };

export const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Deterministic JSON stringification with sorted keys (port of SearchSpec._stringify). */
export const stringify = (value: JsonValue): string => {
  try {
    return stableStringify(value);
  } catch {
    return String(value);
  }
};

const stableStringify = (value: JsonValue): string => {
  const seen = new WeakSet<object>();
  const sort = (val: JsonValue): JsonValue => {
    if (val === null || typeof val !== 'object') return val;
    if (seen.has(val as object)) return val;
    seen.add(val as object);
    if (Array.isArray(val)) return val.map(sort);
    const out: Record<string, JsonValue> = {};
    for (const key of Object.keys(val as Record<string, JsonValue>).sort()) {
      out[key] = sort((val as Record<string, JsonValue>)[key]);
    }
    return out;
  };
  return JSON.stringify(sort(value));
};

/** Resolves a dot/bracket scope to the matching nodes (supports a single `[*]`/`[N]` step). */
export const selectScope = (root: JsonValue, scope: string): PathNode[] => {
  const normalized = scope.startsWith('$') ? scope.slice(1).replace(/^\./, '') : scope;
  if (!normalized) return [{ path: '$', value: root }];

  let nodes: PathNode[] = [{ path: '$', value: root }];
  for (const token of tokenizePath(normalized)) {
    const next: PathNode[] = [];
    for (const node of nodes) {
      if (token.type === 'wildcard') {
        next.push(...childEntries(node));
      } else if (token.type === 'index') {
        if (Array.isArray(node.value) && token.index < node.value.length) {
          next.push({ path: `${node.path}[${token.index}]`, value: node.value[token.index] });
        }
      } else {
        if (node.value && typeof node.value === 'object' && !Array.isArray(node.value)) {
          const obj = node.value as Record<string, JsonValue>;
          if (token.key in obj) {
            next.push({ path: `${node.path}.${token.key}`, value: obj[token.key] });
          }
        }
      }
    }
    nodes = next;
  }
  return nodes;
};

type PathToken =
  | { type: 'key'; key: string }
  | { type: 'index'; index: number }
  | { type: 'wildcard' };

const tokenizePath = (path: string): PathToken[] => {
  const tokens: PathToken[] = [];
  const regex = /\[([^\]]*)\]|([^.[\]]+)/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(path)) !== null) {
    if (match[1] !== undefined) {
      const inner = match[1].replace(/^['"]|['"]$/g, '');
      if (inner === '*') tokens.push({ type: 'wildcard' });
      else if (/^\d+$/.test(inner)) tokens.push({ type: 'index', index: Number(inner) });
      else tokens.push({ type: 'key', key: inner });
    } else if (match[2] !== undefined) {
      if (match[2] === '*') tokens.push({ type: 'wildcard' });
      else tokens.push({ type: 'key', key: match[2] });
    }
  }
  return tokens;
};

const childEntries = (node: PathNode): PathNode[] => {
  const { value, path } = node;
  if (Array.isArray(value)) {
    return value.map((item, i) => ({ path: `${path}[${i}]`, value: item }));
  }
  if (value && typeof value === 'object') {
    return Object.entries(value as Record<string, JsonValue>).map(([k, v]) => ({
      path: `${path}.${k}`,
      value: v,
    }));
  }
  return [];
};

/** Recursive descent (`$..*`): every descendant node with its full path. */
export const descendants = (root: JsonValue, basePath: string): PathNode[] => {
  const out: PathNode[] = [];
  const walk = (node: PathNode): void => {
    for (const child of childEntries(node)) {
      out.push(child);
      walk(child);
    }
  };
  walk({ path: basePath, value: root });
  return out;
};

/** Normalizes a path string: strips the leading `$.`/`$` and `.[` artifacts (port of _clean_jsonpath_path_str). */
export const cleanJsonPath = (path: string): string => {
  let result = path.replace(/^\$\.?/, '');
  result = result.replace(/\.\[/g, '[');
  return result;
};
