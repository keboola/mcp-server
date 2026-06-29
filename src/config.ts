/**
 * Server configuration.
 *
 * Port of the Python `keboola_mcp_server.config.Config`. Values are resolved from
 * a string map (CLI args, `KBC_*` env vars, or `X-*` HTTP headers) by normalizing
 * keys: lowercased with `_`/`-` removed, then matched against the field name, the
 * `KBC_`-prefixed name, and the `X-`-prefixed name (in that order).
 */

export type ConfigFields = {
  /** URL to the Storage API. */
  storageApiUrl?: string;
  /** Token to access the Storage API. */
  storageToken?: string;
  /** Branch ID to access the Storage API. */
  branchId?: string;
  /** Workspace schema for buckets/tables and SQL queries. */
  workspaceSchema?: string;
  /** OAuth client ID registered in the Keboola OAuth server. */
  oauthClientId?: string;
  /** OAuth client secret registered in the Keboola OAuth server. */
  oauthClientSecret?: string;
  /** URL of the OAuth server to authenticate with. */
  oauthServerUrl?: string;
  /** OAuth scope to request. */
  oauthScope?: string;
  /** URL where the MCP server is reachable. */
  mcpServerUrl?: string;
  /** Secret key for encoding/decoding JWT tokens. */
  jwtSecret?: string;
  /** Access token sent in the `Authorization: Bearer <token>` header. */
  bearerToken?: string;
  /** ID of the ongoing conversation (supplied via HTTP header only). */
  conversationId?: string;
  /** Comma-separated allow list of tool names (`X-Allowed-Tools` header). */
  allowedTools?: string;
  /** Comma-separated deny list of tool names (`X-Disallowed-Tools` header). */
  disallowedTools?: string;
  /** Read-only mode flag (`X-Read-Only-Mode` header). */
  readOnlyMode?: string;
};

type FieldName = keyof ConfigFields;

// Aliases accepted in addition to the canonical field name.
const FIELD_ALIASES: Partial<Record<FieldName, string[]>> = {
  storageToken: ['storageApiToken'],
};

const FIELD_NAMES: FieldName[] = [
  'storageApiUrl',
  'storageToken',
  'branchId',
  'workspaceSchema',
  'oauthClientId',
  'oauthClientSecret',
  'oauthServerUrl',
  'oauthScope',
  'mcpServerUrl',
  'jwtSecret',
  'bearerToken',
  'conversationId',
  'allowedTools',
  'disallowedTools',
  'readOnlyMode',
];

const SECRET_HINTS = ['token', 'password', 'secret'];
const BRANCH_PRODUCTION_ALIASES = new Set(['', 'none', 'null', 'default', 'production']);

/** Lowercases and strips `_`/`-` so `KBC_STORAGE_TOKEN`, `storage-token`, `storageToken` all collide. */
const normalize = (name: string): string =>
  name.toLowerCase().replaceAll('_', '').replaceAll('-', '');

const isUrlField = (name: string): boolean => name.toLowerCase().includes('url');

const isSecretField = (name: string): boolean =>
  SECRET_HINTS.some((hint) => name.toLowerCase().includes(hint));

/**
 * Reduces a URL to scheme + host. Mirrors the Python `__post_init__` amendment:
 * a bare `host/path` becomes `https://host`; localhost defaults to `http`.
 */
const amendUrl = (value: string): string => {
  let url: URL | undefined;
  try {
    url = new URL(value);
  } catch {
    url = undefined;
  }

  if (url?.host) {
    const scheme =
      url.protocol === 'http:' || url.protocol === 'https:'
        ? url.protocol.replace(':', '')
        : url.hostname.startsWith('localhost')
          ? 'http'
          : 'https';
    return `${scheme}://${url.host}`;
  }

  // No scheme: treat the first path segment as the host.
  const host = value.split('/', 1)[0];
  if (!host) {
    throw new Error(`Invalid URL: ${value}`);
  }
  const scheme = host.startsWith('localhost') ? 'http' : 'https';
  return `${scheme}://${host}`;
};

const buildLookup = (map: Record<string, string | undefined>): Map<string, string> => {
  const lookup = new Map<string, string>();
  for (const [key, value] of Object.entries(map)) {
    if (value !== undefined) {
      lookup.set(normalize(key), value);
    }
  }
  return lookup;
};

const readOptions = (map: Record<string, string | undefined>): ConfigFields => {
  const lookup = buildLookup(map);
  const options: ConfigFields = {};

  for (const field of FIELD_NAMES) {
    const candidates = [field, ...(FIELD_ALIASES[field] ?? [])];
    for (const candidate of candidates) {
      const variants = [candidate, `KBC_${candidate}`, `X-${candidate}`];
      const hit = variants.map(normalize).find((v) => lookup.has(v));
      if (hit !== undefined) {
        options[field] = lookup.get(hit);
        break;
      }
    }
  }

  return amendFields(options);
};

const amendFields = (fields: ConfigFields): ConfigFields => {
  const amended: ConfigFields = { ...fields };

  for (const field of FIELD_NAMES) {
    const value = amended[field];
    if (value && isUrlField(field)) {
      amended[field] = amendUrl(value);
    }
  }

  if (
    amended.branchId !== undefined &&
    BRANCH_PRODUCTION_ALIASES.has(amended.branchId.toLowerCase())
  ) {
    amended.branchId = undefined;
  }

  return amended;
};

export class Config {
  readonly storageApiUrl?: string;
  readonly storageToken?: string;
  readonly branchId?: string;
  readonly workspaceSchema?: string;
  readonly oauthClientId?: string;
  readonly oauthClientSecret?: string;
  readonly oauthServerUrl?: string;
  readonly oauthScope?: string;
  readonly mcpServerUrl?: string;
  readonly jwtSecret?: string;
  readonly bearerToken?: string;
  readonly conversationId?: string;
  readonly allowedTools?: string;
  readonly disallowedTools?: string;
  readonly readOnlyMode?: string;

  constructor(fields: ConfigFields = {}) {
    Object.assign(this, amendFields(fields));
  }

  /** Builds a Config from a string map (env vars, headers, or CLI args). */
  static fromMap(map: Record<string, string | undefined>): Config {
    return new Config(readOptions(map));
  }

  /** Returns a new Config with values from the map layered over this one. */
  replaceBy(map: Record<string, string | undefined>): Config {
    return new Config({ ...this.toFields(), ...readOptions(map) });
  }

  toFields(): ConfigFields {
    const fields: ConfigFields = {};
    for (const field of FIELD_NAMES) {
      fields[field] = this[field];
    }
    return fields;
  }

  /** String form with secret fields redacted. */
  toString(): string {
    const params = FIELD_NAMES.map((field) => {
      const value = this[field];
      if (!value) return `${field}=None`;
      if (isSecretField(field)) return `${field}='****'`;
      return `${field}='${value}'`;
    });
    return `Config(${params.join(', ')})`;
  }
}
