// Ported from tools/storage/tools.py (BucketDetail, TableSummary, TableDetail, TableColumnInfo)
// and tools/storage_helpers.py / clients/client.py metadata accessors. The dialect-aware FQN
// builder is a port of workspace.py `_SnowflakeWorkspace.get_table_info` /
// `_BigQueryWorkspace.get_table_info`.

import { MetadataField } from '@/constants';
import type { Link } from '@/links';
import { type ComponentUsageReference, getCreatedBy, getLastUpdatedBy } from './usage';

// ---------------------------------------------------------------------------
// Metadata helpers (ports of clients/client.py get_metadata_property and
// tools/components/utils.py get_nested, utils.py parse_iso_timestamp).
// ---------------------------------------------------------------------------

export type RawObj = Record<string, unknown>;

const FAKE_DEVELOPMENT_BRANCH = 'KBC.fakeDevelopmentBranch';
const SHARED_DESCRIPTION = 'KBC.sharedDescription';
const DATATYPE_BASETYPE = 'KBC.datatype.basetype';
const DATATYPE_TYPE = 'KBC.datatype.type';
const DATATYPE_NULLABLE = 'KBC.datatype.nullable';

/** Parse an ISO 8601 timestamp into epoch millis, accepting `Z` and `+HHMM` offsets. */
export const parseIsoTimestamp = (ts: string): number => {
  const normalized = ts.replace('Z', '+00:00').replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
  const millis = Date.parse(normalized);
  if (Number.isNaN(millis)) throw new Error(`Invalid ISO timestamp: ${ts}`);
  return millis;
};

/** Port of get_metadata_property: most-recent value for `key`, optionally provider-ranked. */
export const getMetadataProperty = (
  metadata: unknown,
  key: string,
  preferredProviders?: string[],
): string | null => {
  if (!Array.isArray(metadata)) return null;
  const filtered = (metadata as RawObj[]).filter((m) => m && m.key === key);
  if (filtered.length === 0) return null;
  const sortKey = (m: RawObj): [number, string] => {
    const ts = (m.timestamp as string) ?? '';
    if (preferredProviders) {
      const p = m.provider as string | undefined;
      const idx =
        p && preferredProviders.includes(p)
          ? preferredProviders.indexOf(p)
          : preferredProviders.length;
      return [-1 * idx, ts];
    }
    return [0, ts];
  };
  let best: RawObj | undefined;
  let bestKey: [number, string] | undefined;
  for (const m of filtered) {
    const k = sortKey(m);
    if (!bestKey || k[0] > bestKey[0] || (k[0] === bestKey[0] && k[1] > bestKey[1])) {
      best = m;
      bestKey = k;
    }
  }
  const value = best?.value;
  return value != null ? String(value) : null;
};

/** Port of get_nested: dot-path lookup through nested objects. */
export const getNested = (obj: unknown, path: string): unknown => {
  let cur: unknown = obj;
  for (const part of path.split('.')) {
    if (cur && typeof cur === 'object' && !Array.isArray(cur)) {
      cur = (cur as RawObj)[part];
    } else {
      return undefined;
    }
    if (cur == null) return undefined;
  }
  return cur;
};

/** Most recent of the given ISO timestamps (string-preserving), or null. */
export const maxTimestamp = (...timestamps: (string | null | undefined)[]): string | null => {
  const valid = timestamps.filter((ts): ts is string => Boolean(ts));
  if (valid.length === 0) return null;
  const score = (ts: string): [number, number | string] => {
    try {
      return [1, parseIsoTimestamp(ts)];
    } catch {
      return [0, ts];
    }
  };
  let best = valid[0]!;
  let bestScore = score(best);
  for (const ts of valid.slice(1)) {
    const s = score(ts);
    if (s[0] > bestScore[0] || (s[0] === bestScore[0] && s[1] > bestScore[1])) {
      best = ts;
      bestScore = s;
    }
  }
  return best;
};

const asNumberOrNull = (value: unknown): number | null => {
  if (value == null) return null;
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
};

// ---------------------------------------------------------------------------
// SQL dialect + fully-qualified-name / quoting helpers (port of workspace.py
// `get_backend_path`, `TableFqn`, and the per-backend `get_table_info`).
//
// The project backend is resolved from the verified token's `owner.defaultBackend`
// (see tools.ts), avoiding a live workspace round-trip.
// ---------------------------------------------------------------------------

export type Dialect = 'snowflake' | 'bigquery';

/** Quote a single identifier for the given dialect. */
export const quotedName = (name: string, dialect: Dialect): string =>
  dialect === 'bigquery' ? `\`${name}\`` : `"${name}"`;

/** Default native type for a column with no `KBC.datatype.type` metadata. */
const defaultNativeType = (dialect: Dialect): string =>
  dialect === 'bigquery' ? 'STRING' : 'VARCHAR';

const getBackendPath = (rawTable: RawObj): string[] | null => {
  const bucket = rawTable.bucket;
  const backendPath =
    bucket && typeof bucket === 'object' ? (bucket as RawObj).backendPath : undefined;
  return Array.isArray(backendPath) ? (backendPath as string[]) : null;
};

/**
 * Build a table's fully qualified name from its bucket backendPath, dialect-aware.
 *
 * - Snowflake: database.schema.table → `"db"."schema"."name"` (port of
 *   `_SnowflakeWorkspace.get_table_info`; requires backendPath length >= 2).
 * - BigQuery: dataset.table → `` `dataset`.`name` `` (port of
 *   `_BigQueryWorkspace.get_table_info`). There is no cross-project (database) tier,
 *   backendPath[0] is the dataset name (separators normalized to `_`), and a table that
 *   is an alias in its source project is not materialized into this dataset → no FQN.
 *
 * Returns null when no FQN can be constructed (the table is then not queryable).
 */
export const tableFqn = (rawTable: RawObj, dialect: Dialect): string | null => {
  const name = String(rawTable.name ?? '');
  if (dialect === 'bigquery') {
    const sourceTable = rawTable.sourceTable as RawObj | undefined;
    if (sourceTable && sourceTable.isAlias) return null;
    const bp = getBackendPath(rawTable);
    if (!bp || bp.length < 1) return null;
    const dataset = String(bp[0]).replace(/[.-]/g, '_');
    return [dataset, name].map((p) => quotedName(p, dialect)).join('.');
  }
  const bp = getBackendPath(rawTable);
  if (!bp || bp.length < 2) return null;
  return [bp[0], bp[1], name].map((p) => quotedName(String(p), dialect)).join('.');
};

// ---------------------------------------------------------------------------
// Bucket / table models (ports of BucketDetail, TableSummary, TableDetail).
// ---------------------------------------------------------------------------

export type Bucket = {
  id: string;
  name: string;
  displayName: string;
  description: string | null;
  stage: string;
  created: string;
  updated: string | null;
  dataSizeBytes: number | null;
  tablesCount: number | null;
  links: Link[] | null;
  source_project: string | null;
  created_by: ComponentUsageReference | null;
  last_updated_by: ComponentUsageReference | null;
  // internal, excluded from output
  branch_id: string | null;
  prod_id: string;
};

export const validateBucket = (raw: RawObj): Bucket => {
  const id = String(raw.id ?? '');
  const metadata = raw.metadata;

  const branchId = getMetadataProperty(metadata, FAKE_DEVELOPMENT_BRANCH);
  const prodId = branchId ? id.replace(`c-${branchId}-`, 'c-') : id;

  const description =
    getMetadataProperty(metadata, SHARED_DESCRIPTION) ||
    getMetadataProperty(metadata, MetadataField.DESCRIPTION) ||
    (raw.description as string | undefined) ||
    null;

  const tables = raw.tables;
  const tablesCount = Array.isArray(tables) ? tables.length : null;

  let sourceProject: string | null = null;
  const sp = getNested(raw, 'sourceBucket.project') as RawObj | undefined;
  if (sp) sourceProject = `${sp.name} (ID: ${sp.id})`;

  const updated = (raw.updated as string | undefined) || maxTimestamp(raw.lastChangeDate as string);

  return {
    id,
    name: String(raw.name ?? ''),
    displayName: String(raw.displayName ?? raw.display_name ?? ''),
    description: description || null,
    stage: String(raw.stage ?? ''),
    created: String(raw.created ?? ''),
    updated: updated || null,
    dataSizeBytes: asNumberOrNull(raw.dataSizeBytes),
    tablesCount,
    links: null,
    source_project: sourceProject,
    created_by: null,
    last_updated_by: null,
    branch_id: branchId || null,
    prod_id: prodId,
  };
};

/** Port of BucketDetail.with_lineage_metadata. */
export const withBucketLineage = (bucket: Bucket, raw: RawObj): Bucket => {
  const metadata = raw.metadata;
  if (!Array.isArray(metadata) || metadata.length === 0) return bucket;
  const lastUpdatedBy = getLastUpdatedBy(metadata);
  return {
    ...bucket,
    created_by: getCreatedBy(metadata),
    last_updated_by: lastUpdatedBy,
    updated: maxTimestamp(bucket.updated, lastUpdatedBy?.timestamp ?? null),
  };
};

export type TableColumnInfo = {
  name: string;
  quotedName: string;
  database_native_type: string;
  nullable: boolean;
  keboola_base_type: string | null;
  description: string | null;
};

export type Table = {
  id: string;
  name: string;
  displayName: string;
  description: string | null;
  primaryKey: string | null; // serialized as '|'-joined string (port of serialize_primary_key)
  created: string | null;
  updated: string | null;
  rowsCount: number | null;
  dataSizeBytes: number | null;
  links: Link[] | null;
  source_project: string | null;
  // detail-only fields (absent on summaries)
  columns?: TableColumnInfo[] | null;
  fullyQualifiedName?: string | null;
  used_by?: ComponentUsageReference[] | null;
  created_by?: ComponentUsageReference | null;
  last_updated_by?: ComponentUsageReference | null;
  // internal
  branch_id: string | null;
  prod_id: string;
  isDetail: boolean;
};

export const validateTableCommon = (raw: RawObj): Omit<Table, 'isDetail'> => {
  const id = String(raw.id ?? '');
  const metadata = raw.metadata;

  const branchId = getMetadataProperty(metadata, FAKE_DEVELOPMENT_BRANCH);
  const prodId = branchId ? id.replace(`c-${branchId}-`, 'c-') : id;

  const description =
    getMetadataProperty(metadata, MetadataField.DESCRIPTION) ||
    getMetadataProperty(getNested(raw, 'sourceTable.metadata') ?? [], MetadataField.DESCRIPTION) ||
    (raw.description as string | undefined) ||
    null;

  let sourceProject: string | null = null;
  const sp = getNested(raw, 'sourceTable.project') as RawObj | undefined;
  if (sp) sourceProject = `${sp.name} (ID: ${sp.id})`;

  const updated =
    (raw.updated as string | undefined) ||
    maxTimestamp(raw.lastChangeDate as string, raw.lastImportDate as string);

  const pk = raw.primaryKey;
  const primaryKey = Array.isArray(pk) && pk.length ? (pk as string[]).join('|') : null;

  return {
    id,
    name: String(raw.name ?? ''),
    displayName: String(raw.displayName ?? raw.display_name ?? ''),
    description: description || null,
    primaryKey,
    created: (raw.created as string | undefined) ?? null,
    updated: updated || null,
    rowsCount: asNumberOrNull(raw.rowsCount),
    dataSizeBytes: asNumberOrNull(raw.dataSizeBytes),
    links: null,
    source_project: sourceProject,
    branch_id: branchId || null,
    prod_id: prodId,
  };
};

export const withTableLineage = (table: Table, raw: RawObj): Table => {
  const metadata = raw.metadata;
  if (!Array.isArray(metadata) || metadata.length === 0) return table;
  const lastUpdatedBy = getLastUpdatedBy(metadata);
  return {
    ...table,
    created_by: getCreatedBy(metadata),
    last_updated_by: lastUpdatedBy,
    updated: maxTimestamp(table.updated, lastUpdatedBy?.timestamp ?? null),
  };
};

/**
 * Build the detail column listing for a table (port of the per-column loop in `_get_table`).
 * Native type defaults are dialect-aware (Snowflake VARCHAR, BigQuery STRING).
 */
export const buildTableColumns = (rawTable: RawObj, dialect: Dialect): TableColumnInfo[] => {
  const rawColumns = Array.isArray(rawTable.columns) ? (rawTable.columns as string[]) : [];
  const columnMetadata = (rawTable.columnMetadata as Record<string, unknown>) ?? {};
  const sourceColumnMetadata =
    (getNested(rawTable, 'sourceTable.columnMetadata') as Record<string, unknown>) ?? {};

  return rawColumns.map((colName) => {
    const colMeta = columnMetadata[colName] ?? [];
    const srcMeta = sourceColumnMetadata[colName] ?? [];

    const description =
      getMetadataProperty(colMeta, MetadataField.DESCRIPTION) ||
      getMetadataProperty(srcMeta, MetadataField.DESCRIPTION) ||
      null;
    const baseType =
      getMetadataProperty(colMeta, DATATYPE_BASETYPE, ['user']) ||
      getMetadataProperty(srcMeta, DATATYPE_BASETYPE, ['user']) ||
      null;
    let nativeType =
      getMetadataProperty(colMeta, DATATYPE_TYPE) || getMetadataProperty(srcMeta, DATATYPE_TYPE);
    const nullableStr =
      getMetadataProperty(colMeta, DATATYPE_NULLABLE) ||
      getMetadataProperty(srcMeta, DATATYPE_NULLABLE);

    if (nativeType === null) {
      nativeType = defaultNativeType(dialect);
    }
    const nullable =
      nullableStr != null ? ['1', 'true'].includes(String(nullableStr).toLowerCase()) : false;

    return {
      name: colName,
      quotedName: quotedName(colName, dialect),
      database_native_type: nativeType,
      nullable,
      keboola_base_type: baseType,
      description,
    };
  });
};

// Strip internal/absent fields before emitting (mirrors pydantic exclude + the
// TableSummary vs TableDetail field split). isDetail tables keep their detail fields.
export const serializeBucket = (b: Bucket): RawObj => ({
  id: b.id,
  name: b.name,
  displayName: b.displayName,
  description: b.description,
  stage: b.stage,
  created: b.created,
  updated: b.updated,
  dataSizeBytes: b.dataSizeBytes,
  tablesCount: b.tablesCount,
  links: b.links,
  source_project: b.source_project,
  created_by: b.created_by,
  last_updated_by: b.last_updated_by,
});

export const serializeTable = (t: Table): RawObj => {
  const base: RawObj = {
    id: t.id,
    name: t.name,
    displayName: t.displayName,
    description: t.description,
    primaryKey: t.primaryKey,
    created: t.created,
    updated: t.updated,
    rowsCount: t.rowsCount,
    dataSizeBytes: t.dataSizeBytes,
    links: t.links,
    source_project: t.source_project,
  };
  if (t.isDetail) {
    base.columns = t.columns ?? null;
    base.fullyQualifiedName = t.fullyQualifiedName ?? null;
    base.used_by = t.used_by ?? null;
    base.created_by = t.created_by ?? null;
    base.last_updated_by = t.last_updated_by ?? null;
  }
  return base;
};

export const FAKE_DEVELOPMENT_BRANCH_KEY = FAKE_DEVELOPMENT_BRANCH;
