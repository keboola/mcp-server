// Ported from tools/storage/usage.py (get_created_by / get_last_updated_by).
//
// find_id_usage depends on the search subsystem (tools/search.py), which is not yet
// ported to TypeScript. include_usage therefore returns empty usage; see tools.ts.

import { getMetadataProperty, parseIsoTimestamp, type RawObj } from './model';

const CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';
const CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
const CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
const UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
const UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
const UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';

export type ComponentUsageReference = {
  component_id: string;
  configuration_id: string;
  configuration_row_id: string | null;
  configuration_name: string | null;
  used_in: string | null;
  timestamp: string | null;
};

const latestMetadataTimestamp = (metadata: RawObj[], keys: string[]): string | null => {
  let latest: number | null = null;
  let latestRaw: string | null = null;
  for (const item of metadata) {
    if (!keys.includes(item.key as string)) continue;
    const rawTs = item.timestamp;
    if (typeof rawTs !== 'string') continue;
    let parsed: number;
    try {
      parsed = parseIsoTimestamp(rawTs);
    } catch {
      continue;
    }
    if (latest === null || parsed > latest) {
      latest = parsed;
      latestRaw = rawTs;
    }
  }
  return latestRaw;
};

const lineageReference = (
  metadata: unknown,
  componentKey: string,
  configKey: string,
  rowKey: string,
): ComponentUsageReference | null => {
  if (!Array.isArray(metadata)) return null;
  const items = metadata as RawObj[];
  const componentId = getMetadataProperty(items, componentKey);
  const configurationId = getMetadataProperty(items, configKey);
  const rowId = getMetadataProperty(items, rowKey);
  if (componentId === null || configurationId === null) return null;
  return {
    component_id: String(componentId),
    configuration_id: String(configurationId),
    configuration_row_id: rowId ? String(rowId) : null,
    configuration_name: null,
    used_in: null,
    timestamp: latestMetadataTimestamp(items, [componentKey, configKey, rowKey]),
  };
};

export const getCreatedBy = (metadata: unknown): ComponentUsageReference | null =>
  lineageReference(
    metadata,
    CREATED_BY_COMPONENT_ID,
    CREATED_BY_CONFIGURATION_ID,
    CREATED_BY_CONFIGURATION_ROW_ID,
  );

export const getLastUpdatedBy = (metadata: unknown): ComponentUsageReference | null =>
  lineageReference(
    metadata,
    UPDATED_BY_COMPONENT_ID,
    UPDATED_BY_CONFIGURATION_ID,
    UPDATED_BY_CONFIGURATION_ROW_ID,
  );
