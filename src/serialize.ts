import { encode, type JsonValue } from '@toon-format/toon';

/**
 * Tool outputs are encoded as TOON (Token-Oriented Object Notation) — the same
 * token-efficient, schema-aware format the Python server used via `toon-format`.
 * `toonSerializeCompact` is the default; it drops null fields while preserving
 * TOON's list-of-objects column alignment.
 */

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/**
 * Drops null/undefined fields while keeping TOON's list-of-objects alignment.
 * Port of the Python `_filter_toon_nulls`:
 * - single-item object lists drop keys whose value is null;
 * - multi-item object lists keep every key that has a value in *any* item
 *   (first-seen order), leaving null where an item lacks it, so all rows align.
 */
export const filterToonNulls = (data: unknown): unknown => {
  if (Array.isArray(data)) {
    if (data.length === 0) return data;

    if (data.every(isPlainObject)) {
      if (data.length === 1) return [filterToonNulls(data[0])];

      const orderedKeys: string[] = [];
      const seen = new Set<string>();
      for (const item of data as Record<string, unknown>[]) {
        for (const [key, value] of Object.entries(item)) {
          if (value !== null && value !== undefined && !seen.has(key)) {
            seen.add(key);
            orderedKeys.push(key);
          }
        }
      }

      return (data as Record<string, unknown>[]).map((item) => {
        const cleaned: Record<string, unknown> = {};
        for (const key of orderedKeys) {
          const value = item[key];
          cleaned[key] = value === null || value === undefined ? null : filterToonNulls(value);
        }
        return cleaned;
      });
    }

    return data.map((item) => (item === null || item === undefined ? null : filterToonNulls(item)));
  }

  if (isPlainObject(data)) {
    const cleaned: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(data)) {
      if (value === null || value === undefined) continue;
      cleaned[key] = filterToonNulls(value);
    }
    return cleaned;
  }

  return data;
};

export type ToolSerializer = (data: unknown) => string;

export const toonSerialize: ToolSerializer = (data) => encode(data as JsonValue);

export const toonSerializeCompact: ToolSerializer = (data) =>
  encode(filterToonNulls(data) as JsonValue);
