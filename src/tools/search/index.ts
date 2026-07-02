/**
 * `search` tool module. Public entry point: `registerSearchTools`, used by `src/server.ts`.
 *
 * Decomposed into:
 * - `tools.ts` — `find_component_id` + `search` handlers and registration
 * - `globalSearch.ts` — server-side global textual search + client-side enumeration fallback
 * - `model.ts` — types, constants, `SearchSpec` matching model and metadata helpers
 * - `jsonpath.ts` — the local JSONPath subset used by config-based search
 */

export { registerSearchTools } from './tools';
