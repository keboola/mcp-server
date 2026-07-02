/**
 * Public entry point for the components tool module.
 *
 * Preserves the import path `@/tools/components` for downstream consumers:
 * - `registerComponentTools` (src/server.ts)
 * - `fetchComponent` (src/tools/flow.ts)
 * - `configPreviewInternals` (src/preview.ts)
 *
 * The model/schema layer (zod schemas, SQL/transformation utils, param-update
 * helpers) is re-exported here so the former `@/tools/components.model` symbols
 * remain reachable through `@/tools/components`.
 */
export { configPreviewInternals, registerComponentTools } from './tools';
export { fetchComponent } from './utils';
export * from './model';
