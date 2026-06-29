import { z } from 'zod';

// Ported from tools/semantic/model.py.

/** Semantic object types handled by the semantic tools. */
export const SEMANTIC_OBJECT_TYPE = [
  'semantic-model',
  'semantic-dataset',
  'semantic-metric',
  'semantic-relationship',
  'semantic-glossary',
  'semantic-constraint',
] as const;

export type SemanticObjectType = (typeof SEMANTIC_OBJECT_TYPE)[number];

export const SemanticObjectTypeEnum = z.enum(SEMANTIC_OBJECT_TYPE);

/** Semantic object type selection used by semantic tools (object_type + optional ids). */
export const SemanticObjectTypeSelectionSchema = z.object({
  object_type: SemanticObjectTypeEnum.describe('Semantic object type to load.'),
  ids: z
    .array(z.string())
    .default([])
    .describe(
      'Specific object UUIDs to include. Empty list [] means include all objects of this type.',
    ),
});

export type SemanticObjectTypeSelection = z.infer<typeof SemanticObjectTypeSelectionSchema>;

/** Typed semantic object reference. */
export type SemanticObjectRef = {
  object_type: SemanticObjectType;
  id: string;
};

// --- Metastore object shape (JSON:API `data` envelope item) --------------------

export type MetaObjectMeta = {
  name?: string | null;
  [key: string]: unknown;
};

/** Single object from the Metastore JSON:API response (port of `MetastoreObject`). */
export type MetastoreObject = {
  type?: string | null;
  id?: string | null;
  attributes?: Record<string, unknown> | null;
  relationships?: Record<string, unknown> | null;
  meta?: MetaObjectMeta | null;
};

// --- Typed service objects (port of SemanticServiceData hierarchy) -------------

export type SemanticServiceData = {
  semanticType: SemanticObjectType;
  id: string;
  data: MetastoreObject;
  attributes: Record<string, unknown>;
  /** display_name: own `name`, else meta.name (glossary overrides with term). */
  displayName: string | null;
  // Type-specific fields used by the service heuristics:
  name?: string | null;
  sqlDialect?: string | null;
  tableId?: string | null;
  fqn?: string | null;
  modelUuid?: string | null;
  sql?: string | null;
  dataset?: string | null;
  fromDataset?: string | null;
  toDataset?: string | null;
  on?: string | null;
  term?: string | null;
  description?: string | null;
  constraintType?: string | null;
  severity?: string | null;
  metrics?: string[];
  datasets?: string[];
  errorMessage?: string | null;
  remediation?: string | null;
  preQueryCheck?: boolean;
  validationQuery?: Record<string, unknown> | null;
};

export type SemanticServiceDataTypeGroup = {
  objectType: SemanticObjectType;
  objects: SemanticServiceData[];
};

export type ConstraintValidationFinding = {
  constraint_id: string;
  constraint_name: string;
  severity: string;
  status: string;
  message: string;
  validation_query: string | null;
};

export type SemanticValidationServiceOutput = {
  valid: boolean;
  usedObjectGroups: SemanticServiceDataTypeGroup[];
  matchedRelationships: string[];
  violations: ConstraintValidationFinding[];
  postExecutionChecks: ConstraintValidationFinding[];
};

export type SemanticSearchHit = {
  objectType: SemanticObjectType;
  object: SemanticServiceData;
  semanticModelId: string;
  matchedPatterns: string[];
  matchedPaths: string[];
};
