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
