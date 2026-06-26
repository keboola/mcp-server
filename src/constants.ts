// Well-known Keboola component IDs, ported from clients/client.py.

export const ORCHESTRATOR_COMPONENT_ID = 'keboola.orchestrator';
export const CONDITIONAL_FLOW_COMPONENT_ID = 'keboola.flow';
export const DATA_APP_COMPONENT_ID = 'keboola.data-apps';

export type FlowType = typeof CONDITIONAL_FLOW_COMPONENT_ID | typeof ORCHESTRATOR_COMPONENT_ID;

export const FLOW_TYPES: readonly FlowType[] = [
  CONDITIONAL_FLOW_COMPONENT_ID,
  ORCHESTRATOR_COMPONENT_ID,
];

/** Keboola metadata field keys (subset; ported from config.py MetadataField). */
export const MetadataField = {
  DESCRIPTION: 'KBC.description',
  PROJECT_DESCRIPTION: 'KBC.projectDescription',
} as const;
