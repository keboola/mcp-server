// ---------------------------------------------------------------------------
// Public validators (ported from validation.py public functions).
// ---------------------------------------------------------------------------

import storageSchema from '../storage-schema.json' with { type: 'json' };

import { logger } from '@/logger';
import {
  type ComponentForValidation,
  type JsonDict,
  type JsonSchema,
  type ValidationContext,
} from './types';
import { validateJsonAgainstSchema } from './validate';

const SNOWFLAKE_TRANSFORMATION_ID = 'keboola.snowflake-transformation';
const BIGQUERY_TRANSFORMATION_ID = 'keboola.google-bigquery-transformation';

const STORAGE_VALIDATION_INITIAL_MESSAGE =
  'The provided storage configuration input does not follow the storage schema.\n';
const ROOT_PARAMETERS_INITIAL = (componentId: string) =>
  `The provided Root parameters configuration input does not follow the Root parameter json schema for component ` +
  `id: ${componentId}.\n`;
const ROW_PARAMETERS_INITIAL = (componentId: string) =>
  `The provided Row parameters configuration input does not follow the Row parameter json schema for component ` +
  `id: ${componentId}.\n`;

const validateStorageConfigurationAgainstSchema = (
  storage: JsonDict,
  initialMessage?: string,
  validationContext?: ValidationContext,
): JsonDict => {
  validateJsonAgainstSchema({
    jsonData: storage,
    schema: storageSchema as JsonSchema,
    initialMessage,
    validationContext,
  });
  return storage;
};

const validateStorageConfiguration = (
  storage: JsonDict | null | undefined,
  component: ComponentForValidation,
  initialMessage: string | undefined,
  opts: {
    isRowStorage: boolean;
    configurationId?: string | null;
    configurationRowId?: string | null;
  },
): JsonDict => {
  // Normalize to {'storage': storage | {} } — the agent may pass {storage: …} or just the inner object.
  let storageCfg: JsonDict | null;
  if (storage) {
    const inner = (storage as JsonDict).storage;
    storageCfg = (inner !== undefined ? inner : storage) as JsonDict | null;
  } else {
    storageCfg = {};
  }

  if (storageCfg === null || storageCfg === undefined) {
    logger.warn(
      `No "storage" configuration provided for component ${component.component_id} of type ${component.component_type}.`,
    );
    storageCfg = {};
  }

  if (
    component.component_id === SNOWFLAKE_TRANSFORMATION_ID ||
    component.component_id === BIGQUERY_TRANSFORMATION_ID
  ) {
    if (!storageCfg.input && !storageCfg.output) {
      throw new Error(
        `The "storage" must contain either "input" or "output" mappings in the configuration of the SQL ` +
          `transformation "${component.component_id}".`,
      );
    }
  }

  if (component.component_type === 'writer' && component.capabilities.is_row_based) {
    if (!opts.isRowStorage && Object.keys(storageCfg).length > 0) {
      throw new Error(
        `The "storage" must be empty for root configuration of the writer component ` +
          `"${component.component_id}" since it is row-based. In this case, storage should only be defined ` +
          'in its outgoing row configurations.',
      );
    } else if (opts.isRowStorage && !storageCfg.input) {
      throw new Error(
        `The "storage" must contain "input" mappings for the row configuration of the writer component ` +
          `"${component.component_id}".`,
      );
    }
  }

  if (component.component_type === 'writer' && !component.capabilities.is_row_based) {
    if (opts.isRowStorage) {
      logger.warn(
        `Validating "storage" for row configuration of non-row-based writer ${component.component_id} is not ` +
          'semantically correct. Possible cause: agent error or wrong component flag. Proceeding with validation.',
      );
    }
    if (!storageCfg.input) {
      throw new Error(
        `The "storage" must contain "input" mappings for the root configuration of the writer component ` +
          `"${component.component_id}".`,
      );
    }
  }

  const fullInitial = (initialMessage ?? '') + '\n' + STORAGE_VALIDATION_INITIAL_MESSAGE;
  const validationContext: ValidationContext = {
    component_id: component.component_id,
    configuration_id: opts.configurationId,
    configuration_row_id: opts.configurationRowId,
    scope: 'storage',
  };
  const normalized = validateStorageConfigurationAgainstSchema(
    { storage: storageCfg },
    fullInitial,
    validationContext,
  );
  return (normalized.storage ?? {}) as JsonDict;
};

export const validateRootStorageConfiguration = (
  storage: JsonDict | null | undefined,
  component: ComponentForValidation,
  initialMessage?: string,
  configurationId?: string | null,
): JsonDict =>
  validateStorageConfiguration(storage, component, initialMessage, {
    isRowStorage: false,
    configurationId,
  });

export const validateRowStorageConfiguration = (
  storage: JsonDict | null | undefined,
  component: ComponentForValidation,
  initialMessage?: string,
  configurationId?: string | null,
  configurationRowId?: string | null,
): JsonDict =>
  validateStorageConfiguration(storage, component, initialMessage, {
    isRowStorage: true,
    configurationId,
    configurationRowId,
  });

const validateParametersConfiguration = (
  parameters: JsonDict,
  schema: JsonSchema | null | undefined,
  componentId: string,
  initialMessage: string | undefined,
  configurationId?: string | null,
  configurationRowId?: string | null,
): JsonDict => {
  // Agent may pass {parameters: …} or just the inner object.
  const inner = (parameters as JsonDict).parameters;
  const expected = (inner !== undefined ? inner : parameters) as JsonDict;

  if (!schema || Object.keys(schema).length === 0) {
    logger.warn(`No schema provided for component ${componentId}, skipping validation.`);
    return expected;
  }

  validateJsonAgainstSchema({
    jsonData: expected,
    schema,
    initialMessage,
    validationContext: {
      component_id: componentId,
      configuration_id: configurationId,
      configuration_row_id: configurationRowId,
      scope: 'parameters',
    },
    sanitize: true,
  });
  return expected;
};

export const validateRootParametersConfiguration = (
  parameters: JsonDict,
  component: ComponentForValidation,
  initialMessage?: string,
  configurationId?: string | null,
): JsonDict =>
  validateParametersConfiguration(
    parameters,
    component.configuration_schema,
    component.component_id,
    (initialMessage ?? '') + '\n' + ROOT_PARAMETERS_INITIAL(component.component_id),
    configurationId,
  );

export const validateRowParametersConfiguration = (
  parameters: JsonDict,
  component: ComponentForValidation,
  initialMessage?: string,
  configurationId?: string | null,
  configurationRowId?: string | null,
): JsonDict =>
  validateParametersConfiguration(
    parameters,
    component.configuration_row_schema,
    component.component_id,
    (initialMessage ?? '') + '\n' + ROW_PARAMETERS_INITIAL(component.component_id),
    configurationId,
    configurationRowId,
  );

/**
 * Validates a list of processors against their component schemas. Skips processors
 * with no schema or whose schema is from the template (the `print_hello` marker).
 * Port of `validate_processors_configuration`.
 */
export const validateProcessorsConfiguration = async (
  fetchComponent: (componentId: string) => Promise<ComponentForValidation>,
  processors: JsonDict[],
  initialMessage?: string,
): Promise<JsonDict[]> => {
  for (const processor of processors) {
    const definition = (processor.definition as JsonDict) ?? {};
    const processorId = definition.component as string;
    const processorInfo = await fetchComponent(processorId);

    const schema = processorInfo.configuration_schema;
    if (!schema) continue;
    const required = Array.isArray((schema as JsonDict).required)
      ? ((schema as JsonDict).required as string[])
      : [];
    if (required.includes('print_hello')) continue;

    validateJsonAgainstSchema({
      jsonData: processor.parameters,
      schema,
      initialMessage: `${initialMessage}\nThe configuration of "${processorId}" processor is not valid.`,
      validationContext: { component_id: processorId },
      sanitize: true,
    });
  }
  return processors;
};
