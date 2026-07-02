// ---------------------------------------------------------------------------
// Core validator — a tolerant subset of JSON Schema draft-07.
// ---------------------------------------------------------------------------

import { logger } from '@/logger';
import { sanitizeSchema } from './sanitize';
import {
  isObject,
  type JsonSchema,
  RecoverableValidationError,
  type ValidationContext,
} from './types';

type Fail = (
  validator: string,
  validatorValue: unknown,
  message: string,
  schemaPath: (string | number)[],
  instancePath: (string | number)[],
  instance: unknown,
) => never;

const jsTypeMatches = (type: string, instance: unknown): boolean => {
  switch (type) {
    case 'object':
      return isObject(instance);
    case 'array':
      return Array.isArray(instance);
    case 'string':
      return typeof instance === 'string';
    case 'boolean':
      return typeof instance === 'boolean';
    case 'null':
      return instance === null;
    case 'number':
      return typeof instance === 'number';
    case 'integer':
      return typeof instance === 'number' && Number.isInteger(instance);
    case 'button':
      // UI-only construct accepted as valid (port of check_button_type).
      return isObject(instance) && instance.type === 'button';
    default:
      // Unknown type keyword: be tolerant and accept.
      return true;
  }
};

const deepEqual = (a: unknown, b: unknown): boolean => {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((x, i) => deepEqual(x, b[i]));
  }
  if (isObject(a) && isObject(b)) {
    const ka = Object.keys(a);
    const kb = Object.keys(b);
    return ka.length === kb.length && ka.every((k) => deepEqual(a[k], b[k]));
  }
  return false;
};

/** Returns true if `instance` is valid under `schema` (no error thrown). */
const isValid = (schema: unknown, instance: unknown): boolean => {
  try {
    validateNode(schema, instance, [], [], () => {
      throw new Error('invalid');
    });
    return true;
  } catch {
    return false;
  }
};

const validateNode = (
  schema: unknown,
  instance: unknown,
  schemaPath: (string | number)[],
  instancePath: (string | number)[],
  fail: Fail,
): void => {
  if (typeof schema === 'boolean') {
    if (!schema)
      fail('schema', false, 'False schema rejects all values', schemaPath, instancePath, instance);
    return;
  }
  if (!isObject(schema)) return;

  // type
  if ('type' in schema) {
    const types = Array.isArray(schema.type) ? (schema.type as string[]) : [schema.type as string];
    if (!types.some((t) => jsTypeMatches(t, instance))) {
      fail(
        'type',
        schema.type,
        `${JSON.stringify(instance)} is not of type ${types.map((t) => JSON.stringify(t)).join(', ')}`,
        [...schemaPath, 'type'],
        instancePath,
        instance,
      );
    }
  }

  // enum
  if ('enum' in schema && Array.isArray(schema.enum)) {
    if (!schema.enum.some((v) => deepEqual(v, instance))) {
      fail(
        'enum',
        schema.enum,
        `${JSON.stringify(instance)} is not one of ${JSON.stringify(schema.enum)}`,
        [...schemaPath, 'enum'],
        instancePath,
        instance,
      );
    }
  }

  // const
  if ('const' in schema && !deepEqual(schema.const, instance)) {
    fail(
      'const',
      schema.const,
      `${JSON.stringify(instance)} was expected`,
      [...schemaPath, 'const'],
      instancePath,
      instance,
    );
  }

  // numeric constraints
  if (typeof instance === 'number') {
    if (typeof schema.minimum === 'number' && instance < schema.minimum) {
      fail(
        'minimum',
        schema.minimum,
        `${instance} is less than the minimum of ${schema.minimum}`,
        [...schemaPath, 'minimum'],
        instancePath,
        instance,
      );
    }
    if (typeof schema.maximum === 'number' && instance > schema.maximum) {
      fail(
        'maximum',
        schema.maximum,
        `${instance} is greater than the maximum of ${schema.maximum}`,
        [...schemaPath, 'maximum'],
        instancePath,
        instance,
      );
    }
  }

  // string constraints
  if (typeof instance === 'string') {
    if (typeof schema.minLength === 'number' && instance.length < schema.minLength) {
      fail(
        'minLength',
        schema.minLength,
        `${JSON.stringify(instance)} is too short`,
        [...schemaPath, 'minLength'],
        instancePath,
        instance,
      );
    }
    if (typeof schema.maxLength === 'number' && instance.length > schema.maxLength) {
      fail(
        'maxLength',
        schema.maxLength,
        `${JSON.stringify(instance)} is too long`,
        [...schemaPath, 'maxLength'],
        instancePath,
        instance,
      );
    }
  }

  // array constraints
  if (Array.isArray(instance)) {
    if (typeof schema.minItems === 'number' && instance.length < schema.minItems) {
      fail(
        'minItems',
        schema.minItems,
        `${JSON.stringify(instance)} is too short`,
        [...schemaPath, 'minItems'],
        instancePath,
        instance,
      );
    }
    if (typeof schema.maxItems === 'number' && instance.length > schema.maxItems) {
      fail(
        'maxItems',
        schema.maxItems,
        `${JSON.stringify(instance)} is too long`,
        [...schemaPath, 'maxItems'],
        instancePath,
        instance,
      );
    }
    const items = schema.items;
    if (isObject(items) || typeof items === 'boolean') {
      instance.forEach((item, i) =>
        validateNode(items, item, [...schemaPath, 'items'], [...instancePath, i], fail),
      );
    } else if (Array.isArray(items)) {
      instance.forEach((item, i) => {
        if (i < items.length) {
          validateNode(items[i], item, [...schemaPath, 'items', i], [...instancePath, i], fail);
        }
      });
    }
  }

  // object constraints
  if (isObject(instance)) {
    if (Array.isArray(schema.required)) {
      const missing = (schema.required as string[]).filter((key) => !(key in instance));
      if (missing.length > 0) {
        fail(
          'required',
          schema.required,
          `${JSON.stringify(missing[0])} is a required property`,
          [...schemaPath, 'required'],
          instancePath,
          instance,
        );
      }
    }

    const properties = isObject(schema.properties) ? schema.properties : {};
    for (const key of Object.keys(properties)) {
      if (key in instance) {
        validateNode(
          properties[key],
          instance[key],
          [...schemaPath, 'properties', key],
          [...instancePath, key],
          fail,
        );
      }
    }

    const patternProperties = isObject(schema.patternProperties) ? schema.patternProperties : {};
    const patternKeys = Object.keys(patternProperties);
    for (const key of Object.keys(instance)) {
      for (const pattern of patternKeys) {
        let re: RegExp | null = null;
        try {
          re = new RegExp(pattern);
        } catch {
          re = null;
        }
        if (re && re.test(key)) {
          validateNode(
            patternProperties[pattern],
            instance[key],
            [...schemaPath, 'patternProperties', pattern],
            [...instancePath, key],
            fail,
          );
        }
      }
    }

    if (isObject(schema.additionalProperties)) {
      const declared = new Set(Object.keys(properties));
      for (const key of Object.keys(instance)) {
        if (declared.has(key)) continue;
        if (patternKeys.some((p) => safeTest(p, key))) continue;
        validateNode(
          schema.additionalProperties,
          instance[key],
          [...schemaPath, 'additionalProperties'],
          [...instancePath, key],
          fail,
        );
      }
    } else if (schema.additionalProperties === false) {
      const declared = new Set(Object.keys(properties));
      for (const key of Object.keys(instance)) {
        if (declared.has(key)) continue;
        if (patternKeys.some((p) => safeTest(p, key))) continue;
        fail(
          'additionalProperties',
          false,
          `Additional properties are not allowed (${JSON.stringify(key)} was unexpected)`,
          [...schemaPath, 'additionalProperties'],
          instancePath,
          instance,
        );
      }
    }
  }

  // allOf
  if (Array.isArray(schema.allOf)) {
    schema.allOf.forEach((sub, i) =>
      validateNode(sub, instance, [...schemaPath, 'allOf', i], instancePath, fail),
    );
  }

  // anyOf
  if (Array.isArray(schema.anyOf)) {
    if (!schema.anyOf.some((sub) => isValid(sub, instance))) {
      fail(
        'anyOf',
        schema.anyOf,
        `${JSON.stringify(instance)} is not valid under any of the given schemas`,
        [...schemaPath, 'anyOf'],
        instancePath,
        instance,
      );
    }
  }

  // oneOf
  if (Array.isArray(schema.oneOf)) {
    const matches = schema.oneOf.filter((sub) => isValid(sub, instance)).length;
    if (matches !== 1) {
      fail(
        'oneOf',
        schema.oneOf,
        `${JSON.stringify(instance)} is valid under ${matches} of the given schemas (expected exactly 1)`,
        [...schemaPath, 'oneOf'],
        instancePath,
        instance,
      );
    }
  }

  // not
  if (schema.not !== undefined && isValid(schema.not, instance)) {
    fail(
      'not',
      schema.not,
      `${JSON.stringify(instance)} is not allowed`,
      [...schemaPath, 'not'],
      instancePath,
      instance,
    );
  }

  // if / then / else
  if (schema.if !== undefined) {
    if (isValid(schema.if, instance)) {
      if (schema.then !== undefined) {
        validateNode(schema.then, instance, [...schemaPath, 'then'], instancePath, fail);
      }
    } else if (schema.else !== undefined) {
      validateNode(schema.else, instance, [...schemaPath, 'else'], instancePath, fail);
    }
  }
};

const safeTest = (pattern: string, value: string): boolean => {
  try {
    return new RegExp(pattern).test(value);
  } catch {
    return false;
  }
};

/**
 * Validates `jsonData` against `schema`. On a validation failure, throws a
 * RecoverableValidationError. On a structurally invalid schema, logs and returns
 * (continue as if valid) — the parity behaviour of `_validate_json_against_schema`.
 */
export const validateJsonAgainstSchema = (opts: {
  jsonData: unknown;
  schema: JsonSchema;
  initialMessage?: string;
  validationContext?: ValidationContext;
  sanitize?: boolean;
}): void => {
  let schema = opts.schema;
  try {
    if (opts.sanitize) schema = sanitizeSchema(opts.schema);
  } catch (error) {
    logger.warn({ err: error }, 'The validation schema is not valid; skipping validation.');
    return;
  }

  const fail: Fail = (validator, validatorValue, message, schemaPath, instancePath, instance) => {
    throw new RecoverableValidationError({
      message,
      validator,
      validatorValue,
      schemaPath,
      instancePath,
      instance,
      initialMessage: opts.initialMessage,
      validationContext: opts.validationContext,
    });
  };

  try {
    validateNode(schema, opts.jsonData, [], [], fail);
  } catch (error) {
    if (error instanceof RecoverableValidationError) throw error;
    // Treat any non-validation error as an invalid schema → skip (continue).
    logger.warn({ err: error }, 'The validation schema is not valid; skipping validation.');
  }
};
