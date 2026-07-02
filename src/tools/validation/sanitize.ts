// ---------------------------------------------------------------------------
// Schema sanitization — port of KeboolaParametersValidator.sanitize_schema.
// ---------------------------------------------------------------------------

import { isObject, type JsonDict, type JsonSchema, SchemaError } from './types';

/**
 * Normalizes a JSON schema *in place* and returns it. Mirrors the Python
 * `_sanitize_node`: strips empty `enum`, converts boolean-ish `required` flags into
 * the list form (propagating up to the parent), and turns `properties: []` into `{}`.
 * Returns `[schema, isCurrentRequired]`.
 */
const sanitizeNode = (schema: unknown): [unknown, boolean | null] => {
  if (!isObject(schema)) {
    return [schema, false];
  }

  if ('enum' in schema && Array.isArray(schema.enum) && schema.enum.length === 0) {
    delete schema.enum;
  }

  let isCurrentRequired: boolean | null = null;
  let required = schema.required;
  if (!Array.isArray(required)) {
    if (required !== undefined) {
      isCurrentRequired = String(required).toLowerCase() === 'true';
    }
    required = [];
  }
  const requiredList = required as string[];

  let properties = schema.properties;
  if (properties !== undefined && properties !== null) {
    if (Array.isArray(properties) && properties.length === 0) {
      properties = {};
    } else if (!isObject(properties)) {
      throw new SchemaError(`properties must be a dictionary, got ${typeof properties}`);
    }
    const props = properties as JsonDict;
    for (const propertyName of Object.keys(props)) {
      const [sanitized, isChildRequired] = sanitizeNode(props[propertyName]);
      props[propertyName] = sanitized;
      if (isChildRequired === true && !requiredList.includes(propertyName)) {
        requiredList.push(propertyName);
      } else if (isChildRequired === false && requiredList.includes(propertyName)) {
        requiredList.splice(requiredList.indexOf(propertyName), 1);
      }
    }
    schema.properties = props;
  }

  if (requiredList.length > 0) {
    schema.required = [...requiredList];
  } else {
    delete schema.required;
  }

  if ('items' in schema) {
    const items = schema.items;
    if (isObject(items)) {
      schema.items = sanitizeNode(items)[0];
    } else if (Array.isArray(items)) {
      schema.items = items.map((item) => (isObject(item) ? sanitizeNode(item)[0] : item));
    }
  }

  for (const keyword of ['allOf', 'anyOf', 'oneOf'] as const) {
    if (keyword in schema && Array.isArray(schema[keyword])) {
      schema[keyword] = (schema[keyword] as unknown[]).map((s) =>
        isObject(s) ? sanitizeNode(s)[0] : s,
      );
    }
  }

  for (const keyword of ['not', 'if', 'then', 'else'] as const) {
    if (keyword in schema && isObject(schema[keyword])) {
      schema[keyword] = sanitizeNode(schema[keyword])[0];
    }
  }

  if ('additionalProperties' in schema && isObject(schema.additionalProperties)) {
    schema.additionalProperties = sanitizeNode(schema.additionalProperties)[0];
  }

  if ('patternProperties' in schema && isObject(schema.patternProperties)) {
    const pp = schema.patternProperties as JsonDict;
    for (const pattern of Object.keys(pp)) {
      if (isObject(pp[pattern])) pp[pattern] = sanitizeNode(pp[pattern])[0];
    }
  }

  for (const keyword of ['definitions', '$defs'] as const) {
    if (keyword in schema && isObject(schema[keyword])) {
      const defs = schema[keyword] as JsonDict;
      for (const name of Object.keys(defs)) {
        if (isObject(defs[name])) defs[name] = sanitizeNode(defs[name])[0];
      }
    }
  }

  return [schema, isCurrentRequired];
};

export const sanitizeSchema = (schema: JsonSchema): JsonSchema =>
  sanitizeNode(structuredClone(schema))[0] as JsonSchema;
