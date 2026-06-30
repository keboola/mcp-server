/**
 * Shared types and helpers for the tolerant JSON-schema validator.
 *
 * Ported from `tools/validation.py`. There is no `ajv` (or any JSON-schema library)
 * in this project's dependencies, so the validator is hand-written. It is
 * deliberately *tolerant* of the schema inconsistencies the Keboola Developer Portal
 * UI schemas exhibit (boolean `required`, empty `enum`, `properties: []`, the UI-only
 * `button` type, …) and — matching the Python behaviour — when the schema itself is
 * invalid or absent, validation is skipped (logged + continue) so a broken upstream
 * schema never blocks a write.
 */

export type JsonDict = Record<string, unknown>;
export type JsonSchema = Record<string, unknown>;

/** Minimal Component shape the validators need. */
export type ComponentForValidation = {
  component_id: string;
  component_type: string;
  capabilities: { is_row_based: boolean };
  configuration_schema?: JsonSchema | null;
  configuration_row_schema?: JsonSchema | null;
};

export type ValidationContext = {
  component_id: string;
  configuration_id?: string | null;
  configuration_row_id?: string | null;
  scope?: 'parameters' | 'storage' | string | null;
};

export const isObject = (value: unknown): value is JsonDict =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const contextToString = (ctx: ValidationContext): string => {
  let s = `component_id=${ctx.component_id}`;
  if (ctx.configuration_id) s += `, configuration_id=${ctx.configuration_id}`;
  if (ctx.configuration_row_id) s += `, configuration_row_id=${ctx.configuration_row_id}`;
  if (ctx.scope) s += `, scope=${ctx.scope}`;
  return s;
};

/**
 * Raised when an instance is invalid under a schema. Carries a recoverable, agent-
 * friendly message — the port of Python's RecoverableValidationError.
 */
export class RecoverableValidationError extends Error {
  readonly validator: string;
  readonly validatorValue: unknown;
  readonly schemaPath: (string | number)[];
  readonly instancePath: (string | number)[];
  readonly instance: unknown;
  readonly baseMessage: string;
  initialMessage?: string;
  validationContext?: ValidationContext;

  constructor(opts: {
    message: string;
    validator: string;
    validatorValue: unknown;
    schemaPath: (string | number)[];
    instancePath: (string | number)[];
    instance: unknown;
    initialMessage?: string;
    validationContext?: ValidationContext;
  }) {
    super(opts.message);
    this.name = 'RecoverableValidationError';
    this.baseMessage = opts.message;
    this.validator = opts.validator;
    this.validatorValue = opts.validatorValue;
    this.schemaPath = opts.schemaPath;
    this.instancePath = opts.instancePath;
    this.instance = opts.instance;
    this.initialMessage = opts.initialMessage;
    this.validationContext = opts.validationContext;
    this.message = this.format();
  }

  private format(): string {
    let s = `${this.baseMessage}\n`;
    if (this.validator && this.validatorValue !== undefined && this.validatorValue !== null) {
      const schemaPath = this.schemaPath.map((p) => `[${JSON.stringify(p)}]`).join('');
      s += `Failed validating ${JSON.stringify(this.validator)} in schema${schemaPath}:\n`;
      s += `    ${JSON.stringify({ [this.validator]: this.validatorValue }, null, 2)}\n`;
    }
    if (this.instancePath.length > 0) {
      const instancePath = this.instancePath.map((p) => `[${JSON.stringify(p)}]`).join('');
      s += `On instance${instancePath}:\n`;
      s += `    ${JSON.stringify(this.instance, null, 4)}\n`;
    }
    if (this.initialMessage) s += `${this.initialMessage}\n`;
    if (this.validationContext) {
      s += `Validation component context: ${contextToString(this.validationContext)}\n`;
    }
    if (
      this.validator === 'required' &&
      Array.isArray(this.validatorValue) &&
      this.validationContext?.scope === 'parameters'
    ) {
      const requiredFields = (this.validatorValue as string[]).map((f) => `\`${f}\``).join(', ');
      s +=
        `HINT: Ensure ALL of the following required fields are present in \`parameters\`: ${requiredFields}. ` +
        'Call `get_components` to retrieve the full schema and `get_config_examples` for real-world examples.' +
        '\n';
    }
    return s.replace(/\s+$/, '');
  }
}

/** A schema that is structurally invalid (port of jsonschema.SchemaError). */
export class SchemaError extends Error {}
