/**
 * Public entry point for the tolerant JSON-schema validator module.
 *
 * Preserves the import path `@/tools/validation` for downstream consumers:
 * - `src/tools/components/tools.ts` (the validate* entry points)
 * - `src/tools/components/utils.ts` (ComponentForValidation, JsonDict types)
 * - `__tests__/tools.components.test.ts` (validate* entry points + `__testing`)
 *
 * The split modules are:
 * - `types.ts`    — shared types + RecoverableValidationError / SchemaError
 * - `sanitize.ts` — the tolerant schema sanitizer
 * - `validate.ts` — the draft-07 subset validator
 * - `model.ts`    — the public validate* entry points
 */
import { sanitizeSchema } from './sanitize';
import { validateJsonAgainstSchema } from './validate';

export type { ComponentForValidation, JsonDict, JsonSchema, ValidationContext } from './types';
export { RecoverableValidationError } from './types';
export {
  validateProcessorsConfiguration,
  validateRootParametersConfiguration,
  validateRootStorageConfiguration,
  validateRowParametersConfiguration,
  validateRowStorageConfiguration,
} from './model';

// Exported for unit testing.
export const __testing = { sanitizeSchema, validateJsonAgainstSchema };
