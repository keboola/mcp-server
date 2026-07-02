import type { ZodError, ZodIssue } from 'zod';

/**
 * Validation-error formatting — port of `keboola_mcp_server.errors`
 * (`_format_validation_errors`, `prettify_validation_error`, `ValidationErrorMiddleware`).
 *
 * The Python server used Pydantic + FastMCP middleware to catch a `ValidationError`
 * raised during tool-argument validation and re-render it with explicit field locations
 * so both humans and LLMs can see exactly which fields are missing or invalid. The
 * TypeScript server validates with Zod, so the faithful equivalent formats a `ZodError`.
 *
 * The recovery-hint / per-exception logging behavior of the Python `tool_errors`
 * decorator already lives in `@/mcp/tool` (`registerTool`); this module only adds the
 * validation-error prettifier the Python `errors` module also provided.
 */

export type FormattedValidationError = {
  field: string;
  message: string;
  extra: Record<string, string>;
};

export type FormattedValidationErrors = {
  errors: FormattedValidationError[];
};

/**
 * Formats Zod validation issues into a structured object — port of
 * `_format_validation_errors`. `field` is the dotted location path, `message` is the
 * human-readable message, and `extra` carries every remaining issue field (e.g. the
 * error `code`) as strings, matching the Python `extra` dict.
 */
export const formatValidationErrors = (issues: ZodIssue[]): FormattedValidationErrors => {
  const errors: FormattedValidationError[] = issues.map((issue) => {
    const extra: Record<string, string> = {};
    for (const [key, value] of Object.entries(issue)) {
      if (key === 'path' || key === 'message') continue;
      extra[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
    return {
      field: (issue.path ?? []).map((p) => String(p)).join('.'),
      message: issue.message ?? 'Validation error',
      extra,
    };
  });
  return { errors };
};

/** Renders the structured errors as a YAML-compatible block (no YAML dependency needed). */
const toYaml = (formatted: FormattedValidationErrors): string => {
  const lines: string[] = ['errors:'];
  for (const err of formatted.errors) {
    lines.push(`- field: ${err.field}`);
    lines.push(`  message: ${err.message}`);
    const extraKeys = Object.keys(err.extra);
    if (extraKeys.length === 0) {
      lines.push('  extra: {}');
    } else {
      lines.push('  extra:');
      for (const key of extraKeys) {
        lines.push(`    ${key}: ${err.extra[key]}`);
      }
    }
  }
  return `${lines.join('\n')}\n`;
};

/**
 * Formats a Zod validation error into a human- and LLM-readable string — port of
 * `prettify_validation_error`. Produces the same
 * `Found N validation error(s) for <model>` header followed by the structured body.
 *
 * @param error    The Zod validation error to format.
 * @param modelName The name of the validated model/tool (Pydantic carried this on the
 *                  error's `title`; Zod does not, so callers pass it explicitly).
 */
export const prettifyValidationError = (error: ZodError, modelName = 'unknown'): string => {
  const issues = error.issues ?? [];
  const header = `Found ${issues.length} validation error(s) for ${modelName}`;
  return `${header}\n${toYaml(formatValidationErrors(issues))}`;
};
