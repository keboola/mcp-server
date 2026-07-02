import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { CallToolResult, ToolAnnotations } from '@modelcontextprotocol/sdk/types.js';
import type { z, ZodRawShape } from 'zod';

import { logger } from '@/logger';
import { type ToolSerializer, toonSerializeCompact } from '@/serialize';

/**
 * Declarative tool registration shared by every Keboola MCP tool. Wraps the SDK's
 * `registerTool` to (1) serialize the handler's structured result to TOON text and
 * (2) catch errors, append an optional recovery hint, and return an MCP error
 * result — the parity equivalent of the Python `serializer=` + `tool_errors()`.
 */
export type ToolDefinition<Shape extends ZodRawShape> = {
  name: string;
  title?: string;
  description: string;
  inputSchema?: Shape;
  annotations?: ToolAnnotations;
  /** Output encoder; defaults to compact TOON (nulls dropped). */
  serializer?: ToolSerializer;
  /** Recovery hint appended to error messages, to guide the model on failure. */
  recovery?: string;
  handler: (args: z.infer<z.ZodObject<Shape>>) => Promise<unknown> | unknown;
};

/**
 * Human-useful message from a tool error. `@keboola/api-client`'s `ApiError.message` is
 * only the HTTP status text (e.g. "Bad Request") — the real reason and the support
 * exception id live on `error.data` (`{ error, message, exceptionId }`). Surface them so
 * the client sees "Bad Request: Invalid access token (exception ID: …)" instead of an
 * opaque status. (Our raw client already composes this into its message, so it has no
 * `.data` and passes through unchanged.)
 */
export const describeToolError = (error: unknown): string => {
  const base = error instanceof Error ? error.message : String(error);
  const data = (error as { data?: unknown }).data;
  if (!data || typeof data !== 'object') return base;
  const d = data as Record<string, unknown>;
  const detail = [d.error, d.message].find(
    (v): v is string => typeof v === 'string' && v.trim().length > 0,
  );
  let msg = detail && detail.trim() !== base ? `${base}: ${detail.trim()}` : base;
  if (typeof d.exceptionId === 'string' && d.exceptionId.length > 0) {
    msg += ` (exception ID: ${d.exceptionId})`;
  }
  return msg;
};

export const registerTool = <Shape extends ZodRawShape>(
  server: McpServer,
  def: ToolDefinition<Shape>,
): void => {
  const serialize = def.serializer ?? toonSerializeCompact;

  server.registerTool(
    def.name,
    {
      title: def.title,
      description: def.description,
      inputSchema: def.inputSchema ?? ({} as Shape),
      annotations: def.annotations,
    },
    // The SDK's registerTool overloads don't infer cleanly through a generic
    // wrapper; the handler is fully typed at the ToolDefinition boundary, so we
    // cast just this internal bridge callback.
    (async (args: z.infer<z.ZodObject<Shape>>): Promise<CallToolResult> => {
      try {
        const result = await def.handler(args);
        // String results pass through verbatim (parity with FastMCP); objects are TOON-encoded.
        const text = typeof result === 'string' ? result : serialize(result);
        return { content: [{ type: 'text', text }] };
      } catch (error) {
        const base = describeToolError(error);
        const text = def.recovery ? `${base}\nRecovery: ${def.recovery}` : base;
        logger.error({ err: error, tool: def.name }, `MCP tool "${def.name}" call failed`);
        return { content: [{ type: 'text', text }], isError: true };
      }
    }) as never,
  );
};
