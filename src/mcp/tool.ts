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
        return { content: [{ type: 'text', text: serialize(result) }] };
      } catch (error) {
        const base = error instanceof Error ? error.message : String(error);
        const text = def.recovery ? `${base}\nRecovery: ${def.recovery}` : base;
        logger.error({ err: error, tool: def.name }, `MCP tool "${def.name}" call failed`);
        return { content: [{ type: 'text', text }], isError: true };
      }
    }) as never,
  );
};
