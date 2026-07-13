import { parseArgs } from 'node:util';

import { Config } from '@/config';
import { applyDeploymentDefaults, type Env, parseEnv, redactedEnv } from '@/env';
import { logger } from '@/logger';
import { createServer, SERVER_VERSION } from '@/server';
import { startHttp } from '@/transports/http';
import { startStdio } from '@/transports/stdio';

// 'http-compat' is an alias for 'streamable-http' kept for backwards compatibility.
type Transport = 'stdio' | 'streamable-http' | 'http-compat';

type ParsedCli = { transport: Transport; config: Config; env: Env; host: string; port: number };

const parseCliConfig = (): ParsedCli => {
  const { values } = parseArgs({
    options: {
      transport: { type: 'string', default: 'stdio' },
      'log-level': { type: 'string' },
      'api-url': { type: 'string' },
      'storage-token': { type: 'string' },
      'workspace-schema': { type: 'string' },
      host: { type: 'string' },
      port: { type: 'string' },
    },
    allowPositionals: false,
  });

  const transport = (values.transport ?? 'stdio') as Transport;

  // Process-level deployment env (validated, build/run-segregated).
  const env = parseEnv();

  // Per-request base config: KBC_*/X-* env, then CLI flags layered on top.
  let config = Config.fromMap(process.env).replaceBy({
    storageApiUrl: values['api-url'],
    storageToken: values['storage-token'],
    workspaceSchema: values['workspace-schema'],
  });
  // Derive Storage/OAuth/MCP URLs from HOSTNAME_SUFFIX when not set explicitly.
  config = applyDeploymentDefaults(config, env);

  return {
    transport,
    config,
    env,
    // Precedence: explicit CLI flag > deployment env > schema default.
    host: values.host ?? env.HOST,
    port: values.port ? Number(values.port) : env.PORT,
  };
};

/** One-time startup dump of the resolved config (secrets redacted) — parity with the
 * Python server, and the first thing to check when a tool misbehaves. */
const logStartupConfig = (parsed: ParsedCli): void => {
  const { env, config, transport, host, port } = parsed;
  const docsIndex = env.DATABASE_URL
    ? `configured (model=${env.DOCS_EMBEDDER_MODEL ?? 'unset'}, dim=${env.DOCS_EMBEDDER_DIM ?? 'default'})`
    : 'not configured';
  logger.info(
    {
      transport,
      host,
      port,
      version: SERVER_VERSION,
      docsIndex,
      config: config.toString(),
      env: redactedEnv(env),
    },
    'Keboola MCP server starting',
  );
};

const main = async (): Promise<void> => {
  const parsed = parseCliConfig();
  const { transport, config, host, port } = parsed;
  logStartupConfig(parsed);

  if (transport === 'stdio') {
    if (config.oauthClientId || config.oauthClientSecret) {
      throw new Error('OAuth authorization can only be used with HTTP-based transports.');
    }
    await startStdio(createServer(config));
    return;
  }

  // 'streamable-http' and its 'http-compat' alias both serve the Hono app.
  startHttp(config, host, port);
};

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
