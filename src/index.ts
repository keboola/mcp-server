import { parseArgs } from 'node:util';

import { Config } from '@/config';
import { createServer } from '@/server';
import { startStdio } from '@/transports/stdio';

type Transport = 'stdio' | 'streamable-http' | 'http-compat';

const parseCliConfig = (): { transport: Transport; config: Config } => {
  const { values } = parseArgs({
    options: {
      transport: { type: 'string', default: 'stdio' },
      'log-level': { type: 'string', default: 'INFO' },
      'api-url': { type: 'string' },
      'storage-token': { type: 'string' },
      'workspace-schema': { type: 'string' },
      host: { type: 'string', default: 'localhost' },
      port: { type: 'string', default: '8000' },
    },
    allowPositionals: false,
  });

  const transport = (values.transport ?? 'stdio') as Transport;

  // Base config from the environment, then CLI flags layered on top.
  const config = Config.fromMap(process.env).replaceBy({
    storageApiUrl: values['api-url'],
    storageToken: values['storage-token'],
    workspaceSchema: values['workspace-schema'],
  });

  return { transport, config };
};

const main = async (): Promise<void> => {
  const { transport, config } = parseCliConfig();
  const server = createServer(config);

  if (transport === 'stdio') {
    if (config.oauthClientId || config.oauthClientSecret) {
      throw new Error('OAuth authorization can only be used with HTTP-based transports.');
    }
    await startStdio(server);
    return;
  }

  // ponytail: HTTP/streamable-http transport lands in Phase 1 (Hono app).
  throw new Error(`Transport "${transport}" is not implemented yet.`);
};

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
