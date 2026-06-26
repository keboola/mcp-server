import { parseArgs } from 'node:util';

import { Config } from '@/config';
import { createServer } from '@/server';
import { startHttp } from '@/transports/http';
import { startStdio } from '@/transports/stdio';

// 'http-compat' is an alias for 'streamable-http' kept for backwards compatibility.
type Transport = 'stdio' | 'streamable-http' | 'http-compat';

type ParsedCli = { transport: Transport; config: Config; host: string; port: number };

const parseCliConfig = (): ParsedCli => {
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

  return {
    transport,
    config,
    host: values.host ?? 'localhost',
    port: Number(values.port ?? '8000'),
  };
};

const main = async (): Promise<void> => {
  const { transport, config, host, port } = parseCliConfig();

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
