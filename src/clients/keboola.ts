import { createMetastoreClient } from '@keboola/api-client/metastore';
import { createQueueClient } from '@keboola/api-client/queue';
import { createStorageClient } from '@keboola/api-client/storage';

import type { Config } from '@/config';
import { ProjectLinksManager } from '@/links';
import { deriveServiceUrls } from './urls';

/**
 * The set of Keboola service clients a tool handler operates with, built per
 * request from the resolved Config. Mirrors the Python `KeboolaClient`, but reuses
 * the published `@keboola/api-client` service clients instead of bespoke HTTP code.
 *
 * Clients are added here as the tools that need them are ported (Plan §4). The
 * scheduler and AI-docs surfaces arrive once keboola/ui#6862 is published.
 */
export type KeboolaClients = {
  storage: ReturnType<typeof createStorageClient>;
  queue: ReturnType<typeof createQueueClient>;
  metastore: ReturnType<typeof createMetastoreClient>;
  /**
   * Effective branch id for branch-scoped endpoints. `'default'` is the Storage
   * API's alias for the production branch (matches Python's `branch_id or 'default'`),
   * so no default-branch lookup is needed.
   */
  branchId: string;
};

export const createKeboolaClients = (config: Config): KeboolaClients => {
  if (!config.storageApiUrl) {
    throw new Error('Storage API URL is not configured.');
  }
  if (!config.storageToken) {
    throw new Error('Storage API token is not configured.');
  }

  const urls = deriveServiceUrls(config.storageApiUrl);
  const token = config.storageToken;

  // ponytail: SAPI token via X-StorageApi-Token (the common path). OAuth bearer
  // token handling is layered in with the OAuth provider (Plan §5).
  return {
    storage: createStorageClient({ baseUrl: urls.storage, token, middlewares: [] }),
    queue: createQueueClient({ baseUrl: urls.queue, token, middlewares: [] }),
    metastore: createMetastoreClient({ baseUrl: urls.metastore, token, middlewares: [] }),
    branchId: config.branchId ?? 'default',
  };
};

/**
 * Builds a ProjectLinksManager for the current project. Mirrors the Python
 * `ProjectLinksManager.from_client`: resolves the project id from the verified token.
 */
export const createLinksManager = async (
  config: Config,
  clients: KeboolaClients,
): Promise<ProjectLinksManager> => {
  const token = await clients.storage.tokens.verify();
  const projectId = String((token.owner as { id: string | number }).id);
  return new ProjectLinksManager({
    baseUrl: config.storageApiUrl ?? '',
    projectId,
    branchId: config.branchId,
  });
};
