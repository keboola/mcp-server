import { createDataScienceClient } from '@keboola/api-client/dataScience';
import { createMetastoreClient } from '@keboola/api-client/metastore';
import { createQueueClient } from '@keboola/api-client/queue';
import { createStorageClient } from '@keboola/api-client/storage';
import { createSyncActionsClient } from '@keboola/api-client/syncActions';

import type { Config } from '@/config';
import { ProjectLinksManager } from '@/links';
import { createRawClient, type RawClient } from './raw';
import { createRetryMiddleware } from './retry';
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
  /** Typed Sync Actions client (sendSyncAction, gitRepository.*). */
  syncActions: ReturnType<typeof createSyncActionsClient>;
  /** Typed Data Science client (apps CRUD, runs, logs tail, runtimes). */
  dataScience: ReturnType<typeof createDataScienceClient>;
  /**
   * Raw Storage API client rooted at `<storage>/v2/storage`, for endpoints where
   * api-client's typed methods diverge from the exact SAPI calls (e.g. table+column
   * metadata). Mirrors the Python `KeboolaClient.storage_client` raw access.
   */
  rawStorage: RawClient;
  /** Raw Queue API client (for endpoints not in api-client, e.g. job creation). */
  rawQueue: RawClient;
  /**
   * Raw AI catalog client for `docs/components/{id}` — the component metadata
   * (config schemas + examples) used by `get_components` / `get_config_examples` /
   * config validation. Documentation Q&A and component recommendation moved to the
   * pgvector docs-search index (see clients/docsSearch.ts); this catalog endpoint has
   * no docs-search equivalent (the index holds markdown docs, not config schemas).
   */
  rawAi: RawClient;
  /** Raw Sync Actions service client (POST actions). */
  rawSyncActions: RawClient;
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
  // Storage endpoints accept the OAuth bearer token in preference to the SAPI token
  // (matches Python's `bearer_or_sapi_token`).
  const storageToken = config.bearerToken ? `Bearer ${config.bearerToken}` : token;

  // Retry transient failures (network errors, 5xx, 429) with exponential backoff so a flaky
  // or briefly-unavailable Keboola service doesn't fail a tool call. Mirrors the raw client's
  // own retry on the same status set. A fresh middleware instance per request is cheap.
  const retry = createRetryMiddleware(3);

  // ponytail: SAPI token via X-StorageApi-Token (the common path). OAuth bearer
  // token handling is layered in with the OAuth provider (Plan §5).
  return {
    storage: createStorageClient({ baseUrl: urls.storage, token, middlewares: [retry] }),
    queue: createQueueClient({ baseUrl: urls.queue, token, middlewares: [retry] }),
    metastore: createMetastoreClient({ baseUrl: urls.metastore, token, middlewares: [retry] }),
    syncActions: createSyncActionsClient({
      baseUrl: urls.syncActions,
      token,
      middlewares: [retry],
    }),
    dataScience: createDataScienceClient({
      baseUrl: urls.dataScience,
      token,
      middlewares: [retry],
    }),
    rawStorage: createRawClient({ baseUrl: `${urls.storage}/v2/storage`, token: storageToken }),
    rawQueue: createRawClient({ baseUrl: urls.queue, token }),
    rawAi: createRawClient({ baseUrl: urls.ai, token }),
    rawSyncActions: createRawClient({ baseUrl: urls.syncActions, token }),
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
