/**
 * Per-service base URLs, derived from the Storage API URL's hostname suffix.
 * Faithful port of the URL derivation in the Python `KeboolaClient` constructor:
 * every Keboola service lives at `https://<service>.<suffix>`, where `<suffix>` is
 * whatever follows `connection.` in the Storage API hostname.
 */
export type ServiceUrls = {
  storage: string;
  metastore: string;
  queue: string;
  ai: string;
  dataScience: string;
  encryption: string;
  scheduler: string;
  syncActions: string;
  queryService: string;
};

export const deriveServiceUrls = (storageApiUrl: string): ServiceUrls => {
  let hostname: string;
  try {
    hostname = new URL(storageApiUrl).hostname;
  } catch {
    hostname = '';
  }

  if (!hostname.startsWith('connection.')) {
    throw new Error(`Invalid Keboola Storage API URL: ${storageApiUrl}`);
  }

  const suffix = hostname.slice('connection.'.length);
  const at = (service: string): string => `https://${service}.${suffix}`;

  return {
    storage: at('connection'),
    metastore: at('metastore'),
    queue: at('queue'),
    ai: at('ai'),
    dataScience: at('data-science'),
    encryption: at('encryption'),
    scheduler: at('scheduler'),
    syncActions: at('sync-actions'),
    queryService: at('query'),
  };
};
