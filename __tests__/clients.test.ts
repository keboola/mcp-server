import { describe, expect, it } from 'vitest';

import { createKeboolaClients } from '@/clients/keboola';
import { deriveServiceUrls } from '@/clients/urls';
import { Config } from '@/config';

describe('deriveServiceUrls', () => {
  it('derives every service URL from the storage hostname suffix', () => {
    const urls = deriveServiceUrls('https://connection.eu-central-1.keboola.com');
    expect(urls).toEqual({
      storage: 'https://connection.eu-central-1.keboola.com',
      metastore: 'https://metastore.eu-central-1.keboola.com',
      queue: 'https://queue.eu-central-1.keboola.com',
      ai: 'https://ai.eu-central-1.keboola.com',
      dataScience: 'https://data-science.eu-central-1.keboola.com',
      encryption: 'https://encryption.eu-central-1.keboola.com',
      scheduler: 'https://scheduler.eu-central-1.keboola.com',
      syncActions: 'https://sync-actions.eu-central-1.keboola.com',
    });
  });

  it.each(['https://example.com', 'not-a-url', 'https://storage.keboola.com'])(
    'rejects a non-connection Storage API URL %j',
    (url) => {
      expect(() => deriveServiceUrls(url)).toThrow(/Invalid Keboola Storage API URL/);
    },
  );
});

describe('createKeboolaClients', () => {
  it('builds clients from a configured token + url', () => {
    const config = new Config({
      storageApiUrl: 'https://connection.keboola.com',
      storageToken: 'token',
    });
    const clients = createKeboolaClients(config);
    expect(clients.storage.buckets).toBeDefined();
    expect(clients.queue).toBeDefined();
  });

  it.each([
    ['missing url', { storageToken: 'token' }, /Storage API URL is not configured/],
    [
      'missing token',
      { storageApiUrl: 'https://connection.keboola.com' },
      /token is not configured/,
    ],
  ])('throws on %s', (_label, fields, pattern) => {
    expect(() => createKeboolaClients(new Config(fields))).toThrow(pattern);
  });
});
