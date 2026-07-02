import { describe, expect, it } from 'vitest';

import { Config } from '@/config';

describe('Config', () => {
  it.each([
    ['canonical field name', { storageToken: 'abc' }],
    ['KBC_ env-style key', { KBC_STORAGE_TOKEN: 'abc' }],
    ['X- header-style key', { 'X-StorageApiToken': 'abc' }],
    ['snake_case alias', { storage_api_token: 'abc' }],
  ])('resolves storageToken from %s', (_label, map) => {
    expect(Config.fromMap(map).storageToken).toBe('abc');
  });

  it('amends a Storage API URL down to scheme + host', () => {
    const config = Config.fromMap({ KBC_STORAGE_API_URL: 'connection.keboola.com/some/path' });
    expect(config.storageApiUrl).toBe('https://connection.keboola.com');
  });

  it.each(['', 'none', 'null', 'default', 'production', 'PRODUCTION'])(
    'normalizes branch id %j to undefined',
    (branch) => {
      expect(Config.fromMap({ KBC_BRANCH_ID: branch }).branchId).toBeUndefined();
    },
  );

  it('keeps a real branch id', () => {
    expect(Config.fromMap({ KBC_BRANCH_ID: '12345' }).branchId).toBe('12345');
  });

  it('redacts secret fields in the string form', () => {
    const text = new Config({ storageToken: 'super-secret', branchId: '1' }).toString();
    expect(text).toContain("storageToken='****'");
    expect(text).not.toContain('super-secret');
    expect(text).toContain("branchId='1'");
  });

  it('layers replaceBy values over the base config', () => {
    const base = new Config({ storageToken: 'a', branchId: '1' });
    const next = base.replaceBy({ storageToken: 'b' });
    expect(next.storageToken).toBe('b');
    expect(next.branchId).toBe('1');
  });
});
