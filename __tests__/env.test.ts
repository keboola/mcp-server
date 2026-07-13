import { describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { applyDeploymentDefaults, parseEnv, redactedEnv } from '@/env';

describe('redactedEnv', () => {
  it('masks secrets, strips DATABASE_URL credentials, and keeps non-secrets', () => {
    const dump = redactedEnv(
      parseEnv({
        DATABASE_URL: 'postgres://mcp:s3cret@db.internal:5432/docs',
        DOCS_EMBEDDER_MODEL: 'local',
        DOCS_EMBEDDER_DIM: '384',
        DOCS_EMBEDDER_API_KEY: 'sk-abc',
        KBC_JWT_SECRET: 'jwt-xyz',
        KBC_OAUTH_CLIENT_ID: 'client-123',
      }),
    );
    // Credentials stripped, host/db preserved.
    expect(dump.DATABASE_URL).toBe('postgres://***:***@db.internal:5432/docs');
    // Secrets masked.
    expect(dump.DOCS_EMBEDDER_API_KEY).toBe('***');
    expect(dump.KBC_JWT_SECRET).toBe('***');
    // Non-secrets visible — this is the whole point (see the embedder config at a glance).
    expect(dump.DOCS_EMBEDDER_MODEL).toBe('local');
    expect(dump.DOCS_EMBEDDER_DIM).toBe(384);
    expect(dump.KBC_OAUTH_CLIENT_ID).toBe('client-123');
    // Unset values shown as null so the dump is complete.
    expect(dump.HOSTNAME_SUFFIX).toBeNull();
  });
});

describe('parseEnv', () => {
  it('applies defaults and coerces PORT', () => {
    const env = parseEnv({});
    expect(env.HOST).toBe('localhost');
    expect(env.PORT).toBe(8000);
    expect(env.LOG_LEVEL).toBe('INFO');
    expect(env.APP_ENV).toBe('local');
    expect(env.APP_VERSION).toBe('DEV');
  });

  it('treats empty strings as unset (falls back to defaults)', () => {
    const env = parseEnv({ HOST: '', PORT: '', APP_ENV: '' });
    expect(env.HOST).toBe('localhost');
    expect(env.PORT).toBe(8000);
    expect(env.APP_ENV).toBe('local');
  });

  it('reads provided values and coerces booleans', () => {
    const env = parseEnv({
      PORT: '3000',
      HOSTNAME_SUFFIX: 'keboola.com',
      DD_LOGS_INJECTION: 'true',
    });
    expect(env.PORT).toBe(3000);
    expect(env.HOSTNAME_SUFFIX).toBe('keboola.com');
    expect(env.DD_LOGS_INJECTION).toBe(true);
  });

  it('throws on an invalid PORT when validation is not skipped', () => {
    expect(() => parseEnv({ PORT: 'not-a-number' })).toThrow(/Invalid deployment environment/);
  });

  it('does not throw on a build (SKIP_ENV_VALIDATION)', () => {
    expect(() => parseEnv({ SKIP_ENV_VALIDATION: '1', PORT: 'not-a-number' })).not.toThrow();
  });
});

describe('applyDeploymentDefaults (HOSTNAME_SUFFIX derivation)', () => {
  it('derives the Storage API URL from HOSTNAME_SUFFIX when unset', () => {
    const config = applyDeploymentDefaults(
      new Config(),
      parseEnv({ HOSTNAME_SUFFIX: 'keboola.com' }),
    );
    expect(config.storageApiUrl).toBe('https://connection.keboola.com');
  });

  it('does not override an explicit Storage API URL', () => {
    const base = new Config({ storageApiUrl: 'https://connection.north-europe.azure.keboola.com' });
    const config = applyDeploymentDefaults(base, parseEnv({ HOSTNAME_SUFFIX: 'keboola.com' }));
    expect(config.storageApiUrl).toBe('https://connection.north-europe.azure.keboola.com');
  });

  it('derives OAuth + MCP URLs and default scope only when OAuth is configured', () => {
    const base = new Config({ oauthClientId: 'cid', oauthClientSecret: 'sec' });
    const config = applyDeploymentDefaults(base, parseEnv({ HOSTNAME_SUFFIX: 'keboola.com' }));
    expect(config.oauthServerUrl).toBe('https://connection.keboola.com');
    expect(config.mcpServerUrl).toBe('https://mcp.keboola.com');
    expect(config.oauthScope).toBe('email');
  });

  it('leaves OAuth/MCP URLs unset when OAuth is not configured', () => {
    const config = applyDeploymentDefaults(
      new Config(),
      parseEnv({ HOSTNAME_SUFFIX: 'keboola.com' }),
    );
    expect(config.oauthServerUrl).toBeUndefined();
    expect(config.mcpServerUrl).toBeUndefined();
  });

  it('is a no-op when HOSTNAME_SUFFIX is absent', () => {
    const config = applyDeploymentDefaults(new Config(), parseEnv({}));
    expect(config.storageApiUrl).toBeUndefined();
  });
});
