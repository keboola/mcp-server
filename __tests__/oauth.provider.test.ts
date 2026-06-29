import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import {
  type AccessToken,
  type ExtendedAuthorizationCode,
  InvalidRedirectUriError,
  type RefreshToken,
  SimpleOAuthProvider,
  validateRedirectUri,
} from '@/oauth';

const JWT_KEY = 'secret';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const newProvider = (): SimpleOAuthProvider =>
  new SimpleOAuthProvider({
    storageApiUrl: 'https://sapi',
    mcpServerUrl: 'https://mcp',
    callbackEndpoint: '/callback',
    clientId: 'mcp-server-id',
    clientSecret: 'mcp-server-secret',
    serverUrl: 'https://oauth',
    scope: 'scope',
    jwtSecret: JWT_KEY,
  });

const accessToken = (overrides: Partial<AccessToken> = {}): AccessToken => ({
  token: 'oauth-access-token',
  client_id: 'mcp-server',
  scopes: ['foo'],
  expires_at: null,
  ...overrides,
});

const refreshToken = (overrides: Partial<RefreshToken> = {}): RefreshToken => ({
  token: 'oauth-refresh-token',
  client_id: 'mcp-server',
  scopes: ['foo'],
  expires_at: null,
  ...overrides,
});

const authorizationCode = (opts: {
  scopes?: string[];
  expiresAt?: number;
}): ExtendedAuthorizationCode => ({
  code: 'foo',
  scopes: opts.scopes ?? [],
  expires_at: opts.expiresAt ?? Date.now() / 1000 + 5 * 60, // 5 minutes from now
  client_id: 'foo-client-id',
  code_challenge: 'foo-code-challenge',
  redirect_uri: 'foo://bar',
  redirect_uri_provided_explicitly: true,
  oauth_access_token: accessToken(),
  oauth_refresh_token: refreshToken(),
});

describe('SimpleOAuthProvider', () => {
  describe('loadAuthorizationCode', () => {
    it.each([
      { name: 'valid, no scopes', code: authorizationCode({}), key: JWT_KEY, valid: true },
      {
        name: 'valid, scopes',
        code: authorizationCode({ scopes: ['foo', 'bar'] }),
        key: JWT_KEY,
        valid: true,
      },
      {
        name: 'expired, no scopes',
        code: authorizationCode({ expiresAt: 1 }),
        key: JWT_KEY,
        valid: true,
      },
      { name: 'wrong encryption key', code: authorizationCode({}), key: '!@#$%^&', valid: false },
    ])('$name', async ({ code, key, valid }) => {
      const provider = newProvider();
      const authCodeStr = await provider.encode(code, key);
      const loaded = await provider.loadAuthorizationCode(authCodeStr);
      if (valid) {
        expect(loaded).toEqual(code);
      } else {
        expect(loaded).toBeNull();
      }
    });
  });

  describe('readOauthTokens', () => {
    it.each([
      { rawAt: 'foo', rawRt: 'bar', scopes: ['email'], atExpiresIn: 3600, rtExpiresIn: 168 * 3600 },
      {
        rawAt: 'foo',
        rawRt: 'bar',
        scopes: ['user', 'email'],
        atExpiresIn: 3600,
        rtExpiresIn: 168 * 3600,
      },
      { rawAt: 'foo', rawRt: 'bar', scopes: [], atExpiresIn: 3600, rtExpiresIn: 168 * 3600 },
      // 168 * 1 second rounded up to the nearest hour -> 3600
      { rawAt: 'foo', rawRt: 'bar', scopes: [], atExpiresIn: 1, rtExpiresIn: 3600 },
      { rawAt: 'foo', rawRt: 'bar', scopes: [], atExpiresIn: 7200, rtExpiresIn: 168 * 3600 },
    ])(
      'scopes=$scopes atExpiresIn=$atExpiresIn',
      ({ rawAt, rawRt, scopes, atExpiresIn, rtExpiresIn }) => {
        const provider = newProvider();
        const [at, rt] = provider.readOauthTokens(
          { access_token: rawAt, refresh_token: rawRt, expires_in: atExpiresIn },
          scopes,
        );

        const now = Date.now() / 1000;
        expect(at.token).toBe(rawAt);
        expect(at.scopes).toEqual(scopes);
        expect(atExpiresIn - ((at.expires_at ?? 0) - now)).toBeGreaterThanOrEqual(0);
        expect(atExpiresIn - ((at.expires_at ?? 0) - now)).toBeLessThan(1);

        expect(rt.token).toBe(rawRt);
        expect(rt.scopes).toEqual(scopes);
        expect(rtExpiresIn - ((rt.expires_at ?? 0) - now)).toBeGreaterThanOrEqual(0);
        expect(rtExpiresIn - ((rt.expires_at ?? 0) - now)).toBeLessThan(1);
      },
    );
  });

  describe('validateRedirectUri', () => {
    const cases: [string | null, boolean][] = [
      // === HTTP scheme - localhost only ===
      ['http://localhost:8080/foo', true],
      ['http://localhost:20388/oauth/callback', true],
      ['http://localhost/callback', true],
      ['http://127.0.0.1:1234/bar', true],
      ['http://127.0.0.1:54750/auth/callback', true],
      ['http://127.0.0.1/callback', true],
      // IPv6 localhost
      ['http://[::1]:8080/callback', true],
      ['http://[::1]/callback', true],
      // HTTP to non-localhost should be rejected
      ['http://example.com/callback', false],
      ['http://keboola.com/callback', false],
      ['http://192.168.1.1/callback', false],
      // === HTTPS scheme - whitelisted domains ===
      ['https://foo.keboola.com/bar/baz', true],
      ['https://bar.keboola.dev/baz', true],
      ['https://connection.keboola.com/oauth/callback', true],
      ['https://keboola.com/callback', false], // requires subdomain
      ['https://keboola.dev/callback', false], // requires subdomain
      // Data-app 'hub' subdomains are user-deployable and must be rejected (RISK-76)
      ['https://my-app.hub.keboola.com/callback', false],
      ['https://my-app.hub.north-europe.azure.keboola.com/callback', false],
      ['https://my-app.hub.keboola.dev/callback', false],
      ['https://hub.keboola.com/callback', false], // the hub root itself
      ['https://my-app.hub.us-east4.gcp.keboola.com/callback', false],
      // ChatGPT (subdomain optional)
      ['https://chatgpt.com', true],
      ['https://foo.chatgpt.com/bar', true],
      ['https://chatgpt.com/connector_platform_oauth_redirect', true],
      // Claude (subdomain optional)
      ['https://claude.ai', true],
      ['https://foo.claude.ai/bar', true],
      ['https://claude.ai/api/mcp/auth_callback', true],
      // LibreChat (no subdomains allowed)
      ['https://librechat.glami-ml.com', true],
      ['https://librechat.glami-ml.com/api/mcp/keboola/oauth/callback', true],
      ['https://foo.librechat.glami-ml.com/bar', false], // no subdomains allowed
      // Make.com (subdomain optional)
      ['https://make.com', true],
      ['https://foo.make.com/bar', true],
      ['https://www.make.com/oauth/cb/mcp', true],
      // Devin (exact domain only)
      ['https://api.devin.ai/callback', true],
      ['https://api.devin.ai', true],
      ['https://devin.ai/callback', false], // must be api.devin.ai
      ['https://foo.api.devin.ai/callback', false], // no subdomains
      // Onyx (no subdomains allowed)
      ['https://cloud.onyx.app', true],
      ['https://cloud.onyx.app/mcp/oauth/callback', true],
      ['https://foo.cloud.onyx.app/bar', false], // no subdomains allowed
      ['https://onyx.app/callback', false], // must be cloud.onyx.app
      // Azure APIM (no subdomains allowed)
      ['https://global.consent.azure-apim.net', true],
      ['https://global.consent.azure-apim.net/oauth/callback', true],
      ['https://foo.global.consent.azure-apim.net/bar', false], // no subdomains allowed
      // n8n at Groupon (no subdomains allowed)
      ['https://n8n.groupondev.com', true],
      ['https://n8n.groupondev.com/rest/oauth2-credential/callback', true],
      ['https://n8n-business.groupondev.com', true],
      ['https://n8n-business.groupondev.com/rest/oauth2-credential/callback', true],
      ['https://n8n-merchant.groupondev.com', true],
      ['https://n8n-merchant.groupondev.com/rest/oauth2-credential/callback', true],
      ['https://n8n-llm-traffic.groupondev.com', true],
      ['https://n8n-llm-traffic.groupondev.com/rest/oauth2-credential/callback', true],
      ['https://n8n-finance.groupondev.com', true],
      ['https://n8n-finance.groupondev.com/rest/oauth2-credential/callback', true],
      ['https://n8n-playground.groupondev.com', true],
      ['https://n8n-playground.groupondev.com/rest/oauth2-credential/callback', true],
      ['https://n8n-staging.groupondev.com', true],
      ['https://n8n-staging.groupondev.com/rest/oauth2-credential/callback', true],
      ['https://foo.n8n-playground.groupondev.com/bar', false], // no subdomains allowed
      ['https://n8n-unknown.groupondev.com', false], // not whitelisted
      // Unknown HTTPS domains should be rejected
      ['https://foo.bar.com/callback', false],
      ['https://evil.com/callback', false],
      ['https://fakechatgpt.com/callback', false],
      ['https://evilclaude.ai/callback', false],
      // === Cursor scheme - specific hosts only ===
      ['cursor://anysphere.cursor-retrieval/oauth/user-keboola-Data_warehouse/callback', true],
      ['cursor://anysphere.cursor-mcp/oauth/callback', true],
      ['cursor://anysphere.cursor-mcp/some/path', true],
      // Cursor with unknown hosts should be rejected
      ['cursor://evil.com/callback', false],
      ['cursor://localhost/callback', false],
      ['cursor://anysphere.cursor-other/callback', false],
      // === Unknown/forbidden schemes should be rejected ===
      ['ftp://foo.bar.com', false],
      ['file:///etc/passwd', false],
      ['javascript://alert(1)', false],
      ['data://text/html,<script>alert(1)</script>', false],
      // Custom schemes that are NOT whitelisted should be rejected
      ['vscode://localhost/callback', false],
      ['jetbrains://localhost/callback', false],
      ['zed://localhost/callback', false],
      ['myapp://localhost/callback', false],
      ['evil://localhost/callback', false],
      // === Edge cases ===
      [null, false], // no redirect_uri
    ];

    it.each(cases)('%s -> %s', (uri, valid) => {
      if (valid) {
        expect(validateRedirectUri(uri)).toBe(uri);
      } else {
        expect(() => validateRedirectUri(uri)).toThrow(InvalidRedirectUriError);
      }
    });
  });

  describe('encode/decode', () => {
    it('round-trips an arbitrary payload through the signed gzip JWS', async () => {
      const provider = newProvider();
      const payload = { hello: 'world', nested: { n: [1, 2, 3] } };
      const encoded = await provider.encode(payload);
      expect(await provider.decode(encoded)).toEqual(payload);
    });

    it('rejects a token signed with a different key', async () => {
      const provider = newProvider();
      const encoded = await provider.encode({ a: 1 }, 'other-key');
      await expect(provider.decode(encoded)).rejects.toThrow();
    });
  });

  describe('createSapiToken', () => {
    it('POSTs to the Storage API tokens endpoint and returns the new token', async () => {
      let body: Record<string, unknown> = {};
      let auth: string | null = null;
      server.use(
        http.post('https://sapi/v2/storage/tokens', async ({ request }) => {
          auth = request.headers.get('authorization');
          body = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({ token: 'new-sapi-token' });
        }),
      );

      const provider = newProvider();
      const token = await provider.createSapiToken('oauth-at', 7200);
      expect(token).toBe('new-sapi-token');
      expect(auth).toBe('Bearer oauth-at');
      expect(body).toMatchObject({
        expiresIn: 7200,
        canReadAllFileUploads: true,
        canManageBuckets: true,
      });
    });
  });

  describe('exchangeAuthorizationCode', () => {
    it('mints proxy access/refresh tokens carrying a SAPI token', async () => {
      server.use(
        http.post('https://sapi/v2/storage/tokens', () =>
          HttpResponse.json({ token: 'sapi-from-exchange' }),
        ),
      );

      const provider = newProvider();
      const code = authorizationCode({ scopes: ['email'] });
      code.oauth_access_token = accessToken({ expires_at: Date.now() / 1000 + 3600 });
      code.oauth_refresh_token = refreshToken({ expires_at: Date.now() / 1000 + 168 * 3600 });

      const result = await provider.exchangeAuthorizationCode('claude', code);
      expect(result.token_type).toBe('Bearer');
      expect(result.scope).toBe('email');
      expect(result.expires_in).toBeGreaterThan(0);

      const decodedAccess = await provider.decode(result.access_token);
      expect(decodedAccess['sapi_token']).toBe('sapi-from-exchange');
      expect(decodedAccess['client_id']).toBe('claude');
      expect((decodedAccess['token'] as string).startsWith('mcp_')).toBe(true);
    });
  });
});
