import { type HttpBindings, serve } from '@hono/node-server';
import { RESPONSE_ALREADY_SENT } from '@hono/node-server/utils/response';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { Hono } from 'hono';

import type { Config } from '@/config';
import { logger } from '@/logger';
import {
  buildAuthorizationServerMetadata,
  buildProtectedResourceMetadata,
  InvalidRedirectUriError,
  OAuthHttpError,
  SimpleOAuthProvider,
  validateRedirectUri,
} from '@/oauth';
import { parsePreviewRequest, PreviewHttpError, runPreviewConfigDiff } from '@/preview';
import { createServer, SERVER_NAME, SERVER_VERSION } from '@/server';

type Bindings = HttpBindings;

/** The endpoint where the OAuth server redirects back after the user authorizes. */
const OAUTH_CALLBACK_ENDPOINT = '/oauth/callback';

/**
 * Builds the per-request Config: the base (env/CLI) config with HTTP headers
 * layered on top. `X-*` headers map onto Config fields by name; the OAuth bearer
 * token is taken from the standard `Authorization: Bearer <token>` header.
 */
const configFromHeaders = (base: Config, headers: Headers): Config => {
  const map: Record<string, string | undefined> = {};
  headers.forEach((value, key) => {
    map[key] = value;
  });

  const auth = headers.get('authorization');
  const bearer = auth?.match(/^Bearer\s+(.+)$/i)?.[1];
  if (bearer) {
    map['bearerToken'] = bearer;
  }

  return base.replaceBy(map);
};

/**
 * Whether the OAuth provider can be enabled for this config. OAuth is optional and only
 * wired up for the HTTP transport when both the client ID and secret are configured.
 */
const isOAuthConfigured = (config: Config): boolean =>
  Boolean(config.oauthClientId && config.oauthClientSecret);

/**
 * Registers the OAuth routes on the Hono app, mirroring the Python `SimpleOAuthProvider.get_routes()`
 * plus the GitHub-style `GET /oauth/callback` handler. Gated by {@link isOAuthConfigured}.
 */
const registerOAuthRoutes = (app: Hono<{ Bindings: Bindings }>, config: Config): void => {
  const mcpServerUrl = config.mcpServerUrl!;
  const provider = new SimpleOAuthProvider({
    storageApiUrl: config.storageApiUrl ?? '',
    mcpServerUrl,
    callbackEndpoint: OAUTH_CALLBACK_ENDPOINT,
    clientId: config.oauthClientId!,
    clientSecret: config.oauthClientSecret!,
    serverUrl: config.oauthServerUrl ?? '',
    scope: config.oauthScope ?? '',
    jwtSecret: config.jwtSecret,
  });

  // .well-known metadata documents (discovery).
  app.get('/.well-known/oauth-authorization-server', (c) =>
    c.json(buildAuthorizationServerMetadata(mcpServerUrl)),
  );
  app.get('/.well-known/oauth-protected-resource', (c) =>
    c.json(buildProtectedResourceMetadata(mcpServerUrl)),
  );

  // Dynamic Client Registration: a no-op that echoes back the requested registration.
  // Nothing is persisted, mirroring the Python provider.
  app.post('/register', async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const clientId = `client_${crypto.randomUUID()}`;
    logger.debug(`Client registered: client_id=${clientId}`);
    return c.json(
      {
        ...body,
        client_id: clientId,
        token_endpoint_auth_method: body['token_endpoint_auth_method'] ?? 'none',
      },
      201,
    );
  });

  // Authorization endpoint: validates the redirect URI, then redirects to the OAuth server.
  app.get('/authorize', async (c) => {
    const q = c.req.query();
    const redirectUri = q['redirect_uri'];
    try {
      validateRedirectUri(redirectUri);
    } catch (err) {
      if (err instanceof InvalidRedirectUriError) {
        return c.json({ error: 'invalid_request', error_description: err.message }, 400);
      }
      throw err;
    }

    const scopesParam = q['scope'];
    const authUrl = await provider.authorize(q['client_id'] ?? '', {
      redirectUri: redirectUri!,
      redirectUriProvidedExplicitly: true,
      codeChallenge: q['code_challenge'] ?? '',
      state: q['state'] ?? null,
      scopes: scopesParam ? scopesParam.split(' ') : [],
    });
    return c.redirect(authUrl);
  });

  // GitHub-style callback: the OAuth server redirects here; we redirect back to the client.
  app.get(OAUTH_CALLBACK_ENDPOINT, async (c) => {
    const code = c.req.query('code');
    const state = c.req.query('state');
    if (!code || !state) {
      return c.json({ error: 'invalid_request', error_description: 'Missing code or state' }, 400);
    }
    try {
      const redirect = await provider.handleOAuthCallback(code, state);
      return c.redirect(redirect);
    } catch (err) {
      if (err instanceof OAuthHttpError) {
        return c.json(
          { error: 'invalid_request', error_description: err.message },
          err.status as 400,
        );
      }
      throw err;
    }
  });

  // Token endpoint: handles the authorization_code and refresh_token grants.
  app.post('/token', async (c) => {
    const form = await c.req.parseBody();
    const grantType = String(form['grant_type'] ?? '');
    const clientId = String(form['client_id'] ?? '');
    try {
      if (grantType === 'authorization_code') {
        const authCode = await provider.loadAuthorizationCode(String(form['code'] ?? ''));
        if (!authCode) {
          return c.json(
            { error: 'invalid_grant', error_description: 'Invalid authorization code' },
            400,
          );
        }
        return c.json(await provider.exchangeAuthorizationCode(clientId, authCode));
      }
      if (grantType === 'refresh_token') {
        const refreshToken = await provider.loadRefreshToken(String(form['refresh_token'] ?? ''));
        if (!refreshToken) {
          return c.json(
            { error: 'invalid_grant', error_description: 'Invalid refresh token' },
            400,
          );
        }
        const scopeParam = form['scope'] ? String(form['scope']).split(' ') : undefined;
        return c.json(await provider.exchangeRefreshToken(clientId, refreshToken, scopeParam));
      }
      return c.json({ error: 'unsupported_grant_type' }, 400);
    } catch (err) {
      if (err instanceof OAuthHttpError) {
        return c.json(
          { error: 'invalid_request', error_description: err.message },
          err.status as 400,
        );
      }
      throw err;
    }
  });
};

export const createHttpApp = (baseConfig: Config): Hono<{ Bindings: Bindings }> => {
  const app = new Hono<{ Bindings: Bindings }>();

  app.get('/health-check', (c) => c.json({ status: 'ok' }));
  app.get('/', (c) => c.json({ name: SERVER_NAME, version: SERVER_VERSION }));

  if (isOAuthConfigured(baseConfig)) {
    if (baseConfig.mcpServerUrl) {
      registerOAuthRoutes(app, baseConfig);
      logger.info('OAuth provider enabled for the HTTP transport.');
    } else {
      logger.warn(
        'OAuth client configured but mcpServerUrl is missing; OAuth routes are disabled.',
      );
    }
  }

  // Stateless streamable-HTTP: each POST gets a fresh server + transport, mirroring
  // the Python `stateless_http=True` setup. No session is retained between requests.
  app.post('/mcp', async (c) => {
    const config = configFromHeaders(baseConfig, c.req.raw.headers);
    const body: unknown = await c.req.json().catch(() => undefined);

    const server = createServer(config);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    await server.connect(transport);

    const { incoming, outgoing } = c.env;
    outgoing.on('close', () => {
      void transport.close();
      void server.close();
    });

    await transport.handleRequest(incoming, outgoing, body);
    return RESPONSE_ALREADY_SENT;
  });

  // Custom config-diff preview endpoint (port of the Python Starlette route). It is
  // only registered for the HTTP transport — there is no stdio equivalent. It runs its
  // own authorization (header + project/role/branch) and a read-only client, returning
  // a config diff without performing any write.
  app.post('/preview/configuration', async (c) => {
    const config = configFromHeaders(baseConfig, c.req.raw.headers);
    let body: unknown;
    try {
      body = await c.req.json();
    } catch {
      return c.json({ message: 'Invalid JSON in request body.' }, 400);
    }
    try {
      const rq = parsePreviewRequest(body);
      const resp = await runPreviewConfigDiff(config, rq);
      return c.json(resp);
    } catch (err) {
      if (err instanceof PreviewHttpError) {
        return c.json({ message: err.message }, err.status as 400);
      }
      throw err;
    }
  });

  // Stateless mode has no sessions, so GET (SSE stream) and DELETE (session end)
  // are not supported — match the SDK's stateless contract with 405.
  const notAllowed = {
    jsonrpc: '2.0',
    error: { code: -32000, message: 'Method not allowed.' },
    id: null,
  };
  app.get('/mcp', (c) => c.json(notAllowed, 405));
  app.delete('/mcp', (c) => c.json(notAllowed, 405));

  return app;
};

export const startHttp = (config: Config, host: string, port: number): ReturnType<typeof serve> => {
  const app = createHttpApp(config);
  const server = serve({ fetch: app.fetch, hostname: host, port });
  logger.info(`Starting MCP server with Streamable-HTTP transport on http://${host}:${port}/`);
  return server;
};
