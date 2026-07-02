/**
 * OAuth provider for the MCP server.
 *
 * Port of the Python `keboola_mcp_server.oauth.SimpleOAuthProvider`. It implements a
 * proxying OAuth 2.1 authorization server that delegates the actual authentication to
 * the Keboola OAuth server. To avoid any persistence, all transient state (authorization
 * request state, MCP authorization codes, MCP access/refresh tokens) is carried inside
 * signed + gzipped JWS blobs that the server hands back to the client and later decodes.
 *
 * The provider also mints an extra Storage API (SAPI) token alongside every access token
 * because some Keboola services (AI Service, Jobs Queue) still require the
 * `X-StorageAPI-Token` header instead of `Authorization: Bearer <token>`.
 */

import { CompactSign, compactVerify } from 'jose';
import { gunzipSync, gzipSync } from 'node:zlib';

import { logger } from '@/logger';

const OAUTH_LOG_ALL = Boolean(process.env.KEBOOLA_MCP_SERVER_OAUTH_LOG_ALL);

const RE_LOCALHOST = /^(localhost|127\.0\.0\.1|\[::1]|::1)$/i;

/**
 * Whitelisted redirect-URI hosts, keyed by URL scheme. Mirrors the Python `_ALLOWED_DOMAINS`.
 * Custom schemes (e.g. `cursor://`) require a handler registered in the browser and are used
 * to redirect to a locally running app.
 */
const ALLOWED_DOMAINS: Record<string, RegExp[]> = {
  https: [
    // Any keboola.com/dev subdomain EXCEPT user-deployable data-app subdomains, which live under a
    // '*.hub.<stack>.keboola.com' host. A free-trial user can deploy a data app whose '/callback'
    // would otherwise capture the OAuth code, so we reject any host with a 'hub' DNS label (RISK-76).
    /^(?!(?:.*\.)?hub\.).+\.keboola\.(com|dev)$/i,
    /^(.*\.)?chatgpt\.com$/i,
    /^(.*\.)?claude\.ai$/i,
    /^librechat\.glami-ml\.com$/i, // no subdomains allowed
    /^(.*\.)?make\.com$/i,
    /^api\.devin\.ai$/i, // devin.ai API domain
    /^cloud\.onyx\.app$/i, // onyx.app OAuth callback
    /^global\.consent\.azure-apim\.net$/i, // Azure APIM consent domain
    /^n8n\.groupondev\.com$/i,
    /^n8n-business\.groupondev\.com$/i,
    /^n8n-merchant\.groupondev\.com$/i,
    /^n8n-llm-traffic\.groupondev\.com$/i,
    /^n8n-finance\.groupondev\.com$/i,
    /^n8n-playground\.groupondev\.com$/i,
    /^n8n-staging\.groupondev\.com$/i,
  ],
  http: [RE_LOCALHOST],
  cursor: [/^(anysphere\.cursor-retrieval|anysphere\.cursor-mcp)$/i],
};

/** Logs sensitive information only when `KEBOOLA_MCP_SERVER_OAUTH_LOG_ALL` is set. */
const logDebug = (msg: string): void => {
  if (OAUTH_LOG_ALL) {
    logger.debug(msg);
  }
};

/** Raised when a redirect URI is missing or not on the whitelist. */
export class InvalidRedirectUriError extends Error {}

/** Raised for HTTP-level errors during the OAuth flow; carries an HTTP status code. */
export class OAuthHttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = 'OAuthHttpError';
  }
}

/** Mirrors the Python `mcp.server.auth.provider.AccessToken`. */
export type AccessToken = {
  token: string;
  client_id: string;
  scopes: string[];
  expires_at: number | null;
  resource?: string | null;
};

/** Mirrors the Python `RefreshToken`. */
export type RefreshToken = {
  token: string;
  client_id: string;
  scopes: string[];
  expires_at: number | null;
};

/** Access token wrapping the delegate OAuth token plus the extra SAPI token. */
export type ProxyAccessToken = AccessToken & {
  delegate: AccessToken;
  // Created by the MCP server for calling AI Service and Jobs Queue, which do not yet
  // support 'Authorization: Bearer <access-token>'.
  sapi_token: string;
};

/** Refresh token wrapping the delegate OAuth refresh token. */
export type ProxyRefreshToken = RefreshToken & {
  delegate: RefreshToken;
};

/** Authorization code carrying the OAuth tokens, mirroring `_ExtendedAuthorizationCode`. */
export type ExtendedAuthorizationCode = {
  code: string;
  scopes: string[];
  expires_at: number | null;
  client_id: string;
  code_challenge: string | null;
  redirect_uri: string;
  redirect_uri_provided_explicitly: boolean;
  oauth_access_token: AccessToken;
  oauth_refresh_token: RefreshToken;
};

/** The authorization parameters sent by the downstream MCP OAuth client (e.g. claude.ai). */
export type AuthorizationParams = {
  state?: string | null;
  scopes?: string[] | null;
  codeChallenge: string;
  redirectUri: string;
  redirectUriProvidedExplicitly: boolean;
};

/** OAuth token response, mirroring `mcp.shared.auth.OAuthToken`. */
export type OAuthToken = {
  access_token: string;
  refresh_token: string;
  token_type: 'Bearer';
  expires_in: number;
  scope: string;
};

type ParsedRedirectUri = {
  scheme: string;
  host: string;
  port: number | null;
};

/**
 * Parses a redirect URI into scheme/host/port without normalizing away custom schemes.
 * The host is the authority for hierarchical URIs (`scheme://host[:port]/...`) or, for
 * opaque custom schemes such as `cursor:anysphere.cursor-mcp/...`, the part before the path.
 */
const parseRedirectUri = (uri: string): ParsedRedirectUri | null => {
  const match = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/([^/?#]*)(?:[/?#]|$)/.exec(uri);
  if (!match) {
    // Opaque form: scheme:authority/path (e.g. cursor:anysphere.cursor-mcp/cb)
    const opaque = /^([a-zA-Z][a-zA-Z0-9+.-]*):([^/?#]*)(?:[/?#]|$)/.exec(uri);
    if (!opaque) {
      return null;
    }
    return { scheme: opaque[1]!.toLowerCase(), host: opaque[2] ?? '', port: null };
  }

  const scheme = match[1]!.toLowerCase();
  const authority = match[2] ?? '';
  // IPv6 literal: [::1]:port
  const v6 = /^(\[[^\]]*])(?::(\d+))?$/.exec(authority);
  if (v6) {
    return { scheme, host: v6[1]!, port: v6[2] ? Number(v6[2]) : null };
  }
  const lastColon = authority.lastIndexOf(':');
  if (lastColon !== -1 && /^\d+$/.test(authority.slice(lastColon + 1))) {
    return {
      scheme,
      host: authority.slice(0, lastColon),
      port: Number(authority.slice(lastColon + 1)),
    };
  }
  return { scheme, host: authority, port: null };
};

/**
 * Validates a redirect URI against the whitelist. Mirrors `_OAuthClientInformationFull.validate_redirect_uri`.
 *
 * Because there is no persistent client registry, we require the client to send its redirect URI in the
 * authorization request and discard every URI whose scheme/host is not whitelisted.
 *
 * @returns the redirect URI unchanged when valid.
 * @throws InvalidRedirectUriError when missing, scheme-less, or not whitelisted.
 */
export const validateRedirectUri = (redirectUri: string | null | undefined): string => {
  if (!redirectUri) {
    logger.warn('[validateRedirectUri] No redirect_uri specified.');
    throw new InvalidRedirectUriError('The redirect_uri must be specified.');
  }

  const parsed = parseRedirectUri(redirectUri);
  if (!parsed || !parsed.scheme) {
    logger.warn(`[validateRedirectUri] No scheme in redirect_uri: ${redirectUri}`);
    throw new InvalidRedirectUriError(`Invalid redirect_uri: ${redirectUri}`);
  }

  const allowedDomains = ALLOWED_DOMAINS[parsed.scheme];
  if (allowedDomains) {
    if (!allowedDomains.some((p) => p.test(parsed.host) && fullMatch(p, parsed.host))) {
      logger.warn(`[validateRedirectUri] Unknown domain in redirect_uri: ${redirectUri}`);
      throw new InvalidRedirectUriError(`Invalid redirect_uri: ${redirectUri}`);
    }
  } else {
    logger.warn(`[validateRedirectUri] Forbidden scheme in redirect_uri: ${redirectUri}`);
    throw new InvalidRedirectUriError(`Invalid redirect_uri: ${redirectUri}`);
  }

  logger.info(`[validateRedirectUri] Accepted redirect_uri: ${redirectUri}]`);
  return redirectUri;
};

/** Python's `re.fullmatch` semantics: the pattern must match the entire string. */
const fullMatch = (pattern: RegExp, value: string): boolean => {
  const m = pattern.exec(value);
  return m !== null && m[0] === value;
};

/** Appends params to a base URI's query string, skipping null/undefined. Mirrors `construct_redirect_uri`. */
export const constructRedirectUri = (
  base: string,
  params: Record<string, string | null | undefined>,
): string => {
  const hashIdx = base.indexOf('#');
  const fragment = hashIdx === -1 ? '' : base.slice(hashIdx);
  const withoutFragment = hashIdx === -1 ? base : base.slice(0, hashIdx);

  const queryIdx = withoutFragment.indexOf('?');
  const head = queryIdx === -1 ? withoutFragment : withoutFragment.slice(0, queryIdx);
  const existing = queryIdx === -1 ? '' : withoutFragment.slice(queryIdx + 1);

  const search = new URLSearchParams(existing);
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined) {
      search.append(key, value);
    }
  }

  const query = search.toString();
  return `${head}${query ? `?${query}` : ''}${fragment}`;
};

/** Joins a path onto a base URL the way Python's `urljoin(base, path)` does for absolute paths. */
const urljoin = (base: string, path: string): string => new URL(path, base).toString();

export type SimpleOAuthProviderOptions = {
  storageApiUrl: string;
  mcpServerUrl: string;
  callbackEndpoint: string;
  clientId: string;
  clientSecret: string;
  serverUrl: string;
  scope: string;
  jwtSecret?: string;
};

const ceilToHour = (seconds: number): number => Math.ceil(seconds / 3600) * 3600;

const randomHex = (bytes: number): string => {
  const arr = new Uint8Array(bytes);
  globalThis.crypto.getRandomValues(arr);
  return Array.from(arr, (b) => b.toString(16).padStart(2, '0')).join('');
};

const nowSeconds = (): number => Date.now() / 1000;

/**
 * Proxying OAuth provider. Port of `SimpleOAuthProvider`.
 *
 * Dynamic Client Registration is supported but never persisted: `getClient`/`registerClient`
 * are effectively no-ops, redirect-URI and scope validation are relaxed (we instead whitelist
 * redirect URIs at authorize time), and all state travels inside signed JWS blobs.
 */
export class SimpleOAuthProvider {
  private readonly sapiTokensUrl: string;
  private readonly mcpCallbackUrl: string;
  private readonly oauthClientId: string;
  private readonly oauthClientSecret: string;
  private readonly oauthServerAuthUrl: string;
  private readonly oauthServerTokenUrl: string;
  private readonly oauthScope: string;
  private readonly jwtSecret: Uint8Array;

  constructor(opts: SimpleOAuthProviderOptions) {
    this.sapiTokensUrl = urljoin(opts.storageApiUrl, '/v2/storage/tokens');
    this.mcpCallbackUrl = urljoin(opts.mcpServerUrl, opts.callbackEndpoint);
    this.oauthClientId = opts.clientId;
    this.oauthClientSecret = opts.clientSecret;
    this.oauthServerAuthUrl = urljoin(opts.serverUrl, '/oauth/authorize');
    this.oauthServerTokenUrl = urljoin(opts.serverUrl, '/oauth/token');
    this.oauthScope = opts.scope;
    this.jwtSecret = new TextEncoder().encode(opts.jwtSecret || randomHex(32));
  }

  /**
   * Creates the URL that redirects to the OAuth server for authorization. The state parameter
   * is a signed JWS carrying all authorization parameters and expiring after 5 minutes.
   */
  async authorize(clientId: string, params: AuthorizationParams): Promise<string> {
    const scopes = params.scopes ?? [];
    const state = {
      redirect_uri: params.redirectUri,
      redirect_uri_provided_explicitly: String(params.redirectUriProvidedExplicitly),
      // the scopes sent by the MCP server's OAuth client (e.g. claude.ai)
      scopes,
      code_challenge: params.codeChallenge,
      state: params.state ?? null,
      client_id: clientId,
      expires_at: nowSeconds() + 5 * 60, // 5 minutes from now
    };
    const stateJwt = await this.encode(state);

    const urlParams: Record<string, string> = {
      client_id: this.oauthClientId,
      response_type: 'code',
      redirect_uri: this.mcpCallbackUrl,
      state: stateJwt,
      // send no scopes to Keboola OAuth server and let it use its own default scope
    };

    return constructRedirectUri(this.oauthServerAuthUrl, urlParams);
  }

  /**
   * Handles the callback from the OAuth server: validates the state, exchanges the code with the
   * OAuth server, and returns the redirect URL back to the downstream AI assistant OAuth client.
   */
  async handleOAuthCallback(code: string, state: string): Promise<string> {
    let stateData: Record<string, unknown> | undefined;
    try {
      stateData = await this.decode(state);
    } catch {
      logDebug(`[handleOAuthCallback] Invalid state: ${state}`);
      throw new OAuthHttpError(400, 'Invalid state parameter');
    }

    if (!stateData) {
      throw new OAuthHttpError(400, 'Invalid state parameter');
    }

    if ((stateData['expires_at'] as number) < nowSeconds()) {
      logDebug(`[handleOAuthCallback] Expired state`);
      throw new OAuthHttpError(400, 'Invalid state parameter');
    }

    const response = await this.fetchJson(this.oauthServerTokenUrl, {
      client_id: this.oauthClientId,
      client_secret: this.oauthClientSecret,
      code,
      grant_type: 'authorization_code',
      // Keboola OAuth server requires the redirect_uri; the GitHub one does not.
      redirect_uri: this.mcpCallbackUrl,
    });

    if (response.status !== 200) {
      logger.error(
        `[handleOAuthCallback] Failed to exchange code for token, OAuth server response: ` +
          `status=${response.status}, text=${response.text}`,
      );
      throw new OAuthHttpError(
        400,
        `Failed to exchange code for token: status=${response.status}, text=${response.text}`,
      );
    }

    const data = response.json as Record<string, unknown>;
    if ('error' in data) {
      logger.error(
        `[handleOAuthCallback] Error when exchanging code for token: data=${JSON.stringify(data)}`,
      );
      throw new OAuthHttpError(400, String(data['error_description'] ?? data['error']));
    }

    const redirectUri = stateData['redirect_uri'] as string;
    const scopes = stateData['scopes'] as string[];
    const [accessToken, refreshToken] = this.readOauthTokens(data, scopes);

    const authCode = {
      code: `mcp_${randomHex(16)}`,
      client_id: stateData['client_id'],
      redirect_uri: redirectUri,
      redirect_uri_provided_explicitly: stateData['redirect_uri_provided_explicitly'] === 'True',
      expires_at: Math.trunc(nowSeconds() + 5 * 60), // 5 minutes from now
      scopes,
      code_challenge: stateData['code_challenge'],
      oauth_access_token: accessToken,
      oauth_refresh_token: refreshToken,
    };
    const authCodeJwt = await this.encode(authCode);

    return constructRedirectUri(redirectUri, {
      code: authCodeJwt,
      state: stateData['state'] as string | null,
      code_challenge: stateData['code_challenge'] as string | null,
    });
  }

  /**
   * Decodes + validates a JWS authorization code, returning the `ExtendedAuthorizationCode`
   * or `null` when the code is invalid. (Expiry is logged but not enforced here, matching Python.)
   */
  async loadAuthorizationCode(
    authorizationCode: string,
  ): Promise<ExtendedAuthorizationCode | null> {
    let raw: Record<string, unknown>;
    try {
      raw = await this.decode(authorizationCode);
    } catch {
      logDebug(`[loadAuthorizationCode] Invalid authorization_code: ${authorizationCode}`);
      return null;
    }

    const authCode = raw as unknown as ExtendedAuthorizationCode;
    const now = nowSeconds();
    if (authCode.expires_at && authCode.expires_at < now) {
      logger.info(
        `[loadAuthorizationCode] Expired authorization code: expires_at=${authCode.expires_at}, now=${now}`,
      );
    }
    return authCode;
  }

  /**
   * Swaps an authorization code for fresh MCP access + refresh tokens and mints a SAPI token.
   * Mirrors `exchange_authorization_code`.
   */
  async exchangeAuthorizationCode(
    clientId: string,
    authorizationCode: ExtendedAuthorizationCode,
  ): Promise<OAuthToken> {
    const expiresIn = Math.max(
      0,
      Math.trunc((authorizationCode.oauth_access_token.expires_at ?? 0) - nowSeconds()),
    );
    const sapiToken = await this.createSapiToken(
      authorizationCode.oauth_access_token.token,
      ceilToHour(expiresIn * 2), // twice as much as the access token's time out
    );

    const accessToken: ProxyAccessToken = {
      token: `mcp_${randomHex(32)}`,
      client_id: clientId,
      scopes: authorizationCode.scopes,
      expires_at: authorizationCode.oauth_access_token.expires_at,
      delegate: authorizationCode.oauth_access_token,
      sapi_token: sapiToken,
    };
    const accessTokenJwt = await this.encode(accessToken);

    const refreshToken: ProxyRefreshToken = {
      token: `mcp_${randomHex(32)}`,
      client_id: clientId,
      scopes: authorizationCode.scopes,
      expires_at: authorizationCode.oauth_refresh_token.expires_at,
      delegate: authorizationCode.oauth_refresh_token,
    };
    const refreshTokenJwt = await this.encode(refreshToken);

    return {
      access_token: accessTokenJwt,
      refresh_token: refreshTokenJwt,
      token_type: 'Bearer',
      expires_in: expiresIn,
      scope: accessToken.scopes.join(' '),
    };
  }

  /** Decodes + validates a JWS access token. Returns `null` when invalid. */
  async loadAccessToken(token: string): Promise<ProxyAccessToken | null> {
    let raw: Record<string, unknown>;
    try {
      raw = await this.decode(token);
    } catch {
      logDebug(`[loadAccessToken] Invalid token: ${token}`);
      return null;
    }

    const proxyToken = raw as unknown as ProxyAccessToken;
    const now = nowSeconds();
    if (proxyToken.expires_at && proxyToken.expires_at < now) {
      logger.info(
        `[loadAccessToken] Expired access token: expires_at=${proxyToken.expires_at}, now=${now}`,
      );
    }
    return proxyToken;
  }

  /** Decodes + validates a JWS refresh token. Returns `null` when invalid. */
  async loadRefreshToken(refreshToken: string): Promise<ProxyRefreshToken | null> {
    let raw: Record<string, unknown>;
    try {
      raw = await this.decode(refreshToken);
    } catch {
      logDebug(`[loadRefreshToken] Invalid token: ${refreshToken}`);
      return null;
    }

    const proxyToken = raw as unknown as ProxyRefreshToken;
    const now = nowSeconds();
    if (proxyToken.expires_at && proxyToken.expires_at < now) {
      logger.info(
        `[loadRefreshToken] Expired refresh token: expires_at=${proxyToken.expires_at}, now=${now}`,
      );
    }
    return proxyToken;
  }

  /**
   * Swaps a refresh token for fresh MCP access + refresh tokens and mints a SAPI token.
   * Mirrors `exchange_refresh_token`.
   */
  async exchangeRefreshToken(
    clientId: string,
    refreshToken: ProxyRefreshToken,
    scopes?: string[],
  ): Promise<OAuthToken> {
    const response = await this.fetchJson(this.oauthServerTokenUrl, {
      client_id: this.oauthClientId,
      client_secret: this.oauthClientSecret,
      grant_type: 'refresh_token',
      refresh_token: refreshToken.delegate.token,
    });

    if (response.status !== 200) {
      logger.error(
        `[exchangeRefreshToken] Failed to refresh token, OAuth server response: ` +
          `status=${response.status}, text=${response.text}`,
      );
      throw new OAuthHttpError(
        400,
        `Failed to refresh token: status=${response.status}, text=${response.text}`,
      );
    }

    const data = response.json as Record<string, unknown>;
    if ('error' in data) {
      logger.error(
        `[exchangeRefreshToken] Error when refreshing token: data=${JSON.stringify(data)}`,
      );
      throw new OAuthHttpError(400, String(data['error_description'] ?? data['error']));
    }

    const [oauthAccessToken, oauthRefreshToken] = this.readOauthTokens(
      data,
      scopes && scopes.length > 0 ? scopes : refreshToken.scopes,
    );
    const expiresIn = Math.max(0, Math.trunc((oauthAccessToken.expires_at ?? 0) - nowSeconds()));
    const sapiToken = await this.createSapiToken(oauthAccessToken.token, ceilToHour(expiresIn * 2));

    const accessToken: ProxyAccessToken = {
      token: `mcp_${randomHex(32)}`,
      client_id: clientId,
      scopes: oauthAccessToken.scopes,
      expires_at: oauthAccessToken.expires_at,
      delegate: oauthAccessToken,
      sapi_token: sapiToken,
    };
    const accessTokenJwt = await this.encode(accessToken);

    const newRefreshToken: ProxyRefreshToken = {
      token: `mcp_${randomHex(32)}`,
      client_id: clientId,
      scopes: oauthRefreshToken.scopes,
      expires_at: oauthRefreshToken.expires_at,
      delegate: oauthRefreshToken,
    };
    const refreshTokenJwt = await this.encode(newRefreshToken);

    return {
      access_token: accessTokenJwt,
      refresh_token: refreshTokenJwt,
      token_type: 'Bearer',
      expires_in: Math.max(0, Math.trunc((accessToken.expires_at ?? 0) - nowSeconds())),
      scope: accessToken.scopes.join(' '),
    };
  }

  /** No-op: tokens are not stored, so there is nothing to revoke. */
  async revokeToken(token: string, tokenTypeHint?: string): Promise<void> {
    logDebug(`[revokeToken] token=${token}, token_type_hint=${tokenTypeHint ?? ''}`);
  }

  /**
   * Reads the access + refresh tokens from the OAuth server response. Mirrors `_read_oauth_tokens`.
   * The refresh token lifetime is derived from the access token's: roughly one week by default.
   */
  readOauthTokens(data: Record<string, unknown>, scopes: string[]): [AccessToken, RefreshToken] {
    const expiresIn = Number(data['expires_in']); // seconds
    if (expiresIn <= 0) {
      logger.error(
        `[readOauthTokens] Received already expired token: data=${JSON.stringify(data)}`,
      );
      throw new OAuthHttpError(400, 'The original OAuth access token has already expired.');
    }

    const currentTime = Math.trunc(nowSeconds());

    const accessToken: AccessToken = {
      token: data['access_token'] as string,
      client_id: this.oauthClientId,
      scopes,
      // slightly different from 'expires_at' kept by the OAuth server
      expires_at: currentTime + expiresIn,
    };
    const refreshToken: RefreshToken = {
      token: data['refresh_token'] as string,
      client_id: this.oauthClientId,
      scopes,
      // The expires_in refers to the access token; there is no way to know when the refresh
      // token expires. Keboola issues refresh tokens lasting ~1 month and access tokens ~1 hour.
      // We derive the refresh-token lifespan from the access-token lifespan, ~1 week by default.
      expires_at: currentTime + ceilToHour(Math.min(168 * expiresIn, 168 * 3600)),
    };

    return [accessToken, refreshToken];
  }

  /** Creates a Storage API token for services that do not yet support bearer tokens. */
  async createSapiToken(oauthAccessToken: string, expiresIn: number): Promise<string> {
    const res = await fetch(this.sapiTokensUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        Authorization: `Bearer ${oauthAccessToken}`,
      },
      body: JSON.stringify({
        description: 'Created by the MCP server.',
        expiresIn,
        canReadAllFileUploads: true,
        canManageBuckets: true,
      }),
    });

    if (res.status !== 200) {
      const text = await res.text();
      logger.error(
        `[createSapiToken] Failed to create Storage API token, Storage API response: ` +
          `status=${res.status}, text=${text}`,
      );
      throw new OAuthHttpError(
        res.status,
        `Failed to create Storage API token: status=${res.status}, text=${text}`,
      );
    }

    const data = (await res.json()) as { token: string };
    return data.token;
  }

  /** POSTs a form-urlencoded body and returns status + parsed JSON (or raw text). */
  private async fetchJson(
    url: string,
    form: Record<string, string>,
  ): Promise<{ status: number; json: unknown; text: string }> {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Accept: 'application/json',
      },
      body: new URLSearchParams(form).toString(),
      redirect: 'follow',
    });
    const text = await res.text();
    let json: unknown = {};
    try {
      json = JSON.parse(text);
    } catch {
      json = {};
    }
    return { status: res.status, json, text };
  }

  /**
   * Encodes a value as a compact JWS: JSON → UTF-8 → gzip → HS256-signed JWS.
   * Mirrors the Python `_encode` (gzip payload signed via `jwt.api_jws.encode`).
   */
  async encode(data: unknown, key?: string): Promise<string> {
    const jsonBytes = new TextEncoder().encode(JSON.stringify(data));
    const gz = gzipSync(jsonBytes);
    const signingKey = key ? new TextEncoder().encode(key) : this.jwtSecret;
    return new CompactSign(gz).setProtectedHeader({ alg: 'HS256', typ: 'JWT' }).sign(signingKey);
  }

  /** Inverse of {@link encode}. Throws when the signature is invalid. Mirrors `_decode`. */
  async decode(data: string, key?: string): Promise<Record<string, unknown>> {
    const signingKey = key ? new TextEncoder().encode(key) : this.jwtSecret;
    const { payload } = await compactVerify(data, signingKey, { algorithms: ['HS256'] });
    const jsonStr = gunzipSync(Buffer.from(payload)).toString('utf-8');
    return JSON.parse(jsonStr) as Record<string, unknown>;
  }
}

/** Builds the OAuth authorization-server metadata document (`.well-known`). */
export const buildAuthorizationServerMetadata = (mcpServerUrl: string): Record<string, unknown> => {
  const issuer = new URL(mcpServerUrl).origin;
  return {
    issuer,
    authorization_endpoint: `${issuer}/authorize`,
    token_endpoint: `${issuer}/token`,
    registration_endpoint: `${issuer}/register`,
    response_types_supported: ['code'],
    response_modes_supported: ['query'],
    grant_types_supported: ['authorization_code', 'refresh_token'],
    token_endpoint_auth_methods_supported: ['client_secret_post', 'none'],
    code_challenge_methods_supported: ['S256'],
  };
};

/** Builds the OAuth protected-resource metadata document (`.well-known`). */
export const buildProtectedResourceMetadata = (mcpServerUrl: string): Record<string, unknown> => {
  const issuer = new URL(mcpServerUrl).origin;
  return {
    resource: issuer,
    authorization_servers: [issuer],
  };
};
