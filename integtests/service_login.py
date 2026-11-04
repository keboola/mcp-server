"""Headless login of the CI service user (AI-3774): password + TOTP, no browser.

The MCP server accepts only programmatic (`kbc_at_`/`kbc_pat_`) tokens, so the integration tests and the release
smoke test get theirs the way a person does -- by signing in -- instead of from a stored per-project Storage token.

Flow (the same one the Keboola CLI's `auth login-password` uses; no PKCE, no cookies):
    POST /v1/auth/login {grantType: password, email, password}   -> token pair, or {mfaRequired, mfaToken}
    POST /v1/auth/mfa   {mfaToken, type: totp, code}             -> token pair
    POST /v1/auth/pat/exchange {expiresIn, scope: {projects}}    -> child token scoped to the test project

Environment: INTEGTEST_LOGIN_EMAIL, INTEGTEST_LOGIN_PASSWORD, INTEGTEST_LOGIN_TOTP_SECRET (base32 seed).

CLI (used by the workflows, prints one token to stdout):
    python -m integtests.service_login --api-url https://connection.<stack> --project-id 1234

The server accepts each TOTP code once, so two jobs that log in within the same 30 s window make one of them fail
its MFA step; `login` then waits for the next window and tries once more.
"""

import argparse
import base64
import hashlib
import hmac
import os
import struct
import sys
import time

import httpx

LOGIN_EMAIL_ENV_VAR = 'INTEGTEST_LOGIN_EMAIL'
LOGIN_PASSWORD_ENV_VAR = 'INTEGTEST_LOGIN_PASSWORD'
LOGIN_TOTP_SECRET_ENV_VAR = 'INTEGTEST_LOGIN_TOTP_SECRET'
# Lifetime asked for a project-scoped token; a full integration run takes well under this.
SCOPED_TOKEN_TTL_SECONDS = 3 * 60 * 60
TOTP_PERIOD_SECONDS = 30
_TIMEOUT = httpx.Timeout(30.0)


def totp_code(secret_b32: str, *, now: float | None = None, digits: int = 6, period: int = TOTP_PERIOD_SECONDS) -> str:
    """RFC 6238 code for a base32 seed (HMAC-SHA1, the default every authenticator app uses)."""
    cleaned = secret_b32.strip().upper().replace(' ', '').replace('-', '')
    key = base64.b32decode(cleaned + '=' * (-len(cleaned) % 8))
    counter = int((time.time() if now is None else now) // period)
    digest = hmac.new(key, struct.pack('>Q', counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = (struct.unpack('>I', digest[offset : offset + 4])[0] & 0x7FFFFFFF) % 10**digits
    return str(value).zfill(digits)


class LoginError(RuntimeError):
    def __init__(self, path: str, status_code: int) -> None:
        # Never carry the response body: it can echo the submitted credentials.
        super().__init__(f'POST /{path} failed ({status_code}).')
        self.status_code = status_code


def _post(client: httpx.Client, base_url: str, path: str, payload: dict) -> dict:
    response = client.post(f'{base_url}/{path}', json=payload)
    if response.is_error:
        raise LoginError(path, response.status_code)
    return response.json()


def _verify_mfa(client: httpx.Client, base_url: str, mfa_token: str, totp_secret: str) -> dict:
    payload = {'mfaToken': mfa_token, 'type': 'totp'}
    try:
        return _post(client, base_url, 'v1/auth/mfa', {**payload, 'code': totp_code(totp_secret)})
    except LoginError as e:
        if e.status_code not in (
            400,
            401,
        ):  # not a rejected code (rate limit, outage): retrying would only add attempts
            raise
        # Most likely another job used this window's code. Retry once with the next window's code.
        time.sleep(TOTP_PERIOD_SECONDS - time.time() % TOTP_PERIOD_SECONDS + 1)
        return _post(client, base_url, 'v1/auth/mfa', {**payload, 'code': totp_code(totp_secret)})


def _https_base_url(storage_api_url: str) -> str:
    base_url = storage_api_url.rstrip('/')
    if not base_url.startswith('https://'):
        raise RuntimeError('The Storage API URL must use https: credentials and tokens are sent to it.')
    return base_url


def login(
    storage_api_url: str,
    email: str,
    password: str,
    totp_secret: str,
    *,
    transport: httpx.BaseTransport | None = None,
) -> str:
    """Signs the service user in and returns its whole-stack `kbc_at_` access token."""
    base_url = _https_base_url(storage_api_url)
    with httpx.Client(timeout=_TIMEOUT, transport=transport) as client:
        body = _post(client, base_url, 'v1/auth/login', {'grantType': 'password', 'email': email, 'password': password})
        if body.get('mfaRequired'):
            body = _verify_mfa(client, base_url, body['mfaToken'], totp_secret)
    return body['accessToken']


def login_from_env(storage_api_url: str) -> str:
    missing = [
        name
        for name in (LOGIN_EMAIL_ENV_VAR, LOGIN_PASSWORD_ENV_VAR, LOGIN_TOTP_SECRET_ENV_VAR)
        if not os.environ.get(name)
    ]
    if missing:
        raise RuntimeError(f'The service-user login needs these environment variables: {", ".join(missing)}.')
    return login(
        storage_api_url,
        os.environ[LOGIN_EMAIL_ENV_VAR],
        os.environ[LOGIN_PASSWORD_ENV_VAR],
        os.environ[LOGIN_TOTP_SECRET_ENV_VAR],
    )


def project_token(
    storage_api_url: str,
    session_token: str,
    project_id: int | str,
    *,
    transport: httpx.BaseTransport | None = None,
) -> str:
    """Exchanges the session token for a child token that can reach only `project_id`.

    Plain synchronous HTTP, so it is safe to call from sync fixtures and (via a thread) from async tests alike.
    """
    base_url = _https_base_url(storage_api_url)
    payload = {'expiresIn': SCOPED_TOKEN_TTL_SECONDS, 'scope': {'projects': [str(int(project_id))]}}
    with httpx.Client(timeout=_TIMEOUT, transport=transport, headers={'Authorization': f'Bearer {session_token}'}) as c:
        return _post(c, base_url, 'v1/auth/pat/exchange', payload)['accessToken']


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description='Prints a project-scoped kbc_at_ token for the CI service user.')
    parser.add_argument('--api-url', required=True)
    parser.add_argument('--project-id', required=True)
    args = parser.parse_args(argv)
    print(project_token(args.api_url, login_from_env(args.api_url), args.project_id))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        # Only the message: a traceback from a failed login must not end up in a CI log.
        print(f'service login failed: {e}', file=sys.stderr)
        sys.exit(1)
