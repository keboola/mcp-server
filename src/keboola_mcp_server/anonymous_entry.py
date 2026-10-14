"""The unauthenticated entry point a newcomer with no Keboola account uses to get one
(remote_agent_provisioning RFC).

This is the endpoint the whole provisioning feature exists for: someone who has never heard of
Keboola points their assistant at it, calls ``create_project``, and works in the project the
server provisioned for them until they claim it. They have no account, so there is nothing for
them to log in with and no OAuth flow they could complete first.

It is a **second URL**, not an exception carved into the authenticated one, and that is a
protocol constraint rather than a convenience. An MCP client discovers that a server wants OAuth
by being refused: it calls the endpoint with no credentials, gets a 401 with ``WWW-Authenticate``,
and only then runs the authorization flow. An endpoint that answered that same unauthenticated
call with a working anonymous session would never hand out that 401 -- so no client would ever
log in, and every user of the stack, account or not, would silently end up anonymous.

What makes the endpoint safe to expose is not the transport (there is no authentication on it at
all) but the rule enforced here and in `SessionStateMiddleware`: a request arriving this way gets
**no credential it did not provision for itself**.

* Nothing the caller sends counts as one. A credential header is refused outright rather than
  ignored, so a client that believes it is authenticated finds out here instead of quietly
  provisioning throwaway projects under an anonymous session.
* Nothing the *server* holds counts as one either. `strip_credentials` clears the whole credential
  side of the config -- a `KBC_STORAGE_TOKEN` in the pod's environment, a local login file, an
  auto-leased scope -- so the only token such a request can ever act under is the one
  `resolve_session` hands back for a provisioned session whose handle the caller already has.
"""

import dataclasses
import json
import logging
from typing import Final

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.config import Config

LOG = logging.getLogger(__name__)

#: Mount path of the unauthenticated app. Deliberately not the same shape as `/mcp`: an operator
#: reading a config, a log line or an ingress rule should not have to look twice to tell which of
#: the two endpoints they are looking at.
ANONYMOUS_MOUNT_PATH: Final[str] = '/mcp-anonymous'

#: Marks the ASGI scope of a request that arrived on the anonymous endpoint. Set by
#: `AnonymousEntryMiddleware`, read by `is_anonymous_entry`. A caller cannot forge it: it is set on
#: the scope by the middleware wrapping that one mounted app, never read from a header.
ANONYMOUS_ENTRY_SCOPE_KEY: Final[str] = 'keboola.anonymous_entry'


def is_anonymous_entry(http_rq: object | None) -> bool:
    """Whether this request came in through the unauthenticated endpoint."""
    scope = getattr(http_rq, 'scope', None)
    return bool(scope.get(ANONYMOUS_ENTRY_SCOPE_KEY)) if isinstance(scope, dict) else False


def carries_a_credential(headers: Headers) -> bool:
    """Whether the caller sent anything this server would treat as a Keboola credential.

    Asks `Config` itself rather than matching header names here: `replace_by_headers` is the one
    place that knows every spelling a credential can arrive under (`X-Storage-Api-Token`,
    `KBC_Storage_Token`, the `storage_api_token` alias, ...), and a check that re-listed them would
    silently stop covering a spelling the day one is added. `Authorization` is tested separately
    because it maps to no field name and is routed by hand in `apply_request_config`.
    """
    if headers.get('Authorization'):
        return True
    from_headers = Config().replace_by_headers(headers)
    return bool(from_headers.storage_token or from_headers.bearer_token)


def strip_credentials(config: Config) -> Config:
    """Removes every credential from a request's config, whatever its source.

    Leaves the stack URL and the request's own non-credential settings alone: an anonymous caller
    still needs to reach a stack, and `create_project` needs to know which one.
    """
    return dataclasses.replace(config, storage_token=None, bearer_token=None, project_id=None)


class AnonymousCredentialLeak(RuntimeError):
    """An anonymous request ended up holding a credential that is not its own provisioned session's.

    Fails the request. By construction this cannot happen -- the request arrives with nothing and
    `resolve_session` is the one thing that can hand it a token -- so reaching here means some path
    through `SessionStateMiddleware` started lending credentials to sessions that asked for none,
    and the one endpoint where that is unrecoverable is this one.
    """


def only_provisioned_credential(config: Config, provisioned_session_id: str | None) -> str | None:
    """The single credential an anonymous request may hold: the one `resolve_session` returned for
    the provisioned session whose handle the caller sent. Returns it, to be re-checked later with
    `credential_unchanged`.

    :raises AnonymousCredentialLeak: when the request holds a credential and no provisioned handle.
    """
    if config.storage_token and not provisioned_session_id:
        raise AnonymousCredentialLeak(
            'A request on the anonymous endpoint acquired a credential without holding a provisioned '
            'session handle. Refusing to serve it.'
        )
    return config.storage_token


def credential_unchanged(config: Config, expected: str | None) -> None:
    """Re-checks that nothing *after* the handle was resolved swapped the credential for another.

    Needed because the paths that run after it do not all know who they are serving:
    `_resolve_local_tokens` sees a provisioned `kbc_at_` token on a non-deployed server, reads it as
    an ordinary local programmatic session, and refreshes it from the host's own login store -- which
    on a dev machine with the flag on would hand the anonymous caller the operator's identity.

    :raises AnonymousCredentialLeak: when the credential is no longer the provisioned one.
    """
    if config.storage_token != expected:
        raise AnonymousCredentialLeak(
            'The credential of a request on the anonymous endpoint was replaced after its provisioned '
            'session was resolved. Refusing to serve it.'
        )


class AnonymousEntryMiddleware:
    """Marks every request on the anonymous endpoint, and refuses the ones carrying a credential.

    Pure ASGI (not `BaseHTTPMiddleware`) so it adds no request/response buffering to a transport
    whose whole job is streaming.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self._app(scope, receive, send)
            return

        scope[ANONYMOUS_ENTRY_SCOPE_KEY] = True

        if carries_a_credential(Headers(scope=scope)):
            # Header *names* only; the values are live credentials.
            LOG.info(
                'Refused a credential-carrying request on the anonymous endpoint: '
                f'header_names={list(Headers(scope=scope).keys())}'
            )
            await _refuse(send)
            return

        await self._app(scope, receive, send)


async def _refuse(send: Send) -> None:
    body = json.dumps(
        {
            'error': 'credentials_not_accepted_here',
            'error_description': (
                f'This is the anonymous endpoint ("{ANONYMOUS_MOUNT_PATH}"), which exists only for callers who '
                'have no Keboola account yet: it creates a project for them and signs that session in to it. '
                'It accepts no credentials. Send this request to the authenticated endpoint ("/mcp") instead, '
                'where the credential you are holding is the one that decides what you can reach.'
            ),
        }
    ).encode()
    await send(
        {
            'type': 'http.response.start',
            'status': 400,
            'headers': [(b'content-type', b'application/json'), (b'content-length', str(len(body)).encode())],
        }
    )
    await send({'type': 'http.response.body', 'body': body})
