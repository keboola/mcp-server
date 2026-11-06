"""The name a local MCP client (Codex, ...) reports for itself at `/register`, carried inside its client id (DMD-2600).

Every tool on a loopback IP callback logs in through Connection's one shared `local-mcp-client` row, so the row's name
says nothing about the app. The consent screen and the account's session list can still show the name the tool
reported, worded as unverified (Connection's `ReportedClientName`). Neither this server nor the router keeps state
between `/register` and `/authorize` or `/token`, and the two may run on different replicas or behind the router, so the
name rides in the client id the tool sends back on every request: `<uuid>.<base64url(name)>`.

The name is self-asserted: anyone can register any name, and signing it would prove nothing. It is display-only and
never takes part in a trust decision. It is embedded only when every redirect URI is an http loopback IP callback,
the only clients Connection ever shows a reported name for.
"""

import base64
import binascii
import re
import unicodedata
from urllib.parse import urlparse

from mcp.shared.auth import OAuthClientInformationFull

# Connection caps the name at 48 characters and expects this server to send at most 48 UTF-8 bytes.
MAX_REPORTED_NAME_BYTES = 48
# base64url without padding of MAX_REPORTED_NAME_BYTES bytes.
_MAX_ENCODED_CHARS = 64
_ENCODED = re.compile(r'[A-Za-z0-9_-]+')
_SEPARATOR = '.'


def is_loopback_ip_callback(redirect_uri: str) -> bool:
    """Whether `redirect_uri` is http on 127.0.0.1 or [::1] (any port, no userinfo), the callbacks Connection's
    `local-mcp-client` row accepts. `localhost` is not one: Connection matches it port-exact, and it is not on the row."""
    parsed = urlparse(redirect_uri)
    return (
        parsed.scheme == 'http'
        and parsed.hostname in ('127.0.0.1', '::1')
        and parsed.username is None
        and parsed.password is None
    )


def sanitize_reported_name(name: str) -> str | None:
    """Whitespace collapsed and the result cut to `MAX_REPORTED_NAME_BYTES`, or None when nothing displayable is left.
    A name with a control or format character (tab, bidi, zero-width, ...) is refused rather than stripped, as
    Connection does: what reaches the screen never differs from what the tool sent by more than whitespace and length.
    Connection sanitizes the name again and stays the authority on what it shows.
    """
    if any(unicodedata.category(ch).startswith('C') for ch in name):
        return None
    collapsed = ' '.join(name.split())
    if not collapsed:
        return None
    truncated = collapsed.encode('utf-8')[:MAX_REPORTED_NAME_BYTES].decode('utf-8', 'ignore').rstrip()
    return truncated or None


def embed_reported_name(client_info: OAuthClientInformationFull) -> None:
    """Appends the reported `client_name` to the freshly minted `client_info.client_id` when every redirect URI is a
    loopback IP callback. The SDK's `/register` handler returns this same object, so the tool receives the new id.
    Every other client keeps its plain id."""
    if not client_info.client_id or not client_info.client_name or not client_info.redirect_uris:
        return
    if not all(is_loopback_ip_callback(str(uri)) for uri in client_info.redirect_uris):
        return
    name = sanitize_reported_name(client_info.client_name)
    if name is None:
        return
    encoded = base64.urlsafe_b64encode(name.encode('utf-8')).decode('ascii').rstrip('=')
    client_info.client_id = f'{client_info.client_id}{_SEPARATOR}{encoded}'


def reported_name(client_id: str | None) -> str | None:
    """The name `embed_reported_name` put into `client_id`, sanitized again; None for a plain id or anything that does
    not decode. The client id comes back from the tool, so it is treated as untrusted input."""
    if not client_id or _SEPARATOR not in client_id:
        return None
    encoded = client_id.rsplit(_SEPARATOR, 1)[1]
    if len(encoded) > _MAX_ENCODED_CHARS or not _ENCODED.fullmatch(encoded):
        return None
    try:
        raw = base64.urlsafe_b64decode(encoded + '=' * (-len(encoded) % 4))
        name = raw.decode('utf-8')
    except (binascii.Error, ValueError):
        return None
    return sanitize_reported_name(name)
