"""The name a local MCP client reports at /register, carried in its client id (DMD-2600)."""

import base64

import pytest
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyUrl

from keboola_mcp_server.reported_client_name import (
    embed_reported_name,
    is_loopback_ip_callback,
    reported_name,
    sanitize_reported_name,
)

_UUID = '0b9f6a1e-2c4d-4e8f-9a0b-1c2d3e4f5a6b'
_ZERO_WIDTH_NAME = 'Co\u200bdex'


def _client(redirect_uris: list[str], client_name: str | None) -> OAuthClientInformationFull:
    return OAuthClientInformationFull(
        client_id=_UUID,
        client_name=client_name,
        redirect_uris=[AnyUrl(uri) for uri in redirect_uris],
        token_endpoint_auth_method='none',
    )


@pytest.mark.parametrize(
    ('redirect_uri', 'expected'),
    [
        ('http://127.0.0.1:53123/callback', True),
        ('http://127.0.0.1/callback', True),
        ('http://[::1]:1/callback', True),
        # on Connection's row only 127.0.0.1 and [::1] are matched without the port; localhost is matched port-exact
        ('http://localhost:53123/callback', False),
        ('https://127.0.0.1:53123/callback', False),
        ('http://user@127.0.0.1:53123/callback', False),
        ('http://127.0.0.2:53123/callback', False),
        ('https://chatgpt.com/connector_platform_oauth_redirect', False),
    ],
)
def test_is_loopback_ip_callback(redirect_uri: str, expected: bool) -> None:
    assert is_loopback_ip_callback(redirect_uri) is expected


@pytest.mark.parametrize(
    ('name', 'expected'),
    [
        ('Codex', 'Codex'),
        ('  Codex \u00a0 CLI  ', 'Codex CLI'),  # whitespace, including non-ASCII spaces, collapsed
        ('C\u00f6dex \u2713', 'C\u00f6dex \u2713'),
        ('a' * 60, 'a' * 48),  # Connection expects at most 48 UTF-8 bytes
        ('a' * 47 + '\u00e9', 'a' * 47),  # a multibyte character straddling the cap is dropped, not split
        ('a' * 47 + ' b', 'a' * 47),  # no trailing space left by the cut
        # refused, not stripped, like Connection: the shown name never differs from the sent one by more than whitespace
        ('Codex\u202eevil', None),
        ('Co\u200bdex', None),
        ('Codex\tCLI', None),
        ('   ', None),
        ('', None),
    ],
)
def test_sanitize_reported_name(name: str, expected: str | None) -> None:
    assert sanitize_reported_name(name) == expected


def test_a_loopback_client_gets_its_name_in_the_client_id() -> None:
    client = _client(['http://127.0.0.1:53123/callback'], 'Codex')

    embed_reported_name(client)

    assert client.client_id is not None
    assert client.client_id.startswith(f'{_UUID}.')
    assert reported_name(client.client_id) == 'Codex'


def test_a_non_ascii_name_round_trips() -> None:
    client = _client(['http://[::1]:40000/callback'], 'C\u00f6dex \u2713')

    embed_reported_name(client)

    assert reported_name(client.client_id) == 'C\u00f6dex \u2713'


@pytest.mark.parametrize(
    ('redirect_uris', 'client_name'),
    [
        # Connection never shows a reported name for these rows, so the id stays plain
        (['https://chatgpt.com/connector_platform_oauth_redirect'], 'ChatGPT'),
        (['http://localhost:8787/callback'], 'Cursor'),
        # every redirect URI must be a loopback IP callback, not just one of them
        (['http://127.0.0.1:53123/callback', 'https://evil.example/cb'], 'Codex'),
        (['http://127.0.0.1:53123/callback'], None),
        (['http://127.0.0.1:53123/callback'], 'Co\u200bdex'),
    ],
)
def test_other_clients_keep_a_plain_client_id(redirect_uris: list[str], client_name: str | None) -> None:
    client = _client(redirect_uris, client_name)

    embed_reported_name(client)

    assert client.client_id == _UUID
    assert reported_name(client.client_id) is None


def _encoded(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip('=')


@pytest.mark.parametrize(
    'client_id',
    [
        None,
        '',
        _UUID,
        f'{_UUID}.',
        f'{_UUID}.not base64!',
        f'{_UUID}.{_encoded(b"a" * 49)}',  # longer than this server ever embeds
        f'{_UUID}.{_encoded(bytes([0xFF, 0xFE]))}',  # not UTF-8
        f'{_UUID}.{_encoded(_ZERO_WIDTH_NAME.encode())}',  # a crafted id still gets the same sanitizing
        f'{_UUID}.QQ=',  # padding is never sent
    ],
)
def test_reported_name_treats_the_client_id_as_untrusted(client_id: str | None) -> None:
    assert reported_name(client_id) is None
