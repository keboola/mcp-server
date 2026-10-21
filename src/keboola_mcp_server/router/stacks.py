"""The router's stack table and the stack key prefix that codes and tokens carry for it."""

import dataclasses
import json
import re
from urllib.parse import urlparse

# A stack key is the part of a router-issued code or token before the first dot, so it cannot contain one.
_STACK_KEY_PATTERN = re.compile(r'^[a-z0-9][a-z0-9-]{0,31}$')
STACK_KEY_SEPARATOR = '.'
_NETLOC_PATTERN = re.compile(r'^[A-Za-z0-9.-]+(:[0-9]{1,5})?$')


@dataclasses.dataclass(frozen=True)
class Stack:
    key: str
    """The prefix of every code and token the stack's MCP server issues for the router (`eu-aws.<code>`)."""
    name: str
    """What the stack picker shows, by region and cloud."""
    upstream: str
    """The origin of the stack's MCP server: the only place the router sends anything for this stack."""
    login_host: str
    """The Keboola login hostname, shown under the name so that users recognise their stack."""


DEFAULT_STACKS: tuple[Stack, ...] = (
    Stack('us-aws', 'US Virginia AWS', 'https://mcp.keboola.com', 'connection.keboola.com'),
    Stack('us-gcp', 'US Virginia GCP', 'https://mcp.us-east4.gcp.keboola.com', 'connection.us-east4.gcp.keboola.com'),
    Stack('eu-aws', 'EU Frankfurt AWS', 'https://mcp.eu-central-1.keboola.com', 'connection.eu-central-1.keboola.com'),
    Stack(
        'eu-azure',
        'EU Ireland Azure',
        'https://mcp.north-europe.azure.keboola.com',
        'connection.north-europe.azure.keboola.com',
    ),
    Stack(
        'eu-gcp',
        'EU Frankfurt GCP',
        'https://mcp.europe-west3.gcp.keboola.com',
        'connection.europe-west3.gcp.keboola.com',
    ),
)


def _upstream_origin(url: str) -> str:
    parsed = urlparse(url)
    local = parsed.hostname in ('localhost', '127.0.0.1')
    if parsed.scheme != 'https' and not (parsed.scheme == 'http' and local):
        raise ValueError(f'A stack upstream must be an https URL: {url!r}')
    credentials = parsed.username is not None or parsed.password is not None
    if not parsed.hostname or parsed.path not in ('', '/') or parsed.query or parsed.fragment or credentials:
        raise ValueError(f'A stack upstream must be an origin (no path, query or credentials): {url!r}')
    if not _NETLOC_PATTERN.match(parsed.netloc):
        # The origin goes into redirect Locations and forwarded request URLs, so it carries nothing but a host and port.
        raise ValueError(f'A stack upstream must be a plain host name: {url!r}')
    return f'{parsed.scheme}://{parsed.netloc}'


def parse_stacks(raw: str | None) -> dict[str, Stack]:
    """The stack table: the five multi-tenant stacks, or the JSON list given in the router's configuration (a stack
    added, or one taken out of routing, is a configuration change). Raises ValueError on a malformed table."""
    if raw is None or not raw.strip():
        stacks = DEFAULT_STACKS
    else:
        entries = json.loads(raw)
        if not isinstance(entries, list) or not entries:
            raise ValueError('The router stack table must be a non-empty JSON list.')
        try:
            stacks = tuple(
                Stack(
                    key=str(e['key']),
                    name=str(e['name']),
                    upstream=_upstream_origin(str(e['upstream'])),
                    login_host=str(e['login_host']),
                )
                for e in entries
            )
        except (KeyError, TypeError) as e:
            raise ValueError(f'A router stack table entry needs key, name, upstream and login_host: {e!r}') from e
    table: dict[str, Stack] = {}
    for stack in stacks:
        if not _STACK_KEY_PATTERN.match(stack.key):
            raise ValueError(f'Invalid stack key {stack.key!r}: lowercase letters, digits and dashes only.')
        if stack.key in table:
            raise ValueError(f'Duplicate stack key {stack.key!r}.')
        table[stack.key] = stack
    return table


def stack_key_of(value: str | None) -> str | None:
    """The stack key a router-issued code or token starts with (`eu-aws.<code>` -> `eu-aws`), or None without one."""
    if not value or STACK_KEY_SEPARATOR not in value:
        return None
    return value.split(STACK_KEY_SEPARATOR, 1)[0]


def router_issuer(router_url: str) -> str:
    """The issuer of the one logical authorization server that the router and the regional servers form: the origin
    of the router URL (RFC 8414 §2: https, no query or fragment). The contract is that the router and every regional
    server derive it with this function, so they cannot disagree: a client compares it with the `iss` of an
    authorization response by exact string (RFC 9207)."""
    parsed = urlparse(router_url)
    local = parsed.hostname in ('localhost', '127.0.0.1')
    if (
        not parsed.hostname
        or parsed.username is not None
        or not (parsed.scheme == 'https' or (parsed.scheme == 'http' and local))
    ):
        raise ValueError(f'The router URL must be an https URL without credentials: {router_url!r}')
    return f'{parsed.scheme}://{parsed.netloc.lower()}'


def router_resource(router_url: str) -> str:
    """The router's MCP endpoint: the `resource` of its protected-resource metadata, which clients send back as the
    RFC 8707 `resource` of their authorization and token requests."""
    return f'{router_issuer(router_url)}/mcp'
