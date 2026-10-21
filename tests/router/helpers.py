"""What the router test modules share: a recording upstream and a client of the router app."""

import httpx
from starlette.testclient import TestClient

from keboola_mcp_server.router.app import create_router_app
from keboola_mcp_server.router.settings import RouterSettings
from keboola_mcp_server.router.stacks import parse_stacks

ROUTER_URL = 'https://mcp-router.example.com'


class Upstream:
    """Records what the router sends to the regional servers and answers with a fixed response."""

    def __init__(self, response: httpx.Response | Exception | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self._response = response or httpx.Response(200, json={'access_token': 'eu-aws.at'})

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def router_client(
    upstream: Upstream | None = None, *, now: float = 120.0, raise_server_exceptions: bool = True, **settings
) -> TestClient:
    router_settings = RouterSettings(router_url=ROUTER_URL, stacks=parse_stacks(None), **settings)
    app = create_router_app(
        router_settings,
        upstream_transport=httpx.MockTransport(upstream or Upstream()),
        # one rate-limit window for the whole test, so a minute boundary cannot reset the counts halfway
        rate_limit_clock=lambda: now,
    )
    return TestClient(app, base_url=ROUTER_URL, follow_redirects=False, raise_server_exceptions=raise_server_exceptions)
