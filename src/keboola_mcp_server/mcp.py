"""
This is the extension of mcp.server.FastMCP and mcp.server.Server classes that allows to attach the "state"
to the SSE session. The state is created by the state factory function that can be plugged in to the MCP server,
and that creates a state which contains arbitrary objects keyed by string identifiers. The factory is given the
query parameters from the HTTP request that initiates the SSE connection.

Example:
def factory(params: HttpRequestParams) -> SessionState:
    return { 'sapi_client': KeboolaClient(params['storage_token']) }

mcp = KeboolaMcpServer(name='SAPI Connector', session_state_factory=factory)

@mcp.tool()
def list_all_buckets(ctx: Context):
    client = ctx.session.state['sapi_client']
    return client.storage_client.buckets.list()

mcp.run(transport='sse')

Issues:
  * The current implementation of FastMCP does not support sending `Context` to the registered
    resources' functions. The parameter is passed only to the registered tools.
"""

import logging
import textwrap
from typing import Any, Optional

from fastmcp import FastMCP
from mcp.types import AnyFunction, ToolAnnotations

from keboola_mcp_server.config import Config

LOG = logging.getLogger(__name__)


class KeboolaMcpServer(FastMCP):
    config: Config | None = None

    def add_tool(
        self,
        fn: AnyFunction,
        name: str | None = None,
        description: str | None = None,
        tags: set[str] | None = None,
        annotations: ToolAnnotations | dict[str, Any] | None = None,
    ) -> None:
        super().add_tool(
            fn=fn,
            name=name,
            description=description or textwrap.dedent(fn.__doc__ or '').strip(),
            tags=tags,
            annotations=annotations,
        )

    def set_config(self, config: Optional[Config] = None) -> None:
        self.config = config
