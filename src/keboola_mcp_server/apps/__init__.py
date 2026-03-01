"""
Helpers for registering MCP App resources and tools.

MCP Apps allow tools to render interactive HTML UIs in sandboxed iframes.
This module provides utilities for wiring the MCP Apps protocol on FastMCP 2.x
using standard MCP primitives (tool meta and resources).

See: https://modelcontextprotocol.io/docs/extensions/apps
"""

import logging
from typing import Any

LOG = logging.getLogger(__name__)

APP_RESOURCE_MIME_TYPE = 'text/html;profile=mcp-app'


def build_app_tool_meta(
    resource_uri: str,
    visibility: list[str] | None = None,
    csp_resource_domains: list[str] | None = None,
    csp_connect_domains: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build the ``_meta.ui`` dict for an MCP App tool.

    :param resource_uri: The ``ui://`` URI linking the tool to its HTML resource.
    :param visibility: Who can call the tool — ``["model"]``, ``["app"]``, or ``["model", "app"]``.
        Default (None) means model-visible only.
    :param csp_resource_domains: Allowed domains for loading external scripts/styles.
    :param csp_connect_domains: Allowed domains for fetch/XHR from the app.
    :return: A dict suitable for passing as ``meta=`` to ``FunctionTool.from_function()``.
    """
    ui: dict[str, Any] = {
        'resourceUri': resource_uri,
    }
    if visibility:
        ui['visibility'] = visibility

    csp: dict[str, Any] = {}
    if csp_resource_domains:
        csp['resource_domains'] = csp_resource_domains
    if csp_connect_domains:
        csp['connect_domains'] = csp_connect_domains
    if csp:
        ui['csp'] = csp

    return {'ui': ui}
