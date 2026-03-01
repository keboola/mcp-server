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

__all__ = ['APP_RESOURCE_MIME_TYPE', 'build_app_tool_meta', 'build_app_resource_meta']

APP_RESOURCE_MIME_TYPE = 'text/html;profile=mcp-app'


def build_app_tool_meta(
    resource_uri: str,
    visibility: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build the ``_meta.ui`` dict for an MCP App tool.

    Per the MCP Apps spec, the tool ``_meta.ui`` only contains ``resourceUri``
    and ``visibility``.  CSP configuration belongs on the **resource** metadata
    (see :func:`build_app_resource_meta`).

    :param resource_uri: The ``ui://`` URI linking the tool to its HTML resource.
    :param visibility: Who can call the tool — ``["model"]``, ``["app"]``, or ``["model", "app"]``.
        Default (None) means both model and app visible.
    :return: A dict suitable for passing as ``meta=`` to ``FunctionTool.from_function()``.
    """
    ui: dict[str, Any] = {
        'resourceUri': resource_uri,
    }
    if visibility:
        ui['visibility'] = visibility

    return {'ui': ui}


def build_app_resource_meta(
    csp_resource_domains: list[str] | None = None,
    csp_connect_domains: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build the ``_meta.ui`` dict for an MCP App resource.

    Per the MCP Apps spec, the resource ``_meta.ui`` carries CSP and
    permission settings that the host uses to configure the sandbox.

    :param csp_resource_domains: Allowed origins for loading external scripts/styles.
    :param csp_connect_domains: Allowed origins for fetch/XHR from the app.
    :return: A dict suitable for passing as ``meta=`` to ``@mcp.resource()``.
    """
    ui: dict[str, Any] = {}

    csp: dict[str, Any] = {}
    if csp_resource_domains:
        csp['resourceDomains'] = csp_resource_domains
    if csp_connect_domains:
        csp['connectDomains'] = csp_connect_domains
    if csp:
        ui['csp'] = csp

    return {'ui': ui} if ui else {}
