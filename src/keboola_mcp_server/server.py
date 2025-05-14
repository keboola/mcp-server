"""MCP server implementation for Keboola Connection."""

import logging
import os
from typing import Literal, Optional

# from mcp.server.fastmcp import FastMCP
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_http_request
from starlette.requests import Request

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import KeboolaMcpServer, SessionParams, SessionState, SessionStateFactory
from keboola_mcp_server.tools.components import add_component_tools
from keboola_mcp_server.tools.doc import add_doc_tools
from keboola_mcp_server.tools.jobs import add_job_tools
from keboola_mcp_server.tools.sql import WorkspaceManager, add_sql_tools
from keboola_mcp_server.tools.storage import add_storage_tools

LOG = logging.getLogger(__name__)

TransportType = Literal['stdio', 'sse', 'streamable-http']
RequestParameterSource = Literal['query_params', 'headers']


def _safe_get_http_request() -> Request | None:
    """
    Gets the active starlette request (the current HTTP request).
    If the request is not available (e.g. using stdio transport), returns None.
    """
    try:
        return get_http_request()
    except RuntimeError:
        return None


def _infer_session_params_from_request(request: Request) -> SessionParams:
    """
    Infers session parameters from the current HTTP request.
    """
    if all(required_field in request.query_params for required_field in Config.required_fields()):
        return dict(request.query_params)
    elif all(required_field in request.headers for required_field in Config.required_fields()):
        return dict(request.headers)
    else:
        LOG.warning('No required fields found in the request. Using empty session params.')
        return {}


def _infer_session_params() -> SessionParams:
    """
    Infers session parameters from the current HTTP request if available, or falls back to environment variables.
    :param request_param_source: Determines whether to use query parameters or headers if both are available. Default
        is 'query_params'.
    """

    request = _safe_get_http_request()
    if request is None:
        # Fallback to environment variables for stdio-based communication because there is no HTTP request
        # So the server is running locally
        return dict(os.environ)
    return _infer_session_params_from_request(request)


def _get_session_params(
    transport: Optional[TransportType] = None, request_param_source: Optional[RequestParameterSource] = None
) -> SessionParams:
    """
    Gets or infers the session parameters given the transport and request parameter source.
    :param transport: The transport type.
    :param request_param_source: Determines whether to use query parameters or headers if both are available.
    """

    if not transport:
        return _infer_session_params()
    elif transport == 'stdio':
        return dict(os.environ)
    elif transport in ('streamable-http', 'sse'):
        request = _safe_get_http_request()
        if request is None:
            raise RuntimeError(
                'No HTTP request found, but required for the state parameters given the selected transport.'
            )
        if not request_param_source:
            return _infer_session_params_from_request(request)
        if request_param_source == 'headers':
            return dict(request.headers)
        elif request_param_source == 'query_params':
            return dict(request.query_params)


def _create_session_state_factory_based_on_transport(config: Optional[Config] = None) -> SessionStateFactory:
    transport = config.transport if config else None
    request_param_source = config.request_param_source if config else None

    def _(params: SessionParams | None = None) -> SessionState:
        if not params:
            LOG.info(
                f'Retrieving session params from transport: {transport} and request_param_source: '
                f'{request_param_source}.'
            )
            params = _get_session_params(transport, request_param_source)
        return _create_session_state_factory(config)(params)

    return _


def _create_session_state_factory(config: Optional[Config] = None) -> SessionStateFactory:
    def _(params: SessionParams | None) -> SessionState:
        params = params or {}
        LOG.info(f'Creating SessionState for params: {params.keys() if params else "None"}.')

        if not config:
            cfg = Config.from_dict(params)
        else:
            cfg = config.replace_by(params)

        LOG.info(f'Creating SessionState from config: {cfg}.')

        state: SessionState = {}
        # Create Keboola client instance
        try:
            if not cfg.storage_token:
                raise ValueError('Storage token is not provided.')
            client = KeboolaClient(cfg.storage_token, cfg.storage_api_url)
            state[KeboolaClient.STATE_KEY] = client
            LOG.info('Successfully initialized Storage API client.')
        except Exception as e:
            # TODO: When using the server remotelly (http transport), we need to handle the case when the storage token
            # is not provided. We do not want to fail the server when private session state is created.
            LOG.error(f'Failed to initialize Keboola client: {e}')
            raise
        try:
            if not cfg.workspace_schema:
                raise ValueError('Workspace schema is not provided.')
            workspace_manager = WorkspaceManager(client, cfg.workspace_schema)
            state[WorkspaceManager.STATE_KEY] = workspace_manager
            LOG.info('Successfully initialized Storage API Workspace manager.')
        except Exception as e:
            LOG.error(f'Failed to initialize Storage API Workspace manager: {e}')
            raise

        return state

    return _


def create_server(config: Optional[Config] = None) -> FastMCP:
    """Create and configure the MCP server.

    Args:
        config: Server configuration. If None, loads from environment.

    Returns:
        Configured FastMCP server instance
    """
    # Initialize FastMCP server with system instructions
    mcp = KeboolaMcpServer(
        name='Keboola Explorer', session_state_factory=_create_session_state_factory_based_on_transport(config)
    )

    add_component_tools(mcp)
    add_doc_tools(mcp)
    add_job_tools(mcp)
    add_storage_tools(mcp)
    add_sql_tools(mcp)

    return mcp
