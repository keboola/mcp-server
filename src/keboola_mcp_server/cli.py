"""Command-line interface for the Keboola MCP server."""

import argparse
import asyncio
import logging.config
import os
import pathlib
import sys
from dataclasses import replace
from typing import Optional

from fastmcp import FastMCP
from starlette.middleware import Middleware

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import ForwardSlashMiddleware
from keboola_mcp_server.server import create_server

LOG = logging.getLogger(__name__)


def parse_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        prog='python -m keboola-mcp-server',
        description='Keboola MCP Server',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--transport',
        choices=['stdio', 'sse', 'streamable-http', 'http-compat'],
        default='stdio',
        help='Transport to use for MCP communication',
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level',
    )
    parser.add_argument(
        '--api-url',
        metavar='URL',
        help=(
            'Keboola Storage API URL using format of https://connection.<REGION>.keboola.com. Example: For AWS region '
            '"eu-central-1", use: https://connection.eu-central-1.keboola.com'
        ),
    )
    parser.add_argument('--storage-token', metavar='STR', help='Keboola Storage API token.')
    parser.add_argument('--workspace-schema', metavar='STR', help='Keboola Storage API workspace schema.')
    parser.add_argument('--host', default='localhost', metavar='STR', help='The host to listen on.')
    parser.add_argument('--port', type=int, default=8000, metavar='INT', help='The port to listen on.')
    parser.add_argument(
        '--accept-secrets-in-url',
        action='store_true',
        help='(NOT RECOMMENDED) Read Storage API token and other configuration parameters from the query part '
        'of the MCP server URL. Please note that the URL query parameters are not secure '
        'for sending sensitive information.',
    )
    parser.add_argument('--log-config', type=pathlib.Path, metavar='PATH', help='Logging config file.')

    return parser.parse_args(args)


async def run_server(args: Optional[list[str]] = None) -> None:
    """Runs the MCP server in async mode."""
    parsed_args = parse_args(args)

    log_config: pathlib.Path | None = parsed_args.log_config
    if not log_config and os.environ.get('LOG_CONFIG'):
        log_config = pathlib.Path(os.environ.get('LOG_CONFIG'))
    if log_config and not log_config.is_file():
        LOG.warning(f'Invalid log config file: {log_config}. Using default logging configuration.')
        log_config = None

    if log_config:
        logging.config.fileConfig(log_config, disable_existing_loggers=False)
    else:
        logging.basicConfig(
            format='%(asctime)s %(name)s %(levelname)s: %(message)s',
            level=parsed_args.log_level,
            stream=sys.stderr,
        )

    # Create config from the CLI arguments
    config = Config(
        storage_api_url=parsed_args.api_url,
        storage_token=parsed_args.storage_token,
        workspace_schema=parsed_args.workspace_schema,
        accept_secrets_in_url=parsed_args.accept_secrets_in_url,
    )

    try:
        # Create and run the server
        if parsed_args.transport == 'stdio':
            runtime_config = ServerRuntimeInfo(transport=parsed_args.transport)
            keboola_mcp_server: FastMCP = create_server(config, runtime_info=runtime_config)
            if config.oauth_client_id or config.oauth_client_secret:
                raise RuntimeError('OAuth authorization can only be used with HTTP-based transports.')
            await keboola_mcp_server.run_async(transport=parsed_args.transport)
        elif parsed_args.transport == 'http-compat':
            # Compatibility mode to support both Streamable-HTTP and SSE transports.
            # SSE transport is deprecated and will be removed in the future.
            # Supporting both transports is implemented by creating a parent app and mounting
            # two apps (SSE and Streamable-HTTP) to it. The custom routes (like health check)
            # are added to the parent app. We use local imports here due to temporary nature of this code.

            from contextlib import asynccontextmanager

            import uvicorn
            from starlette.applications import Starlette

            http_runtime_config = ServerRuntimeInfo('http-compat/streamable-http')
            http_mcp_server, custom_routes = create_server(
                config, runtime_info=http_runtime_config, custom_routes_handling='return'
            )
            http_app = http_mcp_server.http_app(
                path='/',
                transport='streamable-http',
            )

            sse_runtime_config = replace(http_runtime_config, transport='http-compat/sse')
            sse_mcp_server, custom_routes = create_server(
                config, runtime_info=sse_runtime_config, custom_routes_handling='return'
            )
            sse_app = sse_mcp_server.http_app(
                path='/',
                transport='sse',
            )

            @asynccontextmanager
            async def lifespan(app: Starlette):
                async with http_app.lifespan(app):
                    async with sse_app.lifespan(app):
                        yield

            app = Starlette(middleware=[Middleware(ForwardSlashMiddleware)], lifespan=lifespan)
            app.mount('/mcp', http_app)
            app.mount('/sse', sse_app)  # serves /sse/ and /messages
            custom_routes.add_to_starlette(app)

            config = uvicorn.Config(
                app,
                host=parsed_args.host,
                port=parsed_args.port,
                log_config=log_config,
                timeout_graceful_shutdown=0,
                lifespan='on',
            )
            server = uvicorn.Server(config)
            LOG.info(
                f'Starting MCP server with Streamable-HTTP and SSE transports'
                f' on http://{parsed_args.host}:{parsed_args.port}/'
            )

            await server.serve()

        else:
            runtime_config = ServerRuntimeInfo(transport=parsed_args.transport)
            keboola_mcp_server: FastMCP = create_server(config, runtime_info=runtime_config)
            await keboola_mcp_server.run_http_async(
                show_banner=False,
                transport=parsed_args.transport,
                host=parsed_args.host,
                port=parsed_args.port,
                uvicorn_config={'log_config': log_config} if log_config else None,
                # Adding ForwardSlashMiddleware in KeboolaMcpServer's constructor doesn't seem to have any effect.
                # See https://github.com/jlowin/fastmcp/pull/896 for the related changes in the fastmcp==2.9.0 library.
                middleware=[Middleware(ForwardSlashMiddleware)],
            )
    except Exception as e:
        LOG.exception(f'Server failed: {e}')
        sys.exit(1)


def main(args: Optional[list[str]] = None) -> None:
    asyncio.run(run_server(args))


if __name__ == '__main__':
    main()
