"""Command-line interface for the Keboola MCP server."""

import argparse
import asyncio
import contextlib
import logging.config
import os
import pathlib
import sys
from typing import Optional

from fastmcp import FastMCP
from starlette.middleware import Middleware
from starlette.routing import Route

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import ForwardSlashMiddleware
from keboola_mcp_server.server import CustomRoutes, create_server

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
        # remove fastmcp's rich handler, which is aggressively set up during "import fastmcp"
        fastmcp_logger = logging.getLogger('fastmcp')
        for hdlr in fastmcp_logger.handlers[:]:
            fastmcp_logger.removeHandler(hdlr)
        fastmcp_logger.propagate = True
        fastmcp_logger.setLevel(logging.NOTSET)
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
    )

    try:
        # Create and run the server
        if parsed_args.transport == 'stdio':
            runtime_config = ServerRuntimeInfo(transport=parsed_args.transport)
            keboola_mcp_server: FastMCP = create_server(config, runtime_info=runtime_config)
            if config.oauth_client_id or config.oauth_client_secret:
                raise RuntimeError('OAuth authorization can only be used with HTTP-based transports.')
            await keboola_mcp_server.run_async(transport=parsed_args.transport)
        else:
            # 'http-compat' is a compatibility mode to support both Streamable-HTTP and SSE transports.
            # SSE transport is deprecated and will be removed in the future.
            # Supporting both transports is implemented by creating a parent app and mounting
            # two apps (SSE and Streamable-HTTP) to it. The custom routes (like health check)
            # are added to the parent app. We use local imports here due to temporary nature of this code.

            from contextlib import asynccontextmanager

            import uvicorn
            from fastmcp.server.http import StarletteWithLifespan
            from starlette.applications import Starlette

            mount_paths: dict[str, StarletteWithLifespan] = {}
            custom_routes: CustomRoutes | None = None
            transports: list[str] = []

            if parsed_args.transport in ['http-compat', 'streamable-http']:
                http_runtime_config = ServerRuntimeInfo('http-compat/streamable-http')
                http_mcp_server, custom_routes = create_server(
                    config, runtime_info=http_runtime_config, custom_routes_handling='return'
                )
                http_app: StarletteWithLifespan = http_mcp_server.http_app(
                    path='/',
                    transport='streamable-http',
                )
                mount_paths['/mcp'] = http_app
                transports.append('Streamable-HTTP')

            if parsed_args.transport in ['http-compat', 'sse']:
                sse_runtime_config = ServerRuntimeInfo('http-compat/sse')
                sse_mcp_server, custom_routes = create_server(
                    config, runtime_info=sse_runtime_config, custom_routes_handling='return'
                )
                sse_app: StarletteWithLifespan = sse_mcp_server.http_app(
                    path='/',
                    transport='sse',
                )

                log_messages: list[str] = []
                for route in sse_app.routes:
                    # make sure that the root path is available for GET requests only
                    # (i.e. POST requests are not allowed)
                    if isinstance(route, Route) and route.path == '/' and not route.methods:
                        route.methods = ['GET', 'HEAD']
                    log_messages.append(str(route))
                LOG.info('SSE Routes:\n{}\n'.format('\n'.join(log_messages)))

                mount_paths['/sse'] = sse_app  # serves /sse/ and /messages
                transports.append('SSE')

            @asynccontextmanager
            async def lifespan(_app: Starlette):
                async with contextlib.AsyncExitStack() as stack:
                    for _inner_app in mount_paths.values():
                        await stack.enter_async_context(_inner_app.lifespan(_app))
                    yield

            app = Starlette(middleware=[Middleware(ForwardSlashMiddleware)], lifespan=lifespan)
            for path, inner_app in mount_paths.items():
                app.mount(path, inner_app)

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
                f'Starting MCP server with {", ".join(transports)} transport{"s" if len(transports) > 1 else ""}'
                f' on http://{parsed_args.host}:{parsed_args.port}/'
            )

            await server.serve()

    except Exception as e:
        LOG.exception(f'Server failed: {e}')
        sys.exit(1)


def main(args: Optional[list[str]] = None) -> None:
    asyncio.run(run_server(args))


if __name__ == '__main__':
    main()
