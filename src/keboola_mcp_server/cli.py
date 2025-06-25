"""Command-line interface for the Keboola MCP server."""

import argparse
import asyncio
import logging.config
import os
import pathlib
import sys
from typing import Optional

from starlette.middleware import Middleware

from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import ForwardSlashMiddleware
from keboola_mcp_server.server import create_server

LOG = logging.getLogger(__name__)


def parse_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        prog='python -m keboola-mcp-server',
        description='Keboola MCP Server',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--transport',
        choices=['stdio', 'sse', 'streamable-http'],
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
        )
    )
    parser.add_argument('--storage-token', metavar='STR', help='Keboola Storage API token.')
    parser.add_argument('--workspace-schema', metavar='STR', help='Keboola Storage API workspace schema.')
    parser.add_argument('--host', default='localhost', metavar='STR', help='The host to listen on.')
    parser.add_argument('--port', type=int, default=8000, metavar='INT', help='The port to listen on.')
    parser.add_argument(
        '--accept-secrets-in-url', action='store_true',
        help='(NOT RECOMMENDED) Read Storage API token and other configuration parameters from the query part '
             'of the MCP server URL. Please note that the URL query parameters are not secure '
             'for sending sensitive information.')
    parser.add_argument('--log-config', type=pathlib.Path, metavar='PATH', help='Logging config file.')

    return parser.parse_args(args)


async def run_server(args: Optional[list[str]] = None) -> None:
    """Runs the MCP server in async mode."""
    parsed_args = parse_args(args)

    if parsed_args.log_config:
        logging.config.fileConfig(parsed_args.log_config, disable_existing_loggers=False)
    elif os.environ.get('LOG_CONFIG'):
        logging.config.fileConfig(os.environ['LOG_CONFIG'], disable_existing_loggers=False)
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
        keboola_mcp_server = create_server(config)
        if parsed_args.transport == 'stdio':
            if config.oauth_client_id or config.oauth_client_secret:
                raise RuntimeError('OAuth authorization can only be used with HTTP-based transports.')
            await keboola_mcp_server.run_async(transport=parsed_args.transport)
        else:
            await keboola_mcp_server.run_http_async(
                transport=parsed_args.transport,
                host=parsed_args.host,
                port=parsed_args.port,
                # Adding ForwardSlashMiddleware in KeboolaMcpServer's constructor doesn't seem to have any effect.
                # See https://github.com/jlowin/fastmcp/pull/896 for the related changes in the fastmcp==2.9.0 library.
                middleware=[Middleware(ForwardSlashMiddleware)]
            )
    except Exception as e:
        LOG.exception(f'Server failed: {e}')
        sys.exit(1)


def main(args: Optional[list[str]] = None) -> None:
    asyncio.run(run_server(args))


if __name__ == '__main__':
    main()
