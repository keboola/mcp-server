"""Command-line interface for the Keboola MCP server."""

import argparse
import asyncio
import logging
import sys
from typing import Optional

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server

LOG = logging.getLogger(__name__)


def parse_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command line arguments.

    Args:
        args: Command line arguments. If None, uses sys.argv[1:].

    Returns:
        Parsed arguments
    """
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
        '--api-url', default='https://connection.keboola.com', metavar='URL', help='Keboola Storage API URL.')
    parser.add_argument('--storage-token', metavar='STR', help='Keboola Storage API token.')
    parser.add_argument('--workspace-schema', metavar='STR', help='Keboola Storage API workspace schema.')
    parser.add_argument('--host', default='127.0.0.1', metavar='STR', help='The host to listen on.')
    parser.add_argument('--port', type=int, default=8000, metavar='INT', help='The port to listen on.')
    parser.add_argument(
        '--accept-secrets-in-url', action='store_true',
        help='(NOT RECOMMENDED) Read Storage API token and other configuration parameters from the query part '
             'of the MCP server URL. Please note that the URL query parameters are not secure '
             'for sending sensitive information.')

    return parser.parse_args(args)


async def run_server(args: Optional[list[str]] = None) -> None:
    """Run the MCP server in async mode.

    Args:
        args: Command line arguments. If None, uses sys.argv[1:].
    """
    parsed_args = parse_args(args)

    # Configure logging
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
        # Create and run server
        LOG.info(f'Creating server with config: {config}')
        keboola_mcp_server = create_server(config)
        if parsed_args.transport == 'stdio':
            await keboola_mcp_server.run_async(transport=parsed_args.transport)
        else:
            await keboola_mcp_server.run_async(
                transport=parsed_args.transport, host=parsed_args.host, port=parsed_args.port)
    except Exception as e:
        LOG.exception(f'Server failed: {e}')
        sys.exit(1)


def main(args: Optional[list[str]] = None) -> None:
    asyncio.run(run_server(args))


if __name__ == '__main__':
    main()
