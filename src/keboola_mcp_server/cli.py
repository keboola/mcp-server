"""Command-line interface for the Keboola MCP server."""

import argparse
import logging
import sys
from typing import Optional

from .config import Config
from .server import create_server

LOG = logging.getLogger(__name__)


def parse_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command line arguments.

    Args:
        args: Command line arguments. If None, uses sys.argv[1:].

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Keboola MCP Server')
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
    parser.add_argument('--api-url', default='https://connection.keboola.com', help=(
        'Keboola Storage API URL. Default is https://connection.keboola.com. Important for server setup, for now we '
        'support rewriting the API URL within each server session, but in the future we will support only one API URL '
        'per server.'
    ))
    parser.add_argument('--storage-token', default=None, help=(
        'Keboola Storage API token (optional). If not provided, the server will use the token from the request or from'
        'the environment variable based on the transport of the server. This parameter serves mainly for local server '
        'setup, because when accessing the MCP server remotely you need to provide token through request.'
    ))
    parser.add_argument('--workspace-schema', default=None, help=(
        'Keboola Storage API workspace schema (optional). If not provided, the server will use the schema from the '
        'request or from the environment variable based on the transport of the server. This parameter serves mainly '
        'for local server setup, because when accessing the MCP server remotely you need to provide schema through '
        'request.'
    ))

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
    config = Config.from_dict(
        {
            'storage_token': parsed_args.storage_token,
            'storage_api_url': parsed_args.api_url,
            'log_level': parsed_args.log_level,
            'workspace_schema': parsed_args.workspace_schema,
            'transport': parsed_args.transport,
        }
    )

    try:
        # Create and run server
        LOG.info(f'Creating server with config: {config}')
        keboola_mcp_server = create_server(config)
        await keboola_mcp_server.run_async(transport=parsed_args.transport)
    except Exception as e:
        LOG.exception(f'Server failed: {e}')
        sys.exit(1)


def main(args: Optional[list[str]] = None) -> None:
    import asyncio
    asyncio.run(run_server(args))


if __name__ == '__main__':
    main()
