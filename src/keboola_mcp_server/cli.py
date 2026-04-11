"""Command-line interface for the Keboola MCP server."""

import argparse
import asyncio
import contextlib
import json
import logging.config
import os
import pathlib
import sys
import traceback
from typing import Optional

import pydantic
from fastmcp import FastMCP
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse

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
    subparsers = parser.add_subparsers(dest='command')

    # 'run' subcommand (default behavior when no subcommand is given)
    run_parser = subparsers.add_parser('run', help='Run the MCP server')
    _add_run_arguments(run_parser)

    # Also add run arguments to the main parser for backward compatibility
    _add_run_arguments(parser)

    # 'init' subcommand
    init_parser = subparsers.add_parser(
        'init',
        help='Initialize multi-project configuration from a Manage API token',
    )
    init_parser.add_argument(
        '--manage-token',
        metavar='STR',
        required=True,
        help='Keboola Manage API token (Personal Access Token). NOT stored in the output config.',
    )
    init_parser.add_argument(
        '--api-url',
        metavar='URL',
        required=True,
        help='Keboola stack URL (e.g. https://connection.north-europe.azure.keboola.com)',
    )
    init_parser.add_argument(
        '--project-ids',
        metavar='IDS',
        help='Comma-separated list of project IDs to configure (e.g. 12345,67890)',
    )
    init_parser.add_argument(
        '--all',
        action='store_true',
        dest='all_projects',
        help='Configure all projects in the organization',
    )
    init_parser.add_argument(
        '--output',
        metavar='PATH',
        default='.mcp.json',
        help='Output config file path (standard MCP client config format)',
    )
    init_parser.add_argument(
        '--forbid-main-branch-writes',
        action='store_true',
        help='Forbid write operations on the main branch for all projects',
    )

    return parser.parse_args(args)


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Add arguments for the run command."""
    parser.add_argument(
        '--transport',
        choices=['stdio', 'streamable-http', 'http-compat'],
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


def _create_exception_handler(status_code: int = 500, log_exception: bool = False):
    """
    Returns a JSON message response for all unhandled errors from request handlers. The response JSON body
    will show exception message and traceback (if the app runs in the debug mode).

    :param status_code: the HTTP status code to return; if not specified 500 (Server Error) status code is used
    """

    async def _exception_handler(request: Request, exc):
        exc_str = f'{type(exc).__name__}: {exc}'
        if log_exception:
            LOG.exception(f'Unhandled error: {exc_str}')

        if request.app.debug:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            exc_text = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            return JSONResponse({'message': exc_str, 'exception': exc_text}, status_code)

        else:
            return JSONResponse({'message': exc_str}, status_code)

    return _exception_handler


async def _http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse({'message': exc.detail}, status_code=exc.status_code)


_bad_request_handler = _create_exception_handler(status_code=400)
_exception_handlers = {
    HTTPException: _http_exception_handler,
    json.JSONDecodeError: _bad_request_handler,
    pydantic.ValidationError: _bad_request_handler,
    ValueError: _bad_request_handler,
    Exception: _create_exception_handler(status_code=500, log_exception=True),
}


def _interactive_checkbox(
    items: list[dict],
    label_fn: callable,
    single_select: bool = False,
    prompt: str | None = None,
) -> list[dict]:
    """Interactive selector using arrow keys, space to toggle, enter to confirm.

    Controls:
      ↑/↓    Move cursor
      Space  Toggle selection (or select in single-select mode)
      a      Select/deselect all (multi-select only)
      Enter  Confirm
    """
    import termios
    import tty

    selected = [False] * len(items)
    cursor = 0
    if prompt is None:
        if single_select:
            prompt = 'Select one (↑↓ move, Enter confirm):'
        else:
            prompt = 'Select projects (↑↓ move, Space toggle, A all, Enter confirm):'

    def render():
        # Move cursor up to overwrite previous render
        if hasattr(render, '_rendered'):
            sys.stdout.write(f'\033[{len(items) + 1}A')
        sys.stdout.write('\033[J')  # Clear from cursor to end
        print(prompt)
        for i, item in enumerate(items):
            if single_select:
                marker = '▸' if i == cursor else ' '
            else:
                marker = '◉' if selected[i] else '○'
            arrow = '▸ ' if i == cursor else '  '
            label = label_fn(item)
            if i == cursor:
                if single_select:
                    print(f'  {arrow}\033[1m{label}\033[0m')
                else:
                    print(f'  {arrow}{marker} \033[1m{label}\033[0m')
            else:
                if single_select:
                    print(f'  {arrow}{label}')
                else:
                    print(f'  {arrow}{marker} {label}')
        sys.stdout.flush()
        render._rendered = True

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)

        # Initial render (need to print outside raw mode for proper newlines)
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        render()
        tty.setraw(fd)

        while True:
            ch = sys.stdin.read(1)
            if ch == '\r' or ch == '\n':  # Enter
                if single_select:
                    selected = [False] * len(items)
                    selected[cursor] = True
                break
            elif ch == ' ':  # Space - toggle
                if single_select:
                    selected = [False] * len(items)
                    selected[cursor] = True
                    break
                else:
                    selected[cursor] = not selected[cursor]
            elif ch in ('a', 'A') and not single_select:  # Select/deselect all
                all_selected = all(selected)
                selected = [not all_selected] * len(items)
            elif ch == '\x1b':  # Escape sequence (arrow keys)
                ch2 = sys.stdin.read(1)
                if ch2 == '[':
                    ch3 = sys.stdin.read(1)
                    if ch3 == 'A':  # Up
                        cursor = (cursor - 1) % len(items)
                    elif ch3 == 'B':  # Down
                        cursor = (cursor + 1) % len(items)
            elif ch == '\x03':  # Ctrl+C
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                print('\nAborted.')
                sys.exit(1)

            # Re-render
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            render()
            tty.setraw(fd)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    print()  # newline after selection
    return [items[i] for i in range(len(items)) if selected[i]]


async def _fetch_all_projects(manage_client) -> list[dict]:
    """Fetch all projects across all organizations the user has access to."""
    orgs = await manage_client.list_organizations()
    if not orgs:
        print('Error: No organizations found for this token.', file=sys.stderr)
        return []

    all_projects: list[dict] = []
    for org in orgs:
        org_id = org.get('id')
        org_name = org.get('name', 'Unknown')
        try:
            org_detail = await manage_client.get_organization(org_id)
            projects = org_detail.get('projects', [])
            for p in projects:
                p['_org_name'] = org_name
            all_projects.extend(projects)
            if projects:
                print(f'Organization "{org_name}": {len(projects)} project(s)')
        except Exception as e:
            print(f'Warning: Failed to get org {org_id} ({org_name}): {e}', file=sys.stderr)

    return all_projects


async def run_init(parsed_args: argparse.Namespace) -> None:
    """Runs the init command to generate a standard .mcp.json with numbered env vars."""
    from keboola_mcp_server.clients.manage import ManageClient

    api_url = parsed_args.api_url
    manage_token = parsed_args.manage_token
    output_path = pathlib.Path(parsed_args.output)
    forbid_writes = parsed_args.forbid_main_branch_writes

    print(f'Verifying manage token against {api_url}...')
    manage_client = ManageClient(stack_url=api_url, manage_token=manage_token)

    try:
        token_info = await manage_client.verify_token()
    except Exception as e:
        print(f'Error: Failed to verify manage token: {e}', file=sys.stderr)
        sys.exit(1)

    user_info = token_info.get('user', {})
    print(f'Authenticated as: {user_info.get("name", "unknown")} ({user_info.get("email", "unknown")})')

    # Determine which projects to configure
    project_ids: list[int] = []

    if parsed_args.project_ids:
        project_ids = [int(pid.strip()) for pid in parsed_args.project_ids.split(',')]
        print(f'Using specified project IDs: {project_ids}')
    elif parsed_args.all_projects:
        all_projects = await _fetch_all_projects(manage_client)
        if not all_projects:
            print('Error: No projects found.', file=sys.stderr)
            sys.exit(1)
        project_ids = [p['id'] for p in all_projects]
    else:
        # Interactive mode: first select org, then projects within it
        orgs = await manage_client.list_organizations()
        if not orgs:
            print('Error: No organizations found.', file=sys.stderr)
            sys.exit(1)

        # Step 1: Select organization
        if len(orgs) == 1:
            selected_org = orgs[0]
            print(f'\nOrganization: {selected_org["name"]}')
        else:
            selected_orgs = _interactive_checkbox(
                orgs,
                label_fn=lambda o: f'{o["name"]} (ID: {o["id"]})',
                single_select=True,
                prompt='Select an organization:',
            )
            if not selected_orgs:
                print('Error: No organization selected.', file=sys.stderr)
                sys.exit(1)
            selected_org = selected_orgs[0]

        # Step 2: Get projects in that org and select
        org_detail = await manage_client.get_organization(selected_org['id'])
        projects = org_detail.get('projects', [])
        if not projects:
            print(f'Error: No projects found in organization "{selected_org["name"]}".', file=sys.stderr)
            sys.exit(1)

        selected_projects = _interactive_checkbox(
            projects,
            label_fn=lambda p: f'{p["name"]} (ID: {p["id"]})',
            prompt=f'Select projects in "{selected_org["name"]}":',
        )
        project_ids = [p['id'] for p in selected_projects]

        if not project_ids:
            print('Error: No projects selected.', file=sys.stderr)
            sys.exit(1)

    # Create Storage API tokens and build env vars
    env_vars: dict[str, str] = {'KBC_STORAGE_API_URL': api_url}
    created_count = 0

    for i, pid in enumerate(project_ids, 1):
        print(f'Creating Storage API token for project {pid}...')
        try:
            project_info = await manage_client.get_project(pid)
            token_data = await manage_client.create_project_token(pid)
            env_vars[f'KBC_STORAGE_TOKEN_{i}'] = token_data['token']
            created_count += 1
            print(f'  OK: {project_info.get("name", pid)} -> KBC_STORAGE_TOKEN_{i}')
        except Exception as e:
            print(f'  Error: Failed to create token for project {pid}: {e}', file=sys.stderr)

    if created_count == 0:
        print('Error: No projects were successfully configured.', file=sys.stderr)
        sys.exit(1)

    if forbid_writes:
        env_vars['KBC_FORBID_MAIN_BRANCH_WRITES'] = 'true'

    # Write standard .mcp.json (manage token is NOT stored)
    mcp_config = {
        'mcpServers': {
            'keboola': {
                'command': 'uvx',
                'args': ['keboola_mcp_server'],
                'env': env_vars,
            }
        }
    }

    with open(output_path, 'w') as f:
        json.dump(mcp_config, f, indent=2)
        f.write('\n')

    print(f'\nConfig written to {output_path} with {created_count} project(s).')
    print('Note: The manage token was NOT stored in the config file.')


async def run_server(args: Optional[list[str]] = None) -> None:
    """Runs the MCP server in async mode."""
    parsed_args = parse_args(args)

    if parsed_args.command == 'init':
        await run_init(parsed_args)
        return

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

    # Create config from the CLI arguments (env vars are read later in create_server)
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
            # 'http-compat' is an alias for 'streamable-http' kept for backwards compatibility.
            # We use local imports here due to the temporary nature of this code.

            from contextlib import asynccontextmanager

            import uvicorn
            from fastmcp.server.http import StarletteWithLifespan
            from starlette.applications import Starlette

            mount_paths: dict[str, StarletteWithLifespan] = {}
            custom_routes: CustomRoutes | None = None
            transports: list[str] = []
            mcp_server: FastMCP | None = None

            if parsed_args.transport in ['http-compat', 'streamable-http']:
                http_runtime_config = ServerRuntimeInfo('http-compat/streamable-http')
                mcp_server, custom_routes = create_server(
                    config, runtime_info=http_runtime_config, custom_routes_handling='return'
                )
                http_app: StarletteWithLifespan = mcp_server.http_app(
                    path='/',
                    transport='streamable-http',
                    stateless_http=True,
                )
                mount_paths['/mcp'] = http_app
                transports.append('Streamable-HTTP')

            @asynccontextmanager
            async def lifespan(_app: Starlette):
                async with contextlib.AsyncExitStack() as stack:
                    for _inner_app in mount_paths.values():
                        await stack.enter_async_context(_inner_app.lifespan(_app))
                    yield

            app = Starlette(
                middleware=[Middleware(ForwardSlashMiddleware)],
                lifespan=lifespan,
                exception_handlers=_exception_handlers,
            )
            for path, inner_app in mount_paths.items():
                app.mount(path, inner_app)

            custom_routes.add_to_starlette(app)

            assert isinstance(mcp_server, FastMCP)
            app.state.mcp_tools_input_schema = {
                tool.name: tool.parameters for tool in (await mcp_server.get_tools()).values()
            }

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
