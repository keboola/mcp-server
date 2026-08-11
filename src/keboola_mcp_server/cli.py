"""Command-line interface for the Keboola MCP server."""

import argparse
import asyncio
import contextlib
import dataclasses
import getpass
import json
import logging.config
import os
import pathlib
import sys
import time
import traceback

import pydantic
from fastmcp import FastMCP
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import ForwardSlashMiddleware, is_read_only_tool, is_semantic_tool
from keboola_mcp_server.server import CustomRoutes, create_server

LOG = logging.getLogger(__name__)


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(
        prog='python -m keboola-mcp-server',
        description='Keboola MCP Server',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
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
    parser.add_argument(
        '--stateless-http',
        action=argparse.BooleanOptionalAction,
        default=True,
        help='Streamable-HTTP session mode. Stateless (default) suits scaled/deployed servers where '
        'any replica handles any request. Use --no-stateless-http for a local server so in-session '
        'state — notably multi-project scope from set_project_scope — persists across requests.',
    )
    parser.add_argument('--log-config', type=pathlib.Path, metavar='PATH', help='Logging config file.')

    subparsers = parser.add_subparsers(dest='command')
    login_parser = subparsers.add_parser(
        'login',
        help='Authenticate the local MCP server via a browser PKCE login and store the leased tokens.',
    )
    login_parser.add_argument(
        '--api-url',
        metavar='URL',
        help='Keboola Storage API URL (e.g. https://connection.<REGION>.keboola.com). '
        'Falls back to KBC_STORAGE_API_URL.',
    )
    login_parser.add_argument(
        '--profile',
        metavar='NAME',
        help='Which local interface this login is for (Claude Desktop, Cursor, a terminal, ...). '
        'Each interface needing its own session should use a distinct profile so they never share '
        'one stored credential/refresh token. Falls back to KBC_LOGIN_PROFILE, then "default".',
    )
    login_parser.add_argument(
        '--project-ids',
        metavar='ID[,ID...]',
        help='Scope this login to these project ids (comma-separated). Skips the interactive prompt. '
        'Required (with this or --all) when not run from a terminal.',
    )
    login_parser.add_argument(
        '--all',
        dest='all_projects',
        action='store_true',
        help='Scope this login to every currently-accessible project. Skips the interactive prompt.',
    )
    login_parser.add_argument(
        '--read-only',
        action='store_true',
        help='Scope this login read-only (no write operations in any scoped project).',
    )
    login_parser.add_argument(
        '--pat',
        action='store_true',
        help='After the browser login, lease a Personal Access Token (kbc_pat_) over the scoped '
        'projects and print it. Requires an MFA code (--totp or --recovery).',
    )
    login_parser.add_argument(
        '--show-token',
        action='store_true',
        help='Also print the session access token (kbc_at_) to stdout — e.g. to pass as a header to a '
        'locally-run streamable-HTTP server. Note: it expires in ~1 hour.',
    )
    login_parser.add_argument(
        '--totp',
        metavar='CODE',
        help='TOTP MFA code for the sudo elevation (--pat). Visible in shell history/`ps` for the '
        'process lifetime — prefer leaving this unset and entering the code at the prompt instead.',
    )
    login_parser.add_argument(
        '--recovery',
        metavar='CODE',
        help='Recovery MFA code for the sudo elevation (--pat); alternative to --totp. Single-use and '
        'high-value — same shell-history/`ps` caveat as --totp; prefer the interactive prompt.',
    )
    login_parser.add_argument(
        '--pat-name',
        metavar='STR',
        default='keboola-mcp-server',
        help='Name for the leased PAT (--pat).',
    )
    login_parser.add_argument(
        '--force',
        action='store_true',
        help='Force a fresh browser login even if a valid stored session exists (e.g. to switch '
        'user/token). Without it, login refreshes the existing session. Also re-prompts for project '
        'scope even if one is already stored.',
    )

    logout_parser = subparsers.add_parser(
        'logout',
        help='Delete the stored PKCE session so the next login starts fresh (switch user/token).',
    )
    logout_parser.add_argument(
        '--api-url',
        metavar='URL',
        help='Stack to log out of (default: KBC_STORAGE_API_URL). Use --all to clear every stack.',
    )
    logout_parser.add_argument(
        '--profile',
        metavar='NAME',
        help='Which local interface to log out (see `login --profile`). Falls back to '
        'KBC_LOGIN_PROFILE, then "default". Ignored with --all, which clears every profile.',
    )
    logout_parser.add_argument('--all', action='store_true', help='Delete stored sessions for all stacks.')

    subparsers.add_parser(
        'migrate',
        help='Applies pending Postgres schema migrations for the OAuth session store, then exits. '
        'Intended to run as a one-shot job before the server deployment rolls out.',
    )

    subparsers.add_parser(
        'gc-sessions',
        help='Ensures upcoming oauth_sessions partitions exist and drops ones past the retention '
        'window, then exits. Intended to run on a recurring schedule (e.g. a kbc-stacks CronJob), '
        'independent of deployments.',
    )

    return parser.parse_args(args)


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


def _parse_project_ids(raw: str) -> list[int]:
    try:
        return [int(x.strip()) for x in raw.split(',') if x.strip()]
    except ValueError:
        raise RuntimeError(f'Could not parse --project-ids value: {raw!r} (expected comma-separated integers).')


def _prompt_project_selection(projects: list) -> tuple[list[int], bool]:
    """Interactively asks which projects to scope this login to. Never returns an implicit
    "everything" without the user seeing the list and choosing it -- see the "Security
    hardening" RFC increment: a local session must be scoped before it's ever usable.

    Skips the "which projects" question when there's only one accessible project -- there's no
    real choice to make, so asking it would just be friction; still asks read-only.
    """
    if len(projects) == 1:
        print(f'\nOnly one accessible project ({projects[0].id}); scoping to it automatically.', file=sys.stderr)
        read_only = input('Read-only (no writes in this project)? [y/N]: ').strip().lower() in ('y', 'yes')
        return [projects[0].id], read_only
    print('\nAccessible projects:', file=sys.stderr)
    for p in projects:
        print(f'  {p.id}' + (f' - {p.name}' if p.name else ''), file=sys.stderr)
    raw = input('\nScope this login to which projects? [a]ll or comma-separated ids (default: all): ').strip()
    if not raw or raw.lower() in ('a', 'all'):
        project_ids = [p.id for p in projects]
    else:
        project_ids = _parse_project_ids(raw)
        valid_ids = {p.id for p in projects}
        if outside := [pid for pid in project_ids if pid not in valid_ids]:
            raise RuntimeError(f'Project(s) {outside} are not accessible with this token.')
    read_only = input('Read-only (no writes in any scoped project)? [y/N]: ').strip().lower() in ('y', 'yes')
    return project_ids, read_only


def _prompt_mfa_code() -> tuple[str | None, str | None]:
    """Prompts for a TOTP or recovery code via hidden input, instead of requiring a CLI argument
    that would sit in shell history/`ps` for the process lifetime -- see the "Security hardening"
    RFC increment. `getpass.getpass` degrades gracefully (visible input, with a stderr warning) on
    a non-interactive stdin, so piped/scripted input still works.
    """
    totp = getpass.getpass('TOTP code (leave blank to use a recovery code instead): ').strip()
    if totp:
        return totp, None
    recovery = getpass.getpass('Recovery code: ').strip()
    if not recovery:
        raise RuntimeError('Leasing a PAT (--pat) requires an MFA code (TOTP or recovery).')
    return None, recovery


async def _local_login_fallback(config: Config, *, allow_interactive: bool) -> Config:
    """Fills in ``config.storage_token`` from the local PKCE `login` credential store when nothing
    else has configured a token or an OAuth client -- so a locally-run server (stdio or
    streamable-http alike) doesn't need `--storage-token`/`KBC_STORAGE_TOKEN` passed explicitly
    once `login` has been run. No-op (returns ``config`` unchanged) when a token is already set,
    there's no Storage API URL to log in against, or OAuth is configured (the deployed server
    case, which authenticates per-session instead).
    """
    if config.storage_token or not config.storage_api_url or config.oauth_client_id or config.oauth_client_secret:
        return config
    from keboola_mcp_server.auth_login import ensure_access_token

    access_token = await ensure_access_token(config.storage_api_url, allow_interactive=allow_interactive)
    return dataclasses.replace(config, storage_token=access_token)


async def _run_login(
    api_url: str | None,
    *,
    profile: str | None = None,
    project_ids_arg: str | None = None,
    all_projects: bool = False,
    read_only: bool = False,
    pat: bool = False,
    totp: str | None = None,
    recovery: str | None = None,
    pat_name: str = 'keboola-mcp-server',
    show_token: bool = False,
    force: bool = False,
) -> None:
    """Establishes a stored session, scoped to an explicit set of projects, and with ``pat=True``
    leases a PAT over that same scope.

    Refresh-first: if a stored session exists and its refresh token is still valid, this refreshes
    (no browser) — so re-running `login` an hour later just leases a fresh access token. A browser
    PKCE login runs only when there is no stored session or the refresh token itself is dead.

    Project scope is chosen once, here, and persisted alongside the tokens (see
    `auth_login.TokenSet`) — a local session is never auto-leased to every project with only a
    prompt-text "ask first" gate; see the "Security hardening" RFC increment. Already-scoped
    sessions keep their existing choice on a plain re-run; pass `--project-ids`/`--all` or
    `--force` to change it.

    With ``pat=True``, additionally leases a Personal Access Token over the same scope
    (sudo with the MFA code → create PAT) and prints it.
    """
    from keboola_mcp_server.auth_login import (
        ensure_access_token,
        forget_tokens,
        introspect_token,
        lease_pat,
        load_tokens,
        perform_login,
        save_tokens,
    )

    storage_api_url = api_url or os.environ.get('KBC_STORAGE_API_URL')
    if not storage_api_url:
        raise RuntimeError('A Storage API URL is required for login: pass --api-url or set KBC_STORAGE_API_URL.')
    if project_ids_arg and all_projects:
        raise RuntimeError('Pass either --project-ids or --all, not both.')

    if force:
        # Drop any stored session and always run the browser flow (e.g. to switch user/token).
        forget_tokens(storage_api_url, profile=profile)
        access_token = (await perform_login(storage_api_url, profile=profile)).access_token
    else:
        # Refresh-first, browser only when dead (interactive: this is the terminal `login` command).
        access_token = await ensure_access_token(storage_api_url, profile=profile, allow_interactive=True)
    tokens = load_tokens(storage_api_url, profile=profile)
    assert tokens is not None  # ensure_access_token/perform_login above always persist one

    if tokens.project_ids is not None and not force and not project_ids_arg and not all_projects:
        # Already scoped from an earlier login (and not asked to change it) -- keep it as-is.
        project_ids, project_read_only = tokens.project_ids, tokens.read_only
    elif project_ids_arg:
        project_ids, project_read_only = _parse_project_ids(project_ids_arg), read_only
    elif all_projects:
        introspection = await introspect_token(storage_api_url, subject_token=access_token)
        project_ids, project_read_only = [p.id for p in introspection.projects], read_only
    elif sys.stdin.isatty():
        introspection = await introspect_token(storage_api_url, subject_token=access_token)
        project_ids, project_read_only = _prompt_project_selection(introspection.projects)
    else:
        raise RuntimeError(
            'A project scope is required for login: pass --project-ids <id,id,...> or --all '
            '(not run from a terminal, so the interactive prompt is unavailable).'
        )
    tokens = dataclasses.replace(tokens, project_ids=project_ids, read_only=project_read_only)
    save_tokens(storage_api_url, tokens, profile=profile)

    remaining = max(0, int(tokens.expires_at - time.time()))
    print(
        f'\n✓ Session ready for {storage_api_url} (access token expires in ~{remaining}s), '
        f'scoped to {len(project_ids)} project(s)' + (', read-only' if project_read_only else '') + '.'
    )

    if show_token:
        # Explicitly requested (e.g. to pass as a header to a local streamable-HTTP server).
        print(f'\nAccess token (kbc_at_, expires in ~{remaining}s):\n\n  {access_token}\n')

    if pat:
        if bool(totp) == bool(recovery):
            if totp or recovery:
                raise RuntimeError('Leasing a PAT (--pat) requires exactly one MFA code: pass --totp or --recovery.')
            totp, recovery = _prompt_mfa_code()
        pat_token = await lease_pat(
            storage_api_url,
            subject_token=access_token,
            project_ids=project_ids,
            totp_code=totp,
            recovery_code=recovery,
            name=pat_name,
        )
        print(f'\n✓ Personal Access Token (valid ~1 month, {len(project_ids)} project(s)):\n\n  {pat_token}\n')


async def _run_logout(api_url: str | None, *, profile: str | None = None, all_stacks: bool = False) -> None:
    """Deletes the stored PKCE session so the next login starts fresh."""
    from keboola_mcp_server.auth_login import forget_tokens

    if all_stacks:
        removed = forget_tokens(None)
        print('✓ Logged out of all stacks.' if removed else 'No stored sessions to remove.')
        return
    storage_api_url = api_url or os.environ.get('KBC_STORAGE_API_URL')
    if not storage_api_url:
        raise RuntimeError(
            'A Storage API URL is required for logout: pass --api-url, set KBC_STORAGE_API_URL, or use --all.'
        )
    removed = forget_tokens(storage_api_url, profile=profile)
    print(f'✓ Logged out of {storage_api_url}.' if removed else f'No stored session for {storage_api_url}.')


async def _run_migrate() -> None:
    """Applies pending Postgres schema migrations for the OAuth session store, then exits.

    Reads the DSN from the same env vars the server itself uses (MCP_DB_URL / KBC_MCP_DB_URL /
    KBC_POSTGRES_DSN) so a migration Job can share the exact same envFrom secret as the deployment.
    """
    import asyncpg

    from keboola_mcp_server.session_store.migrator import apply_migrations
    from keboola_mcp_server.session_store.retention import ensure_partitions

    config = Config().replace_by(os.environ)
    if not config.postgres_dsn:
        raise RuntimeError('A Postgres DSN is required to run migrations: set MCP_DB_URL (or KBC_POSTGRES_DSN).')

    pool = await asyncpg.create_pool(config.postgres_dsn)
    try:
        applied = await apply_migrations(pool)
        # Bootstraps this month's + next month's oauth_sessions partition right after the schema
        # exists, so the app never hits a RANGE-partitioned INSERT with no matching partition on
        # first use -- the same call the recurring gc-sessions job makes on an ongoing basis.
        partitions = await ensure_partitions(pool)
    finally:
        await pool.close()

    if applied:
        print(f"✓ Applied {len(applied)} migration(s): {', '.join(applied)}")
    else:
        print('✓ Schema already up to date -- no migrations applied.')
    if partitions['created']:
        print(f"✓ Ensured oauth_sessions partitions: {', '.join(partitions['created'])}")


async def _run_gc_sessions() -> None:
    """Ensures upcoming oauth_sessions partitions exist and drops ones past the retention window,
    then exits. Reads the DSN from the same env vars the server itself uses, so this can share the
    exact same envFrom secret as the deployment (see cli.py's `migrate` command).
    """
    import asyncpg

    from keboola_mcp_server.session_store.retention import ensure_partitions

    config = Config().replace_by(os.environ)
    if not config.postgres_dsn:
        raise RuntimeError('A Postgres DSN is required to run gc-sessions: set MCP_DB_URL (or KBC_POSTGRES_DSN).')

    pool = await asyncpg.create_pool(config.postgres_dsn)
    try:
        result = await ensure_partitions(pool)
    finally:
        await pool.close()

    created, dropped = result['created'], result['dropped']
    print(f"✓ Partitions created: {', '.join(created) or 'none'}; dropped: {', '.join(dropped) or 'none'}")


async def run_server(args: list[str] | None = None) -> None:
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

    if parsed_args.command == 'login':
        await _run_login(
            getattr(parsed_args, 'api_url', None),
            profile=getattr(parsed_args, 'profile', None),
            project_ids_arg=getattr(parsed_args, 'project_ids', None),
            all_projects=getattr(parsed_args, 'all_projects', False),
            read_only=getattr(parsed_args, 'read_only', False),
            pat=getattr(parsed_args, 'pat', False),
            totp=getattr(parsed_args, 'totp', None),
            recovery=getattr(parsed_args, 'recovery', None),
            pat_name=getattr(parsed_args, 'pat_name', 'keboola-mcp-server'),
            show_token=getattr(parsed_args, 'show_token', False),
            force=getattr(parsed_args, 'force', False),
        )
        return

    if parsed_args.command == 'logout':
        await _run_logout(
            getattr(parsed_args, 'api_url', None),
            profile=getattr(parsed_args, 'profile', None),
            all_stacks=getattr(parsed_args, 'all', False),
        )
        return

    if parsed_args.command == 'migrate':
        await _run_migrate()
        return

    if parsed_args.command == 'gc-sessions':
        await _run_gc_sessions()
        return

    # Create config from the CLI arguments, then apply KBC_* environment overrides up front (not
    # just inside create_server, which does this again but too late for the local-login fallback
    # below to see an env-configured OAuth client id / storage token).
    config = Config(
        storage_api_url=parsed_args.api_url,
        storage_token=parsed_args.storage_token,
        workspace_schema=parsed_args.workspace_schema,
    ).replace_by(os.environ)

    # Local dev convenience, for stdio and streamable-http alike: with no token configured (CLI,
    # env, or OAuth) and a Storage API URL known, use the tokens leased by a prior browser `login`
    # (refreshing them as needed) instead of requiring --storage-token/KBC_STORAGE_TOKEN to be
    # passed explicitly. No-op for a deployed/OAuth-configured server -- see
    # `_local_login_fallback`.
    #
    # Only run the interactive browser login when a real terminal is attached. For stdio, an MCP
    # client launches this process with stdin/stdout as pipes (no TTY) and stdout as the JSON-RPC
    # channel -- an interactive login there would corrupt the protocol and block the initialize
    # handshake. In that case (and for any non-interactive streamable-http launch, e.g. a
    # container) require a prior `login` (or a configured token) and fail fast with guidance
    # instead.
    allow_interactive = sys.stdin.isatty() and sys.stderr.isatty()
    config = await _local_login_fallback(config, allow_interactive=allow_interactive)

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
                http_runtime_config = ServerRuntimeInfo(
                    'http-compat/streamable-http', stateless_http=parsed_args.stateless_http
                )
                mcp_server, custom_routes = create_server(
                    config, runtime_info=http_runtime_config, custom_routes_handling='return'
                )
                http_app: StarletteWithLifespan = mcp_server.http_app(
                    path='/',
                    transport='streamable-http',
                    stateless_http=parsed_args.stateless_http,
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
            _tools = await mcp_server.list_tools(run_middleware=False)
            app.state.mcp_tools_input_schema = {tool.name: tool.parameters for tool in _tools}
            # Used by the /preview/configuration authorization check to enforce X-Read-Only-Mode
            # and the ToolsFilteringMiddleware-parity gating (read-only role, semantic tools).
            app.state.mcp_read_only_tools = {tool.name for tool in _tools if is_read_only_tool(tool)}
            app.state.mcp_semantic_tools = {tool.name for tool in _tools if is_semantic_tool(tool)}

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

    except Exception:
        LOG.exception('Server failed')
        sys.exit(1)


def main(args: list[str] | None = None) -> None:
    asyncio.run(run_server(args))


if __name__ == '__main__':
    main()
