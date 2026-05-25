"""CLI for manually building and querying the per-project search index.

Useful for local e2e testing without spinning up the full MCP server.

Usage::

    export KBC_STORAGE_API_URL=https://connection.<stack>.keboola.com
    export KBC_STORAGE_TOKEN=...
    # optional: export KBC_SEARCH_INDEX_DIR=/tmp/keboola-search-cache

    python -m keboola_mcp_server.search_index build
    python -m keboola_mcp_server.search_index query "customer" --kind table
    python -m keboola_mcp_server.search_index inspect
"""

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.search_index import (
    build_index,
    default_root,
    is_stale,
    path_for,
    run_query,
    verify_and_cache,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
LOG = logging.getLogger('search_index.cli')


def _env_or_exit(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f'Error: ${name} is required', file=sys.stderr)
        sys.exit(2)
    return value


async def _make_client_and_session():
    url = _env_or_exit('KBC_STORAGE_API_URL')
    token = _env_or_exit('KBC_STORAGE_TOKEN')
    client = await KeboolaClient(storage_api_url=url, storage_api_token=token).with_branch_id(None)
    session = await verify_and_cache(client.storage_client, token)
    return client, session


async def _cmd_build(_args: argparse.Namespace) -> int:
    client, session = await _make_client_and_session()
    db_path = await build_index(session, client)
    print(f'Built index at: {db_path}')
    print(f'Project: {session.project_id}')
    return 0


async def _cmd_query(args: argparse.Namespace) -> int:
    _, session = await _make_client_and_session()
    db_path = path_for(session)
    if not db_path.exists():
        print(f'No index file at {db_path}; run "build" first', file=sys.stderr)
        return 3
    hits = run_query(
        db_path=db_path,
        project_id=session.project_id,
        patterns=args.patterns,
        kinds=args.kind,
        limit=args.limit,
    )
    for hit in hits:
        print(f'[{hit.kind:7}] {hit.obj_id}  —  {hit.name}')
        if hit.description:
            print(f'           {hit.description}')
    print(f'\n{len(hits)} hit(s)')
    return 0


async def _cmd_inspect(_args: argparse.Namespace) -> int:
    _, session = await _make_client_and_session()
    db_path = path_for(session)
    print(f'Cache root:   {default_root()}')
    print(f'Project id:   {session.project_id}')
    print(f'Token hash:   {session.token_hash}')
    print(f'DB path:      {db_path}')
    if not db_path.exists():
        print('Status:       MISSING (run "build" to create)')
        return 0
    stat = db_path.stat()
    print(f'Size:         {stat.st_size:,} bytes')
    print(f'Mode:         {oct(stat.st_mode & 0o777)}')
    print(f'Stale:        {is_stale(db_path)}')
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    try:
        counts = dict(conn.execute('SELECT kind, COUNT(*) FROM search GROUP BY kind').fetchall())
        meta = dict(conn.execute('SELECT key, value FROM meta').fetchall())
    finally:
        conn.close()
    print(f'Row counts:   {json.dumps(counts)}')
    print(f'Meta:         {json.dumps(meta, indent=2)}')
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='python -m keboola_mcp_server.search_index')
    sub = parser.add_subparsers(dest='cmd', required=True)

    sub.add_parser('build', help='Build (or rebuild) the index for the current token')

    q = sub.add_parser('query', help='Search the index')
    q.add_argument('patterns', nargs='+', help='One or more patterns (joined with OR)')
    q.add_argument('--kind', action='append', help='Filter by kind (bucket, table). Repeatable.')
    q.add_argument('--limit', type=int, default=20)

    sub.add_parser('inspect', help='Print index metadata and row counts')
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    handlers = {'build': _cmd_build, 'query': _cmd_query, 'inspect': _cmd_inspect}
    return asyncio.run(handlers[args.cmd](args))


if __name__ == '__main__':
    sys.exit(main())
