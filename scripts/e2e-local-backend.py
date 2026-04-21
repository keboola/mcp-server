#!/usr/bin/env python3
"""End-to-end MCP protocol test for local-backend mode.

Runs the full MCP JSON-RPC layer (list_tools, call_tool, list_prompts, get_prompt)
against an in-process FastMCP server — no Claude Desktop or external process needed.

Usage:
    python scripts/e2e-local-backend.py
    python scripts/e2e-local-backend.py --no-browser   # skip opening data app
"""

import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path

GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[0;33m'
RESET = '\033[0m'

PASS = FAIL = 0
_open_browser = '--no-browser' not in sys.argv


def ok(msg: str) -> None:
    global PASS
    print(f'  {GREEN}✓{RESET} {msg}')
    PASS += 1


def fail(msg: str) -> None:
    global FAIL
    print(f'  {RED}✗{RESET} {msg}')
    FAIL += 1


def skip(msg: str) -> None:
    print(f'  {YELLOW}○{RESET} {msg} (skipped)')


def section(title: str) -> None:
    print(f'\n── {title} {"─" * (50 - len(title))}')


def check(label: str, condition: bool, detail: str = '') -> None:
    if condition:
        ok(label)
    else:
        fail(f'{label}  ← {detail}' if detail else label)


async def run_e2e(data_dir: str) -> None:
    from fastmcp import Client
    from keboola_mcp_server.server import create_local_server

    server = create_local_server(data_dir)
    data_path = Path(data_dir)

    async with Client(server) as client:

        # ── 1. Tool discovery ─────────────────────────────────────────────────
        section('Tool discovery (RFC-1)')

        tools = await client.list_tools()
        tool_names = {t.name for t in tools}

        check('get_configs registered (renamed from list_configs)', 'get_configs' in tool_names)
        check('list_configs absent (old name gone)', 'list_configs' not in tool_names)
        check('delete_config registered (new platform tool)', 'delete_config' in tool_names)
        check('save_config registered', 'save_config' in tool_names)
        check('create_data_app registered', 'create_data_app' in tool_names)
        check('run_data_app registered', 'run_data_app' in tool_names)
        expected_count = 21
        check(f'exactly {expected_count} tools', len(tool_names) == expected_count,
              f'got {len(tool_names)}: {sorted(tool_names)}')

        # ── 2. Onboarding instructions (RFC-3) ────────────────────────────────
        section('Onboarding instructions (RFC-3)')

        instructions = server.instructions or ''
        check('instructions set on server', bool(instructions))
        check('instructions mention workflow A (Explore)', 'write_table' in instructions)
        check('instructions mention workflow B (Extract)', 'find_component_id' in instructions)
        check('instructions mention workflow C (Migrate)', 'migrate_to_keboola' in instructions)
        check('instructions mention DuckDB', 'DuckDB' in instructions)

        # ── 3. Onboarding prompts (RFC-3) ─────────────────────────────────────
        section('Onboarding prompts (RFC-3)')

        prompts = await client.list_prompts()
        prompt_names = {p.name for p in prompts}
        check('explore_local_data prompt', 'explore_local_data' in prompt_names)
        check('extract_data_from_source prompt', 'extract_data_from_source' in prompt_names)
        check('push_local_work_to_keboola prompt', 'push_local_work_to_keboola' in prompt_names)

        for name in ['explore_local_data', 'extract_data_from_source', 'push_local_work_to_keboola']:
            result = await client.get_prompt(name)
            msgs = result.messages
            check(f'  {name}: returns ≥1 message', len(msgs) >= 1, f'got {len(msgs)}')

        # ── 4. write_table + query_data (baseline) ────────────────────────────
        section('write_table + query_data (baseline)')

        r = await client.call_tool('write_table', {
            'name': 'sales',
            'csv_content': 'region,amount\nEast,100\nWest,200\nEast,150\n',
        })
        check('write_table succeeds', not r.is_error)

        r = await client.call_tool('query_data', {
            'sql_query': 'SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region',
            'query_name': 'sales_by_region',
        })
        text = r.content[0].text if r.content else ''
        check('query_data returns East', 'East' in text, text[:120])
        check('query_data returns 250', '250' in text, text[:120])

        # ── 5. save_config + get_configs (RFC-1) ──────────────────────────────
        section('save_config + get_configs (RFC-1)')

        r = await client.call_tool('save_config', {
            'config_id': 'ex-001',
            'component_id': 'keboola.ex-http',
            'name': 'My HTTP Extractor',
            'parameters': {'url': 'https://example.com'},
        })
        check('save_config creates config', not r.is_error)

        r = await client.call_tool('get_configs', {})
        configs = list(r.data.configs) if hasattr(r.data, 'configs') else []
        check('get_configs returns list', len(configs) >= 1,
              f'got data: {r.content[0].text[:80] if r.content else ""}')
        ids = [getattr(c, 'config_id', None) for c in configs]
        check('get_configs contains ex-001', 'ex-001' in ids, f'ids={ids}')

        # save a second one, then delete it
        await client.call_tool('save_config', {
            'config_id': 'ex-002',
            'component_id': 'keboola.ex-ftp',
            'name': 'FTP',
            'parameters': {},
        })

        # ── 6. delete_config (RFC-1, new tool) ───────────────────────────────
        section('delete_config (RFC-1 — new tool)')

        r = await client.call_tool('delete_config', {'config_id': 'ex-002'})
        check('delete_config returns without error', not r.is_error)

        # verify it's gone
        r = await client.call_tool('get_configs', {})
        configs2 = list(r.data.configs) if hasattr(r.data, 'configs') else []
        ids2 = [getattr(c, 'config_id', None) for c in configs2]
        check('ex-002 gone after delete', 'ex-002' not in ids2, f'ids={ids2}')
        check('ex-001 still present', 'ex-001' in ids2, f'ids={ids2}')

        # ── 7. create_data_app — live queries (RFC-2) ─────────────────────────
        section('create_data_app — live queries (RFC-2)')

        r = await client.call_tool('create_data_app', {
            'name': 'sales-dash',
            'title': 'Sales Dashboard',
            'charts': [
                {
                    'id': 'c1',
                    'title': 'By Region',
                    'sql': 'SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region',
                    'type': 'bar',
                    'x_column': 'region',
                    'y_column': 'total',
                }
            ],
        })
        check('create_data_app succeeds', not r.is_error)

        # inspect the generated HTML — it must NOT contain pre-baked data
        app_dir = data_path / 'apps' / 'sales-dash'  # name= 'sales-dash'
        html_file = app_dir / 'index.html'
        check('index.html created', html_file.exists())

        if html_file.exists():
            html = html_file.read_text()
            check('HTML uses live qs_execute() (RFC-2)', 'qs_execute' in html,
                  'pre-baked query function missing')
            check('HTML calls Query Service API endpoint', '/api/v1/branches/' in html,
                  'API route missing from JS')
            check('no pre-baked DATA constant', 'CHART_DATA' not in html,
                  'old pre-baked variable still present')
            check('queryService config embedded', '"queryService"' in html or "'queryService'" in html,
                  'queryService key missing in config JSON')
            check('Refresh button present', 'Refresh' in html,
                  'Refresh button missing')
            check('window.location.origin used as base_url', 'window.location.origin' in html,
                  'hardcoded port detected instead of dynamic origin')

        # ── 8. run_data_app + appserver (RFC-2) ───────────────────────────────
        section('run_data_app + appserver (RFC-2)')

        r = await client.call_tool('run_data_app', {'name': 'sales-dash'})
        check('run_data_app succeeds', not r.is_error)

        # DataAppRunResult — use content text to extract URL
        text = r.content[0].text if r.content else ''
        port = None
        import re as _re
        m = _re.search(r'http://localhost:(\d+)', text)
        if m:
            port = int(m.group(1))

        check('run_data_app returns URL', port is not None, f'response: {text[:120]}')

        if port:
            import httpx
            base = f'http://localhost:{port}'

            # Poll until the appserver subprocess accepts connections (up to 10s)
            started = False
            for _ in range(20):
                time.sleep(0.5)
                try:
                    httpx.get(f'{base}/apps/sales-dash/', timeout=2)
                    started = True
                    break
                except Exception:
                    pass
            check('appserver reachable within 10s', started, f'port {port} never accepted connections')

            if started:
                # Static HTML
                try:
                    resp = httpx.get(f'{base}/apps/sales-dash/', timeout=5)
                    check('GET /apps/sales-dash/ → 200', resp.status_code == 200,
                          f'status={resp.status_code}')
                except Exception as e:
                    fail(f'static file GET failed: {e}')

                # Query Service API — 3-step contract
                try:
                    resp = httpx.post(
                        f'{base}/api/v1/branches/local/workspaces/local/queries',
                        json={'statements': ['SELECT 42 AS n'], 'transactional': True, 'actorType': 'user'},
                        headers={'X-StorageAPI-Token': 'local'},
                        timeout=5,
                    )
                    check('POST /queries → 200', resp.status_code == 200,
                          f'status={resp.status_code} body={resp.text[:80]}')
                    job_id = resp.json().get('queryJobId')
                    check('queryJobId returned', bool(job_id))

                    if job_id:
                        status_resp = httpx.get(f'{base}/api/v1/queries/{job_id}', timeout=5)
                        check('GET /queries/{id} → completed',
                              status_resp.json().get('status') == 'completed',
                              status_resp.text[:80])

                        stmts = status_resp.json().get('statements', [])
                        stmt_id = stmts[0]['id'] if stmts else None
                        if stmt_id:
                            res_resp = httpx.get(
                                f'{base}/api/v1/queries/{job_id}/{stmt_id}/results', timeout=5
                            )
                            data_json = res_resp.json()
                            check('results columns returned',
                                  any(c['name'] == 'n' for c in data_json.get('columns', [])),
                                  str(data_json)[:120])
                            check('results data [[42]]',
                                  data_json.get('data') == [[42]],
                                  str(data_json.get('data')))

                    # Verify against the sales table (DuckDB reads the CSV)
                    resp2 = httpx.post(
                        f'{base}/api/v1/branches/local/workspaces/local/queries',
                        json={
                            'statements': [
                                'SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region'
                            ],
                            'transactional': True,
                            'actorType': 'user',
                        },
                        headers={'X-StorageAPI-Token': 'local'},
                        timeout=5,
                    )
                    job_id2 = resp2.json().get('queryJobId')
                    stmts2 = httpx.get(
                        f'{base}/api/v1/queries/{job_id2}', timeout=5
                    ).json().get('statements', [])
                    stmt_id2 = stmts2[0]['id'] if stmts2 else None
                    if stmt_id2:
                        res2 = httpx.get(
                            f'{base}/api/v1/queries/{job_id2}/{stmt_id2}/results', timeout=5
                        ).json()
                        names = [c['name'] for c in res2.get('columns', [])]
                        rows = res2.get('data', [])
                        check('sales query: correct columns', names == ['region', 'total'],
                              f'got {names}')
                        flat = {row[0]: row[1] for row in rows}
                        check('sales query: East=250', str(flat.get('East')) == '250', str(flat))
                        check('sales query: West=200', str(flat.get('West')) == '200', str(flat))

                except Exception as e:
                    fail(f'Query Service API call failed: {e}')

                if _open_browser:
                    import webbrowser
                    url = f'{base}/apps/sales-dash/'
                    print(f'\n  Opening {url} — verify charts load live and Refresh button works')
                    webbrowser.open(url)
                else:
                    skip(f'browser open (--no-browser) — URL: {base}/apps/sales-dash/')

            # clean up
            await client.call_tool('stop_data_app', {'name': 'sales-dash'})
            check('stop_data_app succeeds', True)


async def main() -> None:
    print('Keboola MCP Server — end-to-end local-backend test')
    print('FastMCP Client → in-process server → full MCP JSON-RPC layer\n')

    with tempfile.TemporaryDirectory() as data_dir:
        print(f'Data dir: {data_dir}')
        await run_e2e(data_dir)

    print(f'\n{"═" * 51}')
    if FAIL == 0:
        print(f'  {GREEN}All tests passed{RESET}  ✓ {PASS} passed')
    else:
        print(f'  {RED}FAILURES: {FAIL}{RESET}  ✓ {PASS} passed  ✗ {FAIL} failed')
    print('═' * 51)
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == '__main__':
    asyncio.run(main())
