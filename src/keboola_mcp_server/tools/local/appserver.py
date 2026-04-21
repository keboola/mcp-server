"""Local Query Service emulator + static file server for data apps.

Runs as a standalone process:
    python -m keboola_mcp_server.tools.local.appserver <port> <data_dir>

Single-port combined server that speaks the Query Service HTTP API (backed by
DuckDB) at /api/v1/... and serves static HTML files at all other paths.

The HTTP API contract is identical to https://query.keboola.com so that the
same JS frontend code works in both local and production environments.

API endpoints:
    POST /api/v1/branches/{branchId}/workspaces/{workspaceId}/queries
         Body: {statements:[str], transactional:bool, actorType:str}
         → {queryJobId: str}

    GET  /api/v1/queries/{queryJobId}
         → {status:"completed", statements:[{id:str, status:str}]}

    GET  /api/v1/queries/{queryJobId}/{statementId}/results?offset=0&pageSize=500
         → {status:"completed", columns:[{name,type,nullable}], data:[[val,...],...]}
"""

import json
import logging
import os
import re
import uuid
from html import escape as html_escape
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse

LOG = logging.getLogger(__name__)

# URL pattern matchers for API routes
_RE_SUBMIT = re.compile(r'^/api/v1/branches/[^/]+/workspaces/[^/]+/queries$')
_RE_STATUS = re.compile(r'^/api/v1/queries/([^/]+)$')
_RE_RESULTS = re.compile(r'^/api/v1/queries/([^/]+)/([^/]+)/results$')


def _exec_duckdb(sql: str, tables_dir: Path) -> dict:
    """Execute SQL via DuckDB against local CSV tables.

    Returns {columns:[{name,type,nullable}], data:[[val,...],...]} on success,
    or {error:str, columns:[], data:[]} on failure.
    """
    try:
        import duckdb
    except ImportError:
        return {'error': 'duckdb is not installed', 'columns': [], 'data': []}

    try:
        con = duckdb.connect()
        con.execute("SET enable_external_access = false")
        for csv_file in sorted(tables_dir.glob('*.csv')):
            tname = csv_file.stem.replace('"', '_')
            con.execute(
                f'CREATE OR REPLACE TABLE "{tname}" AS SELECT * FROM read_csv_auto(?)',
                [str(csv_file)],
            )
        cursor = con.execute(sql)
        if cursor.description is None:
            con.close()
            return {'columns': [], 'data': []}
        columns = [{'name': col[0], 'type': 'varchar', 'nullable': True} for col in cursor.description]
        raw = cursor.fetchall()
        con.close()
    except Exception as exc:
        return {'error': str(exc), 'columns': [], 'data': []}

    data = []
    for row in raw:
        data.append([v if v is None or isinstance(v, (int, float)) else str(v) for v in row])
    return {'columns': columns, 'data': data}


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def make_handler(tables_dir: Path, data_dir: Path, jobs: dict):
    """Return a request handler class bound to the given dirs and job store."""

    class DataAppHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(data_dir), **kwargs)

        def log_message(self, fmt, *args):  # silence access log
            LOG.debug(fmt, *args)

        def _send_html(self, html: str) -> None:
            payload = html.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _build_index_html(self) -> str:
            apps_dir = data_dir / 'apps'
            running: dict = {}
            running_json = apps_dir / '.running.json'
            if running_json.exists():
                try:
                    running = json.loads(running_json.read_text(encoding='utf-8'))
                except Exception:
                    pass

            cards_html = []
            for app_dir in sorted(apps_dir.iterdir()):
                if not app_dir.is_dir():
                    continue
                app_json = app_dir / 'app.json'
                if not app_json.exists():
                    continue
                try:
                    cfg = json.loads(app_json.read_text(encoding='utf-8'))
                except Exception:
                    continue
                name = html_escape(cfg.get('name', app_dir.name))
                title = html_escape(cfg.get('title', name))
                description = html_escape(cfg.get('description', ''))
                charts = cfg.get('charts', [])
                info = running.get(cfg.get('name', app_dir.name), {})
                port = info.get('port')
                pid = info.get('pid')
                is_running = bool(port and pid and _pid_alive(pid))
                if is_running:
                    status_badge = f'<span class="badge running">● running :{port}</span>'
                    card_link = f'http://localhost:{port}/apps/{name}/'
                    title_html = f'<a href="{card_link}" target="_blank">{title}</a>'
                else:
                    status_badge = '<span class="badge stopped">○ stopped</span>'
                    title_html = title
                chart_badge = f'<span class="badge charts">{len(charts)} charts</span>'
                cards_html.append(
                    f'<div class="card">'
                    f'<div class="card-header">{title_html}{status_badge}{chart_badge}</div>'
                    f'<div class="card-name">{name}</div>'
                    f'{"<p>" + description + "</p>" if description else ""}'
                    f'</div>'
                )

            cards = '\n'.join(cards_html) if cards_html else '<p>No apps found.</p>'
            return f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Keboola Local — Data Apps</title>
<style>
  body{{font-family:system-ui,sans-serif;margin:0;padding:2rem;background:#f5f5f5;color:#222}}
  h1{{margin:0 0 .25rem;font-size:1.6rem}}
  .subtitle{{color:#666;margin:0 0 2rem;font-size:.95rem}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:1.25rem}}
  .card{{background:#fff;border-radius:8px;padding:1.25rem;box-shadow:0 1px 4px rgba(0,0,0,.1)}}
  .card-header{{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap;margin-bottom:.35rem}}
  .card-header a{{font-weight:600;font-size:1.05rem;color:#1a73e8;text-decoration:none}}
  .card-header a:hover{{text-decoration:underline}}
  .card-header span:not(.badge){{font-weight:600;font-size:1.05rem}}
  .card-name{{font-size:.8rem;color:#888;margin-bottom:.5rem;font-family:monospace}}
  .card p{{margin:.25rem 0 0;font-size:.9rem;color:#555}}
  .badge{{font-size:.72rem;padding:.2rem .55rem;border-radius:99px;white-space:nowrap}}
  .badge.running{{background:#e6f4ea;color:#1e7e34;border:1px solid #a8d5b5}}
  .badge.stopped{{background:#f1f1f1;color:#888;border:1px solid #ddd}}
  .badge.charts{{background:#e8f0fe;color:#1a73e8;border:1px solid #b3c9f7}}
</style>
</head>
<body>
<h1>Keboola Local — Data Apps</h1>
<p class="subtitle">{len(cards_html)} app{"s" if len(cards_html) != 1 else ""} available</p>
<div class="grid">
{cards}
</div>
</body>
</html>'''

        def _send_json(self, status: int, body: dict) -> None:
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(payload)

        def do_OPTIONS(self):  # noqa: N802
            self.send_response(204)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-StorageAPI-Token')
            self.end_headers()

        def do_POST(self):  # noqa: N802
            path = urlparse(self.path).path
            if not _RE_SUBMIT.match(path):
                self.send_error(404)
                return
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length) or b'{}')
            statements = body.get('statements') or []
            job_id = str(uuid.uuid4())
            stmt_results = {}
            for sql in statements:
                stmt_id = str(uuid.uuid4())
                stmt_results[stmt_id] = _exec_duckdb(sql, tables_dir)
            jobs[job_id] = stmt_results
            self._send_json(200, {'queryJobId': job_id})

        def do_GET(self):  # noqa: N802
            path = urlparse(self.path).path

            if path in ('/apps', '/apps/', '/apps/index.html'):
                self._send_html(self._build_index_html())
                return

            m = _RE_STATUS.match(path)
            if m:
                job_id = m.group(1)
                job = jobs.get(job_id)
                if job is None:
                    self._send_json(404, {'error': 'job not found'})
                    return
                stmts = [{'id': sid, 'status': 'error' if 'error' in r else 'completed'} for sid, r in job.items()]
                self._send_json(200, {'status': 'completed', 'statements': stmts})
                return

            m = _RE_RESULTS.match(path)
            if m:
                job_id, stmt_id = m.group(1), m.group(2)
                job = jobs.get(job_id)
                if job is None:
                    self._send_json(404, {'error': 'job not found'})
                    return
                result = job.get(stmt_id)
                if result is None:
                    self._send_json(404, {'error': 'statement not found'})
                    return
                if 'error' in result:
                    self._send_json(400, {'error': result['error']})
                    return
                self._send_json(
                    200,
                    {
                        'status': 'completed',
                        'columns': result['columns'],
                        'data': result['data'],
                    },
                )
                return

            super().do_GET()  # fall through to static file serving

    return DataAppHandler


if __name__ == '__main__':
    import sys

    logging.basicConfig(level=logging.INFO, format='%(message)s')
    if len(sys.argv) < 3:
        print('Usage: python -m keboola_mcp_server.tools.local.appserver <port> <data_dir>')
        sys.exit(1)

    port = int(sys.argv[1])
    data_dir = Path(sys.argv[2])
    tables_dir = data_dir / 'tables'
    jobs: dict = {}
    server = HTTPServer(('127.0.0.1', port), make_handler(tables_dir, data_dir, jobs))
    LOG.info('appserver listening on port %d, serving %s', port, data_dir)
    server.serve_forever()
