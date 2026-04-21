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
import re
import uuid
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


def make_handler(tables_dir: Path, data_dir: Path, jobs: dict):
    """Return a request handler class bound to the given dirs and job store."""

    class DataAppHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(data_dir), **kwargs)

        def log_message(self, fmt, *args):  # silence access log
            LOG.debug(fmt, *args)

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
    server = HTTPServer(('', port), make_handler(tables_dir, data_dir, jobs))
    LOG.info('appserver listening on port %d, serving %s', port, data_dir)
    server.serve_forever()
