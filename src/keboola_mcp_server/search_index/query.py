"""Read-only FTS5 query against the per-project index.

Every query is filtered by ``project_id`` even though the file path already
segregates projects (defense in depth, per RFC).
"""

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

LOG = logging.getLogger(__name__)


@dataclass
class IndexedHit:
    kind: str
    obj_id: str
    name: str
    description: str
    metadata: dict[str, Any] = field(default_factory=dict)


def run_query(
    db_path: Path,
    project_id: str,
    patterns: Sequence[str],
    kinds: Iterable[str] | None = None,
    limit: int = 100,
) -> list[IndexedHit]:
    """Run an FTS5 MATCH against ``db_path``.

    ``patterns`` are joined with OR. Each pattern is wrapped as an FTS5 phrase,
    so spaces in a pattern are treated as a multi-token phrase (the same
    semantics ``mode='literal'`` callers expect).
    """
    match_expr = _build_match_expression(patterns)
    if not match_expr:
        return []

    sql_parts = [
        'SELECT kind, obj_id, name, description, metadata',
        'FROM search',
        'WHERE search MATCH ? AND project_id = ?',
    ]
    params: list[Any] = [match_expr, project_id]

    kind_list = [k for k in (kinds or []) if k]
    if kind_list:
        placeholders = ','.join(['?'] * len(kind_list))
        sql_parts.append(f'AND kind IN ({placeholders})')
        params.extend(kind_list)

    sql_parts.append('ORDER BY rank LIMIT ?')
    params.append(limit)
    sql = ' '.join(sql_parts)

    uri = f'file:{db_path}?mode=ro'
    conn = sqlite3.connect(uri, uri=True)
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    return [
        IndexedHit(
            kind=row[0],
            obj_id=row[1],
            name=row[2] or '',
            description=row[3] or '',
            metadata=json.loads(row[4]) if row[4] else {},
        )
        for row in rows
    ]


def _build_match_expression(patterns: Sequence[str]) -> str:
    parts = []
    for pattern in patterns:
        cleaned = (pattern or '').strip()
        if not cleaned:
            continue
        # FTS5 phrase quoting: wrap in double quotes; escape embedded quotes.
        parts.append(f'"{cleaned.replace(chr(34), chr(34) * 2)}"')
    return ' OR '.join(parts)
