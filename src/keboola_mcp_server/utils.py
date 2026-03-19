"""Shared utility helpers for the Keboola MCP server."""

import re
from datetime import datetime


def parse_iso_timestamp(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp string into a datetime object.

    Handles both ``Z`` and numeric timezone offsets in ``+HHMM`` form
    (as returned by the Keboola Storage API), which Python ≤ 3.10's
    ``datetime.fromisoformat`` does not accept without normalization.
    """
    normalized = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', ts.replace('Z', '+00:00'))
    return datetime.fromisoformat(normalized)
