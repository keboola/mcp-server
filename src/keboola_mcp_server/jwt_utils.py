"""
Shared helpers for signing small JSON payloads into opaque, self-contained JWTs.

Used wherever this server hands a client something to carry and resend later instead of keeping
it in server-side memory (OAuth state/access/refresh tokens in ``oauth.py``; the multi-project
``scope_token`` in ``mcp.py``) -- gzip-compressed JSON, HMAC-signed so any process holding the same
secret can verify it without a shared store.
"""

import gzip
import json
from collections.abc import Mapping
from typing import Any

import jwt.api_jws


def encode_jwt(data: Mapping[str, Any], secret: str) -> str:
    json_gzip = gzip.compress(json.dumps(data).encode('utf-8'))
    return jwt.api_jws.encode(json_gzip, secret)


def decode_jwt(token: str, secret: str) -> dict[str, Any]:
    json_gzip = jwt.api_jws.decode(token, secret, algorithms=['HS256'])
    return json.loads(gzip.decompress(json_gzip).decode('utf-8'))
