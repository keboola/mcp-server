"""Audit-log helpers for row- and column-level security.

Kept out of `rls.py` on purpose: that module is the pure, secret-free rewrite engine (which is meant to be extracted
into a package the Query Service runs without any credentials), while these helpers need a secret key.
"""

import hashlib
import hmac

from keboola_mcp_server.rls import RlsAccessDenied, fold_principal


def subject_id(principal: str, key: str) -> str:
    """A keyed, truncated hash of a principal, for logs and audit records.

    It lets one user's events be correlated without writing the address down. It is pseudonymous, not
    anonymous: anyone holding `key` can recompute it from an email, so the key is a secret and the hash is still
    personal data. The principal is folded exactly as rule matching folds it, so every service that uses the same
    key derives the same value for the same user. It is for logs only, never for an access decision.
    """
    return hmac.new(key.encode(), fold_principal(principal).encode(), hashlib.sha256).hexdigest()[:16]


def refusal_code(error: Exception) -> str:
    """A fixed code for why a query was refused, safe to log: never the message, which can quote the SQL.

    The codes are a subset of the engine contract's: `ACCESS_DENIED` (no rule for the caller) or `UNSUPPORTED`
    (anything else the rewrite refuses, including a query that is too long or too deeply nested).
    """
    return 'ACCESS_DENIED' if isinstance(error, RlsAccessDenied) else 'UNSUPPORTED'
