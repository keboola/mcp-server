"""Value objects for the per-project search index.

The classes here are constructed only after token verification (see ``verify.py``)
and never from LLM-supplied arguments. They are validated at construction so that
any later path construction can trust their fields without re-validating.
"""

import re
from dataclasses import dataclass
from datetime import datetime

_IDENT_PATTERN = re.compile(r'^[A-Za-z0-9_-]+$')
_HASH_PATTERN = re.compile(r'^[a-f0-9]{16}$')


@dataclass(frozen=True)
class VerifiedSession:
    """A token whose ``project_id`` has been confirmed by ``tokens/verify``.

    ``project_id`` and ``token_hash`` are validated against strict allowlists so
    they are safe to use directly as filesystem path components.
    """

    project_id: str
    token_hash: str
    verified_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.project_id, str) or not _IDENT_PATTERN.match(self.project_id):
            raise ValueError(f'Invalid project_id: {self.project_id!r}')
        if not isinstance(self.token_hash, str) or not _HASH_PATTERN.match(self.token_hash):
            raise ValueError(f'Invalid token_hash: {self.token_hash!r}')
        if not isinstance(self.verified_at, datetime):
            raise TypeError(f'verified_at must be datetime, got {type(self.verified_at).__name__}')
