"""Partial results for batch ID lookups: return what was found and report what was not."""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Generic, TypeVar

import httpx
from pydantic import BaseModel, Field

from keboola_mcp_server.errors import _is_caller_error
from keboola_mcp_server.mcp import AggregateError

R = TypeVar('R')

_MAX_REASON_LEN = 200


class LookupFailure(BaseModel, frozen=True):
    """A requested ID that could not be retrieved."""

    id: str = Field(description='The requested ID.')
    reason: str = Field(description='Short reason why the item could not be retrieved.')


def _short_reason(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        try:
            reason = f'HTTP {status} {HTTPStatus(status).phrase}'
        except ValueError:
            reason = f'HTTP {status}'
        api_error = next((line for line in str(exc).splitlines() if line.startswith('API error: ')), None)
        if api_error:
            reason = f'{reason}: {api_error.removeprefix("API error: ")}'
    else:
        reason = next(iter(str(exc).splitlines()), '') or type(exc).__name__
    return reason if len(reason) <= _MAX_REASON_LEN else f'{reason[: _MAX_REASON_LEN - 3]}...'


@dataclass(frozen=True)
class LookupResults(Generic[R]):
    found: list[R] = field(default_factory=list)
    not_found: list[LookupFailure] = field(default_factory=list)
    errors: list[LookupFailure] = field(default_factory=list)

    def failure_fields(self) -> dict[str, list[LookupFailure] | None]:
        """The ``not_found`` and ``errors`` output fields, ``None`` when empty."""
        return {'not_found': self.not_found or None, 'errors': self.errors or None}


def unwrap_lookup_results(
    ids: Sequence[str], results: Iterable[R | BaseException], message: str = 'Failed to fetch all requested items'
) -> LookupResults[R]:
    """
    Split the results of a batch ID lookup into found items and failures.

    Caller errors (e.g. 404) go to ``not_found``, anything else (e.g. 5xx) goes to ``errors``.

    :param ids: The requested IDs, in the same order as ``results``.
    :param results: Results or exceptions from ``process_concurrently``.
    :param message: Message for the ``AggregateError`` raised when every lookup failed.
    :return: The found items and the IDs that could not be retrieved, with reasons.
    :raises AggregateError: If every lookup failed.
    """
    found: list[R] = []
    not_found: list[LookupFailure] = []
    errors: list[LookupFailure] = []
    exceptions: list[BaseException] = []

    for item_id, result in zip(ids, results, strict=True):
        if isinstance(result, BaseException):
            exceptions.append(result)
            failure = LookupFailure(id=item_id, reason=_short_reason(result))
            (not_found if _is_caller_error(result) else errors).append(failure)
        else:
            found.append(result)

    if exceptions and not found:
        raise AggregateError(message, exceptions)

    return LookupResults(found=found, not_found=not_found, errors=errors)
