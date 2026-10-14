"""Loading the row-/column-level security policies that apply to the current project.

Shared by every tool that has to honour them -- `query_data` (rewrites the SQL) and `get_tables`
(hides the columns a column-level policy withholds) -- so they cannot disagree about which policies
exist.
"""

import asyncio

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.metastore import MetastoreClient, MetastoreObject
from keboola_mcp_server.config import deployed_sa_token_path
from keboola_mcp_server.rls import ClsRules, RlsRules

# Policies are listed a page at a time: large enough that a project with many policies is not dozens of
# sequential round trips per query. Every object is needed: a policy that is never read leaves its table
# ungoverned. So the next offset is the number of objects actually received and only an EMPTY page ends the
# listing -- a server or proxy that returns fewer than `limit` (a cap) must not read as "no more policies".
_PAGE_SIZE = 100
_MAX_PAGES = 1000
# The metastore pages by offset and returns no total or cursor, so a listing that changes between two page
# requests (a policy deleted or re-scoped on an earlier page) shifts the later objects across the page boundary
# and one is never fetched -- its table would look ungoverned. A listing that took more than one request is
# therefore read twice and only accepted when both reads agree; otherwise it is read again, and refused after
# `_MAX_LISTING_ATTEMPTS` unstable reads. A listing of one page is a single atomic response and needs no check.
_MAX_LISTING_ATTEMPTS = 3


async def _read_pages(metastore: MetastoreClient, object_type: str) -> tuple[list[MetastoreObject], int]:
    """One pass over the listing: `(objects, number of non-empty pages)`."""
    objects: list[MetastoreObject] = []
    for pages in range(_MAX_PAGES):
        page = await metastore.list_objects(object_type, limit=_PAGE_SIZE, offset=len(objects))
        if not page:
            return objects, pages
        objects.extend(page)
    raise ValueError(f'RLS: more than {_MAX_PAGES} pages of {object_type} objects, refusing to guess')


async def _list_all(metastore: MetastoreClient, object_type: str) -> list[MetastoreObject]:
    for _ in range(_MAX_LISTING_ATTEMPTS):
        objects, pages = await _read_pages(metastore, object_type)
        if pages <= 1:
            return objects
        verification, _ = await _read_pages(metastore, object_type)
        if verification == objects:
            return objects
    raise ValueError(f'RLS: the {object_type} listing kept changing while it was read, so the query is refused')


# Project feature that switches the RLS/CLS mechanism on, one gate for both (see the RFC).
RLS_FEATURE = 'row-level-security'


async def load_policy_rules(client: KeboolaClient, *, dialect: str) -> tuple[RlsRules, ClsRules]:
    """Reads the project's `rls-policy` and `cls-policy` objects from the metastore.

    Callers check `RLS_FEATURE` first. Policies are served to regular members only on the
    Kubernetes ServiceAccount step-up (see `KeboolaClient.step_up_metastore_client`); without it they
    would see none and a governed table would look ungoverned. Not narrowed by `?principal=`: a policy
    that names only other users must still mark its table as governed.

    :param dialect: lower-cased workspace SQL dialect the rules must be pinned to.
    :raises ValueError: when the step-up cannot be made (not a deployed server, or not the server's own
        stack), or a policy is malformed.
    """
    project_id = int(await client.storage_client.project_id())
    kubernetes_token_path = deployed_sa_token_path()
    if not kubernetes_token_path:
        # Without the step-up the metastore shows a regular member no policies at all, which would read
        # as "nothing is governed" and run the query unfiltered. Refuse instead.
        raise ValueError(
            'RLS: the security policies can only be evaluated by a deployed MCP server, so the query is refused.'
        )
    metastore = client.step_up_metastore_client(kubernetes_token_path)
    rls_objects, cls_objects = await asyncio.gather(
        _list_all(metastore, 'rls-policy'), _list_all(metastore, 'cls-policy')
    )
    return (
        RlsRules.from_metastore(rls_objects, dialect=dialect, project_id=project_id),
        ClsRules.from_metastore(cls_objects, dialect=dialect, project_id=project_id),
    )
