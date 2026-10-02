"""Loading the row-/column-level security policies that apply to the current project.

Shared by every tool that has to honour them -- `query_data` (rewrites the SQL) and `get_tables`
(hides the columns a column-level policy withholds) -- so they cannot disagree about which policies
exist.
"""

import asyncio

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import deployed_sa_token_path
from keboola_mcp_server.rls import ClsRules, RlsRules

# Project feature that switches the RLS/CLS mechanism on, one gate for both (see the RFC).
RLS_FEATURE = 'row-level-security'


async def load_policy_rules(client: KeboolaClient, *, dialect: str) -> tuple[RlsRules, ClsRules]:
    """Reads the project's `rls-policy` and `cls-policy` objects from the metastore.

    Callers check `RLS_FEATURE` first. Policies are served to regular members only on the
    Kubernetes ServiceAccount step-up (see `KeboolaClient.step_up_metastore_client`); without it they
    would see none and a governed table would look ungoverned. Not narrowed by `?principal=`: a policy
    that names only other users must still mark its table as governed.

    :param dialect: lower-cased workspace SQL dialect the rules must be pinned to.
    :raises ValueError: when the step-up is required but cannot be made, or a policy is malformed.
    """
    project_id = int(await client.storage_client.project_id())
    metastore = client.metastore_client
    if kubernetes_token_path := deployed_sa_token_path():
        metastore = client.step_up_metastore_client(kubernetes_token_path)
    rls_objects, cls_objects = await asyncio.gather(
        metastore.list_objects('rls-policy'), metastore.list_objects('cls-policy')
    )
    return (
        RlsRules.from_metastore(rls_objects, dialect=dialect, project_id=project_id),
        ClsRules.from_metastore(cls_objects, dialect=dialect, project_id=project_id),
    )
