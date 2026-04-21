"""Branch-aware fetch helpers for storage objects.

These helpers handle the dual-fetch + merge pattern needed when working on dev branches:
- On the default/production branch: single fetch from the default endpoint
- On a dev branch with storage-branches feature: parallel fetch from both default and branch endpoints,
  merged so that branch data wins on ID collision
- On a dev branch without storage-branches (legacy): single fetch from the default endpoint
  (legacy branches embed branch data in the default endpoint)

All callers (storage tools, search, etc.) should use these helpers instead of calling
the storage client bucket/table methods directly.
"""

import asyncio
import logging
from typing import Any

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.config import MetadataField

LOG = logging.getLogger(__name__)

STORAGE_BRANCHES_FEATURE = 'storage-branches'


async def has_storage_branches(client: KeboolaClient) -> bool:
    """Checks if the project has the storage-branches feature enabled and client is on a dev branch."""
    return client.branch_id is not None and await client.has_feature(STORAGE_BRANCHES_FEATURE)


async def merged_bucket_list(client: KeboolaClient, **kwargs: Any) -> list[JsonDict]:
    """
    List all buckets visible from the current branch context.

    Returns production buckets merged with the current branch's buckets (branch wins on ID collision).
    On the default branch or legacy branches, returns production data from the default endpoint.
    """
    if await has_storage_branches(client):
        prod_data, branch_data = await asyncio.gather(
            client.storage_client.bucket_list(branch_id='default', **kwargs),
            client.storage_client.bucket_list(branch_id=client.branch_id, **kwargs),
        )
        return _merge_by_id(prod_data, branch_data)
    else:
        raw = await client.storage_client.bucket_list(branch_id='default', **kwargs)
        return _filter_current_branch(raw, client.branch_id)


async def merged_bucket_table_list(client: KeboolaClient, bucket_id: str, **kwargs: Any) -> list[JsonDict]:
    """
    List all tables in a bucket, merging production and branch data.

    For storage-branches: fetches from both default and branch endpoints for the given bucket_id.
    For legacy branches: fetches from the default endpoint (which includes branched data).
    """
    if await has_storage_branches(client):
        prod_data, branch_data = await asyncio.gather(
            client.storage_client.bucket_table_list(bucket_id, branch_id='default', **kwargs),
            client.storage_client.bucket_table_list(bucket_id, branch_id=client.branch_id, **kwargs),
        )
        return _merge_by_id(prod_data, branch_data)
    else:
        raw = await client.storage_client.bucket_table_list(bucket_id, branch_id='default', **kwargs)
        return _filter_current_branch(raw, client.branch_id)


async def merged_bucket_detail(client: KeboolaClient, bucket_id: str) -> tuple[JsonDict | None, JsonDict | None]:
    """
    Fetch production and branch versions of a bucket.

    Returns (prod_raw, dev_raw) tuple. Either may be None if not found.
    """
    if await has_storage_branches(client):
        prod_raw, dev_raw = await asyncio.gather(
            _safe_bucket_detail(client, bucket_id, branch_id='default'),
            _safe_bucket_detail(client, bucket_id, branch_id=client.branch_id),
        )
        return prod_raw, dev_raw
    else:
        # Legacy: both prod and dev are accessible from the default endpoint
        prod_raw = await _safe_bucket_detail(client, bucket_id, branch_id='default')
        dev_raw = None
        if client.branch_id:
            if f'c-{client.branch_id}-' in bucket_id:
                dev_id = bucket_id
            else:
                dev_id = bucket_id.replace('c-', f'c-{client.branch_id}-')
            dev_raw = await _safe_bucket_detail(client, dev_id, branch_id='default')
        return prod_raw, dev_raw


async def merged_table_detail(client: KeboolaClient, table_id: str) -> tuple[JsonDict | None, JsonDict | None]:
    """
    Fetch production and branch versions of a table.

    Returns (prod_raw, dev_raw) tuple. Either may be None if not found.
    """
    if await has_storage_branches(client):
        prod_raw, dev_raw = await asyncio.gather(
            _safe_table_detail(client, table_id, branch_id='default'),
            _safe_table_detail(client, table_id, branch_id=client.branch_id),
        )
        return prod_raw, dev_raw
    else:
        prod_raw = await _safe_table_detail(client, table_id, branch_id='default')
        dev_raw = None
        if client.branch_id:
            if f'c-{client.branch_id}-' in table_id:
                dev_id = table_id
            else:
                dev_id = table_id.replace('c-', f'c-{client.branch_id}-')
            dev_raw = await _safe_table_detail(client, dev_id, branch_id='default')
        return prod_raw, dev_raw


def _filter_current_branch(items: list[JsonDict], branch_id: str | None) -> list[JsonDict]:
    """Filter out items belonging to other dev branches (legacy mode).

    Keeps items that are either production (no branch metadata) or belong to the current branch.
    When branch_id is None (production), returns all items without branch metadata.
    """
    result = []
    for item in items:
        item_branch = get_metadata_property(item.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
        if not item_branch or item_branch == branch_id:
            result.append(item)
    return result


def _merge_by_id(prod_data: list[JsonDict], branch_data: list[JsonDict]) -> list[JsonDict]:
    """Merge production and branch data lists. Branch wins on ID collision."""
    branch_ids = {item.get('id') for item in branch_data}
    merged = list(branch_data)
    for item in prod_data:
        if item.get('id') not in branch_ids:
            merged.append(item)
    return merged


async def _safe_bucket_detail(client: KeboolaClient, bucket_id: str, **kwargs: Any) -> JsonDict | None:
    """Fetch bucket detail, returning None on 404."""
    import httpx

    try:
        return await client.storage_client.bucket_detail(bucket_id, **kwargs)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        raise


async def _safe_table_detail(client: KeboolaClient, table_id: str, **kwargs: Any) -> JsonDict | None:
    """Fetch table detail, returning None on 404."""
    import httpx

    try:
        return await client.storage_client.table_detail(table_id, **kwargs)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        raise
