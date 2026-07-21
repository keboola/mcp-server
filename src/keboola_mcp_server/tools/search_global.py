"""Server-side global-search backed textual search for the `search` tool.

Extracted from `search.py` to keep that module focused on the tool entry point and the
legacy client-side enumeration path. Depends only on `search_models` (shared models and
constants), so there is no import cycle with `search.py`.
"""

import asyncio
import logging
from collections import defaultdict
from typing import Any, Literal, Sequence, cast

from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    DATA_APP_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    KeboolaClient,
)
from keboola_mcp_server.clients.storage import GlobalSearchResponse
from keboola_mcp_server.clients.storage import ItemType as ApiItemType
from keboola_mcp_server.tools.search_models import (
    MAX_GLOBAL_SEARCH_LIMIT,
    SEARCH_ITEM_TYPE_TO_API_TYPES,
    WORKSPACE_COMPONENT_ID,
    SearchHit,
    SearchItemType,
    SearchOutput,
    SearchSpec,
)

LOG = logging.getLogger(__name__)


def _api_types_for(item_types: Sequence[SearchItemType]) -> list[ApiItemType]:
    """Maps the tool's item types to a deduplicated list of API types for the global-search endpoint."""
    api_types: list[ApiItemType] = []
    for item_type in item_types:
        for api_type in SEARCH_ITEM_TYPE_TO_API_TYPES.get(item_type, ()):
            if api_type not in api_types:
                api_types.append(api_type)
    return api_types


def _retype_configuration(component_id: str | None) -> SearchItemType:
    """Maps a 'configuration' global-search item to the tool's more specific item type by its component."""
    if component_id in (ORCHESTRATOR_COMPONENT_ID, CONDITIONAL_FLOW_COMPONENT_ID):
        return 'flow'
    if component_id == DATA_APP_COMPONENT_ID:
        return 'data-app'
    if component_id == WORKSPACE_COMPONENT_ID:
        return 'workspace'
    return 'configuration'


def _global_search_hit(item: GlobalSearchResponse.Item) -> SearchHit | None:
    """Maps a global-search item to a SearchHit; returns None for items that cannot be mapped."""
    common: dict[str, Any] = {
        'updated': item.created.isoformat(),
        'name': item.name,
        'branch_id': item.branch_id,
        'branch_name': item.branch_name,
    }

    if item.type == 'bucket':
        return SearchHit(bucket_id=item.id, item_type='bucket', **common)

    if item.type == 'table':
        bucket = item.full_path.get('bucket')
        bucket_id = str(bucket['id']) if isinstance(bucket, dict) and bucket.get('id') else None
        return SearchHit(table_id=item.id, bucket_id=bucket_id, item_type='table', **common)

    if item.type in ('configuration-row', 'rows'):
        configuration = item.full_path.get('configuration')
        configuration_id = (
            str(configuration['id']) if isinstance(configuration, dict) and configuration.get('id') else None
        )
        if not (item.component_id and configuration_id):
            LOG.warning(f'Skipping global-search row hit with no parent configuration in fullPath: {item.id}')
            return None
        return SearchHit(
            component_id=item.component_id,
            configuration_id=configuration_id,
            configuration_row_id=item.id,
            item_type='configuration-row',
            **common,
        )

    # The remaining types (configuration, transformation, flow, workspace, shared-code, state) are all
    # configuration-like items whose id is the configuration ID.
    component_id = item.component_id or (WORKSPACE_COMPONENT_ID if item.type == 'workspace' else None)
    if not component_id:
        LOG.warning(f'Skipping global-search hit with no component id: {item.type} {item.id}')
        return None
    item_type = _retype_configuration(component_id) if item.type == 'configuration' else cast(SearchItemType, item.type)
    return SearchHit(component_id=component_id, configuration_id=item.id, item_type=item_type, **common)


async def _global_textual_search(
    client: KeboolaClient,
    spec: SearchSpec,
    limit: int,
    offset: int,
) -> SearchOutput:
    """
    Searches item names server-side via the SAPI global-search endpoint, scoped to the current project.

    Runs one request per pattern (patterns are OR-ed, mirroring the legacy behavior) against the current
    branch context first; when nothing is found, widens the search to the whole project (all branches).
    """
    api_types = _api_types_for(spec.item_types)
    # 'rows' hits are reported as 'configuration-row' and 'component' expands to configuration (rows);
    # normalize the requested types accordingly for the client-side narrowing.
    requested_types = {'configuration-row' if t == 'rows' else t for t in spec.item_types if t != 'component'}

    # The 'configuration' API type is lossy: a single server-side 'configuration' item may re-type to
    # configuration/data-app/flow/workspace, so when the caller filters by type the server can fill a page
    # with items that are dropped during client-side narrowing, under-filling the page. Over-fetch up to the
    # server max in that case so the narrowed page is more likely to reach `limit`. Deep pagination (large
    # offset) remains approximate because the server paginates in un-narrowed space.
    needs_overfetch = bool(requested_types) and 'configuration' in api_types
    fetch_limit = MAX_GLOBAL_SEARCH_LIMIT if needs_overfetch else limit

    async def query(branch_scope: Literal['current', 'all']) -> list[GlobalSearchResponse]:
        return list(
            await asyncio.gather(
                *(
                    client.storage_client.global_search(
                        query=pattern, types=api_types, limit=fetch_limit, offset=offset, branch_scope=branch_scope
                    )
                    for pattern in spec.patterns
                )
            )
        )

    def collect(responses: list[GlobalSearchResponse]) -> list[SearchHit]:
        hits_by_key: dict[tuple[str, str], SearchHit] = {}
        for response in responses:
            for item in response.items:
                if (hit := _global_search_hit(item)) is None:
                    continue
                if requested_types and hit.item_type not in requested_types:
                    continue
                hits_by_key.setdefault((item.type, item.id), hit)
        return list(hits_by_key.values())

    branch_scope: Literal['current', 'all'] = 'current'
    responses = await query(branch_scope)
    hits = collect(responses)
    if not hits and offset == 0:
        # Nothing in the current branch context — widen to the whole project so that items living
        # in other branches can be discovered. Hits carry branch_id/branch_name for attribution.
        branch_scope = 'all'
        responses = await query(branch_scope)
        hits = collect(responses)

    hits.sort(
        key=lambda x: (
            x.updated,
            x.bucket_id or x.table_id or x.component_id or x.configuration_id or x.configuration_row_id,
        ),
        reverse=True,
    )

    by_type: dict[str, int] = defaultdict(int)
    for response in responses:
        for type_name, count in response.by_type.items():
            by_type[type_name] += count

    return SearchOutput(
        hits=hits[:limit],
        total=sum(response.all for response in responses),
        by_type=dict(by_type),
        branch_scope='current-branch' if branch_scope == 'current' else 'all-branches',
    )
