from collections import defaultdict
from typing import Mapping, Optional, Sequence

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonStruct
from keboola_mcp_server.clients.client import (
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.search.tools import SearchItemType, SearchSpec, _fetch_configurations


class ComponentUsageReference(BaseModel):
    component_id: str = Field(description='The ID of the component.')
    configuration_id: str = Field(description='The ID of the configuration.')
    configuration_row_id: str | None = Field(default=None, description='The ID of the configuration row.')
    configuration_name: str | None = Field(default=None, description='The name of the configuration.')
    used_in: str | None = Field(default=None, description='The scope in which the target item was used.')
    timestamp: str | None = Field(default=None, description='The timestamp of the usage.')


class UsageById(BaseModel):
    target_id: str
    usage_references: list[ComponentUsageReference]


async def find_id_usage(
    client: KeboolaClient,
    target_ids: Sequence[str],
    item_types: Optional[Sequence[SearchItemType]] = None,
    scopes: list[str] = list,
) -> list[UsageById]:
    """
    Finds component configurations (including rows) that reference any of the target IDs in the specified configuration
    scopes.

    :param client: The Keboola client to use.
    :param target_ids: The IDs to search for.
    :param item_types: The item types to search for, if not provided, all item types are searched.
    :param scopes: Dot-separated keys to search in the configuration.
    :return: A list of UsageById objects.
    """

    spec = SearchSpec(
        patterns=target_ids,
        item_types=item_types or tuple(),
        search_scopes=scopes,
        pattern_mode='literal',
        search_type='config-based',
        return_all_matched_patterns=True,
        stop_searching_after_first_value_match=False,
    )

    search_hits = await _fetch_configurations(client, spec)

    # group usage references by pattern = target_id
    output: dict[str, list[ComponentUsageReference]] = defaultdict(list)
    for search_hit in search_hits:
        for match in search_hit._matches:
            for pattern in match.patterns:
                output[pattern].append(
                    # TODO: Consider whether adding configuration description is useful, it could overload context.
                    ComponentUsageReference(
                        component_id=search_hit.component_id,
                        configuration_id=search_hit.configuration_id,
                        configuration_row_id=search_hit.configuration_row_id,
                        configuration_name=search_hit.name,
                        used_in=match.scope,
                        timestamp=search_hit.updated,
                    )
                )
    return [
        UsageById(target_id=target_id, usage_references=usage_references)
        for target_id, usage_references in output.items()
    ]


def get_created_by(
    metadata: Sequence[Mapping[str, JsonStruct]] | Mapping[str, JsonStruct],
) -> ComponentUsageReference | None:
    """
    Gets the created by reference from the metadata.
    :param metadata: The metadata to search in.
    :return: The created by reference.
    """
    metadata_items = _coerce_metadata_list(metadata)
    component_id = get_metadata_property(metadata_items, MetadataField.CREATED_BY_COMPONENT_ID)
    configuration_id = get_metadata_property(metadata_items, MetadataField.CREATED_BY_CONFIGURATION_ID)
    configuration_row_id = get_metadata_property(metadata_items, MetadataField.CREATED_BY_CONFIGURATION_ROW_ID)
    timestamp = _get_latest_metadata_timestamp(
        metadata_items,
        [
            MetadataField.CREATED_BY_COMPONENT_ID,
            MetadataField.CREATED_BY_CONFIGURATION_ID,
            MetadataField.CREATED_BY_CONFIGURATION_ROW_ID,
        ],
    )
    if component_id is None or configuration_id is None or timestamp is None:
        return None
    return ComponentUsageReference(
        component_id=str(component_id),
        configuration_id=str(configuration_id),
        configuration_row_id=str(configuration_row_id) if configuration_row_id else None,
        configuration_scope=None,
        timestamp=timestamp,
    )


def get_last_updated_by(
    metadata: Sequence[Mapping[str, JsonStruct]] | Mapping[str, JsonStruct],
) -> ComponentUsageReference | None:
    """
    Gets the last updated by reference from the metadata.
    :param metadata: The metadata to search in.
    :return: The last updated by reference.
    """
    metadata_items = _coerce_metadata_list(metadata)
    component_id = get_metadata_property(metadata_items, MetadataField.UPDATED_BY_COMPONENT_ID)
    configuration_id = get_metadata_property(metadata_items, MetadataField.UPDATED_BY_CONFIGURATION_ID)
    configuration_row_id = get_metadata_property(metadata_items, MetadataField.UPDATED_BY_CONFIGURATION_ROW_ID)
    timestamp = _get_latest_metadata_timestamp(
        metadata_items, [MetadataField.UPDATED_BY_COMPONENT_ID, MetadataField.UPDATED_BY_CONFIGURATION_ID]
    )
    if component_id is None or configuration_id is None or timestamp is None:
        return None
    return ComponentUsageReference(
        component_id=str(component_id),
        configuration_id=str(configuration_id),
        configuration_row_id=str(configuration_row_id) if configuration_row_id else None,
        configuration_scope=None,
        timestamp=timestamp,
    )


def _coerce_metadata_list(
    metadata: Sequence[Mapping[str, JsonStruct]] | Mapping[str, JsonStruct],
) -> list[Mapping[str, JsonStruct]]:
    if isinstance(metadata, Mapping):
        metadata_value = metadata.get('metadata')
        if isinstance(metadata_value, list):
            return [item for item in metadata_value if isinstance(item, Mapping)]
        if 'key' in metadata and 'value' in metadata:
            return [metadata]
        return []
    return [item for item in metadata if isinstance(item, Mapping)]


def _get_latest_metadata_timestamp(metadata: list[Mapping[str, JsonStruct]], keys: Sequence[str]) -> str | None:
    latest = None
    for key in keys:
        for item in metadata:
            if item.get('key') != key:
                continue
            timestamp = item.get('timestamp')
            if timestamp is None:
                continue
            if latest is None or str(timestamp) > str(latest):
                latest = str(timestamp)
    return latest
