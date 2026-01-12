import json
from typing import Literal, Mapping, Optional, Sequence

from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonStruct
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    DATA_APP_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.config import MetadataField

SearchSection = Literal['parameters', 'storage', 'processors', 'tasks']
UsageSection = Literal['parameters', 'processors', 'storage.input', 'storage.output', 'tasks']
SearchType = Literal['data-apps', 'flow', 'transformation', 'component', 'any']
SearchDataType = Literal['bucket', 'table', 'component', 'flow', 'data-app', 'transformation', 'any']
COMPONENT_TYPES = ['application', 'extractor', 'writer']
ALL_SEARCH_TYPES = ['data-apps', 'flow', 'transformation', 'application', 'extractor', 'writer']
ALL_DATA_SEARCH_TYPES = ['bucket', 'table', 'component', 'flow', 'data-app', 'transformation']
USAGE_TOOLS_TAG = 'usage'


class ComponentUsage(BaseModel):
    section: UsageSection
    component_id: str = Field(description='The ID of the component.')
    configuration_id: str = Field(description='The ID of the configuration.')
    configuration_row_id: str | None = Field(default=None, description='The ID of the configuration row.')
    configuration_name: str | None = Field(default=None, description='The name of the configuration.')
    configuration_description: str | None = Field(default=None, description='The description of the configuration.')


class UsageMatch(ComponentUsage):
    matched_ids: list[str] | None = None

    def to_component_usage(self) -> ComponentUsage:
        return ComponentUsage(
            section=self.section,
            component_id=self.component_id,
            configuration_id=self.configuration_id,
            configuration_row_id=self.configuration_row_id,
            configuration_name=self.configuration_name,
            configuration_description=self.configuration_description,
        )


class ComponentUsageRef(BaseModel):
    component_id: str
    configuration_id: str
    timestamp: str


def _stringify_for_search(value: JsonStruct) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return str(value)


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


def _normalize_ids(target_ids: Sequence[str]) -> list[str]:
    return [str(item) for item in target_ids if item is not None and str(item)]


def _normalize_search_types(search_types: Sequence[SearchType]) -> list[SearchType]:
    if 'any' in search_types or not search_types:
        return list(set(ALL_SEARCH_TYPES))
    if 'component' in search_types:
        tmp = [item for item in search_types if item != 'component']
        return list(set(list(tmp) + COMPONENT_TYPES))
    return list(set(search_types))


def _normalize_data_search_types(search_types: Sequence[SearchDataType]) -> list[SearchDataType]:
    if 'any' in search_types or not search_types:
        return list(set(ALL_DATA_SEARCH_TYPES))
    return list(set(search_types))


def _find_ids_in_value(value: JsonStruct, target_ids: Sequence[str]) -> list[str]:
    haystack = _stringify_for_search(value)
    return [target_id for target_id in target_ids if target_id in haystack]


def _get_configuration_id(configuration: Mapping[str, JsonStruct]) -> str | None:
    return (
        configuration.get('id') or configuration.get('configurationId') or configuration.get('configuration_id') or None
    )


def _search_configuration_sections(
    *,
    component_id: str,
    configuration_id: str,
    configuration_row_id: str | None,
    configuration_name: str | None,
    configuration_description: str | None,
    configuration: Mapping[str, JsonStruct],
    search_sections: Sequence[SearchSection],
    target_ids: Sequence[str],
    matches: list[UsageMatch],
) -> None:
    if 'parameters' in search_sections:
        parameters = configuration.get('parameters')
        if parameters is not None:
            matched_ids = _find_ids_in_value(parameters, target_ids)
            if matched_ids:
                matches.append(
                    UsageMatch(
                        component_id=component_id,
                        configuration_id=configuration_id,
                        configuration_row_id=configuration_row_id,
                        configuration_name=configuration_name,
                        configuration_description=configuration_description,
                        section='parameters',
                        matched_ids=matched_ids,
                    )
                )

    if 'processors' in search_sections:
        processors = configuration.get('processors')
        if processors is not None:
            matched_ids = _find_ids_in_value(processors, target_ids)
            if matched_ids:
                matches.append(
                    UsageMatch(
                        component_id=component_id,
                        configuration_id=configuration_id,
                        configuration_row_id=configuration_row_id,
                        configuration_name=configuration_name,
                        configuration_description=configuration_description,
                        section='processors',
                        matched_ids=matched_ids,
                    )
                )

    if 'storage' in search_sections:
        storage = configuration.get('storage') or {}
        for storage_key, section in (('input', 'storage.input'), ('output', 'storage.output')):
            storage_mapping = storage.get(storage_key)
            if storage_mapping is None:
                continue
            matched_ids = _find_ids_in_value(storage_mapping, target_ids)
            if matched_ids:
                matches.append(
                    UsageMatch(
                        component_id=component_id,
                        configuration_id=configuration_id,
                        configuration_row_id=configuration_row_id,
                        configuration_name=configuration_name,
                        configuration_description=configuration_description,
                        section=section,
                        matched_ids=matched_ids,
                    )
                )

    if 'tasks' in search_sections:
        tasks = configuration.get('tasks')
        if tasks is not None:
            matched_ids = _find_ids_in_value(tasks, target_ids)
            if matched_ids:
                matches.append(
                    UsageMatch(
                        component_id=component_id,
                        configuration_id=configuration_id,
                        configuration_row_id=configuration_row_id,
                        configuration_name=configuration_name,
                        configuration_description=configuration_description,
                        section='tasks',
                        matched_ids=matched_ids,
                    )
                )
    if 'configuration' in search_sections:
        matched_ids = _find_ids_in_value(configuration, target_ids)
        if matched_ids:
            matches.append(
                UsageMatch(
                    component_id=component_id,
                    configuration_id=configuration_id,
                    configuration_row_id=configuration_row_id,
                    configuration_name=configuration_name,
                    configuration_description=configuration_description,
                    section='configuration',
                    matched_ids=matched_ids,
                )
            )


async def find_id_usage(
    client: KeboolaClient,
    target_ids: Sequence[str],
    search_sections: Sequence[SearchSection],
    search_types: Optional[Sequence[SearchType]] = None,
) -> list[UsageMatch]:
    """
    Finds component configurations (including rows) that reference any of the target IDs in the specified configuration
    sections.

    :param client: The Keboola client to use.
    :param target_ids: The IDs to search for.
    :param search_sections: The sections to search in.
    :param search_types: The component types to search for, if not provided, all component types are searched.
    :return: A list of UsageMatch objects.

    """
    normalized_ids = _normalize_ids(target_ids)
    if not normalized_ids or not search_sections:
        return {}

    normalized_search_types = _normalize_search_types(search_types or [])
    allowed_component_types = [item for item in normalized_search_types if item in COMPONENT_TYPES]
    allowed_transformation_type = 'transformation' in normalized_search_types
    allowed_component_ids_wrt_type = list(
        filter(
            lambda x: x is not None,
            [
                DATA_APP_COMPONENT_ID if 'data-apps' in normalized_search_types else None,
                CONDITIONAL_FLOW_COMPONENT_ID if 'flow' in normalized_search_types else None,
                ORCHESTRATOR_COMPONENT_ID if 'flow' in normalized_search_types else None,
            ],
        )
    )

    matches: list[UsageMatch] = []
    components = await client.storage_client.component_list(include=['configuration', 'rows'])
    for component in components:
        component_id = component.get('id')
        if not component_id:
            continue

        component_type = component.get('type')
        if not component_type:
            continue
        elif component_type == 'transformation' and not allowed_transformation_type:
            continue
        # writers, extractors, applications
        elif (
            component_type != 'other'
            and component_type != 'transformation'
            and component_type not in allowed_component_types
        ):
            continue
        # other - data-apps, flow, orchestrator, scheduler...
        elif component_type == 'other' and component_id not in allowed_component_ids_wrt_type:
            continue

        configurations = component.get('configurations', []) or []
        for configuration in configurations:
            configuration_name = configuration.get('name')
            configuration_description = configuration.get('description')
            configuration_id = _get_configuration_id(configuration)
            if not configuration_id:
                continue

            config_definition = configuration.get('configuration') or {}
            if isinstance(config_definition, Mapping):
                _search_configuration_sections(
                    component_id=component_id,
                    configuration_id=configuration_id,
                    configuration_row_id=None,
                    configuration_name=configuration_name,
                    configuration_description=configuration_description,
                    configuration=config_definition,
                    search_sections=search_sections,
                    target_ids=normalized_ids,
                    matches=matches,
                )

            rows = configuration.get('rows', []) or []
            for row in rows:
                row_id = row.get('id')
                row_name = row.get('name')
                row_description = row.get('description')
                row_config = row.get('configuration') or {}
                if not row_id or not isinstance(row_config, Mapping):
                    continue
                _search_configuration_sections(
                    component_id=component_id,
                    configuration_id=configuration_id,
                    configuration_row_id=row_id,
                    configuration_name=row_name,
                    configuration_description=row_description,
                    configuration=row_config,
                    search_sections=search_sections,
                    target_ids=normalized_ids,
                    matches=matches,
                )

    return matches


def get_created_by(
    metadata: Sequence[Mapping[str, JsonStruct]] | Mapping[str, JsonStruct],
) -> ComponentUsageRef | None:
    metadata_items = _coerce_metadata_list(metadata)
    component_id = get_metadata_property(metadata_items, MetadataField.CREATED_BY_COMPONENT_ID)
    configuration_id = get_metadata_property(metadata_items, MetadataField.CREATED_BY_CONFIGURATION_ID)
    timestamp = _get_latest_metadata_timestamp(
        metadata_items, [MetadataField.CREATED_BY_COMPONENT_ID, MetadataField.CREATED_BY_CONFIGURATION_ID]
    )
    if component_id is None or configuration_id is None or timestamp is None:
        return None
    return ComponentUsageRef(
        component_id=str(component_id),
        configuration_id=str(configuration_id),
        timestamp=timestamp,
    )


def get_last_updated_by(
    metadata: Sequence[Mapping[str, JsonStruct]] | Mapping[str, JsonStruct],
) -> ComponentUsageRef | None:
    metadata_items = _coerce_metadata_list(metadata)
    component_id = get_metadata_property(metadata_items, MetadataField.UPDATED_BY_COMPONENT_ID)
    configuration_id = get_metadata_property(metadata_items, MetadataField.UPDATED_BY_CONFIGURATION_ID)
    timestamp = _get_latest_metadata_timestamp(
        metadata_items, [MetadataField.UPDATED_BY_COMPONENT_ID, MetadataField.UPDATED_BY_CONFIGURATION_ID]
    )
    if component_id is None or configuration_id is None or timestamp is None:
        return None
    return ComponentUsageRef(
        component_id=str(component_id),
        configuration_id=str(configuration_id),
        timestamp=timestamp,
    )
