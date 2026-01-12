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


def search_json_string(
    json_string: str,
    pattern: str,
    *,
    mode: Literal['literal', 'wildcard', 'regex'] = 'literal',
    whole_word: bool = False,
    ignore_case: bool = True,
) -> bool:
    """
    Search inside a JSON string using different pattern modes.

    Args:
        json_string: Stringified JSON (e.g. json.dumps(obj))
        pattern: Search pattern as string
        mode:
            - "literal": exact text match
            - "wildcard": supports '*' like shell glob
            - "regex": full regular expression
        whole_word: Match full words only
        ignore_case: Case-insensitive search

    Returns:
        True if pattern is found, False otherwise
    """

    flags = re.IGNORECASE if ignore_case else 0

    # Escape literal pattern
    if mode == 'literal':
        regex = re.escape(pattern)

    # Convert wildcard -> regex
    elif mode == 'wildcard':
        # Escape everything except '*'
        regex = re.escape(pattern).replace(r'\*', '.*')

    # Regex mode
    elif mode == 'regex':
        regex = pattern

    else:
        raise ValueError(f'Unsupported mode: {mode}')

    # Whole word match
    if whole_word:
        regex = rf'\b{regex}\b'

    return re.search(regex, json_string, flags) is not None


def _matches_patterns(
    value: JsonStruct,
    patterns: Sequence[str],
    *,
    mode: Literal['literal', 'wildcard', 'regex'],
    whole_word: bool,
    ignore_case: bool,
) -> bool:
    haystack = _stringify_for_search(value)
    return any(
        search_json_string(
            haystack,
            pattern,
            mode=mode,
            whole_word=whole_word,
            ignore_case=ignore_case,
        )
        for pattern in patterns
    )


class DataMatch(BaseModel):
    item_type: Literal[ 'component', 'flow', 'data-app', 'transformation']
    bucket_id: str | None = None
    table_id: str | None = None
    component_id: str | None = None
    configuration_id: str | None = None
    configuration_row_id: str | None = None
    name: str | None = None
    description: str | None = None


def add_usage_tools(mcp: KeboolaMcpServer) -> None:
    """Add usage/search tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            search_keboola_objects,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer,
            tags={USAGE_TOOLS_TAG},
        )
    )


async def search_data_matches(
    client: KeboolaClient,
    patterns: Sequence[str],
    *,
    mode: Literal['literal', 'wildcard', 'regex'] = 'literal',
    whole_word: bool = False,
    ignore_case: bool = True,
    search_types: Optional[Sequence[SearchDataType]] = None,
) -> list[DataMatch]:
    """
    Searches through configurations (components, flows, data apps) and optionally buckets/tables.

    :param client: The Keboola client to use.
    :param patterns: Patterns to search for.
    :param mode: Search mode (literal, wildcard, regex).
    :param whole_word: Match whole words only.
    :param ignore_case: Case-insensitive search.
    :param search_types: Types to search in (bucket, table, component, flow, data-app).
    :return: A list of data matches.
    """
    normalized_patterns = _normalize_ids(patterns)
    if not normalized_patterns:
        return []

    normalized_types = _normalize_data_search_types(search_types or [])
    include_components = 'component' in normalized_types
    include_transformations = 'transformation' in normalized_types
    include_flow = 'flow' in normalized_types
    include_data_apps = 'data-app' in normalized_types

    matches: list[DataMatch] = []

    if 'bucket' in normalized_types:
        for bucket in await client.storage_client.bucket_list():
            if _matches_patterns(
                bucket, normalized_patterns, mode=mode, whole_word=whole_word, ignore_case=ignore_case
            ):
                matches.append(
                    DataMatch(
                        item_type='bucket',
                        bucket_id=bucket.get('id'),
                        name=bucket.get('displayName') or bucket.get('name'),
                        description=bucket.get('description'),
                    )
                )

    if 'table' in normalized_types:
        for bucket in await client.storage_client.bucket_list():
            bucket_id = bucket.get('id')
            if not bucket_id:
                continue
            tables = await client.storage_client.bucket_table_list(bucket_id, include=['columns', 'columnMetadata'])
            for table in tables:
                if _matches_patterns(
                    table, normalized_patterns, mode=mode, whole_word=whole_word, ignore_case=ignore_case
                ):
                    matches.append(
                        DataMatch(
                            item_type='table',
                            bucket_id=bucket_id,
                            table_id=table.get('id'),
                            name=table.get('displayName') or table.get('name'),
                            description=table.get('description'),
                        )
                    )

    if include_components or include_flow or include_data_apps:
        components = await client.storage_client.component_list(include=['configuration', 'rows'])
        for component in components:
            component_id = component.get('id')
            if not component_id:
                continue
            component_type = component.get('type')

            if component_id == DATA_APP_COMPONENT_ID and not include_data_apps:
                continue
            if component_id in {CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID} and not include_flow:
                continue
            if component_id not in {DATA_APP_COMPONENT_ID, CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID}:
                if component_type == 'transformation' and not include_transformations:
                    continue
                if component_type != 'transformation' and not include_components:
                    continue

            configurations = component.get('configurations', []) or []
            for configuration in configurations:
                configuration_id = _get_configuration_id(configuration)
                if not configuration_id:
                    continue

                config_name = configuration.get('name')
                config_description = configuration.get('description')
                config_definition = configuration.get('configuration') or {}

                config_match = _matches_patterns(
                    {
                        'component_id': component_id,
                        'component_type': component_type,
                        'configuration_id': configuration_id,
                        'name': config_name,
                        'description': config_description,
                        'configuration': config_definition,
                    },
                    normalized_patterns,
                    mode=mode,
                    whole_word=whole_word,
                    ignore_case=ignore_case,
                )

                if config_match:
                    matches.append(
                        DataMatch(
                            item_type=(
                                'data-app'
                                if component_id == DATA_APP_COMPONENT_ID
                                else (
                                    'flow'
                                    if component_id in {CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID}
                                    else 'transformation' if component_type == 'transformation' else 'component'
                                )
                            ),
                            component_id=component_id,
                            configuration_id=configuration_id,
                            name=config_name,
                            description=config_description,
                        )
                    )

                rows = configuration.get('rows', []) or []
                for row in rows:
                    row_id = row.get('id')
                    row_name = row.get('name')
                    row_description = row.get('description')
                    row_config = row.get('configuration') or {}
                    if not row_id:
                        continue

                    row_match = _matches_patterns(
                        {
                            'component_id': component_id,
                            'component_type': component_type,
                            'configuration_id': configuration_id,
                            'row_id': row_id,
                            'name': row_name,
                            'description': row_description,
                            'configuration': row_config,
                        },
                        normalized_patterns,
                        mode=mode,
                        whole_word=whole_word,
                        ignore_case=ignore_case,
                    )

                    if row_match:
                        matches.append(
                            DataMatch(
                                item_type=(
                                    'data-app'
                                    if component_id == DATA_APP_COMPONENT_ID
                                    else (
                                        'flow'
                                        if component_id in {CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID}
                                        else 'transformation' if component_type == 'transformation' else 'component'
                                    )
                                ),
                                component_id=component_id,
                                configuration_id=configuration_id,
                                configuration_row_id=row_id,
                                name=row_name or config_name,
                                description=row_description or config_description,
                            )
                        )

    return matches


@tool_errors()
async def search_keboola_objects(
    ctx: Context,
    patterns: Annotated[
        Sequence[str],
        Field(
            description=(
                'Search patterns to match. Multiple patterns use OR logic (matches ANY pattern). '
                'Examples: ["customer"], ["sales*", "revenue*"] for wildcards, ["flow-.*"] for regex. '
                'Do not pass empty strings.'
            )
        ),
    ],
    mode: Annotated[
        Literal['literal', 'wildcard', 'regex'],
        Field(
            description=(
                'Pattern matching mode: '
                '"literal" - exact text match (default, fastest), '
                '"wildcard" - use * for glob patterns (e.g., "sales*"), '
                '"regex" - full regular expressions (most powerful).'
            )
        ),
    ] = 'literal',
    whole_word: Annotated[
        bool,
        Field(
            description=(
                'When true, only matches complete words. Prevents partial matches like finding "test" in "latest". '
                'Useful for searching IDs or specific terms.'
            )
        ),
    ] = False,
    ignore_case: Annotated[
        bool,
        Field(description='When true, search ignores letter casing (e.g., "Sales" matches "sales"). Default: true.'),
    ] = True,
    search_types: Annotated[
        Sequence[SearchDataType],
        Field(
            description=(
                'Filter by object types: "bucket", "table", "component", "transformation", "flow", "data-app". '
                'Empty list or ["any"] searches all types. Use to narrow results when you know what you need.'
            )
        ),
    ] = tuple(),
) -> list[DataMatch]:
    """
    Deep search across Keboola objects including their full JSON configuration data.

    WHAT IT SEARCHES:
    - Buckets/Tables: name, description, metadata, column names, column descriptions, and entire API payload
    - Components/Flows/Data Apps/Transformations: name, description, and entire configuration JSON in raw format:
      * All configuration parameters and nested settings
      * Storage mappings (input/output tables)
      * Credentials and connection details
      * SQL queries and code blocks
      * Any other data stored in the configuration

    WHEN TO USE:
    - Find configurations by specific parameter values (e.g., API endpoints, database hosts)
    - Search deep in nested JSON structures (e.g., table mappings, processors)
    - Locate objects containing specific SQL code or queries
    - Find configurations with particular credentials or connection strings
    - Use advanced pattern matching with wildcards or regex

    PATTERN MATCHING:
    - literal (default): Exact text matching - patterns=["salesforce.com"]
    - wildcard: Glob patterns with * - patterns=["sales*"] matches "sales", "salesforce", "sales_data"
    - regex: Regular expressions - patterns=["flow-[0-9]+"] matches "flow-1", "flow-123"
    - Multiple patterns use OR logic (matches ANY pattern)

    USAGE EXAMPLES:

    1. Find extractors connecting to a specific database:
       patterns=["prod-db-server.company.com"], search_types=["component"]

    2. Find transformations using a specific input table:
       patterns=["in.c-main.customers"], search_types=["transformation"]

    3. Find all objects with "test" or "staging" in their configuration:
       patterns=["test", "staging"], mode="literal", search_types=["component", "transformation", "flow", "data-app"]

    4. Find in which flows is this component used? kds-team.ex-shopify 01k9cz233cvd1rga3zzx40g8qj
       patterns=["01k9cz233cvd1rga3zzx40g8qj"], search_types=["flows"]

    5. Find components with API version v2 or v3:
       patterns=["api/v[23]"], mode="regex", search_types=["component"]

    6. Find data apps using specific Python packages:
       patterns=["pandas", "streamlit"], search_types=["data-app"]

    7. Search for exact table IDs (avoid partial matches):
       patterns=["in.c-bucket.table"], whole_word=True

    8. Find configs with nested JSON structure (key-value in parameters):
       patterns=["\"parameters\":\\s*\\{.*api\\.paychex.*\\}"], mode="regex"

    9. Find configs with specific authentication type:
       patterns=["\"authentication\":\\s*\\{.*\"type\":\\s*\"oauth20\""], mode="regex"

    10. Find configs with incremental loading enabled:
        patterns=["\"incremental\":\\s*true"], mode="regex", search_types=["component", "transformation"]

    11. Find storage mappings referencing specific tables:
        patterns=["\"source\":\\s*\"in\\.*\\.customers\""], mode="regex", search_types=["transformation", "component"]
    
    12. Find SQL transformations that calculate avg_monetary_value or create rfm_segment_summary:
        patterns=["avg_monetary_value", "rfm_segment_summary"], mode="literal", search_types=["transformation"]
    
    13. Find which components use a specific table in input/output mappings (both directions):
        patterns=["out\\.c-RFM-Segment-Summary-for-App\\.rfm_segment_summary"], 
        mode="regex", 
        search_types=["component", "transformation"]
        
        # Or more specific - find only input mappings:
        patterns=["\"source\":\\s*\"out\\.c-RFM-Segment-Summary-for-App\\.rfm_segment_summary\""], 
        mode="regex"
        
        # Or find only output mappings:
        patterns=["\"destination\":\\s*\"out\\.c-RFM-Segment-Summary-for-App\\.rfm_segment_summary\""], 
        mode="regex"

    TIPS:
    - Use whole_word=True when searching for IDs to avoid partial matches
    - Start with literal mode for speed, use wildcard/regex for flexibility
    - Narrow results with search_types when you know the object type
    - Results include direct links to objects in Keboola UI
    """
    client = KeboolaClient.from_state(ctx.session.state)
    return await search_data_matches(
        client=client,
        patterns=patterns,
        mode=mode,
        whole_word=whole_word,
        ignore_case=ignore_case,
        search_types=search_types,
    )
