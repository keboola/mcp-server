from typing import Any, Mapping, Sequence

import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.search import PatternMatch, SearchHit
from keboola_mcp_server.tools.storage import usage as storage_usage


def _sorted_usage(output: Sequence[storage_usage.UsageById]) -> list[storage_usage.UsageById]:
    return sorted(output, key=lambda item: item.target_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('hits', 'expected'),
    [
        (
            [
                SearchHit(
                    component_id='keboola.ex-db',
                    configuration_id='cfg-1',
                    item_type='configuration',
                    updated='2024-01-01T00:00:00Z',
                    name='Config 1',
                ).with_matches([PatternMatch(scope='storage.input', patterns=['id-1', 'id-2'])]),
                SearchHit(
                    component_id='keboola.ex-db',
                    configuration_id='cfg-2',
                    item_type='configuration',
                    updated='2024-01-02T00:00:00Z',
                    name='Config 2',
                ).with_matches([PatternMatch(scope='storage.output', patterns=['id-1'])]),
            ],
            {
                'id-1': [
                    {
                        'component_id': 'keboola.ex-db',
                        'configuration_id': 'cfg-1',
                        'configuration_row_id': None,
                        'configuration_name': 'Config 1',
                        'used_in': 'storage.input',
                        'timestamp': '2024-01-01T00:00:00Z',
                    },
                    {
                        'component_id': 'keboola.ex-db',
                        'configuration_id': 'cfg-2',
                        'configuration_row_id': None,
                        'configuration_name': 'Config 2',
                        'used_in': 'storage.output',
                        'timestamp': '2024-01-02T00:00:00Z',
                    },
                ],
                'id-2': [
                    {
                        'component_id': 'keboola.ex-db',
                        'configuration_id': 'cfg-1',
                        'configuration_row_id': None,
                        'configuration_name': 'Config 1',
                        'used_in': 'storage.input',
                        'timestamp': '2024-01-01T00:00:00Z',
                    }
                ],
            },
        ),
        ([], {}),
    ],
    ids=['grouped_usage', 'empty_hits'],
)
async def test_find_id_usage_groups_matches(
    mocker: MockerFixture, hits: list[SearchHit], expected: dict[str, list[dict[str, Any]]]
) -> None:
    mocker.patch.object(storage_usage, 'fetch_configurations', autospec=True, return_value=hits)

    client = mocker.Mock()
    output = await storage_usage.find_id_usage(
        client, target_ids=['id-1', 'id-2'], scopes=('storage.input', 'storage.output')
    )
    output_sorted = _sorted_usage(output)

    output_map = {item.target_id: [ref.model_dump() for ref in item.usage_references] for item in output_sorted}
    assert output_map == expected


@pytest.mark.parametrize(
    ('metadata', 'expected'),
    [
        ([], None),
        (
            [
                {
                    'key': MetadataField.CREATED_BY_COMPONENT_ID,
                    'value': 'keboola.ex-db',
                    'timestamp': '2024-01-01T00:00:00Z',
                },
                {
                    'key': MetadataField.CREATED_BY_CONFIGURATION_ID,
                    'value': 'cfg-1',
                    'timestamp': '2024-01-02T00:00:00Z',
                },
                {
                    'key': MetadataField.CREATED_BY_CONFIGURATION_ROW_ID,
                    'value': 'row-1',
                    'timestamp': '2024-01-03T00:00:00Z',
                },
            ],
            {
                'component_id': 'keboola.ex-db',
                'configuration_id': 'cfg-1',
                'configuration_row_id': 'row-1',
                'used_in': None,
                'timestamp': '2024-01-03T00:00:00Z',
            },
        ),
        (
            [
                {
                    'key': MetadataField.CREATED_BY_CONFIGURATION_ID,
                    'value': 'cfg-1',
                    'timestamp': '2024-01-02T00:00:00Z',
                },
            ],
            None,
        ),
    ],
    ids=['empty', 'complete', 'missing_component'],
)
def test_get_created_by(metadata: list[Mapping[str, Any]], expected: dict[str, Any] | None) -> None:
    result = storage_usage.get_created_by(metadata)
    assert result.model_dump() if result else None is expected


@pytest.mark.parametrize(
    ('metadata', 'expected'),
    [
        ([], None),
        (
            [
                {
                    'key': MetadataField.UPDATED_BY_COMPONENT_ID,
                    'value': 'keboola.ex-db',
                    'timestamp': '2024-01-01T00:00:00Z',
                },
                {
                    'key': MetadataField.UPDATED_BY_CONFIGURATION_ID,
                    'value': 'cfg-1',
                    'timestamp': '2024-01-02T00:00:00Z',
                },
                {
                    'key': MetadataField.UPDATED_BY_CONFIGURATION_ROW_ID,
                    'value': 'row-1',
                    'timestamp': '2024-01-03T00:00:00Z',
                },
            ],
            {
                'component_id': 'keboola.ex-db',
                'configuration_id': 'cfg-1',
                'configuration_row_id': 'row-1',
                'used_in': None,
                'timestamp': '2024-01-03T00:00:00Z',
            },
        ),
        (
            [
                {
                    'key': MetadataField.UPDATED_BY_COMPONENT_ID,
                    'value': 'keboola.ex-db',
                    'timestamp': '2024-01-01T00:00:00Z',
                },
                {
                    'key': MetadataField.UPDATED_BY_CONFIGURATION_ID,
                    'value': 'cfg-1',
                    'timestamp': '2024-01-02T00:00:00Z',
                },
            ],
            {
                'component_id': 'keboola.ex-db',
                'configuration_id': 'cfg-1',
                'configuration_row_id': None,
                'used_in': None,
                'timestamp': '2024-01-02T00:00:00Z',
            },
        ),
        (
            [
                {
                    'key': MetadataField.UPDATED_BY_CONFIGURATION_ID,
                    'value': 'cfg-1',
                    'timestamp': '2024-01-02T00:00:00Z',
                },
            ],
            None,
        ),
    ],
    ids=['empty', 'complete-config-row', 'complete-config', 'missing_component'],
)
def test_get_last_updated_by(metadata: list[Mapping[str, Any]], expected: dict[str, Any] | None) -> None:
    result = storage_usage.get_last_updated_by(metadata)
    assert result.model_dump() if result else None is expected


@pytest.mark.parametrize(
    ('metadata', 'expected'),
    [
        ({'metadata': [{'key': 'a', 'value': '1'}]}, [{'key': 'a', 'value': '1'}]),
        ({'key': 'a', 'value': '1'}, [{'key': 'a', 'value': '1'}]),
        ({'metadata': ['bad']}, []),
        ({'metadata': [{'key': 'a', 'value': '1'}, 'bad']}, [{'key': 'a', 'value': '1'}]),
        ([{'key': 'a', 'value': '1'}, 'bad'], [{'key': 'a', 'value': '1'}]),
    ],
    ids=['metadata_list', 'single_item', 'invalid_metadata_list', 'mixed_metadata_list', 'mixed_list'],
)
def test_coerce_metadata_list(metadata: Any, expected: list[Mapping[str, Any]]) -> None:
    assert storage_usage._coerce_metadata_list(metadata) == expected


@pytest.mark.parametrize(
    ('metadata', 'keys', 'expected'),
    [
        ([], [MetadataField.CREATED_BY_COMPONENT_ID], None),
        (
            [
                {'key': MetadataField.CREATED_BY_COMPONENT_ID, 'timestamp': '2024-01-01T00:00:00Z'},
                {'key': MetadataField.CREATED_BY_COMPONENT_ID, 'timestamp': '2024-01-02T00:00:00Z'},
            ],
            [MetadataField.CREATED_BY_COMPONENT_ID],
            '2024-01-02T00:00:00Z',
        ),
        (
            [
                {'key': MetadataField.CREATED_BY_COMPONENT_ID, 'timestamp': '2024-01-01T00:00:00Z'},
                {'key': MetadataField.CREATED_BY_CONFIGURATION_ID, 'timestamp': '2024-01-02T00:00:00Z'},
            ],
            [MetadataField.CREATED_BY_CONFIGURATION_ID],
            '2024-01-02T00:00:00Z',
        ),
    ],
    ids=['empty', 'latest', 'filter_keys'],
)
def test_get_latest_metadata_timestamp(
    metadata: list[Mapping[str, Any]], keys: Sequence[str], expected: str | None
) -> None:
    assert storage_usage._get_latest_metadata_timestamp(metadata, keys) == expected
