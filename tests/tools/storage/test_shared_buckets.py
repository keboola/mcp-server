from typing import Any

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.storage.shared_buckets import (
    MAX_SHARED_BUCKETS_LIMIT,
    GetSharedBucketsOutput,
    SharedBucketDetail,
    get_shared_buckets,
    link_shared_bucket,
)
from keboola_mcp_server.tools.storage.tools import BucketDetail


def _shared_bucket_raw(bucket_id: str, **overrides: Any) -> dict[str, Any]:
    raw = {
        'id': bucket_id,
        'displayName': f'Display {bucket_id}',
        'stage': 'in',
        'description': 'A shared bucket.',
        'project': {'id': 'proj-1', 'name': 'Source Project'},
        'sharing': 'organization',
        'linkedBy': [],
        'tablesCount': 3,
        'rowsCount': 1000,
        'dataSizeBytes': 2048,
    }
    raw.update(overrides)
    return raw


class TestSharedBucketDetail:
    def test_flattens_project_fields(self) -> None:
        bucket = SharedBucketDetail.model_validate(_shared_bucket_raw('in.c-foo'))
        assert bucket.project_id == 'proj-1'
        assert bucket.project_name == 'Source Project'
        assert bucket.sharing == 'organization'
        assert bucket.tables_count == 3
        assert bucket.rows_count == 1000
        assert bucket.data_size_bytes == 2048


@pytest.mark.asyncio
class TestGetSharedBuckets:
    async def test_returns_shared_buckets_sorted_by_id(
        self, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(
            return_value=[_shared_bucket_raw('in.c-zzz'), _shared_bucket_raw('in.c-aaa')]
        )

        result = await get_shared_buckets(mcp_context_client)

        assert isinstance(result, GetSharedBucketsOutput)
        assert [b.id for b in result.shared_buckets] == ['in.c-aaa', 'in.c-zzz']
        assert result.total_count == 2
        assert result.message == 'Returning 2 of 2 shared buckets.'

    async def test_empty_result_has_no_message(self, mocker: MockerFixture, mcp_context_client: Context) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(return_value=[])

        result = await get_shared_buckets(mcp_context_client)

        assert result.shared_buckets == []
        assert result.total_count == 0
        assert result.message is None

    @pytest.mark.parametrize(
        ('limit', 'offset', 'expected_ids', 'expected_message'),
        [
            (1, 0, ['in.c-a'], 'Returning 1 of 3 shared buckets. Use offset=1 to see more.'),
            (2, 1, ['in.c-b', 'in.c-c'], 'Returning 2 of 3 shared buckets.'),
            (10, 0, ['in.c-a', 'in.c-b', 'in.c-c'], 'Returning 3 of 3 shared buckets.'),
            (10, 100, [], 'Returning 0 of 3 shared buckets.'),
        ],
        ids=['first-page', 'second-page-exact-fit', 'limit-larger-than-total', 'offset-beyond-total'],
    )
    async def test_pagination(
        self,
        limit: int,
        offset: int,
        expected_ids: list[str],
        expected_message: str,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(
            return_value=[_shared_bucket_raw('in.c-a'), _shared_bucket_raw('in.c-b'), _shared_bucket_raw('in.c-c')]
        )

        result = await get_shared_buckets(mcp_context_client, limit=limit, offset=offset)

        assert [b.id for b in result.shared_buckets] == expected_ids
        assert result.total_count == 3
        assert result.message == expected_message

    @pytest.mark.parametrize('bad_limit', [0, -1, MAX_SHARED_BUCKETS_LIMIT + 1])
    async def test_invalid_limit_falls_back_to_default(
        self, bad_limit: int, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(return_value=[_shared_bucket_raw('in.c-a')])

        result = await get_shared_buckets(mcp_context_client, limit=bad_limit)

        assert result.total_count == 1
        assert len(result.shared_buckets) == 1

    async def test_negative_offset_clamped_to_zero(self, mocker: MockerFixture, mcp_context_client: Context) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(return_value=[_shared_bucket_raw('in.c-a')])

        result = await get_shared_buckets(mcp_context_client, offset=-5)

        assert len(result.shared_buckets) == 1


@pytest.mark.asyncio
class TestLinkSharedBucket:
    async def test_links_bucket_and_returns_bucket_detail(
        self, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.bucket_link = mocker.AsyncMock(
            return_value={
                'id': 'in.c-linked',
                'name': 'c-linked',
                'displayName': 'Linked bucket',
                'stage': 'in',
                'created': '2026-01-01T00:00:00+0000',
            }
        )

        result = await link_shared_bucket(
            mcp_context_client,
            source_project_id='proj-1',
            source_bucket_id='in.c-foo',
            target_bucket_name='linked',
        )

        assert isinstance(result, BucketDetail)
        assert result.id == 'in.c-linked'
        keboola_client.storage_client.bucket_link.assert_called_once_with(
            name='linked',
            stage='in',
            source_project_id='proj-1',
            source_bucket_id='in.c-foo',
            display_name=None,
        )

    async def test_stage_derived_from_out_prefix(self, mocker: MockerFixture, mcp_context_client: Context) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.bucket_link = mocker.AsyncMock(
            return_value={
                'id': 'out.c-linked',
                'name': 'c-linked',
                'displayName': 'Linked bucket',
                'stage': 'out',
                'created': '2026-01-01T00:00:00+0000',
            }
        )

        await link_shared_bucket(
            mcp_context_client,
            source_project_id='proj-1',
            source_bucket_id='out.c-foo',
            target_bucket_name='linked',
        )

        assert keboola_client.storage_client.bucket_link.call_args.kwargs['stage'] == 'out'

    async def test_explicit_target_stage_overrides_derived_stage(
        self, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.bucket_link = mocker.AsyncMock(
            return_value={
                'id': 'out.c-linked',
                'name': 'c-linked',
                'displayName': 'Linked bucket',
                'stage': 'out',
                'created': '2026-01-01T00:00:00+0000',
            }
        )

        await link_shared_bucket(
            mcp_context_client,
            source_project_id='proj-1',
            source_bucket_id='in.c-foo',
            target_bucket_name='linked',
            target_stage='out',
        )

        assert keboola_client.storage_client.bucket_link.call_args.kwargs['stage'] == 'out'

    async def test_unparseable_stage_raises(self, mcp_context_client: Context) -> None:
        with pytest.raises(ToolError, match='Could not determine stage'):
            await link_shared_bucket(
                mcp_context_client,
                source_project_id='proj-1',
                source_bucket_id='no-stage-prefix',
                target_bucket_name='linked',
            )
